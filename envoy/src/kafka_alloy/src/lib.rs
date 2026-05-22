use core::num;
use kafka_protocol::messages::list_offsets_response::ListOffsetsPartitionResponse;
use kafka_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequestBuilder, TopicProduceData, TopicProduceDataBuilder,
};
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
};
use log::{debug, info, trace};
use std::any::Any;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::fs::metadata;
use std::rc::Rc;

use bytes::{Buf, BytesMut};
use std::io::Cursor;

use kafka_protocol::protocol::buf::ByteBuf;
use routing::*;

use byteorder::{BigEndian, ReadBytesExt};
use fatal::fatal;
use kafka_protocol::messages::*;
use kafka_protocol::protocol::buf::ByteBufMut;
use kafka_protocol::protocol::HeaderVersion;
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use std::convert::TryFrom;
extern crate serde_json; // This line is often optional in the 2018 edition of Rust
use multimap::MultiMap;
use serde_json::Value;

#[derive(Debug)]
pub struct BiDirectionalMap {
    original_to_virtuals: MultiMap<i32, i32>,
    virtual_to_original: HashMap<i32, i32>,
}

impl BiDirectionalMap {
    // Creates a new instance of BiDirectionalMap
    pub fn new() -> Self {
        BiDirectionalMap {
            original_to_virtuals: MultiMap::new(),
            virtual_to_original: HashMap::new(),
        }
    }

    // Inserts a new relation between an original and a virtual index
    fn insert(&mut self, original_index: i32, virtual_index: i32) {
        self.original_to_virtuals
            .insert(original_index, virtual_index);
        self.virtual_to_original
            .insert(virtual_index, original_index);
    }

    // Retrieves a vector of virtual indices associated with an original index
    pub fn get_virtuals_by_original(&self, original_index: i32) -> Option<&Vec<i32>> {
        self.original_to_virtuals.get_vec(&original_index)
    }

    // Retrieves the original index associated with a virtual index
    pub fn get_original_by_virtual(&self, virtual_index: i32) -> Option<&i32> {
        self.virtual_to_original.get(&virtual_index)
    }
}

pub struct KafkaRoutingProtocol {
    pub debug_protocol: i32,
}

impl KafkaRoutingProtocol {
    // https://docs.rs/kafka-protocol/0.2.0/kafka_protocol/

    // Computes vp / p mapping
    pub fn compute_virtual_partitions(num_sources: i32, num_partitions: i32) -> BiDirectionalMap {
        let mut bd_map = BiDirectionalMap::new();
        for vp in 0..num_sources * num_partitions {
            let p_origin =
                KafkaRoutingProtocol::get_partition_original_id(num_sources, num_partitions, vp);
            bd_map.insert(p_origin, vp);
        }
        bd_map
    }

    // https://stackoverflow.com/questions/69799181/flink-streaming-do-the-events-get-distributed-to-each-task-slots-separately-acc
    // operatorIndex
    //
    // https://github.com/apache/flink/blob/release-1.16/flink-connectors/flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/source/enumerator/KafkaSourceEnumerator.java#L437
    fn java_string_hashcode(s: &str) -> i32 {
        let mut hash = 0i32;
        for c in s.chars() {
            hash = 31i32.wrapping_mul(hash) + (c as i32);
        }
        hash
    }

    fn bit_mix(mut input: i32) -> i32 {
        input ^= (input as u32).wrapping_shr(16) as i32;
        input = input.wrapping_mul(0x85ebca6bu32 as i32);
        input ^= (input as u32).wrapping_shr(13) as i32;
        input = input.wrapping_mul(0xc2b2ae35u32 as i32);
        input ^= (input as u32).wrapping_shr(16) as i32;
        input
    }

    fn murmur_hash(mut code: i32) -> i32 {
        code = code.wrapping_mul(0xcc9e2d51u32 as i32);
        code = code.rotate_left(15);
        code = code.wrapping_mul(0x1b873593u32 as i32);

        code = code.rotate_left(13);
        code = code.wrapping_mul(5).wrapping_add(0xe6546b64u32 as i32);

        code ^= 4;
        code = Self::bit_mix(code);

        if code >= 0 {
            code
        } else if code != i32::MIN {
            -code
        } else {
            0
        }
    }
    pub fn get_keygroup(s: &str, max_parallelism: i32) -> i32 {
        let hash = Self::java_string_hashcode(s);
        let h = Self::murmur_hash(hash);
        h % max_parallelism
    }

    // {TODO: check in Flink}
    pub fn get_keygroup_slot(s: &str, parallelism: i32, max_parallelism: i32) -> i32 {
        Self::get_keygroup(s, max_parallelism) * parallelism / max_parallelism
    }

    pub fn get_split_owner(topic: &str, partition: i32, num_readers: i32) -> i32 {
        let hash_code = Self::java_string_hashcode(topic);
        let start_index = (hash_code.wrapping_mul(31) & 0x7FFFFFFF) as i32 % num_readers;
        debug!(
            "get_split_owner (start_index = {:?}, partition = {:?}, num_readers ={:?}",
            start_index, partition, num_readers
        );
        (start_index + partition) % num_readers
    }

    fn filter_key(key: &str, split: i32, parallelism: i32, max_parallelism: i32) -> bool {
        if Self::get_keygroup_slot(key, parallelism, max_parallelism) == split {
            true
        } else {
            false
        }
    }

    fn real_offset_to_virtual(num_sources: i64, offset: i64, split_id: i64) -> i64 {
        offset * num_sources as i64 + split_id as i64
    }

    fn virtual_offset_to_real(num_sources: i64, offset: i64, split_id: i64) -> i64 {
        (offset - split_id as i64) / num_sources as i64
    }

    // compute original partition id for virtual partition
    pub fn get_partition_original_id(
        num_sources: i32,
        num_partitions: i32,
        virtual_partition: i32,
    ) -> i32 {
        //let mut p = 0;
        let mut s = 0;
        let mut map_partitions: HashMap<i32, i32> = HashMap::new();
        for vp in 0..(num_sources * num_partitions) {
            let p = match map_partitions.get(&s) {
                Some(p) => (p.clone() + 1) % num_partitions,
                None => s % num_partitions, // if not existing (regular set current)
            };

            if vp == virtual_partition {
                return p;
            }
            map_partitions.insert(s, p);
            s = (s + 1) % num_sources;
        }
        return -1;
    }

    pub fn default() -> Rc<RefCell<dyn RoutingProtocol>> {
        let a = Self {
            //bootstrap_mapping: map
            debug_protocol: 0,
        };
        Rc::new(RefCell::new(a))
    }

    pub fn new(debug_protocol: i32) -> Rc<RefCell<dyn RoutingProtocol>> {
        let a = Self {
            //bootstrap_mapping: map
            debug_protocol: debug_protocol,
        };
        Rc::new(RefCell::new(a))
    }

    pub fn rc(self) -> Rc<RefCell<dyn RoutingProtocol>> {
        Rc::new(RefCell::new(self))
    }

    pub fn get_request_information(
        &self,
        request: &Vec<u8>,
    ) -> (RequestHeader, ApiKey, i16, i16, u64) {
        let mut buf = Cursor::new(request);

        //https://docs.rs/kafka-protocol/0.2.0/kafka_protocol/index.html#deserializing-an-unknown-request
        //https://kafka.apache.org/protocol.html#protocol_network
        let version = buf.peek_bytes(2 + 4..4 + 4).get_i16();
        let api_key = buf.peek_bytes(0 + 4..2 + 4).get_i16();
        let api_key = ApiKey::try_from(api_key).unwrap();
        let header_version = self.header_version(api_key, true, version);

        buf.set_position(4);
        let header = RequestHeader::decode(&mut buf, header_version).unwrap();
        let api_key = ApiKey::try_from(api_key).unwrap();

        (header, api_key, version, header_version, buf.position())
    }

    pub fn header_version(&self, api_key: ApiKey, header: bool, version: i16) -> i16 {
        match api_key {
            ApiKey::ApiVersionsKey => {
                if header {
                    ApiVersionsRequest::header_version(version)
                } else {
                    ApiVersionsResponse::header_version(version)
                }
            }
            ApiKey::MetadataKey => {
                if header {
                    MetadataRequest::header_version(version)
                } else {
                    MetadataResponse::header_version(version)
                }
            }
            ApiKey::ProduceKey => {
                if header {
                    ProduceRequest::header_version(version)
                } else {
                    ProduceResponse::header_version(version)
                }
            }

            ApiKey::FindCoordinatorKey => {
                if header {
                    FindCoordinatorRequest::header_version(version)
                } else {
                    FindCoordinatorResponse::header_version(version)
                }
            }
            ApiKey::JoinGroupKey => {
                if header {
                    JoinGroupRequest::header_version(version)
                } else {
                    JoinGroupResponse::header_version(version)
                }
            }
            ApiKey::OffsetFetchKey => {
                if header {
                    OffsetFetchRequest::header_version(version)
                } else {
                    OffsetFetchResponse::header_version(version)
                }
            }
            ApiKey::SyncGroupKey => {
                if header {
                    SyncGroupRequest::header_version(version)
                } else {
                    SyncGroupResponse::header_version(version)
                }
            }
            ApiKey::ListOffsetsKey => {
                if header {
                    ListOffsetsRequest::header_version(version)
                } else {
                    ListOffsetsResponse::header_version(version)
                }
            }
            ApiKey::FetchKey => {
                if header {
                    FetchRequest::header_version(version)
                } else {
                    FetchResponse::header_version(version)
                }
            }
            ApiKey::InitProducerIdKey => {
                if header {
                    InitProducerIdRequest::header_version(version)
                } else {
                    InitProducerIdResponse::header_version(version)
                }
            }
            ApiKey::OffsetCommitKey => {
                if header {
                    OffsetCommitRequest::header_version(version)
                } else {
                    OffsetCommitResponse::header_version(version)
                }
            }
            ApiKey::HeartbeatKey => {
                if header {
                    HeartbeatRequest::header_version(version)
                } else {
                    HeartbeatResponse::header_version(version)
                }
            }
            _ => fatal!("ApiKey not encoded : {:?}, please add it.", api_key),
        }
    }

    pub fn encode<T: Encodable, U: Encodable>(
        header_version: i16,
        header: T,
        body_version: i16,
        body: U,
    ) -> Vec<u8> {
        let mut response = BytesMut::new();
        response.put_gap(4);

        header.encode(&mut response, header_version);

        body.encode(&mut response, body_version);
        let length: u32 = response.len() as u32 - 4;
        let bytes_length = length.to_be_bytes();
        let beginning = &mut response[0..][..4];
        beginning.copy_from_slice(&bytes_length);
        response.to_vec()
    }

    pub fn decode_request(
        &self,
        payload: &Vec<u8>,
    ) -> Result<(i16, RequestHeader, Option<RequestKind>), &'static str> {
        let (header, api_key, version, header_version, position) =
            self.get_request_information(&payload);

        let mut buf = Cursor::new(payload);
        buf.set_position(position);
        let req = match api_key {
            ApiKey::ApiVersionsKey => Some(RequestKind::ApiVersionsRequest(
                ApiVersionsRequest::decode(&mut buf, header.request_api_version).unwrap(),
            )),
            ApiKey::MetadataKey => Some(RequestKind::MetadataRequest(
                MetadataRequest::decode(&mut buf, header.request_api_version).unwrap(),
            )),
            ApiKey::ProduceKey => Some(RequestKind::ProduceRequest(
                ProduceRequest::decode(&mut buf, header.request_api_version).unwrap(),
            )),

            ApiKey::FindCoordinatorKey => Some(RequestKind::FindCoordinatorRequest(
                FindCoordinatorRequest::decode(&mut buf, header.request_api_version).unwrap(),
            )),
            ApiKey::JoinGroupKey => Some(RequestKind::JoinGroupRequest(
                JoinGroupRequest::decode(&mut buf, header.request_api_version).unwrap(),
            )),
            ApiKey::OffsetFetchKey => Some(RequestKind::OffsetFetchRequest(
                OffsetFetchRequest::decode(&mut buf, header.request_api_version).unwrap(),
            )),
            ApiKey::SyncGroupKey => Some(RequestKind::SyncGroupRequest(
                SyncGroupRequest::decode(&mut buf, header.request_api_version).unwrap(),
            )),
            ApiKey::ListOffsetsKey => Some(RequestKind::ListOffsetsRequest(
                ListOffsetsRequest::decode(&mut buf, header.request_api_version).unwrap(),
            )),
            ApiKey::FetchKey => Some(RequestKind::FetchRequest(
                FetchRequest::decode(&mut buf, header.request_api_version).unwrap(),
            )),
            ApiKey::HeartbeatKey => Some(RequestKind::HeartbeatRequest(
                HeartbeatRequest::decode(&mut buf, header.request_api_version).unwrap(),
            )),
            ApiKey::InitProducerIdKey => Some(RequestKind::InitProducerIdRequest(
                InitProducerIdRequest::decode(&mut buf, header.request_api_version).unwrap(),
            )),
            _ => {
                debug!("default");
                None
            }
        };
        Ok((header_version, header, req))
    }

    pub fn decode_response(
        &self,
        payload_request: &Vec<u8>,
        payload_response: &Vec<u8>,
    ) -> Result<
        (
            i16,
            RequestHeader,
            ResponseHeader,
            Option<ResponseKind>,
            u32,
        ),
        &'static str,
    > {
        let mut buf = Cursor::new(payload_response);
        let length: u32 = buf.read_u32::<BigEndian>().unwrap();
        if payload_response.len() - 4 < length as usize {
            return Err("split");
        }

        let (header_req, api_key, version, header_version, _) =
            self.get_request_information(payload_request);
        trace!(
            "Merge request: Api Key: {} - header: {:?}",
            header_req.request_api_key,
            header_req
        );

        buf.set_position(4); // we skip the length

        let header_version = self.header_version(api_key, false, version);
        let header = ResponseHeader::decode(&mut buf, version).unwrap();

        debug!("Merge response: header: {:?}", header);
        buf.set_position(header.compute_size(header_version).unwrap() as u64 + 4);

        let resp = match api_key {
            ApiKey::ApiVersionsKey => Some(ResponseKind::ApiVersionsResponse(
                ApiVersionsResponse::decode(&mut buf, header_req.request_api_version).unwrap(),
            )),

            ApiKey::MetadataKey => Some(ResponseKind::MetadataResponse(
                MetadataResponse::decode(&mut buf, header_req.request_api_version).unwrap(),
            )),

            ApiKey::ProduceKey => Some(ResponseKind::ProduceResponse(
                ProduceResponse::decode(&mut buf, header_req.request_api_version).unwrap(),
            )),

            ApiKey::FindCoordinatorKey => {
                let fcr = FindCoordinatorResponse::decode(&mut buf, header_req.request_api_version);
                debug!("decode FC: {:?}", fcr);
                Some(ResponseKind::FindCoordinatorResponse(fcr.unwrap()))
            }
            ApiKey::JoinGroupKey => Some(ResponseKind::JoinGroupResponse(
                JoinGroupResponse::decode(&mut buf, header_req.request_api_version).unwrap(),
            )),
            ApiKey::LeaveGroupKey => Some(ResponseKind::LeaveGroupResponse(
                LeaveGroupResponse::decode(&mut buf, header_req.request_api_version).unwrap(),
            )),
            ApiKey::OffsetFetchKey => Some(ResponseKind::OffsetFetchResponse(
                OffsetFetchResponse::decode(&mut buf, header_req.request_api_version).unwrap(),
            )),
            ApiKey::SyncGroupKey => Some(ResponseKind::SyncGroupResponse(
                SyncGroupResponse::decode(&mut buf, header_req.request_api_version).unwrap(),
            )),
            ApiKey::ListOffsetsKey => Some(ResponseKind::ListOffsetsResponse(
                ListOffsetsResponse::decode(&mut buf, header_req.request_api_version).unwrap(),
            )),
            ApiKey::FetchKey => Some(ResponseKind::FetchResponse(
                FetchResponse::decode(&mut buf, header_req.request_api_version).unwrap(),
            )),
            ApiKey::HeartbeatKey => Some(ResponseKind::HeartbeatResponse(
                HeartbeatResponse::decode(&mut buf, header_req.request_api_version).unwrap(),
            )),

            _ => {
                debug!("default");
                None
            }
        };

        Ok((header_version, header_req, header, resp, length))
    }
    pub fn get_encoding_options() -> RecordEncodeOptions {
        RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        }
    }
}

impl RoutingProtocol for KafkaRoutingProtocol {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn payload_compare_size(&self, _is_request: bool, payload: &Vec<u8>) -> isize {
        let mut buf = Cursor::new(payload);
        let length: u32 = buf.read_u32::<BigEndian>().unwrap();
        (payload.len() - 4) as isize - length as isize
    }

    fn request_response_type(
        &self,
        payload: &Vec<u8>,
    ) -> (RequestType, RequestResponseKind, RequestResponseKind) {
        let mut buf = Cursor::new(payload);
        let length: u32 = buf.read_u32::<BigEndian>().unwrap();
        debug!(
            "Kafka request : packet ({} bytes received for total length {})",
            payload.len() - 4,
            length
        );
        if payload.len() - 4 < length as usize {
            return (
                RequestType::Incomplete,
                RequestResponseKind::Incomplete,
                RequestResponseKind::Incomplete,
            );
        }
        //https://docs.rs/kafka-protocol/0.2.0/kafka_protocol/index.html#deserializing-an-unknown-request
        //https://kafka.apache.org/protocol.html#protocol_network
        let version = buf.peek_bytes(2 + 4..4 + 4).get_i16();
        let api_key = buf.peek_bytes(0 + 4..2 + 4).get_i16();
        let api_key = ApiKey::try_from(api_key).unwrap();
        let header_version = self.header_version(api_key, true, version);

        buf.set_position(4);
        let header = RequestHeader::decode(&mut buf, header_version).unwrap();

        debug!(
            "RRT: Api Key: {} - header: {:?} - version: {} - size {:?}",
            header.request_api_key,
            header,
            version,
            header.compute_size(version)
        );

        if self.debug_protocol > 0 {
            info!("{:?}", self.decode_request(payload));
        }
        match api_key {
            ApiKey::ApiVersionsKey => (
                RequestType::RequestResponse,
                RequestResponseKind::Local,
                RequestResponseKind::Local,
            ),
            ApiKey::MetadataKey => (
                RequestType::RequestResponse,
                RequestResponseKind::Local,
                RequestResponseKind::Local,
            ),
            ApiKey::ProduceKey => (
                RequestType::RequestResponse,
                RequestResponseKind::Routable,
                RequestResponseKind::Routable,
            ),

            ApiKey::FindCoordinatorKey => (
                RequestType::RequestResponse,
                RequestResponseKind::Routable,
                RequestResponseKind::Routable,
            ),
            ApiKey::JoinGroupKey => (
                RequestType::RequestResponse,
                RequestResponseKind::Routable,
                RequestResponseKind::Routable,
            ),
            ApiKey::OffsetFetchKey => (
                RequestType::RequestResponse,
                RequestResponseKind::Routable,
                RequestResponseKind::Routable,
            ),
            ApiKey::SyncGroupKey => (
                RequestType::RequestResponse,
                RequestResponseKind::System,
                RequestResponseKind::System,
            ),
            ApiKey::ListOffsetsKey => (
                RequestType::RequestResponse,
                RequestResponseKind::Routable,
                RequestResponseKind::Routable,
            ),
            ApiKey::FetchKey => (
                RequestType::RequestResponse,
                RequestResponseKind::Routable,
                RequestResponseKind::Routable,
            ),
            ApiKey::HeartbeatKey => (
                RequestType::RequestResponse,
                RequestResponseKind::Local,
                RequestResponseKind::Local,
            ),
            _ => (
                RequestType::RequestResponse,
                RequestResponseKind::Routable,
                RequestResponseKind::Routable,
            ),
        }

        //(RequestType::RequestResponse, RequestResponseKind::Routable, RequestResponseKind::Routable)
    }
    fn response_type(
        &self,
        _routing_table: &RoutingTable,
        payload: &Vec<u8>,
    ) -> (RequestResponseKind) {
        let mut buf_req = Cursor::new(payload);
        let length: u32 = buf_req.read_u32::<BigEndian>().unwrap();
        if payload.len() - 4 < length as usize {
            return RequestResponseKind::Incomplete;
        }
        RequestResponseKind::Routable
    }

    fn split_queries(
        &self,
        routing_table: &RoutingTable,
        request: &Vec<u8>,
    ) -> Result<BTreeMap<String, Vec<u8>>, &'static str> {
        let mut requests: BTreeMap<String, Vec<u8>> = BTreeMap::new();

        if self.debug_protocol > 0 {
            let (header_version, header, req) = match self.decode_request(request) {
                Ok(values) => values,
                Err(e) => return Err(e),
            };
        }

        requests.insert(
            routing_table.default_cluster.clone().unwrap(),
            request.clone(),
        );
        Ok(requests)
    }

    fn merge_responses(
        &self,
        routing_table: &RoutingTable,
        response_map: &BTreeMap<u32, Vec<u8>>,
    ) -> Result<Vec<u8>, &'static str> {
        let mut response: Vec<u8>;
        response = response_map.values().next().unwrap().to_vec(); // default: first response
        for entry in &routing_table.current_requests {
            // if local available, we take it
            let token = entry.0;
            let cluster = &entry.1 .0;
            let payload = &entry.1 .1;

            if Some(cluster) == routing_table.local_cluster.as_ref() {
                debug!("local response found, using it");
                response = response_map.get(token).unwrap().to_vec();
                break;
            }
        }
        if self.debug_protocol > 0 {
            let (header_version, header_req, header, resp, length) = match self
                .decode_response(routing_table.origin_request.as_ref().unwrap(), &response)
            {
                Ok(values) => values,
                Err(e) => return Err(e),
            };

            info!("response: {:?}", resp);
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_partition_splitid() {
        let num_sources = 2;
        let parallelism = 2;
        let virtual_partition = 2;
        let expected = 1;

        /*
         * Assumption : 4 sources are distributed amongst TM using modulo
         *
         * TM0 - s0 -(0)-> p0
         * TM1 - s1 -(1)-> p1
         * TM0 - s2 -(1)-> p1
         * TM1 - s3 -(0)-> p0
         */
        let split_id = KafkaRoutingProtocol::get_partition_original_id(
            num_sources,
            parallelism,
            virtual_partition,
        );
        assert_eq!(
            split_id, expected,
            "Expected split id to be {:?} for given parameters",
            expected
        );

        let num_sources = 2;
        let parallelism = 2;
        let virtual_partition = 3;
        let expected = 0;

        /*
         * Assumption : 4 sources are distributed amongst TM using modulo
         *
         * TM0 - s0 -(0)-> p0
         * TM1 - s1 -(1)-> p1
         * TM0 - s2 -(1)-> p1
         * TM1 - s3 -(0)-> p0
         */
        let split_id = KafkaRoutingProtocol::get_partition_original_id(
            num_sources,
            parallelism,
            virtual_partition,
        );
        assert_eq!(
            split_id, expected,
            "Expected split id to be {:?} for given parameters",
            expected
        );

        /*
         * Assumption : 6 sources are distributed amongst TM using modulo
         *
         * TM0 - s0 -(0)-> p0
         * TM1 - s1 -(1)-> p1
         * TM2 - s2 -(0)-> p0
         * TM0 - s3 -(1)-> p1
         * TM1 - s4 -(0)-> p0
         * TM2 - s5 -(1)-> p1
         */
        let num_sources = 3;
        let parallelism = 2;
        let virtual_partition = 3;
        let expected = 1;

        let split_id = KafkaRoutingProtocol::get_partition_original_id(
            num_sources,
            parallelism,
            virtual_partition,
        );
        assert_eq!(
            split_id, expected,
            "Expected split id to be {:?} for given parameters",
            expected
        );
    }
}
