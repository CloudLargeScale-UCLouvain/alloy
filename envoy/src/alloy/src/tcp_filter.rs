use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use bytes::Buf;
use bytes::BytesMut;
use kafka_alloy::KafkaRoutingProtocol;
use kafka_protocol::messages::fetch_response::FetchableTopicResponse;
use kafka_protocol::messages::fetch_response::PartitionData;
use kafka_protocol::messages::list_offsets_request::ListOffsetsPartition;
use kafka_protocol::messages::list_offsets_request::ListOffsetsTopic;
use kafka_protocol::messages::list_offsets_response::ListOffsetsPartitionResponse;
use kafka_protocol::messages::FetchResponse;
use kafka_protocol::messages::ListOffsetsRequest;
use kafka_protocol::messages::ResponseHeader;
use kafka_protocol::messages::ResponseKind;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::Builder;
use kafka_protocol::protocol::Decodable;
use kafka_protocol::records::RecordBatchDecoder;
use kafka_protocol::records::RecordBatchEncoder;
use log::{debug, error, trace, warn};
use proxy_wasm::hostcalls;

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Cursor;
use std::rc::Rc;

use kafka_alloy::BiDirectionalMap;
use kafka_protocol::messages::FetchRequest;
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::messages::RequestKind;
use kafka_protocol::records::Record;
use proxy_wasm::traits::*;
use proxy_wasm::types::*;
use regex::bytes::Regex;
use routing::RoutingConfiguration;
use routing::RoutingProtocol;
use routing::RoutingTable;
use serde_json::Value;

use crate::data::*;

pub struct AlloyFilter {
    pub context_id: u32,
    pub configuration: RoutingConfiguration,
    pub protocol: Rc<RefCell<dyn RoutingProtocol>>,
    pub routing_table: RoutingTable,
    pub request_header: Option<RequestHeader>,
    pub request: Option<RequestKind>,
    pub fetch_request_api_version: i16,
    pub correlation_id: i32,
    pub data_size: usize,
    pub map_alloy_filters: HashMap<String, AlloyFilterCriteria>,
    pub max_parallelism: i32,
    pub map_split_id: HashMap<(String, i32), i32>,
    pub map_index_query_partition: HashMap<(String, i32), i64>, // keep trace of each msg index for each vp
    pub bimap_virtual_origin: HashMap<String, BiDirectionalMap>,
    pub remove_empty_records: bool,
    pub session_id: i32,
    pub session_epoch: i32,
    pub origin_fetch_request: Option<FetchRequest>,
    pub map_queue_vp: HashMap<String, u32>,
    pub map_vp_records: RefCell<HashMap<(String, i32), Vec<Record>>>,
    pub fetch_min_bytes_virtual_partition: usize,
}
impl Context for AlloyFilter {}
impl AlloyFilter {
    // remove "empty records" except the last one
    fn filter_vector_records(elements: &mut Vec<Record>, last_element: &Record) -> (usize, usize) {
        let total_count = elements.len();

        // Retain elements based on the condition
        elements.retain(|e| e.key.is_some() || e.value.is_some());
        if elements.len() == 0 || elements.last().unwrap().offset != last_element.offset {
            elements.push(last_element.clone());
        }

        let removed_count = total_count - elements.len();
        (removed_count, elements.len())
    }

    fn filter_json_in_place(json: &mut Value, field_vec: &Vec<String>, reordering: bool) {
        if let Value::Object(ref mut map) = json {
            map.retain(|k, _| field_vec.contains(k)); // Check presence of each field in `field_set`
            if reordering {
                for key in field_vec {
                    if let Some(value) = map.remove(key) {
                        map.insert(key.to_string(), value);
                    }
                }
            }
        }
    }

    /// Efficiently removes and returns a slice of records within `max_size` from `records`.
    fn take_records_within_size(&self, records: &mut Vec<Record>, max_size: usize) -> Vec<Record> {
        let mut total_size = 0;
        let mut count = 0;

        // First, count how many records fit within max_size
        for record in &*records {
            let key_size = record.key.as_ref().map_or(0, |v| v.len()); // Handle Option<Bytes>
            let value_size = record.value.as_ref().map_or(0, |v| v.len()); // Handle Option<Bytes>
            let record_size = key_size + value_size;

            if total_size + record_size > max_size {
                break;
            }
            total_size += record_size;
            count += 1;
        }
        debug!("total size {} max_size {}", total_size, max_size);

        if total_size >= max_size {
            records.drain(..count).collect()
        } else {
            Vec::new()
        }
    }

    fn records_at_least_size(&self, records: &mut Vec<Record>, max_size: usize) -> bool {
        let mut total_size = 0;

        // First, count how many records fit within max_size
        for record in &*records {
            let key_size = record.key.as_ref().map_or(0, |v| v.len()); // Handle Option<Bytes>
            let value_size = record.value.as_ref().map_or(0, |v| v.len()); // Handle Option<Bytes>
            let record_size = key_size + value_size;

            if total_size + record_size > max_size {
                return true;
            }
            total_size += record_size;
        }
        false
    }

    fn process_records(
        &self,
        req: &FetchRequest,
        resp: &mut FetchResponse,
        map_vp_records: &mut HashMap<(String, i32), Vec<Record>>,
    ) {
        resp.responses.iter_mut().for_each(|resp_topic| {
            debug!(
                "resp_topic : {:?} resp_partitions",
                resp_topic.partitions.len()
            );
            resp_topic.partitions.iter_mut().for_each(|resp_partition| {
                debug!(
                    "resp_partition {:?} {:?}",
                    resp_partition.partition_index,
                    resp_partition.records.as_ref().unwrap().len()
                );
            });
            resp_topic.partitions.iter_mut().for_each(|resp_partition| {
                // find corresponding topic request. It must exist so unwrap.
                if let Some(req_topic) = req.topics.iter().find(|t| t.topic == resp_topic.topic) {
                    //TODO: may optimize this part checking bimap every time
                    let req_partition = req_topic
                        .partitions
                        .iter()
                        .find(|p| {
                            self.bimap_virtual_origin
                                .get(&**req_topic.topic)
                                .unwrap()
                                .get_original_by_virtual(p.partition)
                                .unwrap()
                                == &resp_partition.partition_index
                        })
                        .unwrap();
                    let req_offset = req_partition.fetch_offset;
                    if resp_partition.records.is_none() {
                        debug!("records is none {:?}", resp_partition);
                    }
                    debug!(
                        "resp_partition data size : {:?}",
                        resp_partition.records.as_ref().unwrap().len()
                    );
                    self.filter_records_optimized(
                        &resp_topic.topic,
                        req_partition.partition,
                        resp_partition,
                        req_offset,
                        map_vp_records,
                    );
                    // TODO: put back the original target partition index instead of the original
                    resp_partition.partition_index = req_partition.partition;
                    //TODO: see if necessary to trace fetch_offsets
                    //AlloyFilter::set_map_index_query_partition(&mut self.map_index_query_partition, (req_topic.topic.to_string(), req_partition.partition) , req_offset);
                    let topic: String = req_topic.topic.to_string();
                    let target_origin_partition = self
                        .bimap_virtual_origin
                        .get(&topic)
                        .unwrap()
                        .get_original_by_virtual(req_partition.partition)
                        .unwrap();
                    let target_virtual_partitions = self
                        .bimap_virtual_origin
                        .get(&topic)
                        .unwrap()
                        .get_virtuals_by_original(target_origin_partition.clone())
                        .unwrap();

                    // Synchronize

                    // check timeout before send

                    for vp in target_virtual_partitions {
                        if *vp != req_partition.partition {
                            debug!("get {:?}", &(resp_topic.topic.to_string(), *vp));
                            let records = map_vp_records
                                .entry((topic.to_string(), *vp))
                                .or_insert_with(|| Vec::new());

                            debug!("Virtual partition {}: {} records", vp, records.len());
                            //FIXME: change this
                            if self.records_at_least_size(
                                records,
                                self.fetch_min_bytes_virtual_partition,
                            ) {
                                //let transmit_records = self.take_records_within_size(records, self.fetch_min_bytes_virtual_partition);
                                debug!(
                                    "Encoding {:?} records for virtual partition {:?} - last: {:?}",
                                    records.len(),
                                    vp,
                                    records.last().unwrap()
                                );
                                let mut buf: BytesMut = BytesMut::new();
                                // serialize each virtual partition
                                match RecordBatchEncoder::encode(
                                    &mut buf,
                                    records.iter(),
                                    &KafkaRoutingProtocol::get_encoding_options(),
                                ) {
                                    Ok(v) => v,
                                    Err(e) => error!("error encoding records: {:?}", e),
                                };
                                let data = Some(buf.freeze().to_vec());
                                self.push_partition_records_to_shared_data(
                                    &topic, *vp, req_offset, data,
                                );
                                records.clear();
                            }
                        }
                    }
                }
            });
        });
    }

    fn filter_records_optimized(
        &self,
        topic: &str,
        req_partition: i32,
        data: &mut PartitionData,
        fetch_offset: i64,
        map_vp_records: &mut HashMap<(String, i32), Vec<Record>>,
    ) {
        let target_origin_partition = self
            .bimap_virtual_origin
            .get(topic)
            .unwrap()
            .get_original_by_virtual(req_partition)
            .unwrap();
        let target_virtual_partitions = self
            .bimap_virtual_origin
            .get(topic)
            .unwrap()
            .get_virtuals_by_original(target_origin_partition.clone())
            .unwrap();

        let alloy_filter = self.map_alloy_filters.get(topic).unwrap();

        // prepare map of projections for reuse
        let mut buffer_selection;

        if let Some(mut data_records) = data.records.take() {
            debug!(
                "Decoding records for {:?} {:?}(req:{:?}) - length {:?}",
                topic,
                data.partition_index,
                req_partition,
                data_records.len()
            );
            // clone fully the records for each
            if alloy_filter.partition.is_empty()
                && alloy_filter.projections.len() == 0
                && alloy_filter.selections.len() == 0
            {
                data.records = Some(data_records);
                return;
            }
            //FIXME: it seems that record batches are limited by max size max.message.bytes / max.partition.fetch.bytes
            let mut records = Vec::new();
            while data_records.has_remaining() {
                match RecordBatchDecoder::decode_batch(&mut data_records, &mut records) {
                    Ok(_) => {}
                    Err(e) => {
                        debug!("error {:?} on {:?}", e, data_records);
                        break;
                    }
                };
            }
            if records.is_empty() {
                debug!("no records to filter {:?}", data);
                return;
            }

            let last_element = records.last().unwrap().clone();
            let mut last_element_partition: Option<i32> = None;
            let last_index = records.len() - 1;

            for (index, record) in records.iter_mut().enumerate() {
                let key = record.key.take();
                let value = record.value.take();
                if let Some(mut value_bytes) = value {
                    let mut json_value: Value =
                        serde_json::from_slice(&value_bytes).expect("Failed to deserialize");

                    // Selection
                    let mut is_match = true;
                    for filter in &alloy_filter.selections {
                        if let Some(attribute) = &filter.attribute {
                            if let Some(value) = json_value.get(attribute) {
                                let value_as_str = match value {
                                    Value::String(s) => s,
                                    Value::Number(n) => {
                                        // use buffer_selection
                                        buffer_selection = n.to_string();
                                        &buffer_selection
                                    }
                                    Value::Bool(b) => {
                                        if *b {
                                            "true"
                                        } else {
                                            "false"
                                        }
                                    } // Convert booleans to "true" or "false"
                                    Value::Null => "null",
                                    _ => {
                                        error!("unsupported type {}", attribute);
                                        ""
                                    } // Handle other types if needed
                                };
                                is_match = filter
                                    .value_regex
                                    .as_ref()
                                    .map_or(true, |r| r.is_match(value_as_str.as_bytes()));
                                trace!(
                                    "matching {:?} to {:?} : {:?}",
                                    value,
                                    filter.value,
                                    is_match
                                );
                                if !is_match {
                                    break;
                                }
                            }
                        }
                    }
                    if !is_match {
                        continue; // as the record key & value is none, we skip it
                    }

                    // Projection
                    if !alloy_filter.projections.is_empty() {
                        Self::filter_json_in_place(
                            &mut json_value,
                            &alloy_filter.projections,
                            !alloy_filter.partition.is_empty(),
                        );
                        value_bytes = Bytes::from(json_value.to_string()).into();
                    }

                    // Partition
                    if !alloy_filter.partition.is_empty() {
                        let values = extract(&json_value, &alloy_filter);

                        let keygroup_slot = get_keygroup_slot(
                            &values,
                            alloy_filter.num_sources,
                            self.max_parallelism,
                        );
                        debug!("Values: {:?} Keygroupslot {:?}", &values, keygroup_slot);
                        let mut opt_value = Some(value_bytes);
                        for vp in target_virtual_partitions {
                            if vp != &req_partition {
                                //debug!("get split_id virtual p {} {} in {:?}", topic, *vp, self.map_split_id);
                                let split_id =
                                    self.map_split_id.get(&(topic.to_string(), *vp)).unwrap();
                                if &keygroup_slot == split_id {
                                    debug!("Found vp splitid for {} {}", split_id, vp);
                                    let mut virtual_record = record.clone();

                                    if index == last_index {
                                        last_element_partition = Some(*vp);
                                    }

                                    virtual_record.value = opt_value.take();
                                    map_vp_records
                                        .entry((topic.to_string(), *vp))
                                        .or_insert(Vec::new())
                                        .push(virtual_record);
                                    break;
                                }
                            }
                        }
                        if opt_value.is_some() {
                            // set for regular partition if not taken
                            //trace!("get split_id regular p {} {} in {:?}", topic, req_partition, self.map_split_id);
                            let split_id = self
                                .map_split_id
                                .get(&(topic.to_string(), req_partition))
                                .unwrap();
                            debug!("Found p splitid for {} {}", split_id, req_partition);
                            if &keygroup_slot == split_id {
                                record.value = opt_value.take();
                            } else {
                                error!(
                                    "split id {:?} not found! keygroupslot : {:?}",
                                    &split_id, keygroup_slot
                                );
                            }
                        }
                    } else {
                        record.key = key;
                        record.value = Some(value_bytes);
                    }
                }
            }

            for vp in target_virtual_partitions {
                if *vp != req_partition {
                    // add last element if needede
                    if let Some(last_element_partition) = last_element_partition {
                        if last_element_partition != *vp {
                            map_vp_records
                                .entry((topic.to_string(), *vp))
                                .or_insert(Vec::new())
                                .push(last_element.clone());
                        }
                    }
                }
            }

            if self.remove_empty_records {
                // remove "empty" records except the last one. to avoid transmit useless transfer, the last one is there for the SP engine to keep track of the position
                //TODO: optimization: do this on the fly while dispatching
                let (removed_count, final_count) =
                    AlloyFilter::filter_vector_records(&mut records, &last_element);
                debug!(
                    "Filtering virtual partition (request) {} : Filtered {} out of {}, final {} ",
                    req_partition,
                    removed_count,
                    removed_count + final_count,
                    final_count
                );
            }
            debug!(
                "Encoding {:?} records for virtual partition (request) {:?} last: {:?}",
                records.len(),
                req_partition,
                records.last().unwrap()
            );

            // serialize regular partition records
            let mut buf: BytesMut = BytesMut::new();
            match RecordBatchEncoder::encode(
                &mut buf,
                records.iter(),
                &KafkaRoutingProtocol::get_encoding_options(),
            ) {
                Ok(v) => v,
                Err(e) => error!("error encoding records: {:?}", e),
            };
            data.records = Some(buf.freeze());
        }
    }

    fn pop_shared_optimized(
        &self,
        topic: &str,
        partition: i32,
        fetch_offset: i64,
    ) -> (i32, i32, Option<PartitionData>) {
        let shared_data_key = format!("{:?}-{:?}-{}", topic, partition, fetch_offset);
        debug!("pop_shared_optimized {:?}", shared_data_key);
        let mut loo: Option<Status> = Some(Status::NotFound);
        let mut partition_data: Option<PartitionData> = None;
        let mut session_id = -1;
        let mut session_epoch = -1;
        while loo.is_some() {
            let (data, cas) = self.get_shared_data(&shared_data_key);
            if data.is_some() {
                let mut buf = Cursor::new(data.unwrap());
                let body_version = self.request_header.as_ref().unwrap().request_api_version;
                session_id = buf.read_i32::<BigEndian>().ok().unwrap();
                session_epoch = buf.read_i32::<BigEndian>().ok().unwrap();
                partition_data = match PartitionData::decode(&mut buf, body_version) {
                    Ok(v) => Some(v),
                    e => {
                        error!(
                            "unable to deserialize shared data partition {:?}: {:?} - {:?}",
                            shared_data_key, e, buf
                        );
                        None
                    }
                };
                debug!(
                    "pop_shared_optimized {:?}, body v : {:?} - found({:?}): {:?} ",
                    shared_data_key, body_version, cas, partition_data
                );
            } else {
                debug!(
                    "pop_shared_optimized {:?}, found({:?}): {:?} ",
                    shared_data_key, cas, "None!"
                );
                // shared data not yet set
                return (session_id, session_epoch, None);
            }
            loo = match self.set_shared_data(&shared_data_key, None, cas) {
                Ok(_) => None,
                Err(e) => {
                    if e == Status::CasMismatch {
                        debug!("cas error! retrying for {:?}", shared_data_key);
                        Some(e)
                    } else {
                        fatal::fatal!(
                            "error {:?} while setting shared data for {:?}",
                            e,
                            shared_data_key
                        )
                    }
                }
            };
        }
        (session_id, session_epoch, partition_data)
    }

    fn push_shared_data(&self, shared_data_key: String, serialized_data: Option<&Vec<u8>>) {
        let mut loo: Option<Status> = Some(Status::NotFound);
        while loo.is_some() {
            let (data, cas) = self.get_shared_data(&shared_data_key);
            if data.is_some() {
                debug!("data already present in shared data {:?}", shared_data_key);
            }
            loo = match self.set_shared_data(
                &shared_data_key,
                Some(&serialized_data.as_ref().unwrap()),
                cas,
            ) {
                Ok(_) => {
                    debug!("pushed to {:?}", shared_data_key);
                    None
                }
                Err(e) => {
                    if e == Status::CasMismatch {
                        debug!("cas error! retrying for {:?}", shared_data_key);
                        Some(e)
                    } else {
                        fatal::fatal!(
                            "error {:?} while setting shared data for {:?}",
                            e,
                            shared_data_key
                        )
                    }
                }
            };
        }
    }

    fn push_partition_records_to_shared_data(
        &self,
        topic: &str,
        partition: i32,
        fetch_offset: i64,
        data: Option<Bytes>,
    ) {
        let frd = FetchResponseData {
            session_id: self.session_id,
            session_epoch: self.session_epoch,
            topic: topic.to_string(),
            partition: partition,
            fetch_offset: fetch_offset,
            records: data.unwrap().to_vec(),
        };
        let shared_data_key = frd.get_shared_data_key();
        let encoded = bincode::serialize(&frd).expect("failed to serialize");
        let key = format!("{}-{}", topic, partition);
        if let Some(queue_id) = self.map_queue_vp.get(&key) {
            match self.enqueue_shared_queue(*queue_id, Some(&encoded)) {
                Ok(_) => {
                    debug!("enqueud {:?}, fetch offset {:?}", key, fetch_offset);
                }
                Err(e) => {
                    error!(
                        "error while enqueuing {:?}, fetch offset {:?} : {:?}",
                        key, fetch_offset, e
                    );
                }
            }
        } else {
            // shared queue not yet ready, we use shared data
            self.push_shared_data(shared_data_key, Some(&encoded));
        }
    }

    fn fetch_virtual_partitions(
        &self,
        kafka_protocol: &KafkaRoutingProtocol,
        header_version: i16,
        r: &mut FetchResponse,
    ) -> (i32, i32) {
        let mut session_id = -1;
        let mut session_epoch = -1;
        let req = self.origin_fetch_request.as_ref().unwrap();
        req.topics.iter().for_each(|req_topic| {
            r.session_id = req.session_id;
            r.error_code = 0;

            let mut ftr = FetchableTopicResponse::default();
            ftr.topic = req_topic.topic.clone();
            req_topic.partitions.iter().for_each(|req_partition| {
                let mut already_fetched = false;
                // find corresponding response, if exist.
                if let Some(resp_topic) = r
                    .responses
                    .iter()
                    .find(|resp| resp.topic == req_topic.topic)
                {
                    if let Some(resp_partition) = resp_topic
                        .partitions
                        .iter()
                        .find(|resp| resp.partition_index == req_partition.partition)
                    {
                        // already fetched, we pass
                        already_fetched = true;
                        debug!(
                            "partition data {:?}/{:?} should have been already fetched, pass.",
                            &req_topic.topic, req_partition.partition
                        );
                    }
                }
                // if not already fetched in responses, get from shared data
                if !already_fetched {
                    debug!(
                        "partition data {:?}/{:?} has not been fetched, poping from shared data",
                        &req_topic.topic, req_partition.partition
                    );

                    let part_data;
                    (session_id, session_epoch, part_data) = self.pop_shared_optimized(
                        &req_topic.topic,
                        req_partition.partition,
                        req_partition.fetch_offset,
                    );

                    let part_data = match part_data {
                        Some(v) => {
                            debug!(
                                "Data found {:?} - session {:?}({:?}): {:?}",
                                req_partition.partition, session_id, session_epoch, v
                            );

                            r.session_id = session_id;

                            v
                        }
                        None => {
                            let v = PartitionData::builder()
                                .partition_index(req_partition.partition)
                                .high_watermark(req_partition.fetch_offset)
                                .last_stable_offset(req_partition.fetch_offset)
                                .log_start_offset(0)
                                .records(None)
                                .aborted_transactions(None)
                                .build()
                                .unwrap();
                            debug!("Data not found {:?}: {:?}", req_partition.partition, v);
                            v
                        }
                    };
                    if let Some(s) = part_data.unknown_tagged_fields.get(&4) {
                        let session_id = i32::from_le_bytes(s[0..4].try_into().unwrap());
                        r.session_id = session_id;
                    }

                    ftr.partitions.push(part_data);
                }
            });
            let mut ftr2 = FetchableTopicResponse::default();
            ftr2.topic = ftr.topic.clone();
            r.responses.push(ftr);
            r.responses.push(ftr2);
        });
        (session_id, session_epoch)
    }
}

impl StreamContext for AlloyFilter {
    fn on_upstream_close(&mut self, peer_type: PeerType) {
        error!(
            "upstream connection closed {:?} {:?}",
            self.context_id, peer_type
        );
        WAITING_CONTEXTS.with(|waiters| {
            waiters.borrow_mut().remove(&self.context_id);
        })
    }

    fn on_downstream_close(&mut self, peer_type: PeerType) {
        error!(
            "downstream connection closed {:?} {:?}",
            self.context_id, peer_type
        );
        WAITING_CONTEXTS.with(|waiters| {
            waiters.borrow_mut().remove(&self.context_id);
        })
    }

    fn on_downstream_data(&mut self, data_size: usize, end_of_stream: bool) -> Action {
        if let Some(data) = self.get_downstream_data(0, data_size) {
            trace!(
                "C>S {:?}/{} Data:{} EoS:{} size:{}",
                self.configuration.type_proxy,
                self.context_id,
                String::from_utf8_lossy(&data),
                end_of_stream,
                data_size
            );
            self.routing_table.register_downstream_request(Some(data));
            let (routes, remaining) = self.routing_table.route_request(Rc::clone(&self.protocol));

            let local_cluster: &String = self.routing_table.local_cluster.as_ref().unwrap();
            let unwrapped_routes = routes.unwrap();
            let transformed_payload = unwrapped_routes.get(local_cluster);

            if remaining == 0 {
                if let Some(kafka_protocol) = self
                    .protocol
                    .borrow()
                    .as_any()
                    .downcast_ref::<KafkaRoutingProtocol>()
                {
                    let (mut header_version, mut header, req) =
                        match kafka_protocol.decode_request(transformed_payload.unwrap()) {
                            Ok(values) => values,
                            Err(e) => {
                                error!("err {:?} while decoding in filter", e);
                                return Action::Continue;
                            }
                        };
                    self.correlation_id = header.correlation_id;
                    self.request = req.clone();
                    self.request_header = Some(header.clone());
                    let mut body_version = header.request_api_version.clone();
                    trace!("Origin request: {:?}", &req);
                    match req {
                        Some(RequestKind::MetadataRequest(req)) => {
                            let payload = KafkaRoutingProtocol::encode(
                                header_version,
                                header,
                                body_version,
                                req,
                            );
                            self.routing_table.register_upstream_request(
                                0,
                                self.routing_table
                                    .local_cluster
                                    .as_ref()
                                    .unwrap()
                                    .to_string(),
                                payload.to_vec(),
                            );
                            self.set_downstream_data(0, data_size, &payload);
                        }
                        Some(RequestKind::ListOffsetsRequest(mut req)) => {
                            debug!("Listoffsets request: {:?}", req);
                            req.topics.iter_mut().for_each(|topic| {
                                if let Some(alloy_filter) =
                                    self.map_alloy_filters.get(&**topic.name)
                                {
                                    topic.partitions.iter_mut().for_each(|t| {
                                        t.partition_index = self
                                            .bimap_virtual_origin
                                            .get(&**topic.name)
                                            .unwrap()
                                            .get_original_by_virtual(t.partition_index)
                                            .unwrap()
                                            .clone();
                                    });
                                }
                            });
                            trace!("payload LO: header_version: {:?} header: {:?} body_version: {:?} body: {:?}", header_version, header, body_version,req);
                            let payload = KafkaRoutingProtocol::encode(
                                header_version,
                                header,
                                body_version,
                                req,
                            );
                            self.routing_table.register_upstream_request(
                                0,
                                self.routing_table
                                    .local_cluster
                                    .as_ref()
                                    .unwrap()
                                    .to_string(),
                                payload.to_vec(),
                            );
                            self.set_downstream_data(0, data_size, &payload);
                        }
                        Some(RequestKind::FetchRequest(mut req)) => {
                            debug!("Fetch request: {:?}", req);
                            self.fetch_request_api_version = header.request_api_version;

                            //let mut full_request;
                            if req.topics.len() > 0 {
                                // get origin request information
                                self.origin_fetch_request = Some(req.clone());
                                self.session_id = req.session_id;
                            } else {
                                self.session_id = req.session_id;
                                self.session_epoch = req.session_epoch;
                            }

                            // Get first topic & partition before replacing/removing partitions
                            let first_topic_name: TopicName;
                            let first_partition;

                            if req.topics.len() > 0 {
                                first_topic_name = req.topics.first().unwrap().topic.clone();
                                let t = req.topics.first().unwrap();
                                first_partition = t.partitions.first().unwrap().partition;
                            } else {
                                first_topic_name = self
                                    .origin_fetch_request
                                    .as_ref()
                                    .unwrap()
                                    .topics
                                    .first()
                                    .unwrap()
                                    .topic
                                    .clone();
                                first_partition = self
                                    .origin_fetch_request
                                    .as_ref()
                                    .unwrap()
                                    .topics
                                    .first()
                                    .unwrap()
                                    .partitions
                                    .first()
                                    .unwrap()
                                    .partition;
                            }

                            self.fetch_request_api_version = header.request_api_version;
                            let mut fetch = false;
                            let mut request_data_vp = FetchRequestData {
                                context_id: self.context_id,
                                correlation_id: self.correlation_id,
                                session_id: req.session_id,
                                session_epoch: req.session_epoch,
                                body_version: body_version,
                                partitions: Vec::new(),
                            };

                            for topic in req.topics.iter_mut() {
                                // if sessioned request, it does not iterate here, no topic
                                for part in topic.partitions.iter_mut() {
                                    let origin_partition = self
                                        .bimap_virtual_origin
                                        .get(&topic.topic.to_string())
                                        .unwrap()
                                        .get_original_by_virtual(part.partition)
                                        .unwrap()
                                        .clone();

                                    // check if exists, if at least one exists we need a Fetch
                                    //FIXME: this is not origin partition to consider here but minimal one (think of 2px3s, there is no vp0!)
                                    if origin_partition == part.partition {
                                        // Origin partition
                                        // we send the request on the original partition (as virtual probably don't exist on Kafka)

                                        // we register every shared queue from vp that we can
                                        part.partition = self
                                            .bimap_virtual_origin
                                            .get(&**topic.topic)
                                            .unwrap()
                                            .get_original_by_virtual(part.partition)
                                            .unwrap()
                                            .clone();
                                        for p in self
                                            .bimap_virtual_origin
                                            .get(&**topic.topic)
                                            .unwrap()
                                            .get_virtuals_by_original(part.partition)
                                            .unwrap()
                                        {
                                            let key =
                                                format!("{}-{:?}", topic.topic.to_string(), p);
                                            if let Some(_queue_id) = self.map_queue_vp.get(&key) {
                                                // already there
                                            } else {
                                                let queue_id =
                                                    self.resolve_shared_queue("alloy", &key);
                                                if let Some(queue_id) = queue_id {
                                                    debug!(
                                                        "inserting shared queue {:?} - {:?}",
                                                        key, queue_id
                                                    );
                                                    self.map_queue_vp.insert(key, queue_id);
                                                } else {
                                                    debug!(
                                                        "shared queue {:?} not yet initialized",
                                                        key
                                                    );
                                                }
                                            }
                                        }
                                    } else {
                                        // Virtual partition
                                        // set partition to -1 to remove them (already there)
                                        let frp = FetchRequestDataPartition {
                                            topic: topic.topic.to_string(),
                                            partition: part.partition,
                                            fetch_offset: part.fetch_offset,
                                        };
                                        request_data_vp.partitions.push(frp);
                                        part.partition = -1;
                                    }
                                }
                                topic.partitions.retain(|x| x.partition != -1);
                            }
                            debug!("request ");
                            if request_data_vp.partitions.len() == 0 {
                                let payload = KafkaRoutingProtocol::encode(
                                    header_version,
                                    header,
                                    body_version,
                                    req,
                                );
                                self.routing_table.register_upstream_request(
                                    0,
                                    self.routing_table
                                        .local_cluster
                                        .as_ref()
                                        .unwrap()
                                        .to_string(),
                                    payload.to_vec(),
                                );
                                self.set_downstream_data(0, data_size, &payload);
                                self.routing_table
                                    .register_downstream_request(Some(payload));
                            } else {
                                // ListOffsets
                                // if some calls to virtual partitions (but no real!), prepare some lo
                                let mut lo = ListOffsetsRequest::builder()
                                    .replica_id(req.replica_id)
                                    .isolation_level(req.isolation_level)
                                    .topics(Vec::new())
                                    .build()
                                    .unwrap();
                                let mut lot = ListOffsetsTopic::builder()
                                    .name(first_topic_name.clone())
                                    .partitions(Vec::new())
                                    .build()
                                    .unwrap();
                                //FIXME: only first topic considered here, should be ok but think this through
                                let lop = ListOffsetsPartition::builder()
                                    .partition_index(
                                        self.bimap_virtual_origin
                                            .get(&first_topic_name.to_string())
                                            .unwrap()
                                            .get_original_by_virtual(first_partition)
                                            .unwrap()
                                            .clone(),
                                    )
                                    .build()
                                    .unwrap();
                                lot.partitions.push(lop);

                                lo.topics.push(lot);
                                header_version = 2;
                                header.request_api_key = 2;
                                header.request_api_version = 7;
                                body_version = 7;
                                trace!("payload F->LO: header_version: {:?} header: {:?} body_version: {:?} body: {:?}", header_version, header, body_version,lo);
                                let payload = KafkaRoutingProtocol::encode(
                                    header_version,
                                    header,
                                    body_version,
                                    lo,
                                );

                                self.routing_table.register_upstream_request(
                                    0,
                                    self.routing_table
                                        .local_cluster
                                        .as_ref()
                                        .unwrap()
                                        .to_string(),
                                    payload.to_vec(),
                                );
                                self.set_downstream_data(0, data_size, &payload);
                                self.routing_table
                                    .register_downstream_request(Some(payload));

                                let current_time = hostcalls::get_current_time().unwrap();
                                debug!(
                                    "Context id : {:?} System Time {:?}",
                                    self.context_id, current_time
                                );
                                let body_version =
                                    self.request_header.as_ref().unwrap().request_api_version;
                                //TODO: only if partition defined, no need for this complexity in other

                                trace!("sending to queue request {:?}", &request_data_vp);

                                WAITING_CONTEXTS.with(|waiters| {
                                    let connection_info: ConnectionInfo =
                                        match waiters.borrow_mut().remove(&self.context_id) {
                                            Some(mut v) => {
                                                debug!("recycle previous connection_info");
                                                v.timeout = false;
                                                v.response_initial_time = current_time;
                                                v
                                            }
                                            None => ConnectionInfo {
                                                response_initial_time: current_time,
                                                timeout: false,
                                                request_data: request_data_vp,
                                                response_data: None,
                                            },
                                        };
                                    debug!(
                                        "insert connection info for context {:?}",
                                        self.context_id
                                    );
                                    waiters
                                        .borrow_mut()
                                        .insert(self.context_id, connection_info);
                                });
                                return Action::Pause;
                            }
                        }
                        _ => {
                            // nothing
                            self.routing_table.register_upstream_request(
                                0,
                                self.routing_table
                                    .local_cluster
                                    .as_ref()
                                    .unwrap()
                                    .to_string(),
                                transformed_payload.unwrap().to_vec(),
                            );
                            self.set_downstream_data(0, data_size, &transformed_payload.unwrap());
                        }
                    };
                } else {
                    self.routing_table.register_upstream_request(
                        0,
                        self.routing_table
                            .local_cluster
                            .as_ref()
                            .unwrap()
                            .to_string(),
                        transformed_payload.unwrap().to_vec(),
                    );
                    self.set_downstream_data(0, data_size, &transformed_payload.unwrap());
                }
            }
        }
        Action::Continue
    }

    fn on_upstream_data(&mut self, data_size: usize, end_of_stream: bool) -> Action {
        if let Some(data) = self.get_upstream_data(0, data_size) {
            trace!(
                "C<S {:?}/{} Data:{:?} EoS:{} counter:{}",
                self.configuration.type_proxy,
                self.context_id,
                data,
                end_of_stream,
                self.routing_table.get_responses().len()
            );

            let length = &data.len();
            let (transformed_payload, remaining) =
                self.routing_table
                    .route_response(Rc::clone(&self.protocol), 0, data);
            debug!(
                "route response: received {:?} packet, remaining {:?}",
                length, remaining
            );
            let transformed_payload = match transformed_payload {
                Ok(r) => Some(r),
                Err("incomplete") => {
                    debug!("can't answer for now, waiting for next");
                    self.set_upstream_data(0, data_size, &[]); // can't answer for now.
                    return Action::Continue;
                }
                e => {
                    error!("Error {:?}", e);
                    return Action::Continue;
                }
            };

            self.data_size = data_size;
            if remaining == 0 {
                // Downcasting to call some_fn
                if let Some(kafka_protocol) = self
                    .protocol
                    .borrow()
                    .as_any()
                    .downcast_ref::<KafkaRoutingProtocol>()
                {
                    let (header_version, header_req, header, resp, length) = match kafka_protocol
                        .decode_response(
                            self.routing_table.origin_request.as_ref().unwrap(),
                            transformed_payload.as_ref().unwrap(),
                        ) {
                        Ok(values) => values,
                        Err(e) => {
                            error!("unable to read response {:?}", e);
                            return Action::Continue;
                        }
                    };
                    let _body_version = header_req.request_api_version.clone();
                    debug!(
                        "full record: expected len {:?} (+4) for length payload {:?}",
                        length,
                        transformed_payload.as_ref().unwrap().len()
                    );
                    trace!("response to {:?}", self.request);
                    match resp {
                        Some(ResponseKind::MetadataResponse(mut resp)) => {
                            resp.topics.iter_mut().for_each(|(topic, mrt)| {
                                let part = mrt.partitions.clone();

                                for mrp in part.iter() {
                                    let mut mrp_virtual = mrp.clone();
                                    let partition_origin = mrp.partition_index;
                                    let vp_list = self
                                        .bimap_virtual_origin
                                        .get(&***topic)
                                        .unwrap()
                                        .get_virtuals_by_original(partition_origin)
                                        .unwrap();
                                    // for each corresponding vp for return p
                                    for vp in vp_list {
                                        debug!(
                                            "return vp partition {:?}(origin partition: {:?})",
                                            vp, partition_origin
                                        );
                                        mrp_virtual.partition_index = vp.clone();
                                        mrt.partitions.push(mrp_virtual);
                                        mrp_virtual = mrp.clone();
                                    }
                                }
                            });

                            let body_version =
                                self.request_header.as_ref().unwrap().request_api_version;
                            trace!("payload M: header_version: {:?} header: {:?} body_version: {:?} body: {:?}", header_version, header, body_version, resp);

                            let payload = KafkaRoutingProtocol::encode(
                                header_version,
                                header,
                                body_version,
                                resp,
                            );
                            self.set_upstream_data(0, data_size, &payload);

                            return Action::Continue;
                        }
                        Some(ResponseKind::ListOffsetsResponse(mut resp)) => {
                            match self.request.take() {
                                None => {
                                    // regular heartbeat, let it be
                                    error!("no origin request");
                                }
                                Some(RequestKind::ListOffsetsRequest(mut req)) => {
                                    req.topics.iter_mut().for_each(|req_topic|{
                                            let alloy_filter = self.map_alloy_filters.get(&**req_topic.name).unwrap();
                                            let mut resp_topic = resp.topics.iter_mut().find(|t| t.name == req_topic.name).unwrap();
                                            let mut vec_parts: Vec<ListOffsetsPartitionResponse> = Vec::new();
                                            req_topic.partitions.iter().for_each(|req_partition: &ListOffsetsPartition| {
                                                let partition = req_partition.partition_index;
                                                let origin_partition: i32 = self.bimap_virtual_origin.get(&**req_topic.name).unwrap().get_original_by_virtual(partition).unwrap().clone();
                                                debug!("Trying to find origin_partition {:?} for partition {:?} in responses {:?}", origin_partition, partition, resp_topic.partitions.iter().map(|r| r.partition_index).collect::<Vec<_>>());
                                                let mut resp_partition = resp_topic.partitions.iter().find(|p| p.partition_index == origin_partition).unwrap().clone();
                                                resp_partition.partition_index = req_partition.partition_index;
                                                vec_parts.push(resp_partition);
                                            });
                                            resp_topic.partitions = vec_parts;

                                            // initiate ALL tp for this topic,
                                            for partition in 0..alloy_filter.num_partitions * alloy_filter.num_sources {
                                                self.map_split_id.insert((resp_topic.name.to_string(), partition), KafkaRoutingProtocol::get_split_owner(&req_topic.name.to_string(), partition,alloy_filter.num_sources));
                                                self.map_index_query_partition.insert((req_topic.name.to_string(), partition), 0);
                                            }
                                            debug!("Init split: {:?}", self.map_split_id);
                                            debug!("Init map_index_query_partition: {:?}", self.map_index_query_partition);
                                        });

                                    let body_version =
                                        self.request_header.as_ref().unwrap().request_api_version;

                                    let payload = KafkaRoutingProtocol::encode(
                                        header_version,
                                        header,
                                        body_version,
                                        resp,
                                    );
                                    //data_size???
                                    self.set_upstream_data(0, payload.len(), &payload);
                                }
                                Some(RequestKind::FetchRequest(req)) => {
                                    debug!(
                                        "response to fetch {:?} to vp {:?}",
                                        self.context_id, req
                                    );

                                    let mut no_response = true;
                                    WAITING_CONTEXTS.with(|waiters| {
                                        match waiters.borrow_mut().remove(&self.context_id) {
                                            Some(removed) => {
                                                if let Some(payload) = removed.response_data {
                                                    debug!("found response to send!");
                                                    self.set_upstream_data(0, data_size, &payload);
                                                    no_response = false;
                                                } else {
                                                    debug!("timeout...");
                                                    no_response = true;
                                                }
                                            }
                                            None => {
                                                error!(
                                                    "unkown context {:?} for response",
                                                    self.context_id
                                                );
                                                no_response = true;
                                            }
                                        }
                                    });

                                    if no_response {
                                        debug!("sending an empty fetch response since no data could be get");
                                        let mut r = FetchResponse::default();
                                        r.error_code = 0;
                                        r.session_id = req.session_id;

                                        for topic in req.topics {
                                            let mut ftr = FetchableTopicResponse::builder()
                                                .topic(topic.topic)
                                                .build()
                                                .unwrap();

                                            for partition in topic.partitions {
                                                let fp = PartitionData::builder()
                                                    .partition_index(partition.partition)
                                                    .build()
                                                    .unwrap();
                                                ftr.partitions.push(fp)
                                            }
                                            r.responses.push(ftr);
                                        }

                                        r.session_id = 0;
                                        trace!("Response virtual: {:?}", r);

                                        let body_version = self
                                            .request_header
                                            .as_ref()
                                            .unwrap()
                                            .request_api_version;
                                        let correlation_id =
                                            self.request_header.as_ref().unwrap().correlation_id;

                                        let header = ResponseHeader::builder()
                                            .correlation_id(correlation_id)
                                            .build()
                                            .unwrap();
                                        trace!("payload response F(virtual) : header_version: {:?} header: {:?} body_version: {:?} body: {:?}", header_version, header, body_version,r);
                                        //TODO: check header_version : 1 for F>=12 (Flink 1.16)
                                        let payload = KafkaRoutingProtocol::encode(
                                            1,
                                            header,
                                            body_version,
                                            r,
                                        );
                                        trace!("payload response F(virtual): {:?}", payload);

                                        self.set_upstream_data(0, data_size, &payload);
                                    }
                                    return Action::Continue;
                                }
                                req => {
                                    warn!("req not managed: {:?}", req);
                                }
                            }
                        }
                        Some(ResponseKind::FetchResponse(mut resp)) => {
                            trace!("FetchResponse: {:?}", resp);
                            match self.request.take() {
                                Some(RequestKind::FetchRequest(req)) => {
                                    {
                                        if resp.session_id > 0 {
                                            debug!(
                                                "origin : setting session_id to {:?}",
                                                resp.session_id
                                            );
                                            self.session_id = resp.session_id; // new session we keep it
                                        } else {
                                            resp.session_id = self.session_id; // session not defined we set the stored one (or zero)
                                        }
                                        let mut map_vp_records = self.map_vp_records.borrow_mut();
                                        self.process_records(&req, &mut resp, &mut *map_vp_records);
                                    }

                                    /*
                                    if resp.session_id > 0 {
                                        debug!("origin : setting session_id to {:?}", resp.session_id);
                                        self.session_id = resp.session_id; // new session we keep it
                                    } else {
                                        resp.session_id = self.session_id; // session not defined we set the stored one (or zero)
                                    }
                                    resp.responses.iter_mut().for_each(|resp_topic| {
                                        debug!("resp_topic : {:?} resp_partitions", resp_topic.partitions.len());
                                        resp_topic.partitions.iter_mut().for_each(|resp_partition|{
                                            debug!("resp_partition {:?} {:?}", resp_partition.partition_index, resp_partition.records.as_ref().unwrap().len());
                                        });
                                        resp_topic.partitions.iter_mut().for_each(|resp_partition|{
                                            // find corresponding topic request. It must exist so unwrap.
                                            if let Some(req_topic) = req.topics.iter().find(|t| t.topic == resp_topic.topic) {
                                                //TODO: may optimize this part checking bimap every time
                                                let req_partition = req_topic.partitions.iter().find(|p| self.bimap_virtual_origin.get(&**req_topic.topic).unwrap().get_original_by_virtual(p.partition).unwrap() == &resp_partition.partition_index).unwrap();
                                                let req_offset = req_partition.fetch_offset;
                                                if resp_partition.records.is_none() {
                                                    debug!("records is none {:?}", resp_partition);
                                                }
                                                debug!("resp_partition data size : {:?}", resp_partition.records.as_ref().unwrap().len());
                                                self.filter_records_optimized(&resp_topic.topic, req_partition.partition, resp_partition, req_offset);
                                                // TODO: put back the original target partition index instead of the original
                                                resp_partition.partition_index = req_partition.partition;
                                                //TODO: see if necessary to trace fetch_offsets
                                                //AlloyFilter::set_map_index_query_partition(&mut self.map_index_query_partition, (req_topic.topic.to_string(), req_partition.partition) , req_offset);

                                            }
                                        });
                                    });

                                     */
                                    //if !self.partition_criteria.is_empty() {
                                    self.fetch_virtual_partitions(
                                        kafka_protocol,
                                        header_version,
                                        &mut resp,
                                    );
                                    debug!("Response fetch origin: {:?}", resp);

                                    self.session_id = resp.session_id;
                                    /*} else {
                                        //FIXME: deserialize resp in payload
                                        let body_version: i16 = self.request_header.as_ref().unwrap().request_api_version;
                                        payload = kafka_protocol.encode(header_version, header, body_version, resp);
                                    }*/
                                    let body_version =
                                        self.request_header.as_ref().unwrap().request_api_version;
                                    let correlation_id =
                                        self.request_header.as_ref().unwrap().correlation_id;

                                    let header = ResponseHeader::builder()
                                        .correlation_id(correlation_id)
                                        .build()
                                        .unwrap();
                                    trace!("payload response F(origin): header_version: {:?} header: {:?} body_version: {:?} body: {:?}", header_version, header, body_version, resp);
                                    //TODO: check header_version : 1 for F>=12 (Flink 1.16)
                                    let payload =
                                        KafkaRoutingProtocol::encode(1, header, body_version, resp);

                                    trace!("payload response F(origin): {:?}", payload);
                                    self.set_upstream_data(0, data_size, &payload);
                                }
                                req => {
                                    warn!("should not happen for fetchresponse: {:?}", req);
                                }
                            }
                        }
                        _ => {
                            // nothing.
                        }
                    }
                } else {
                    println!("Not a KafkaRoutingProtocol");
                }
            } else {
                warn!("C<S packet fragmented/merged remaining = {:?}", remaining);
            }
        }
        Action::Continue
    }
}

mod tests {
    use std::collections::HashSet;

    use bytes::BytesMut;
    use kafka_alloy::KafkaRoutingProtocol;
    use kafka_protocol::{
        messages::fetch_response::PartitionData, protocol::Encodable, records::RecordBatchEncoder,
    };
    use serde_json::json;

    use super::*;

    #[test]
    fn test_empty_array() {
        let mut buf: BytesMut = BytesMut::new();
        match RecordBatchEncoder::encode(
            &mut buf,
            Vec::new().iter(),
            &KafkaRoutingProtocol::get_encoding_options(),
        ) {
            Ok(v) => v,
            Err(e) => error!("error encoding records: {:?}", e),
        };
        println!("{:?}", buf.to_vec());
    }

    #[test]
    fn test_deser() {
        let mut p = PartitionData::default();
        p.unknown_tagged_fields.insert(4, [0, 5, 10, 20].to_vec());
        let mut buf = BytesMut::new();
        p.encode(&mut buf, 12);
        let data = buf.to_vec();
        println!("ser: {:?}", data);

        let mut buf = Cursor::new(data);
        let partition_data = PartitionData::decode(&mut buf, 12);
        println!("deser: {:?}", partition_data);
    }

    #[test]
    fn test_serde_complex_kafka_payload() {
        let little = "0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 7, 210, 0, 0, 0, 0, 0, 0, 7, 210, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 69, 0, 0, 0, 0, 0, 0, 6, 36, 0, 0, 0, 56, 0, 0, 0, 0, 2, 172, 84, 134, 21, 0, 0, 0, 0, 0, 0, 0, 0, 1, 147, 198, 254, 149, 78, 0, 0, 1, 147, 198, 254, 149, 78, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 3, 4, 0, 0, 0, 1, 12, 0, 0, 0, 1, 1, 0, 1";
        let big = "0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 2, 86, 0, 0, 0, 0, 0, 0, 2, 86, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 214, 102, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 120, 0, 0, 0, 0, 2, 8, 219, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0, 1, 147, 198, 254, 106, 47, 0, 0, 1, 147, 198, 254, 106, 47, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 138, 1, 0, 0, 0, 8, 49, 48, 50, 48, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 117, 108, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 4, 156, 0, 0, 0, 0, 2, 168, 82, 249, 131, 0, 0, 0, 0, 0, 16, 0, 0, 1, 147, 198, 254, 106, 54, 0, 0, 1, 147, 198, 254, 106, 88, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 138, 1, 0, 0, 0, 8, 49, 48, 50, 48, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 117, 108, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 138, 1, 0, 54, 4, 8, 49, 48, 50, 48, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 75, 97, 116, 101, 32, 74, 111, 110, 101, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 144, 1, 0, 54, 6, 8, 49, 48, 50, 48, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 80, 104, 111, 101, 110, 105, 120, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 75, 97, 116, 101, 32, 65, 98, 114, 97, 109, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 146, 1, 0, 56, 8, 8, 49, 48, 50, 48, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 80, 111, 114, 116, 108, 97, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 152, 1, 0, 56, 10, 8, 49, 48, 50, 48, 130, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 80, 111, 114, 116, 108, 97, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 114, 97, 104, 32, 83, 112, 101, 110, 99, 101, 114, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 154, 1, 0, 58, 12, 8, 49, 48, 50, 48, 132, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 76, 111, 115, 32, 65, 110, 103, 101, 108, 101, 115, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 76, 117, 107, 101, 32, 87, 97, 108, 116, 111, 110, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 150, 1, 0, 58, 14, 8, 49, 48, 50, 48, 128, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 80, 104, 111, 101, 110, 105, 120, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 142, 1, 0, 60, 16, 8, 49, 48, 50, 48, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 80, 104, 111, 101, 110, 105, 120, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 117, 108, 32, 74, 111, 110, 101, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 156, 1, 0, 60, 18, 8, 49, 48, 50, 48, 134, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 97, 110, 32, 70, 114, 97, 110, 99, 105, 115, 99, 111, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 76, 117, 107, 101, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 158, 1, 0, 62, 22, 8, 49, 48, 50, 48, 136, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 76, 111, 115, 32, 65, 110, 103, 101, 108, 101, 115, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 117, 108, 105, 101, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 144, 1, 0, 62, 24, 8, 49, 48, 50, 48, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 67, 104, 101, 121, 101, 110, 110, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 97, 117, 108, 32, 74, 111, 110, 101, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 146, 1, 0, 64, 26, 8, 49, 48, 50, 48, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 82, 101, 100, 109, 111, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 68, 101, 105, 116, 101, 114, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 144, 1, 0, 64, 28, 8, 49, 48, 50, 48, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 67, 104, 101, 121, 101, 110, 110, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 111, 104, 110, 32, 78, 111, 114, 105, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 152, 1, 0, 66, 30, 8, 49, 48, 50, 48, 130, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 76, 111, 115, 32, 65, 110, 103, 101, 108, 101, 115, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 111, 104, 110, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 138, 1, 0, 68, 32, 8, 49, 48, 50, 48, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 101, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 114, 97, 104, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 0, 0, 0, 0, 0, 0, 0, 19, 0, 0, 4, 85, 0, 0, 0, 0, 2, 62, 21, 241, 13, 0, 0, 0, 0, 0, 14, 0, 0, 1, 147, 198, 254, 106, 66, 0, 0, 1, 147, 198, 254, 106, 73, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 2, 0, 0, 0, 14, 138, 1, 0, 0, 0, 8, 49, 48, 50, 48, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 75, 97, 116, 101, 32, 74, 111, 110, 101, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 144, 1, 0, 2, 2, 8, 49, 48, 50, 48, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 80, 104, 111, 101, 110, 105, 120, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 75, 97, 116, 101, 32, 65, 98, 114, 97, 109, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 146, 1, 0, 4, 4, 8, 49, 48, 50, 48, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 80, 111, 114, 116, 108, 97, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 152, 1, 0, 4, 6, 8, 49, 48, 50, 48, 130, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 80, 111, 114, 116, 108, 97, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 114, 97, 104, 32, 83, 112, 101, 110, 99, 101, 114, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 154, 1, 0, 4, 8, 8, 49, 48, 50, 48, 132, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 76, 111, 115, 32, 65, 110, 103, 101, 108, 101, 115, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 76, 117, 107, 101, 32, 87, 97, 108, 116, 111, 110, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 150, 1, 0, 6, 10, 8, 49, 48, 50, 48, 128, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 80, 104, 111, 101, 110, 105, 120, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 142, 1, 0, 6, 12, 8, 49, 48, 50, 48, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 80, 104, 111, 101, 110, 105, 120, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 117, 108, 32, 74, 111, 110, 101, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 156, 1, 0, 8, 14, 8, 49, 48, 50, 48, 134, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 97, 110, 32, 70, 114, 97, 110, 99, 105, 115, 99, 111, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 76, 117, 107, 101, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 158, 1, 0, 10, 18, 8, 49, 48, 50, 48, 136, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 76, 111, 115, 32, 65, 110, 103, 101, 108, 101, 115, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 117, 108, 105, 101, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 144, 1, 0, 10, 20, 8, 49, 48, 50, 48, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 67, 104, 101, 121, 101, 110, 110, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 97, 117, 108, 32, 74, 111, 110, 101, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 146, 1, 0, 10, 22, 8, 49, 48, 50, 48, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 82, 101, 100, 109, 111, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 68, 101, 105, 116, 101, 114, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 144, 1, 0, 12, 24, 8, 49, 48, 50, 48, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 67, 104, 101, 121, 101, 110, 110, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 111, 104, 110, 32, 78, 111, 114, 105, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 152, 1, 0, 12, 26, 8, 49, 48, 50, 48, 130, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 76, 111, 115, 32, 65, 110, 103, 101, 108, 101, 115, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 111, 104, 110, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 138, 1, 0, 14, 28, 8, 49, 48, 50, 48, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 101, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 114, 97, 104, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 0, 0, 0, 0, 0, 0, 0, 38, 0, 0, 0, 122, 0, 0, 0, 0, 2, 64, 216, 187, 81, 0, 0, 0, 0, 0, 0, 0, 0, 1, 147, 198, 254, 106, 89, 0, 0, 1, 147, 198, 254, 106, 89, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 19, 0, 0, 0, 1, 142, 1, 0, 0, 0, 8, 49, 48, 50, 48, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 65, 98, 114, 97, 109, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 0, 0, 0, 0, 0, 0, 0, 39, 0, 0, 0, 192, 0, 0, 0, 0, 2, 94, 70, 206, 1, 0, 0, 0, 0, 0, 1, 0, 0, 1, 147, 198, 254, 106, 74, 0, 0, 1, 147, 198, 254, 106, 74, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 19, 0, 0, 0, 2, 142, 1, 0, 0, 0, 8, 49, 48, 50, 48, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 65, 98, 114, 97, 109, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 136, 1, 0, 0, 2, 8, 49, 48, 50, 48, 116, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 117, 108, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 0, 0, 0, 0, 0, 0, 0, 41, 0, 0, 0, 119, 0, 0, 0, 0, 2, 228, 1, 64, 189, 0, 0, 0, 0, 0, 0, 0, 0, 1, 147, 198, 254, 106, 89, 0, 0, 1, 147, 198, 254, 106, 89, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 20, 0, 0, 0, 1, 136, 1, 0, 0, 0, 8, 49, 48, 50, 48, 116, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 117, 108, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0, 121, 0, 0, 0, 0, 2, 128, 197, 112, 217, 0, 0, 0, 0, 0, 0, 0, 0, 1, 147, 198, 254, 106, 74, 0, 0, 1, 147, 198, 254, 106, 74, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 21, 0, 0, 0, 1, 140, 1, 0, 0, 0, 8, 49, 48, 50, 48, 120, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 97, 117, 108, 32, 83, 112, 101, 110, 99, 101, 114, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 0, 0, 0, 0, 0, 0, 0, 43, 0, 0, 3, 251, 0, 0, 0, 0, 2, 250, 78, 190, 119, 0, 0, 0, 0, 0, 26, 0, 0, 1, 147, 198, 254, 106, 90, 0, 0, 1, 147, 198, 254, 106, 99, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 21, 0, 0, 0, 13, 140, 1, 0, 0, 0, 8, 49, 48, 50, 48, 120, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 97, 117, 108, 32, 83, 112, 101, 110, 99, 101, 114, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 144, 1, 0, 4, 12, 8, 49, 48, 50, 48, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 101, 97, 116, 116, 108, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 76, 117, 107, 101, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 138, 1, 0, 6, 14, 8, 49, 48, 50, 48, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 101, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 75, 97, 116, 101, 32, 87, 97, 108, 116, 111, 110, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 158, 1, 0, 6, 18, 8, 49, 48, 50, 48, 136, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 76, 111, 115, 32, 65, 110, 103, 101, 108, 101, 115, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 114, 97, 104, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 142, 1, 0, 8, 20, 8, 49, 48, 50, 48, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 101, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 68, 101, 105, 116, 101, 114, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 156, 1, 0, 10, 24, 8, 49, 48, 50, 48, 134, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 76, 111, 115, 32, 65, 110, 103, 101, 108, 101, 115, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 65, 98, 114, 97, 109, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 156, 1, 0, 10, 28, 8, 49, 48, 50, 48, 134, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 76, 111, 115, 32, 65, 110, 103, 101, 108, 101, 115, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 86, 105, 99, 107, 121, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 144, 1, 0, 12, 32, 8, 49, 48, 50, 48, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 82, 101, 100, 109, 111, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 117, 108, 105, 101, 32, 78, 111, 114, 105, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 140, 1, 0, 14, 40, 8, 49, 48, 50, 48, 120, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 97, 117, 108, 32, 65, 98, 114, 97, 109, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 144, 1, 0, 16, 42, 8, 49, 48, 50, 48, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 82, 101, 100, 109, 111, 110, 100, 34, 44, 34, 105, 10test_envoy1.1.niwx6cv9wii5@ecotype-26.nantes.grid5000.fr    | 0, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 86, 105, 99, 107, 121, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 146, 1, 0, 16, 46, 8, 49, 48, 50, 48, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 82, 101, 100, 109, 111, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 87, 97, 108, 116, 111, 110, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 136, 1, 0, 18, 50, 8, 49, 48, 50, 48, 116, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 76, 117, 107, 101, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 144, 1, 0, 18, 52, 8, 49, 48, 50, 48, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 82, 101, 100, 109, 111, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 76, 117, 107, 101, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 0, 0, 0, 0, 0, 0, 0, 76, 0, 0, 3, 179, 0, 0, 0, 0, 2, 26, 45, 94, 24, 0, 0, 0, 0, 0, 20, 0, 0, 1, 147, 198, 254, 106, 76, 0, 0, 1, 147, 198, 254, 106, 81, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 27, 0, 0, 0, 12, 144, 1, 0, 0, 0, 8, 49, 48, 50, 48, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 101, 97, 116, 116, 108, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 76, 117, 107, 101, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 138, 1, 0, 0, 2, 8, 49, 48, 50, 48, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 101, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 75, 97, 116, 101, 32, 87, 97, 108, 116, 111, 110, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 158, 1, 0, 2, 6, 8, 49, 48, 50, 48, 136, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 76, 111, 115, 32, 65, 110, 103, 101, 108, 101, 115, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 114, 97, 104, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 142, 1, 0, 2, 8, 8, 49, 48, 50, 48, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 101, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 68, 101, 105, 116, 101, 114, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 156, 1, 0, 4, 12, 8, 49, 48, 50, 48, 134, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 76, 111, 115, 32, 65, 110, 103, 101, 108, 101, 115, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 65, 98, 114, 97, 109, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 156, 1, 0, 4, 16, 8, 49, 48, 50, 48, 134, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 76, 111, 115, 32, 65, 110, 103, 101, 108, 101, 115, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 86, 105, 99, 107, 121, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 144, 1, 0, 6, 20, 8, 49, 48, 50, 48, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 82, 101, 100, 109, 111, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 117, 108, 105, 101, 32, 78, 111, 114, 105, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 140, 1, 0, 8, 28, 8, 49, 48, 50, 48, 120, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 97, 117, 108, 32, 65, 98, 114, 97, 109, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 144, 1, 0, 8, 30, 8, 49, 48, 50, 48, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 82, 101, 100, 109, 111, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 86, 105, 99, 107, 121, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 146, 1, 0, 10, 34, 8, 49, 48, 50, 48, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 82, 101, 100, 109, 111, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 87, 97, 108, 116, 111, 110, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 136, 1, 0, 10, 38, 8, 49, 48, 50, 48, 116, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 76, 117, 107, 101, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 144, 1, 0, 10, 40, 8, 49, 48, 50, 48, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 82, 101, 100, 109, 111, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 48, 44, 34, 110, 97, 109, 101, 34, 58, 34, 76, 117, 107, 101, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 8, 107, 0, 0, 0, 0, 2, 44, 23, 214, 50, 0, 0, 0, 0, 0, 47, 0, 0, 1, 147, 198, 254, 118, 7, 0, 0, 1, 147, 198, 254, 118, 15, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 50, 0, 0, 0, 28, 144, 1, 0, 0, 0, 8, 49, 48, 50, 52, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 117, 108, 105, 101, 32, 83, 112, 101, 110, 99, 101, 114, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 144, 1, 0, 2, 10, 8, 49, 48, 50, 52, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 82, 101, 100, 109, 111, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 74, 111, 110, 101, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 144, 1, 0, 4, 18, 8, 49, 48, 50, 52, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 80, 104, 111, 101, 110, 105, 120, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 117, 108, 105, 101, 32, 74, 111, 110, 101, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 140, 1, 0, 4, 20, 8, 49, 48, 50, 52, 120, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 97, 117, 108, 32, 65, 98, 114, 97, 109, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 160, 1, 0, 4, 24, 8, 49, 48, 50, 52, 138, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 97, 110, 32, 70, 114, 97, 110, 99, 105, 115, 99, 111, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 75, 97, 116, 101, 32, 83, 112, 101, 110, 99, 101, 114, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 156, 1, 0, 4, 26, 8, 49, 48, 50, 52, 134, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 97, 110, 32, 70, 114, 97, 110, 99, 105, 115, 99, 111, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 117, 108, 32, 74, 111, 110, 101, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 142, 1, 0, 6, 28, 8, 49, 48, 50, 52, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 114, 97, 104, 32, 65, 98, 114, 97, 109, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 158, 1, 0, 6, 30, 8, 49, 48, 50, 52, 136, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 97, 110, 32, 70, 114, 97, 110, 99, 105, 115, 99, 111, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 146, 1, 0, 6, 36, 8, 49, 48, 50, 52, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 101, 97, 116, 116, 108, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 117, 108, 105, 101, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 138, 1, 0, 6, 38, 8, 49, 48, 50, 52, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 101, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 114, 97, 104, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 142, 1, 0, 8, 46, 8, 49, 48, 50, 52, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 82, 101, 100, 109, 111, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 117, 108, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 138, 1, 0, 8, 48, 8, 49, 48, 50, 52, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 101, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 114, 97, 104, 32, 78, 111, 114, 105, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 150, 1, 0, 8, 50, 8, 49, 48, 50, 52, 128, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 67, 104, 101, 121, 101, 110, 110, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 75, 97, 116, 101, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 162, 1, 0, 8, 52, 8, 49, 48, 50, 52, 140, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 97, 110, 32, 70, 114, 97, 110, 99, 105, 115, 99, 111, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 87, 97, 108, 116, 101, 114, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 138, 1, 0, 10, 54, 8, 49, 48, 50, 52, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 140, 1, 0, 10, 60, 8, 49, 48, 50, 52, 120, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 117, 108, 105, 101, 32, 74, 111, 110, 101, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 158, 1, 0, 10, 62, 8, 49, 48, 50, 52, 136, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 97, 110, 32, 70, 114, 97, 110, 99, 105, 115, 99, 111, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 86, 105, 99, 107, 121, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 150, 1, 0, 10, 64, 8, 49, 48, 50, 52, 128, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 101, 97, 116, 116, 108, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 68, 101, 105, 116, 101, 114, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 146, 1, 0, 12, 68, 8, 49, 48, 50, 52, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 101, 97, 116, 116, 108, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 65, 98, 114, 97, 109, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 136, 1, 0, 12, 70, 8, 49, 48, 50, 52, 116, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 97, 117, 108, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 142, 1, 0, 12, 76, 8, 49, 48, 50, 52, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 117, 108, 105, 101, 32, 65, 98, 114, 97, 109, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 152, 1, 0, 14, 80, 8, 49, 48, 50, 52, 130, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 76, 111, 115, 32, 65, 110, 103, 101, 108, 101, 115, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 76, 117, 107, 101, 32, 78, 111, 114, 105, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 162, 1, 0, 14, 82, 8, 49, 48, 50, 52, 140, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 97, 110, 32, 70, 114, 97, 110, 99, 105, 115, 99, 111, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 87, 97, 108, 116, 101, 114, 32, 87, 97, 108, 116, 111, 110, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 144, 1, 0, 14, 84, 8, 49, 48, 50, 52, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 101, 97, 116, 116, 108, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 117, 108, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 136, 1, 0, 14, 86, 8, 49, 48, 50, 52, 116, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 117, 108, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 146, 1, 0, 14, 88, 8, 49, 48, 50, 52, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 80, 104, 111, 101, 110, 105, 120, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 68, 101, 105, 116, 101, 114, 32, 78, 111, 114, 105, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 142, 1, 0, 14, 92, 8, 49, 48, 50, 52, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 144, 1, 0, 16, 94, 8, 49, 48, 50, 52, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 67, 104, 101, 121, 101, 110, 110, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 111, 104, 110, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 0, 0, 0, 0, 0, 0, 0, 150, 0, 0, 8, 107, 0, 0, 0, 0, 2, 193, 180, 48, 75, 0, 0, 0, 0, 0, 47, 0, 0, 1, 147, 198, 254, 118, 134, 0, 0, 1, 147, 198, 254, 118, 141, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 50, 0, 0, 0, 28, 144, 1, 0, 0, 0, 8, 49, 48, 50, 52, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 117, 108, 105, 101, 32, 83, 112, 101, 110, 99, 101, 114, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 144, 1, 0, 2, 10, 8, 49, 48, 50, 52, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 82, 101, 100, 109, 111, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 74, 111, 110, 101, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 144, 1, 0, 4, 18, 8, 49, 48, 50, 52, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 80, 104, 111, 101, 110, 105, 120, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 117, 108, 105, 101, 32, 74, 111, 110, 101, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 140, 1, 0, 4, 20, 8, 49, 48, 50, 52, 120, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 97, 117, 108, 32, 65, 98, 114, 97, 109, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 160, 1, 0, 4, 24, 8, 49, 48, 50, 52, 138, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 97, 110, 32, 70, 114, 97, 110, 99, 105, 115, 99, 111, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 75, 97, 116, 101, 32, 83, 112, 101, 110, 99, 101, 114, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 156, 1, 0, 4, 26, 8, 49, 48, 50, test_envoy1.1.niwx6cv9wii5@ecotype-26.nantes.grid5000.fr    | 52, 134, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 97, 110, 32, 70, 114, 97, 110, 99, 105, 115, 99, 111, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 117, 108, 32, 74, 111, 110, 101, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 142, 1, 0, 4, 28, 8, 49, 48, 50, 52, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 114, 97, 104, 32, 65, 98, 114, 97, 109, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 158, 1, 0, 4, 30, 8, 49, 48, 50, 52, 136, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 97, 110, 32, 70, 114, 97, 110, 99, 105, 115, 99, 111, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 146, 1, 0, 6, 36, 8, 49, 48, 50, 52, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 101, 97, 116, 116, 108, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 117, 108, 105, 101, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 138, 1, 0, 6, 38, 8, 49, 48, 50, 52, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 101, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 114, 97, 104, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 142, 1, 0, 8, 46, 8, 49, 48, 50, 52, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 82, 101, 100, 109, 111, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 117, 108, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 138, 1, 0, 8, 48, 8, 49, 48, 50, 52, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 101, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 114, 97, 104, 32, 78, 111, 114, 105, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 150, 1, 0, 8, 50, 8, 49, 48, 50, 52, 128, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 67, 104, 101, 121, 101, 110, 110, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 75, 97, 116, 101, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 162, 1, 0, 8, 52, 8, 49, 48, 50, 52, 140, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 97, 110, 32, 70, 114, 97, 110, 99, 105, 115, 99, 111, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 87, 97, 108, 116, 101, 114, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 138, 1, 0, 8, 54, 8, 49, 48, 50, 52, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 140, 1, 0, 10, 60, 8, 49, 48, 50, 52, 120, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 117, 108, 105, 101, 32, 74, 111, 110, 101, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 158, 1, 0, 10, 62, 8, 49, 48, 50, 52, 136, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 97, 110, 32, 70, 114, 97, 110, 99, 105, 115, 99, 111, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 86, 105, 99, 107, 121, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 150, 1, 0, 10, 64, 8, 49, 48, 50, 52, 128, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 101, 97, 116, 116, 108, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 68, 101, 105, 116, 101, 114, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 146, 1, 0, 10, 68, 8, 49, 48, 50, 52, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 101, 97, 116, 116, 108, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 65, 98, 114, 97, 109, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 136, 1, 0, 12, 70, 8, 49, 48, 50, 52, 116, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 97, 117, 108, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 142, 1, 0, 12, 76, 8, 49, 48, 50, 52, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 117, 108, 105, 101, 32, 65, 98, 114, 97, 109, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 152, 1, 0, 12, 80, 8, 49, 48, 50, 52, 130, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 76, 111, 115, 32, 65, 110, 103, 101, 108, 101, 115, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 76, 117, 107, 101, 32, 78, 111, 114, 105, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 162, 1, 0, 12, 82, 8, 49, 48, 50, 52, 140, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 97, 110, 32, 70, 114, 97, 110, 99, 105, 115, 99, 111, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 87, 97, 108, 116, 101, 114, 32, 87, 97, 108, 116, 111, 110, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 144, 1, 0, 14, 84, 8, 49, 48, 50, 52, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 101, 97, 116, 116, 108, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 117, 108, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 136, 1, 0, 14, 86, 8, 49, 48, 50, 52, 116, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 117, 108, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 146, 1, 0, 14, 88, 8, 49, 48, 50, 52, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 80, 104, 111, 101, 110, 105, 120, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 68, 101, 105, 116, 101, 114, 32, 78, 111, 114, 105, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 142, 1, 0, 14, 92, 8, 49, 48, 50, 52, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 144, 1, 0, 14, 94, 8, 49, 48, 50, 52, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 67, 104, 101, 121, 101, 110, 110, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 50, 52, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 111, 104, 110, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 0, 0, 0, 0, 0, 0, 1, 244, 0, 0, 7, 47, 0, 0, 0, 0, 2, 20, 154, 16, 223, 0, 0, 0, 0, 0, 48, 0, 0, 1, 147, 198, 254, 161, 14, 0, 0, 1, 147, 198, 254, 161, 18, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 250, 0, 0, 0, 24, 158, 1, 0, 0, 0, 8, 49, 48, 51, 53, 136, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 97, 110, 32, 70, 114, 97, 110, 99, 105, 115, 99, 111, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 86, 105, 99, 107, 121, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 146, 1, 0, 0, 10, 8, 49, 48, 51, 53, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 80, 104, 111, 101, 110, 105, 120, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 76, 117, 107, 101, 32, 83, 112, 101, 110, 99, 101, 114, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 138, 1, 0, 0, 12, 8, 49, 48, 51, 53, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 75, 97, 116, 101, 32, 87, 97, 108, 116, 111, 110, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 138, 1, 0, 2, 16, 8, 49, 48, 51, 53, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 101, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 114, 97, 104, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 142, 1, 0, 2, 20, 8, 49, 48, 51, 53, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 101, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 86, 105, 99, 107, 121, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 146, 1, 0, 2, 26, 8, 49, 48, 51, 53, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 67, 104, 101, 121, 101, 110, 110, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 114, 97, 104, 32, 78, 111, 114, 105, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 146, 1, 0, 2, 28, 8, 49, 48, 51, 53, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 101, 97, 116, 116, 108, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 97, 117, 108, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 142, 1, 0, 2, 30, 8, 49, 48, 51, 53, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 87, 97, 108, 116, 101, 114, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 136, 1, 0, 4, 34, 8, 49, 48, 51, 53, 116, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 101, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 117, 108, 32, 78, 111, 114, 105, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 146, 1, 0, 4, 36, 8, 49, 48, 51, 53, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 68, 101, 105, 116, 101, 114, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 136, 1, 0, 4, 38, 8, 49, 48, 51, 53, 116, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 75, 97, 116, 101, 32, 74, 111, 110, 101, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 146, 1, 0, 4, 40, 8, 49, 48, 51, 53, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 101, 97, 116, 116, 108, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 87, 97, 108, 116, 101, 114, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 142, 1, 0, 4, 44, 8, 49, 48, 51, 53, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 86, 105, 99, 107, 121, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 152, 1, 0, 6, 52, 8, 49, 48, 51, 53, 130, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 67, 104, 101, 121, 101, 110, 110, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 68, 101, 105, 116, 101, 114, 32, 87, 97, 108, 116, 111, 110, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 150, 1, 0, 6, 60, 8, 49, 48, 51, 53, 128, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 67, 104, 101, 121, 101, 110, 110, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 75, 97, 116, 101, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 138, 1, 0, 6, 62, 8, 49, 48, 51, 53, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 101, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 111, 104, 110, 32, 65, 98, 114, 97, 109, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 140, 1, 0, 8, 80, 8, 49, 48, 51, 53, 120, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 144, 1, 0, 8, 82, 8, 49, 48, 51, 53, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 67, 104, 101, 121, 101, 110, 110, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 117, 108, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 150, 1, 0, 8, 86, 8, 49, 48, 51, 53, 128, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 80, 111, 114, 116, 108, 97, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 75, 97, 116, 101, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 150, 1, 0, 8, 88, 8, 49, 48, 51, 53, 128, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 67, 104, 101, 121, 101, 110, 110, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 142, 1, 0, 8, 90, 8, 49, 48, 51, 53, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 101, 97, 116, 116, 108, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 97, 117, 108, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 140, 1, 0, 8, 92, 8, 49, 48, 51, 53, 120, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 86, 105, 99, 107, 121, 32, 78, 111, 114, 105, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 156, 1, 0, 8, 94, 8, 49, 48, 51, 53, 134, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 76, 111, 115, 32, 65, 110, 103, 101, 108, 101, 115, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 111, 104, 110, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 160, 1, 0, 8, 96, 8, 49, 48, 51, 53, 138, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 97, 110, 32, 70, 114, 97, 110, 99, 105, 115, 99, 111, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 117, 108, 105, 101, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 0, 0, 0, 0, 0, 0, 2, 37, 0, 0, 7, 47, 0, 0, 0, 0, 2, 16, 124, 46, 253, 0, 0, 0, 0, 0, 48, 0, 0, 1, 147, 198, 254, 161, 39, 0, 0, 1, 147, 198, 254, 161, 43, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 250, 0, 0, 0, 24, 158, 1, 0, 0, 0, 8, 49, 48, 51, 53, 136, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 97, 110, 32, 70, 114, 97, 110, 99, 105, 115, 99, 111, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 86, 105, 99, 107, 121, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 146, 1, 0, 2, 10, 8, 49, 48, 51, 53, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 80, 104, 111, 101, 110, 105, 120, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 76, 117, 107, 101, 32, 83, 112, 101, 110, 99, 101, 114, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 138, 1, 0, 2, 12, 8, 49, 48, 51, 53, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 75, 97, 116, 101, 32, 87, 97, 108, 116, 111, 110, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 138, 1, 0, 2, 16, 8, 49, 48, 51, 53, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 101, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 114, 97, 104, 32, 83, 109, 105, 116, 104, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 142, 1, 0, 2, 20, 8, 49, 48, 51, 53, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 101, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 86, 105, 99, 107, 121, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 146, 1, 0, 2, 26, 8, 49, 48, 51, 53, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 67, 104, 101, 121, 101, 110, 110, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 114, 97, 104, 32, 78, 111, 114, 105, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 146, 1, 0, 2, 28, 8, 49, 48, 51, 53, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 101, 97, 116, 116, 108, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 97, 117, 108, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 142, 1, 0, 2, 30, 8, 49, 48, 51, 53, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 87, 97, 108, 116, 101, 114, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 136, 1, 0, 4, 34, 8, 49, 48, 51, 53, 116, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 101, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 117, 108, 32, 78, 111, 114, 105, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 146, 1, 0, 4, 36, 8, 49, 48, 51, 53, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 68, 101, 105, 116, 101, 114, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 136, 1, 0, 4, 38, 8, 49, 48, 51, 53, 116, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 75, 97, 116, 101, 32, 74, 111, 110, 101, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 146, 1, 0, 4, 40, 8, 49, 48, 51, 53, 126, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 101, 97, 116, 116, 108, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 87, 97, 108, 116, 101, 114, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 142, 1, 0, 4, 44, 8, 49, 48, 51, 53, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 75, 101, 110, 116, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 86, 105, 99, 107, 121, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 152, 1, 0, 4, 52, 8, 49, 48, 51, 53, 130, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 67, 104, 101, 121, 101, 110, 110, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 68, 101, 105, 116, 101, 114, 32, 87, 97, 108, 116, 111, 110, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 150, 1, 0, 6, 60, 8, 49, 48, 51, 53, 128, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 67, 104, 101, 121, 101, 110, 110, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 75, 97, 116, 101, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 138, 1, 0, 6, 62, 8, 49, 48, 51, 53, 118, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 101, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 111, 104, 110, 32, 65, 98, 114, 97, 109, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 140, 1, 0, 6, 80, 8, 49, 48, 51, 53, 120, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 144, 1, 0, 6, 82, 8, 49, 48, 51, 53, 124, 123, 34, 99, 105, 116, 121, 34, 58, 34, 67, 104, 101, 121, 101, 110, 110, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 83, 97, 117, 108, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 150, 1, 0, 8, 86, 8, 49, 48, 51, 53, 128, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 80, 111, 114, 116, 108, 97, 110, 100, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 75, 97, 116, 101, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 79, 82, 34, 125, 0, 150, 1, 0, 8, 88, 8, 49, 48, 51, 53, 128, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 67, 104, 101, 121, 101, 110, 110, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 101, 116, 101, 114, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 142, 1, 0, 8, 90, 8, 49, 48, 51, 53, 122, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 101, 97, 116, 116, 108, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 80, 97, 117, 108, 32, 87, 104, 105, 116, 101, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 140, 1, 0, 8, 92, 8, 49, 48, 51, 53, 120, 123, 34, 99, 105, 116, 121, 34, 58, 34, 66, 111, 105, 115, 101, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 86, 105, 99, 107, 121, 32, 78, 111, 114, 105, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 156, 1, 0, 8, 94, 8, 49, 48, 51, 53, 134, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 76, 111, 115, 32, 65, 110, 103, 101, 108, 101, 115, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 111, 104, 110, 32, 66, 97, 114, 116, 101, 108, 115, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 73, 68, 34, 125, 0, 160, 1, 0, 8, 96, 8, 49, 48, 51, 53, 138, 1, 123, 34, 99, 105, 116, 121, 34, 58, 34, 83, 97, 110, 32, 70, 114, 97, 110, 99, 105, 115, 99, 111, 34, 44, 34, 105, 100, 34, 58, 49, 48, 51, 53, 44, 34, 110, 97, 109, 101, 34, 58, 34, 74, 117, 108, 105, 101, 32, 83, 104, 117, 108, 116, 122, 34, 44, 34, 115, 116, 97, 116, 101, 34, 58, 34, 67, 65, 34, 125, 0, 0, 0, 0, 0, 0, 0, 1, 241, 0, 0, 0, 56, 0, 0, 0, 0, 2, 191, 169, 87, 116, 0, 0, 0, 0, 0, 0, 0, 0, 1, 147, 198, 254, 153, 89, 0, 0, 1, 147, 198, 254, 153, 89, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 248, 0, 0, 0, 1, 12, 0, 0, 0, 1, 1, 0, 1], pos: 13179 }";
        let vec_u8: Vec<u8> = little
            .split(',')
            .filter_map(|s| s.trim().parse::<u8>().ok()) // Parse each substring to u8
            .collect();

        println!("{:?}", vec_u8.len());
        let mut buf = Cursor::new(vec_u8);
        let res = PartitionData::decode(&mut buf, 12);

        println!("{:?}", res);
    }

    #[test]
    fn test_serde_json() {
        let mut json = json!({
            "id": 2,
            "label": "BLABLA",
            "price": 1.2564});
        let mut field_set = HashSet::new();
        field_set.insert("label");
        //AlloyFilter::filter_json_in_place(&mut json, &field_set, false);
        println!("{:?}", serde_json::to_string(&json).unwrap());
    }
}
