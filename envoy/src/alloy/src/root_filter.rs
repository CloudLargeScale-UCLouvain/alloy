use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use crate::data;
use crate::tcp_filter;
use data::*;
use kafka_alloy::KafkaRoutingProtocol;
use kafka_protocol::messages::fetch_response::FetchableTopicResponse;
use kafka_protocol::messages::fetch_response::PartitionData;
use kafka_protocol::messages::FetchResponse;
use kafka_protocol::messages::ResponseHeader;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::Builder;
use kafka_protocol::protocol::StrBytes;
use log::{debug, error, info, trace, warn};
use proxy_wasm::hostcalls;
use proxy_wasm::traits::*;
use proxy_wasm::types::*;
use regex::bytes::Regex;
use routing::*;
use tcp_filter::*;

extern crate alloc;
pub struct AlloyRootFilter {
    pub context_id: u32,
    pub config_bytes: Vec<u8>,
    pub configuration: RoutingConfiguration,
    pub requests_queue_id: u32,
    pub responses_queue_id: u32,
    pub map_requests: HashMap<String, Rc<FetchRequestData>>,
    pub map_responses: HashMap<String, FetchResponseData>,
    //pub map_responses: HashMap<String, Vec<FetchResponseData>>,
    pub identifier: String,
    pub fetch_max_wait_ms_virtual_partition: u128,
    pub fetch_min_bytes_virtual_partition: usize,
    pub map_queue: HashMap<u32, String>,
}

impl AlloyRootFilter {
    fn synchronize(&mut self) {
        WAITING_CONTEXTS.with(|waiters| {
            for (context_id, connection_info) in waiters.borrow_mut().iter_mut() {
                let elapsed = connection_info
                    .response_initial_time
                    .elapsed()
                    .unwrap()
                    .as_millis();
                debug!(
                    "checking context_id {:?} connection_info {:?} : elapsed {:?} - timeout {:?}",
                    context_id, connection_info.request_data, elapsed, connection_info.timeout
                );

                /*
                if connection_info.timeout {
                    continue
                }
                */
                /*
                if elapsed > self.fetch_max_wait_ms_virtual_partition {
                    connection_info.timeout = true;
                    trace!("set effective context {:?}", context_id);
                    hostcalls::set_effective_context(*context_id).unwrap();
                    trace!("resume downstream");
                    match hostcalls::resume_downstream() {
                        Ok(_) => {
                            debug!("timeout: downstream resumed");
                        },
                        Err(e) => {
                            error!("error {:?} while resuming downstream", e);
                        }
                    }
                } else { */
                if connection_info.response_data.is_some() {
                    continue;
                }
                let frd = &connection_info.request_data;
                if self.check_response(frd) {
                    match self.generate_fetch_response(frd) {
                        Ok((_, payload)) => {
                            //self.send_response(*context_id, payload);
                            debug!("full. setting response");
                            connection_info.response_data = Some(payload);
                            hostcalls::set_effective_context(*context_id).unwrap();
                            debug!("resume downstream");
                            match hostcalls::resume_downstream() {
                                Ok(_) => {
                                    debug!(": downstream resumed");
                                }
                                Err(e) => {
                                    error!("error {:?} while resuming downstream", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("erreur while generating fetch response for {:?}", frd)
                        }
                    } /*
                      } else {
                          debug!("missing responses for request {:?}", frd);
                      } */
                }
            }
        });
    }

    fn generate_fetch_response(&self, request: &FetchRequestData) -> Result<(u32, Vec<u8>), &str> {
        let mut r = FetchResponse::default();
        r.error_code = 0;
        r.session_id = request.session_id;

        for frdp in &request.partitions {
            let shared_data_key = frdp.get_shared_queue_key();
            trace!("looking for response {:?}", shared_data_key);
            if let Some(response) = self.map_responses.get(&shared_data_key) {
                let mut ftr = FetchableTopicResponse::default();

                //TODO: update kafka protocol .. https://github.com/tychedelia/kafka-protocol-rs/issues/43
                let bytes = Bytes::from(response.topic.clone());
                unsafe {
                    let t = StrBytes::from_utf8_unchecked(bytes::Bytes::from(bytes));
                    ftr.topic = TopicName(t);
                }

                let mut pd = PartitionData::default();
                pd.partition_index = frdp.partition;
                //TODO: remove clone
                pd.records = Some(response.records.clone().into());
                ftr.partitions.push(pd);
                r.responses.push(ftr);
            } else {
                error!("generate_fetch_response: {:?} not found!", &shared_data_key);
                return Err("not found");
            }
        }
        let header = ResponseHeader::builder()
            .correlation_id(request.correlation_id)
            .build()
            .unwrap();
        trace!(
            "generated fetchresponse data for context_id {:?}: correlation_id {:?} - {:?}",
            request.context_id,
            request.correlation_id,
            r
        );
        let payload = KafkaRoutingProtocol::encode(1, header, request.body_version, r);
        Ok((request.context_id, payload))
    }

    fn add_response(&mut self, data: Vec<u8>) {
        let frd = match bincode::deserialize::<FetchResponseData>(&data) {
            Ok(v) => Some(v),
            Err(e) => {
                error!("error while deserializing response {:?}", e);
                None
            }
        };
        if let Some(frd) = frd {
            debug!("adding response {:?} in cache", frd.get_shared_queue_key());
            self.map_responses.insert(frd.get_shared_queue_key(), frd);
        }
    }

    fn check_response(&mut self, request: &FetchRequestData) -> bool {
        let mut full_request: bool = true;

        // We iterate on each tp of this request, and check if there is some result
        for frdp in &request.partitions {
            let shared_data_key = frdp.get_shared_queue_key();
            if let Some(_resp) = self.map_responses.get(&shared_data_key) {
                debug!("response is complete");
                // found a response
            } else {
                let shared_data_key = frdp.get_shared_data_key();
                // some is not found! we abandon here.
                debug!(
                    "request for {:?} is incomplete trying to get it from shared_data",
                    shared_data_key
                );
                let (data, _cas) = self.get_shared_data(&shared_data_key);
                if let Some(data) = data {
                    let frd = match bincode::deserialize::<FetchResponseData>(&data) {
                        Ok(v) => Some(v),
                        Err(e) => {
                            error!("error while deserializing response {:?}", e);
                            None
                        }
                    };
                    if let Some(frd) = frd {
                        debug!(
                            "found response {:?} adding it in cache",
                            frd.get_shared_queue_key()
                        );
                        self.map_responses.insert(frd.get_shared_queue_key(), frd);
                    }
                } else {
                    full_request = false;
                }
            }
        }
        return full_request;
    }
}

impl Context for AlloyRootFilter {}

impl RootContext for AlloyRootFilter {
    fn on_configure(&mut self, _: usize) -> bool {
        debug!("on_configure");
        if let Some(config_bytes) = self.get_plugin_configuration() {
            let config = RoutingConfiguration::retrieve(config_bytes.clone())
                .expect("unable to decode yaml config");
            self.configuration = config;
            self.config_bytes = config_bytes;
            let empty_tree = BTreeMap::new();
            let additional_configuration = self
                .configuration
                .additional_configuration
                .as_ref()
                .unwrap_or(&empty_tree);
            self.set_tick_period(Duration::from_millis(
                additional_configuration
                    .get("period_virtual_partition")
                    .unwrap_or(&String::from("100"))
                    .parse()
                    .unwrap(),
            ));
            self.fetch_max_wait_ms_virtual_partition = additional_configuration
                .get("fetch_max_wait_ms_virtual_partition")
                .unwrap_or(&String::from("1000"))
                .parse()
                .unwrap();
            info!("Root configuration: {:?}", self.configuration);
        }

        self.requests_queue_id = self.register_shared_queue(&format!("requests"));
        self.responses_queue_id = self.register_shared_queue(&format!("responses"));
        true
    }

    fn on_queue_ready(&mut self, queue_id: u32) {
        debug!("on_queue_ready {}", queue_id);
        loop {
            match self.dequeue_shared_queue(queue_id) {
                Ok(v) => {
                    if let Some(payload) = v {
                        debug!("on_queue_ready {}, found {}", queue_id, payload.len());
                        self.add_response(payload);
                    } else {
                        debug!("on_queue_ready {} no more messages", queue_id);
                        // end of queue
                        break;
                    }
                }
                Err(e) => {
                    error!(
                        "error {:?} while dequeueing {:?} ({:?}",
                        e,
                        queue_id,
                        self.map_queue.get(&queue_id)
                    );
                }
            }
        }
        self.synchronize();
    }

    fn on_tick(&mut self) {
        WAITING_CONTEXTS.with(|waiters| {
            for (context_id, connection_info) in waiters.borrow_mut().iter_mut() {
                for partition in &connection_info.request_data.partitions {
                    let key = partition.get_shared_queue_key();
                    // if value exists in map
                    trace!("check if queue {} registered", key);
                    if !self.map_queue.values().any(|val| val == &key) {
                        let queue_id = self.register_shared_queue(&key);
                        debug!("registering queue {} ({})", key, queue_id);
                        self.map_queue.insert(queue_id, key);
                    }
                    if connection_info
                        .response_initial_time
                        .elapsed()
                        .unwrap()
                        .as_millis()
                        > self.fetch_max_wait_ms_virtual_partition
                    {
                        connection_info.timeout = true;
                        trace!("set effective context {:?}", context_id);
                        hostcalls::set_effective_context(*context_id).unwrap();
                        trace!("resume downstream");
                        match hostcalls::resume_downstream() {
                            Ok(_) => {
                                debug!("timeout: downstream resumed (on_tick) {:?}", context_id);
                            }
                            Err(e) => {
                                error!("error {:?} while resuming downstream", e);
                            }
                        }
                    }
                }
            }
        });

        //self.synchronize();
    }

    fn create_stream_context(&self, context_id: u32) -> Option<Box<dyn StreamContext>> {
        info!(
            "Create stream context {:?}, config:{:?}",
            context_id, &self.configuration
        );
        let additional_configuration = self
            .configuration
            .clone()
            .additional_configuration
            .unwrap_or(BTreeMap::new());

        let mut routing_table = RoutingTable::new();
        routing_table.configure(self.configuration.clone());
        let map_alloy_filters;

        let max_parallelism = additional_configuration
            .get("max_parallelism")
            .unwrap_or(&String::from("128"))
            .parse()
            .unwrap();
        let debug_protocol = additional_configuration
            .get("debug_protocol")
            .unwrap_or(&String::from("0"))
            .parse()
            .unwrap();
        let fetch_min_bytes_virtual_partition = additional_configuration
            .get("fetch_min_bytes_virtual_partition")
            .unwrap_or(&String::from("1000000"))
            .parse()
            .unwrap();

        let config =
            String::from_utf8(self.config_bytes.clone()).expect("config utf8 decoding error");
        let deser = serde_yaml::from_str(&config[..]);
        let map: MapAlloyFilterCriteria = deser.unwrap();
        map_alloy_filters = map.alloy_filters.unwrap_or_default();

        let mut map_alloy_filters = map_alloy_filters.clone();
        let mut map_bimap_virtual_origin = HashMap::new();

        for (topic, alloy_filter) in map_alloy_filters.iter_mut() {
            info!(
                "Alloy filter criteria for topic {:?}: {:?} ",
                topic, alloy_filter
            );
            //TODO: add dynamic reconfiguration in sources / partitions
            for selection in alloy_filter.selections.iter_mut() {
                if let Some(value) = &selection.value {
                    selection.value_regex = Some(Regex::new(&value).unwrap());
                }
            }

            let bimap_virtual_origin;
            if !alloy_filter.partition.is_empty() {
                bimap_virtual_origin = KafkaRoutingProtocol::compute_virtual_partitions(
                    alloy_filter.num_sources,
                    alloy_filter.num_partitions,
                );
                info!(
                    "Initialized Kafka connection with virtual partitions: {:?}",
                    bimap_virtual_origin
                );
            } else {
                bimap_virtual_origin = KafkaRoutingProtocol::compute_virtual_partitions(
                    1,
                    alloy_filter.num_partitions,
                );
                info!(
                    "Initialized Kafka connection with origin partitions: {:?}",
                    bimap_virtual_origin
                );
            }
            map_bimap_virtual_origin.insert(topic.clone(), bimap_virtual_origin);
        }
        for (_topic, alloy_filter) in map_alloy_filters.iter_mut() {
            for selection in alloy_filter.selections.iter_mut() {
                if let Some(value) = &selection.value {
                    selection.value_regex = Some(Regex::new(&value).unwrap());
                }
            }
        }
        let remove_empty_records = additional_configuration
            .get("remove_empty_records")
            .unwrap_or(&String::from("true"))
            .contains("true");

        let rt: Rc<RefCell<dyn RoutingProtocol>> =
            match self.configuration.workload_type.as_ref().unwrap().as_str() {
                "kafka-alloy" => kafka_alloy::KafkaRoutingProtocol::new(debug_protocol),
                _ => {
                    warn!("Caution: unknown protocol, using DefaultRoutingProtocol instead.");
                    DefaultRoutingProtocol::new()
                }
            };

        let instance = Box::new(AlloyFilter {
            context_id: context_id,
            configuration: self.configuration.clone(),
            protocol: rt,
            routing_table: routing_table,
            request_header: None,
            request: None,
            fetch_request_api_version: -1,
            correlation_id: -1,
            data_size: 0,
            map_alloy_filters: map_alloy_filters,
            max_parallelism: max_parallelism,
            map_split_id: HashMap::new(),
            map_index_query_partition: HashMap::new(),
            bimap_virtual_origin: map_bimap_virtual_origin,
            remove_empty_records: remove_empty_records,
            session_id: -1,
            session_epoch: -1,
            origin_fetch_request: None,
            map_queue_vp: HashMap::new(),
            map_vp_records: RefCell::new(HashMap::new()),
            fetch_min_bytes_virtual_partition: fetch_min_bytes_virtual_partition,
        });
        Some(instance)
    }

    fn get_type(&self) -> Option<ContextType> {
        match self.configuration.type_proxy.as_ref().unwrap().as_str() {
            "control" => Some(ContextType::HttpContext),
            _ => Some(ContextType::StreamContext),
        }
    }
}
