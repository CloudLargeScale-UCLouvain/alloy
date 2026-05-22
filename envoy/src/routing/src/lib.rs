use std::any::Any;
use std::collections::{BTreeMap};
use std::rc::{Rc};
use std::cell::RefCell;
extern crate bytes;

use log::{debug,warn,trace};
use regex::bytes::Regex;

// TODO: integrate time, need access to <plugin>.get_current_time()
/*
fn get_timestamp() -> u64 {
    
    match self.get_current_time() {
        Ok(n) => u64::try_from(n.as_nanos()).expect("error while converting timestamp to u64"), 
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}
*/

//TODO: add exception of multi RR

#[derive(PartialEq)]
#[derive(Debug)]
#[derive(Clone)]
#[derive(Copy)]
pub enum RequestResponseKind {
    System,
    Local,
    Routable,
    Incomplete,
}

#[derive(PartialEq)]
#[derive(Debug)]
#[derive(Clone)]
#[derive(Copy)]
pub enum RequestType {
    OneWay, // 1 request, 0 response
    RequestResponse, // 1 request, 1 response
    Subscribe, // 1 request n responses
    Incomplete,
}


#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone, Copy)]
pub enum RoutingMechanism {
    Synchronous,
    DeferRemoteLocalCommands,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[derive(Debug)]
pub struct RoutingConfiguration {
    pub workload_type: Option<String>,
    pub type_proxy: Option<String>,
    pub local_cluster: Option<String>,
    pub default_cluster: Option<String>,
    //pub routing_table: Option<BTreeMap<String, RoutingEntry>>,
    pub routing_table: Option<Vec<RoutingEntry>>,
    pub routing_mechanism: Option<RoutingMechanism>,
    pub additional_configuration: Option<BTreeMap<String, String>>,
}

impl RoutingConfiguration {
    pub fn new() -> Self {
        Self {
            workload_type:None,
            type_proxy: None,
            local_cluster: None,
            default_cluster: None,
            routing_table: Some(Vec::new()),
            routing_mechanism: None,
            additional_configuration: None,
        }
    }
    pub fn retrieve(yaml: Vec<u8>) -> Result<RoutingConfiguration, serde_yaml::Error> {
        let config = String::from_utf8(yaml).expect("config utf8 decoding error");
        let config: RoutingConfiguration = serde_yaml::from_str(&config[..])?;
        Ok(config)
    }

    pub fn add_route(& mut self, entry:RoutingEntry) {
        self.routing_table.as_mut().unwrap().push(entry);
    }
}


#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[derive(Debug)]
pub struct RoutingEntry {
    pub key: Option<String>,
    #[serde(skip_serializing, skip_deserializing)]
    pub key_regex: Option<Regex>,
    pub value: Option<String>, 
    #[serde(skip_serializing, skip_deserializing)]
    pub value_regex: Option<Regex>,
    pub cluster: String,
}

pub trait RoutingProtocol: Any {
    fn payload_compare_size(&self, is_request: bool, payload: &Vec<u8>) -> isize;
    fn request_response_type(&self, payload: &Vec<u8>) -> (RequestType, RequestResponseKind, RequestResponseKind);
    fn response_type(&self, routing_table: &RoutingTable, payload: &Vec<u8>) -> RequestResponseKind;
    fn split_queries(&self, routing_table: &RoutingTable, request: &Vec<u8>) -> Result<BTreeMap<String, Vec<u8>>, &'static str>;    
    fn merge_responses(&self, routing_table: &RoutingTable, response_map: &BTreeMap<u32, Vec<u8>>) -> Result<Vec<u8>, &'static str>;
    fn as_any(&self) -> &dyn Any;
}
pub struct DefaultRoutingProtocol {
}
impl DefaultRoutingProtocol {
    pub fn new() -> Rc<RefCell<dyn RoutingProtocol>> {
        let a = Self {
        };
        Rc::new(RefCell::new(a))
    }

}

impl RoutingProtocol for DefaultRoutingProtocol {
    fn as_any(&self) -> &dyn Any {
        self
    }
        
    fn payload_compare_size(&self, _is_request: bool, _payload: &Vec<u8>) -> isize {
        0 // always the good size
    }

    fn request_response_type(&self, _payload: &Vec<u8>) -> (RequestType, RequestResponseKind, RequestResponseKind) {
        (RequestType::RequestResponse, RequestResponseKind::Routable, RequestResponseKind::Routable)
    }        

    fn response_type(&self, _routing_table: &RoutingTable, _payload: &Vec<u8>) -> (RequestResponseKind) {
        RequestResponseKind::Routable
    }        

    fn split_queries(&self, _routing_table: &RoutingTable, request: &Vec<u8>) -> Result<BTreeMap<String, Vec<u8>>, &'static str> {

        let mut requests: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        requests.insert(_routing_table.local_cluster.clone().unwrap(), request.clone());                

        trace!("Default split_queries: {:?}", requests);
        Ok(requests)
    }
    
    fn merge_responses(&self, _routing_table: &RoutingTable, response_map: &BTreeMap<u32, Vec<u8>>) -> Result<Vec<u8>, &'static str> {
        let response: Vec<u8> = response_map.values().next().unwrap().to_vec();
        Ok(response)
    }    

}

#[derive(Debug, Clone)]
pub struct TimestampedPayload {
    pub timestamp: u64,
    pub is_request: bool,
    pub request_type: RequestType,
    pub kind: RequestResponseKind,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct RoutingTable {
    //pub map: BTreeMap<String, RoutingEntry>,
    pub vec: Vec<RoutingEntry>,
    pub clusters: Vec<String>,
    pub default_cluster: Option<String>,
    pub local_cluster: Option<String>,
    pub layer: String,
    pub routing_mechanism: RoutingMechanism,

    //pub requests: Vec<TimestampedPayload>,
    //pub responses: Vec<TimestampedPayload>,

    pub origin_request: Option<Vec<u8>>,

    pub current_requests: BTreeMap<u32, (String, Vec<u8>)>,
    pub current_responses: BTreeMap<u32, Vec<u8>>,

    pub current_request_type: RequestType,
    pub current_request_kind: RequestResponseKind,
    pub current_response_kind: RequestResponseKind,

    pub are_initial_requests_set: BTreeMap<String, bool>,
    pub local_requests: Option<Vec<Vec<u8>>>,
    pub future_requests_counter: usize,    
    pub future_requests: BTreeMap<String, Vec<Vec<u8>>>,
    pub complete_requests:usize,
}

impl RoutingTable {
    pub fn new() -> Self {
        Self {
            //map: BTreeMap::new(),
            vec: Vec::new(),
            clusters: Vec::new(),
            origin_request: None,
            current_requests: BTreeMap::new(),
            current_responses: BTreeMap::new(),
            default_cluster: None,
            local_cluster: None,
            routing_mechanism: RoutingMechanism::Synchronous,
            local_requests: None,
            are_initial_requests_set: BTreeMap::new(),
            layer: String::from(""),
            //requests: Vec::new(),
            //responses: Vec::new(),
            current_request_type: RequestType::RequestResponse,
            current_request_kind: RequestResponseKind::Routable,
            current_response_kind: RequestResponseKind::Routable,
            future_requests: BTreeMap::new(),
            future_requests_counter: 0,
            complete_requests: 0,
        }
    }

    pub fn find_subsequence(&self, haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|window| window == needle)
    } 


    // default behaviour
    pub fn find_entry_by_key(&self, key: &[u8]) -> Option<&RoutingEntry> {
        let mut entry:Option<&RoutingEntry> = None;
        
        for routing_entry in self.get_routing_table() {
            if let Some(_size) = self.find_subsequence(key, routing_entry.key.as_ref().unwrap().as_bytes()) {
                // found!
                entry = Some(&routing_entry);
                break;
            } 
        }
        entry
    }    

    pub fn find_entry(&self, key: &[u8], value: &[u8]) -> Option<&RoutingEntry> {
        let mut entry:Option<&RoutingEntry> = None;

        for routing_entry in self.get_routing_table() {
            /*
            let text =  match routing_entry.entry_type {
                Some(EntryType::KeyValue) => value,
                Some(EntryType::Key) | None => key,
            };
             */
            let is_match = 
                routing_entry.key_regex.as_ref().map_or(true, |r| r.is_match(key))
             && routing_entry.value_regex.as_ref().map_or(true, |r| r.is_match(value));

            if is_match {
                entry = Some(routing_entry);
                break
            }
        }
        entry
    }
    pub fn configure(& mut self, configuration: RoutingConfiguration) {
        debug!("Configuring routing table with {:?}", configuration);
        /*        if configuration.routing_table.is_some() {
            self.map = configuration.routing_table.unwrap();
        } else {
            self.map.clear();
        }
        */

        self.routing_mechanism = configuration.routing_mechanism.unwrap_or(RoutingMechanism::Synchronous);

        self.clusters = vec![];
        self.default_cluster = configuration.default_cluster;
        if self.default_cluster.is_some() {
            self.clusters.push(self.default_cluster.clone().unwrap());
        }
        self.local_cluster = configuration.local_cluster;
        if self.local_cluster.is_some() {
            self.clusters.push(self.local_cluster.clone().unwrap());
        }
        self.layer = configuration.type_proxy.unwrap();

        self.vec = Vec::new();
        if configuration.routing_table.is_some() {
            for mut state in configuration.routing_table.unwrap().drain(..) {
                if &state.cluster != self.default_cluster.as_ref().unwrap() && &state.cluster != self.local_cluster.as_ref().unwrap() {
                    self.clusters.push(state.cluster.clone());
                }
                
                state.key_regex = state.key.as_ref().map(|key_value| Regex::new(key_value).unwrap());
                state.value_regex = state.value.as_ref().map(|value_value| Regex::new(value_value).unwrap());

                self.vec.push(state);
            }
        } else {
            self.vec.clear();
        }
        self.clusters.sort();
        self.clusters.dedup();
        
    }

    fn get_routing_table(&self) -> &Vec<RoutingEntry> {
        return &self.vec;
    }

    pub fn get_requests(&self) -> &BTreeMap<u32, (String, Vec<u8>)> {
        return &self.current_requests
    }

    pub fn get_responses(&self) -> &BTreeMap<u32, Vec<u8>> {
        return &self.current_responses
    }

    pub fn register_downstream_request(&mut self, request: Option<Vec<u8>>) {
        trace!("Register origin request (layer: {}): {:?}", self.layer, request);
        self.origin_request = request;
    }
    pub fn register_upstream_request(&mut self, token: u32, cluster: String, payload: Vec<u8>) {
        trace!("Register processed request {} (layer: {}) to {}: {:?}", token, self.layer, cluster, payload);
        self.current_requests.insert(token, (cluster, payload));        
    }
    pub fn register_upstream_response(&mut self, _protocol:Rc<RefCell<dyn RoutingProtocol>>, token: u32, payload: Vec<u8>) {
        trace!("register upstream response token:{}, response_kind:{:?}", token, self.current_response_kind);

        self.current_responses.insert(token, payload);
    }

    /// Returns a map of cluster/request depending on the layer and the given request
    /// 
    /// # Arguments
    /// 
    /// * `request` - The request that will be analyzed
    pub fn route_request(&mut self, protocol:Rc<RefCell<dyn RoutingProtocol>>) -> (Result<BTreeMap<String, Vec<u8>>, &'static str>, usize) {
        trace!("Route request (layer: {}) {:?}", self.layer, &self.origin_request);
        
        // check if incomplete payload / complete / multiple
        let difference:isize = protocol.borrow_mut().payload_compare_size(true, self.origin_request.as_ref().unwrap());
        match difference {
            difference if difference < 0 => { // incomplete payload, no need to continue
                debug!("Incomplete downstream message: {}", difference);
                self.current_request_type = RequestType::Incomplete;
                self.current_request_kind = RequestResponseKind::Incomplete;
                self.current_response_kind = RequestResponseKind::Incomplete;
                //self.current_response_kind = RequestResponseKind::Incomplete;
                return (Err("incomplete"), 0)
            },
            difference if difference > 0 => { // multiple messages, we take only the first
                debug!("Merged downstream messages: {}", difference);
                //payload = payload[..payload.len() - difference as usize].to_vec();
                let payload = self.origin_request.take().unwrap();
                let first_message = payload[..payload.len() - difference as usize].to_vec();
                debug!("Considering only the first {} bytes. : {:?}", payload.len() - difference as usize, first_message);
                self.origin_request = Some(first_message);
                //self.origin_request
                //self.current_responses.insert(token, first_message);
                //self.get_responses().set()
            },
            _difference => { // equality
                debug!("Exact downstream message!")
                // nothing to do here
                //actual_payload = payload[..].to_vec();
            }
        }
        let difference = difference as usize;


        let (current_request_type, current_request_kind, current_response_kind) = protocol.borrow_mut().request_response_type(self.origin_request.as_ref().unwrap());
        self.current_request_type = current_request_type;
        self.current_request_kind = current_request_kind;
        self.current_response_kind = current_response_kind;

        let request = self.origin_request.as_ref().unwrap();
        /* //TODO: for next iteration
        self.requests.push( // archive request
            TimestampedPayload {
                timestamp: 0,
                is_request: true,
                request_type: self.current_request_type,
                kind: self.current_request_kind,
                payload: request.clone(),
            }
        );*/

        let requests:BTreeMap<String, Vec<u8>>;
        debug!("processing request wih kind {:?} and type {:?}", current_request_kind, current_request_type);
        requests = match self.current_request_kind {
            RequestResponseKind::System | RequestResponseKind::Local => {
                // route to local only these kind of requests,
                let mut requests = BTreeMap::new();
                match self.routing_mechanism {
                    RoutingMechanism::Synchronous => { 
                        // broadcast local command
                        debug!("clusters: {:?}", &self.clusters);
                        for cluster in &self.clusters {
                            requests.insert(cluster.clone(), request.clone());
                        }
                    },
                    RoutingMechanism::DeferRemoteLocalCommands => {
                        // send only to local
                        requests.insert(self.local_cluster.as_ref().unwrap().clone(), request.clone());
                        if self.current_request_kind == RequestResponseKind::Local && *self.are_initial_requests_set.get(self.local_cluster.as_ref().unwrap()).unwrap_or(&false) == false {
                            if self.local_requests.is_none() {
                                self.local_requests = Some(Vec::new());
                            }
                            self.local_requests.as_mut().unwrap().push(request.clone());
                        }
                    },
                }
                requests
            }
            RequestResponseKind::Routable => {
                // split the queries 
                let mut requests = match Rc::clone(&protocol).borrow().split_queries(self, &request) {
                    Ok(v) => v,
                    Err(e) => return (Err(e), 0),
                };
                
                trace!("Found split_queries routes: {:?}", &requests);
                // no route, we route to default
                if requests.len() == 0 {
                    trace!("Routing request {:?} to default {}", &request, self.default_cluster.as_ref().unwrap());
                    requests.insert(self.default_cluster.as_ref().unwrap().clone(), request.clone());
                }

                // add routing commands, and their initial commands
                if self.routing_mechanism == RoutingMechanism::DeferRemoteLocalCommands {
                    // local requests are now set
                    self.are_initial_requests_set.insert(self.local_cluster.clone().unwrap(), true);

                    trace!("Get initial local requests: {:?} for requests {:?}", &self.local_requests, requests);
                    for (cluster, payload) in requests {
                        // get Local commands and add them on the remote list
                        let list: &mut Vec<Vec<u8>> = match self.future_requests.get_mut(&cluster) {
                            Some(v) => v,
                            None => {
                                self.future_requests.insert(cluster.clone(), Vec::new());
                                self.future_requests.get_mut(&cluster).unwrap()
                            },
                        };                
                        // initiate local requests if defined
                        if *self.are_initial_requests_set.get(&cluster).unwrap_or(&false) == false {
                            debug!("Initialize local requests for {:?}", &cluster);
                            self.are_initial_requests_set.insert(cluster.clone(), true);
                            if &cluster != self.local_cluster.as_ref().unwrap() {
                                for request in self.local_requests.as_ref().unwrap() {
                                    list.push(request.clone());
                                }
                            } else {
                                for _request in self.local_requests.as_ref().unwrap() {
                                    list.push(vec![]); // empty payload = pass this request for local
                                }
                            }
                        }
                        // add routable command 
                        list.push(payload);
                    }
                    trace!("Set future requests: {:?}", &self.future_requests);

                    // send next 
                    requests = self.next_requests().unwrap_or(BTreeMap::new());
                };

                requests
            },
            RequestResponseKind::Incomplete => {
                return (Err("incomplete"), 0)
            }
        };
        trace!("obtained routes: {:?}", &requests);
        self.complete_requests = 0;

        (Ok(requests), difference)
    }    

    pub fn check_last_request(&self, cluster:&String) -> bool {
        if let Some(list) = self.future_requests.get(cluster) {
            debug!("Check last request for cluster {}: {}", cluster, list.len());
            list.len() == 0
        } else {
            debug!("Check last request for cluster {}: no last request", cluster);
            true
        }
    }

    pub fn add_future_request(&mut self, cluster:String, payload:Vec<u8>) {
        let future_requests = self.future_requests.get_mut(&cluster).unwrap();
        future_requests.push(payload);
    }

    pub fn next_requests(&mut self) -> Option<BTreeMap<String, Vec<u8>>> {
        // iterate on current list
        if self.future_requests.len() > 0 {
            let mut map:BTreeMap<String, Vec<u8>> = BTreeMap::new();
            let mut clusters: Vec<String> = Vec::new();
            for (cluster, value) in &self.future_requests {
                if value.len() > 0 { // if there is still some future request
                    clusters.push(cluster.clone());
                }
            }
            
            for cluster in clusters { // for each cluster with future request
                let payload = self.future_requests.get_mut(&cluster).unwrap(); 
                
                let val = payload.remove(0); // we add payload and remove
                map.insert(cluster.clone(), val);
            }
            trace!("Next requests: {:?}", &map);
            Some(map)
        } else {
            None
        }
    }

    /// Returns a byte vector depending on layer and the list of received responses
    /// 
    /// # Arguments
    /// 
    /// * `response_map` - The list of responses that will be analyzed, in this case just the first one.
    pub fn route_response(&mut self, protocol:Rc<RefCell<dyn RoutingProtocol>>, token: u32, payload: Vec<u8>) -> (Result<Vec<u8>, &'static str>, usize) { 
        trace!("Route response {} {:?}", token, payload);
        
        trace!("Find request {} in {:?}", token, self.current_requests);
        let (cluster, request) = self.current_requests.get(&token).unwrap(); // identify cluster
        //let (current_request_type, _, _) = protocol.borrow_mut().request_response_type(request);
        let val = self.current_responses.get_mut(&token);
        if let Some(previous) = val {
            // extend with previous value (as Kafka split packets) ... this may wil provoke big bugs does not work with // inflight connections (ping/heartbeat...)
            // TODO: take into consideration // inflight packets
            match self.current_response_kind {
                RequestResponseKind::Incomplete => {
                    trace!("Token {}, cluster {} - extending previous {:?} with payload {:?}", token, cluster, previous, payload);
                    previous.extend(payload);
                },
                _ => {
                    // inflight request
                    warn!("Inflight request");
                },
            }
        } else {
            //self.current_response_kind = protocol.borrow_mut().response_type(self, &payload);
            self.current_responses.insert(token, payload);    
        }
        
        let payload = self.get_responses().get(&token).unwrap();
        
        // check if incomplete payload / complete / multiple
        let difference:isize = protocol.borrow_mut().payload_compare_size(false, self.get_responses().get(&token).unwrap());
        
        match difference {
            difference if difference < 0 => { // incomplete payload, no need to continue
                debug!("Incomplete upstream message: {}", difference);
                self.current_response_kind = RequestResponseKind::Incomplete;
                return (Err("incomplete"), 0)
            },
            difference if difference > 0 => { // multiple messages, we take only the first
                debug!("Merged upstream messages: {}", difference);
                //payload = payload[..payload.len() - difference as usize].to_vec();
                let first_message = payload[..payload.len() - difference as usize].to_vec();
                debug!("Considering only the first {} bytes. : {:?}", payload.len() - difference as usize, first_message);
                self.current_responses.insert(token, first_message);
                //self.get_responses().set()
            },
            _difference => { // equality
                self.complete_requests += 1;
                debug!("Exact upstream message!")
                // nothing to do here
                //actual_payload = payload[..].to_vec();
            }
        }
        let difference = difference as usize;

        //TODO: add check on self.RoutingMechanism
        debug!("Interpreting result {:?}", self.current_request_type);
        if !self.check_last_request(&cluster) {
            trace!("Result for initialization request: {:?}", self.get_responses());
            // current request, no 
            let response = protocol.borrow_mut().merge_responses(self, self.get_responses());
            if response.is_ok() {
                self.current_responses.remove(&token); // we remove this response
            }
            
            (response, difference)
        } else {
            trace!("Result for actual request: {:?}", self.get_responses());
            let response = match self.current_request_type {
                RequestType::RequestResponse => {
                    trace!("RRtrace: check last reponses, {}/{} - {}", self.current_responses.len(), self.current_requests.len(), self.check_last_request(cluster));
                    if self.current_requests.len() == self.complete_requests {
                    //if self.current_requests.len() == self.current_responses.len() {
                        protocol.borrow_mut().merge_responses(self, self.get_responses())

                    } else {
                        // we keep the current response
                        Err("wait") //TODO: add exception of multi RR

                    }
                },
                request_type => {
                    // FIXME: this will not work if requests and responses are not correlated, to fix in identifier based version
                    trace!("{:?}: merge response {:?}", request_type, self.get_responses());
                    protocol.borrow_mut().merge_responses(self, self.get_responses())
                }
            };
            if response.is_ok() {
                let tokens = &mut self.current_requests.keys();
                //let tokens = self.get_requests().keys();
                
                for token in tokens {
                    self.current_responses.remove(&token); // we remove this response                    
                }
                // we clear the requests if there is no more responses, and if it is not a subscribe (multiple responses)
                if self.current_responses.len() == 0 && self.current_request_type == RequestType::RequestResponse {
                    self.current_requests.clear();
                }
            }
            
            (response, difference)
        }
    }    
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_default_protocol() {
        let protocol = DefaultRoutingProtocol::new();
        
        let mut routing = RoutingTable::new();
        let mut configuration = RoutingConfiguration::new();
        configuration.type_proxy = Some(String::from("downstream"));
        configuration.add_route(
            RoutingEntry{ key: Some(String::from("tomato")), key_regex:None,  value: None, value_regex:None, cluster: String::from("tomato-cluster")}) ;        
        configuration.default_cluster = Some(String::from("tomato-cluster"));        
        routing.configure(configuration);

        let request = b"*3\r\n$3\r\nSET\r\n$7\r\ntomato1\r\n$5\r\nvalue\r\n";
        routing.register_downstream_request(Some(request.to_vec()));
        let routes = routing.route_request(Rc::clone(&protocol));
        println!("{:?}", routes);

    }

    #[test]
    fn test_perf_regex() {
        let mut routing = RoutingTable::new();
        let mut configuration = RoutingConfiguration::new();
        configuration.type_proxy = Some(String::from("downstream"));
        configuration.add_route(
            RoutingEntry{ key:Some(String::from("tomato")), key_regex:None, value:None, value_regex:None, cluster: String::from("tomato-cluster")}
        );        
        configuration.add_route(
            RoutingEntry{ key:Some(String::from("potato")), key_regex:None, value:Some(String::from("caution")), value_regex:None, cluster: String::from("tomato-cluster")}
        );    
        configuration.default_cluster = Some(String::from("potato-cluster")); 
        configuration.local_cluster = Some(String::from("potato-cluster")); 
        routing.configure(configuration);
        
        use std::time::Instant;
        let now = Instant::now();
        for _ in 0..1000000 {
            let result = routing.find_entry_by_key(b"1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890I23456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890tomato");
            assert!(result.is_some());
        }
        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);

        let now = Instant::now();
        for _ in 0..1000000 {
            let result = routing.find_entry(b"1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890I23456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890tomato", b"value");
            assert!(result.is_some());
        }
        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);

        let now = Instant::now();
        for _ in 0..1000000 {
            let result = routing.find_entry(b"1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890I23456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890tomato", b"value");
            assert!(result.is_some());
        }
        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);

        let now = Instant::now();
        for _ in 0..1000000 {
            let result = routing.find_entry(b"potato", b"caution");
            assert!(result.is_some());
        }
        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);

        let now = Instant::now();
        for _ in 0..1000000 {
            let result = routing.find_entry(b"potato", b"1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890I23456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890caution");
            assert!(result.is_some());
        }
        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);

    }
}