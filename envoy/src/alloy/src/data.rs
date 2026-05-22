use std::{collections::HashMap, time::SystemTime};
use std::cell::RefCell;
use log::debug;
use regex::bytes::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug)]
pub struct FetchRequestData {
    pub context_id: u32,
    //pub request: FetchRequest,
    pub correlation_id: i32,
    pub session_id: i32,
    pub session_epoch: i32,
    pub body_version: i16,
    pub partitions: Vec<FetchRequestDataPartition>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FetchRequestDataPartition {
    pub topic: String,
    pub partition: i32,
    pub fetch_offset: i64,
}

impl FetchRequestDataPartition {
    pub fn get_shared_data_key(&self) -> String {
        format!("{:?}-{:?}-{}", self.topic, self.partition, self.fetch_offset)
    }

    pub fn get_shared_queue_key(&self) -> String {
        format!("{}-{}", self.topic, self.partition)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FetchResponseData {
    pub session_id: i32,
    pub session_epoch: i32,
    pub topic: String,
    pub partition: i32,
    pub fetch_offset: i64,
    pub records: Vec<u8>,
}

impl FetchResponseData {
    pub fn get_shared_data_key(&self) -> String {
        format!("{}-{}-{}", self.topic, self.partition, self.fetch_offset)
    }
    pub fn get_shared_queue_key(&self) -> String {
        format!("{}-{}", self.topic, self.partition)
    }    
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[derive(Debug)]
pub struct FilterEntry {
    pub attribute: Option<String>,
    pub value: Option<String>,
    #[serde(skip_serializing, skip_deserializing)]
    pub value_regex: Option<Regex>,    
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[derive(Debug)]
pub struct MapAlloyFilterCriteria {
    pub alloy_filters: Option<HashMap<String, AlloyFilterCriteria>>,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone, Copy)]
pub enum FieldType {
    #[serde(rename = "VARCHAR(2147483647)")]
    String,
    Integer,
    #[serde(rename = "BIGINT")]
    Long,
}
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
pub struct Field {
    pub attribute: String,
    pub field_type: FieldType,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[derive(Debug)]
pub struct AlloyFilterCriteria {
    pub num_sources: i32,
    pub num_partitions: i32,
    pub selections: Vec<FilterEntry>,
    pub projections: Vec<String>,
    pub partition: Vec<Field>,
}

impl AlloyFilterCriteria {
    pub fn new() -> AlloyFilterCriteria {
        AlloyFilterCriteria {
            num_sources: 1,
            num_partitions: 1,
            selections: Vec::new(),
            projections: Vec::new(),
            partition: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct ConnectionInfo {
    pub response_initial_time: SystemTime,
    pub timeout: bool,
    pub request_data: FetchRequestData,
    pub response_data: Option<Vec<u8>>,
}


#[derive(Debug)]
pub enum JavaHashValue<'a> {
    Str(&'a str),
    Integer(i32),
    Long(i64),
}

impl<'a> JavaHashValue<'a> {
    fn java_hash(&self) -> i32 {
        match self {
            JavaHashValue::Str(s) => {
                let mut hash: i32 = 0;
                for c in s.chars() {
                    hash = 31i32.wrapping_mul(hash) + (c as i32) ;
                }
                hash
            }
            JavaHashValue::Integer(i) => *i,
            JavaHashValue::Long(l) => {
                let high = (*l >> 32) as i32;
                let low = *l as i32;
                high ^ low
            }
        }
    }
}

fn java_composite_hash(values: &[JavaHashValue]) -> i32 {
    values.iter().fold(0, |acc, v| acc ^ v.java_hash())
}
pub fn extract<'a>(json_value: &'a serde_json::Value, criteria: &'a AlloyFilterCriteria) -> Vec<JavaHashValue<'a>> {
    let mut fields_to_hash = Vec::new();

    for field in &criteria.partition {
        if let Some(value) = json_value.get(&field.attribute) {
            // Insert the attribute into the new JSON map
            let hashable_value = match field.field_type {
                FieldType::String => JavaHashValue::Str(value.as_str().unwrap_or("")),
                FieldType::Integer => {
                    // Extract as i64 then convert to i32
                    JavaHashValue::Integer(value.as_i64().unwrap_or(0) as i32)
                }
                FieldType::Long => {
                    JavaHashValue::Long(value.as_i64().unwrap_or(0))
                }
            };
            fields_to_hash.push(hashable_value);
        }
    }
    fields_to_hash
}

pub fn extract_and_hash(json_value: &serde_json::Value, criteria: &AlloyFilterCriteria) -> (i32, serde_json::Value) {
    let mut fields_to_hash = Vec::new();
    let mut new_json_map = serde_json::Map::new();

    for field in &criteria.partition {
        if let Some(value) = json_value.get(&field.attribute) {
            // Insert the attribute into the new JSON map
            new_json_map.insert(field.attribute.clone(), value.clone());

            let hashable_value = match field.field_type {
                FieldType::String => JavaHashValue::Str(value.as_str().unwrap_or("")),
                FieldType::Integer => {
                    // Extract as i64 then convert to i32
                    JavaHashValue::Integer(value.as_i64().unwrap_or(0) as i32)
                }
                FieldType::Long => {
                    JavaHashValue::Long(value.as_i64().unwrap_or(0))
                }
            };
            fields_to_hash.push(hashable_value);
        }
    }

    let hash_value = java_composite_hash(&fields_to_hash);
    (hash_value, serde_json::Value::Object(new_json_map))
}

// ----------
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
    code = bit_mix(code);

    if code >= 0 {
        code
    } else if code != i32::MIN {
        -code
    } else {
        0
    }
}
pub fn get_keygroup(v: &[JavaHashValue], max_parallelism: i32) -> i32 {
    let hash = java_composite_hash(v);
    let h = murmur_hash(hash);
    h % max_parallelism
}

// {TODO: check in Flink}
pub fn get_keygroup_slot(v:  &[JavaHashValue], parallelism: i32, max_parallelism: i32) -> i32 {
    let kg = get_keygroup(v, max_parallelism);
    debug!("v {:?} - keygroup {:?}", v, kg);
    kg * parallelism / max_parallelism
}

pub fn get_split_owner(topic: &str, partition: i32, num_readers: i32) -> i32 {
    let hash_code = JavaHashValue::Str(topic).java_hash();
    let start_index = (hash_code.wrapping_mul(31) & 0x7FFFFFFF) as i32 % num_readers;
    debug!("get_split_owner (start_index = {:?}, partition = {:?}, num_readers ={:?}", start_index, partition, num_readers);
    (start_index + partition) % num_readers
}

// compute original partition id for virtual partition
pub fn get_partition_original_id(num_sources: i32, num_partitions: i32, virtual_partition: i32) -> i32 {
    //let mut p = 0;
    let mut s = 0;
    let mut map_partitions: HashMap<i32, i32> = HashMap::new();
    for vp in 0..(num_sources * num_partitions)  {

        let p = match map_partitions.get(&s) {
            Some(p) => {
                (p.clone() + 1) % num_partitions
            },
            None => s % num_partitions // if not existing (regular set current)
        };
        
        if vp == virtual_partition {
            return p
        }
        map_partitions.insert(s, p);
        s = (s + 1) % num_sources;
        
    }
    return -1
}

thread_local! {
    pub static WAITING_CONTEXTS: RefCell<HashMap<u32, ConnectionInfo>> = RefCell::new(HashMap::new());
}