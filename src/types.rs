use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct ValueEntry {
    pub value: String,
    pub expires_at: Option<std::time::Instant>, // None = no expiry
}

#[derive(Debug)]
pub struct List {
    pub name: String,
    pub vec: Vec<String>,
}

impl List {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            vec: Vec::new(),
        }
    }
}

pub type SharedStore = Arc<Mutex<HashMap<String, ValueEntry>>>;
pub type SharedMainList = Arc<Mutex<Vec<List>>>;
