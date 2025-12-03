use std::collections::HashMap;
use std::sync::{ Arc, Mutex };

#[derive(Debug)]
pub struct ValueEntry {
    pub value: String,
    pub expires_at: Option<std::time::Instant>, // None = no expiry
}

pub type SharedStore = Arc<Mutex<HashMap<String, ValueEntry>>>;
