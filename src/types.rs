use clap::Parser;
use ordered_float::OrderedFloat;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Condvar, Mutex};

#[derive(Debug)]
pub struct ZSet {
    // stores actual value
    pub scores: HashMap<String, f64>,

    // store for ordering purposes
    pub ordered: BTreeSet<(OrderedFloat<f64>, String)>,
}

impl ZSet {
    pub fn new() -> Self {
        Self {
            scores: HashMap::new(),
            ordered: BTreeSet::new(),
        }
    }
}

pub struct UserInfo {
    pub name: String,
    pub is_authenticated: bool,
}

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long)]
    pub port: Option<u32>,
    #[arg(short, long)]
    pub replicaof: Option<String>,
    #[arg(long)]
    pub dir: Option<String>,
    #[arg(long)]
    pub dbfilename: Option<String>,
}

#[derive(Debug)]
pub enum StoredValue {
    String(String),
    Stream(Vec<Entry>),
}

#[derive(Debug)]
pub struct Entry {
    pub id: String,
    pub map: HashMap<String, String>,
}

#[derive(Debug)]
pub struct ValueEntry {
    pub value: StoredValue,
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

pub type SharedStore = Arc<(Mutex<HashMap<String, ValueEntry>>, Condvar)>;
pub type SharedMainList = Arc<(Mutex<Vec<List>>, Condvar)>;
