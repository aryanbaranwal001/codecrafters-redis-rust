use std::collections::HashMap;
use std::io::{ Read, Write };
use std::net::{ TcpListener, TcpStream };
use std::sync::{ Arc, Condvar, Mutex };
use std::thread;

mod commands;
mod helper;
mod types;

fn main() {
    let store: types::SharedStore = Arc::new(Mutex::new(HashMap::new()));
    let main_list: types::SharedMainList = Arc::new((Mutex::new(Vec::new()), Condvar::new()));

    println!("------------------------------------------------");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for connection in listener.incoming() {
        // cloning store for each thread
        let store_clone = store.clone();
        let main_list_clone = main_list.clone();

        let _ = thread::spawn(move || {
            match connection {
                Ok(mut stream) => {
                    println!("accepted new connection");

                    let mut buffer = [0; 512];

                    loop {
                        match stream.read(&mut buffer) {
                            Ok(0) => {
                                println!("client disconnected");
                                return;
                            }
                            Ok(n) => {
                                let received = String::from_utf8_lossy(&buffer[..n])
                                    .trim()
                                    .to_owned();

                                println!("logger::received string ==> {:?}", received);

                                stream = handle_connection(&buffer, stream, &store_clone, &main_list_clone);
                            }
                            Err(e) => {
                                println!("error reading stream: {e}");
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        });
    }
}

fn handle_connection(
    bytes_received: &[u8],
    mut stream: TcpStream,
    store: &types::SharedStore,
    main_list_store: &types::SharedMainList
) -> TcpStream {
    let mut counter = 0;

    match bytes_received[counter] {
        b'*' => {
            let no_of_elements;
            (no_of_elements, counter) = helper::parse_number(bytes_received, counter);

            // println!("logger::no of elements in resp array ==> {}", no_of_elements);

            let elements_array = helper::parsing_elements(bytes_received, counter, no_of_elements);

            match elements_array[0].to_ascii_lowercase().as_str() {
                "echo" => {
                    commands::handle_echo(&mut stream, elements_array);
                }

                "ping" => {
                    let _ = stream.write_all("+PONG\r\n".as_bytes());
                }

                "set" => {
                    commands::handle_set(&mut stream, elements_array, store);
                }

                "get" => {
                    commands::handle_get(&mut stream, elements_array, store);
                }

                "rpush" => {
                    commands::handle_rpush(&mut stream, elements_array, main_list_store);
                }

                "lpush" => {
                    commands::handle_lpush(&mut stream, elements_array, main_list_store);
                }

                "lrange" => {
                    commands::handle_lrange(&mut stream, elements_array, main_list_store);
                }

                "llen" => {
                    commands::handle_llen(&mut stream, elements_array, main_list_store);
                }

                "lpop" => {
                    commands::handle_lpop(&mut stream, elements_array, main_list_store);
                }

                "blpop" => {
                    commands::handle_blpop(&mut stream, elements_array, main_list_store);
                }

                "type" => {
                    commands::handle_type(&mut stream, elements_array, store);
                }

                "xadd" => {
                    let mut map = store.lock().unwrap();

                    if let Some(value_entry) = map.get_mut(&elements_array[1]) {
                        // getting the key value pairs and pushing to Stream vec as entry

                        let id = elements_array[2].clone();

                        let mut map_to_enter: HashMap<String, String> = HashMap::new();

                        let no_of_key_value_pairs = (elements_array.len() - 3) / 2;

                        for i in 0..no_of_key_value_pairs {
                            map_to_enter.insert(elements_array[i + 2].clone(), elements_array[i + 3].clone());
                        }

                        let data_to_send = format!("*{}\r\n{}\r\n", id.len(), id);

                        match &mut value_entry.value {
                            types::StoredValue::Stream(entry_vector) => {
                                entry_vector.push(types::Entry {
                                    id,
                                    map: map_to_enter,
                                });
                            }
                            _ => {}
                        }
                        let _ = stream.write_all(data_to_send.as_bytes());
                    } else {
                        let (id, map_to_enter, data_to_send) = helper::get_stream_related_data(&elements_array);

                        let mut vec_to_move: Vec<types::Entry> = Vec::new();

                        vec_to_move.push(types::Entry {
                            id,
                            map: map_to_enter,
                        });

                        // creates a new stream
                        map.insert(elements_array[1].to_string(), types::ValueEntry {
                            value: types::StoredValue::Stream(vec_to_move),
                            expires_at: None,
                        });
                        let _ = stream.write_all(data_to_send.as_bytes());
                    }
                }
                _ => {
                    let _ = stream.write_all("Not a valid command".as_bytes());
                }
            }
        }

        _ => {
            let _ = stream.write_all(b"invalid resp string");
        }
    }

    stream
}
