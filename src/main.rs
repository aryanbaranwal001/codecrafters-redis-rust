use std::collections::HashMap;
use std::io::{ Read, Write };
use std::net::{ TcpListener, TcpStream };
use std::sync::{ Arc, Condvar, Mutex };
use std::{ thread };

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
                            Ok(_n) => {
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

            let mut elements_array = helper::parsing_elements(bytes_received, counter, no_of_elements);

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
                    commands::handle_xadd(&mut stream, &mut elements_array, store);
                }

                "xrange" => {
                    commands::handle_xrange(&mut stream, &mut elements_array, store);
                }

                "xread" => {
                    let map = store.lock().unwrap();

                    // will get these in u128
                    let (start_id_time, start_id_sequence, end_id_time, end_id_sequence) =
                        helper::get_start_and_end_indexes_for_xread(&elements_array);

                    let mut final_array_data_of_streams = format!("*{}\r\n", 1);

                    let mut streams_array = format!(
                        "*2\r\n${}\r\n{}\r\n",
                        &elements_array[2].len(),
                        &elements_array[2]
                    );

                    // getting the full entries array
                    // stream exists
                    if let Some(value_entry) = map.get(&elements_array[2]) {
                        match &value_entry.value {
                            types::StoredValue::Stream(entry_vector) => {
                                let filtered_data: Vec<&types::Entry> = entry_vector
                                    .iter()
                                    .filter(|e| {
                                        let mut s = e.id.splitn(2, "-");
                                        let (iteration_id_time, iteration_id_seq) = (
                                            s.next().unwrap().parse::<u128>().unwrap(),
                                            s.next().unwrap().parse::<u128>().unwrap(),
                                        );
                                        iteration_id_time <= end_id_time &&
                                            iteration_id_time >= start_id_time &&
                                            iteration_id_seq >= start_id_sequence &&
                                            iteration_id_seq <= end_id_sequence
                                    })
                                    .collect();

                                // array of array data is all the data for a particular stream
                                let mut array_of_array_data = format!("*{}\r\n", filtered_data.len());

                                for entry in filtered_data {
                                    let entry_id = &entry.id;
                                    let hashmap = &entry.map;

                                    let mut entry_array = format!("*2\r\n");
                                    let mut hashmap_array = format!("*{}\r\n", hashmap.len() * 2);

                                    for (key, value) in hashmap {
                                        let map_bulk_data = format!(
                                            "${}\r\n{}\r\n${}\r\n{}\r\n",
                                            key.len(),
                                            key,
                                            value.len(),
                                            value
                                        );
                                        println!("key value {}, {}", key, value);
                                        hashmap_array.push_str(&map_bulk_data);
                                    }

                                    entry_array.push_str(&format!("${}\r\n{}\r\n", entry_id.len(), entry_id));
                                    entry_array.push_str(&hashmap_array);

                                    array_of_array_data.push_str(&entry_array);
                                }

                                streams_array.push_str(&array_of_array_data);
                                final_array_data_of_streams.push_str(&streams_array);

                                let _ = stream.write(final_array_data_of_streams.as_bytes());
                            }
                            _ => {}
                        }

                        // stream doesn't exists
                    } else {
                        let _ = stream.write_all(b"stream with given stream key doesn't exists");
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
