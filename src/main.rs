use std::net::{ TcpListener, TcpStream };
use std::io::{ Write, Read };
use std::thread;
use std::collections::HashMap;
use std::time::Instant;
use std::sync::{ Arc, Mutex };

mod types;
mod helper;

fn main() {
    let store: types::SharedStore = Arc::new(Mutex::new(HashMap::new()));

    let main_list: types::SharedMainList = Arc::new(Mutex::new(Vec::new()));

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

                                stream = handle_connection(
                                    &buffer,
                                    stream,
                                    &store_clone,
                                    &main_list_clone
                                );
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

    let mut map = store.lock().unwrap();
    let mut main_list = main_list_store.lock().unwrap();

    match bytes_received[counter] {
        b'*' => {
            let no_of_elements;
            (no_of_elements, counter) = helper::parse_number(bytes_received, counter);

            // println!("logger::no of elements in resp array ==> {}", no_of_elements);

            let elements_array = helper::parsing_elements(bytes_received, counter, no_of_elements);

            match elements_array[0].to_ascii_lowercase().as_str() {
                "echo" => {
                    println!("elements array ==> {:?}", elements_array);
                    let bulk_str_to_send = format!(
                        "${}\r\n{}\r\n",
                        elements_array[1].as_bytes().len(),
                        elements_array[1]
                    );
                    let _ = stream.write_all(bulk_str_to_send.as_bytes());
                }

                "ping" => {
                    let _ = stream.write_all("+PONG\r\n".as_bytes());
                }

                "set" => {
                    let mut expires_at = None;

                    if let Some(time_setter_args) = elements_array.get(3) {
                        expires_at = helper::handle_expiry(
                            time_setter_args,
                            elements_array.clone()
                        );
                    }

                    let value_entry = types::ValueEntry {
                        value: elements_array[2].clone(),
                        expires_at,
                    };

                    map.insert(elements_array[1].clone(), value_entry);

                    println!("logger::map ==> {:?}", map);

                    let _ = stream.write_all("+OK\r\n".as_bytes());
                }

                "get" => {
                    if let Some(val) = map.get(&elements_array[1]) {
                        match val.expires_at {
                            Some(expire_time) => {
                                if Instant::now() >= expire_time {
                                    map.remove(&elements_array[1]);
                                    let _ = stream.write_all("$-1\r\n".as_bytes());
                                } else {
                                    println!("value, word {:?}", val.value);

                                    let value_to_send = format!(
                                        "${}\r\n{}\r\n",
                                        val.value.as_bytes().len(),
                                        val.value
                                    );
                                    println!("value to send {:?}", value_to_send);
                                    let _ = stream.write_all(value_to_send.as_bytes());
                                }
                            }
                            None => {
                                let value_to_send = format!(
                                    "${}\r\n{}\r\n",
                                    val.value.as_bytes().len(),
                                    val.value
                                );
                                println!("value to send {:?}", value_to_send);
                                let _ = stream.write_all(value_to_send.as_bytes());
                            }
                        }
                    } else {
                        println!("Key Doesn't exists");
                    }
                }

                "rpush" => {
                    if
                        let Some(inner) = main_list
                            .iter_mut()
                            .find(|inner| inner.name == elements_array[1])
                    {
                        inner.vec.push(elements_array[2].clone());

                        println!("no of elements in list {}", inner.vec.len());

                        let data_to_send = format!(":{}\r\n", inner.vec.len());

                        let _ = stream.write_all(data_to_send.as_bytes());
                    } else {
                        let mut new_list = types::List::new(elements_array[1].as_str());
                        new_list.vec.push(elements_array[2].clone());

                        let data_to_send = format!(":{}\r\n", new_list.vec.len());
                        let _ = stream.write_all(data_to_send.as_bytes());
                        main_list.push(new_list);
                    }

                    println!("logger::main_list => {:?}", main_list);
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
