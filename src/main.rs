use std::collections::HashMap;
use std::io::{ Read, Write };
use std::net::{ TcpListener, TcpStream };
use std::sync::{ Arc, Condvar, Mutex };
use std::thread;
use std::time::Instant;

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

    let (main_list_guard, cvar) = &**main_list_store;

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
                    let mut map = store.lock().unwrap();

                    let mut expires_at = None;

                    if let Some(time_setter_args) = elements_array.get(3) {
                        expires_at = helper::handle_expiry(time_setter_args, elements_array.clone());
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
                    let mut map = store.lock().unwrap();

                    if let Some(val) = map.get(&elements_array[1]) {
                        match val.expires_at {
                            Some(expire_time) => {
                                if Instant::now() >= expire_time {
                                    map.remove(&elements_array[1]);
                                    let _ = stream.write_all("$-1\r\n".as_bytes());
                                } else {
                                    println!("value, word {:?}", val.value);

                                    let value_to_send = format!("${}\r\n{}\r\n", val.value.as_bytes().len(), val.value);
                                    println!("value to send {:?}", value_to_send);
                                    let _ = stream.write_all(value_to_send.as_bytes());
                                }
                            }
                            None => {
                                let value_to_send = format!("${}\r\n{}\r\n", val.value.as_bytes().len(), val.value);
                                println!("value to send {:?}", value_to_send);
                                let _ = stream.write_all(value_to_send.as_bytes());
                            }
                        }
                    } else {
                        println!("Key Doesn't exists");
                    }
                }

                "rpush" => {
                    println!("rpush getting triggered");

                    let mut main_list = main_list_guard.lock().unwrap();

                    if let Some(inner) = main_list.iter_mut().find(|inner| inner.name == elements_array[1]) {
                        for i in 2..elements_array.len() {
                            inner.vec.push(elements_array[i].clone());
                        }

                        println!("no of elements in list {}", inner.vec.len());

                        let data_to_send = format!(":{}\r\n", inner.vec.len());

                        let _ = stream.write_all(data_to_send.as_bytes());
                    } else {
                        let mut new_list = types::List::new(elements_array[1].as_str());

                        for i in 2..elements_array.len() {
                            new_list.vec.push(elements_array[i].clone());
                        }

                        let data_to_send = format!(":{}\r\n", new_list.vec.len());
                        let _ = stream.write_all(data_to_send.as_bytes());
                        main_list.push(new_list);
                    }
                    cvar.notify_one();

                    println!("logger::main_list => {:?}", main_list);
                }

                "lpush" => {
                    let mut main_list = main_list_guard.lock().unwrap();

                    if let Some(inner) = main_list.iter_mut().find(|inner| inner.name == elements_array[1]) {
                        for i in 2..elements_array.len() {
                            inner.vec.insert(0, elements_array[i].clone());
                        }

                        println!("no of elements in list {}", inner.vec.len());

                        let data_to_send = format!(":{}\r\n", inner.vec.len());

                        let _ = stream.write_all(data_to_send.as_bytes());
                    } else {
                        let mut new_list = types::List::new(elements_array[1].as_str());

                        for i in 2..elements_array.len() {
                            new_list.vec.insert(0, elements_array[i].clone());
                        }

                        let data_to_send = format!(":{}\r\n", new_list.vec.len());
                        let _ = stream.write_all(data_to_send.as_bytes());
                        main_list.push(new_list);
                    }

                    println!("logger::main_list => {:?}", main_list);
                }

                "lrange" => {
                    let mut main_list = main_list_guard.lock().unwrap();

                    if let Some(inner) = main_list.iter_mut().find(|inner| inner.name == elements_array[1]) {
                        let mut starting_index = elements_array[2].parse::<i32>().unwrap();
                        let mut ending_index = elements_array[3].parse::<i32>().unwrap();

                        let length_of_list = inner.vec.len();

                        // checking if starting_index and ending_index are all valid

                        println!(
                            "starting index {}, ending index  {}, length of list {}",
                            starting_index,
                            ending_index,
                            length_of_list
                        );

                        if starting_index < 0 {
                            if starting_index < -1 * (length_of_list as i32) {
                                starting_index = 0;
                            } else {
                                starting_index =
                                    (length_of_list as i32) + -1 * ((-1 * starting_index) % (length_of_list as i32));
                            }
                        }

                        if ending_index < 0 {
                            if ending_index < -1 * (length_of_list as i32) {
                                ending_index = 0;
                            } else {
                                ending_index =
                                    (length_of_list as i32) + -1 * ((-1 * ending_index) % (length_of_list as i32));
                            }
                        }

                        println!("starting index {}, ending index  {}", starting_index, ending_index);

                        if starting_index >= (length_of_list as i32) {
                            let _ = stream.write_all(b"*0\r\n");
                        } else if starting_index > ending_index {
                            let _ = stream.write_all(b"*0\r\n");
                        } else {
                            if ending_index >= (length_of_list as i32) {
                                ending_index = (length_of_list as i32) - 1;
                            }

                            let mut vec_to_send: Vec<String> = Vec::new();

                            for i in starting_index..ending_index + 1 {
                                vec_to_send.push(inner.vec[i as usize].clone());
                            }

                            let mut string_to_send = format!("*{}\r\n", vec_to_send.len());

                            let mut tail_string = String::new();

                            for word in vec_to_send {
                                let tail_part = format!("${}\r\n{}\r\n", word.len(), word);
                                tail_string.push_str(&tail_part);
                            }

                            print!("String to send {}", string_to_send);

                            string_to_send.push_str(&tail_string);

                            let _ = stream.write_all(string_to_send.as_bytes());
                        }
                    } else {
                        // list doesn't exists
                        let _ = stream.write_all(b"*0\r\n");
                    }
                }

                "llen" => {
                    let mut main_list = main_list_guard.lock().unwrap();

                    if let Some(inner) = main_list.iter_mut().find(|inner| inner.name == elements_array[1]) {
                        let data_to_send = format!(":{}\r\n", inner.vec.len());

                        let _ = stream.write_all(data_to_send.as_bytes());
                    } else {
                        let _ = stream.write_all(b":0\r\n");
                    }
                }

                "lpop" => {
                    let mut main_list = main_list_guard.lock().unwrap();

                    if let Some(inner) = main_list.iter_mut().find(|inner| inner.name == elements_array[1]) {
                        if let Some(_) = elements_array.get(2) {
                            let mut no_of_elements_to_remove = elements_array[2].parse::<usize>().unwrap();

                            if no_of_elements_to_remove > inner.vec.len() {
                                no_of_elements_to_remove = inner.vec.len();
                            }

                            let mut data_to_send: String = String::new();
                            data_to_send.push_str(&format!("*{}\r\n", no_of_elements_to_remove));

                            for _ in 0..no_of_elements_to_remove {
                                let first_element = inner.vec.remove(0);
                                data_to_send.push_str(&format!("${}\r\n{}\r\n", first_element.len(), first_element));
                            }

                            let _ = stream.write_all(data_to_send.as_bytes());
                        } else {
                            let first_element = inner.vec.remove(0);

                            let data_to_send = format!("${}\r\n{}\r\n", first_element.len(), first_element);

                            let _ = stream.write_all(data_to_send.as_bytes());
                        }
                    } else {
                        let _ = stream.write_all(b"$-1\r\n");
                    }
                }

                "blpop" => {
                    let mut main_list = main_list_guard.lock().unwrap();

                    let mut _is_empty: bool = false;
                    loop {
                        if let Some(inner) = main_list.iter_mut().find(|inner| inner.name == elements_array[1]) {
                            if inner.vec.is_empty() {
                                _is_empty = true;
                            } else {
                                let first_element = inner.vec.remove(0);

                                let data_to_send = format!(
                                    "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                                    inner.name.len(),
                                    inner.name,
                                    first_element.len(),
                                    first_element
                                );

                                let _ = stream.write_all(data_to_send.as_bytes());
                                break;
                            }
                        } else {
                            _is_empty = true;
                        }

                        if _is_empty {
                            println!("waiting for vec to get filled");
                            main_list = cvar.wait(main_list).unwrap();
                            _is_empty = false;
                        }
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
