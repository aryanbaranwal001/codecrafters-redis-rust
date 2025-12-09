use std::collections::HashMap;
use std::io::{ Read, Write };
use std::net::{ TcpListener, TcpStream };
use std::sync::{ Arc, Condvar, Mutex };
use std::{ thread };

mod commands;
mod helper;
mod types;

fn main() {
    let store: types::SharedStore = Arc::new((Mutex::new(HashMap::new()), Condvar::new()));
    let main_list: types::SharedMainList = Arc::new((Mutex::new(Vec::new()), Condvar::new()));

    println!("--------------------------------------------");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for connection in listener.incoming() {
        let store_clone = Arc::clone(&store);
        let main_list_clone = Arc::clone(&main_list);

        thread::spawn(move || {
            match connection {
                Ok(mut stream) => {
                    println!("[INFO] accepted new connection");

                    let mut buffer = [0; 512];

                    loop {
                        match stream.read(&mut buffer) {
                            Ok(0) => {
                                println!("[INFO] client disconnected");
                                return;
                            }
                            Ok(_n) => {
                                stream = handle_connection(&buffer, stream, &store_clone, &main_list_clone);
                            }
                            Err(e) => {
                                println!("[ERROR] error reading stream: {e}");
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("[ERROR] error making connection: {e}");
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
                    commands::handle_xread(&mut stream, &mut elements_array, store);
                }

                "incr" => {
                    commands::handle_incr(&mut stream, &mut elements_array, store);
                }

                "multi" => {
                    let mut multi_cmd_buffer = [0; 512];
                    let _ = stream.write_all(b"+OK\r\n");

                    let mut vector_of_commands: Vec<Vec<String>> = Vec::new();

                    loop {
                        match stream.read(&mut multi_cmd_buffer) {
                            Ok(0) => {
                                println!("[INFO] connection disconnected");
                            }
                            Ok(_n) => {
                                let mut counter = 0;
                                let no_of_elements;

                                (no_of_elements, counter) = helper::parse_number(&multi_cmd_buffer, counter);

                                let cmd_array = helper::parsing_elements(&multi_cmd_buffer, counter, no_of_elements);

                                if cmd_array[0].to_ascii_lowercase() == "exec" {
                                    helper::handle_exec_under_multi(&vector_of_commands, &mut stream);
                                    break;
                                }

                                vector_of_commands.push(cmd_array);

                                let _ = stream.write_all(b"+QUEUED\r\n");
                            }
                            Err(e) => {
                                println!("[ERROR] error reading stream: {e}");
                            }
                        }
                    }
                }

                // exec without multi
                "exec" => {
                    let _ = stream.write_all(b"-ERR EXEC without MULTI\r\n");
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
