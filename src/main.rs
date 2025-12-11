use std::collections::HashMap;
use std::io::{ Read, Write };
use std::net::{ TcpListener, TcpStream };
use std::sync::{ Arc, Condvar, Mutex };
use std::{ thread };
use clap::Parser;

mod commands;
mod helper;
mod types;

fn main() {
    let args = types::Args::parse();
    let port;
    let role;

    println!("--------------------------------------------");

    let store: types::SharedStore = Arc::new((Mutex::new(HashMap::new()), Condvar::new()));
    let main_list: types::SharedMainList = Arc::new((Mutex::new(Vec::new()), Condvar::new()));

    println!("[INFO] {:?}", args);

    if let Some(port_num) = args.port {
        port = format!("{}", port_num);
        println!("[INFO] provided with port number: {}", port);
    } else {
        port = "6379".to_string();
        println!("[INFO] not provided with port number, starting with default: 6379");
    }

    if let Some(replicaof_string) = args.replicaof {
        role = "role:slave";

        // Trying to connect to master server
        let mut s = replicaof_string.splitn(2, " ");
        let (master_url, marter_port) = (s.next().unwrap(), s.next().unwrap());
        let master_url_with_port = format!("{}:{}", master_url, marter_port);

        println!("[INFO] trying to connect with master with link: {}", master_url_with_port);

        let mut stream = TcpStream::connect(master_url_with_port).unwrap();

        let replconf_second: &str = &format!("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{}\r\n", port);

        let commands_to_send: [(&str, Option<&str>); 4] = [
            ("*1\r\n$4\r\nPING\r\n", Some("+PONG\r\n")),
            (replconf_second, Some("+OK\r\n")),
            ("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n", Some("+OK\r\n")),
            ("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n", None),
        ];

        for i in 0..commands_to_send.len() {
            let is_expected_response = helper::send_and_validate(
                &mut stream,
                commands_to_send[i].0,
                commands_to_send[i].1
            );

            if !is_expected_response {
                panic!("[DEBUG] exp_resp doesn't equal actual_resp");
            }
        }

        println!("[INFO] sent all the commands");
    } else {
        role = "role:master";
    }

    let link = format!("127.0.0.1:{}", port);

    println!("[INFO] starting server with port number: {}", port);
    let listener = TcpListener::bind(link).unwrap();

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
                                stream = handle_connection(&buffer, stream, &store_clone, &main_list_clone, role);
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
    main_list_store: &types::SharedMainList,
    role: &str
) -> TcpStream {
    let mut counter = 0;

    match bytes_received[counter] {
        b'*' => {
            let no_of_elements;
            (no_of_elements, counter) = helper::parse_number(bytes_received, counter);

            let mut elements_array = helper::parsing_elements(bytes_received, counter, no_of_elements);

            match elements_array[0].to_ascii_lowercase().as_str() {
                "echo" => {
                    let response = commands::handle_echo(elements_array);
                    let _ = stream.write_all(response.as_bytes());
                }

                "ping" => {
                    let _ = stream.write_all("+PONG\r\n".as_bytes());
                }

                "set" => {
                    let response = commands::handle_set(elements_array, store);
                    let _ = stream.write_all(response.as_bytes());
                }

                "get" => {
                    let response = commands::handle_get(elements_array, store);
                    let _ = stream.write_all(response.as_bytes());
                    println!("[DEBUG] xadd response: {:?}", response);
                }

                "rpush" => {
                    let response = commands::handle_rpush(elements_array, main_list_store);
                    let _ = stream.write_all(response.as_bytes());
                }

                "lpush" => {
                    let response = commands::handle_lpush(elements_array, main_list_store);
                    let _ = stream.write_all(response.as_bytes());
                }

                "lrange" => {
                    let response = commands::handle_lrange(elements_array, main_list_store);
                    let _ = stream.write_all(response.as_bytes());
                }

                "llen" => {
                    let response = commands::handle_llen(elements_array, main_list_store);
                    let _ = stream.write_all(response.as_bytes());
                }

                "lpop" => {
                    let response = commands::handle_lpop(elements_array, main_list_store);
                    let _ = stream.write_all(response.as_bytes());
                }

                "blpop" => {
                    let response = commands::handle_blpop(elements_array, main_list_store);
                    let _ = stream.write_all(response.as_bytes());
                }

                "type" => {
                    let response = commands::handle_type(elements_array, store);
                    let _ = stream.write_all(response.as_bytes());
                }

                "xadd" => {
                    let response = commands::handle_xadd(&mut elements_array, store);
                    let _ = stream.write_all(response.as_bytes());
                }

                "xrange" => {
                    let response = commands::handle_xrange(&mut elements_array, store);
                    let _ = stream.write_all(response.as_bytes());
                }

                "xread" => {
                    let response = commands::handle_xread(&mut elements_array, store);
                    let _ = stream.write_all(response.as_bytes());
                }

                "incr" => {
                    let response = commands::handle_incr(&mut elements_array, store);
                    let _ = stream.write_all(response.as_bytes());
                }

                "multi" => {
                    commands::handle_multi(&mut stream, store, main_list_store);
                }

                // discard without multi
                "discard" => {
                    let _ = stream.write_all(b"-ERR DISCARD without MULTI\r\n");
                }

                // exec without multi
                "exec" => {
                    let _ = stream.write_all(b"-ERR EXEC without MULTI\r\n");
                }

                "info" => {
                    if elements_array[1] == "replication" {
                        let data =
                            format!("role:{}\r\nmaster_repl_offset:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", role);

                        let response = format!("${}\r\n{}\r\n", data.len(), data);

                        let _ = stream.write_all(response.as_bytes());
                    }
                }

                "replconf" => {
                    println!("[INFO]: replconf {:?}", elements_array);

                    let _ = stream.write_all(b"+OK\r\n");
                }

                "psync" => {
                    let _ = stream.write_all(b"+FULLRESYNC <REPL_ID> 0\r\n");
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
