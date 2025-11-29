#![allow(unused_imports)]
use std::net::{ TcpListener, TcpStream };
use std::io::{ Write, Read };
use std::thread;
use std::str;
use tracing::{ info, warn, error };
use tracing_subscriber;
use tracing_subscriber::fmt::format;
use std::collections::HashMap;
use std::sync::{ Arc, Mutex };
use bytes::buf;

type SharedStore = Arc<Mutex<HashMap<String, String>>>;

fn main() {
    tracing_subscriber::fmt::init();
    let store: SharedStore = Arc::new(Mutex::new(HashMap::new()));

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for result_stream in listener.incoming() {
        let store_clone = store.clone();

        let _ = thread::spawn(move || {
            match result_stream {
                Ok(mut stream) => {
                    println!("accepted new connection");

                    let mut buffer = [0; 512];

                    loop {
                        let bytes_read = stream.read(&mut buffer).unwrap();

                        let received = String::from_utf8_lossy(&buffer[..bytes_read])
                            .trim()
                            .to_owned();

                        println!("received string ==> {:?}", received);

                        if received == "" {
                            break;
                        }

                        stream = parse_and_operate(&received, stream, &store_clone);
                    }
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        });
    }
}

fn parse_and_operate(received: &String, mut stream: TcpStream, store: &SharedStore) -> TcpStream {
    let mut map = store.lock().unwrap();

    let mut counter = 0;

    let bytes_received = received.as_bytes();

    if bytes_received[counter] == b'*' {
        counter += 1;
        let no_of_elements = bytes_received[counter] - b'0';
        counter += 3;

        println!("no of elements in resp array ==> {}", no_of_elements);

        let mut elements_array: Vec<String> = Vec::new();

        for _ in 0..no_of_elements {
            if bytes_received[counter] == b'$' {
                // now counter is at number after the dollar
                counter += 1;

                let no_of_elements_in_bulk_str = bytes_received[counter] - b'0';

                // now counter is at word's first letter
                counter += 3;

                let word =
                    &bytes_received[counter..counter + (no_of_elements_in_bulk_str as usize)];

                elements_array.push(String::from_utf8_lossy(word).to_string());

                counter += (no_of_elements_in_bulk_str as usize) - 1;

                // to jump over \r\n
                counter += 3;
            }
        }

        println!("elements array after for loop ==> {:?}", elements_array);

        if elements_array[0].eq_ignore_ascii_case("echo") {
            println!("elements array ==> {:?}", elements_array);
            let bulk_str_to_send = format!(
                "${}\r\n{}\r\n",
                elements_array[1].as_bytes().len(),
                elements_array[1]
            );
            let _ = stream.write_all(bulk_str_to_send.as_bytes());
        } else if elements_array[0].eq_ignore_ascii_case("ping") {
            let _ = stream.write_all("+PONG\r\n".as_bytes());
        } else if elements_array[0].eq_ignore_ascii_case("set") {
            map.insert(elements_array[1].clone(), elements_array[2].clone());

            println!("map ==> {:?}", map);

            let _ = stream.write_all("+OK\r\n".as_bytes());
        } else if elements_array[0].eq_ignore_ascii_case("get") {
            if let Some(val) = map.get(&elements_array[1]) {
                let value_to_send = format!("${}\r\n{}\r\n", val.as_bytes().len(), val);
                let _ = stream.write_all(value_to_send.as_bytes());
            } else {
                println!("Key Doesn't exists");
            }
        }

        // use match expression here
    }

    stream
}
