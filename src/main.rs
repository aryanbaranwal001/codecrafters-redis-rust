#![allow(unused_imports)]
use std::net::{ TcpListener, TcpStream };
use std::io::{ Write, Read };
use std::thread;
use std::str;
use tracing::{ info, warn, error };
use tracing_subscriber;

use bytes::buf;

fn main() {
    tracing_subscriber::fmt::init();

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for result_stream in listener.incoming() {
        let _ = thread::spawn(|| {
            match result_stream {
                Ok(mut stream) => {
                    println!("accepted new connection");

                    let mut buffer = [0; 512];

                    loop {
                        let bytes_read = stream.read(&mut buffer).unwrap();

                        let received = String::from_utf8_lossy(&buffer[..bytes_read])
                            .trim()
                            .to_owned();

                        info!("received string ==> {:?}", received);

                        stream = parse_and_operate(&received, stream);
                    }
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        });
    }
}

fn parse_and_operate(received: &String, mut stream: TcpStream) -> TcpStream {
    let mut counter = 0;

    let bytes_received = received.as_bytes();

    if bytes_received[counter] == b'*' {
        counter += 1;
        let no_of_elements = bytes_received[counter] - b'0';
        counter += 3;

        info!("no of elements in resp array ==> {}", no_of_elements);

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

        
        if elements_array[0].eq_ignore_ascii_case("echo") {

            info!("elements array ==> {:?}", elements_array);
            let bulk_str_to_send = format!("${}\r\n{}\r\n", elements_array[1].as_bytes().len(), elements_array[1]);
            let _ = stream.write_all(bulk_str_to_send.as_bytes());
        
        } else if elements_array[0].eq_ignore_ascii_case("ping") {
        
            let _ = stream.write_all("+PONG\r\n".as_bytes());
        
        }
    }

    stream
}
