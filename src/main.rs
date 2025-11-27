#![allow(unused_imports)]
use std::net::TcpListener;
use std::io::Write;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");

                let response = format!("+PONG\r\n");

                _stream.write_all(response.as_bytes()).expect("failed to send the data");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
