#![allow(unused_imports)]
use std::net::TcpListener;
use std::io::{ Write, Read };
use std::thread;

use bytes::buf;

fn main() {
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
        
                            println!("This is what is received {:?}", received);
        
                            if received == "*1\r\n$4\r\nPING" {
                                let response = format!("+PONG\r\n");
        
                                stream.write_all(response.as_bytes()).expect("failed to send the data");
                            } else {
                                stream
                                    .write_all("Go Fuck Yourself\n".as_bytes())
                                    .expect("failed to send the data");
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



