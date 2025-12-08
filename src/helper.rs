use std::io::Write;
use std::u32;
use std::{ collections::HashMap, time::Instant };
use std::net::TcpStream;

use crate::types;

// bool getting returned is "is_entry_id_invalid"
pub fn validate_entry_id(
    elements_array: &Vec<String>,
    map: &HashMap<String, types::ValueEntry>,
    stream: &mut TcpStream
) -> bool {
    let id = elements_array[2].clone();

    let s = &mut id.splitn(2, "-");
    let (incoming_id_one_str, incoming_id_two_str) = (s.next().unwrap(), s.next().unwrap());

    // checks for 0-0
    if incoming_id_one_str.parse::<u32>().unwrap() == 0 && incoming_id_two_str.parse::<u32>().unwrap() == 0 {
        let data_to_send = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
        let _ = stream.write_all(data_to_send.as_bytes());
        return true;
    }

    // if stream exists
    if let Some(value_entry) = map.get(&elements_array[1]) {
        match &value_entry.value {
            types::StoredValue::Stream(entry_vector) => {
                let last_entry_id = &entry_vector.last().unwrap().id;
                let s = &mut last_entry_id.splitn(2, "-");
                let (last_id_one, last_id_two) = (
                    s.next().unwrap().parse::<u32>().unwrap(),
                    s.next().unwrap().parse::<u32>().unwrap(),
                );

                // checking if time part is correct

                if incoming_id_one_str.parse::<u32>().unwrap() > last_id_one {
                    return false;
                } else if incoming_id_one_str.parse::<u32>().unwrap() == last_id_one {
                    // checking if sequence number is correct
                    if incoming_id_two_str.parse::<u32>().unwrap() <= last_id_two {
                        let data_to_send =
                            "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                        let _ = stream.write_all(data_to_send.as_bytes());
                        return true;
                    }
                    return false;
                } else {
                    let data_to_send =
                        "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                    let _ = stream.write_all(data_to_send.as_bytes());
                    return true;
                }
            }
            _ => {
                // this will never run
                return false;
            }
        }
    } else {
        // if stream doesn't exists and we have checked for 0-0 then that entry id is valid
        return false;
    }
}

// get stream related data
pub fn get_stream_related_data(elements_array: &Vec<String>) -> (String, HashMap<String, String>, String) {
    let id = elements_array[2].clone();

    let mut map_to_enter: HashMap<String, String> = HashMap::new();

    let no_of_key_value_pairs = (elements_array.len() - 3) / 2;

    for i in 0..no_of_key_value_pairs {
        map_to_enter.insert(elements_array[i + 3].clone(), elements_array[i + 4].clone());
    }

    let data_to_send = format!("${}\r\n{}\r\n", id.len(), id);

    (id, map_to_enter, data_to_send)
}

// handle expiry
pub fn handle_expiry(time_setter_args: &str, elements_array: Vec<String>) -> Option<Instant> {
    match time_setter_args.to_ascii_lowercase().as_str() {
        "px" => {
            let time = elements_array[4].parse::<u32>().unwrap(); // in milliseconds

            Some(Instant::now() + std::time::Duration::from_millis(time as u64))
        }

        "ex" => {
            let time = elements_array[4].parse::<u32>().unwrap() * 1000; // in milliseconds

            Some(Instant::now() + std::time::Duration::from_millis(time as u64))
        }

        _ => None,
    }
}

// counter must be at '$' or '*'
pub fn parse_number(bytes_received: &[u8], mut counter: usize) -> (usize, usize) {
    let mut num_in_bytes: Vec<u8> = vec![];

    match bytes_received[counter] {
        b'$' | b'*' => {
            counter += 1;
            loop {
                if bytes_received[counter] == b'\r' {
                    break;
                }
                num_in_bytes.push(bytes_received[counter]);
                counter += 1;
            }
            counter += 2;

            let number_required = String::from_utf8(num_in_bytes).unwrap().parse::<usize>().unwrap();

            (number_required, counter)
        }

        _ => {
            panic!("Didn't recieved the dollar or asterik");
        }
    }
}

// get the elements in Vec<String> from resp
pub fn parsing_elements(bytes_received: &[u8], mut counter: usize, no_of_elements: usize) -> Vec<String> {
    let mut elements_array: Vec<String> = Vec::new();

    for _ in 0..no_of_elements as u32 {
        match bytes_received[counter] {
            b'$' => {
                let no_of_elements_in_bulk_str;
                (no_of_elements_in_bulk_str, counter) = parse_number(bytes_received, counter);

                let word = &bytes_received[counter..counter + no_of_elements_in_bulk_str];

                elements_array.push(String::from_utf8_lossy(word).to_string());

                // println!("single element in elements array ==> {:?}", String::from_utf8_lossy(word));

                counter += (no_of_elements_in_bulk_str as usize) - 1;

                // to jump over \r\n to next element
                counter += 3;
            }

            _ => {}
        }
    }
    println!("logger::elements array  ==> {:?}", elements_array);

    elements_array
}

// get starting and ending indexes
pub fn get_start_and_end_indexes(elements_array: &Vec<String>) -> (u128, u128, u128, u128) {
    let star_id = elements_array[2].clone();
    let end_id = elements_array[3].clone();

    let start_id_time;
    let start_id_seq;

    let end_id_time;
    let end_id_seq;

    if star_id == "-" {
        start_id_time = 0;
        start_id_seq = 1;
    } else if star_id.contains("-") {
        let mut s = star_id.splitn(2, "-");
        (start_id_time, start_id_seq) = (
            s.next().unwrap().parse::<u128>().unwrap(),
            s.next().unwrap().parse::<u128>().unwrap(),
        );
    } else {
        start_id_time = star_id.parse::<u128>().unwrap();
        start_id_seq = 0;
    }

    if end_id == "+" {
        end_id_time = u128::MAX;
        end_id_seq = u128::MAX;
    } else if end_id.contains("-") {
        let mut s = end_id.splitn(2, "-");
        (end_id_time, end_id_seq) = (
            s.next().unwrap().parse::<u128>().unwrap(),
            s.next().unwrap().parse::<u128>().unwrap(),
        );
    } else {
        end_id_time = end_id.parse::<u128>().unwrap();
        end_id_seq = u128::MAX;
    }

    (start_id_time, start_id_seq, end_id_time, end_id_seq)
}

pub fn get_start_and_end_indexes_for_xread(start_id: &String) -> (u128, u128, u128, u128) {

    let start_id_time;
    let start_id_seq;

    let end_id_time;
    let end_id_seq;

    if start_id.contains("-") {
        let mut s = start_id.splitn(2, "-");
        (start_id_time, start_id_seq) = (
            s.next().unwrap().parse::<u128>().unwrap(),
            s.next().unwrap().parse::<u128>().unwrap(),
        );
    } else {
        start_id_time = start_id.parse::<u128>().unwrap();
        start_id_seq = 0;
    }

    end_id_time = u128::MAX;
    end_id_seq = u128::MAX;

    (start_id_time, start_id_seq, end_id_time, end_id_seq)
}
