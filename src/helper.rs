use std::{ collections::HashMap, time::Instant };

use crate::types;

pub fn validate_entry_id(elements_array: &Vec<String>, map: &HashMap<String, types::ValueEntry>) -> (bool, bool) {
    let id = elements_array[2].clone();

    let mut is_incoming_id_invalid: bool = false;
    let is_special_invalid_case_true: bool = false;

    // checking for 0-0
    let incoming_id_parts: Vec<&str> = id.split("-").collect();

    if incoming_id_parts[0].parse::<u32>().unwrap() == 0 && incoming_id_parts[1].parse::<u32>().unwrap() == 0 {
        return (false, true);
    }

    // if stream exists
    if let Some(value_entry) = map.get(&elements_array[1]) {
        match &value_entry.value {
            types::StoredValue::Stream(entry_vector) => {
                let last_entry_id = &entry_vector.last().unwrap().id;
                let last_id_parts: Vec<&str> = last_entry_id.split("-").collect();
                let incoming_id_parts: Vec<&str> = id.split("-").collect();

                if incoming_id_parts[0].parse::<u32>().unwrap() > last_id_parts[0].parse::<u32>().unwrap() {
                    is_incoming_id_invalid = false;
                } else if incoming_id_parts[0].parse::<u32>().unwrap() == last_id_parts[0].parse::<u32>().unwrap() {
                    if incoming_id_parts[1].parse::<u32>().unwrap() > last_id_parts[1].parse::<u32>().unwrap() {
                        is_incoming_id_invalid = false;
                    } else {
                        println!(
                            "incomign id second {}, last id second {}",
                            incoming_id_parts[1].parse::<u32>().unwrap(),
                            last_id_parts[1].parse::<u32>().unwrap()
                        );
                        is_incoming_id_invalid = true;
                    }
                } else {
                    is_incoming_id_invalid = true;
                }
            }
            _ => {}
        }
    }

    println!("is incoming id valid {}, is special case true {}", is_incoming_id_invalid, is_special_invalid_case_true);

    (is_incoming_id_invalid, is_special_invalid_case_true)
}

pub fn get_stream_related_data(elements_array: &Vec<String>) -> (String, HashMap<String, String>, String) {
    let id = elements_array[2].clone();

    let mut map_to_enter: HashMap<String, String> = HashMap::new();

    let no_of_key_value_pairs = (elements_array.len() - 3) / 2;

    for i in 0..no_of_key_value_pairs {
        map_to_enter.insert(elements_array[i + 2].clone(), elements_array[i + 3].clone());
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
