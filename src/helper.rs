use std::time::Instant;

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

        _ => { None }
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

            let number_required = String::from_utf8(num_in_bytes)
                .unwrap()
                .parse::<usize>()
                .unwrap();

            (number_required, counter)
        }

        _ => {
            panic!("Didn't recieved the dollar or asterik");
        }
    }
}

// get the elements in Vec<String> from resp
pub fn parsing_elements(
    bytes_received: &[u8],
    mut counter: usize,
    no_of_elements: usize
) -> Vec<String> {
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
