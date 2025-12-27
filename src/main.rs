use clap::Parser;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use crate::types::UserInfo;

mod commands;
mod helper;
mod types;

fn main() {
    let args = types::Args::parse();
    let port = args
        .port
        .map(|p| p.to_string())
        .unwrap_or_else(|| "6379".to_string());

    let dir = Arc::new(Mutex::new(args.dir));
    let dbfilename = Arc::new(Mutex::new(args.dbfilename));

    let store: types::SharedStore = Arc::new((Mutex::new(HashMap::new()), Condvar::new()));
    let main_list: types::SharedMainList = Arc::new((Mutex::new(Vec::new()), Condvar::new()));

    let role = if let Some(replicaof_string) = args.replicaof {
        // handling connection with master as slave
        let (master_url, marter_port) = replicaof_string
            .split_once(" ")
            .expect("[error] invalid format (expected: HOST PORT)");

        let master_addr = format!("{master_url}:{marter_port}");
        let mut master_stream = TcpStream::connect(&master_addr).unwrap();
        println!("[info] connected with master with addr: {master_addr}");

        let store_clone = Arc::clone(&store);
        let main_list_clone = Arc::clone(&main_list);
        let mut offset: usize = 0;

        // run the left commands
        if let Some(left_cmds) = helper::hand_shake(&port, &mut master_stream) {
            let mut counter = 0;
            while counter < left_cmds.len() {
                let (elems, count) = helper::get_elems_with_counter(counter, &left_cmds);

                master_stream = helper::handle_connection_as_slave_with_master(
                    master_stream,
                    elems.clone(),
                    &store_clone,
                    &main_list_clone,
                    "role:slave",
                    offset,
                );

                offset += count - counter;
                counter = count;
            }
        }

        // handling connection as slave with master
        thread::spawn(move || {
            let mut buffer = [0; 512];

            loop {
                match master_stream.read(&mut buffer) {
                    Ok(0) => {
                        println!("[info] disconnected from master");
                        return;
                    }
                    Ok(n) => {
                        let buffer_checked = helper::skip_rdb_if_present(&buffer[..n]);

                        let mut counter = 0;
                        while counter < buffer_checked.len() {
                            let (elems, count) =
                                helper::get_elems_with_counter(counter, &buffer_checked);

                            master_stream = helper::handle_connection_as_slave_with_master(
                                master_stream,
                                elems.clone(),
                                &store_clone,
                                &main_list_clone,
                                "role:slave",
                                offset,
                            );

                            offset += count - counter;
                            counter = count;
                        }
                    }
                    Err(e) => {
                        eprintln!("[error] error reading stream: {e}");
                    }
                }
            }
        });
        "role:slave"

    // if role != slave
    } else {
        "role:master"
    };

    //////////// HANDLING CONNECTIONS ////////////

    let shared_replicas_count: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
    let master_repl_offset = Arc::new(Mutex::new(0usize));
    let tcpstream_vector: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(Vec::new()));
    let subs_htable: Arc<Mutex<HashMap<String, Vec<TcpStream>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let zset_hmap: Arc<Mutex<HashMap<String, types::ZSet>>> = Arc::new(Mutex::new(HashMap::new()));
    let userpw_hmap: Arc<Mutex<HashMap<String, Vec<[u8; 32]>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    println!("[info] {} server with port number: {}", role, port);

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();

    for connection in listener.incoming() {
        let store_clone = Arc::clone(&store);
        let dir_clone = Arc::clone(&dir);
        let zset_hmap = Arc::clone(&zset_hmap);
        let main_list_clone = Arc::clone(&main_list);
        let dbfilename_clone = Arc::clone(&dbfilename);
        let userpw_hmap_clone = Arc::clone(&userpw_hmap);
        let subs_htable_clone = Arc::clone(&subs_htable);
        let tcpstream_vector_clone = Arc::clone(&tcpstream_vector);
        let master_repl_offset_clone = Arc::clone(&master_repl_offset);
        let shared_replica_count_clone = Arc::clone(&shared_replicas_count);
        let channels_subscribed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let user: Arc<Mutex<types::UserInfo>> = Arc::new(Mutex::new(UserInfo {
            name: "default".to_string(),
            is_authenticated: false,
        }));

        thread::spawn(move || match connection {
            Ok(mut stream) => {
                println!("[info] accepted new connection");

                let mut buffer = [0; 512];

                let mut is_subscribed = false;

                loop {
                    match stream.read(&mut buffer) {
                        Ok(0) => {
                            println!("[info] client disconnected");
                            return;
                        }
                        Ok(n) => {
                            if role == "role:slave" {
                                stream = helper::handle_other_clients_as_slave(
                                    stream,
                                    &buffer,
                                    &store_clone,
                                    role,
                                );

                                continue;
                            }

                            if is_subscribed {
                                stream = helper::handle_subscribed_mode(
                                    stream,
                                    &buffer,
                                    &subs_htable_clone,
                                    &mut is_subscribed,
                                    &channels_subscribed,
                                );

                                continue;
                            }

                            let elems = helper::get_elems(&buffer[..n]);

                            let is_write_cmd = matches!(
                                elems[0].to_ascii_lowercase().as_str(),
                                "set" | "rpush" | "lpush" | "lpop" | "blpop" | "xadd" | "incr"
                            );

                            if is_write_cmd {
                                helper::handle_slaves(&tcpstream_vector_clone, &buffer[..n]);
                                *master_repl_offset_clone.lock().unwrap() += buffer[..n].len();
                            }

                            stream = handle_connection(
                                &buffer,
                                stream,
                                &store_clone,
                                &main_list_clone,
                                role,
                                &dir_clone,
                                &dbfilename_clone,
                                &tcpstream_vector_clone,
                                &master_repl_offset_clone,
                                &shared_replica_count_clone,
                                &subs_htable_clone,
                                &mut is_subscribed,
                                &channels_subscribed,
                                &zset_hmap,
                                &userpw_hmap_clone,
                                &user,
                            );
                        }
                        Err(e) => {
                            println!("[error] error reading stream: {e}");
                        }
                    }
                }
            }
            Err(e) => {
                println!("[error] error making connection: {e}");
            }
        });
    }
}

//////////// CONNECTIONS HELPER FUNCTION ////////////

fn handle_connection(
    bytes_received: &[u8],
    mut stream: TcpStream,
    store: &types::SharedStore,
    main_list_store: &types::SharedMainList,
    role: &str,
    dir_clone: &Arc<Mutex<Option<String>>>,
    dbfilename_clone: &Arc<Mutex<Option<String>>>,
    tcpstream_vector_clone: &Arc<Mutex<Vec<TcpStream>>>,
    master_repl_offset: &Arc<Mutex<usize>>,
    shared_replica_count_clone: &Arc<Mutex<usize>>,
    subs_htable: &Arc<Mutex<HashMap<String, Vec<TcpStream>>>>,
    is_subscribed: &mut bool,
    channels_subscribed: &Arc<Mutex<Vec<String>>>,
    zset_hmap: &Arc<Mutex<HashMap<String, types::ZSet>>>,
    userpw_hmap_clone: &Arc<Mutex<HashMap<String, Vec<[u8; 32]>>>>,
    user_guard: &Arc<Mutex<types::UserInfo>>,
) -> TcpStream {
    match bytes_received[0] {
        b'*' => {
            let mut elems = helper::get_elems(bytes_received);
            println!("[info] elements array: {:?}", elems);

            let resp: String = match elems[0].to_ascii_lowercase().as_str() {
                "echo" => commands::handle_echo(elems),

                "ping" => "+PONG\r\n".to_string(),

                "set" => commands::handle_set(elems, store),

                "get" => commands::handle_get(dir_clone, dbfilename_clone, &elems, store),

                "rpush" => commands::handle_rpush(elems, main_list_store),

                "lpush" => commands::handle_lpush(elems, main_list_store),

                "lrange" => commands::handle_lrange(elems, main_list_store),

                "llen" => commands::handle_llen(elems, main_list_store),

                "lpop" => commands::handle_lpop(elems, main_list_store),

                "blpop" => commands::handle_blpop(elems, main_list_store),

                "type" => commands::handle_type(elems, store),

                "xadd" => commands::handle_xadd(&mut elems, store),

                "xrange" => commands::handle_xrange(&mut elems, store),

                "xread" => commands::handle_xread(&mut elems, store),

                "incr" => commands::handle_incr(&mut elems, store),

                "multi" => {
                    commands::handle_multi(&mut stream, store, main_list_store);
                    "".to_string()
                }

                // discard without multi
                "discard" => "-ERR DISCARD without MULTI\r\n".to_string(),

                // exec without multi
                "exec" => "-ERR EXEC without MULTI\r\n".to_string(),

                "info" => match commands::handle_info(&elems, role) {
                    Some(resp) => resp,
                    None => "".to_string(),
                },

                "replconf" => {
                    match commands::handle_replconf(
                        &elems,
                        master_repl_offset,
                        shared_replica_count_clone,
                    ) {
                        Some(resp) => resp,
                        None => "".to_string(),
                    }
                }

                "psync" => {
                    commands::handle_psync(&mut stream, tcpstream_vector_clone);
                    "".to_string()
                }

                "wait" => commands::handle_wait(
                    &elems,
                    tcpstream_vector_clone,
                    master_repl_offset,
                    shared_replica_count_clone,
                ),
                "config" => commands::handle_config(dir_clone, dbfilename_clone, elems),

                "keys" => commands::handle_keys(dir_clone, dbfilename_clone, elems),

                "subscribe" => commands::handle_subscribe(
                    elems,
                    is_subscribed,
                    channels_subscribed,
                    subs_htable,
                    &mut stream,
                ),

                "publish" => commands::handle_publish(elems, subs_htable),

                "unsubscribe" => commands::handle_unsubscribe(
                    elems,
                    subs_htable,
                    channels_subscribed,
                    &mut stream,
                ),

                "zadd" => commands::handle_zadd(zset_hmap, elems),

                "zrank" => commands::handle_zrank(zset_hmap, elems),

                "zrange" => commands::handle_zrange(zset_hmap, elems),

                "zcard" => commands::handle_zcard(zset_hmap, elems),

                "zscore" => commands::handle_zscore(zset_hmap, elems),

                "zrem" => commands::handle_zrem(zset_hmap, elems),

                "geoadd" => commands::handle_geoadd(zset_hmap, elems),

                "geopos" => commands::handle_geopos(zset_hmap, elems),

                "geodist" => commands::handle_geodist(zset_hmap, elems),

                "geosearch" => commands::handle_geosearch(zset_hmap, elems),

                "acl" => commands::handle_acl(elems, userpw_hmap_clone, user_guard),

                "auth" => commands::handle_auth(elems, userpw_hmap_clone, user_guard),

                _ => "-ERR Not a valid command\r\n".to_string(),
            };
            let _ = stream.write_all(resp.as_bytes());
        }

        _ => {
            eprint!("[error] invalid resp string");
        }
    }

    stream
}
