## Table of Contents

- [Table of Contents](#table-of-contents)
- [Build Your Own Redis (Rust)](#build-your-own-redis-rust)
- [Features Implemented](#features-implemented)
- [Why I Built This](#why-i-built-this)
- [Architecture Overview](#architecture-overview)
- [Running \& Testing](#running--testing)
- [Credits](#credits)
  
## Build Your Own Redis (Rust)

This is a Redis implementation written in Rust — built as part of the
[CodeCrafters “Build Your Own Redis” challenge](https://codecrafters.io/challenges/redis).

This project focuses on understanding how Redis works **under the hood** — sockets, event
loops, and the RESP (Redis Serialization Protocol), etc — instead of relying on high-level libraries.

> **Note:** 
> No AI tools or code generators were used while implementing this. \
> The entire project was written by hand.

## Features Implemented

This implementation currently supports:

* Core commands
  `PING`, `ECHO`, `SET`, `GET`, `INCR`, `TYPE`, `INFO`, `CONFIG`, `KEYS`

* Lists
  `LPUSH`, `RPUSH`, `LRANGE`, `LLEN`, `LPOP`, `BLPOP`

* Streams
  `XADD`, `XRANGE`, `XREAD`

* Transactions
  `MULTI`, `EXEC`, `DISCARD`

* Pub/Sub
  `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`

* Replication
  `REPLCONF`, `PSYNC`, `WAIT`

* Sorted Sets
  `ZADD`, `ZRANK`, `ZRANGE`, `ZCARD`, `ZSCORE`, `ZREM`

* Geospatial
  `GEOADD`, `GEOPOS`, `GEODIST`, `GEOSEARCH`

* ACL / Auth
  `ACL`, `AUTH`


## Why I Built This

I wanted to:

* understand networking and sockets in Rust
* learn **how Redis actually processes commands**
* deepen my Rust skills (ownership, lifetimes, error handling, design)
* gain confidence structuring real-world Rust projects


## Architecture Overview

```
src/
├── main.rs        # server entrypoint
├── commands.rs    # command implementations
└── helper.rs      # helpers + utilities
```

* `main.rs` — server entrypoint, handles incoming connections & routing
* `commands.rs` — logic for each Redis command
* `helper.rs` — parsing, serialization and other helpers


## Running & Testing

**Requirements**

* Rust & Cargo
* Redis CLI

Start the server:

```bash
./your_program.sh --port 6400
```

(Defaults to `6379` if not provided.)

Connect using:

```bash
redis-cli -p 6400
```

Try some commands:

```bash
ping
set foo bar
get foo
```

```bash
set books 1
incr books
incr books
get books
```


## Credits

Challenge by **CodeCrafters** \
Implementation written by **me** in **Rust 🦀**

