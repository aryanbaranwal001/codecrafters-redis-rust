

`echo -e '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n' | nc localhost 6379`

`echo -e '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n' | nc localhost 6379`
