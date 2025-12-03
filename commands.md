

`echo -e '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n' | nc localhost 6379`

`echo -e '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n' | nc localhost 6379`


`echo -e '*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$4\r\n1000\r\n' | nc localhost 6379`

`echo -e '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n' | nc localhost 6379`




`echo -e '*3\r\n$3\r\nSET\r\n$4\r\npear\r\n$10\r\nstrawberry\r\n' | nc localhost 6379`
`echo -e '*2\r\n$3\r\nGET\r\n$4\r\npear' | nc localhost 6379`



`codecrafters test `