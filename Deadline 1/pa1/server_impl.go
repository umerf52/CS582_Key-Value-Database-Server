// Implementation of a KeyValueServer. Students should write their code in this file.

package pa1

import (
	"DS_PA1/rpcs"
	"bufio"
	"net"
	"strconv"
)

type keyValuePair struct {
	key   string
	value string
}

type keyValueServer struct {
	// TODO: implement this!
	numberOfClients  int
	putChannel       chan keyValuePair
	getChannel       chan string
	broadcastChannel chan keyValuePair
	listOfClients    map[net.Conn]net.Conn
	deleteChannel    chan net.Conn
	addClientChannel chan net.Conn
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	// TODO: implement this!
	var kvs *keyValueServer
	kvs = new(keyValueServer)
	return kvs
}

func (kvs *keyValueServer) StartModel1(port int) error {
	// TODO: implement this!
	initDB()
	kvs.putChannel, kvs.broadcastChannel = make(chan keyValuePair), make(chan keyValuePair)
	kvs.getChannel = make(chan string)
	kvs.listOfClients = make(map[net.Conn]net.Conn)
	kvs.deleteChannel, kvs.addClientChannel = make(chan net.Conn), make(chan net.Conn)
	go kvs.broadCasterModel1()
	go kvs.putGetHandler()
	go kvs.clientListHandler()

	stringPort := ":" + strconv.Itoa(port)
	ln, err := net.Listen("tcp", stringPort)

	if err != nil {
		return err
	} else {
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
				} else {
					kvs.numberOfClients++
					// kvs.listOfClients[conn] = conn
					kvs.addClientChannel <- conn
					go kvs.serverHandler1(conn)
				}
			}
		}()
		return nil
	}
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
}

func (kvs *keyValueServer) Count() int {
	// TODO: implement this!
	return kvs.numberOfClients
}

func (kvs *keyValueServer) StartModel2(port int) error {
	// TODO: implement this!
	//
	// Do not forget to call rpcs.Wrap(...) on your kvs struct before
	// passing it to <sv>.Register(...)
	//
	// Wrap ensures that only the desired methods (RecvGet and RecvPut)
	// are available for RPC access. Other KeyValueServer functions
	// such as Close(), StartModel1(), etc. are forbidden for RPCs.
	//
	// Example: <sv>.Register(rpcs.Wrap(kvs))
	return nil
}

func (kvs *keyValueServer) RecvGet(args *rpcs.GetArgs, reply *rpcs.GetReply) error {
	// TODO: implement this!
	return nil
}

func (kvs *keyValueServer) RecvPut(args *rpcs.PutArgs, reply *rpcs.PutReply) error {
	// TODO: implement this!
	return nil
}

// TODO: add additional methods/functions below!

func (kvs *keyValueServer) serverHandler1(conn net.Conn) {

	// clean up once the connection closes
	defer Clean(conn)

	// obtain a buffered reader / writer on the connection
	rw := ConnectionToRW(conn)

	for {
		// get client message
		msg, err := rw.ReadString('\n')
		if err != nil {
			kvs.numberOfClients--
			kvs.deleteChannel <- conn
			return
		}
		if msg[:3] == "put" {
			msg = msg[4:]
			for pos, char := range msg {
				if char == ',' {
					keyString := msg[:pos]
					valueString := msg[pos+1:]
					valueString = valueString[:len(valueString)-1]
					pair := keyValuePair{keyString, valueString}
					kvs.putChannel <- pair
					break
				}
			}
		} else {
			keyString := msg[4 : len(msg)-1]
			kvs.getChannel <- keyString
		}
	}
}

// Clean closes a connection
func Clean(conn net.Conn) {
	// clean up connection related data structures and goroutines here
	conn.Close()
}

// ConnectionToRW takes a connection and returns a buffered reader / writer on it
func ConnectionToRW(conn net.Conn) *bufio.ReadWriter {
	return bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
}

func (kvs *keyValueServer) broadCasterModel1() {
	for {
		pair := <-kvs.broadcastChannel
		for _, conn := range kvs.listOfClients {
			rw := ConnectionToRW(conn)
			valueToSend := pair.key + "," + pair.value + "\n"
			_, err := rw.WriteString(valueToSend)
			if err != nil {
				return
			}
			err = rw.Flush()
			if err != nil {
				return
			}
		}
	}
}

func (kvs *keyValueServer) putGetHandler() {
	for {
		select {
		case pair := <-kvs.putChannel:
			valueBytes := []byte(pair.value)
			put(pair.key, valueBytes)

		case keyString := <-kvs.getChannel:
			valueString := string(get(keyString))
			pair := keyValuePair{keyString, valueString}
			kvs.broadcastChannel <- pair
		}
	}
}

func (kvs *keyValueServer) clientListHandler() {
	for {
		select {
		case conn := <-kvs.deleteChannel:
			delete(kvs.listOfClients, conn)
			conn.Close()

		case conn := <-kvs.addClientChannel:
			kvs.listOfClients[conn] = conn
		}
	}
}
