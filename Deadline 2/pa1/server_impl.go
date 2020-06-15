// Implementation of a KeyValueServer. Students should write their code in this file.

package pa1

import (
	"DS_PA1/rpcs"
	"bufio"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
)

const bufferSize = 500

type keyValuePair struct {
	key   string
	value string
}

type keyValueServer struct {
	// TODO: implement this!
	numberOfClients     int
	putChannel          chan keyValuePair
	getChannel          chan string
	broadcastChannel    chan keyValuePair
	listOfClients       map[net.Conn]chan string
	deleteClientChannel chan net.Conn
	addClientChannel    chan net.Conn
	askCount            chan bool
	getCount            chan int
	getReplyChannel     chan []byte
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
	kvs.listOfClients = make(map[net.Conn]chan string)
	kvs.deleteClientChannel, kvs.addClientChannel = make(chan net.Conn), make(chan net.Conn)
	kvs.askCount = make(chan bool)
	kvs.getCount = make(chan int)
	go kvs.putGetHandler()
	go kvs.sharedVariableHandler()

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
	kvs.askCount <- true
	return (<-kvs.getCount)
}

func (kvs *keyValueServer) StartModel2(port int) error {
	// TODO: implement this!
	initDB()
	kvs.putChannel = make(chan keyValuePair)
	kvs.getChannel = make(chan string)
	kvs.getReplyChannel = make(chan []byte)
	stringPort := ":" + strconv.Itoa(port)
	ln, err := net.Listen("tcp", stringPort)

	if err != nil {
		return err
	} else {
		rpcServer := rpc.NewServer()
		rpcServer.Register(rpcs.Wrap(kvs))
		http.DefaultServeMux = http.NewServeMux()
		rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
		go http.Serve(ln, nil)
		go kvs.putGetHandlerModel2()
		return nil
	}
}

func (kvs *keyValueServer) RecvGet(args *rpcs.GetArgs, reply *rpcs.GetReply) error {
	// TODO: implement this!
	keyString := args.Key
	kvs.getChannel <- keyString
	returnedValue := <-kvs.getReplyChannel
	reply.Value = returnedValue
	return nil
}

func (kvs *keyValueServer) RecvPut(args *rpcs.PutArgs, reply *rpcs.PutReply) error {
	// TODO: implement this!
	pair := keyValuePair{args.Key, string(args.Value)}
	kvs.putChannel <- pair
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
			kvs.deleteClientChannel <- conn
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

func (kvs *keyValueServer) sharedVariableHandler() {
	for {
		select {
		case conn := <-kvs.deleteClientChannel:
			delete(kvs.listOfClients, conn)
			conn.Close()
			kvs.numberOfClients--

		case conn := <-kvs.addClientChannel:
			newChannel := make(chan string, bufferSize)
			kvs.listOfClients[conn] = newChannel
			kvs.numberOfClients++
			go clientSender(conn, newChannel)

		case pair := <-kvs.broadcastChannel:
			for _, channel := range kvs.listOfClients {
				if len(channel) >= bufferSize {
					continue
				} else {
					valueToSend := pair.key + "," + pair.value + "\n"
					channel <- valueToSend
				}
			}

		case <-kvs.askCount:
			kvs.getCount <- kvs.numberOfClients
		}
	}
}

func (kvs *keyValueServer) putGetHandlerModel2() {
	for {
		select {
		case pair := <-kvs.putChannel:
			valueBytes := []byte(pair.value)
			put(pair.key, valueBytes)

		case keyString := <-kvs.getChannel:
			valueBytes := get(keyString)
			kvs.getReplyChannel <- valueBytes
		}
	}
}

func clientSender(conn net.Conn, channel chan string) {
	for {
		valueToSend := <-channel
		rw := ConnectionToRW(conn)
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
