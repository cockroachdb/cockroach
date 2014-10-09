// Copyright 2014 Square, Inc
// Author: Ben Darnell (bdarnell@)

package multiraft

import (
	"net"
	"net/rpc"

	"github.com/cockroachdb/cockroach/util/log"
)

type localRPCTransport struct {
	listeners map[int64]net.Listener
}

// NewLocalRPCTransport creates a Transport for local testing use. MultiRaft instances
// sharing the same local Transport can find and communicate with each other by ID (which
// can be an arbitrary string). Each instance binds to a different unused port on
// localhost.
// Because this is just for local testing, it doesn't use TLS.
func NewLocalRPCTransport() Transport {
	return &localRPCTransport{make(map[int64]net.Listener)}
}

func (lt *localRPCTransport) Listen(id int64, server ServerInterface) error {
	rpcServer := rpc.NewServer()
	err := rpcServer.RegisterName("MultiRaft", &rpcAdapter{server})
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}

	lt.listeners[id] = listener
	go lt.accept(rpcServer, listener)
	return nil
}

func (lt *localRPCTransport) accept(server *rpc.Server, listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if opError, ok := err.(*net.OpError); ok {
				if opError.Err.Error() == "use of closed network connection" {
					return
				}
			}
			log.Errorf("rpc.Serve: accept: %s", err.Error())
		}
		go server.ServeConn(conn)
	}
}

func (lt *localRPCTransport) Stop(id int64) {
	lt.listeners[id].Close()
	delete(lt.listeners, id)
}

func (lt *localRPCTransport) Connect(id int64) (ClientInterface, error) {
	address := lt.listeners[id].Addr().String()
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return client, nil
}
