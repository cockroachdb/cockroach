// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

package multiraft

import (
	"net"
	"net/rpc"
	"strings"

	"github.com/cockroachdb/cockroach/util/log"
	"github.com/coreos/etcd/raft/raftpb"
)

// The Transport interface is supplied by the application to manage communication with
// other nodes. It is responsible for mapping from IDs to some communication channel
// (in the simplest case, a host:port pair could be used as an ID, although this would
// make it impossible to move an instance from one host to another except by syncing
// up a new node from scratch).
type Transport interface {
	// Listen informs the Transport of the local node's ID and callback interface.
	// The Transport should associate the given id with the server object so other Transport's
	// Connect methods can find it.
	Listen(id uint64, server ServerInterface) error

	// Stop undoes a previous Listen.
	Stop(id uint64)

	// Connect looks up a node by id and returns a stub interface to submit RPCs to it.
	Connect(id uint64) (ClientInterface, error)
}

// SendMessageRequest wraps a raft message.
type SendMessageRequest struct {
	GroupID uint64
	Message raftpb.Message
}

// SendMessageResponse is empty (raft uses a one-way messaging model; if a response
// is needed it will be sent as a separate message).
type SendMessageResponse struct {
}

// ServerInterface is a generic interface based on net/rpc.
type ServerInterface interface {
	DoRPC(name string, req, resp interface{}) error
}

// RPCInterface is the methods we expose for use by net/rpc.
type RPCInterface interface {
	SendMessage(req *SendMessageRequest, resp *SendMessageResponse) error
}

var (
	sendMessageName = "MultiRaft.SendMessage"
)

// ClientInterface is the interface expected of the client provided by a transport.
// It is satisfied by rpc.Client, but could be implemented in other ways (using
// rpc.Call as a dumb data structure)
type ClientInterface interface {
	Go(serviceMethod string, args interface{}, reply interface{}, done chan *rpc.Call) *rpc.Call
	Close() error
}

// rpcAdapter converts the generic ServerInterface to the concrete RPCInterface
type rpcAdapter struct {
	server ServerInterface
}

func (r *rpcAdapter) SendMessage(req *SendMessageRequest, resp *SendMessageResponse) error {
	return r.server.DoRPC(sendMessageName, req, resp)
}

// asyncClient bridges MultiRaft's channel-oriented interface with the synchronous RPC interface.
// Outgoing requests are run in a goroutine and their response ops are returned on the
// given channel.
type asyncClient struct {
	nodeID uint64
	conn   ClientInterface
	ch     chan *rpc.Call
}

func (a *asyncClient) sendMessage(req *SendMessageRequest) {
	a.conn.Go(sendMessageName, req, &SendMessageResponse{}, a.ch)
}

type localRPCTransport struct {
	listeners map[uint64]net.Listener
}

// NewLocalRPCTransport creates a Transport for local testing use. MultiRaft instances
// sharing the same local Transport can find and communicate with each other by ID (which
// can be an arbitrary string). Each instance binds to a different unused port on
// localhost.
// Because this is just for local testing, it doesn't use TLS.
func NewLocalRPCTransport() Transport {
	return &localRPCTransport{make(map[uint64]net.Listener)}
}

func (lt *localRPCTransport) Listen(id uint64, server ServerInterface) error {
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
			if strings.HasSuffix(err.Error(), "use of closed network connection") {
				return
			}
			log.Errorf("localRPCTransport.accept: %s", err.Error())
			continue
		}
		go server.ServeConn(conn)
	}
}

func (lt *localRPCTransport) Stop(id uint64) {
	lt.listeners[id].Close()
	delete(lt.listeners, id)
}

func (lt *localRPCTransport) Connect(id uint64) (ClientInterface, error) {
	address := lt.listeners[id].Addr().String()
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return client, nil
}
