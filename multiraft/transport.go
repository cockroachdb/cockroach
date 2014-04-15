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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

package multiraft

import "github.com/golang/glog"

// The Transport interface is supplied by the application to manage communication with
// other nodes.  It is responsible for mapping from IDs to some communication channel
// (in the simplest case, a host:port pair could be used as an ID, although this would
// make it impossible to move an instance from one host to another except by syncing
// up a new node from scratch).
type Transport interface {
	// Listen informs the Transport of the local node's ID and callback interface.
	// The Transport should associate the given id with the server object so other Transport's
	// Connect methods can find it.
	Listen(id string, server RPC) error

	// Connect looks up a node by id and returns a stub interface to submit RPCs to it.
	Connect(id string) (RPC, error)
}

type localTransport struct {
	servers map[string]RPC
}

// NewLocalTransport returns an in-process Transport for testing purposes.
// MultiRaft instances sharing the same local Transport can communicate with each other.
func NewLocalTransport() Transport {
	return &localTransport{make(map[string]RPC)}
}

func (lt *localTransport) Listen(id string, server RPC) error {
	lt.servers[id] = server
	return nil
}

func (lt *localTransport) Connect(id string) (RPC, error) {
	return lt.servers[id], nil
}

// RequestVoteRequest is a part of the Raft protocol.  It is public so it can be used
// by the net/rpc system but should not be used outside this package except to serialize it.
type RequestVoteRequest struct {
	Group        string
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteResponse is a part of the Raft protocol.  It is public so it can be used
// by the net/rpc system but should not be used outside this package except to serialize it.
type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesRequest is a part of the Raft protocol.  It is public so it can be used
// by the net/rpc system but should not be used outside this package except to serialize it.
type AppendEntriesRequest struct {
	Group        string
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesResponse is a part of the Raft protocol.  It is public so it can be used
// by the net/rpc system but should not be used outside this package except to serialize it.
type AppendEntriesResponse struct {
	Term    int
	Success bool
}

// RPC is the interface implemented by a MultiRaft server.
type RPC interface {
	RequestVote(req *RequestVoteRequest, resp *RequestVoteResponse) error
	AppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) error
}

type requestVoteRequestOp struct {
	req  *RequestVoteRequest
	resp *RequestVoteResponse
	ch   chan error
}

type appendEntriesRequestOp struct {
	req  *AppendEntriesRequest
	resp *AppendEntriesResponse
	ch   chan error
}

// serverChannel bridges the synchronous RPC interface with MultiRaft's channel-oriented
// interface. Incoming rpcs are wrapped in "Op" objects and passed onto the channel.
// The RPC's goroutine remains blocked until an error (or nil) is written to the op's return
// channel.
type serverChannel chan<- interface{}

func (ch serverChannel) RequestVote(req *RequestVoteRequest, resp *RequestVoteResponse) error {
	op := &requestVoteRequestOp{req, resp, make(chan error)}
	ch <- op
	return <-op.ch
}

func (ch serverChannel) AppendEntries(req *AppendEntriesRequest,
	resp *AppendEntriesResponse) error {
	op := &appendEntriesRequestOp{req, resp, make(chan error)}
	ch <- op
	return <-op.ch
}

// asyncClient bridges MultiRaft's channel-oriented interface with the synchronous RPC interface.
// Outgoing requests are run in a goroutine and their response ops are returned on the
// given channel.
type asyncClient struct {
	nodeID string
	conn   RPC
	ch     chan<- interface{}
}

type requestVoteResponseOp struct {
	nodeID string
	req    *RequestVoteRequest
	resp   *RequestVoteResponse
}

type appendEntriesResponseOp struct {
	nodeID string
	req    *AppendEntriesRequest
	resp   *AppendEntriesResponse
}

func (a *asyncClient) requestVote(req *RequestVoteRequest) {
	op := &requestVoteResponseOp{a.nodeID, req, &RequestVoteResponse{}}
	go func() {
		a.handleRPCResponse("requestVote", op, a.conn.RequestVote(op.req, op.resp))
	}()
}

func (a *asyncClient) appendEntries(req *AppendEntriesRequest) {
	op := &appendEntriesResponseOp{a.nodeID, req, &AppendEntriesResponse{}}
	go func() {
		a.handleRPCResponse("appendEntries", op, a.conn.AppendEntries(op.req, op.resp))
	}()
}

func (a *asyncClient) handleRPCResponse(name string, op interface{}, err error) {
	if err != nil {
		glog.Errorf("%s error: %s", name, err)
		return
	}
	a.ch <- op
}
