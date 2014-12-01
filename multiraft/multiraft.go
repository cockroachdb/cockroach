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
	"container/list"
	"math/rand"
	"net/rpc"
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/coreos/etcd/Godeps/_workspace/src/code.google.com/p/go.net/context"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

// Config contains the parameters necessary to construct a MultiRaft object.
type Config struct {
	Storage   Storage
	Transport Transport
	// Ticker may be nil to use real time and TickInterval.
	Ticker Ticker

	// A new election is called if the ElectionTimeout elapses with no contact from the leader.
	// The actual ElectionTimeout is chosen randomly from the range [ElectionTimeoutMin,
	// ElectionTimeoutMax) to minimize the chances of several servers trying to become leaders
	// simultaneously. The Raft paper suggests a range of 150-300ms for local networks;
	// geographically distributed installations should use higher values to account for the
	// increased round trip time.
	ElectionTimeoutTicks   int
	HeartbeatIntervalTicks int
	TickInterval           time.Duration

	// If Strict is true, some warnings become fatal panics and additional (possibly expensive)
	// sanity checks will be done.
	Strict bool
}

// Validate returns an error if any required elements of the Config are missing or invalid.
// Called automatically by NewMultiRaft.
func (c *Config) Validate() error {
	if c.Transport == nil {
		return util.Error("Transport is required")
	}
	if c.ElectionTimeoutTicks == 0 {
		return util.Error("ElectionTimeoutTicks must be non-zero")
	}
	if c.HeartbeatIntervalTicks == 0 {
		return util.Error("HeartbeatIntervalTicks must be non-zero")
	}
	if c.TickInterval == 0 {
		return util.Error("TickInterval must be non-zero")
	}
	return nil
}

// MultiRaft represents a local node in a raft cluster. The owner is responsible for consuming
// the Events channel in a timely manner.
type MultiRaft struct {
	Config
	multiNode raft.MultiNode
	Events    chan interface{}
	nodeID    uint64
	ops       chan interface{}
	requests  chan *rpc.Call
	stopped   chan struct{}
}

// NewMultiRaft creates a MultiRaft object.
func NewMultiRaft(nodeID uint64, config *Config) (*MultiRaft, error) {
	if nodeID == 0 {
		return nil, util.Error("Invalid NodeID")
	}
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	if config.Ticker == nil {
		config.Ticker = newTicker(config.TickInterval)
	}

	m := &MultiRaft{
		Config: *config,
		multiNode: raft.StartMultiNode(nodeID, config.ElectionTimeoutTicks,
			config.HeartbeatIntervalTicks),
		nodeID:   nodeID,
		Events:   make(chan interface{}, 1000),
		ops:      make(chan interface{}, 100),
		requests: make(chan *rpc.Call, 100),
		stopped:  make(chan struct{}),
	}

	err = m.Transport.Listen(nodeID, m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// Start runs the raft algorithm in a background goroutine.
func (m *MultiRaft) Start() {
	s := newState(m)
	go s.start()
}

// Stop terminates the running raft instance and shuts down all network interfaces.
func (m *MultiRaft) Stop() {
	m.Transport.Stop(m.nodeID)
	m.ops <- &stopOp{}
	<-m.stopped
	m.multiNode.Stop()
}

// DoRPC implements ServerInterface
func (m *MultiRaft) DoRPC(name string, req, resp interface{}) error {
	call := &rpc.Call{
		ServiceMethod: name,
		Args:          req,
		Reply:         resp,
		Done:          make(chan *rpc.Call, 1),
	}
	select {
	case m.requests <- call:
	default:
		m.strictErrorLog("RPC request channel blocked")
		// In non-strict mode, try again with blocking.
		m.requests <- call
	}
	<-call.Done
	return call.Error

}

// strictErrorLog panics in strict mode and logs an error otherwise. Arguments are printf-style
// and will be passed directly to either log.Errorf or log.Fatalf.
func (m *MultiRaft) strictErrorLog(format string, args ...interface{}) {
	if m.Strict {
		log.Fatalf(format, args...)
	} else {
		log.Errorf(format, args...)
	}
}

func (m *MultiRaft) sendEvent(event interface{}) {
	select {
	case m.Events <- event:
		return
	default:
		// TODO(bdarnell): how should we handle filling up the Event queue?
		// Is there any place to apply backpressure?
		panic("MultiRaft.Events backlog reached limit")
	}
}

// CreateGroup creates a new consensus group and joins it. The application should
// arrange to call CreateGroup on all nodes named in initialMembers.
func (m *MultiRaft) CreateGroup(groupID uint64, initialMembers []uint64) error {
	for _, id := range initialMembers {
		if id == 0 {
			return util.Error("Invalid NodeID")
		}
	}
	op := &createGroupOp{
		groupID,
		initialMembers,
		make(chan error),
	}
	m.ops <- op
	return <-op.ch
}

// SubmitCommand sends a command (a binary blob) to the cluster. This method returns
// when the command has been successfully sent, not when it has been committed.
// TODO(bdarnell): should SubmitCommand wait until the commit?
// TODO(bdarnell): what do we do if we lose leadership before a command we proposed commits?
func (m *MultiRaft) SubmitCommand(groupID uint64, command []byte) error {
	op := &submitCommandOp{groupID, command, make(chan error, 1)}
	m.ops <- op
	return <-op.ch
}

// ChangeGroupMembership submits a proposed membership change to the cluster.
// TODO(bdarnell): same concerns as SubmitCommand
// TODO(bdarnell): do we expose ChangeMembershipAdd{Member,Observer} to the application
// level or does MultiRaft take care of the non-member -> observer -> full member
// cycle?
func (m *MultiRaft) ChangeGroupMembership(groupID uint64, changeOp ChangeMembershipOperation,
	nodeID uint64) error {
	op := &changeGroupMembershipOp{
		groupID,
		ChangeMembershipPayload{changeOp, nodeID},
		make(chan error, 1),
	}
	m.ops <- op
	return <-op.ch
}

// pendingCall represents an RPC that we should not respond to until we have persisted
// up to the given point. term and logIndex may be -1 if the rpc didn't modify that
// variable and therefore can be resolved regardless of its value.
type pendingCall struct {
	call     *rpc.Call
	term     int
	logIndex int
}

// group represents the state of a consensus group.
type group struct {
	groupID uint64

	raftStorage *raft.MemoryStorage

	// a List of *pendingCall
	pendingCalls list.List

	// softState is the last value received from node.Ready() so we can compare
	// old and new values.
	softState raft.SoftState
}

type stopOp struct{}

type createGroupOp struct {
	groupID        uint64
	initialMembers []uint64
	ch             chan error
}

type submitCommandOp struct {
	groupID uint64
	command []byte
	ch      chan error
}

type changeGroupMembershipOp struct {
	groupID uint64
	payload ChangeMembershipPayload
	ch      chan error
}

// node represents a connection to a remote node.
type node struct {
	nodeID   uint64
	refCount int
	client   *asyncClient
}

// state represents the internal state of a MultiRaft object. All variables here
// are accessible only from the state.start goroutine so they can be accessed without
// synchronization.
type state struct {
	*MultiRaft
	rand          *rand.Rand
	groups        map[uint64]*group
	nodes         map[uint64]*node
	electionTimer *time.Timer
	responses     chan *rpc.Call
	writeTask     *writeTask
}

func newState(m *MultiRaft) *state {
	return &state{
		MultiRaft: m,
		rand:      util.NewPseudoRand(),
		groups:    make(map[uint64]*group),
		nodes:     make(map[uint64]*node),
		responses: make(chan *rpc.Call, 100),
		writeTask: newWriteTask(m.Storage),
	}
}

func (s *state) start() {
	log.V(1).Infof("node %v starting", s.nodeID)
	go s.writeTask.start()
	// These maps form a kind of state machine: We don't want to read from the
	// ready channel until the groups we got from the last read have made their
	// way through the rest of the pipeline.
	// TODO(bdarnell): Does it still make sense to structure this as a select loop?
	// I think we can untangle some of the channels here now that the core raft
	// implementation has been moved to etcd.
	var readyGroups map[uint64]raft.Ready
	var writingGroups map[uint64]raft.Ready
	for {
		// raftReady signals that the Raft state machine has pending
		// work. That work is supplied over the raftReady channel as a map
		// from group ID to raft.Ready struct.
		var raftReady <-chan map[uint64]raft.Ready
		// writeReady is set to the write task's ready channel, which
		// receives when the write task is prepared to persist ready data
		// from the Raft state machine.
		var writeReady chan struct{}

		// The order of operations in this loop structure is as follows:
		// start by setting raftReady to the multiNode's Ready()
		// channel. Once a new raftReady has been consumed from the
		// channel, set writeReady to the write task's ready channel and
		// set raftReady back to nil. This advances our read-from-raft /
		// write-to-storage state machine to the next step: wait for the
		// write task to be ready to persist the new data.
		if readyGroups != nil {
			writeReady = s.writeTask.ready
		} else if writingGroups == nil {
			raftReady = s.multiNode.Ready()
		}

		log.V(8).Infof("node %v: selecting", s.nodeID)
		select {
		case op := <-s.ops:
			log.V(6).Infof("node %v: got op %#v", s.nodeID, op)
			switch op := op.(type) {
			case *stopOp:
				s.stop()
				return

			case *createGroupOp:
				s.createGroup(op)

			case *submitCommandOp:
				s.submitCommand(op)

			case *changeGroupMembershipOp:
				s.changeGroupMembership(op)

			default:
				s.strictErrorLog("unknown op: %#v", op)
			}

		case call := <-s.requests:
			log.V(6).Infof("node %v: got request %v", s.nodeID, call)
			switch call.ServiceMethod {
			case sendMessageName:
				s.sendMessageRequest(call.Args.(*SendMessageRequest),
					call.Reply.(*SendMessageResponse), call)

			default:
				s.strictErrorLog("unknown rpc request: %#v", call.Args)
			}

		case call := <-s.responses:
			log.V(6).Infof("node %v: got response %v", s.nodeID, call)
			switch call.ServiceMethod {
			case sendMessageName:

			default:
				s.strictErrorLog("unknown rpc response: %#v", call.Reply)
			}

		case writeReady <- struct{}{}:
			s.handleWriteReady(readyGroups)
			writingGroups = readyGroups
			readyGroups = nil

		case resp := <-s.writeTask.out:
			s.handleWriteResponse(resp, writingGroups)
			writingGroups = nil

		case <-s.Ticker.Chan():
			log.V(6).Infof("node %v: got tick", s.nodeID)
			s.multiNode.Tick()

		case readyGroups = <-raftReady:
			s.handleRaftReady(readyGroups)
		}
	}
}

func (s *state) stop() {
	log.V(6).Infof("node %v stopping", s.nodeID)
	for _, n := range s.nodes {
		err := n.client.conn.Close()
		if err != nil {
			log.Warning("error stopping client:", err)
		}
	}
	s.writeTask.stop()
	close(s.stopped)
}

func (s *state) createGroup(op *createGroupOp) {
	if _, ok := s.groups[op.groupID]; ok {
		op.ch <- util.Errorf("group %v already exists", op.groupID)
		return
	}
	log.V(6).Infof("node %v creating group %v", s.nodeID, op.groupID)

	peers := make([]raft.Peer, len(op.initialMembers))
	for i, member := range op.initialMembers {
		peers[i].ID = member
		if node, ok := s.nodes[member]; ok {
			node.refCount++
			continue
		}
		conn, err := s.Transport.Connect(member)
		if err != nil {
			op.ch <- err
			return
		}
		s.nodes[member] = &node{member, 1, &asyncClient{member, conn, s.responses}}
	}
	storage := raft.NewMemoryStorage()
	s.multiNode.CreateGroup(op.groupID, peers, storage)
	s.groups[op.groupID] = &group{
		groupID:     op.groupID,
		raftStorage: storage,
	}
	op.ch <- nil
}

func (s *state) submitCommand(op *submitCommandOp) {
	log.V(6).Infof("node %v submitting command to group %v", s.nodeID, op.groupID)
	err := s.multiNode.Propose(context.Background(), op.groupID, op.command)
	op.ch <- err
}

func (s *state) changeGroupMembership(op *changeGroupMembershipOp) {
	log.V(6).Infof("node %v proposing membership change to group %v", s.nodeID, op.groupID)
	err := s.multiNode.ProposeConfChange(context.Background(), op.groupID, raftpb.ConfChange{})
	op.ch <- err
}

func (s *state) sendMessageRequest(req *SendMessageRequest, resp *SendMessageResponse,
	call *rpc.Call) {
	err := s.multiNode.Step(context.Background(), req.GroupID, req.Message)
	if err != nil {
		log.Errorf("raft: %s", err)
	}
	call.Error = err
	call.Done <- call
}

func (s *state) handleRaftReady(readyGroups map[uint64]raft.Ready) {
	// Soft state is updated immediately; everything else waits for handleWriteReady.
	for groupID, ready := range readyGroups {
		g := s.groups[groupID]
		log.V(6).Infof("node %v: group %v: got %#v from raft", s.nodeID, groupID, ready)
		if ready.SoftState != nil {
			if ready.SoftState.Lead != g.softState.Lead {
				s.sendEvent(&EventLeaderElection{groupID, ready.SoftState.Lead})
			}
			g.softState = *ready.SoftState
		}
		g.raftStorage.Append(ready.Entries)
	}
	s.multiNode.Advance(readyGroups)
}

func (s *state) handleWriteReady(readyGroups map[uint64]raft.Ready) {
	log.V(6).Infof("node %v write ready, preparing request", s.nodeID)
	writeRequest := newWriteRequest()
	for groupID, ready := range readyGroups {
		gwr := &groupWriteRequest{}
		if !raft.IsEmptyHardState(ready.HardState) {
			gwr.state = &GroupPersistentState{
				GroupID:   groupID,
				HardState: ready.HardState,
			}
		}
		if len(ready.Entries) > 0 {
			gwr.entries = make([]*LogEntry, len(ready.Entries))
			for i, ent := range ready.Entries {
				gwr.entries[i] = &LogEntry{ent}
			}
		}
		writeRequest.groups[groupID] = gwr
	}
	s.writeTask.in <- writeRequest
}

func (s *state) handleWriteResponse(response *writeResponse, readyGroups map[uint64]raft.Ready) {
	log.V(6).Infof("node %v got write response: %#v", s.nodeID, *response)
	// Everything has been written to disk; now we can apply updates to the state machine
	// and send outgoing messages.
	for groupID, ready := range readyGroups {
		for _, entry := range ready.CommittedEntries {
			switch entry.Type {
			case raftpb.EntryNormal:
				// TODO(bdarnell): etcd raft adds a nil entry upon election; should this be given a different Type?
				if entry.Data != nil {
					s.sendEvent(&EventCommandCommitted{entry.Data})
				}
			case raftpb.EntryConfChange:
				cc := raftpb.ConfChange{}
				err := cc.Unmarshal(entry.Data)
				if err != nil {
					log.Fatalf("invalid ConfChange data: %s", err)
				}
				log.V(3).Infof("node %v applying configuration change %v", s.nodeID, cc)
				s.multiNode.ApplyConfChange(groupID, cc)
			}
		}
		for _, msg := range ready.Messages {
			if msg.To == 0 {
				// TODO(bdarnell): figure out why these are happening
				log.Warningf("dropping message for node 0")
				continue
			}
			log.V(6).Infof("node %v sending %s message to %v", s.nodeID, msg.Type, msg.To)
			s.nodes[msg.To].client.sendMessage(&SendMessageRequest{groupID, msg})
		}
	}
}

func (s *state) addPendingCall(g *group, call *pendingCall) {
	if !s.resolvePendingCall(g, call) {
		g.pendingCalls.PushBack(call)
	}
}

func (s *state) resolvePendingCall(g *group, call *pendingCall) bool {
	// TODO(bdarnell): rewrite resolvePendingCall for etcd raft
	/*if g.persistedElectionState == nil || g.persistedLastIndex == -1 {
		return false
	}
	if call.term != -1 && call.term > g.persistedElectionState.CurrentTerm {
		return false
	}
	if call.logIndex != -1 && call.logIndex > g.persistedLastIndex {
		return false
	}*/
	call.call.Done <- call.call
	return true
}
