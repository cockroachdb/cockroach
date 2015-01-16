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
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
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
	multiNode       raft.MultiNode
	Events          chan interface{}
	nodeID          uint64
	createGroupChan chan *createGroupOp
	removeGroupChan chan *removeGroupOp
	proposalChan    chan proposal
	stopper         *util.Stopper
}

// multiraftServer is a type alias to separate RPC methods
// (which net/rpc finds via reflection) from others.
type multiraftServer MultiRaft

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
		nodeID:          nodeID,
		Events:          make(chan interface{}, 1000),
		createGroupChan: make(chan *createGroupOp, 100),
		removeGroupChan: make(chan *removeGroupOp, 100),
		proposalChan:    make(chan proposal, 100),
		stopper:         util.NewStopper(1),
	}

	err = m.Transport.Listen(nodeID, (*multiraftServer)(m))
	if err != nil {
		return nil, err
	}

	return m, nil
}

// Start runs the raft algorithm in a background goroutine.
func (m *MultiRaft) Start() error {
	s := newState(m)
	go s.start()

	return nil
}

// Stop terminates the running raft instance and shuts down all network interfaces.
func (m *MultiRaft) Stop() {
	m.Transport.Stop(m.nodeID)
	m.stopper.Stop()
	m.multiNode.Stop()
}

// RaftMessage implements ServerInterface; this method is called by net/rpc
// when we receive a message.
func (ms *multiraftServer) RaftMessage(req *RaftMessageRequest,
	resp *RaftMessageResponse) error {
	m := (*MultiRaft)(ms)
	log.V(5).Infof("node %v: group %v got message %s", m.nodeID, req.GroupID,
		raft.DescribeMessage(req.Message))
	return m.multiNode.Step(context.Background(), req.GroupID, req.Message)
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

// CreateGroup creates a new consensus group and joins it. The initial membership of this
// group is determined by the InitialState method of the group's Storage object.
func (m *MultiRaft) CreateGroup(groupID uint64) error {
	op := &createGroupOp{
		groupID: groupID,
		ch:      make(chan error, 1),
	}
	m.createGroupChan <- op
	return <-op.ch
}

// RemoveGroup destroys the consensus group with the given ID.
// No events for this group will be emitted after this method returns
// (but some events may still be in the channel buffer).
func (m *MultiRaft) RemoveGroup(groupID uint64) error {
	op := &removeGroupOp{
		groupID: groupID,
		ch:      make(chan error, 1),
	}
	m.removeGroupChan <- op
	return <-op.ch
}

// SubmitCommand sends a command (a binary blob) to the cluster. This method returns
// when the command has been successfully sent, not when it has been committed.
// The returned channel is closed when the command is committed.
// As long as this node is alive, the command will be retried until committed
// (e.g. in the event of leader failover). There is no guarantee that commands will be
// committed in the same order as they were originally submitted.
func (m *MultiRaft) SubmitCommand(groupID uint64, commandID string, command []byte) chan struct{} {
	log.V(6).Infof("node %v submitting command to group %v", m.nodeID, groupID)
	ch := make(chan struct{})
	m.proposalChan <- proposal{
		groupID:   groupID,
		commandID: commandID,
		fn: func() {
			m.multiNode.Propose(context.Background(), groupID, encodeCommand(commandID, command))
		},
		ch: ch,
	}
	return ch
}

// ChangeGroupMembership submits a proposed membership change to the cluster.
func (m *MultiRaft) ChangeGroupMembership(groupID uint64, commandID string,
	changeType raftpb.ConfChangeType, nodeID uint64) chan struct{} {
	log.V(6).Infof("node %v proposing membership change to group %v", m.nodeID, groupID)
	ch := make(chan struct{})
	m.proposalChan <- proposal{
		groupID:   groupID,
		commandID: commandID,
		fn: func() {
			m.multiNode.ProposeConfChange(context.Background(), groupID,
				raftpb.ConfChange{
					Type:    changeType,
					NodeID:  nodeID,
					Context: encodeCommand(commandID, nil),
				})
		},
		ch: ch,
	}
	return ch
}

type proposal struct {
	groupID   uint64
	commandID string
	fn        func()
	ch        chan struct{}
}

// group represents the state of a consensus group.
type group struct {
	// committedTerm is the term of the most recently committed entry.
	committedTerm uint64

	// leader is the node ID of the last known leader for this group, or
	// 0 if an election is in progress.
	leader uint64

	// pending contains all commands that have been proposed but not yet
	// committed. When a proposal is committed, proposal.ch is closed
	// and it is removed from this map.
	pending map[string]proposal
}

type createGroupOp struct {
	groupID uint64
	ch      chan error
}

type removeGroupOp struct {
	groupID uint64
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
	groups        map[uint64]*group
	nodes         map[uint64]*node
	electionTimer *time.Timer
	writeTask     *writeTask
}

func newState(m *MultiRaft) *state {
	return &state{
		MultiRaft: m,
		groups:    make(map[uint64]*group),
		nodes:     make(map[uint64]*node),
		writeTask: newWriteTask(m.Storage),
	}
}

func (s *state) start() {
	log.V(1).Infof("node %v starting", s.nodeID)
	go s.writeTask.start()
	// These maps form a kind of state machine: We don't want to read from the
	// ready channel until the groups we got from the last read have made their
	// way through the rest of the pipeline.
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
		case <-s.stopper.ShouldStop():
			log.V(6).Infof("node %v: stopping", s.nodeID)
			s.stop()
			return

		case op := <-s.createGroupChan:
			log.V(6).Infof("node %v: got op %#v", s.nodeID, op)
			s.createGroup(op)

		case op := <-s.removeGroupChan:
			log.V(6).Infof("node %v: got op %#v", s.nodeID, op)
			s.removeGroup(op)

		case prop := <-s.proposalChan:
			s.propose(prop)

		case readyGroups = <-raftReady:
			s.handleRaftReady(readyGroups)

		case writeReady <- struct{}{}:
			s.handleWriteReady(readyGroups)
			writingGroups = readyGroups
			readyGroups = nil

		case resp := <-s.writeTask.out:
			s.handleWriteResponse(resp, writingGroups)
			s.multiNode.Advance(writingGroups)
			writingGroups = nil

		case <-s.Ticker.Chan():
			log.V(8).Infof("node %v: got tick", s.nodeID)
			s.multiNode.Tick()
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
	s.stopper.SetStopped()
}

// addNode creates a node or increments the refcount on an existing node.
func (s *state) addNode(nodeID uint64) error {
	if node, ok := s.nodes[nodeID]; ok {
		node.refCount++
		return nil
	}
	conn, err := s.Transport.Connect(nodeID)
	if err != nil {
		return err
	}
	s.nodes[nodeID] = &node{nodeID, 1, &asyncClient{nodeID, conn}}
	return nil
}

func (s *state) createGroup(op *createGroupOp) {
	if _, ok := s.groups[op.groupID]; ok {
		op.ch <- util.Errorf("group %v already exists", op.groupID)
		return
	}
	log.V(6).Infof("node %v creating group %v", s.nodeID, op.groupID)

	gs := s.Storage.GroupStorage(op.groupID)
	_, cs, err := gs.InitialState()
	if err != nil {
		op.ch <- err
	}
	for _, nodeID := range cs.Nodes {
		s.addNode(nodeID)
	}

	s.multiNode.CreateGroup(op.groupID, nil, gs)
	s.groups[op.groupID] = &group{
		pending: map[string]proposal{},
	}

	op.ch <- nil
}

func (s *state) removeGroup(op *removeGroupOp) {
	s.multiNode.RemoveGroup(op.groupID)
	delete(s.groups, op.groupID)
	op.ch <- nil
}

func (s *state) propose(p proposal) {
	g := s.groups[p.groupID]
	g.pending[p.commandID] = p
	p.fn()
}

func (s *state) handleRaftReady(readyGroups map[uint64]raft.Ready) {
	// Soft state is updated immediately; everything else waits for handleWriteReady.
	for groupID, ready := range readyGroups {
		if log.V(5) {
			log.Infof("node %v: group %v raft ready", s.nodeID, groupID)
			if ready.SoftState != nil {
				log.Infof("SoftState updated: %+v", *ready.SoftState)
			}
			if !raft.IsEmptyHardState(ready.HardState) {
				log.Infof("HardState updated: %+v", ready.HardState)
			}
			for i, e := range ready.Entries {
				log.Infof("New Entry[%d]: %s", i, raft.DescribeEntry(e))
			}
			for i, e := range ready.CommittedEntries {
				log.Infof("Committed Entry[%d]: %s", i, raft.DescribeEntry(e))
			}
			if !raft.IsEmptySnap(ready.Snapshot) {
				log.Infof("Snapshot updated: %s", ready.Snapshot)
			}
			for i, m := range ready.Messages {
				log.Infof("Outgoing Message[%d]: %s", i, raft.DescribeMessage(m))
			}
		}

		g, ok := s.groups[groupID]
		if !ok {
			// This is a stale message for a removed group
			log.V(4).Infof("node %v: dropping stale ready message for group %v", s.nodeID, groupID)
			continue
		}
		leader, term := g.leader, g.committedTerm
		if ready.SoftState != nil {
			leader = ready.SoftState.Lead
		}
		if len(ready.CommittedEntries) > 0 {
			term = ready.CommittedEntries[len(ready.CommittedEntries)-1].Term
		}
		if term != g.committedTerm || leader != g.leader {
			g.leader, g.committedTerm = leader, term
			s.sendEvent(&EventLeaderElection{groupID, g.leader, g.committedTerm})

			// Re-submit all pending proposals
			for _, prop := range g.pending {
				s.proposalChan <- prop
			}
		}
	}
}

func (s *state) handleWriteReady(readyGroups map[uint64]raft.Ready) {
	log.V(6).Infof("node %v write ready, preparing request", s.nodeID)
	writeRequest := newWriteRequest()
	for groupID, ready := range readyGroups {
		gwr := &groupWriteRequest{}
		if !raft.IsEmptyHardState(ready.HardState) {
			gwr.state = ready.HardState
		}
		if len(ready.Entries) > 0 {
			gwr.entries = ready.Entries
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
		g, ok := s.groups[groupID]
		if !ok {
			log.V(4).Infof("dropping stale write to group %v", groupID)
			continue
		}
		for _, entry := range ready.CommittedEntries {
			var commandID string
			switch entry.Type {
			case raftpb.EntryNormal:
				// etcd raft occasionally adds a nil entry (e.g. upon election); ignore these.
				if entry.Data != nil {
					var command []byte
					commandID, command = decodeCommand(entry.Data)
					s.sendEvent(&EventCommandCommitted{commandID, command})
				}
			case raftpb.EntryConfChange:
				cc := raftpb.ConfChange{}
				err := cc.Unmarshal(entry.Data)
				if err != nil {
					log.Fatalf("invalid ConfChange data: %s", err)
				}
				if len(cc.Context) > 0 {
					commandID, _ = decodeCommand(cc.Context)
				}
				log.V(3).Infof("node %v applying configuration change %v", s.nodeID, cc)
				// TODO(bdarnell): dedupe by keeping a record of recently-applied commandIDs
				// TODO(bdarnell): support removing nodes; fix double-application of initial entries
				err = s.addNode(cc.NodeID)
				if err != nil {
					log.Errorf("error applying configuration change %v: %s", cc, err)
				}
				s.multiNode.ApplyConfChange(groupID, cc)
			}
			if p, ok := g.pending[commandID]; ok {
				// TODO(bdarnell): the command is now committed, but not applied until the
				// application consumes EventCommandCommitted. Is closing the channel
				// at this point useful or do we need to wait for the command to be
				// applied too?
				close(p.ch)
				delete(g.pending, commandID)
			}
		}
		for _, msg := range ready.Messages {
			log.V(6).Infof("node %v sending message %s to %v", s.nodeID,
				raft.DescribeMessage(msg), msg.To)
			s.nodes[msg.To].client.raftMessage(&RaftMessageRequest{groupID, msg})
		}
	}
}
