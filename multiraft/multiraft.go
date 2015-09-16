// Copyright 2015 The Cockroach Authors.
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
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"golang.org/x/net/context"
)

const (
	noGroup = proto.RangeID(0)

	reqBufferSize = 100
)

// An ErrGroupDeleted is returned for commands which are pending while their
// group is deleted.
var ErrGroupDeleted = errors.New("raft group deleted")

// ErrStopped is returned for commands that could not be completed before the
// node was stopped.
var ErrStopped = errors.New("raft processing stopped")

// Config contains the parameters necessary to construct a MultiRaft object.
type Config struct {
	Storage   Storage
	Transport Transport
	// Ticker may be nil to use real time and TickInterval.
	Ticker Ticker
	// StateMachine may be nil if the state machine is transient and always starts from
	// a blank slate.
	StateMachine StateMachine

	// A new election is called if the election timeout elapses with no
	// contact from the leader.  The actual timeout is chosen randomly
	// from the range [ElectionTimeoutTicks*TickInterval,
	// ElectionTimeoutTicks*TickInterval*2) to minimize the chances of
	// several servers trying to become leaders simultaneously. The Raft
	// paper suggests a range of 150-300ms for local networks;
	// geographically distributed installations should use higher values
	// to account for the increased round trip time.
	ElectionTimeoutTicks   int
	HeartbeatIntervalTicks int
	TickInterval           time.Duration

	// EventBufferSize is the capacity (in number of events) of the
	// MultiRaft.Events channel. In tests, we use 0 to ensure that there
	// are no deadlocks when the limit is reached; real deployments may
	// want to set a buffer so that applying a command committed on one
	// group does not interfere with other groups or cause heartbeats to
	// be missed.
	EventBufferSize int

	EntryFormatter raft.EntryFormatter
}

// validate returns an error if any required elements of the Config are missing or invalid.
// Called automatically by NewMultiRaft.
func (c *Config) validate() error {
	if c.Transport == nil {
		return util.Errorf("Transport is required")
	}
	if c.ElectionTimeoutTicks <= 0 {
		return util.Errorf("ElectionTimeoutTicks must be greater than zero")
	}
	if c.HeartbeatIntervalTicks <= 0 {
		return util.Errorf("HeartbeatIntervalTicks must be greater than zero")
	}
	if c.TickInterval <= 0 {
		return util.Errorf("TickInterval must be greater than zero")
	}
	return nil
}

// MultiRaft represents a local node in a raft cluster. The owner is responsible for consuming
// the Events channel in a timely manner.
type MultiRaft struct {
	Config
	stopper         *stop.Stopper
	multiNode       raft.MultiNode
	Events          chan interface{}
	nodeID          proto.RaftNodeID
	reqChan         chan *RaftMessageRequest
	createGroupChan chan *createGroupOp
	removeGroupChan chan *removeGroupOp
	proposalChan    chan *proposal
	// callbackChan is a generic hook to run a callback in the raft thread.
	callbackChan chan func()
}

// multiraftServer is a type alias to separate RPC methods
// (which net/rpc finds via reflection) from others.
type multiraftServer MultiRaft

// NewMultiRaft creates a MultiRaft object.
func NewMultiRaft(nodeID proto.RaftNodeID, config *Config, stopper *stop.Stopper) (*MultiRaft, error) {
	if nodeID == 0 {
		return nil, util.Errorf("Invalid RaftNodeID")
	}
	if err := config.validate(); err != nil {
		return nil, err
	}

	if config.Ticker == nil {
		config.Ticker = newTicker(config.TickInterval)
		stopper.AddCloser(config.Ticker)
	}

	if config.EntryFormatter != nil {
		// Wrap the EntryFormatter to strip off the command id.
		ef := config.EntryFormatter
		config.EntryFormatter = func(data []byte) string {
			if len(data) == 0 {
				return "[empty]"
			}
			id, cmd := decodeCommand(data)
			formatted := ef(cmd)
			return fmt.Sprintf("%x: %s", id, formatted)
		}
	}

	m := &MultiRaft{
		Config:    *config,
		stopper:   stopper,
		multiNode: raft.StartMultiNode(uint64(nodeID)),
		nodeID:    nodeID,

		// Output channel.
		Events: make(chan interface{}, config.EventBufferSize),

		// Input channels.
		reqChan:         make(chan *RaftMessageRequest, reqBufferSize),
		createGroupChan: make(chan *createGroupOp),
		removeGroupChan: make(chan *removeGroupOp),
		proposalChan:    make(chan *proposal),
		callbackChan:    make(chan func()),
	}

	if err := m.Transport.Listen(nodeID, (*multiraftServer)(m)); err != nil {
		return nil, err
	}

	return m, nil
}

// Start runs the raft algorithm in a background goroutine.
func (m *MultiRaft) Start() {
	newState(m).start()
}

// RaftMessage implements ServerInterface; this method is called by net/rpc
// when we receive a message. It returns as soon as the request has been
// enqueued without waiting for it to be processed.
func (ms *multiraftServer) RaftMessage(req *RaftMessageRequest) (*RaftMessageResponse, error) {
	select {
	case ms.reqChan <- req:
		return nil, nil
	case <-ms.stopper.ShouldStop():
		return nil, ErrStopped
	}
}

func (m *MultiRaft) sendEvent(event interface{}) {
	select {
	case m.Events <- event:
	case <-m.stopper.ShouldStop():
	}
}

// fanoutHeartbeat sends the given heartbeat to all groups which believe that
// their leader resides on the sending node.
func (s *state) fanoutHeartbeat(req *RaftMessageRequest) {
	// A heartbeat message is expanded into a heartbeat for each group
	// that the remote node is a part of.
	fromID := proto.RaftNodeID(req.Message.From)
	groupCount := 0
	followerCount := 0
	if originNode, ok := s.nodes[fromID]; ok {
		for groupID := range originNode.groupIDs {
			groupCount++
			// If we don't think that the sending node is leading that group, don't
			// propagate.
			if s.groups[groupID].leader != fromID || fromID == s.nodeID {
				if log.V(8) {
					log.Infof("node %v: not fanning out heartbeat to %v, msg is from %d and leader is %d",
						s.nodeID, req.Message.To, fromID, s.groups[groupID].leader)
				}
				continue
			}
			followerCount++
			if err := s.multiNode.Step(context.Background(), uint64(groupID), req.Message); err != nil {
				if log.V(4) {
					log.Infof("node %v: coalesced heartbeat step to group %v failed for message %s", s.nodeID, groupID,
						raft.DescribeMessage(req.Message, s.EntryFormatter))
				}
			}
		}
	}
	// We must respond whether we forwarded the heartbeat to any groups
	// or not. When a leader considers a follower to be down, it doesn't
	// begin recovery until the follower has successfully responded to a
	// heartbeat. If we get a heartbeat from a node we don't know, it
	// must think we are a follower of some group, so we need to respond
	// so it can activate the recovery process.
	s.sendMessage(nil,
		raftpb.Message{
			From: uint64(s.nodeID),
			To:   req.Message.From,
			Type: raftpb.MsgHeartbeatResp,
		})
	if log.V(7) {
		log.Infof("node %v: received coalesced heartbeat from node %v; "+
			"fanned out to %d followers in %d overlapping groups",
			s.nodeID, fromID, followerCount, groupCount)
	}
}

// fanoutHeartbeatResponse sends the given heartbeat response to all groups
// which overlap with the sender's groups and consider themselves leader.
func (s *state) fanoutHeartbeatResponse(fromID proto.RaftNodeID) {
	originNode, ok := s.nodes[fromID]
	if !ok {
		log.Warningf("node %v: not fanning out heartbeat response from unknown node %v",
			s.nodeID, fromID)
		return
	}
	// Term in HeartbeatResponse is no meaning in fanouting. Otherwise it
	// will cause Leader change to Follower if another group's term is
	// greater than this.
	req := raftpb.Message{
		From: uint64(fromID),
		To:   uint64(s.nodeID),
		Type: raftpb.MsgHeartbeatResp,
	}

	cnt := 0
	for groupID := range originNode.groupIDs {
		// If we don't think that the local node is leader, don't propagate.
		if s.groups[groupID].leader != s.nodeID || fromID == s.nodeID {
			if log.V(8) {
				log.Infof("node %v: not fanning out heartbeat response to %v, msg is from %v and leader is %v",
					s.nodeID, groupID, fromID, s.groups[groupID].leader)
			}
			continue
		}
		if err := s.multiNode.Step(context.Background(), uint64(groupID), req); err != nil {
			if log.V(4) {
				log.Infof("node %v: coalesced heartbeat response step to group %v failed", s.nodeID, groupID)
			}
		}
		cnt++
	}
	if log.V(7) {
		log.Infof("node %v: received coalesced heartbeat response from node %v; "+
			"fanned out to %d leaders in %d overlapping groups",
			s.nodeID, fromID, cnt, len(originNode.groupIDs))
	}
}

// CreateGroup creates a new consensus group and joins it. The initial membership of this
// group is determined by the InitialState method of the group's Storage object.
func (m *MultiRaft) CreateGroup(groupID proto.RangeID) error {
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
func (m *MultiRaft) RemoveGroup(groupID proto.RangeID) error {
	op := &removeGroupOp{
		groupID: groupID,
		ch:      make(chan error, 1),
	}
	m.removeGroupChan <- op
	return <-op.ch
}

// SubmitCommand sends a command (a binary blob) to the cluster. This method returns
// when the command has been successfully sent, not when it has been committed.
// An error or nil will be written to the returned channel when the command has
// been committed or aborted.
func (m *MultiRaft) SubmitCommand(groupID proto.RangeID, commandID string, command []byte) <-chan error {
	if log.V(6) {
		log.Infof("node %v submitting command to group %v", m.nodeID, groupID)
	}
	ch := make(chan error, 1)
	m.proposalChan <- &proposal{
		groupID:   groupID,
		commandID: commandID,
		fn: func() {
			if err := m.multiNode.Propose(context.Background(), uint64(groupID),
				encodeCommand(commandID, command)); err != nil {
				log.Errorf("node %v: error proposing command to group %v: %s", m.nodeID, groupID, err)
			}
		},
		ch: ch,
	}
	return ch
}

// ChangeGroupMembership submits a proposed membership change to the cluster.
// Payload is an opaque blob that will be returned in EventMembershipChangeCommitted.
func (m *MultiRaft) ChangeGroupMembership(groupID proto.RangeID, commandID string,
	changeType raftpb.ConfChangeType, nodeID proto.RaftNodeID, payload []byte) <-chan error {
	if log.V(6) {
		log.Infof("node %v proposing membership change to group %v", m.nodeID, groupID)
	}
	ch := make(chan error, 1)
	m.proposalChan <- &proposal{
		groupID:   groupID,
		commandID: commandID,
		fn: func() {
			if err := m.multiNode.ProposeConfChange(context.Background(), uint64(groupID),
				raftpb.ConfChange{
					Type:    changeType,
					NodeID:  uint64(nodeID),
					Context: encodeCommand(commandID, payload),
				},
			); err != nil {
				log.Errorf("node %v: error proposing membership change to node %v: %s", m.nodeID,
					groupID, err)
			}

		},
		ch: ch,
	}
	return ch
}

// Status returns the current status of the given group.
func (m *MultiRaft) Status(groupID proto.RangeID) *raft.Status {
	return m.multiNode.Status(uint64(groupID))
}

type proposal struct {
	groupID   proto.RangeID
	commandID string
	fn        func()
	ch        chan<- error
}

// group represents the state of a consensus group.
type group struct {
	id proto.RangeID

	// committedTerm is the term of the most recently committed entry.
	committedTerm uint64

	// leader is the node ID of the last known leader for this group, or
	// 0 if an election is in progress.
	leader proto.RaftNodeID

	// pending contains all commands that have been proposed but not yet
	// committed in the current term. When a proposal is committed, nil
	// is written to proposal.ch and it is removed from this
	// map.
	pending map[string]*proposal
	// writing is true while an active writeTask exists for this group.
	// We need to keep track of this since groups can be removed and
	// re-added at any time, and we don't want a writeTask started by
	// an earlier incarnation to be fed into a later one.
	writing bool
	// nodeIDs track the remote nodes associated with this group.
	nodeIDs []proto.RaftNodeID
}

type createGroupOp struct {
	groupID proto.RangeID
	ch      chan error
}

type removeGroupOp struct {
	groupID proto.RangeID
	ch      chan error
}

// node represents a connection to a remote node.
type node struct {
	nodeID   proto.RaftNodeID
	groupIDs map[proto.RangeID]struct{}
}

func (n *node) registerGroup(groupID proto.RangeID) {
	if groupID == noGroup {
		panic("must not call registerGroup with noGroup")
	}
	n.groupIDs[groupID] = struct{}{}
}

func (n *node) unregisterGroup(groupID proto.RangeID) {
	delete(n.groupIDs, groupID)
}

// state represents the internal state of a MultiRaft object. All variables here
// are accessible only from the state.start goroutine so they can be accessed without
// synchronization.
type state struct {
	*MultiRaft
	groups    map[proto.RangeID]*group
	nodes     map[proto.RaftNodeID]*node
	writeTask *writeTask
}

func newState(m *MultiRaft) *state {
	return &state{
		MultiRaft: m,
		groups:    make(map[proto.RangeID]*group),
		nodes:     make(map[proto.RaftNodeID]*node),
		writeTask: newWriteTask(m.Storage),
	}
}

func (s *state) start() {
	s.stopper.RunWorker(func() {
		defer s.stop()
		if log.V(1) {
			log.Infof("node %v starting", s.nodeID)
		}
		s.writeTask.start(s.stopper)
		// These maps form a kind of state machine: We don't want to read from the
		// ready channel until the groups we got from the last read have made their
		// way through the rest of the pipeline.
		var readyGroups map[uint64]raft.Ready
		var writingGroups map[uint64]raft.Ready
		// Counts up to heartbeat interval and is then reset.
		ticks := 0
		for {
			// raftReady signals that the Raft state machine has pending
			// work. That work is supplied over the raftReady channel as a map
			// from group ID to raft.Ready struct.
			var raftReady <-chan map[uint64]raft.Ready
			// writeReady is set to the write task's ready channel, which
			// receives when the write task is prepared to persist ready data
			// from the Raft state machine.
			// The writeReady mechanism is currently disabled as we are testing
			// performing all writes synchronously.
			// TODO(bdarnell): either reinstate writeReady or rip it out completely.
			//var writeReady chan struct{}

			// The order of operations in this loop structure is as follows:
			// start by setting raftReady to the multiNode's Ready()
			// channel. Once a new raftReady has been consumed from the
			// channel, set writeReady to the write task's ready channel and
			// set raftReady back to nil. This advances our read-from-raft /
			// write-to-storage state machine to the next step: wait for the
			// write task to be ready to persist the new data.
			if readyGroups != nil {
				//writeReady = s.writeTask.ready
			} else if writingGroups == nil {
				raftReady = s.multiNode.Ready()
			}

			if log.V(8) {
				log.Infof("node %v: selecting", s.nodeID)
			}
			select {
			case <-s.stopper.ShouldStop():
				return

			case req := <-s.reqChan:
				if log.V(5) {
					log.Infof("node %v: group %v got message %.200s", s.nodeID, req.GroupID,
						raft.DescribeMessage(req.Message, s.EntryFormatter))
				}
				switch req.Message.Type {
				case raftpb.MsgHeartbeat:
					s.fanoutHeartbeat(req)
				case raftpb.MsgHeartbeatResp:
					s.fanoutHeartbeatResponse(proto.RaftNodeID(req.Message.From))
				default:
					// We only want to lazily create the group if it's not heartbeat-related;
					// our heartbeats are coalesced and contain a dummy GroupID.
					// TODO(tschottdorf) still shouldn't hurt to move this part outside,
					// but suddenly tests will start failing. Should investigate.
					if _, ok := s.groups[req.GroupID]; !ok {
						if log.V(1) {
							log.Infof("node %v: got message for unknown group %d; creating it", s.nodeID, req.GroupID)
						}
						if err := s.createGroup(req.GroupID); err != nil {
							log.Warningf("Error creating group %d: %s", req.GroupID, err)
							break
						}
					}

					if err := s.multiNode.Step(context.Background(), uint64(req.GroupID), req.Message); err != nil {
						if log.V(4) {
							log.Infof("node %v: multinode step to group %v failed for message %.200s", s.nodeID, req.GroupID,
								raft.DescribeMessage(req.Message, s.EntryFormatter))
						}
					}
				}
			case op := <-s.createGroupChan:
				if log.V(6) {
					log.Infof("node %v: got op %#v", s.nodeID, op)
				}
				op.ch <- s.createGroup(op.groupID)

			case op := <-s.removeGroupChan:
				if log.V(6) {
					log.Infof("node %v: got op %#v", s.nodeID, op)
				}
				op.ch <- s.removeGroup(op.groupID, readyGroups)

			case prop := <-s.proposalChan:
				s.propose(prop)

			case readyGroups = <-raftReady:
				// readyGroups are saved in a local variable until they can be sent to
				// the write task (and then the real work happens after the write is
				// complete). All we do for now is log them.
				s.logRaftReady(readyGroups)

				select {
				case s.writeTask.ready <- struct{}{}:
				case <-s.stopper.ShouldStop():
					return
				}
				s.handleWriteReady(readyGroups)
				writingGroups = readyGroups
				readyGroups = nil

				select {
				case resp := <-s.writeTask.out:
					s.handleWriteResponse(resp, writingGroups)
					s.multiNode.Advance(writingGroups)
					writingGroups = nil
				case <-s.stopper.ShouldStop():
					return
				}

			case <-s.Ticker.Chan():
				if log.V(8) {
					log.Infof("node %v: got tick", s.nodeID)
				}
				s.multiNode.Tick()
				ticks++
				if ticks >= s.HeartbeatIntervalTicks {
					ticks = 0
					s.coalescedHeartbeat()
				}

			case cb := <-s.callbackChan:
				cb()
			}
		}
	})
}

func (s *state) removePending(g *group, prop *proposal, err error) {
	if prop == nil {
		return
	}
	// Because of the way we re-queue proposals during leadership
	// changes, we may finish the same proposal object twice.
	if prop.ch != nil {
		prop.ch <- err
		prop.ch = nil
	}
	if g != nil {
		delete(g.pending, prop.commandID)
	}
}

func (s *state) coalescedHeartbeat() {
	// TODO(Tobias): We don't need to send heartbeats to nodes that have
	// no group following one of our local groups. But that's unlikely
	// to be the case for many of our nodes. It could make sense though
	// to space out the heartbeats over the heartbeat interval so that
	// we don't try to send for all nodes at once.
	for nodeID := range s.nodes {
		// Don't heartbeat yourself.
		if nodeID == s.nodeID {
			continue
		}
		if log.V(6) {
			log.Infof("node %v: triggering coalesced heartbeat to node %v", s.nodeID, nodeID)
		}
		s.sendMessage(nil,
			raftpb.Message{
				From: uint64(s.nodeID),
				To:   uint64(nodeID),
				Type: raftpb.MsgHeartbeat,
			})
	}
}

func (s *state) stop() {
	if log.V(6) {
		log.Infof("node %v stopping", s.nodeID)
	}
	s.MultiRaft.multiNode.Stop()
	s.MultiRaft.Transport.Stop(s.nodeID)
}

// addNode creates a node and registers the given group (if not nil)
// for that node.
func (s *state) addNode(nodeID proto.RaftNodeID, g *group) error {
	newNode, ok := s.nodes[nodeID]
	if !ok {
		s.nodes[nodeID] = &node{
			nodeID:   nodeID,
			groupIDs: make(map[proto.RangeID]struct{}),
		}
		newNode = s.nodes[nodeID]
	}

	if g != nil {
		newNode.registerGroup(g.id)
		g.nodeIDs = append(g.nodeIDs, nodeID)
	}
	return nil
}

// removeNode removes a node from a group.
func (s *state) removeNode(nodeID proto.RaftNodeID, g *group) error {
	node, ok := s.nodes[nodeID]
	if !ok {
		return util.Errorf("cannot remove unknown node %s", nodeID)
	}

	for i := range g.nodeIDs {
		if g.nodeIDs[i] == nodeID {
			g.nodeIDs[i] = g.nodeIDs[len(g.nodeIDs)-1]
			g.nodeIDs = g.nodeIDs[:len(g.nodeIDs)-1]
			break
		}
	}
	// TODO(bdarnell): when a node has no more groups, remove it.
	node.unregisterGroup(g.id)

	// Cancel any outstanding proposals.
	if nodeID == s.nodeID {
		for _, prop := range g.pending {
			s.removePending(g, prop, ErrGroupDeleted)
		}
	}

	return nil
}

func (s *state) createGroup(groupID proto.RangeID) error {
	if _, ok := s.groups[groupID]; ok {
		return nil
	}
	if log.V(3) {
		log.Infof("node %v creating group %v", s.nodeID, groupID)
	}

	gs := s.Storage.GroupStorage(groupID)
	_, cs, err := gs.InitialState()
	if err != nil {
		return err
	}

	var appliedIndex uint64
	if s.StateMachine != nil {
		appliedIndex, err = s.StateMachine.AppliedIndex(groupID)
		if err != nil {
			return err
		}
	}

	raftCfg := &raft.Config{
		Applied:       appliedIndex,
		ElectionTick:  s.ElectionTimeoutTicks,
		HeartbeatTick: s.HeartbeatIntervalTicks,
		Storage:       gs,
		// TODO(bdarnell): make these configurable; evaluate defaults.
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		Logger:          &raftLogger{group: uint64(groupID)},
	}
	if err := s.multiNode.CreateGroup(uint64(groupID), raftCfg, nil); err != nil {
		return err
	}
	g := &group{
		id:      groupID,
		pending: map[string]*proposal{},
	}
	s.groups[groupID] = g

	for _, nodeID := range cs.Nodes {
		if err := s.addNode(proto.RaftNodeID(nodeID), g); err != nil {
			return err
		}
	}

	// Automatically campaign and elect a leader for this group if there's
	// exactly one known node for this group.
	//
	// A grey area for this being correct happens in the case when we're
	// currently in the progress of adding a second node to the group,
	// with the change committed but not applied.
	// Upon restarting, the node would immediately elect itself and only
	// then apply the config change, where really it should be applying
	// first and then waiting for the majority (which would now require
	// two votes, not only its own).
	// However, in that special case, the second node has no chance to
	// be elected master while this node restarts (as it's aware of the
	// configuration and knows it needs two votes), so the worst that
	// could happen is both nodes ending up in candidate state, timing
	// out and then voting again. This is expected to be an extremely
	// rare event.
	if len(cs.Nodes) == 1 && s.MultiRaft.nodeID == proto.RaftNodeID(cs.Nodes[0]) {
		return s.multiNode.Campaign(context.Background(), uint64(groupID))
	}
	return nil
}

func (s *state) removeGroup(groupID proto.RangeID, readyGroups map[uint64]raft.Ready) error {
	// Group creation is lazy and idempotent; so is removal.
	g, ok := s.groups[groupID]
	if !ok {
		return nil
	}
	if log.V(3) {
		log.Infof("node %v removing group %v", s.nodeID, groupID)
	}

	// Cancel commands which are still in transit.
	for _, prop := range g.pending {
		s.removePending(g, prop, ErrGroupDeleted)
	}

	if err := s.multiNode.RemoveGroup(uint64(groupID)); err != nil {
		return err
	}

	for _, nodeID := range g.nodeIDs {
		s.nodes[nodeID].unregisterGroup(groupID)
	}

	// Delete any entries for this group in readyGroups.
	if readyGroups != nil {
		delete(readyGroups, uint64(groupID))
	}

	delete(s.groups, groupID)
	return nil
}

func (s *state) propose(p *proposal) {
	g, ok := s.groups[p.groupID]
	if !ok {
		s.removePending(nil /* group */, p, ErrGroupDeleted)
		return
	}

	found := false
	for _, nodeID := range g.nodeIDs {
		if nodeID == s.nodeID {
			found = true
			break
		}
	}
	if !found {
		// If we are not a member of the group, don't allow any proposals.
		s.removePending(nil, p, ErrGroupDeleted)
		return
	}

	if log.V(3) {
		log.Infof("group %d: new proposal %x", p.groupID, p.commandID)
	}
	g.pending[p.commandID] = p
	p.fn()
}

func (s *state) logRaftReady(readyGroups map[uint64]raft.Ready) {
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
				log.Infof("New Entry[%d]: %.200s", i, raft.DescribeEntry(e, s.EntryFormatter))
			}
			for i, e := range ready.CommittedEntries {
				log.Infof("Committed Entry[%d]: %.200s", i, raft.DescribeEntry(e, s.EntryFormatter))
			}
			if !raft.IsEmptySnap(ready.Snapshot) {
				log.Infof("Snapshot updated: %.200s", ready.Snapshot.String())
			}
			for i, m := range ready.Messages {
				log.Infof("Outgoing Message[%d]: %.200s", i, raft.DescribeMessage(m, s.EntryFormatter))
			}
		}
	}
}

// handleWriteReady converts a set of raft.Ready structs into a writeRequest
// to be persisted, marks the group as writing and sends it to the writeTask.
func (s *state) handleWriteReady(readyGroups map[uint64]raft.Ready) {
	if log.V(6) {
		log.Infof("node %v write ready, preparing request", s.nodeID)
	}
	writeRequest := newWriteRequest()
	for groupID, ready := range readyGroups {
		raftGroupID := proto.RangeID(groupID)
		g, ok := s.groups[raftGroupID]
		if !ok {
			if log.V(6) {
				log.Infof("dropping write request to group %d", groupID)
			}
			continue
		}
		g.writing = true

		gwr := &groupWriteRequest{}
		if !raft.IsEmptyHardState(ready.HardState) {
			gwr.state = ready.HardState
		}
		if !raft.IsEmptySnap(ready.Snapshot) {
			gwr.snapshot = ready.Snapshot
		}
		if len(ready.Entries) > 0 {
			gwr.entries = ready.Entries
		}
		writeRequest.groups[raftGroupID] = gwr
	}
	s.writeTask.in <- writeRequest
}

// processCommittedEntry tells the application that a command was committed.
// Returns the commandID, or an empty string if the given entry was not a command.
func (s *state) processCommittedEntry(groupID proto.RangeID, g *group, entry raftpb.Entry) string {
	var commandID string
	switch entry.Type {
	case raftpb.EntryNormal:
		// etcd raft occasionally adds a nil entry (e.g. upon election); ignore these.
		if entry.Data != nil {
			var command []byte
			commandID, command = decodeCommand(entry.Data)
			s.sendEvent(&EventCommandCommitted{
				GroupID:   groupID,
				CommandID: commandID,
				Command:   command,
				Index:     entry.Index,
			})
		}

	case raftpb.EntryConfChange:
		cc := raftpb.ConfChange{}
		if err := cc.Unmarshal(entry.Data); err != nil {
			log.Fatalf("invalid ConfChange data: %s", err)
		}
		var payload []byte
		if len(cc.Context) > 0 {
			commandID, payload = decodeCommand(cc.Context)
		}
		s.sendEvent(&EventMembershipChangeCommitted{
			GroupID:    groupID,
			CommandID:  commandID,
			Index:      entry.Index,
			NodeID:     proto.RaftNodeID(cc.NodeID),
			ChangeType: cc.Type,
			Payload:    payload,
			Callback: func(err error) {
				select {
				case s.callbackChan <- func() {
					if err == nil {
						if log.V(3) {
							log.Infof("node %v applying configuration change %v", s.nodeID, cc)
						}
						// TODO(bdarnell): dedupe by keeping a record of recently-applied commandIDs
						switch cc.Type {
						case raftpb.ConfChangeAddNode:
							err = s.addNode(proto.RaftNodeID(cc.NodeID), g)
						case raftpb.ConfChangeRemoveNode:
							err = s.removeNode(proto.RaftNodeID(cc.NodeID), g)
						case raftpb.ConfChangeUpdateNode:
							// Updates don't concern multiraft, they are simply passed through.
						}
						if err != nil {
							log.Errorf("error applying configuration change %v: %s", cc, err)
						}
						s.multiNode.ApplyConfChange(uint64(groupID), cc)
					} else {
						log.Warningf("aborting configuration change: %s", err)
						s.multiNode.ApplyConfChange(uint64(groupID),
							raftpb.ConfChange{})
					}

					// Re-submit all pending proposals, in case any of them were config changes
					// that were dropped due to the one-at-a-time rule. This is a little
					// redundant since most pending proposals won't benefit from this but
					// config changes should be rare enough (and the size of the pending queue
					// small enough) that it doesn't really matter.
					for _, prop := range g.pending {
						s.propose(prop)
					}
				}:
				case <-s.stopper.ShouldStop():
				}
			},
		})
	}
	return commandID
}

// sendMessage sends a raft message on the given group. Coalesced heartbeats
// address nodes, not groups; they will use the noGroup constant as groupID.
func (s *state) sendMessage(g *group, msg raftpb.Message) {
	if log.V(6) {
		log.Infof("node %v sending message %.200s to %v", s.nodeID,
			raft.DescribeMessage(msg, s.EntryFormatter), msg.To)
	}
	nodeID := proto.RaftNodeID(msg.To)
	groupID := noGroup
	if g != nil {
		groupID = g.id
	}
	if _, ok := s.nodes[nodeID]; !ok {
		if log.V(4) {
			log.Infof("node %v: connecting to new node %v", s.nodeID, nodeID)
		}
		if err := s.addNode(nodeID, g); err != nil {
			log.Errorf("node %v: error adding group %v to node %v: %v",
				s.nodeID, groupID, nodeID, err)
		}
	}
	err := s.Transport.Send(&RaftMessageRequest{groupID, msg})
	snapStatus := raft.SnapshotFinish
	if err != nil {
		log.Warningf("node %v failed to send message to %v: %s", s.nodeID, nodeID, err)
		if groupID != noGroup {
			s.multiNode.ReportUnreachable(msg.To, uint64(groupID))
		}
		snapStatus = raft.SnapshotFailure
	}
	if msg.Type == raftpb.MsgSnap {
		// TODO(bdarnell): add an ack for snapshots and don't report status until
		// ack, error, or timeout.
		if groupID != noGroup {
			s.multiNode.ReportSnapshot(msg.To, uint64(groupID), snapStatus)
		}
	}
}

// maybeSendLeaderEvent processes a raft.Ready to send events in response to leadership
// changes (this includes both sending an event to the app and retrying any pending
// proposals).
func (s *state) maybeSendLeaderEvent(groupID proto.RangeID, g *group, ready *raft.Ready) {
	term := g.committedTerm
	if ready.SoftState != nil {
		// Always save the leader whenever we get a SoftState.
		g.leader = proto.RaftNodeID(ready.SoftState.Lead)
	}
	if len(ready.CommittedEntries) > 0 {
		term = ready.CommittedEntries[len(ready.CommittedEntries)-1].Term
	}
	if term != g.committedTerm && g.leader != 0 {
		// Whenever the committed term has advanced and we know our leader,
		// emit an event.
		g.committedTerm = term
		s.sendEvent(&EventLeaderElection{
			GroupID: groupID,
			NodeID:  proto.RaftNodeID(g.leader),
			Term:    g.committedTerm,
		})

		// Re-submit all pending proposals
		for _, prop := range g.pending {
			s.propose(prop)
		}
	}
}

// handleWriteResponse updates the state machine and sends messages for a raft Ready batch.
func (s *state) handleWriteResponse(response *writeResponse, readyGroups map[uint64]raft.Ready) {
	if log.V(6) {
		log.Infof("node %v got write response: %#v", s.nodeID, *response)
	}
	// Everything has been written to disk; now we can apply updates to the state machine
	// and send outgoing messages.
	for groupID, ready := range readyGroups {
		raftGroupID := proto.RangeID(groupID)
		g, ok := s.groups[raftGroupID]
		if !ok {
			if log.V(4) {
				log.Infof("dropping stale write to group %v", groupID)
			}
			continue
		} else if !g.writing {
			if log.V(4) {
				log.Infof("dropping stale write to reincarnation of group %v", groupID)
			}
			delete(readyGroups, groupID) // they must not make it to Advance.
			continue
		}
		g.writing = false

		// Process committed entries.
		for _, entry := range ready.CommittedEntries {
			commandID := s.processCommittedEntry(raftGroupID, g, entry)
			// TODO(bdarnell): the command is now committed, but not applied until the
			// application consumes EventCommandCommitted. Is returning via the channel
			// at this point useful or do we need to wait for the command to be
			// applied too?
			// This could be done with a Callback as in EventMembershipChangeCommitted
			// or perhaps we should move away from a channel to a callback-based system.
			s.removePending(g, g.pending[commandID], nil /* err */)
		}

		if !raft.IsEmptySnap(ready.Snapshot) {
			// Sync the group/node mapping with the information contained in the snapshot.
			for _, nodeID := range ready.Snapshot.Metadata.ConfState.Nodes {
				// TODO(bdarnell): if we had any information that predated this snapshot
				// we must remove those nodes.
				if err := s.addNode(proto.RaftNodeID(nodeID), g); err != nil {
					log.Errorf("node %v: error adding node %v", s.nodeID, nodeID)
				}
			}
		}

		// Process SoftState and leader changes.
		s.maybeSendLeaderEvent(raftGroupID, g, &ready)

		// Send all messages.
		for _, msg := range ready.Messages {
			switch msg.Type {
			case raftpb.MsgHeartbeat:
				if log.V(8) {
					log.Infof("node %v dropped individual heartbeat to node %v",
						s.nodeID, msg.To)
				}
			case raftpb.MsgHeartbeatResp:
				if log.V(8) {
					log.Infof("node %v dropped individual heartbeat response to node %v",
						s.nodeID, msg.To)
				}
			default:
				s.sendMessage(g, msg)
			}
		}
	}
}
