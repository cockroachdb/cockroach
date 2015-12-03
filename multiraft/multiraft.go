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

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/cache"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

const (
	noGroup = roachpb.RangeID(0)

	reqBufferSize = 100

	// TODO(bdarnell): Determine the right size for this cache. Should
	// the cache be partitioned so that replica descriptors from the
	// range descriptors (which are the bulk of the data and can be
	// reloaded from disk as needed) don't crowd out the
	// message/snapshot descriptors (whose necessity is short-lived but
	// cannot be recovered through other means if evicted)?
	maxReplicaDescCacheSize = 1000
)

// ErrGroupDeleted is returned for commands which are pending while their
// group is deleted.
var ErrGroupDeleted = errors.New("raft group deleted")

// ErrStopped is returned for commands that could not be completed before the
// node was stopped.
var ErrStopped = errors.New("raft processing stopped")

// ErrReplicaIDMismatch is returned for commands which have different replicaID
// with that group.
var ErrReplicaIDMismatch = errors.New("raft group replicaID mismatch")

// Config contains the parameters necessary to construct a MultiRaft object.
type Config struct {
	Storage   Storage
	Transport Transport
	// Ticker may be nil to use real time and TickInterval.
	Ticker Ticker

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
	Events          chan []interface{}
	nodeID          roachpb.NodeID
	storeID         roachpb.StoreID
	reqChan         chan *RaftMessageRequest
	createGroupChan chan createGroupOp
	removeGroupChan chan removeGroupOp
	proposalChan    chan *proposal
	campaignChan    chan roachpb.RangeID
	statusChan      chan statusOp
	// callbackChan is a generic hook to run a callback in the raft thread.
	callbackChan chan func()
}

// multiraftServer is a type alias to separate RPC methods
// (which net/rpc finds via reflection) from others.
type multiraftServer MultiRaft

// NewMultiRaft creates a MultiRaft object.
func NewMultiRaft(nodeID roachpb.NodeID, storeID roachpb.StoreID, config *Config, stopper *stop.Stopper) (*MultiRaft, error) {
	if nodeID <= 0 {
		return nil, util.Errorf("invalid NodeID: %s", nodeID)
	}
	if storeID <= 0 {
		return nil, util.Errorf("invalid StoreID: %s", storeID)
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
		Config:  *config,
		stopper: stopper,
		nodeID:  nodeID,
		storeID: storeID,

		// Output channel.
		Events: make(chan []interface{}),

		// Input channels.
		reqChan:         make(chan *RaftMessageRequest, reqBufferSize),
		createGroupChan: make(chan createGroupOp),
		removeGroupChan: make(chan removeGroupOp),
		proposalChan:    make(chan *proposal),
		campaignChan:    make(chan roachpb.RangeID),
		callbackChan:    make(chan func()),
		statusChan:      make(chan statusOp),
	}

	if err := m.Transport.Listen(storeID, (*multiraftServer)(m)); err != nil {
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

func (s *state) sendEvent(event interface{}) {
	s.pendingEvents = append(s.pendingEvents, event)
}

// fanoutHeartbeat sends the given heartbeat to all groups which believe that
// their leader resides on the sending node.
func (s *state) fanoutHeartbeat(req *RaftMessageRequest) {
	s.lockStorage()
	defer s.unlockStorage()
	// A heartbeat message is expanded into a heartbeat for each group
	// that the remote node is a part of.
	fromID := roachpb.NodeID(req.Message.From)
	groupCount := 0
	followerCount := 0
	if originNode, ok := s.nodes[fromID]; ok {
		for groupID := range originNode.groupIDs {
			groupCount++
			// If we don't think that the sending node is leading that group, don't
			// propagate.
			if s.groups[groupID].leader.NodeID != fromID || fromID == s.nodeID {
				if log.V(8) {
					log.Infof("node %s: not fanning out heartbeat to %s, msg is from %s and leader is %s",
						s.nodeID, groupID, fromID, s.groups[groupID].leader)
				}
				continue
			}

			fromRepID, err := s.Storage().ReplicaIDForStore(groupID, req.FromReplica.StoreID)
			if err != nil {
				if log.V(3) {
					log.Infof("node %s: not fanning out heartbeat to %s, could not find replica id for sending store %s",
						s.nodeID, groupID, req.FromReplica.StoreID)
				}
				continue
			}

			toRepID, err := s.Storage().ReplicaIDForStore(groupID, req.ToReplica.StoreID)
			if err != nil {
				if log.V(3) {
					log.Infof("node %s: not fanning out heartbeat to %s, could not find replica id for receiving store %s",
						s.nodeID, groupID, req.ToReplica.StoreID)
				}
				continue
			}
			followerCount++

			groupMsg := raftpb.Message{
				Type: raftpb.MsgHeartbeat,
				To:   uint64(toRepID),
				From: uint64(fromRepID),
			}

			if err := s.groups[groupID].raftGroup.Step(groupMsg); err != nil {
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
func (s *state) fanoutHeartbeatResponse(req *RaftMessageRequest) {
	s.lockStorage()
	defer s.unlockStorage()
	fromID := roachpb.NodeID(req.Message.From)
	originNode, ok := s.nodes[fromID]
	if !ok {
		log.Warningf("node %v: not fanning out heartbeat response from unknown node %v",
			s.nodeID, fromID)
		return
	}

	cnt := 0
	for groupID := range originNode.groupIDs {
		// If we don't think that the local node is leader, don't propagate.
		if s.groups[groupID].leader.NodeID != s.nodeID || fromID == s.nodeID {
			if log.V(8) {
				log.Infof("node %v: not fanning out heartbeat response to %v, msg is from %v and leader is %v",
					s.nodeID, groupID, fromID, s.groups[groupID].leader)
			}
			continue
		}

		fromRepID, err := s.Storage().ReplicaIDForStore(groupID, req.FromReplica.StoreID)
		if err != nil {
			if log.V(3) {
				log.Infof("node %s: not fanning out heartbeat to %s, could not find replica id for sending store %s",
					s.nodeID, groupID, req.FromReplica.StoreID)
			}
			continue
		}

		toRepID, err := s.Storage().ReplicaIDForStore(groupID, req.ToReplica.StoreID)
		if err != nil {
			if log.V(3) {
				log.Infof("node %s: not fanning out heartbeat to %s, could not find replica id for receiving store %s",
					s.nodeID, groupID, req.ToReplica.StoreID)
			}
			continue
		}

		msg := raftpb.Message{
			Type: raftpb.MsgHeartbeatResp,
			From: uint64(fromRepID),
			To:   uint64(toRepID),
		}

		if err := s.groups[groupID].raftGroup.Step(msg); err != nil {
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
func (m *MultiRaft) CreateGroup(groupID roachpb.RangeID) error {
	op := createGroupOp{
		groupID: groupID,
		ch:      make(chan error, 1),
	}
	m.createGroupChan <- op
	return <-op.ch
}

// RemoveGroup destroys the consensus group with the given ID.
// No events for this group will be emitted after this method returns
// (but some events may still be in the channel buffer).
func (m *MultiRaft) RemoveGroup(groupID roachpb.RangeID, replicaID roachpb.ReplicaID) error {
	op := removeGroupOp{
		groupID:   groupID,
		replicaID: replicaID,
		ch:        make(chan error, 1),
	}
	m.removeGroupChan <- op
	return <-op.ch
}

// SubmitCommand sends a command (a binary blob) to the cluster. This method returns
// when the command has been successfully sent, not when it has been committed.
// An error or nil will be written to the returned channel when the command has
// been committed or aborted.
func (m *MultiRaft) SubmitCommand(groupID roachpb.RangeID, commandID string, command []byte) <-chan error {
	if log.V(6) {
		log.Infof("node %v submitting command to group %v", m.nodeID, groupID)
	}
	ch := make(chan error, 1)
	m.proposalChan <- &proposal{
		groupID:   groupID,
		commandID: commandID,
		fn: func(rn *raft.RawNode) {
			if err := rn.Propose(encodeCommand(commandID, command)); err != nil {
				log.Errorf("node %v: error proposing command to group %v: %s", m.nodeID, groupID, err)
			}
		},
		ch: ch,
	}
	return ch
}

// ChangeGroupMembership submits a proposed membership change to the cluster.
// Payload is an opaque blob that will be returned in EventMembershipChangeCommitted.
func (m *MultiRaft) ChangeGroupMembership(groupID roachpb.RangeID, commandID string,
	changeType raftpb.ConfChangeType, replica roachpb.ReplicaDescriptor, payload []byte) <-chan error {
	if log.V(6) {
		log.Infof("node %v proposing membership change to group %v", m.nodeID, groupID)
	}
	ch := make(chan error, 1)
	if err := replica.Validate(); err != nil {
		ch <- err
		return ch
	}
	m.proposalChan <- &proposal{
		groupID:   groupID,
		commandID: commandID,
		fn: func(rn *raft.RawNode) {
			ctx := ConfChangeContext{
				CommandID: commandID,
				Payload:   payload,
				Replica:   replica,
			}
			encodedCtx, err := ctx.Marshal()
			if err != nil {
				log.Errorf("node %v: error encoding context protobuf", m.nodeID)
				return
			}
			if err := rn.ProposeConfChange(raftpb.ConfChange{
				Type:    changeType,
				NodeID:  uint64(replica.ReplicaID),
				Context: encodedCtx,
			},
			); err != nil {
				log.Errorf("node %v: error proposing membership change to node %v: %s", m.nodeID,
					groupID, err)
				return
			}
		},
		ch: ch,
	}
	return ch
}

// Status returns the current status of the given group.
func (m *MultiRaft) Status(groupID roachpb.RangeID) *raft.Status {
	statusOp := statusOp{
		groupID: groupID,
		ch:      make(chan *raft.Status),
	}
	m.statusChan <- statusOp
	return <-statusOp.ch
}

// Campaign causes this node to start an election. Use with caution as
// contested elections may cause periods of unavailability. Only use
// Campaign() when you can be sure that only one replica will call it.
func (m *MultiRaft) Campaign(groupID roachpb.RangeID) {
	m.campaignChan <- groupID
}

type proposal struct {
	groupID   roachpb.RangeID
	commandID string
	fn        func(*raft.RawNode)
	ch        chan<- error
}

// group represents the state of a consensus group.
type group struct {
	groupID   roachpb.RangeID
	replicaID roachpb.ReplicaID

	// committedTerm is the term of the most recently committed entry.
	committedTerm uint64

	// leader is the last known leader for this group, or all zeros
	// if an election is in progress.
	leader roachpb.ReplicaDescriptor

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
	nodeIDs []roachpb.NodeID
	// raftGroup is the raft.RawNode
	raftGroup *raft.RawNode
}

type createGroupOp struct {
	groupID roachpb.RangeID
	ch      chan error
}

type removeGroupOp struct {
	groupID   roachpb.RangeID
	replicaID roachpb.ReplicaID
	ch        chan error
}

type statusOp struct {
	groupID roachpb.RangeID
	ch      chan *raft.Status
}

// node represents a connection to a remote node.
type node struct {
	nodeID   roachpb.NodeID
	groupIDs map[roachpb.RangeID]struct{}
}

func (n *node) registerGroup(groupID roachpb.RangeID) {
	if groupID == noGroup {
		panic("must not call registerGroup with noGroup")
	}
	n.groupIDs[groupID] = struct{}{}
}

func (n *node) unregisterGroup(groupID roachpb.RangeID) {
	delete(n.groupIDs, groupID)
}

// state represents the internal state of a MultiRaft object. All variables here
// are accessible only from the state.start goroutine so they can be accessed without
// synchronization.
type state struct {
	*MultiRaft
	groups           map[roachpb.RangeID]*group
	nodes            map[roachpb.NodeID]*node
	writeTask        *writeTask
	replicaDescCache *cache.UnorderedCache
	storageLocked    bool
	// Buffer the events and send them in batch to avoid the deadlock
	// between s.Events channel and callbackChan.
	pendingEvents []interface{}
}

func newState(m *MultiRaft) *state {
	return &state{
		MultiRaft: m,
		groups:    make(map[roachpb.RangeID]*group),
		nodes:     make(map[roachpb.NodeID]*node),
		writeTask: newWriteTask(m.Storage),
		replicaDescCache: cache.NewUnorderedCache(cache.Config{
			Policy: cache.CacheLRU,
			ShouldEvict: func(size int, key, value interface{}) bool {
				return size > maxReplicaDescCacheSize
			},
		}),
	}
}

func (s *state) lockStorage() {
	if s.storageLocked {
		panic("storage already locked")
	}
	// s.storageLocked, like all fields of `state`, is only accessed
	// through the state goroutine so it doesn't need any synchronization.
	// The ordering of storageLocked relative to RaftLocker differs between
	// lock and unlock to avoid the panic in Storage().
	s.storageLocked = true
	locker := s.Storage().RaftLocker()
	if locker != nil {
		locker.Lock()
	}
}

func (s *state) unlockStorage() {
	locker := s.Storage().RaftLocker()
	if locker != nil {
		locker.Unlock()
	}
	s.storageLocked = false
}

func (s *state) assertStorageLocked() {
	if !s.storageLocked {
		panic("storage lock must be held")
	}
}

// Storage returns the configured Storage object. It masks direct
// access to the embedded field so we can enforce correct locking.
func (s *state) Storage() Storage {
	s.assertStorageLocked()
	return s.MultiRaft.Storage
}

func (s *state) start() {
	s.stopper.RunWorker(func() {
		defer s.stop()
		if log.V(1) {
			log.Infof("node %v starting", s.nodeID)
		}
		s.writeTask.start(s.stopper)
		// Counts up to heartbeat interval and is then reset.
		ticks := 0
		// checkReadyGroupIDs keeps track of all the groupIDs which
		// should be checked if there is any pending Ready for that group.
		checkReadyGroupIDs := map[roachpb.RangeID]struct{}{}
		for {
			// writeReady is set to the write task's ready channel, which
			// receives when the write task is prepared to persist ready data
			// from the Raft state machine.
			var writeReady chan struct{}
			var eventsChan chan []interface{}

			// If there is pending check, then check if writeTask is ready to do its work.
			if len(checkReadyGroupIDs) > 0 {
				writeReady = s.writeTask.ready
			}

			// If there is any pending events, then check the s.Events to see
			// if it's free to accept pending events.
			if len(s.pendingEvents) > 0 {
				eventsChan = s.Events
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
				s.handleMessage(req)
				checkReadyGroupIDs[req.GroupID] = struct{}{}

			case op := <-s.createGroupChan:
				if log.V(6) {
					log.Infof("node %v: got op %#v", s.nodeID, op)
				}
				op.ch <- s.createGroup(op.groupID, 0)
				checkReadyGroupIDs[op.groupID] = struct{}{}

			case op := <-s.removeGroupChan:
				if log.V(6) {
					log.Infof("node %v: got op %#v", s.nodeID, op)
				}
				op.ch <- s.removeGroup(op.groupID, op.replicaID)

			case prop := <-s.proposalChan:
				s.propose(prop)
				checkReadyGroupIDs[prop.groupID] = struct{}{}

			case groupID := <-s.campaignChan:
				if log.V(6) {
					log.Infof("node %v: got campaign for group %#v", s.nodeID, groupID)
				}
				if _, ok := s.groups[groupID]; !ok {
					if err := s.createGroup(groupID, 0); err != nil {
						log.Warningf("node %s failed to create group %s during MultiRaft.Campaign: %s",
							s.nodeID, groupID, err)
						continue
					}
				}
				if err := s.groups[groupID].raftGroup.Campaign(); err != nil {
					log.Warningf("node %s failed to campaign for group %s: %s", s.nodeID, groupID, err)
				}
				checkReadyGroupIDs[groupID] = struct{}{}

			case writeReady <- struct{}{}:
				writingGroups := s.handleWriteReady(checkReadyGroupIDs)
				checkReadyGroupIDs = map[roachpb.RangeID]struct{}{}
				// No Ready for any group
				if len(writingGroups) == 0 {
					break
				}
				s.logRaftReady(writingGroups)

				select {
				case resp := <-s.writeTask.out:
					s.handleWriteResponse(resp, writingGroups)
					// Advance rd and check these groups again.
					// handleWriteResponse may call s.proposal to generate new Ready.
					for groupID, rd := range writingGroups {
						if g, ok := s.groups[groupID]; ok {
							g.raftGroup.Advance(rd)
							checkReadyGroupIDs[groupID] = struct{}{}
						}
					}
				case <-s.stopper.ShouldStop():
					return
				}

			case <-s.Ticker.Chan():
				if log.V(8) {
					log.Infof("node %v: got tick", s.nodeID)
				}
				for groupID, g := range s.groups {
					g.raftGroup.Tick()
					checkReadyGroupIDs[groupID] = struct{}{}
				}
				ticks++
				if ticks >= s.HeartbeatIntervalTicks {
					ticks = 0
					s.coalescedHeartbeat()
				}

			case cb := <-s.callbackChan:
				if log.V(8) {
					log.Infof("node %v: got callback", s.nodeID)
				}
				cb()

			case eventsChan <- s.pendingEvents:
				if log.V(8) {
					log.Infof("node %v: send pendingEvents len %d", s.nodeID, len(s.pendingEvents))
				}
				s.pendingEvents = nil

			case statusOp := <-s.statusChan:
				if log.V(8) {
					log.Infof("node %v: receive status groupID %d", s.nodeID, statusOp.groupID)
				}
				g := s.groups[statusOp.groupID]
				if g == nil {
					log.Infof("node %v: g is nil for statusOp for gropuID %v", s.nodeID, statusOp.groupID)
					statusOp.ch <- nil
					break
				}
				statusOp.ch <- g.raftGroup.Status()
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
		log.Infof("store %s stopping", s.storeID)
	}
	s.MultiRaft.Transport.Stop(s.storeID)

	// Ensure that any remaining commands are not left hanging.
	for _, g := range s.groups {
		for _, p := range g.pending {
			if p.ch != nil {
				p.ch <- util.Errorf("shutting down")
			}
		}
	}
}

// addNode creates a node and registers the given group (if not nil)
// for that node.
func (s *state) addNode(nodeID roachpb.NodeID, g *group) error {
	newNode, ok := s.nodes[nodeID]
	if !ok {
		s.nodes[nodeID] = &node{
			nodeID:   nodeID,
			groupIDs: make(map[roachpb.RangeID]struct{}),
		}
		newNode = s.nodes[nodeID]
	}

	if g != nil {
		newNode.registerGroup(g.groupID)
		g.nodeIDs = append(g.nodeIDs, nodeID)
	}
	return nil
}

// removeNode removes a node from a group.
func (s *state) removeNode(nodeID roachpb.NodeID, g *group) error {
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
	node.unregisterGroup(g.groupID)

	return nil
}

func (s *state) handleMessage(req *RaftMessageRequest) {
	// We only want to lazily create the group if it's not heartbeat-related;
	// our heartbeats are coalesced and contain a dummy GroupID.
	switch req.Message.Type {
	case raftpb.MsgHeartbeat:
		s.fanoutHeartbeat(req)
		return

	case raftpb.MsgHeartbeatResp:
		s.fanoutHeartbeatResponse(req)
		return

	case raftpb.MsgSnap:
		s.lockStorage()
		canApply := s.Storage().CanApplySnapshot(req.GroupID, req.Message.Snapshot)
		s.unlockStorage()
		if !canApply {
			// If the storage cannot accept the snapshot, drop it before
			// passing it to RawNode.Step, since our error handling
			// options past that point are limited.
			return
		}
	}

	s.CacheReplicaDescriptor(req.GroupID, req.FromReplica)
	s.CacheReplicaDescriptor(req.GroupID, req.ToReplica)
	if g, ok := s.groups[req.GroupID]; ok {
		if g.replicaID > req.ToReplica.ReplicaID {
			log.Warningf("node %v: got message for group %s with stale replica ID %s (expected %s)",
				s.nodeID, req.GroupID, req.ToReplica.ReplicaID, g.replicaID)
			return
		} else if g.replicaID < req.ToReplica.ReplicaID {
			// The message has a newer ReplicaID than we know about. This
			// means that this node has been removed from a group and
			// re-added to it, before our GC process was able to remove the
			// remnants of the old group.
			log.Infof("node %v: got message for group %s with newer replica ID (%s vs %s), recreating group",
				s.nodeID, req.GroupID, req.ToReplica.ReplicaID, g.replicaID)
			if err := s.removeGroup(req.GroupID, g.replicaID); err != nil {
				log.Warningf("Error removing group %d (in response to incoming message): %s",
					req.GroupID, err)
				return
			}
			if err := s.createGroup(req.GroupID, req.ToReplica.ReplicaID); err != nil {
				log.Warningf("Error recreating group %d (in response to incoming message): %s",
					req.GroupID, err)
				return
			}
		}
	} else {
		if log.V(1) {
			log.Infof("node %v: got message for unknown group %d; creating it", s.nodeID, req.GroupID)
		}
		if err := s.createGroup(req.GroupID, req.ToReplica.ReplicaID); err != nil {
			log.Warningf("Error creating group %d (in response to incoming message): %s",
				req.GroupID, err)
			return
		}
	}

	if err := s.groups[req.GroupID].raftGroup.Step(req.Message); err != nil {
		if log.V(4) {
			log.Infof("node %v: step to group %v failed for message %.200s", s.nodeID, req.GroupID,
				raft.DescribeMessage(req.Message, s.EntryFormatter))
		}
	}
}

// createGroup is called in two situations: by the application at
// startup (in which case the replicaID argument is zero and the
// replicaID will be loaded from storage), and in response to incoming
// messages (in which case the replicaID comes from the incoming
// message, since nothing is on disk yet).
func (s *state) createGroup(groupID roachpb.RangeID, replicaID roachpb.ReplicaID) error {
	s.lockStorage()
	defer s.unlockStorage()
	if g, ok := s.groups[groupID]; ok {
		if replicaID != 0 && g.replicaID != replicaID {
			return util.Errorf("cannot create group %s with replica ID %s; already exists with replica ID %s",
				groupID, replicaID, g.replicaID)
		}
		return nil
	}
	if log.V(3) {
		log.Infof("node %v creating group %v", s.nodeID, groupID)
	}

	gs, err := s.Storage().GroupStorage(groupID, replicaID)
	if err != nil {
		return err
	}
	_, cs, err := gs.InitialState()
	if err != nil {
		return err
	}

	// Find our store ID in the replicas list.
	for _, r := range cs.Nodes {
		repDesc, err := s.ReplicaDescriptor(groupID, roachpb.ReplicaID(r))
		if err != nil {
			return err
		}
		if repDesc.StoreID == s.storeID {
			if replicaID == 0 {
				replicaID = repDesc.ReplicaID
			} else if replicaID < repDesc.ReplicaID {
				// Note that our ConfState may be stale, so if we were given a
				// replica ID we only consult the ConfState to ensure we don't
				// regress.
				return util.Errorf("inconsistent replica ID: passed %d, but found %s by scanning ConfState for store %s",
					replicaID, repDesc.ReplicaID, s.storeID)
			}
			break
		}
	}
	if replicaID == 0 {
		return util.Errorf("couldn't find replica ID for this store (%s) in range %d",
			s.storeID, groupID)
	}
	s.CacheReplicaDescriptor(groupID, roachpb.ReplicaDescriptor{
		ReplicaID: replicaID,
		NodeID:    s.nodeID,
		StoreID:   s.storeID,
	})

	appliedIndex, err := s.Storage().AppliedIndex(groupID)
	if err != nil {
		return err
	}

	raftCfg := &raft.Config{
		ID:            uint64(replicaID),
		Applied:       appliedIndex,
		ElectionTick:  s.ElectionTimeoutTicks,
		HeartbeatTick: s.HeartbeatIntervalTicks,
		Storage:       gs,
		// TODO(bdarnell): make these configurable; evaluate defaults.
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		Logger:          &raftLogger{group: uint64(groupID)},
	}
	raftGroup, err := raft.NewRawNode(raftCfg, nil)
	if err != nil {
		return err
	}
	g := &group{
		groupID:   groupID,
		replicaID: replicaID,
		pending:   map[string]*proposal{},
		raftGroup: raftGroup,
	}
	s.groups[groupID] = g

	for _, id := range cs.Nodes {
		replicaID := roachpb.ReplicaID(id)
		replica, err := s.ReplicaDescriptor(groupID, replicaID)
		if err != nil {
			return err
		}

		if err := s.addNode(replica.NodeID, g); err != nil {
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
	if len(cs.Nodes) == 1 {
		replica, err := s.ReplicaDescriptor(groupID, roachpb.ReplicaID(cs.Nodes[0]))
		if err != nil {
			return err
		}
		if replica.StoreID == s.storeID {
			log.Infof("node %s campaigning because initial confstate is %v", s.nodeID, cs.Nodes)
			if err := raftGroup.Campaign(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *state) removeGroup(groupID roachpb.RangeID, replicaID roachpb.ReplicaID) error {
	// Group creation is lazy and idempotent; so is removal.
	g, ok := s.groups[groupID]
	if !ok {
		return nil
	}
	if log.V(3) {
		log.Infof("node %v removing group %v replicaID %v", s.nodeID, groupID, replicaID)
	}

	if replicaID == 0 {
		replicaID = g.replicaID
	}
	// Use ">" check other than "!=" check to ignore obsolete removing.
	// See TestStoreRemoveReplicaOldDescriptor.
	if g.replicaID > replicaID {
		return ErrReplicaIDMismatch
	}
	// Cancel commands which are still in transit.
	for _, prop := range g.pending {
		s.removePending(g, prop, ErrGroupDeleted)
	}

	for _, nodeID := range g.nodeIDs {
		s.nodes[nodeID].unregisterGroup(groupID)
	}

	// groupID is not deleted from checkReadyGroupIDs, but
	// s.handleWriteReady can handle this case.
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
		// If we are no longer a member of the group, don't allow any additional
		// proposals.
		//
		// If this is an initial proposal, go ahead and remove the command. If
		// this is a re-proposal, there is a good chance that the original
		// proposal will still commit even though the node has been removed from
		// the group.
		//
		// Therefore, we will not remove the pending command until the group
		// itself is removed, the node is re-added to the group
		// (which will trigger another re-proposal that will succeed), or the
		// proposal is committed.
		if _, ok := g.pending[p.commandID]; !ok {
			s.removePending(nil, p, ErrGroupDeleted)
		}
		return
	}

	if log.V(3) {
		log.Infof("group %d: new proposal %x", p.groupID, p.commandID)
	}
	g.pending[p.commandID] = p
	p.fn(g.raftGroup)
}

func (s *state) logRaftReady(readyGroups map[roachpb.RangeID]raft.Ready) {
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
// It will only do this for groups which are tagged via the map.
func (s *state) handleWriteReady(checkReadyGroupIDs map[roachpb.RangeID]struct{}) map[roachpb.RangeID]raft.Ready {
	if log.V(6) {
		log.Infof("node %v write ready, preparing request", s.nodeID)
	}
	s.lockStorage()
	defer s.unlockStorage()
	writeRequest := newWriteRequest()
	readys := make(map[roachpb.RangeID]raft.Ready)
	for groupID := range checkReadyGroupIDs {
		g, ok := s.groups[groupID]
		if !ok {
			if log.V(6) {
				log.Infof("dropping write request to group %d", groupID)
			}
			continue
		}
		if !g.raftGroup.HasReady() {
			continue
		}
		ready := g.raftGroup.Ready()
		readys[groupID] = ready
		g.writing = true

		gwr := &groupWriteRequest{}
		var err error
		gwr.replicaID, err = s.Storage().ReplicaIDForStore(groupID, s.storeID)
		if err != nil {
			if log.V(1) {
				log.Warningf("failed to look up replica ID for range %v (disabling replica ID check): %s",
					groupID, err)
			}
			gwr.replicaID = 0
		}
		if !raft.IsEmptyHardState(ready.HardState) {
			gwr.state = ready.HardState
		}
		if !raft.IsEmptySnap(ready.Snapshot) {
			gwr.snapshot = ready.Snapshot
		}
		if len(ready.Entries) > 0 {
			gwr.entries = ready.Entries
		}
		writeRequest.groups[groupID] = gwr
	}
	// If no ready, don't write to writeTask as caller will
	// not wait on s.writeTask.out when len(readys) == 0.
	if len(readys) > 0 {
		s.writeTask.in <- writeRequest
	}
	return readys
}

// processCommittedEntry tells the application that a command was committed.
// Returns the commandID, or an empty string if the given entry was not a command.
func (s *state) processCommittedEntry(groupID roachpb.RangeID, g *group, entry raftpb.Entry) string {
	var commandID string
	switch entry.Type {
	case raftpb.EntryNormal:
		var command []byte
		commandID, command = decodeCommand(entry.Data)
		s.sendEvent(&EventCommandCommitted{
			GroupID:   groupID,
			CommandID: commandID,
			Command:   command,
			Index:     entry.Index,
		})

	case raftpb.EntryConfChange:
		cc := raftpb.ConfChange{}
		if err := cc.Unmarshal(entry.Data); err != nil {
			log.Fatalf("invalid ConfChange data: %s", err)
		}
		var payload []byte
		if len(cc.Context) > 0 {
			var ctx ConfChangeContext
			if err := ctx.Unmarshal(cc.Context); err != nil {
				log.Fatalf("invalid ConfChangeContext: %s", err)
			}
			commandID = ctx.CommandID
			payload = ctx.Payload
			s.CacheReplicaDescriptor(groupID, ctx.Replica)
		}
		replica, err := s.ReplicaDescriptor(groupID, roachpb.ReplicaID(cc.NodeID))
		if err != nil {
			// TODO(bdarnell): stash Replica information somewhere so we can have it here
			// with no chance of failure.
			log.Fatalf("could not look up replica info (node %s, group %d, replica %d): %s",
				s.nodeID, groupID, cc.NodeID, err)
		}
		s.sendEvent(&EventMembershipChangeCommitted{
			GroupID:    groupID,
			CommandID:  commandID,
			Index:      entry.Index,
			Replica:    replica,
			ChangeType: cc.Type,
			Payload:    payload,
			Callback: func(err error) {
				select {
				case s.callbackChan <- func() {
					gInner, ok := s.groups[groupID]
					if !ok {
						log.Infof("group %d no longer exists, aborting configuration change", groupID)
					} else if gInner != g {
						log.Infof("passed in group and fetched group objects do not match\noriginal:%+v\nfetched:%+v\n, aborting configuration change",
							g, gInner)
					} else if err == nil {
						if log.V(3) {
							log.Infof("node %v applying configuration change %v", s.nodeID, cc)
						}
						// TODO(bdarnell): dedupe by keeping a record of recently-applied commandIDs
						switch cc.Type {
						case raftpb.ConfChangeAddNode:
							err = s.addNode(replica.NodeID, g)
						case raftpb.ConfChangeRemoveNode:
							err = s.removeNode(replica.NodeID, g)
						case raftpb.ConfChangeUpdateNode:
							// Updates don't concern multiraft, they are simply passed through.
						}
						if err != nil {
							log.Errorf("error applying configuration change %v: %s", cc, err)
						}
						g.raftGroup.ApplyConfChange(cc)
					} else {
						log.Warningf("aborting configuration change: %s", err)
						g.raftGroup.ApplyConfChange(raftpb.ConfChange{})
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
	groupID := noGroup
	var toReplica roachpb.ReplicaDescriptor
	var fromReplica roachpb.ReplicaDescriptor
	if g == nil {
		// No group (a coalesced heartbeat): To/From fields are NodeIDs.
		// TODO(bdarnell): test transports route by store ID, not node ID.
		// In tests they're always the same, so we can hack it here but
		// it would be better to fix the transports.
		// I think we need to fix this before we can support a range
		// with two replicas on different stores of the same node.
		toReplica.NodeID = roachpb.NodeID(msg.To)
		toReplica.StoreID = roachpb.StoreID(msg.To)
		fromReplica.NodeID = roachpb.NodeID(msg.From)
		fromReplica.StoreID = roachpb.StoreID(msg.From)
	} else {
		// Regular message: To/From fields are replica IDs.
		groupID = g.groupID
		var err error
		toReplica, err = s.ReplicaDescriptor(groupID, roachpb.ReplicaID(msg.To))
		if err != nil {
			log.Warningf("failed to lookup recipient replica %d in group %d: %s", msg.To, groupID, err)
			return
		}
		fromReplica, err = s.ReplicaDescriptor(groupID, roachpb.ReplicaID(msg.From))
		if err != nil {
			log.Warningf("failed to lookup sender replica %d in group %d: %s", msg.From, groupID, err)
			return
		}
	}
	if _, ok := s.nodes[toReplica.NodeID]; !ok {
		if log.V(4) {
			log.Infof("node %v: connecting to new node %v", s.nodeID, toReplica.NodeID)
		}
		if err := s.addNode(toReplica.NodeID, g); err != nil {
			log.Errorf("node %v: error adding group %v to node %v: %v",
				s.nodeID, groupID, toReplica.NodeID, err)
		}
	}
	err := s.Transport.Send(&RaftMessageRequest{
		GroupID:     groupID,
		ToReplica:   toReplica,
		FromReplica: fromReplica,
		Message:     msg,
	})
	snapStatus := raft.SnapshotFinish
	if err != nil {
		log.Warningf("node %v failed to send message to %v: %s", s.nodeID, toReplica.NodeID, err)
		if groupID != noGroup {
			g.raftGroup.ReportUnreachable(msg.To)
		}
		snapStatus = raft.SnapshotFailure
	}
	if msg.Type == raftpb.MsgSnap {
		// TODO(bdarnell): add an ack for snapshots and don't report status until
		// ack, error, or timeout.
		if groupID != noGroup {
			g.raftGroup.ReportSnapshot(msg.To, snapStatus)
		}
	}
}

// maybeSendLeaderEvent processes a raft.Ready to send events in response to leadership
// changes (this includes both sending an event to the app and retrying any pending
// proposals).
// It may call into Storage so it must be called with the storage lock held.
func (s *state) maybeSendLeaderEvent(groupID roachpb.RangeID, g *group, ready *raft.Ready) {
	s.assertStorageLocked()
	term := g.committedTerm
	if ready.SoftState != nil {
		// Always save the leader whenever it changes.
		if roachpb.ReplicaID(ready.SoftState.Lead) != g.leader.ReplicaID {
			if ready.SoftState.Lead == 0 {
				g.leader = roachpb.ReplicaDescriptor{}
			} else {
				if repl, err := s.ReplicaDescriptor(g.groupID, roachpb.ReplicaID(ready.SoftState.Lead)); err != nil {
					log.Warningf("node %s: failed to look up address of replica %d in group %d: %s",
						s.nodeID, ready.SoftState.Lead, g.groupID, err)
					g.leader = roachpb.ReplicaDescriptor{}
				} else {
					g.leader = repl
				}
			}
		}
	}
	if len(ready.CommittedEntries) > 0 {
		term = ready.CommittedEntries[len(ready.CommittedEntries)-1].Term
	}
	if term != g.committedTerm && g.leader.ReplicaID != 0 {
		// Whenever the committed term has advanced and we know our leader,
		// emit an event.
		g.committedTerm = term
		s.sendEvent(&EventLeaderElection{
			GroupID:   groupID,
			ReplicaID: g.leader.ReplicaID,
			Term:      g.committedTerm,
		})
	}
}

// handleWriteResponse updates the state machine and sends messages for a raft Ready batch.
func (s *state) handleWriteResponse(response *writeResponse, readyGroups map[roachpb.RangeID]raft.Ready) {
	if log.V(6) {
		log.Infof("node %v got write response: %#v", s.nodeID, *response)
	}
	s.lockStorage()
	defer s.unlockStorage()
	// Everything has been written to disk; now we can apply updates to the state machine
	// and send outgoing messages.
	for groupID, ready := range readyGroups {
		raftGroupID := roachpb.RangeID(groupID)
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

		// Process committed entries. etcd raft occasionally adds a nil
		// entry (our own commands are never empty). This happens in two
		// situations: When a new leader is elected, and when a config
		// change is dropped due to the "one at a time" rule. In both
		// cases we may need to resubmit our pending proposals (In the
		// former case we resubmit everything because we proposed them to
		// a former leader that is no longer able to commit them. In the
		// latter case we only need to resubmit pending config changes,
		// but it's hard to distinguish so we resubmit everything
		// anyway). We delay resubmission until after we have processed
		// the entire batch of entries.
		hasEmptyEntry := false
		for _, entry := range ready.CommittedEntries {
			if entry.Type == raftpb.EntryNormal && len(entry.Data) == 0 {
				hasEmptyEntry = true
				continue
			}
			commandID := s.processCommittedEntry(raftGroupID, g, entry)
			// TODO(bdarnell): the command is now committed, but not applied until the
			// application consumes EventCommandCommitted. Is returning via the channel
			// at this point useful or do we need to wait for the command to be
			// applied too?
			// This could be done with a Callback as in EventMembershipChangeCommitted
			// or perhaps we should move away from a channel to a callback-based system.
			s.removePending(g, g.pending[commandID], nil /* err */)
		}
		if hasEmptyEntry {
			for _, prop := range g.pending {
				s.propose(prop)
			}
		}

		if !raft.IsEmptySnap(ready.Snapshot) {
			// Sync the group/node mapping with the information contained in the snapshot.
			replicas, err := s.Storage().ReplicasFromSnapshot(ready.Snapshot)
			if err != nil {
				log.Fatalf("failed to parse snapshot: %s", err)
			}
			for _, rep := range replicas {
				// TODO(bdarnell): if we had any information that predated this snapshot
				// we must remove those nodes.
				if err := s.addNode(rep.NodeID, g); err != nil {
					log.Errorf("node %v: error adding node %v", s.nodeID, rep.NodeID)
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

type replicaDescCacheKey struct {
	groupID   roachpb.RangeID
	replicaID roachpb.ReplicaID
}

// ReplicaDescriptor returns a (possibly cached) ReplicaDescriptor.
// It may call into Storage so it must be called with the storage lock held.
func (s *state) ReplicaDescriptor(groupID roachpb.RangeID, replicaID roachpb.ReplicaID) (roachpb.ReplicaDescriptor, error) {
	// Manually assert the lock instead of relying on the check in Storage()
	// so we don't have locking issues masked by cache hits.
	s.assertStorageLocked()
	if rep, ok := s.replicaDescCache.Get(replicaDescCacheKey{groupID, replicaID}); ok {
		return rep.(roachpb.ReplicaDescriptor), nil
	}
	rep, err := s.Storage().ReplicaDescriptor(groupID, replicaID)
	if err == nil {
		err = rep.Validate()
	}
	if err == nil {
		s.replicaDescCache.Add(replicaDescCacheKey{groupID, replicaID}, rep)
	}
	return rep, err
}

func (s *state) CacheReplicaDescriptor(groupID roachpb.RangeID, replica roachpb.ReplicaDescriptor) {
	s.replicaDescCache.Add(replicaDescCacheKey{groupID, replica.ReplicaID}, replica)
}
