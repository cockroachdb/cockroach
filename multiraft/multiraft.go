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

import (
	"container/list"
	"math"
	"math/rand"
	"net/rpc"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/golang/glog"
)

// NodeID is a unique non-zero identifier for the node within the cluster.
type NodeID int32

// GroupID is a unique identifier for a consensus group within the cluster.
type GroupID int64

// isSet returns true if the NodeID is valid (i.e. non-zero)
func (n NodeID) isSet() bool {
	return int32(n) != 0
}

// Config contains the parameters necessary to construct a MultiRaft object.
type Config struct {
	Storage   Storage
	Transport Transport
	// Clock may be nil to use real time.
	Clock Clock

	// A new election is called if the ElectionTimeout elapses with no contact from the leader.
	// The actual ElectionTimeout is chosen randomly from the range [ElectionTimeoutMin,
	// ElectionTimeoutMax) to minimize the chances of several servers trying to become leaders
	// simultaneously.  The Raft paper suggests a range of 150-300ms for local networks;
	// geographically distributed installations should use higher values to account for the
	// increased round trip time.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

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
	if c.ElectionTimeoutMin == 0 || c.ElectionTimeoutMax == 0 {
		return util.Error("ElectionTimeout{Min,Max} must be non-zero")
	}
	if c.ElectionTimeoutMin > c.ElectionTimeoutMax {
		return util.Error("ElectionTimeoutMin must be <= ElectionTimeoutMax")
	}
	return nil
}

// MultiRaft represents a local node in a raft cluster.  The owner is responsible for consuming
// the Events channel in a timely manner.
type MultiRaft struct {
	Config
	Events   chan interface{}
	nodeID   NodeID
	ops      chan interface{}
	requests chan *rpc.Call
	stopped  chan struct{}
}

// NewMultiRaft creates a MultiRaft object.
func NewMultiRaft(nodeID NodeID, config *Config) (*MultiRaft, error) {
	if !nodeID.isSet() {
		return nil, util.Error("Invalid NodeID")
	}
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	if config.Clock == nil {
		config.Clock = RealClock
	}

	m := &MultiRaft{
		Config:   *config,
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

// strictErrorLog panics in strict mode and logs an error otherwise.  Arguments are printf-style
// and will be passed directly to either glog.Errorf or glog.Fatalf.
func (m *MultiRaft) strictErrorLog(format string, args ...interface{}) {
	if m.Strict {
		glog.Fatalf(format, args...)
	} else {
		glog.Errorf(format, args...)
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

// CreateGroup creates a new consensus group and joins it.  The application should
// arrange to call CreateGroup on all nodes named in initialMembers.
func (m *MultiRaft) CreateGroup(groupID GroupID, initialMembers []NodeID) error {
	for _, id := range initialMembers {
		if !id.isSet() {
			return util.Error("Invalid NodeID")
		}
	}
	op := &createGroupOp{newGroup(groupID, initialMembers), make(chan error)}
	m.ops <- op
	return <-op.ch
}

// SubmitCommand sends a command (a binary blob) to the cluster.  This method returns
// when the command has been successfully sent, not when it has been committed.
// TODO(bdarnell): should SubmitCommand wait until the commit?
func (m *MultiRaft) SubmitCommand(groupID GroupID, command []byte) error {
	op := &submitCommandOp{groupID, command, make(chan error)}
	m.ops <- op
	return <-op.ch
}

// Role represents the state of the node in a group.
type Role int

// Nodes can be either followers, candidates, or leaders.
const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

// pendingCall represents an RPC that we should not respond to until we have persisted
// up to the given point.  term and logIndex may be -1 if the rpc didn't modify that
// variable and therefore can be resolved regardless of its value.
type pendingCall struct {
	call     *rpc.Call
	term     int
	logIndex int
}

// group represents the state of a consensus group.
type group struct {
	groupID GroupID
	// Persistent state.  When an RPC is received (or another event occurs), the in-memory fields
	// are updated immediately; the 'persisted' versions are updated later after they have been
	// (asynchronously) written to stable storage.  The group is 'dirty' whenever the current
	// and persisted data differ.
	metadata           *GroupMetadata
	lastLogIndex       int
	lastLogTerm        int
	persistedMetadata  *GroupMetadata
	persistedLastIndex int
	persistedLastTerm  int

	// Volatile state
	role             Role
	commitIndex      int
	lastApplied      int
	electionDeadline time.Time
	votes            map[NodeID]bool

	// Leader state.  Reset on election.
	nextIndex  map[NodeID]int // default: lastLogIndex + 1
	matchIndex map[NodeID]int // default: 0

	// a List of *pendingCall
	pendingCalls list.List

	// LogEntries that have not been persisted.  The group is 'dirty' when this is non-empty.
	pendingEntries []*LogEntry
}

func newGroup(groupID GroupID, members []NodeID) *group {
	return &group{
		groupID: groupID,
		metadata: &GroupMetadata{
			Members: members,
		},
		role:       RoleFollower,
		nextIndex:  make(map[NodeID]int),
		matchIndex: make(map[NodeID]int),
	}
}

// findQuorumIndex examines matchIndex to find the smallest log index that a quorum has
// agreed on.
func (g *group) findQuorumIndex() int {
	var indices []int
	for _, nodeID := range g.metadata.Members {
		// We want a zero if the node has not yet reported a matching index, so use the ", ok"
		// form but discard the "ok".
		matchIndex, _ := g.matchIndex[nodeID]
		indices = append(indices, matchIndex)
	}
	sort.Ints(indices)
	quorumPos := len(indices)/2 + 1
	return indices[quorumPos]
}

type stopOp struct{}

type createGroupOp struct {
	group *group
	ch    chan error
}

type submitCommandOp struct {
	groupID GroupID
	command []byte
	ch      chan error
}

// node represents a connection to a remote node.
type node struct {
	nodeID   NodeID
	refCount int
	client   *asyncClient
}

// state represents the internal state of a MultiRaft object.  All variables here
// are accessible only from the state.start goroutine so they can be accessed without
// synchronization.
type state struct {
	*MultiRaft
	rand          *rand.Rand
	groups        map[GroupID]*group
	dirtyGroups   map[GroupID]*group
	nodes         map[NodeID]*node
	electionTimer *time.Timer
	responses     chan *rpc.Call
	writeTask     *writeTask
}

func newState(m *MultiRaft) *state {
	return &state{
		MultiRaft:   m,
		rand:        util.NewPseudoRand(),
		groups:      make(map[GroupID]*group),
		dirtyGroups: make(map[GroupID]*group),
		nodes:       make(map[NodeID]*node),
		responses:   make(chan *rpc.Call, 100),
		writeTask:   newWriteTask(m.Storage),
	}
}

func (s *state) updateElectionDeadline(g *group) {
	timeout := util.RandIntInRange(s.rand, int(s.ElectionTimeoutMin), int(s.ElectionTimeoutMax))
	g.electionDeadline = s.Clock.Now().Add(time.Duration(timeout))
}

func (s *state) nextElectionTimer() *time.Timer {
	minTimeout := time.Duration(math.MaxInt64)
	now := s.Clock.Now()
	for _, g := range s.groups {
		timeout := g.electionDeadline.Sub(now)
		if timeout < minTimeout {
			minTimeout = timeout
		}
	}
	return s.Clock.NewElectionTimer(minTimeout)
}

func (s *state) start() {
	glog.V(1).Infof("node %v starting", s.nodeID)
	go s.writeTask.start()
	for {
		electionTimer := s.nextElectionTimer()
		var writeReady chan struct{}
		if len(s.dirtyGroups) > 0 {
			writeReady = s.writeTask.ready
		} else {
			writeReady = nil
		}
		glog.V(6).Infof("node %v: selecting", s.nodeID)
		select {
		case op := <-s.ops:
			glog.V(6).Infof("node %v: got op %#v", s.nodeID, op)
			switch op := op.(type) {
			case *stopOp:
				s.stop()
				return

			case *createGroupOp:
				s.createGroup(op)

			case *submitCommandOp:
				s.submitCommand(op)

			default:
				s.strictErrorLog("unknown op: %#v", op)
			}

		case call := <-s.requests:
			glog.V(6).Infof("node %v: got request %v", s.nodeID, call)
			switch call.ServiceMethod {
			case requestVoteName:
				s.requestVoteRequest(call.Args.(*RequestVoteRequest),
					call.Reply.(*RequestVoteResponse), call)

			case appendEntriesName:
				s.appendEntriesRequest(call.Args.(*AppendEntriesRequest),
					call.Reply.(*AppendEntriesResponse), call)

			default:
				s.strictErrorLog("unknown rpc request: %#v", call.Args)
			}

		case call := <-s.responses:
			glog.V(6).Infof("node %v: got response %v", s.nodeID, call)
			switch call.ServiceMethod {
			case requestVoteName:
				s.requestVoteResponse(call.Args.(*RequestVoteRequest), call.Reply.(*RequestVoteResponse))

			case appendEntriesName:
				s.appendEntriesResponse(call.Args.(*AppendEntriesRequest),
					call.Reply.(*AppendEntriesResponse))

			default:
				s.strictErrorLog("unknown rpc response: %#v", call.Reply)
			}

		case writeReady <- struct{}{}:
			s.handleWriteReady()

		case resp := <-s.writeTask.out:
			s.handleWriteResponse(resp)

		case now := <-electionTimer.C:
			glog.V(6).Infof("node %v: got election timer", s.nodeID)
			s.handleElectionTimers(now)
		}
		s.Clock.StopElectionTimer(electionTimer)
	}
}

func (s *state) stop() {
	glog.V(6).Infof("node %v stopping", s.nodeID)
	for _, n := range s.nodes {
		err := n.client.conn.Close()
		if err != nil {
			glog.Warning("error stopping client:", err)
		}
	}
	s.writeTask.stop()
	close(s.stopped)
}

func (s *state) createGroup(op *createGroupOp) {
	glog.V(6).Infof("node %v creating group %v", s.nodeID, op.group.groupID)
	if _, ok := s.groups[op.group.groupID]; ok {
		op.ch <- util.Errorf("group %v already exists", op.group.groupID)
		return
	}
	for _, member := range op.group.metadata.Members {
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
	s.updateElectionDeadline(op.group)
	s.groups[op.group.groupID] = op.group
	op.ch <- nil
}

func (s *state) submitCommand(op *submitCommandOp) {
	glog.V(6).Infof("node %v submitting command to group %v", s.nodeID, op.groupID)
	g := s.groups[op.groupID]
	if g.role != RoleLeader {
		op.ch <- util.Error("TODO(bdarnell): forward commands to leader")
		return
	}

	g.lastLogIndex++
	entry := &LogEntry{
		Term:    g.metadata.CurrentTerm,
		Index:   g.lastLogIndex,
		Type:    LogEntryCommand,
		Payload: op.command,
	}
	g.pendingEntries = append(g.pendingEntries, entry)
	s.updateDirtyStatus(g)
	op.ch <- nil
}

func (s *state) requestVoteRequest(req *RequestVoteRequest, resp *RequestVoteResponse,
	call *rpc.Call) {
	g, ok := s.groups[req.GroupID]
	if !ok {
		call.Error = util.Errorf("unknown group %v", req.GroupID)
		call.Done <- call
		return
	}
	if g.metadata.VotedFor.isSet() && g.metadata.VotedFor != req.CandidateID {
		resp.VoteGranted = false
	} else {
		// TODO: check log positions
		g.metadata.CurrentTerm = req.Term
		resp.VoteGranted = true
	}
	resp.Term = g.metadata.CurrentTerm
	g.pendingCalls.PushBack(&pendingCall{call, g.metadata.CurrentTerm, -1})
	s.updateDirtyStatus(g)
}

func (s *state) requestVoteResponse(req *RequestVoteRequest, resp *RequestVoteResponse) {
	g := s.groups[req.GroupID]
	if resp.Term < g.metadata.CurrentTerm {
		return
	}
	if resp.VoteGranted {
		g.votes[req.DestNode] = resp.VoteGranted
	}
	if g.role == RoleCandidate && len(g.votes)*2 > len(g.metadata.Members) {
		g.role = RoleLeader
		glog.V(1).Infof("node %v becoming leader for group %v", s.nodeID, g.groupID)
		s.sendEvent(&EventLeaderElection{g.groupID, s.nodeID})
	}
	s.updateDirtyStatus(g)
}

// From the Raft paper:
// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex
// whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex =
// min(leaderCommit, last log index)
func (s *state) appendEntriesRequest(req *AppendEntriesRequest, resp *AppendEntriesResponse,
	call *rpc.Call) {
	g := s.groups[req.GroupID]
	resp.Term = g.metadata.CurrentTerm
	if req.Term < g.metadata.CurrentTerm {
		resp.Success = false
		call.Done <- call
		return
	}
	// TODO(bdarnell): check prevLogIndex and terms
	g.pendingEntries = append(g.pendingEntries, req.Entries...)
	if len(g.pendingEntries) > 0 {
		lastEntry := g.pendingEntries[len(g.pendingEntries)-1]
		g.lastLogIndex = lastEntry.Index
		g.lastLogTerm = lastEntry.Term
	}
	s.updateDirtyStatus(g)
	resp.Success = true
	g.pendingCalls.PushBack(&pendingCall{call, -1, g.lastLogIndex})
	s.commitEntries(g, req.LeaderCommit)
}

// From the Raft paper:
// If successful: update nextIndex and matchIndex for follower (§5.3)
// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and
// log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
func (s *state) appendEntriesResponse(req *AppendEntriesRequest, resp *AppendEntriesResponse) {
	g := s.groups[req.GroupID]
	if resp.Success {
		if len(req.Entries) > 0 {
			lastIndex := req.Entries[len(req.Entries)-1].Index
			g.nextIndex[req.DestNode] = lastIndex + 1
			g.matchIndex[req.DestNode] = lastIndex
		}
	} else {
		g.nextIndex[req.DestNode]--
	}

	s.commitEntries(g, g.findQuorumIndex())
}

func (s *state) handleWriteReady() {
	glog.V(6).Infof("node %v write ready, preparing request", s.nodeID)
	writeRequest := newWriteRequest()
	for groupID, group := range s.dirtyGroups {
		req := &groupWriteRequest{}
		writeRequest.groups[groupID] = req
		if !group.metadata.Equal(group.persistedMetadata) {
			copy := *group.metadata
			req.metadata = &copy
		}
		if len(group.pendingEntries) > 0 {
			req.entries = group.pendingEntries
			group.pendingEntries = nil
		}
	}
	s.writeTask.in <- writeRequest
}

func (s *state) handleWriteResponse(response *writeResponse) {
	glog.V(6).Infof("node %v got write response: %#v", s.nodeID, *response)
	for groupID, persistedGroup := range response.groups {
		g := s.groups[groupID]
		if persistedGroup.metadata != nil {
			g.persistedMetadata = persistedGroup.metadata
		}
		if persistedGroup.lastIndex != -1 {
			glog.V(6).Infof("updating persisted log index to %v", persistedGroup.lastIndex)
			for _, id := range g.metadata.Members {
				node := s.nodes[id]
				node.client.appendEntries(&AppendEntriesRequest{
					RequestHeader: RequestHeader{s.nodeID, id},
					GroupID:       g.groupID,
					Term:          g.metadata.CurrentTerm,
					LeaderID:      s.nodeID,
					PrevLogIndex:  g.persistedLastIndex,
					PrevLogTerm:   g.persistedLastTerm,
					LeaderCommit:  g.commitIndex,
					Entries:       persistedGroup.entries,
				})
			}
			g.persistedLastIndex = persistedGroup.lastIndex
			g.persistedLastTerm = persistedGroup.lastTerm
		}

		// Resolve any pending RPCs that have been waiting for persistence to catch up.
		var toDelete []*list.Element
		for e := g.pendingCalls.Front(); e != nil; e = e.Next() {
			call := e.Value.(*pendingCall)
			if g.persistedMetadata == nil || g.persistedLastIndex == -1 {
				continue
			}
			if call.term != -1 && call.term > g.persistedMetadata.CurrentTerm {
				continue
			}
			if call.logIndex != -1 && call.logIndex > g.persistedLastIndex {
				continue
			}
			call.call.Done <- call.call
			toDelete = append(toDelete, e)
		}
		for _, e := range toDelete {
			g.pendingCalls.Remove(e)
		}
		s.updateDirtyStatus(g)
	}
}

func (s *state) handleElectionTimers(now time.Time) {
	for _, g := range s.groups {
		if !now.Before(g.electionDeadline) {
			s.becomeCandidate(g)
		}
	}
}

func (s *state) becomeCandidate(g *group) {
	glog.V(1).Infof("node %v becoming candidate (was %v) for group %s", s.nodeID, g.role, g.groupID)
	if g.role == RoleLeader {
		panic("cannot transition from leader to candidate")
	}
	g.role = RoleCandidate
	g.metadata.CurrentTerm++
	g.metadata.VotedFor = s.nodeID
	g.votes = make(map[NodeID]bool)
	s.updateElectionDeadline(g)
	for _, id := range g.metadata.Members {
		node := s.nodes[id]
		node.client.requestVote(&RequestVoteRequest{
			RequestHeader: RequestHeader{s.nodeID, id},
			GroupID:       g.groupID,
			Term:          g.metadata.CurrentTerm,
			CandidateID:   s.nodeID,
			LastLogIndex:  g.lastLogIndex,
			LastLogTerm:   g.lastLogTerm,
		})
	}
	s.updateDirtyStatus(g)
}

func (s *state) commitEntries(g *group, index int) {
	if index <= g.commitIndex {
		// Commit index cannot actually move backwards, but a newly-elected leader might
		// report stale positions for a short time so just ignore them.
		return
	}
	if index > g.persistedLastIndex {
		// If we are not caught up with the leader, just commit as far as we can.
		// We'll continue to commit new entries as we receive AppendEntriesRequests.
		index = g.persistedLastIndex
	}
	glog.V(6).Infof("node %v advancing commit position for group %v from %v to %v",
		s.nodeID, g.groupID, g.commitIndex, index)
	// TODO(bdarnell): move storage access (incl. the channel iteration) to a goroutine
	entries := make(chan *LogEntryState, 100)
	go s.Storage.GetLogEntries(g.groupID, g.commitIndex+1, index, entries)
	for entry := range entries {
		if entry.Entry.Type == LogEntryCommand {
			s.sendEvent(&EventCommandCommitted{entry.Entry.Payload})
		}
	}
	g.commitIndex = index
}

// updateDirtyStatus sets the dirty flag for the given group.
func (s *state) updateDirtyStatus(g *group) {
	dirty := false
	if !g.metadata.Equal(g.persistedMetadata) {
		dirty = true
	}
	if len(g.pendingEntries) > 0 {
		dirty = true
	}
	if dirty {
		s.dirtyGroups[g.groupID] = g
	} else {
		delete(s.dirtyGroups, g.groupID)
	}
}
