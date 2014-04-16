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
	"math"
	"math/rand"
	"net/rpc"
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/golang/glog"
)

// NodeID is a unique non-zero identifier for the node within the cluster.
type NodeID int32

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

// MultiRaft represents a local node in a raft cluster.
type MultiRaft struct {
	Config
	id       NodeID
	ops      chan interface{}
	requests chan *rpc.Call
	stopped  chan struct{}
}

// NewMultiRaft creates a MultiRaft object.
func NewMultiRaft(id NodeID, config *Config) (*MultiRaft, error) {
	if !id.isSet() {
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
		id:       id,
		ops:      make(chan interface{}, 100),
		requests: make(chan *rpc.Call, 100),
		stopped:  make(chan struct{}),
	}

	err = m.Transport.Listen(id, m)
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
	m.Transport.Stop(m.id)
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

// CreateGroup creates a new consensus group and joins it.  The application should
// arrange to call CreateGroup on all nodes named in initialMembers.
func (m *MultiRaft) CreateGroup(name string, initialMembers []NodeID) error {
	for _, id := range initialMembers {
		if !id.isSet() {
			return util.Error("Invalid NodeID")
		}
	}
	op := &createGroupOp{newGroup(name, initialMembers), make(chan error)}
	m.ops <- op
	return <-op.ch
}

type role int

const (
	follower role = iota
	candidate
	leader
)

// group represents the state of a consensus group.
type group struct {
	// Persistent state
	name         string
	members      []NodeID
	currentTerm  int
	votedFor     NodeID
	lastLogIndex int
	lastLogTerm  int

	// Volatile state
	role             role
	commitIndex      int
	lastApplied      int
	electionDeadline time.Time
	votes            map[NodeID]bool

	// Leader state.  Reset on election.
	nextIndex  map[string]int // default: lastLogIndex + 1
	matchIndex map[string]int // default: 0
}

func newGroup(name string, members []NodeID) *group {
	return &group{
		name:       name,
		members:    members,
		role:       follower,
		nextIndex:  make(map[string]int),
		matchIndex: make(map[string]int),
	}
}

type stopOp struct{}

type createGroupOp struct {
	group *group
	ch    chan error
}

// node represents a connection to a remote node.
type node struct {
	id       NodeID
	refCount int
	client   *asyncClient
}

// state represents the internal state of a MultiRaft object.  All variables here
// are accessible only from the state.start goroutine so they can be accessed without
// synchronization.
type state struct {
	*MultiRaft
	rand          *rand.Rand
	groups        map[string]*group
	nodes         map[NodeID]*node
	electionTimer *time.Timer
	responses     chan *rpc.Call
}

func newState(m *MultiRaft) *state {
	return &state{
		MultiRaft: m,
		rand:      util.NewPseudoRand(),
		groups:    make(map[string]*group),
		nodes:     make(map[NodeID]*node),
		responses: make(chan *rpc.Call, 100),
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
	glog.V(1).Infof("node %v starting", s.id)
	for {
		electionTimer := s.nextElectionTimer()
		glog.V(6).Infof("node %v: selecting", s.id)
		select {
		case op := <-s.ops:
			glog.V(6).Infof("node %v: got op %#v", s.id, op)
			switch op := op.(type) {
			case *stopOp:
				s.stop()
				return

			case *createGroupOp:
				s.createGroup(op)

			default:
				s.strictErrorLog("unknown op: %#v", op)
			}

		case call := <-s.requests:
			glog.V(6).Infof("node %v: got request %v", s.id, call)
			switch call.ServiceMethod {
			case requestVoteName:
				s.requestVoteRequest(call.Args.(*RequestVoteRequest),
					call.Reply.(*RequestVoteResponse), call)

			default:
				s.strictErrorLog("unknown rpc request: %#v", call.Args)
			}

		case call := <-s.responses:
			glog.V(6).Infof("node %v: got response %v", s.id, call)
			switch call.ServiceMethod {
			case requestVoteName:
				s.requestVoteResponse(call.Args.(*RequestVoteRequest), call.Reply.(*RequestVoteResponse))

			default:
				s.strictErrorLog("unknown rpc response: %#v", call.Reply)
			}

		case now := <-electionTimer.C:
			glog.V(6).Infof("node %v: got election timer", s.id)
			s.handleElectionTimers(now)
		}
		s.Clock.StopElectionTimer(electionTimer)
	}
}

func (s *state) stop() {
	glog.V(6).Infof("node %v stopping", s.id)
	for _, n := range s.nodes {
		err := n.client.conn.Close()
		if err != nil {
			glog.Warning("error stopping client:", err)
		}
	}
	close(s.stopped)
}

func (s *state) createGroup(op *createGroupOp) {
	if _, ok := s.groups[op.group.name]; ok {
		op.ch <- util.Errorf("group %s already exists", op.group.name)
		return
	}
	for _, member := range op.group.members {
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
	s.groups[op.group.name] = op.group
	op.ch <- nil
}

func (s *state) requestVoteRequest(req *RequestVoteRequest, resp *RequestVoteResponse,
	call *rpc.Call) {
	g, ok := s.groups[req.Group]
	if !ok {
		call.Error = util.Errorf("unknown group %s", req.Group)
		call.Done <- call
		return
	}
	if g.votedFor.isSet() && g.votedFor != req.CandidateID {
		resp.VoteGranted = false
	} else {
		// TODO: check log positions
		g.currentTerm = req.Term
		resp.VoteGranted = true
	}
	resp.Term = g.currentTerm
	call.Done <- call
}

func (s *state) requestVoteResponse(req *RequestVoteRequest, resp *RequestVoteResponse) {
	g := s.groups[req.Group]
	if resp.Term < g.currentTerm {
		return
	}
	if resp.VoteGranted {
		g.votes[req.NodeID] = resp.VoteGranted
	}
	if g.role == candidate && len(g.votes)*2 > len(g.members) {
		g.role = leader
		glog.V(1).Infof("node %v becoming leader for group %s", s.id, g.name)
		hackyTestChannel <- s.id
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
	glog.V(1).Infof("node %v becoming candidate (was %v) for group %s", s.id, g.role, g.name)
	if g.role == leader {
		panic("cannot transition from leader to candidate")
	}
	g.role = candidate
	g.currentTerm++
	g.votedFor = s.id
	g.votes = make(map[NodeID]bool)
	s.updateElectionDeadline(g)
	for _, id := range g.members {
		node := s.nodes[id]
		node.client.requestVote(&RequestVoteRequest{
			NodeID:       id,
			Group:        g.name,
			Term:         g.currentTerm,
			CandidateID:  s.id,
			LastLogIndex: g.lastLogIndex,
			LastLogTerm:  g.lastLogTerm,
		})
	}
}

// Temporary channel so we can test the current incomplete state; will be removed
// as more of the interface is fleshed out.
var hackyTestChannel = make(chan NodeID, 1)
