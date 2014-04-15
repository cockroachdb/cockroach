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
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/golang/glog"
)

// Config contains the parameters necessary to construct a MultiRaft object.
type Config struct {
	Storage   Storage
	Transport Transport

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
	id      string
	ops     chan interface{}
	Events  chan interface{}
	stopped chan struct{}
}

// NewMultiRaft creates a MultiRaft object.
func NewMultiRaft(id string, config *Config) (*MultiRaft, error) {
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	m := &MultiRaft{
		Config:  *config,
		id:      id,
		ops:     make(chan interface{}, 100),
		Events:  make(chan interface{}, 100),
		stopped: make(chan struct{}),
	}

	err = m.Transport.Listen(id, serverChannel(m.ops))
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

// Stop terminates the running raft instance.
func (m *MultiRaft) Stop() {
	m.ops <- &stopOp{}
	<-m.stopped
}

// CreateGroup creates a new consensus group and joins it.  The application should
// arrange to call CreateGroup on all nodes named in initialMembers.
func (m *MultiRaft) CreateGroup(name string, initialMembers []string) error {
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
	members      []string
	currentTerm  int
	votedFor     string
	lastLogIndex int
	lastLogTerm  int

	// Volatile state
	role             role
	commitIndex      int
	lastApplied      int
	electionDeadline time.Time
	votes            map[string]bool

	// Leader state.  Reset on election.
	nextIndex  map[string]int // default: lastLogIndex + 1
	matchIndex map[string]int // default: 0
}

func newGroup(name string, members []string) *group {
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
	id       string
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
	nodes         map[string]*node
	electionTimer *time.Timer
}

func newState(m *MultiRaft) *state {
	return &state{
		MultiRaft: m,
		rand:      util.NewPseudoRand(),
		groups:    make(map[string]*group),
		nodes:     make(map[string]*node),
	}
}

func (s *state) updateElectionDeadline(g *group) {
	timeout := util.RandIntInRange(s.rand, int(s.ElectionTimeoutMin), int(s.ElectionTimeoutMax))
	g.electionDeadline = time.Now().Add(time.Duration(timeout))
}

func (s *state) nextElectionTimer() *time.Timer {
	minTimeout := time.Duration(math.MaxInt64)
	now := time.Now()
	for _, g := range s.groups {
		timeout := g.electionDeadline.Sub(now)
		if timeout < minTimeout {
			minTimeout = timeout
		}
	}
	return time.NewTimer(minTimeout)
}

func (s *state) start() {
	for {
		electionTimer := s.nextElectionTimer()
		select {
		case op := <-s.ops:
			switch op := op.(type) {
			case *stopOp:
				close(s.stopped)
				return

			case *createGroupOp:
				s.createGroup(op)

			case *requestVoteRequestOp:
				s.requestVoteRequest(op)

			case *requestVoteResponseOp:
				s.requestVoteResponse(op)

			default:
				if s.Strict {
					panic(util.Errorf("unknown op: %#v", op))
				}
				glog.Errorf("unknown op: %#v", op)
			}

		case now := <-electionTimer.C:
			s.handleElectionTimers(now)
		}
		electionTimer.Stop()
	}
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
		s.nodes[member] = &node{member, 1, &asyncClient{member, conn, s.ops}}
	}
	s.updateElectionDeadline(op.group)
	s.groups[op.group.name] = op.group
	op.ch <- nil
}

func (s *state) requestVoteRequest(op *requestVoteRequestOp) {
	g, ok := s.groups[op.req.Group]
	if !ok {
		op.ch <- util.Errorf("unknown group %s", op.req.Group)
		return
	}
	if g.votedFor != "" && g.votedFor != op.req.CandidateID {
		op.resp.VoteGranted = false
	} else {
		// TODO: check log positions
		g.currentTerm = op.req.Term
		op.resp.VoteGranted = true
	}
	op.resp.Term = g.currentTerm
	op.ch <- nil
}

func (s *state) requestVoteResponse(op *requestVoteResponseOp) {
	g := s.groups[op.req.Group]
	if op.resp.Term < g.currentTerm {
		return
	}
	if op.resp.VoteGranted {
		g.votes[op.nodeID] = op.resp.VoteGranted
	}
	if len(g.votes)*2 > len(g.members) {
		hackyTestChannel <- s.id
	}
}

func (s *state) handleElectionTimers(now time.Time) {
	for _, g := range s.groups {
		if now.After(g.electionDeadline) {
			s.becomeCandidate(g)
		}
	}
}

func (s *state) becomeCandidate(g *group) {
	if g.role == leader {
		panic("cannot transition from leader to candidate")
	}
	g.role = candidate
	g.currentTerm++
	g.votedFor = s.id
	g.votes = make(map[string]bool)
	s.updateElectionDeadline(g)
	for _, id := range g.members {
		node := s.nodes[id]
		node.client.requestVote(&RequestVoteRequest{
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
var hackyTestChannel = make(chan string)
