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
	"sync"
	"time"
)

// Clock encapsulates the timing-related parts of the raft protocol.
// Types of events are separated in the API (i.e. NewElectionTimer() instead of
// NewTimer() so they can be triggered individually in tests.
type Clock interface {
	Now() time.Time

	// NewElectionTimer returns a timer to be used for triggering elections.  The resulting
	// Timer struct will have its C field filled out, but may not be a "real" timer, so it
	// must be stopped with StopElectionTimer instead of t.Stop()
	NewElectionTimer(time.Duration) *time.Timer
	StopElectionTimer(*time.Timer)
}

type realClock struct{}

// RealClock is the standard implementation of the Clock interface.
var RealClock = realClock{}

func (realClock) Now() time.Time {
	return time.Now()
}

func (realClock) NewElectionTimer(t time.Duration) *time.Timer {
	return time.NewTimer(t)
}

func (realClock) StopElectionTimer(t *time.Timer) {
	t.Stop()
}

// manualClock is a fake implementation of the Clock interface.  With this clock
// time does not flow normally, but time-based events can be triggered manually with
// methods like triggerElection.
type manualClock struct {
	sync.Mutex
	now             time.Time
	electionChannel chan time.Time
	nextElection    time.Time
}

func newManualClock() *manualClock {
	return &manualClock{
		now:             time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
		electionChannel: make(chan time.Time),
	}
}

func (m *manualClock) Now() time.Time {
	m.Lock()
	defer m.Unlock()
	return m.now
}

func (m *manualClock) NewElectionTimer(t time.Duration) *time.Timer {
	m.Lock()
	defer m.Unlock()
	m.nextElection = m.now.Add(t)
	return &time.Timer{C: m.electionChannel}
}

func (m *manualClock) StopElectionTimer(*time.Timer) {
}

func (m *manualClock) triggerElection() {
	m.Lock()
	m.now = m.nextElection
	now := m.now
	m.Unlock()
	m.electionChannel <- now
}
