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

import "time"

// Ticker encapsulates the timing-related parts of the raft protocol.
type Ticker interface {
	// This channel will be readable once per tick. The time value returned is unspecified;
	// the channel has this type for compatibility with time.Ticker but other implementations
	// may not return real times.
	Chan() <-chan time.Time
}

type realTicker struct {
	*time.Ticker
}

func newTicker(interval time.Duration) Ticker {
	return &realTicker{time.NewTicker(interval)}
}

func (t *realTicker) Chan() <-chan time.Time {
	return t.C
}

// manualTicker is a fake implementation of the Ticker interface.  With this ticker
// time does not flow normally, but time-based events can be triggered manually with
// the Tick method.
type manualTicker struct {
	ch chan time.Time
}

func newManualTicker() *manualTicker {
	return &manualTicker{make(chan time.Time)}
}

func (m *manualTicker) Chan() <-chan time.Time {
	return m.ch
}

func (m *manualTicker) Tick() {
	m.ch <- time.Time{}
}
