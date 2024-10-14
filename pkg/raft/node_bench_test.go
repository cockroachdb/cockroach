// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func BenchmarkOneNode(b *testing.B) {
	s := newTestMemoryStorage(withPeers(1))
	rn := newTestRawNode(1, 10, 1, s)
	require.NoError(b, rn.Campaign())
	rd := rn.Ready()
	require.NoError(b, s.Append(rd.Entries))
	rn.Advance(rd)

	var mu sync.Mutex
	propose := func() {
		mu.Lock()
		defer mu.Unlock()
		_ = rn.Propose([]byte("foo"))
	}
	ready := func() bool {
		var rd Ready
		func() {
			mu.Lock()
			defer mu.Unlock()
			rd = rn.Ready()
		}()
		_ = s.Append(rd.Entries)
		// a reasonable disk sync latency
		time.Sleep(1 * time.Millisecond)
		func() {
			mu.Lock()
			defer mu.Unlock()
			rn.Advance(rd)
		}()
		return rd.HardState.Commit != uint64(b.N+1)
	}

	go func() {
		for i := 0; i < b.N; i++ {
			propose()
		}
	}()
	for ready() {
	}
}
