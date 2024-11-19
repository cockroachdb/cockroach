// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2019 The etcd Authors
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

package rafttest

import (
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/datadriven"
)

func (env *InteractionEnv) handleRaftLog(t *testing.T, d datadriven.TestData) error {
	idx := firstAsNodeIdx(t, d)
	return env.RaftLog(idx)
}

// RaftLog pretty prints the raft log to the output buffer.
func (env *InteractionEnv) RaftLog(idx int) error {
	s := env.Nodes[idx].Storage
	fi, li := s.FirstIndex(), s.LastIndex()
	if li < fi {
		// TODO(tbg): this is what MemoryStorage returns, but unclear if it's
		// the "correct" thing to do.
		fmt.Fprintf(env.Output, "log is empty: first index=%d, last index=%d", fi, li)
		return nil
	}
	ents, err := s.Entries(fi, li+1, math.MaxUint64)
	if err != nil {
		return err
	}
	env.Output.WriteString(raft.DescribeEntries(ents, defaultEntryFormatter))
	return err
}
