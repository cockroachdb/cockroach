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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/datadriven"
)

func (env *InteractionEnv) handleProcessReady(t *testing.T, d datadriven.TestData) error {
	idxs := nodeIdxs(t, d)
	for _, idx := range idxs {
		var err error
		if len(idxs) > 1 {
			fmt.Fprintf(env.Output, "> %d handling Ready\n", idx+1)
			env.withIndent(func() { err = env.ProcessReady(idx) })
		} else {
			err = env.ProcessReady(idx)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// ProcessReady runs Ready handling on the node with the given index.
func (env *InteractionEnv) ProcessReady(idx int) error {
	// TODO(tbg): Allow simulating crashes here.
	n := &env.Nodes[idx]
	rd := n.Ready()
	env.Output.WriteString(raft.DescribeReady(rd, defaultEntryFormatter))
	// Immediately send the messages that are not contingent on storage sync.
	env.Messages = append(env.Messages, rd.Messages...)

	if !n.asyncWrites {
		if err := processAppend(n, rd.StorageAppend); err != nil {
			return err
		}
		ack := rd.Ack()
		for msg := range ack.Send(raftpb.PeerID(idx + 1)) {
			env.Messages = append(env.Messages, msg)
		}

		if !rd.Committed.Empty() {
			ls := n.RawNode.LogSnapshot()
			apply, err := ls.Slice(rd.Committed, n.Config.MaxCommittedSizePerReady)
			if err != nil {
				return err
			}
			// TODO(pav-kv): move printing to processApply when the async write path
			// is refactored to also use LogSnapshot.
			env.Output.WriteString("Applying:\n")
			env.Output.WriteString(raft.DescribeEntries(apply, defaultEntryFormatter))
			if err := processApply(n, apply); err != nil {
				return err
			}
			n.AckApplied(apply)
		}

		n.AckAppend(ack)
		return nil
	}

	if app := rd.StorageAppend; !app.Empty() {
		n.AppendWork = append(n.AppendWork, app)
	}
	if span := rd.Committed; !span.Empty() {
		if was := n.ApplyWork; span.After > was.Last {
			n.ApplyWork = span
		} else {
			n.ApplyWork.Last = span.Last
		}
		n.AckApplying(span.Last)
	}
	return nil
}
