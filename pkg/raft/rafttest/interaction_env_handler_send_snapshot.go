// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2023 The etcd Authors
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func (env *InteractionEnv) handleSendSnapshot(t *testing.T, d datadriven.TestData) error {
	idxs := nodeIdxs(t, d)
	require.Len(t, idxs, 2)
	return env.SendSnapshot(idxs[0], idxs[1])
}

// SendSnapshot sends a snapshot.
func (env *InteractionEnv) SendSnapshot(fromIdx, toIdx int) error {
	snap, err := env.Nodes[fromIdx].Snapshot()
	if err != nil {
		return err
	}
	from, to := raftpb.PeerID(fromIdx+1), raftpb.PeerID(toIdx+1)
	msg := raftpb.Message{
		Type:     raftpb.MsgSnap,
		Term:     env.Nodes[fromIdx].BasicStatus().Term,
		From:     from,
		To:       to,
		Snapshot: &snap,
	}
	env.Messages = append(env.Messages, msg)
	_, _ = env.Output.WriteString(raft.DescribeMessage(msg, nil))
	return nil
}
