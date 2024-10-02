// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rafttest

import (
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func (env *InteractionEnv) handleSendMsgApp(t *testing.T, d datadriven.TestData) error {
	require.Len(t, d.CmdArgs, 4)
	from := firstAsNodeIdx(t, d)
	var to int
	var lo, hi uint64
	d.ScanArgs(t, "to", &to)
	d.ScanArgs(t, "lo", &lo)
	d.ScanArgs(t, "hi", &hi)
	return env.SendMsgApp(from, pb.PeerID(to), lo, hi)
}

func (env *InteractionEnv) SendMsgApp(from int, to pb.PeerID, lo, hi uint64) error {
	rn := env.Nodes[from].RawNode
	snap := rn.LogSnapshot()
	ls, err := snap.LogSlice(lo, hi, math.MaxUint64)
	if err != nil {
		return err
	}
	if msg, ok := rn.SendMsgApp(to, ls); ok {
		env.Output.WriteString(raft.DescribeMessage(msg, defaultEntryFormatter))
		env.Messages = append(env.Messages, msg)
	} else {
		env.Output.WriteString(fmt.Sprintf("could not send MsgApp (%d,%d] to %d", lo, hi, to))
	}
	return nil
}
