// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rafttest

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func (env *InteractionEnv) handleSetLazyReplication(t *testing.T, d datadriven.TestData) error {
	require.Len(t, d.CmdArgs, 2)
	idx := firstAsNodeIdx(t, d)
	lazy, err := strconv.ParseBool(d.CmdArgs[1].Key)
	require.NoError(t, err)
	env.Nodes[idx].SetLazyReplication(lazy)
	return nil
}

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
