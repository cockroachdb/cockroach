// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package apply_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/storage/apply"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/raftpb"
)

type cmd struct {
	index   uint64
	trivial bool
	local   bool

	shouldReject bool
	acked        bool
	finished     bool
}

type checkedCmd struct {
	*cmd
	rejected bool
}

type appliedCmd struct {
	*checkedCmd
}

func (c *cmd) Index() uint64                        { return c.index }
func (c *cmd) IsTrivial() bool                      { return c.trivial }
func (c *cmd) IsLocal() bool                        { return c.local }
func (c *checkedCmd) Rejected() bool                { return c.rejected }
func (c *checkedCmd) CanAckBeforeApplication() bool { return false }
func (c *checkedCmd) AckSuccess() error {
	c.acked = true
	return nil
}
func (c *appliedCmd) AckOutcomeAndFinish() error {
	c.acked = true
	c.finished = true
	return nil
}

type cmdSlice []cmd
type checkedCmdSlice []checkedCmd
type appliedCmdSlice []appliedCmd

func (s *cmdSlice) Valid() bool                              { return len(*s) > 0 }
func (s *cmdSlice) Next()                                    { *s = (*s)[1:] }
func (s *cmdSlice) NewList() apply.CommandList               { return new(cmdSlice) }
func (s *cmdSlice) NewCheckedList() apply.CheckedCommandList { return new(checkedCmdSlice) }
func (s *cmdSlice) NewAppliedList() apply.AppliedCommandList { return new(appliedCmdSlice) }
func (s *cmdSlice) Close()                                   {}
func (s *cmdSlice) Cur() apply.Command                       { return &(*s)[0] }
func (s *cmdSlice) Append(c apply.Command)                   { *s = append(*s, *c.(*cmd)) }

func (s *checkedCmdSlice) Valid() bool                              { return len(*s) > 0 }
func (s *checkedCmdSlice) Next()                                    { *s = (*s)[1:] }
func (s *checkedCmdSlice) NewList() apply.CommandList               { return new(cmdSlice) }
func (s *checkedCmdSlice) NewCheckedList() apply.CheckedCommandList { return new(checkedCmdSlice) }
func (s *checkedCmdSlice) NewAppliedList() apply.AppliedCommandList { return new(appliedCmdSlice) }
func (s *checkedCmdSlice) Close()                                   {}
func (s *checkedCmdSlice) CurChecked() apply.CheckedCommand         { return &(*s)[0] }
func (s *checkedCmdSlice) AppendChecked(c apply.CheckedCommand)     { *s = append(*s, *c.(*checkedCmd)) }

func (s *appliedCmdSlice) Valid() bool                              { return len(*s) > 0 }
func (s *appliedCmdSlice) Next()                                    { *s = (*s)[1:] }
func (s *appliedCmdSlice) NewList() apply.CommandList               { return new(cmdSlice) }
func (s *appliedCmdSlice) NewCheckedList() apply.CheckedCommandList { return new(checkedCmdSlice) }
func (s *appliedCmdSlice) NewAppliedList() apply.AppliedCommandList { return new(appliedCmdSlice) }
func (s *appliedCmdSlice) Close()                                   {}
func (s *appliedCmdSlice) CurApplied() apply.AppliedCommand         { return &(*s)[0] }
func (s *appliedCmdSlice) AppendApplied(c apply.AppliedCommand)     { *s = append(*s, *c.(*appliedCmd)) }

var _ apply.Command = &cmd{}
var _ apply.CheckedCommand = &checkedCmd{}
var _ apply.AppliedCommand = &appliedCmd{}
var _ apply.CommandList = &cmdSlice{}
var _ apply.CheckedCommandList = &checkedCmdSlice{}
var _ apply.AppliedCommandList = &appliedCmdSlice{}

type testingStateMachine struct {
	batches            [][]uint64
	applied            []uint64
	appliedSideEffects []uint64
	batchOpen          bool
}

func (sm *testingStateMachine) NewBatch(mock bool) apply.Batch {
	if sm.batchOpen {
		panic("batch not closed")
	}
	sm.batchOpen = true
	return &testingBatch{sm: sm, mock: mock}
}
func (sm *testingStateMachine) ApplySideEffects(
	cmdI apply.CheckedCommand,
) (apply.AppliedCommand, error) {
	cmd := cmdI.(*checkedCmd)
	sm.appliedSideEffects = append(sm.appliedSideEffects, cmd.index)
	acmd := appliedCmd{checkedCmd: cmd}
	return &acmd, nil
}

type testingBatch struct {
	sm     *testingStateMachine
	mock   bool
	staged []uint64
}

func (b *testingBatch) Stage(cmdI apply.Command) (apply.CheckedCommand, error) {
	cmd := cmdI.(*cmd)
	b.staged = append(b.staged, cmd.index)
	ccmd := checkedCmd{cmd: cmd, rejected: cmd.shouldReject}
	return &ccmd, nil
}
func (b *testingBatch) Commit(_ context.Context) error {
	if b.mock {
		return errors.New("can't commit a mock batch")
	}
	b.sm.batches = append(b.sm.batches, b.staged)
	b.sm.applied = append(b.sm.applied, b.staged...)
	return nil
}
func (b *testingBatch) Close() {
	b.sm.batchOpen = false
}

type testingDecoder struct{ cmds []cmd }

func (d *testingDecoder) DecodeAndBind(_ context.Context, _ []raftpb.Entry) (bool, error) {
	return true, nil
}
func (d *testingDecoder) NewCommandIter() apply.CommandIterator {
	return (*cmdSlice)(&d.cmds)
}
func (d *testingDecoder) Reset() {}

func TestApplyCommittedEntries(t *testing.T) {
	ctx := context.Background()
	sm := testingStateMachine{}
	dec := testingDecoder{cmds: []cmd{
		{index: 1, trivial: true, local: true, shouldReject: false},
		{index: 2, trivial: true, local: true, shouldReject: false},
		{index: 3, trivial: false, local: true, shouldReject: false},
		{index: 4, trivial: false, local: true, shouldReject: false},
		{index: 5, trivial: true, local: true, shouldReject: false},
		{index: 6, trivial: false, local: true, shouldReject: false},
	}}

	// Use an apply.Task to apply all commands.
	appT := apply.MakeTask(&sm, &dec)
	defer appT.Close()
	require.NoError(t, appT.Decode(ctx, nil /* ents */))
	require.NoError(t, appT.ApplyCommittedEntries(ctx))

	// Assert that all commands were applied in the correct batches.
	exp := testingStateMachine{
		batches:            [][]uint64{{1, 2}, {3}, {4}, {5}, {6}},
		applied:            []uint64{1, 2, 3, 4, 5, 6},
		appliedSideEffects: []uint64{1, 2, 3, 4, 5, 6},
	}
	require.Equal(t, exp, sm)

	// Assert that all commands were acknowledged and finished.
	for _, cmd := range dec.cmds {
		require.True(t, cmd.acked)
		require.True(t, cmd.finished)
	}
}

func TestApplyCommittedEntriesWithBatchSize(t *testing.T) {
	ctx := context.Background()
	sm := testingStateMachine{}
	dec := testingDecoder{cmds: []cmd{
		{index: 1, trivial: true, local: true, shouldReject: false},
		{index: 2, trivial: true, local: true, shouldReject: false},
		{index: 3, trivial: true, local: true, shouldReject: false},
		{index: 4, trivial: false, local: true, shouldReject: false},
		{index: 5, trivial: true, local: true, shouldReject: false},
		{index: 6, trivial: true, local: true, shouldReject: false},
		{index: 7, trivial: true, local: true, shouldReject: false},
	}}

	// Use an apply.Task to apply all commands with a batch size limit.
	appT := apply.MakeTask(&sm, &dec)
	appT.SetMaxBatchSize(2)
	defer appT.Close()
	require.NoError(t, appT.Decode(ctx, nil /* ents */))
	require.NoError(t, appT.ApplyCommittedEntries(ctx))

	// Assert that all commands were applied in the correct batches.
	exp := testingStateMachine{
		batches:            [][]uint64{{1, 2}, {3}, {4}, {5, 6}, {7}},
		applied:            []uint64{1, 2, 3, 4, 5, 6, 7},
		appliedSideEffects: []uint64{1, 2, 3, 4, 5, 6, 7},
	}
	require.Equal(t, exp, sm)

	// Assert that all commands were acknowledged and finished.
	for _, cmd := range dec.cmds {
		require.True(t, cmd.acked)
		require.True(t, cmd.finished)
	}
}

func TestAckCommittedEntriesBeforeApplication(t *testing.T) {
	ctx := context.Background()
	sm := testingStateMachine{}
	dec := testingDecoder{cmds: []cmd{
		{index: 1, trivial: true, local: true, shouldReject: false},
		{index: 2, trivial: true, local: false, shouldReject: false},
		{index: 3, trivial: true, local: true, shouldReject: true},
		{index: 4, trivial: true, local: true, shouldReject: false},
		{index: 5, trivial: true, local: true, shouldReject: false},
		{index: 6, trivial: false, local: true, shouldReject: false},
		{index: 7, trivial: false, local: true, shouldReject: false},
		{index: 8, trivial: true, local: true, shouldReject: false},
		{index: 9, trivial: false, local: true, shouldReject: false},
	}}

	// Use an apply.Task to ack all commands before applying them.
	appT := apply.MakeTask(&sm, &dec)
	defer appT.Close()
	require.NoError(t, appT.Decode(ctx, nil /* ents */))
	require.NoError(t, appT.AckCommittedEntriesBeforeApplication(ctx, 10 /* maxIndex */))

	// Assert that the state machine was not updated.
	require.Equal(t, testingStateMachine{}, sm)

	// Assert that some commands were acknowledged early and that none were finished.
	for _, cmd := range dec.cmds {
		var exp bool
		switch cmd.index {
		case 1, 4, 5:
			exp = true // local and successful
		case 2:
			exp = false // remote
		case 3:
			exp = false // local and rejected
		default:
			exp = false // after first non-trivial cmd
		}
		require.Equal(t, exp, cmd.acked)
		require.False(t, cmd.finished)
	}

	// Try again with a lower maximum log index.
	appT.Close()
	appT = apply.MakeTask(&sm, &dec)
	dec = testingDecoder{cmds: []cmd{
		{index: 1, trivial: true, local: true, shouldReject: false},
		{index: 2, trivial: true, local: false, shouldReject: false},
		{index: 3, trivial: true, local: true, shouldReject: true},
		{index: 4, trivial: true, local: true, shouldReject: false},
		{index: 5, trivial: true, local: true, shouldReject: false},
	}}
	require.NoError(t, appT.Decode(ctx, nil /* ents */))
	require.NoError(t, appT.AckCommittedEntriesBeforeApplication(ctx, 4 /* maxIndex */))

	// Assert that the state machine was not updated.
	require.Equal(t, testingStateMachine{}, sm)

	// Assert that some commands were acknowledged early and that none were finished.
	for _, cmd := range dec.cmds {
		var exp bool
		switch cmd.index {
		case 1, 4:
			exp = true // local and successful
		case 2:
			exp = false // remote
		case 3:
			exp = false // local and rejected
		case 5:
			exp = false // index too high
		default:
			t.Fatalf("unexpected index %d", cmd.index)
		}
		require.Equal(t, exp, cmd.acked)
		require.False(t, cmd.finished)
	}
}
