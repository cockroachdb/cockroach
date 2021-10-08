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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/apply"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// logging is used for Example.
var logging bool

func setLogging(on bool) func() {
	bef := logging
	logging = on
	return func() {
		logging = bef
	}
}

type cmd struct {
	index                 uint64
	nonTrivial            bool
	nonLocal              bool
	shouldReject          bool
	shouldThrowErrRemoved bool

	acked    bool
	finished bool
}

type checkedCmd struct {
	*cmd
	rejected bool
}

type appliedCmd struct {
	*checkedCmd
}

func (c *cmd) Index() uint64   { return c.index }
func (c *cmd) IsTrivial() bool { return !c.nonTrivial }
func (c *cmd) IsLocal() bool   { return !c.nonLocal }
func (c *cmd) AckErrAndFinish(_ context.Context, err error) error {
	c.acked = true
	c.finished = true
	if logging {
		fmt.Printf(" acknowledging rejected command %d with err=%s\n", c.Index(), err)
	}
	return nil
}
func (c *checkedCmd) Rejected() bool                { return c.rejected }
func (c *checkedCmd) CanAckBeforeApplication() bool { return true }
func (c *checkedCmd) AckSuccess(context.Context) error {
	c.acked = true
	if logging {
		fmt.Printf(" acknowledging command %d before application\n", c.Index())
	}
	return nil
}
func (c *appliedCmd) AckOutcomeAndFinish(context.Context) error {
	c.finished = true
	if c.acked {
		if logging {
			fmt.Printf(" finishing command %d; rejected=%t\n", c.Index(), c.Rejected())
		}
	} else {
		if logging {
			fmt.Printf(" acknowledging and finishing command %d; rejected=%t\n", c.Index(), c.Rejected())
		}
		c.acked = true
	}
	return nil
}

type cmdSlice []*cmd
type checkedCmdSlice []*checkedCmd
type appliedCmdSlice []*appliedCmd

func (s *cmdSlice) Valid() bool                              { return len(*s) > 0 }
func (s *cmdSlice) Next()                                    { *s = (*s)[1:] }
func (s *cmdSlice) NewList() apply.CommandList               { return new(cmdSlice) }
func (s *cmdSlice) NewCheckedList() apply.CheckedCommandList { return new(checkedCmdSlice) }
func (s *cmdSlice) Close()                                   {}
func (s *cmdSlice) Cur() apply.Command                       { return (*s)[0] }
func (s *cmdSlice) Append(c apply.Command)                   { *s = append(*s, c.(*cmd)) }

func (s *checkedCmdSlice) Valid() bool                              { return len(*s) > 0 }
func (s *checkedCmdSlice) Next()                                    { *s = (*s)[1:] }
func (s *checkedCmdSlice) NewAppliedList() apply.AppliedCommandList { return new(appliedCmdSlice) }
func (s *checkedCmdSlice) Close()                                   {}
func (s *checkedCmdSlice) CurChecked() apply.CheckedCommand         { return (*s)[0] }
func (s *checkedCmdSlice) AppendChecked(c apply.CheckedCommand)     { *s = append(*s, c.(*checkedCmd)) }

func (s *appliedCmdSlice) Valid() bool                          { return len(*s) > 0 }
func (s *appliedCmdSlice) Next()                                { *s = (*s)[1:] }
func (s *appliedCmdSlice) Close()                               {}
func (s *appliedCmdSlice) CurApplied() apply.AppliedCommand     { return (*s)[0] }
func (s *appliedCmdSlice) AppendApplied(c apply.AppliedCommand) { *s = append(*s, c.(*appliedCmd)) }

var _ apply.Command = &cmd{}
var _ apply.CheckedCommand = &checkedCmd{}
var _ apply.AppliedCommand = &appliedCmd{}
var _ apply.CommandList = &cmdSlice{}
var _ apply.CheckedCommandList = &checkedCmdSlice{}
var _ apply.AppliedCommandList = &appliedCmdSlice{}

type testStateMachine struct {
	batches            [][]uint64
	applied            []uint64
	appliedSideEffects []uint64
	batchOpen          bool
}

func getTestStateMachine() *testStateMachine {
	return new(testStateMachine)
}

func (sm *testStateMachine) NewBatch(ephemeral bool) apply.Batch {
	if sm.batchOpen {
		panic("batch not closed")
	}
	sm.batchOpen = true
	return &testBatch{sm: sm, ephemeral: ephemeral}
}
func (sm *testStateMachine) ApplySideEffects(
	cmdI apply.CheckedCommand,
) (apply.AppliedCommand, error) {
	cmd := cmdI.(*checkedCmd)
	sm.appliedSideEffects = append(sm.appliedSideEffects, cmd.index)
	if logging {
		fmt.Printf(" applying side-effects of command %d\n", cmd.Index())
	}
	if cmd.shouldThrowErrRemoved {
		err := apply.ErrRemoved
		_ = cmd.AckErrAndFinish(context.Background(), err)
		return nil, err
	}
	acmd := appliedCmd{checkedCmd: cmd}
	return &acmd, nil
}

type testBatch struct {
	sm        *testStateMachine
	ephemeral bool
	staged    []uint64
}

func (b *testBatch) Stage(cmdI apply.Command) (apply.CheckedCommand, error) {
	cmd := cmdI.(*cmd)
	b.staged = append(b.staged, cmd.index)
	ccmd := checkedCmd{cmd: cmd, rejected: cmd.shouldReject}
	return &ccmd, nil
}
func (b *testBatch) ApplyToStateMachine(_ context.Context) error {
	if b.ephemeral {
		return errors.New("can't commit an ephemeral batch")
	}
	b.sm.batches = append(b.sm.batches, b.staged)
	b.sm.applied = append(b.sm.applied, b.staged...)
	if logging {
		fmt.Printf(" applying batch with commands=%v\n", b.staged)
	}
	return nil
}
func (b *testBatch) Close() {
	b.sm.batchOpen = false
}

type testDecoder struct {
	nonTrivial            map[uint64]bool
	nonLocal              map[uint64]bool
	shouldReject          map[uint64]bool
	shouldThrowErrRemoved map[uint64]bool

	cmds []*cmd
}

func newTestDecoder() *testDecoder {
	return &testDecoder{
		nonTrivial:            make(map[uint64]bool),
		nonLocal:              make(map[uint64]bool),
		shouldReject:          make(map[uint64]bool),
		shouldThrowErrRemoved: make(map[uint64]bool),
	}
}

func (d *testDecoder) DecodeAndBind(_ context.Context, ents []raftpb.Entry) (bool, error) {
	d.cmds = make([]*cmd, len(ents))
	for i, ent := range ents {
		idx := ent.Index
		cmd := &cmd{
			index:                 idx,
			nonTrivial:            d.nonTrivial[idx],
			nonLocal:              d.nonLocal[idx],
			shouldReject:          d.shouldReject[idx],
			shouldThrowErrRemoved: d.shouldThrowErrRemoved[idx],
		}
		d.cmds[i] = cmd
		if logging {
			fmt.Printf(" decoding command %d; local=%t\n", cmd.Index(), cmd.IsLocal())
		}
	}
	return true, nil
}
func (d *testDecoder) NewCommandIter() apply.CommandIterator {
	it := cmdSlice(d.cmds)
	return &it
}
func (d *testDecoder) Reset() {}

func makeEntries(num int) []raftpb.Entry {
	ents := make([]raftpb.Entry, num)
	for i := range ents {
		ents[i].Index = uint64(i + 1)
	}
	return ents
}

func TestApplyCommittedEntries(t *testing.T) {
	ctx := context.Background()
	ents := makeEntries(6)

	sm := getTestStateMachine()
	dec := newTestDecoder()
	dec.nonTrivial[3] = true
	dec.nonTrivial[4] = true
	dec.nonTrivial[6] = true

	// Use an apply.Task to apply all commands.
	appT := apply.MakeTask(sm, dec)
	defer appT.Close()
	require.NoError(t, appT.Decode(ctx, ents))
	require.NoError(t, appT.ApplyCommittedEntries(ctx))

	// Assert that all commands were applied in the correct batches.
	exp := testStateMachine{
		batches:            [][]uint64{{1, 2}, {3}, {4}, {5}, {6}},
		applied:            []uint64{1, 2, 3, 4, 5, 6},
		appliedSideEffects: []uint64{1, 2, 3, 4, 5, 6},
	}
	require.Equal(t, exp, *sm)

	// Assert that all commands were acknowledged and finished.
	for _, cmd := range dec.cmds {
		require.True(t, cmd.acked)
		require.True(t, cmd.finished)
	}
}

func TestApplyCommittedEntriesWithBatchSize(t *testing.T) {
	ctx := context.Background()
	ents := makeEntries(7)

	sm := getTestStateMachine()
	dec := newTestDecoder()
	dec.nonTrivial[4] = true

	// Use an apply.Task to apply all commands with a batch size limit.
	appT := apply.MakeTask(sm, dec)
	appT.SetMaxBatchSize(2)
	defer appT.Close()
	require.NoError(t, appT.Decode(ctx, ents))
	require.NoError(t, appT.ApplyCommittedEntries(ctx))

	// Assert that all commands were applied in the correct batches.
	exp := testStateMachine{
		batches:            [][]uint64{{1, 2}, {3}, {4}, {5, 6}, {7}},
		applied:            []uint64{1, 2, 3, 4, 5, 6, 7},
		appliedSideEffects: []uint64{1, 2, 3, 4, 5, 6, 7},
	}
	require.Equal(t, exp, *sm)

	// Assert that all commands were acknowledged and finished.
	for _, cmd := range dec.cmds {
		require.True(t, cmd.acked)
		require.True(t, cmd.finished)
	}
}

func TestAckCommittedEntriesBeforeApplication(t *testing.T) {
	ctx := context.Background()
	ents := makeEntries(9)

	sm := getTestStateMachine()
	dec := newTestDecoder()
	dec.nonTrivial[6] = true
	dec.nonTrivial[7] = true
	dec.nonTrivial[9] = true
	dec.nonLocal[2] = true
	dec.shouldReject[3] = true

	// Use an apply.Task to ack all commands before applying them.
	appT := apply.MakeTask(sm, dec)
	defer appT.Close()
	require.NoError(t, appT.Decode(ctx, ents))
	require.NoError(t, appT.AckCommittedEntriesBeforeApplication(ctx, 10 /* maxIndex */))

	// Assert that the state machine was not updated.
	require.Equal(t, testStateMachine{}, *sm)

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
	ents = makeEntries(5)

	dec = newTestDecoder()
	dec.nonLocal[2] = true
	dec.shouldReject[3] = true

	appT = apply.MakeTask(sm, dec)
	require.NoError(t, appT.Decode(ctx, ents))
	require.NoError(t, appT.AckCommittedEntriesBeforeApplication(ctx, 4 /* maxIndex */))

	// Assert that the state machine was not updated.
	require.Equal(t, testStateMachine{}, *sm)

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

func TestApplyCommittedEntriesWithErr(t *testing.T) {
	ctx := context.Background()
	ents := makeEntries(6)

	sm := getTestStateMachine()
	dec := newTestDecoder()
	dec.nonTrivial[3] = true
	dec.shouldThrowErrRemoved[3] = true
	dec.nonTrivial[6] = true

	// Use an apply.Task to apply all commands.
	appT := apply.MakeTask(sm, dec)
	defer appT.Close()
	require.NoError(t, appT.Decode(ctx, ents))
	require.Equal(t, apply.ErrRemoved, appT.ApplyCommittedEntries(ctx))

	// Assert that only commands up to the replica removal were applied.
	exp := testStateMachine{
		batches:            [][]uint64{{1, 2}, {3}},
		applied:            []uint64{1, 2, 3},
		appliedSideEffects: []uint64{1, 2, 3},
	}
	require.Equal(t, exp, *sm)

	// Assert that all commands were acknowledged and finished, even though not
	// all were applied.
	for _, cmd := range dec.cmds {
		require.True(t, cmd.acked)
		require.True(t, cmd.finished)
	}
}
