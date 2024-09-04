// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rac2

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func mark(term, index uint64) LogMark {
	return LogMark{Term: term, Index: index}
}

func (l *LogTracker) check(t *testing.T) {
	require.LessOrEqual(t, l.stable, l.last.Index)
	stable := l.Stable()
	require.Equal(t, l.last.Term, stable.Term)
	for _, waiting := range l.waiting {
		if ln := len(waiting); ln != 0 {
			require.LessOrEqual(t, waiting[ln-1].Index, l.last.Index)
			require.LessOrEqual(t, waiting[ln-1].Term, l.last.Term)
		}
		for i, ln := 1, len(waiting); i < ln; i++ {
			require.Less(t, waiting[i-1].Index, waiting[i].Index)
			require.LessOrEqual(t, waiting[i-1].Term, waiting[i].Term)
		}
	}
	a := l.Admitted()
	require.Equal(t, stable.Term, a.Term)
	for _, index := range a.Admitted {
		require.LessOrEqual(t, index, stable.Index)
	}
}

func TestLogTrackerAppend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tt := range []struct {
		last   LogMark
		stable uint64

		after uint64
		to    LogMark
		notOk bool
		want  uint64 // stable index
	}{
		// Invalid appends. Writes with stale term or index gaps.
		{last: mark(10, 100), after: 100, to: mark(9, 200), notOk: true},
		{last: mark(10, 100), after: 200, to: mark(10, 300), notOk: true},
		{last: mark(10, 100), after: 20, to: mark(10, 50), notOk: true},
		// Valid appends.
		{after: 0, to: mark(10, 100), want: 0},
		{last: mark(10, 100), after: 100, to: mark(10, 150)},
		{last: mark(10, 100), after: 100, to: mark(15, 150)},
		{last: mark(10, 100), after: 50, to: mark(15, 150)},
		// Stable index does not change.
		{last: mark(10, 100), stable: 50, after: 100, to: mark(10, 150), want: 50},
		{last: mark(10, 100), stable: 50, after: 100, to: mark(11, 150), want: 50},
		{last: mark(10, 100), stable: 50, after: 70, to: mark(11, 150), want: 50},
		// Stable index regresses.
		{last: mark(10, 100), stable: 50, after: 30, to: mark(11, 150), want: 30},
	} {
		t.Run("", func(t *testing.T) {
			defer func() {
				failed := recover() != nil
				require.Equal(t, tt.notOk, failed)
			}()
			l := NewLogTracker(tt.last)
			l.stable = tt.stable
			l.check(t)
			l.Append(context.Background(), tt.after, tt.to)
			l.check(t)
			require.Equal(t, tt.to, l.last)
			require.Equal(t, tt.want, l.Stable().Index)
		})
	}
}

func TestLogTrackerLogSynced(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tt := range []struct {
		last   LogMark
		stable uint64

		sync  LogMark
		notOk bool
		want  uint64 // stable index
	}{
		// Invalid syncs.
		{last: mark(5, 20), sync: mark(7, 10), notOk: true},
		{last: mark(5, 20), sync: mark(5, 25), notOk: true},

		// Valid syncs. The sync mark <= the latest observed mark.
		{last: mark(5, 20), stable: 5, sync: mark(4, 10), want: 5},
		{last: mark(5, 20), stable: 15, sync: mark(4, 10), want: 15},
		{last: mark(5, 20), stable: 15, sync: mark(4, 100), want: 15},
		{last: mark(5, 20), sync: mark(5, 10), want: 10},
		{last: mark(5, 20), stable: 15, sync: mark(5, 10), want: 15},
		{last: mark(5, 20), sync: mark(5, 20), want: 20},
		{last: mark(5, 40), stable: 15, sync: mark(5, 30), want: 30},
	} {
		t.Run("", func(t *testing.T) {
			defer func() {
				failed := recover() != nil
				require.Equal(t, tt.notOk, failed)
			}()
			l := NewLogTracker(tt.last)
			l.stable = tt.stable
			l.check(t)
			l.LogSynced(context.Background(), tt.sync)
			l.check(t)
			require.Equal(t, tt.last, l.last)
			require.Equal(t, tt.want, l.Stable().Index)
		})
	}
}

func TestLogTrackerSnapSynced(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tt := range []struct {
		last   LogMark
		stable uint64
		snap   LogMark
		notOk  bool
	}{
		// Invalid snapshots.
		{last: mark(5, 20), snap: mark(4, 30), notOk: true},
		{last: mark(5, 20), snap: mark(5, 10), notOk: true},
		// Valid snapshots.
		{last: mark(5, 20), snap: mark(5, 30)},
		{last: mark(5, 20), snap: mark(6, 10)},
		{last: mark(5, 20), snap: mark(6, 30)},
	} {
		t.Run("", func(t *testing.T) {
			defer func() {
				failed := recover() != nil
				require.Equal(t, tt.notOk, failed)
			}()
			l := NewLogTracker(tt.last)
			l.stable = tt.stable
			l.check(t)
			l.SnapSynced(context.Background(), tt.snap)
			l.check(t)
			require.Equal(t, tt.snap, l.last)
			require.Equal(t, tt.snap.Index, l.Stable().Index)
		})
	}
}

func TestLogTracker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	readPri := func(t *testing.T, d *datadriven.TestData) raftpb.Priority {
		var s string
		d.ScanArgs(t, "pri", &s)
		pri := priFromString(s)
		if pri == raftpb.NumPriorities {
			t.Fatalf("unknown pri: %s", s)
		}
		return pri
	}
	readMark := func(t *testing.T, d *datadriven.TestData, idxName string) LogMark {
		var mark LogMark
		d.ScanArgs(t, "term", &mark.Term)
		d.ScanArgs(t, idxName, &mark.Index)
		return mark
	}

	var tracker LogTracker
	ctx := context.Background()
	run := func(t *testing.T, d *datadriven.TestData) (out string) {
		defer func() {
			if err := recover(); err != nil {
				out = "error"
			}
		}()

		switch d.Cmd {
		case "reset": // Example: reset term=1 index=10
			stable := readMark(t, d, "index")
			tracker = NewLogTracker(stable)
			return tracker.DebugString()

		case "append": // Example: append term=10 after=100 to=200
			var after uint64
			d.ScanArgs(t, "after", &after)
			to := readMark(t, d, "to")
			tracker.Append(ctx, after, to)
			return tracker.DebugString()

		case "sync": // Example: sync term=10 index=100
			mark := readMark(t, d, "index")
			tracker.LogSynced(ctx, mark)
			return tracker.DebugString()

		case "register": // Example: register term=10 index=100 pri=LowPri
			mark := readMark(t, d, "index")
			pri := readPri(t, d)
			tracker.Register(ctx, mark, pri)
			return tracker.DebugString()

		case "admit": // Example: admit term=10 index=100 pri=LowPri
			mark := readMark(t, d, "index")
			pri := readPri(t, d)
			tracker.LogAdmitted(ctx, mark, pri)
			return tracker.DebugString()

		default:
			t.Fatalf("unknown command: %s", d.Cmd)
			return ""
		}
	}

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "log_tracker"), run)
}

// priFromString converts a string to Priority.
// TODO(pav-kv): move to the package next to Priority.
func priFromString(s string) raftpb.Priority {
	switch s {
	case "LowPri":
		return raftpb.LowPri
	case "NormalPri":
		return raftpb.NormalPri
	case "AboveNormalPri":
		return raftpb.AboveNormalPri
	case "HighPri":
		return raftpb.HighPri
	default:
		return raftpb.NumPriorities
	}
}
