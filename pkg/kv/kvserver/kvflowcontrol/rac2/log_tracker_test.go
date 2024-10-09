// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
		if ln := waiting.Length(); ln != 0 {
			require.LessOrEqual(t, waiting.At(ln-1).Index, l.last.Index)
			require.LessOrEqual(t, waiting.At(ln-1).Term, l.last.Term)
		}
		for i, ln := 1, waiting.Length(); i < ln; i++ {
			require.Less(t, waiting.At(i-1).Index, waiting.At(i).Index)
			require.LessOrEqual(t, waiting.At(i-1).Term, waiting.At(i).Term)
		}
	}
	a := l.Admitted()
	require.Equal(t, stable.Term, a.Term)
	for _, index := range a.Admitted {
		require.LessOrEqual(t, index, stable.Index)
	}
}

func TestAdmittedVectorMerge(t *testing.T) {
	av := func(term uint64, indices ...uint64) AdmittedVector {
		av := AdmittedVector{Term: term}
		require.Len(t, indices, len(av.Admitted))
		copy(av.Admitted[:], indices)
		return av
	}
	for _, tt := range [][3]AdmittedVector{
		// Different terms. Higher term wins. Merge is symmetric.
		{av(3, 10, 11, 12, 12), av(4, 10, 10, 20, 20), av(4, 10, 10, 20, 20)},
		{av(4, 10, 10, 20, 20), av(3, 10, 11, 12, 12), av(4, 10, 10, 20, 20)},
		// Same term. Highest index wins at each priority.
		{av(3, 10, 10, 10, 10), av(3, 20, 20, 20, 20), av(3, 20, 20, 20, 20)},
		{av(3, 20, 20, 20, 20), av(3, 10, 10, 10, 10), av(3, 20, 20, 20, 20)},
		{av(3, 10, 11, 12, 12), av(3, 8, 9, 20, 20), av(3, 10, 11, 20, 20)},
		{av(3, 5, 10, 5, 10), av(3, 10, 5, 10, 5), av(3, 10, 10, 10, 10)},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt[2], tt[0].Merge(tt[1]))
		})
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

func TestLogTrackerSnap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tt := range []struct {
		last   LogMark
		stable uint64
		snap   LogMark
		want   LogMark // stable mark after registering the snap
		notOk  bool
	}{
		// Invalid snapshots.
		{last: mark(5, 20), snap: mark(4, 30), notOk: true},
		{last: mark(5, 20), snap: mark(5, 10), notOk: true},
		// Valid snapshots.
		{last: mark(5, 20), stable: 20, snap: mark(5, 30), want: mark(5, 20)},
		{last: mark(5, 20), stable: 15, snap: mark(6, 10), want: mark(6, 10)},
		{last: mark(5, 20), stable: 20, snap: mark(6, 30), want: mark(6, 20)},
	} {
		t.Run("", func(t *testing.T) {
			defer func() {
				failed := recover() != nil
				require.Equal(t, tt.notOk, failed)
			}()
			l := NewLogTracker(tt.last)
			l.stable = tt.stable
			l.check(t)
			l.Snap(context.Background(), tt.snap)
			l.check(t)
			require.Equal(t, tt.snap, l.last)
			require.Equal(t, tt.want, l.Stable())
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
	state := func(updated bool) string {
		var s string
		if updated {
			s += "[upd] "
		}
		return s + tracker.DebugString()
	}

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
			return state(false)

		case "append": // Example: append term=10 after=100 to=200
			var after uint64
			d.ScanArgs(t, "after", &after)
			to := readMark(t, d, "to")
			updated := tracker.Append(ctx, after, to)
			return state(updated)

		case "snap":
			mark := readMark(t, d, "index")
			updated := tracker.Snap(ctx, mark)
			str := state(updated)
			return str

		case "sync": // Example: sync term=10 index=100
			mark := readMark(t, d, "index")
			updated := tracker.LogSynced(ctx, mark)
			return state(updated)

		case "register": // Example: register term=10 index=100 pri=LowPri
			mark := readMark(t, d, "index")
			pri := readPri(t, d)
			tracker.Register(ctx, mark, pri)
			return state(false)

		case "admit": // Example: admit term=10 index=100 pri=LowPri
			mark := readMark(t, d, "index")
			pri := readPri(t, d)
			updated := tracker.LogAdmitted(ctx, mark, pri)
			return state(updated)

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
