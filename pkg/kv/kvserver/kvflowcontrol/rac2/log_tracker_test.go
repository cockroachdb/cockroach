package rac2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func mark(term, index uint64) LogMark {
	return LogMark{Term: term, Index: index}
}

func (l *LogTracker) check(t *testing.T) {
	require.LessOrEqual(t, l.stable, l.last.Index)
	require.Equal(t, l.last.Term, l.Stable().Term)
	require.Equal(t, l.last.Index+1, l.NextIndex())
}

func TestLogTrackerAppend(t *testing.T) {
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
			l := NewLogTracker(tt.last)
			l.stable = tt.stable
			l.check(t)
			was := l

			require.Equal(t, !tt.notOk, l.Append(tt.after, tt.to))
			l.check(t)
			if !tt.notOk {
				require.Equal(t, tt.to, l.last)
				require.Equal(t, tt.want, l.Stable().Index)
			} else {
				require.Equal(t, was, l)
			}
		})
	}
}

func TestLogTrackerSync(t *testing.T) {
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
			l := NewLogTracker(tt.last)
			l.stable = tt.stable
			l.check(t)
			was := l

			require.Equal(t, !tt.notOk, l.Sync(tt.sync))
			l.check(t)
			require.Equal(t, tt.last, l.last)
			if !tt.notOk {
				require.Equal(t, tt.want, l.Stable().Index)
			} else {
				require.Equal(t, was, l)
			}
		})
	}
}

func TestLogTrackerSnap(t *testing.T) {
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
			l := NewLogTracker(tt.last)
			l.stable = tt.stable
			l.check(t)
			was := l

			require.Equal(t, !tt.notOk, l.Snap(tt.snap))
			l.check(t)
			if !tt.notOk {
				require.Equal(t, tt.snap, l.last)
				require.Equal(t, tt.snap.Index, l.Stable().Index)
			} else {
				require.Equal(t, was, l)
			}
		})
	}
}
