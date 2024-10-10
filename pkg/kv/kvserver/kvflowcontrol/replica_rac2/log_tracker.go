// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replica_rac2

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// logTracker wraps rac2.LogTracker with a mutex and state that helps track
// admitted vector changes and schedule their delivery to the leader. The
// semantics and requirements for all the methods is equivalent to the
// corresponding methods of rac2.LogTracker.
//
// The logTracker has its own mutex in order to avoid interference with objects
// that use wider mutexes such as raftMu.
type logTracker struct {
	syncutil.Mutex
	lt rac2.LogTracker
	// dirty is true when the admitted vector has changed and should be sent to
	// the leader.
	dirty bool
	// scheduled is true when the admitted vector change has been scheduled for
	// processing by raft Ready.
	scheduled bool
}

func (l *logTracker) init(stable rac2.LogMark) {
	l.Lock()
	defer l.Unlock()
	l.lt = rac2.NewLogTracker(stable)
}

// admitted returns the current admitted vector, and resets the dirty bit.
func (l *logTracker) admitted() rac2.AdmittedVector {
	l.Lock()
	defer l.Unlock()
	l.dirty = false
	return l.lt.Admitted()
}

// admittedDirty returns the current admitted vector if it hasn't been read yet,
// either by admitted() or admittedDirty() method. Returns zero value if the
// latest admitted vector has been read.
//
// This call is made from the Ready handler. It resets the scheduled flag, which
// allows the next logAdmitted call to return true and allow scheduling a Ready
// iteration again. This flow avoids unnecessary Ready scheduling events.
func (l *logTracker) admittedDirty() (av rac2.AdmittedVector, dirty bool) {
	l.Lock()
	defer l.Unlock()
	l.scheduled = false
	if !l.dirty {
		return
	}
	l.dirty = false
	return l.lt.Admitted(), true
}

func (l *logTracker) snap(ctx context.Context, mark rac2.LogMark) {
	l.Lock()
	defer l.Unlock()
	if l.lt.Snap(ctx, mark) {
		l.dirty = true
	}
}

func (l *logTracker) append(ctx context.Context, after uint64, to rac2.LogMark) {
	l.Lock()
	defer l.Unlock()
	if l.lt.Append(ctx, after, to) {
		l.dirty = true
	}
}

func (l *logTracker) register(ctx context.Context, at rac2.LogMark, pri raftpb.Priority) {
	l.Lock()
	defer l.Unlock()
	l.lt.Register(ctx, at, pri)
}

func (l *logTracker) logSynced(ctx context.Context, stable rac2.LogMark) {
	l.Lock()
	defer l.Unlock()
	if l.lt.LogSynced(ctx, stable) {
		l.dirty = true
	}
}

// logAdmitted returns true if the admitted vector has advanced and must be
// scheduled for delivery to the leader. At the moment, this schedules a Ready
// handling cycle.
//
// The returned bool helps to avoid scheduling Ready many times in a row, in
// situations when there are many consecutive logAdmitted calls. The next
// scheduling event will be allowed after the next admitted(true) call.
func (l *logTracker) logAdmitted(ctx context.Context, at rac2.LogMark, pri raftpb.Priority) bool {
	l.Lock()
	defer l.Unlock()
	if !l.lt.LogAdmitted(ctx, at, pri) {
		return false
	}
	l.dirty = true
	if !l.scheduled {
		l.scheduled = true
		return true
	}
	return false
}

func (l *logTracker) debugString() string {
	l.Lock()
	defer l.Unlock()
	var flags string
	if l.dirty {
		flags += "+dirty"
	}
	if l.scheduled {
		flags += "+sched"
	}
	if len(flags) != 0 {
		flags = " [" + flags + "]"
	}
	return fmt.Sprintf("LogTracker%s: %s", flags, l.lt.DebugString())
}
