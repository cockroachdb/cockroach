// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
}

func (l *logTracker) init(stable rac2.LogMark) {
	l.Lock()
	defer l.Unlock()
	l.lt = rac2.NewLogTracker(stable)
}

func (l *logTracker) admitted() (av rac2.AdmittedVector, dirty bool) {
	l.Lock()
	defer l.Unlock()
	dirty, l.dirty = l.dirty, false
	av = l.lt.Admitted()
	return av, dirty
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

func (l *logTracker) logAdmitted(ctx context.Context, at rac2.LogMark, pri raftpb.Priority) {
	l.Lock()
	defer l.Unlock()
	if l.lt.LogAdmitted(ctx, at, pri) {
		l.dirty = true
	}
}

func (l *logTracker) snapSynced(ctx context.Context, mark rac2.LogMark) {
	l.Lock()
	defer l.Unlock()
	if l.lt.SnapSynced(ctx, mark) {
		l.dirty = true
	}
}

func (l *logTracker) debugString() string {
	l.Lock()
	defer l.Unlock()
	var flags string
	if l.dirty {
		flags += "+dirty"
	}
	if len(flags) != 0 {
		flags = " [" + flags + "]"
	}
	return fmt.Sprintf("LogTracker%s: %s", flags, l.lt.DebugString())
}
