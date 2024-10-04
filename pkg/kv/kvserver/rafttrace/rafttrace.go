// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rafttrace

import (
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type Logger interface {
	Tracef(format string, v ...any)
}

type RaftTracer struct {
	mu   syncutil.RWMutex
	mark raft.LogMark
	log  Logger
}

func New(logger Logger) *RaftTracer {
	return &RaftTracer{log: logger}
}

func (r *RaftTracer) MaybeRegisterProposal(mark raft.LogMark, id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mark.Term == 0 {
		r.mark = mark
		r.log.Tracef("[raft] registered proposal %x at log mark (t%d,i%d) for tracing",
			id, mark.Term, mark.Index)
	}
}

func (r *RaftTracer) MaybeRegister(mark raft.LogMark) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mark.Term == 0 {
		r.mark = mark
		r.log.Tracef("[raft] registered log mark (t%d,i%d) for tracing", mark.Term, mark.Index)
	}
}

func (r *RaftTracer) Unregister() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.log.Tracef("[raft] unregistered log mark (t%d,i%d) from tracing", r.mark.Term, r.mark.Index)
	r.mark = raft.LogMark{}
}

func (r *RaftTracer) MaybeTrace(m raftpb.Message) (raft.LogMark, bool) {
	mark := r.logMark()
	if mark.Term == 0 {
		return raft.LogMark{}, false
	}
	switch m.Type {
	case raftpb.MsgProp, raftpb.MsgApp, raftpb.MsgStorageAppend, raftpb.MsgStorageApply:
		return mark, r.traceIfCovered(mark, m)
	case raftpb.MsgAppResp, raftpb.MsgStorageAppendResp, raftpb.MsgStorageApplyResp:
		if !r.traceIfPast(mark, m) {
			return raft.LogMark{}, false
		}
		if m.Type == raftpb.MsgStorageApplyResp {
			r.Unregister()
			return raft.LogMark{}, false
		}
		return mark, true
	}
	return raft.LogMark{}, false
}

func (r *RaftTracer) traceIfCovered(mark raft.LogMark, m raftpb.Message) bool {
	covered := false
	switch m.Type {
	case raftpb.MsgProp:
		covered = inRange(mark.Index, m.Entries)
	case raftpb.MsgApp:
		covered = coveredBy(mark, m.Term, m.Entries)
	case raftpb.MsgStorageAppend:
		covered = coveredBy(mark, m.LogTerm, m.Entries)
	case raftpb.MsgStorageApply:
		covered = inRange(mark.Index, m.Entries)
	}
	if !covered {
		return false
	}
	r.log.Tracef("[raft] message covering mark (t%d,i%d): %s",
		mark.Term, mark.Index,
		raft.DescribeMessage(m, func(bytes []byte) string { return "" }))
	return true
}

func (r *RaftTracer) traceIfPast(mark raft.LogMark, m raftpb.Message) bool {
	if m.Reject {
		return false
	}
	passed := false
	switch m.Type {
	case raftpb.MsgAppResp:
		passed = !mark.After(raft.LogMark{Term: m.Term, Index: m.Index})
	case raftpb.MsgStorageAppendResp:
		passed = !mark.After(raft.LogMark{Term: m.LogTerm, Index: m.Index})
	case raftpb.MsgStorageApplyResp:
		passed = len(m.Entries) != 0 && m.Entries[len(m.Entries)-1].Index >= mark.Index
	}
	if !passed {
		return false
	}
	r.log.Tracef("[raft] message past mark (t%d,i%d): %s",
		mark.Term, mark.Index,
		raft.DescribeMessage(m, func(bytes []byte) string { return "" }))
	return true
}

func coveredBy(mark raft.LogMark, term uint64, entries []raftpb.Entry) bool {
	return term == mark.Term && inRange(mark.Index, entries)
}

func inRange(index uint64, entries []raftpb.Entry) bool {
	return len(entries) != 0 &&
		entries[0].Index <= index && index <= entries[len(entries)-1].Index
}

func (r *RaftTracer) logMark() raft.LogMark {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mark
}
