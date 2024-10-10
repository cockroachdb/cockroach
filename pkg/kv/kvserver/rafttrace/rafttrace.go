// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rafttrace

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/logtags"
)

type traceValue struct {
	traceID tracingpb.TraceID
	spanID  tracingpb.SpanID
	// ctx is a trace specific context used to log events on this trace.
	ctx context.Context
	// baseCtx is the underlying proposal buffer which we additionally log to.
	baseCtx withContext
	// these track whether range messages have already been logged to log at
	// most once.
	appRespSent, storageAppendSent atomic.Bool
}

// logf logs the message to the trace context and the base context. The base
// context is populated on the leaseholder and is attached to the SQL trace.
func (t *traceValue) logf(depth int, format string, args ...interface{}) {
	log.InfofDepth(t.ctx, depth+1, format, args...)
	if t.baseCtx != nil {
		if ctx := t.baseCtx.Context(); ctx != nil {
			log.VEventfDepth(ctx, depth+1, 3, format, args...)
		}
	}
}

func tracingID(traceID tracingpb.TraceID, spanID tracingpb.SpanID) string {
	// We attempt to balance uniqueness with readability by only keeping the
	// lower 16 bits of the trace and span.
	return fmt.Sprintf("%x.%x", uint16(traceID), uint16(spanID))
}

// RaftTracer is a utility to trace raft messages. Note that it may include
// additional messages since it does not consider the term, however it should
// not miss useful messages.
//
// An example higher latency case when this additional info might be useful:
// * We submit a proposal which gets appended as (term=10,index=100), but this
// was racing with a leader change.
// * The new leader at term=11 has overwritten this entry with
// (term=11,index=100). We will see this in the trace (there should be a bunch
// of MsgApp/MsgStorageAppend events overlapping this index again).
// * This causes the leaseholder to wait, and eventually realize that the
// proposal never popped out committed. It submits a reproposal, which now ends
// up at (term=11,index=110). We also should see that in the trace (the
// reproposal will inherit the same trace ID etc).
type RaftTracer struct {
	m             syncutil.Map[kvpb.RaftIndex, traceValue]
	numRegistered atomic.Int64
	ctx           context.Context
}

// NewRaftTracer creates a new RaftTracer with the given context.
func NewRaftTracer(ctx context.Context) *RaftTracer {
	return &RaftTracer{ctx: ctx}
}

// maybeRegister is a helper function to check if we should register a new
// trace. If there are too many registered traces it will return false.
func (r *RaftTracer) maybeRegister() bool {
	if r.numRegistered.Load() >= 10 {
		return false
	}
	r.numRegistered.Add(1)
	return true
}

// RegisterRemote is used to register a remote trace. This is used when we
// receive a raft message over the wire with a request to continue tracing it.
func (r *RaftTracer) RegisterRemote(e kvserverpb.TracedEntry) {
	if !r.maybeRegister() {
		return
	}
	// NB: We don't currently return remote traces, if we did, we would pass the
	// remote ctx here and trace it. The problem is knowing when to send it
	// back to the remote node.
	ctx := logtags.AddTag(r.ctx, "id", tracingID(e.TraceID, e.SpanID))
	t := traceValue{traceID: e.TraceID, spanID: e.SpanID, ctx: ctx, baseCtx: nil}
	t.logf(1, "start trace for index (i%d) from remote", e.Index)
	r.m.Store(e.Index, &t)
}

// withContext allows us to get the context from the object.
type withContext interface {
	Context() context.Context
}

// Register is called on an entry that is about to be proposed. This will begin
// logging all subsequent updates to this entry.
func (r *RaftTracer) Register(baseCtx withContext, ent raftpb.Entry) {
	if !r.maybeRegister() {
		return
	}
	span := tracing.SpanFromContext(baseCtx.Context())
	ctx := logtags.AddTag(r.ctx, "id", tracingID(span.TraceID(), span.SpanID()))
	// NB: We grab the trace and span id now because the baseCtx.Context may be
	// nil'ed later.
	t := traceValue{traceID: span.TraceID(), spanID: span.SpanID(), ctx: ctx, baseCtx: baseCtx}

	// Log the identical entry to both to make it easier to match up later.
	t.logf(1, "registering entry (i%d) with tracing id (%s)", ent.Index, tracingID(t.traceID, t.spanID))
	r.m.Store(kvpb.RaftIndex(ent.Index), &t)
}

// MaybeTrace will log the message if it is covered by a trace.
func (r *RaftTracer) MaybeTrace(m raftpb.Message) []kvserverpb.TracedEntry {
	// NB: This check is an optimization to handle the common case where there
	// are no registered traces. numRegistered is not a strict count of the
	// number of traces, however it is incremented before we store a trace in
	// the map and decremented before we remove it.
	if r.numRegistered.Load() == 0 {
		return nil
	}

	switch m.Type {
	case raftpb.MsgProp, raftpb.MsgApp, raftpb.MsgStorageAppend, raftpb.MsgStorageApply:
		return r.traceIfCovered(m)
	case raftpb.MsgAppResp, raftpb.MsgStorageAppendResp, raftpb.MsgStorageApplyResp:
		r.traceIfPast(m)
		return nil
	}
	return nil
}

// Close will unregister all the traces and log them. It should be called if the
// replica is destroyed as it is possible some traces were never closed.
func (r *RaftTracer) Close() {
	r.m.Range(func(index kvpb.RaftIndex, t *traceValue) bool {
		r.numRegistered.Add(-1)
		r.m.Delete(index)
		t.logf(2, "cleanup log index (i%d) during close", index)
		return true
	})
}

// traceIfCovered will log the message if it touches any of the registered trace
// points. Additionally it returns any saved contexts by index for sending to
// remote nodes. This typically applies to messages that the leader sends to the
// followers.
func (r *RaftTracer) traceIfCovered(m raftpb.Message) []kvserverpb.TracedEntry {
	var tracedEntries []kvserverpb.TracedEntry
	r.m.Range(func(index kvpb.RaftIndex, t *traceValue) bool {
		if inRange(index, m.Entries) {
			tracedEntries = append(tracedEntries,
				kvserverpb.TracedEntry{
					Index:   index,
					TraceID: t.traceID,
					SpanID:  t.spanID,
				},
			)

			// TODO(baptist): Not all the fields are relevant to log for all
			// message types. Consider cleaning up what is logged.
			t.logf(4,
				"%s->%s %v Term:%d Log:%d/%d",
				raft.DescribeTarget(m.From),
				raft.DescribeTarget(m.To),
				m.Type,
				m.Term,
				m.LogTerm,
				m.Index,
			)
		}
		return true
	})
	return tracedEntries
}

// inRange returns true if the index is within the range of the entries. It
// assumes the entries are sorted by index. Specifically it returns if the index
// is in the set [first_index,last_index].
func inRange(index kvpb.RaftIndex, entries []raftpb.Entry) bool {
	return len(entries) != 0 &&
		index >= kvpb.RaftIndex(entries[0].Index) &&
		index <= kvpb.RaftIndex(entries[len(entries)-1].Index)
}

// traceIfPast will log the message the message is past any registered tracing
// points. It will additionally unregister traces that are no longer useful.
// This typically applies to messages that followers send back to a leader.
func (r *RaftTracer) traceIfPast(m raftpb.Message) {
	if m.Reject {
		return
	}
	r.m.Range(func(index kvpb.RaftIndex, t *traceValue) bool {
		shouldLog := false
		shouldUnregister := false
		switch m.Type {
		case raftpb.MsgAppResp:
			shouldLog = kvpb.RaftIndex(m.Index) >= index && t.appRespSent.CompareAndSwap(false, true)
		case raftpb.MsgStorageAppendResp:
			shouldLog = kvpb.RaftIndex(m.Index) >= index && t.storageAppendSent.CompareAndSwap(false, true)
		case raftpb.MsgStorageApplyResp:
			if len(m.Entries) == 0 {
				return true
			}
			msgIndex := m.Entries[len(m.Entries)-1].Index
			shouldLog = kvpb.RaftIndex(msgIndex) >= index
			// We unregister the index here because we are now "done" with
			// this and don't expect more useful events. There could still
			// be non-quorum messages, but they don't impact anything the
			// client cares about.
			shouldUnregister = shouldLog
		}
		if shouldLog {
			t.logf(4,
				"%s->%s %v Term:%d Log:%d/%d",
				raft.DescribeTarget(m.From),
				raft.DescribeTarget(m.To),
				m.Type,
				m.Term,
				m.LogTerm,
				m.Index,
			)
		}
		if shouldUnregister {
			r.m.Delete(index)
			r.numRegistered.Add(-1)
			t.logf(4, "unregistered log index (i%d) from tracing", index)
		}
		return true
	})
}
