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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/logtags"
)

// traceValue represents the trace information for a single registration.
type traceValue struct {
	traceID tracingpb.TraceID
	spanID  tracingpb.SpanID
	// ctx is a trace specific context used to log events on this trace.
	ctx context.Context
	// baseCtx is the underlying proposal buffer which we additionally log to.
	baseCtx withContext
	// appRespSent and storageAppendRespSent track whether range messages have
	// already been logged to log at most once. This limits the log from growing
	// too large at a small risk of missing some messages in the case of dropped
	// messages or reproposals.
	// TODO(baptist): If we find we are missing important logs, consider adding
	// a mode to disable these checks and allowing all messages to be logged.
	appRespSent, storageAppendRespSent atomic.Bool
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

// tracingID attempts to balance uniqueness with readability by only keeping the
// lower 16 bits of the trace and span.
func tracingID(traceID tracingpb.TraceID, spanID tracingpb.SpanID) string {
	return fmt.Sprintf("%x.%x", uint16(traceID), uint16(spanID))
}

// RaftTracer is a utility to trace the lifetime of raft log entries. It may
// include some unrelated entries, since it does not consider the term. It
// traces at most one MsgAppResp and MsgStorageAppendResp per index which is the
// first one that is past our index entry. This limitation means it may not
// capture all the relevant messages particularly if the term changes.
//
// The library will log in two different ways once to the standard cockroach log
// and once to the SQL trace on the leader.
// TODO(baptist): Look at logging traces on followers as well and sending back
// to the leader in the MsgAppResp. It would need to be best effort, but might
// still be useful.
type RaftTracer struct {
	// m is a map of all the currently traced entries for this replica. The
	// aggregate size of the map is equal to or less than numRegistered. We add
	// to numRegistered before we update m, and delete from m before we remove
	// from numRegistered to keep this invariant.
	m syncutil.Map[kvpb.RaftIndex, traceValue]
	// numRegistered is the number of currently registered traces for this
	// store, not this replica. The number of registered will always be less
	// than the MaxConcurrentRaftTraces setting. If the setting is lowered, we
	// flush all traces on all replicas.
	numRegistered *atomic.Int64
	// This is the ambient context for the replica and is used for remote
	// traces. It contains the replica/range information. On each trace we
	// additionally append the unique trace/span IDs.
	ctx context.Context
	st  *cluster.Settings
}

// NewRaftTracer creates a new RaftTracer with the given ambient context for the
// replica.
func NewRaftTracer(
	ctx context.Context, st *cluster.Settings, numRegistered *atomic.Int64,
) *RaftTracer {
	return &RaftTracer{ctx: ctx, st: st, numRegistered: numRegistered}
}

// MaxConcurrentRaftTraces is the maximum number of entries that can be traced
// at any time on this store. Additional traces will be ignored until the number
// of traces drops below the limit. Having too many active traces can negatively
// impact performance as we iterate over all of them for some messages. 10 is a
// reasonable default that balances usefulness with performance impact. It isn't
// expected that this limit will normally be hit.
var MaxConcurrentRaftTraces = settings.RegisterIntSetting(
	settings.SystemVisible,
	"kv.raft.max_concurrent_traces",
	"the maximum number of tracked raft traces",
	0,
	settings.NonNegativeInt,
)

// maybeRegister checks if should register a new trace. If there are too many
// registered traces it will not register and return false. The invariant is
// that numRegistered <= numAllowed.  This method will return true if we can
// keep the invariant and added one to the number registered, otherwise it will
// return false.
func (r *RaftTracer) maybeRegister() bool {
	numAllowed := MaxConcurrentRaftTraces.Get(&r.st.SV)
	numRegistered := r.numRegistered.Load()

	// The maximum number of traces has been reached. We don't register this
	// trace and return false.
	if numRegistered == numAllowed {
		return false
	}

	// This can only happen if numAllowed has changed. If this happens flush all
	// our current traces and don't register this request.
	for numRegistered > numAllowed {
		r.FlushAll()
		return false
	}

	// Only increment the number of registered traces if the numRegistered
	// hasn't changed. In the case of an ABA update, it does not break the
	// invariant since some other trace was registered and deregistered, but
	// there is still a slot available.
	return r.numRegistered.CompareAndSwap(numRegistered, numRegistered+1)
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
	t.logf(1, "start trace for index %d from remote", e.Index)
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
	t.logf(1, "registering entry index %d with tracing id (%s)", ent.Index, tracingID(t.traceID, t.spanID))
	r.m.Store(kvpb.RaftIndex(ent.Index), &t)
}

// MaybeTrace will log the message if it is covered by a trace.
func (r *RaftTracer) MaybeTrace(m raftpb.Message) []kvserverpb.TracedEntry {
	// NB: This check is an optimization to handle the common case where there
	// are no registered traces on the store.
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

// FlushAll will unregister all the currently active traces. It is safe to call
// multiple times, but should always be called when the replica is destroyed.
func (r *RaftTracer) FlushAll() {
	r.m.Range(func(index kvpb.RaftIndex, t *traceValue) bool {
		r.m.Delete(index)
		r.numRegistered.Add(-1)
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
		if !inRange(index, m.Entries) {
			return true
		}
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
// This call is for events that move the needle/watermark forward (e.g. the log
// storage syncs), but don't have an exact range of entries affected. So, being
// unable to match these events to entries exactly once, we instead check that
// the watermark passed the entry. To protect against overly verbose logging, we
// only allow MsgAppResp and MsgStorageAppendResp to be logged once per trace.
func (r *RaftTracer) traceIfPast(m raftpb.Message) {
	if m.Reject {
		return
	}
	r.m.Range(func(index kvpb.RaftIndex, t *traceValue) bool {
		switch m.Type {
		case raftpb.MsgAppResp:
			if kvpb.RaftIndex(m.Index) >= index && t.appRespSent.CompareAndSwap(false, true) {
				t.logf(4,
					"%s->%s %v Term:%d Index:%d",
					raft.DescribeTarget(m.From),
					raft.DescribeTarget(m.To),
					m.Type,
					m.Term,
					m.Index,
				)
			}
		case raftpb.MsgStorageAppendResp:
			if kvpb.RaftIndex(m.Index) >= index && t.storageAppendRespSent.CompareAndSwap(false, true) {
				t.logf(4,
					"%s->%s %v Log:%d/%d",
					raft.DescribeTarget(m.From),
					raft.DescribeTarget(m.To),
					m.Type,
					m.LogTerm,
					m.Index,
				)
			}
		case raftpb.MsgStorageApplyResp:
			if len(m.Entries) == 0 {
				return true
			}
			// Use the last entry to determine if we should log this message.
			msgIndex := m.Entries[len(m.Entries)-1].Index
			if kvpb.RaftIndex(msgIndex) >= index {
				t.logf(4,
					"%s->%s %v Term:%d Index:%d",
					raft.DescribeTarget(m.From),
					raft.DescribeTarget(m.To),
					m.Type,
					m.Entries[len(m.Entries)-1].Term,
					msgIndex,
				)
				// We unregister the index here because we are now "done" with
				// this and don't expect more useful events. There could still
				// be non-quorum messages, but they don't impact anything the
				// client cares about.
				r.m.Delete(index)
				r.numRegistered.Add(-1)
				t.logf(4, "unregistered log index %d from tracing", index)
			}
		}
		return true
	})
}
