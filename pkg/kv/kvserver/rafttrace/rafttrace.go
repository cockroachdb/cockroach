// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rafttrace

import (
	"context"
	"math"
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
	"github.com/cockroachdb/redact"
)

// MaxConcurrentRaftTraces is the maximum number of entries that can be traced
// at any time on this store. Additional traces will be ignored until the number
// of traces drops below the limit. Having too many active traces can negatively
// impact performance as we iterate over all of them for some messages.
//
// TODO(baptist): Bump the default to a reasonable value like 10 that balances
// usefulness with performance impact once we have validated the performance
// impact.
var MaxConcurrentRaftTraces = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.raft.max_concurrent_traces",
	"the maximum number of tracked raft traces, 0 will disable tracing",
	0,
	settings.IntInRange(0, 1000),
)

// traceValue represents the trace information for a single registration.
type traceValue struct {
	traced kvserverpb.TracedEntry
	// ctx is a trace specific context used to log events on this trace.
	ctx context.Context

	mu struct {
		syncutil.Mutex

		// seenMsgAppResp tracks whether a MsgAppResp message has already been
		// logged by each replica peer. This limits the size of the log at a
		// small risk of missing some important messages in the case of dropped
		// messages or reproposals.
		seenMsgAppResp map[raftpb.PeerID]bool

		// seenMsgStorageAppendResp tracks whether a MsgStorageAppendResp
		// message has already been logged.
		seenMsgStorageAppendResp bool

		// propCtx is the underlying proposal context used for tracing to the
		// SQL trace.
		propCtx context.Context

		// propSpan is the span connected to the propCtx. It must be finished
		// when the trace is removed.
		propSpan *tracing.Span
	}
}

// logf logs the message to the trace context and the proposal context. The
// proposal context is populated on the leaseholder and is attached to the SQL
// trace.
func (t *traceValue) logf(depth int, format string, args ...interface{}) {
	log.InfofDepth(t.ctx, depth+1, format, args...)

	t.mu.Lock()
	propCtx := t.mu.propCtx
	t.mu.Unlock()
	if propCtx != nil {
		log.VEventfDepth(propCtx, depth+1, 3, format, args...)
	}
}

// seenMsgAppResp returns true if it hasn't seen an MsgAppResp for this peer.
func (t *traceValue) seenMsgAppResp(p raftpb.PeerID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mu.seenMsgAppResp[p] {
		return true
	}
	t.mu.seenMsgAppResp[p] = true
	return false
}

// seenMsgStorageAppendResp returns true if it hasn't seen a
// MsgStorageAppendResp for this trace.
func (t *traceValue) seenMsgStorageAppendResp() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mu.seenMsgStorageAppendResp {
		return true
	}
	t.mu.seenMsgStorageAppendResp = true
	return false
}

// String attempts to balance uniqueness with readability by only keeping the
// lower 16 bits of the trace and span.
func (tv *traceValue) String() string {
	return redact.StringWithoutMarkers(tv)
}

func (tv *traceValue) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("i%d/%x.%x", tv.traced.Index, uint16(tv.traced.TraceID), uint16(tv.traced.SpanID))
}

// RaftTracer is a utility to trace the lifetime of raft log entries. It may log
// some unrelated entries, since it does not consider entry or leader term. It
// traces at most one MsgAppResp and MsgStorageAppendResp per index which is the
// first one that is past our index entry. This limitation means it may not
// capture all the relevant messages particularly if the term changes.
//
// The library will log in two different ways once to the standard cockroach log
// and once to the SQL trace on the leaseholder.
// TODO(baptist): Look at logging traces on followers and sending back to the
// leader. It would need to be best effort, but might still be useful.
// Alternatively, double-down on distributed trace collection if/when it's
// supported. So that the trace does not need to be plumbed back to the
// leaseholder / txn coordinator.
type RaftTracer struct {
	// m is a map of all the currently traced entries for this replica. The
	// aggregate size of the map across all replicas is equal to or less than
	// numRegisteredStore unless the setting changes in which case we flush all
	// entries on the next register call. We add to numRegistered before we
	// update m, and delete from m before we remove from numRegistered to keep
	// this invariant. On a setting change we flush all existing traces on the
	// next call to register.
	// TODO(baptist): Look at alternatives to using a map such as a sparse array
	// or circular buffer. Specifically, we might be able to save some memory
	// allocations. Note that the propCtx in the traceValue is already pulled
	// from a pool inside the tracer.
	m syncutil.Map[kvpb.RaftIndex, traceValue]

	// numRegisteredStore is the number of currently registered traces for this
	// store, not this replica. The number of registered will normally be less
	// than the MaxConcurrentRaftTraces setting. If the setting is lowered, we
	// flush all traces on all replicas.
	numRegisteredStore *atomic.Int64

	// numRegisteredReplica is the number of currently registered traces for
	// this replica. The sum(numRegisteredReplica) <= numRegisteredStore. We set
	// numRegisteredReplica to MaxInt32 when we close the tracer to prevent new
	// registrations.
	//
	// TODO(baptist/pav-kv): Look at optimizing to avoid the need for this to be
	// an atomic. It likely doesn't need to be atomic since the callers should
	// be holding Replica.raftMu and/or Replica.mu.
	numRegisteredReplica atomic.Int64

	// ctx is the ambient context for the replica and is used for remote
	// traces. It contains the replica/range information. On each trace we
	// additionally append the unique trace/span IDs.
	ctx context.Context
	st  *cluster.Settings

	tracer *tracing.Tracer
}

// NewRaftTracer creates a new RaftTracer with the given ambient context for the
// replica.
func NewRaftTracer(
	ctx context.Context,
	tracer *tracing.Tracer,
	st *cluster.Settings,
	numRegisteredStore *atomic.Int64,
) *RaftTracer {
	return &RaftTracer{ctx: ctx, tracer: tracer, st: st, numRegisteredStore: numRegisteredStore}
}

// reserveSpace checks if should register a new trace. If there are too many
// registered traces it will not register and return false. The soft invariant
// is that numRegisteredStore <= numAllowed which can be temporarily violated if
// MaxConcurrentRaftTraces is lowered. This method will return true if we can
// add one to the number registered for both the store and replica, otherwise it
// will return false. This method is optimized for the `numAllowed == 0` case
// and avoids loading `numRegisteredStore` until after this check.`
func (r *RaftTracer) reserveSpace() bool {
	numAllowed := MaxConcurrentRaftTraces.Get(&r.st.SV)
	numRegisteredReplica := r.numRegisteredReplica.Load()

	// This can only occur if the numAllowed setting has changed since a
	// previous call to reserveSpace. If this happens flush all our current
	// traces and don't register this request. Note that when this happens we
	// also wont't log this request.
	if numRegisteredReplica > numAllowed {
		log.Infof(r.ctx, "flushing all traces due to setting change")
		r.m.Range(func(index kvpb.RaftIndex, t *traceValue) bool {
			r.removeEntry(index)
			return true
		})
		return false
	}

	if numAllowed == 0 {
		return false
	}

	// The maximum number of traces has been reached for the store. We don't
	// register tracing and return false.
	numRegisteredStore := r.numRegisteredStore.Load()
	if numRegisteredStore >= numAllowed {
		return false
	}

	// Only increment the number of registered traces if the numRegistered
	// hasn't changed. In the case of an ABA update, it does not break the
	// invariant since some other trace was registered and deregistered, but
	// there is still a slot available. We will not register this trace if
	// someone else is concurrently registering a trace on this store, but this
	// is acceptable as it is a rare case.
	registerSucceeded := r.numRegisteredStore.CompareAndSwap(numRegisteredStore, numRegisteredStore+1)
	if registerSucceeded {
		// Add one unconditionally to the replica count.
		r.numRegisteredReplica.Add(1)
	}
	// Note we can't assert numRegisteredStore <= numAllowed because if the
	// setting is changed it can be temporarily violated on other replicas.
	return registerSucceeded
}

// tryStore attempts to store this value. If the index is already in the map it
// will not store this entry and return false. It will also decrement counters
// that were incremented by reserveSpace.
// This is a rare case where we already have the index in the map. We
// don't want to store this entry, but also need to decrement the
// counter to avoid double tracing.
func (r *RaftTracer) tryStore(tv *traceValue) (*traceValue, bool) {
	if existingTv, loaded := r.m.LoadOrStore(tv.traced.Index, tv); loaded {
		tv.logf(2, "duplicate registration ignored - existing trace: %s", existingTv)
		existingTv.logf(2, "additional registration for same index: %s", tv)
		r.destroy(tv)
		return existingTv, false
	}
	return tv, true
}

// newTraceValue creates a new traceValue for the given traced entry. Note that
// it doesn't pass `propCtx` as the first parameter since this isn't the
// relevant context that should be used for logging and it can be nil.
func (r *RaftTracer) newTraceValue(
	te kvserverpb.TracedEntry, propCtx context.Context, propSpan *tracing.Span,
) *traceValue {
	tv := &traceValue{traced: te}
	tv.ctx = logtags.AddTag(r.ctx, "id", redact.Safe(tv.String()))
	tv.mu.seenMsgAppResp = make(map[raftpb.PeerID]bool)
	tv.mu.propCtx = propCtx
	tv.mu.propSpan = propSpan
	return tv
}

// RegisterRemote registers a remote trace. This is called when we receive a
// raft message over the wire with a request to continue tracing it.
func (r *RaftTracer) RegisterRemote(te kvserverpb.TracedEntry) {
	if !r.reserveSpace() {
		return
	}
	// NB: We don't currently return remote traces, if we did, we would pass the
	// remote ctx here and trace it.
	if tv, created := r.tryStore(r.newTraceValue(te, nil, nil)); created {
		tv.logf(1, "registering remote trace %s", tv)
	}
}

// MaybeRegister is called on an entry that has been proposed to raft. This will
// begin logging all subsequent updates to this entry. It returns true if the
// registration is successful. A duplicate registration of the same index is
// considered a success and returns true, however the older registration is kept
// and this registration is ignored.
func (r *RaftTracer) MaybeRegister(ctx context.Context, ent raftpb.Entry) bool {
	// If the index is nil, then we can't trace this entry. This can happen if
	// there is a leader/leaseholder spilt. We don't have an easy way to handle
	// this today, so don't attempt to trace it.
	if ent.Index == 0 {
		log.VEvent(ctx, 2, "skip registering raft proposal without index")
		return false
	}

	// Only register the entry if this is a traced context with verbose logging.
	span := tracing.SpanFromContext(ctx)
	if span == nil || span.RecordingType() != tracingpb.RecordingVerbose {
		return false
	}

	// This must be the last conditional. If this returns true we must call
	// storeEntryWithTracing to not leak a registered permit.
	if !r.reserveSpace() {
		log.VEvent(ctx, 2, "too many active raft traces, skipping")
		return false
	}

	ctx, span = r.tracer.StartSpanCtx(ctx, "raft trace",
		tracing.WithParent(span), tracing.WithFollowsFrom())
	if tv, created := r.tryStore(r.newTraceValue(kvserverpb.TracedEntry{
		Index:   kvpb.RaftIndex(ent.Index),
		TraceID: span.TraceID(),
		SpanID:  span.SpanID(),
	}, ctx, span)); created {
		tv.logf(1, "registering local trace %s", tv)
	}
	return true
}

// MaybeTrace logs the message in every trace it is relevant to.
func (r *RaftTracer) MaybeTrace(m raftpb.Message) []kvserverpb.TracedEntry {
	// NB: This check is an optimization to handle the common case where there
	// are no registered traces on this replica.
	if r.numRegisteredReplica.Load() == 0 {
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

// removeEntry removes the trace at the given index and decrements the
// registered counters at the replica and store level.
func (r *RaftTracer) removeEntry(index kvpb.RaftIndex) {
	tv, found := r.m.LoadAndDelete(index)
	if !found {
		return
	}
	// Don't allow additional tracing to this context.
	r.destroy(tv)
}

func (r *RaftTracer) destroy(tv *traceValue) {
	r.numRegisteredReplica.Add(-1)
	r.numRegisteredStore.Add(-1)

	tv.mu.Lock()
	defer tv.mu.Unlock()
	if tv.mu.propSpan != nil {
		tv.mu.propSpan.Finish()
		tv.mu.propCtx = nil
		tv.mu.propSpan = nil
	}
}

// Close will unregister all the currently active traces and prevent additional
// traces from being added. It is safe to call multiple times, but should always
// be called at least once when the replica is destroyed to prevent leaking
// traces.
// Note that there could be a race between another caller calling Register and
// us closing the tracer, however we won't allow any new registrations to come
// through after this call. Note that we set this to MaxInt32 instead of
// MaxInt64 to avoid a rare race where another thread is in the middle of
// `reserveSpace` and calls `Add(1)` which cause overflow.
func (r *RaftTracer) Close() {
	r.numRegisteredReplica.Store(math.MaxInt32)

	r.m.Range(func(index kvpb.RaftIndex, t *traceValue) bool {
		t.logf(2, "cleanup log index %d during Close", index)
		r.removeEntry(index)
		return true
	})
}

func peer(p raftpb.PeerID) redact.SafeString {
	return redact.SafeString(raft.DescribeTarget(p))
}

// traceIfCovered will log the message if it touches any of the registered trace
// points. Additionally it returns any saved trace/span IDs for sending to
// remote nodes. This applies both to messages that the leader sends to
// followers, and messages replicas send to their local storage.
func (r *RaftTracer) traceIfCovered(m raftpb.Message) []kvserverpb.TracedEntry {
	if len(m.Entries) == 0 {
		return nil
	}
	minEntryIndex := kvpb.RaftIndex(m.Entries[0].Index)
	maxEntryIndex := kvpb.RaftIndex(m.Entries[len(m.Entries)-1].Index)
	var tracedEntries []kvserverpb.TracedEntry
	r.m.Range(func(index kvpb.RaftIndex, t *traceValue) bool {
		// If the traced index is not in the range of the entries, we can skip
		// it. We don't need to check each individual entry since they are
		// contiguous.
		if t.traced.Index < minEntryIndex || t.traced.Index > maxEntryIndex {
			return true
		}
		tracedEntries = append(tracedEntries, t.traced)
		// TODO(baptist): Not all the fields are relevant to log for all
		// message types. Consider cleaning up what is logged.
		t.logf(4,
			"%s->%s %v Term:%d Log:%d/%d Entries:[%d-%d]",
			peer(m.From),
			peer(m.To),
			m.Type,
			m.Term,
			m.LogTerm,
			m.Index,
			minEntryIndex,
			maxEntryIndex,
		)
		return true
	})
	return tracedEntries
}

// traceIfPast will log the message to all registered traceValues the message is
// past. It will additionally unregister traces that are no longer useful. This
// call is for events that move the needle/watermark forward (e.g. the log
// storage syncs), but don't have an exact range of entries affected. So, being
// unable to match these events to entries exactly once, we instead check that
// the watermark passed the entry. To protect against overly verbose logging, we
// only allow MsgAppResp to be logged once per peer, and only one
// MsgStorageAppendResp. When we receive a MsgStorageApplyResp we will log and
// unregister the tracing.
func (r *RaftTracer) traceIfPast(m raftpb.Message) {
	if m.Reject {
		return
	}
	r.m.Range(func(index kvpb.RaftIndex, t *traceValue) bool {
		switch m.Type {
		case raftpb.MsgAppResp:
			if kvpb.RaftIndex(m.Index) >= index && !t.seenMsgAppResp(m.From) {
				t.logf(4,
					"%s->%s %v Term:%d Index:%d",
					peer(m.From),
					peer(m.To),
					m.Type,
					m.Term,
					m.Index,
				)
			}
		case raftpb.MsgStorageAppendResp:
			if kvpb.RaftIndex(m.Index) >= index && !t.seenMsgStorageAppendResp() {
				t.logf(4,
					"%s->%s %v Log:%d/%d",
					peer(m.From),
					peer(m.To),
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
					"%s->%s %v LastEntry:%d/%d",
					peer(m.From),
					peer(m.To),
					m.Type,
					m.Entries[len(m.Entries)-1].Term,
					m.Entries[len(m.Entries)-1].Index,
				)
				// We unregister the index here because we are now "done" with
				// this entry and don't expect more useful events.
				t.logf(4, "unregistered log index %d from tracing", index)
				r.removeEntry(index)
			}
		}
		return true
	})
}
