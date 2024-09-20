// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rafttrace

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
)

type traceValue struct {
	// ctx is a trace specific context used to log events on this trace.
	ctx context.Context
	// baseCtx is the underlying proposal buffer which we additionally log to.
	baseCtx withContext
	// cleanup will retrieve all events that were logged in this trace.
	cleanup func() tracingpb.Recording
}

// RaftTracer is a utility to trace raft messages. Note that it may erroneously
// include incorrect messages since it does not consider the term, however it is
// easier to filter out extra messages that deal with missing messages.
type RaftTracer struct {
	// If this is too slow consider a bloom filter to see if this event is
	// possibly traced. This map should normally be very small.
	m syncutil.Map[uint64, traceValue]
}

func prettyTraceID(ctx context.Context) string {
	span := tracing.SpanFromContext(ctx)
	return fmt.Sprintf("[raft - %d.%d]", span.TraceID(), span.SpanID())
}

// We need a tracer that allows writing after the span is finished. This is
// because we have post-quorum messages that can still be written after the span
// is finished.
var tracer = tracing.NewTracerWithOpt(context.Background(), tracing.WithUseAfterFinishOpt(false, false))

func (r *RaftTracer) RegisterRemote(
	_ context.Context, index uint64, traceID uint64, remoteSpanID uint64,
) {
	ctx, cleanup := tracing.ContextWithRecordingSpan(context.Background(), tracer, "remote raft")
	log.VEventfDepth(ctx, 1, 3, "%s trace index %d from remote %d.%d", prettyTraceID(ctx), index, traceID, remoteSpanID)
	r.m.Store(index, &traceValue{ctx, nil, cleanup})
}

type withContext interface {
	Context() context.Context
}

func (r *RaftTracer) Register(baseCtx withContext, ents []raftpb.Entry) {
	// We store all the entries in the map. This is because we don't know which
	// one was traced. Typically this is called with only a single entry.
	for _, ent := range ents {
		ctx, cleanup := tracing.ContextWithRecordingSpan(context.Background(), tracer, "leader raft")
		// Log the identical entry to both to make it easier to match up later.
		log.VEventfDepth(ctx, 1, 3, "registering %d entries (i%d) for tracing with id %s", len(ents), ents[0].Index, prettyTraceID(ctx))
		log.VEventfDepth(baseCtx.Context(), 1, 3, "registering %d entries (i%d) for tracing with id %s", len(ents), ents[0].Index, prettyTraceID(ctx))
		r.m.Store(ent.Index, &traceValue{ctx, baseCtx, cleanup})
	}
}

func (r *RaftTracer) unregisterIndex(index uint64) {
	if val, found := r.m.LoadAndDelete(index); found {
		log.VEventfDepth(val.ctx, 5, 3, "unregistered log mark (i%d) from tracing", index)
		trace := val.cleanup().String()
		// Consider writing this to a different log if it becomes too noisy.
		log.InfofDepth(context.Background(), 5, "%s", trace)
	}
}

// IndexContext is a pair of the index and the context that was used to trace
// it.
type IndexContext struct {
	Index uint64
	Ctx   context.Context
}

// MaybeTrace will log the message if it is covered by a trace.
func (r *RaftTracer) MaybeTrace(m raftpb.Message) []IndexContext {
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
	r.m.Range(func(index uint64, val *traceValue) bool {
		log.VEventfDepth(val.ctx, 5, 3, "cleanup log mark (i%d) during close", index)
		trace := val.cleanup().String()
		// Consider writing this to a different log if it becomes too noisy.
		log.InfofDepth(context.Background(), 5, "%s", trace)
		return true
	})
}

func (r *RaftTracer) traceIfCovered(m raftpb.Message) []IndexContext {
	var savedCtxs []IndexContext
	r.m.Range(func(index uint64, val *traceValue) bool {
		switch m.Type {
		case raftpb.MsgProp, raftpb.MsgApp, raftpb.MsgStorageAppend, raftpb.MsgStorageApply:
			if inRange(index, m.Entries) {
				savedCtxs = append(savedCtxs, IndexContext{Index: index, Ctx: val.ctx})
				log.VEventfDepth(val.ctx, 4, 3, "%s->%s %v Term:%d Log:%d/%d", raft.DescribeTarget(m.From), raft.DescribeTarget(m.To), m.Type, m.Term, m.LogTerm, m.Index)
				if val.baseCtx != nil {
					log.VEventfDepth(val.baseCtx.Context(), 4, 3, "%s->%s %v Term:%d Log:%d/%d", raft.DescribeTarget(m.From), raft.DescribeTarget(m.To), m.Type, m.Term, m.LogTerm, m.Index)
				}
			}
		}
		return true
	})
	return savedCtxs
}

func (r *RaftTracer) traceIfPast(m raftpb.Message) {
	if m.Reject {
		return
	}
	r.m.Range(func(index uint64, val *traceValue) bool {
		shouldLog := false
		shouldUnregister := false
		switch m.Type {
		case raftpb.MsgAppResp, raftpb.MsgStorageAppendResp:
			shouldLog = m.Index >= index
		case raftpb.MsgStorageApplyResp:
			var msgIndex uint64
			if len(m.Entries) != 0 {
				index = m.Entries[len(m.Entries)-1].Index
			}
			shouldLog = index > msgIndex
			if shouldLog {
				shouldUnregister = true
			}
		}
		if shouldLog {
			log.VEventfDepth(val.ctx, 4, 3, "%s->%s %v Term:%d Log:%d/%d", raft.DescribeTarget(m.From), raft.DescribeTarget(m.To), m.Type, m.Term, m.LogTerm, m.Index)
			if val.baseCtx != nil {
				log.VEventfDepth(val.baseCtx.Context(), 4, 3, "%s->%s %v Term:%d Log:%d/%d", raft.DescribeTarget(m.From), raft.DescribeTarget(m.To), m.Type, m.Term, m.LogTerm, m.Index)
			}
		}
		if shouldUnregister {
			// We unregister the index here because we are now "done" with
			// this and don't expect more useful entries. There could still
			// be non-quorum messages, but they don't impact anything the
			// client cares about.
			r.unregisterIndex(index)
		}
		return true
	})
}

func inRange(index uint64, entries []raftpb.Entry) bool {
	return len(entries) != 0 &&
		entries[0].Index <= index && index <= entries[len(entries)-1].Index
}
