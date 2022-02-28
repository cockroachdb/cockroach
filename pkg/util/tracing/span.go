// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracing

import (
	"fmt"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/logtags"
	"github.com/petermattis/goid"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
	"golang.org/x/net/trace"
)

const (
	// TagPrefix is prefixed to all tags that should be output in SHOW TRACE.
	TagPrefix = "cockroach."
)

// Span is the tracing Span that we use in CockroachDB. Depending on the tracing
// configuration, it can hold anywhere between zero and three destinations for
// trace information:
//
// 1. external OpenTelemetry-compatible trace collector (Jaeger, Zipkin, Lightstep),
// 2. /debug/requests endpoint (net/trace package); mostly useful for local debugging
// 3. CRDB-internal trace span (powers SQL session tracing).
//
// When there is no need to allocate either of these three destinations,
// a "noop span", i.e. an immutable *Span wrapping the *Tracer, may be
// returned, to allow starting additional nontrivial Spans from the return
// value later, when direct access to the tracer may no longer be available.
//
// The CockroachDB-internal Span (crdbSpan) is more complex because
// rather than reporting to some external sink, the caller's "owner"
// must propagate the trace data back across process boundaries towards
// the root of the trace span tree; see WithParent
// and WithRemoteParentFromSpanMeta, respectively.
//
// Additionally, the internal span type also supports turning on, stopping,
// and restarting its data collection (see Span.StartRecording), and this is
// used extensively in SQL session tracing.
//
// Span is a fairly thin wrapper around spanInner, dealing with guarding against
// use-after-Finish and reference counting for pooling and re-allocating.
type Span struct {
	// helper is the allocation helper that this span is part of. Used to release
	// back into a pool on Finish().
	helper *spanAllocHelper

	i spanInner

	// finished is set on Finish(). Used to detect use-after-Finish.
	finished int32 // atomic

	// refCnt counts the references to this span held by other spans. The Span can
	// only be re-allocated when refCnt reaches zero.
	//
	// The reference counting mechanism is used only internally by the tracing
	// infrastructure. Clients using Spans do not track their references because
	// it'd be too burdensome to require all the references to have a clearly
	// defined life cycle, and to be explicitly released when they're no longer in
	// use. For clients, the contract is that a span can be used until Finish().
	//
	// For convenience, the span itself holds a reference to itself until
	// Finish(). This ensures that the reference count will not become zero before
	// Finish().
	//
	// The reference count serves a couple of purposes:
	// 1) Prevent re-allocation of a Span while child spans are still operating on
	// it. In particular, this ensures that races between Finish()ing a parent and
	// a child cannot result in the child operating on a re-allocated parent.
	// Because of the span's lock ordering convention, a child cannot hold its
	// lock while operating on the parent. During Finish(), the child drops its
	// lock and informs the parent that it is Finish()ing. If the parent
	// Finish()es at the same time, that call could erroneously conclude that the
	// parent can be made available for re-use, even through the child goroutine
	// has a pending call into the parent.
	//
	// 2) Prevent re-allocation of child spans while a Finish()ing parent is in
	// the process of transforming the children into roots and inserting them into
	// the active spans registry. Operating on the registry is done without
	// holding any span's lock, so a race with a child's Finish() could result in
	// the registry operating on a re-allocated span.
	//
	// 3) Prevent re-allocation of a Span in between the time that WithParent(s)
	// captures a reference to s and the time when the parent option is used to
	// create the child. Such an inopportune reuse of a span could only happen is
	// the span is Finish()ed concurrently with the creation of its child, which
	// is illegal. Still, we optionally tolerate use-after-Finishes, and this use
	// cannot be tolerated with the reference count protection. Without this
	// protection, the tree of spans in a trace could degenerate into a graph
	// through the introduction of loops. A loop could lead to deadlock due to the
	// fact that we lock multiple spans at once. The lock ordering convention is
	// that the parent needs to be locked before the child, which ensures
	// deadlock-freedom in the absence of loops.
	// For example:
	// 1. parent := tr.StartSpan()
	// 2. parentOpt := WithParent(parent)
	// 3. parent.Finish()
	// 4. child := tr.StartSpan(parentOpt)
	// If "parent" would be re-allocated as "child", then child would have itself
	// as a parent. The use of parentOpt in step 4) after parent was finished in
	// step 3) is a use-after-Finish of parent; it is illegal and, if detection is
	// enabled, it might be detected as such. However, if span pooling and re-use
	// is enabled, then the detection is not realiable (it does not catch cases
	// where a span is re-used before a reference to it taken before the prior
	// Finish() is used).
	// A span having itself as a parent is just the trivial case of the problem;
	// loops of arbitrary length are also possible. For example, for a loop of
	// length 2:
	// 1. Say we have span A as a parent with span B as a child (A -> B).
	// 2. parentA := WithParent(A)
	// 3. parentB := WithParent(B)
	// 4. A.Finish(); B.Finish();
	// 5. X := tr.StartSpan(parentA); Y := tr.StartSpan(parentB);
	// If B is re-used as X, and A is re-used as Y, we get the following graph:
	//  B<-┐
	//  |  |
	//  └->A
	//
	// We avoid these hazards by having WithParent(s) increment s' refCnt. Spans
	// are not re-used while the creation of a child is pending. Spans can be
	// Finish()ed while the creation of the child is pending, in which case the
	// creation of the child will reliably detect the use-after-Finish (and turn
	// it into a no-op if configured to tolerate such illegal uses).
	refCnt int32 // atomic

	// finishStack is set if debugUseAfterFinish is set. It represents the stack
	// that called Finish(), in order to report it on further use.
	finishStack string
}

// IsNoop returns true if this span is a black hole - it doesn't correspond to a
// CRDB span and it doesn't output either to an OpenTelemetry tracer, or to
// net.Trace.
//
// As opposed to other spans, a noop span can be used after Finish(). In
// practice, noop spans are pre-allocated by the Tracer and handed out to
// everybody (shared) if the Tracer is configured to not always create real
// spans (i.e. TracingModeOnDemand).
func (sp *Span) IsNoop() bool {
	return sp.i.isNoop()
}

// detectUseAfterFinish() checks whether sp has already been Finish()ed. If it
// did, the behavior depends on the Tracer's configuration: if it was configured
// to tolerate use-after-Finish, detectUseAfterFinish returns true. If it has
// been configured to not tolerate use-after-Finish, it crashes.
//
// Exported methods on Span are supposed to call this and short-circuit if true
// is returrned.
//
// Note that a nil or no-op span will return true.
func (sp *Span) detectUseAfterFinish() bool {
	if sp == nil {
		return true
	}
	if sp.IsNoop() {
		return true
	}
	alreadyFinished := atomic.LoadInt32(&sp.finished) != 0
	// In test builds, we panic on span use after Finish. This is in preparation
	// of span pooling, at which point use-after-Finish would become corruption.
	if alreadyFinished && sp.i.tracer.PanicOnUseAfterFinish() {
		var finishStack string
		if sp.finishStack == "" {
			finishStack = "<stack not captured. Set debugUseAfterFinish>"
		} else {
			finishStack = sp.finishStack
		}
		panic(fmt.Sprintf("use of Span after Finish. Span: %s. Finish previously called at: %s",
			sp.i.OperationName(), finishStack))
	}

	return alreadyFinished
}

// incRef increments sp's reference count. The span will not be re-allocated
// before a corresponding decRef() is performed (i.e. before the count reaches
// zero).
//
// Most code should not call incRef() directly, but instead use makeSpanRef().
func (sp *Span) incRef() {
	atomic.AddInt32(&sp.refCnt, 1)
}

// decRef decrements the span's reference count. If it reaches zero, the span is
// made available for reuse if the Tracer was so configured.
//
// Returns true if the value dropped to zero.
func (sp *Span) decRef() bool {
	refCnt := atomic.AddInt32(&sp.refCnt, -1)
	if refCnt < 0 {
		panic(fmt.Sprintf("refCnt below 0: %s", sp))
	}
	// If this was the last reference, make the span available for re-use.
	if refCnt == 0 {
		alreadyFinished := atomic.LoadInt32(&sp.finished) != 0
		if !alreadyFinished {
			panic("Span reference count dropped to zero before the span was Finish()ed")
		}
		if detectSpanRefLeaks {
			sp.setFinalizer(nil)
		}

		sp.i.crdb.tracer.releaseSpanToPool(sp)
		return true
	}
	return false
}

// Tracer exports the tracer this span was created using.
func (sp *Span) Tracer() *Tracer {
	sp.detectUseAfterFinish()
	return sp.i.Tracer()
}

func (sp *Span) String() string {
	return sp.OperationName()
}

// Redactable returns true if this Span's tracer is marked redactable
func (sp *Span) Redactable() bool {
	if sp == nil || sp.i.isNoop() {
		return false
	}
	sp.detectUseAfterFinish()
	return sp.Tracer().Redactable()
}

// Finish marks the Span as completed. It is illegal to use a Span after calling
// Finish().
//
// Finishing a nil *Span is a noop.
func (sp *Span) Finish() {
	sp.finishInternal()
}

// finishInternal finishes the span.
func (sp *Span) finishInternal() {
	if sp == nil || sp.IsNoop() || sp.detectUseAfterFinish() {
		return
	}
	if sp.Tracer().debugUseAfterFinish {
		sp.finishStack = string(debug.Stack())
	}
	atomic.StoreInt32(&sp.finished, 1)
	sp.i.Finish()
	// Release the reference that the span held to itself. Unless we're racing
	// with the finishing of the parent or one of the children, this one will be
	// the last reference, and the span will be made available for re-allocation.
	sp.decRef()
}

// FinishAndGetRecording finishes the span and gets a recording at the same
// time. This is offered as a combined operation because, otherwise, the caller
// would be forced to collect the recording before finishing and so the span
// would appear to be unfinished in the recording (it's illegal to collect the
// recording after the span finishes, except by using this method).
//
// Returns nil if the span is not currently recording (even if it had been
// recording in the past).
func (sp *Span) FinishAndGetRecording(recType RecordingType) Recording {
	rec := Recording(nil)
	if sp.RecordingType() != RecordingOff {
		rec = sp.i.GetRecording(recType, true /* finishing */)
	}
	// Reach directly into sp.i to pass the finishing argument.
	sp.finishInternal()
	return rec
}

// FinishAndGetConfiguredRecording is like FinishAndGetRecording, except that
// the type of recording returned is the type that the span was configured to
// record.
//
// Returns nil if the span is not currently recording (even if it had been
// recording in the past).
func (sp *Span) FinishAndGetConfiguredRecording() Recording {
	rec := Recording(nil)
	recType := sp.RecordingType()
	if recType != RecordingOff {
		rec = sp.i.GetRecording(recType, true /* finishing */)
	}
	// Reach directly into sp.i to pass the finishing argument.
	sp.finishInternal()
	return rec
}

// GetRecording retrieves the current recording, if the Span has recording
// enabled. This can be called while spans that are part of the recording are
// still open; it can run concurrently with operations on those spans.
//
// Returns nil if the span is not currently recording (even if it had been
// recording in the past).
//
// recType indicates the type of information to be returned: structured info or
// structured + verbose info. The caller can ask for either regardless of the
// current recording mode (and also regardless of past recording modes) but, of
// course, GetRecording(RecordingVerbose) will not return verbose info if it was
// never collected.
//
// As a performance optimization, GetRecording does not return tags when
// recType == RecordingStructured. Returning tags requires expensive
// stringification.
//
// A few internal tags are added to denote span properties:
//
//    "_unfinished"	The span was never Finish()ed
//    "_verbose"	The span is a verbose one
//    "_dropped"	The span dropped recordings due to sizing constraints
//
// If recType is RecordingStructured, the return value will be nil if the span
// doesn't have any structured events.
func (sp *Span) GetRecording(recType RecordingType) Recording {
	if sp.detectUseAfterFinish() {
		return nil
	}
	if sp.RecordingType() == RecordingOff {
		return nil
	}
	return sp.i.GetRecording(recType, false /* finishing */)
}

// GetConfiguredRecording is like GetRecording, except the type of recording it
// returns is the one that the span has been previously configured with.
//
// Returns nil if the span is not currently recording (even if it had been
// recording in the past).
func (sp *Span) GetConfiguredRecording() Recording {
	if sp.detectUseAfterFinish() {
		return nil
	}
	recType := sp.RecordingType()
	if recType == RecordingOff {
		return nil
	}
	return sp.i.GetRecording(recType, false /* finishing */)
}

// ImportRemoteSpans adds RecordedSpan data to the recording of the given Span;
// these spans will be part of the result of GetRecording. Used to import
// recorded traces from other nodes.
func (sp *Span) ImportRemoteSpans(remoteSpans []tracingpb.RecordedSpan) {
	if !sp.detectUseAfterFinish() {
		sp.i.ImportRemoteSpans(remoteSpans)
	}
}

// Meta returns the information which needs to be propagated across process
// boundaries in order to derive child spans from this Span. This may return an
// empty SpanMeta (which is a valid input to WithRemoteParentFromSpanMeta) if
// the Span has been optimized out.
func (sp *Span) Meta() SpanMeta {
	if sp.detectUseAfterFinish() {
		return SpanMeta{}
	}
	return sp.i.Meta()
}

// SetRecordingType sets the recording mode of the span and its children,
// recursively. Setting it to RecordingOff disables further recording.
// Everything recorded so far remains in memory.
func (sp *Span) SetRecordingType(to RecordingType) {
	if sp.detectUseAfterFinish() {
		return
	}
	sp.i.SetRecordingType(to)
}

// RecordingType returns the range's current recording mode.
func (sp *Span) RecordingType() RecordingType {
	if sp.detectUseAfterFinish() {
		return RecordingOff
	}
	return sp.i.RecordingType()
}

// IsVerbose returns true if the Span is verbose. See SetVerbose for details.
func (sp *Span) IsVerbose() bool {
	return sp.RecordingType() == RecordingVerbose
}

// Record provides a way to record free-form text into verbose spans. Recordings
// may be dropped due to sizing constraints.
//
// TODO(tbg): make sure `msg` is lint-forced to be const.
func (sp *Span) Record(msg string) {
	if sp.detectUseAfterFinish() {
		return
	}
	sp.i.Record(msg)
}

// Recordf is like Record, but accepts a format specifier.
func (sp *Span) Recordf(format string, args ...interface{}) {
	if sp.detectUseAfterFinish() {
		return
	}
	sp.i.Recordf(format, args...)
}

// RecordStructured adds a Structured payload to the Span. It will be added to
// the recording even if the Span is not verbose; however it will be discarded
// if the underlying Span has been optimized out (i.e. is a noop span). Payloads
// may also be dropped due to sizing constraints.
//
// The caller must not mutate the item once RecordStructured has been called.
func (sp *Span) RecordStructured(item Structured) {
	if sp.detectUseAfterFinish() {
		return
	}
	sp.i.RecordStructured(item)
}

// SetTag adds a tag to the span. If there is a pre-existing tag set for the
// key, it is overwritten.
func (sp *Span) SetTag(key string, value attribute.Value) {
	if sp.detectUseAfterFinish() {
		return
	}
	sp.i.SetTag(key, value)
}

// TraceID retrieves a span's trace ID.
func (sp *Span) TraceID() tracingpb.TraceID {
	if sp.detectUseAfterFinish() {
		return 0
	}
	return sp.i.TraceID()
}

// SpanID retrieves a span's ID.
func (sp *Span) SpanID() tracingpb.SpanID {
	return sp.i.SpanID()
}

// OperationName returns the name of this span assigned when the span was
// created.
func (sp *Span) OperationName() string {
	if sp == nil {
		return "<nil>"
	}
	if sp.IsNoop() {
		return "noop"
	}
	sp.detectUseAfterFinish()
	return sp.i.crdb.operation
}

// IsSterile returns true if this span does not want to have children spans. In
// that case, trying to create a child span will result in the would-be child
// being a root span.
func (sp *Span) IsSterile() bool {
	if sp.detectUseAfterFinish() {
		return true
	}
	return sp.i.sterile
}

// UpdateGoroutineIDToCurrent updates the span's goroutine ID to the current
// goroutine. This should be called when a different goroutine takes ownership
// of a span.
func (sp *Span) UpdateGoroutineIDToCurrent() {
	if sp.detectUseAfterFinish() {
		return
	}
	sp.i.crdb.setGoroutineID(goid.Get())
}

// parentFinished makes sp a root.
func (sp *Span) parentFinished() {
	sp.i.crdb.parentFinished()
}

// reset prepares sp for (re-)use.
//
// sp might be a re-allocated span that was previously used and Finish()ed. In
// such cases, nobody should still be using a reference to the span - doing so
// would be a use-after-Finish. The Tracer can be configured to do best-effort
// detection of such cases (and crash), or it can be configured to tolerate
// them. When tolerating them, in the face of buggy code, this method might be
// called concurrently with the bugster accessing the span. This can result in
// data races, and we don't make much of an effort to avoid them - some fields
// in the span are not protected by a mutex; others are atomics but this method
// doesn't always bother doing atomic operations on them. This method does take
// the crdbSpan's lock, though. That will avoid some races.
func (sp *Span) reset(
	traceID tracingpb.TraceID,
	spanID tracingpb.SpanID,
	operation string,
	goroutineID uint64,
	startTime time.Time,
	logTags *logtags.Buffer,
	kind oteltrace.SpanKind,
	otelSpan oteltrace.Span,
	netTr trace.Trace,
	sterile bool,
) {
	if sp.i.crdb == nil {
		// We assume that spans being reset have come from the sync.Pool.
		panic("unexpected reset of no-op Spans")
	}

	if refCnt := sp.refCnt; refCnt != 0 {
		// The span should not have been made available for reuse with live
		// references.
		panic(fmt.Sprintf("expected 0 refCnt but found %d: %s", refCnt, sp))
	}

	// Take a self-reference. This will be held until Finish(), ensuring that the
	// reference count is not zero before Finish().
	sp.incRef()
	if detectSpanRefLeaks {
		// Register a finalizer that, if not cleared by then, will panic at GC time
		// indicating that some references to the span were leaked (or that the span
		// wasn't Finish()ed, which is one way for the refCount to not reach zero).
		// The finalizer will be cleared when the refCount reaches zero.
		//
		// Note that, in tests that use the TestCluster, there's also detection of
		// non-finished spans through the active spans registry on cluster shutdown.
		sp.setFinalizer(func(sp *Span) {
			// If the Tracer that produced this Span was Close()d, then let's keep
			// quiet. We've detected a Span leak nonetheless, but it's likely that the
			// test that leaked the span has already finished. Panicking in the middle
			// of an unrelated test would be poor UX.
			if sp.Tracer().closed() {
				return
			}
			panic(fmt.Sprintf("Span not finished or references not released; "+
				"span: %s, finished: %t, refCnt: %d.\n"+
				"If running multiple tests, it's possible that the leaked span came from a previous "+
				"test. In particular, if a previous test failed, the failure might have caused some unclean "+
				"unwind that leaked the span. If there are previous failures, consider ignoring this panic.",
				sp, atomic.LoadInt32(&sp.finished) == 1, atomic.LoadInt32(&sp.refCnt)))
		})
	}

	c := sp.i.crdb
	sp.i = spanInner{
		tracer:   sp.i.tracer,
		crdb:     c,
		otelSpan: otelSpan,
		netTr:    netTr,
		sterile:  sterile,
	}

	c.traceID = traceID
	c.spanID = spanID
	c.parentSpanID = 0
	c.operation = operation
	c.startTime = startTime
	c.logTags = logTags
	{
		// Nobody is supposed to have a reference to the span at this point, but let's
		// take the lock anyway to protect against buggy clients accessing the span
		// after Finish().
		c.mu.Lock()
		if len(c.mu.openChildren) != 0 {
			panic(fmt.Sprintf("unexpected children in span being reset: %v", c.mu.openChildren))
		}
		if len(c.mu.tags) != 0 {
			panic(fmt.Sprintf("unexpected tags in span being reset: %v", c.mu.tags))
		}
		if len(c.mu.recording.finishedChildren) != 0 {
			panic(fmt.Sprintf("unexpected finished children in span being reset: %v", c.mu.recording.finishedChildren))
		}
		if c.mu.recording.structured.Len() != 0 {
			panic("unexpected structured recording in span being reset")
		}
		if c.mu.recording.logs.Len() != 0 {
			panic("unexpected logs in span being reset")
		}

		h := sp.helper
		c.mu.crdbSpanMu = crdbSpanMu{
			duration:     -1, // unfinished
			openChildren: h.childrenAlloc[:0],
			goroutineID:  goroutineID,
			recording: recordingState{
				logs:       makeSizeLimitedBuffer(maxLogBytesPerSpan, nil /* scratch */),
				structured: makeSizeLimitedBuffer(maxStructuredBytesPerSpan, h.structuredEventsAlloc[:]),
			},
			tags: h.tagsAlloc[:0],
		}

		if kind != oteltrace.SpanKindUnspecified {
			c.setTagLocked(spanKindTagKey, attribute.StringValue(kind.String()))
		}
		c.mu.Unlock()
	}

	// We only mark the span as not finished at the end so that accesses to the
	// span concurrent with this call are detected if the Tracer is configured to
	// detect use-after-Finish. Similarly, we do the write atomically to prevent
	// reorderings.
	atomic.StoreInt32(&sp.finished, 0)
	sp.finishStack = ""
}

// spanRef represents a reference to a span. In addition to a simple *Span, a
// spanRef prevents the referenced span from being reallocated in between the
// time when this spanRef was created and the time when release() is called. It
// does this by holding open a reference in the span's reference counter.
type spanRef struct {
	*Span
}

// makeSpanRef increments sp's reference counter and returns a spanRef which
// will drop that reference on release().
func makeSpanRef(sp *Span) spanRef {
	sp.incRef()
	return makeSpanRefRaw(sp)
}

// makeSpanRefRaw creates a spanRef for sp, but doesn't increment sp's reference
// counter. The caller must increment the reference count separately.
func makeSpanRefRaw(sp *Span) spanRef {
	return spanRef{Span: sp}
}

// tryMakeSpanRef creates a spanRef for sp unless sp's reference count has
// already reached zero (in which case sp might have been made available for
// reuse). In case of failure, returns spanRef{}, false.
//
// tryMakeSpanRef avoids hazards otherwise made possible by spans being
// Finish()ed and reused concurrently with this tryMakeSpanRef call. For
// example, the WithParent(sp) option holds a reference to sp. We don't want
// releasing sp to the sync.Pool for reuse to race with the creation of that
// option in such a way that WithParent(sp) succeeds and also sp is made
// available for reuse. That can lead to deadlock if, for example, sp ends up
// being reallocated as its own child. If tryMakeSpanRef() succeeds, sp will not
// be made available for reuse until this reference is release()ed.
func tryMakeSpanRef(sp *Span) (spanRef, bool) {
	// What comes is tricky. We want to guarantee that sp is not reused after this
	// method returns. It can be re-used before this method is called, and it can
	// be re-used during our loop below, but not after. More exactly, we want to
	// "atomically" check whether sp is available for reuse and, if it isn't, make
	// sure it doesn't become available for reuse until the would-be child is
	// created (and beyond, for as long as the child holds a reference to the
	// parent). We want to do this using atomics only; there's no locks to work
	// with because the races we're concerned about are races with sp.decRef() and
	// that method cannot take locks because it can run either under a parent's or
	// a child's lock when operating on the parent's refCnt.
	//
	// The way we do it is through instituting the invariant that, once refCnt
	// reaches 0, the only thing that increments it away from 0 is sp.reset(). In
	// particular, WithParent(sp) does not make refCnt 0->1 transitions. This way,
	// if a non-zero refCnt is found, then we know that the span is good to use
	// and we can add our own reference. Once we've added our reference, this
	// reference will keep the span good to use until we eventually release it. On
	// the other hand, if we ever see a zero refCnt, then we can't use the span.

	// Loop until we don't race on sp.refCnt.
	for {
		cnt := atomic.LoadInt32(&sp.refCnt)
		if cnt == 0 {
			// sp was Finish()ed, and it might be in the pool awaiting reallocation.
			// It'd be unsafe to hold a reference to sp because of deadlock hazards,
			// so we'll return an empty option (i.e. the resulting child will be a
			// root).
			return spanRef{}, false
		}
		// Attempt to acquire a reference on sp, but only if we're not racing with anyone.
		swapped := atomic.CompareAndSwapInt32(&sp.refCnt, cnt, cnt+1)
		if swapped {
			// We have a reference. The span can not be re-used while we hold the
			// reference, so we're good to create the child.
			break
		} else {
			// We raced with someone - possibly with a sp.decRef() that might have
			// brought the refCnt to zero and made the span available for reuse. We
			// need to restart to see if that was the case.
			continue
		}
	}

	ref := makeSpanRefRaw(sp)
	return ref, true
}

func (sr *spanRef) empty() bool {
	return sr.Span == nil
}

// release decrements the reference counter of the span that this spanRef refers
// to. If the spanRef does not reference a span because move() has already been
// called, or if release() has already been called, a release() call is a no-op.
//
// Note that release() is not thread-safe. When a spanRef is shared between
// goroutines, the owner span is expected to serialize accesses through the
// span's lock. Different `spanRefs` can reference the same span though, and
// they can be used concurrently.
//
// Returns true if the span's refcount dropped to zero.
func (sr *spanRef) release() bool {
	if sr.empty() {
		// There's no parent; nothing to do.
		return false
	}
	sp := sr.Span
	sr.Span = nil
	return sp.decRef()
}

// move returns a new spanRef holding a reference to sr's span. sr's reference
// is released.
func (sr *spanRef) move() spanRef {
	cpy := *sr
	sr.Span = nil
	return cpy
}

// SpanMeta is information about a Span that is not local to this
// process. Typically, SpanMeta is populated from information
// about a Span on the other end of an RPC, and is used to derive
// a child span via `Tracer.StartSpan`. For local spans, SpanMeta
// is not used, as the *Span directly can be derived from.
//
// SpanMeta contains the trace and span identifiers of the parent,
// along with additional metadata. In particular, this specifies
// whether the child should be recording, in which case the contract
// is that the recording is to be returned to the caller when the
// child finishes, so that the caller can inductively construct the
// entire trace.
type SpanMeta struct {
	traceID tracingpb.TraceID
	spanID  tracingpb.SpanID

	// otelCtx is the OpenTelemetry span context. This is only populated when the
	// remote Span is reporting to an external OpenTelemetry tracer. Setting this
	// will cause child spans to also get an OpenTelemetry span.
	otelCtx oteltrace.SpanContext

	// If set, all spans derived from this context are being recorded.
	recordingType RecordingType

	// sterile is set if this span does not want to have children spans. In that
	// case, trying to create a child span will result in the would-be child being
	// a root span. This is useful for span corresponding to long-running
	// operations that don't want to be associated with derived operations.
	//
	// Note that this field is unlike all the others in that it doesn't make it
	// across the wire through a carrier. As can be seen in
	// Tracer.InjectMetaInto(carrier), if sterile is set, then we don't propagate
	// any info about the span in order to not have a child be created on the
	// other side. Similarly, ExtractMetaFrom does not deserialize this field.
	sterile bool
}

// Empty returns whether or not the SpanMeta is a zero value.
func (sm SpanMeta) Empty() bool {
	return sm.spanID == 0 && sm.traceID == 0
}

func (sm SpanMeta) String() string {
	var s strings.Builder
	s.WriteString(fmt.Sprintf("[spanID: %d, traceID: %d rec: %d", sm.spanID, sm.traceID, sm.recordingType))
	hasOtelSpan := sm.otelCtx.IsValid()
	if hasOtelSpan {
		s.WriteString(" hasOtel")
		s.WriteString(fmt.Sprintf(" trace: %d span: %d", sm.otelCtx.TraceID(), sm.otelCtx.SpanID()))
	}
	s.WriteRune(']')
	return s.String()
}

// ToProto converts a SpanMeta to the TraceInfo proto.
func (sm SpanMeta) ToProto() tracingpb.TraceInfo {
	ti := tracingpb.TraceInfo{
		TraceID:       sm.traceID,
		ParentSpanID:  sm.spanID,
		RecordingMode: sm.recordingType.ToProto(),
	}
	if sm.otelCtx.HasTraceID() {
		var traceID [16]byte = sm.otelCtx.TraceID()
		var spanID [8]byte = sm.otelCtx.SpanID()
		ti.Otel = &tracingpb.TraceInfo_OtelInfo{
			TraceID: traceID[:],
			SpanID:  spanID[:],
		}
	}
	return ti
}

// SpanMetaFromProto converts a TraceInfo proto to SpanMeta.
func SpanMetaFromProto(info tracingpb.TraceInfo) SpanMeta {
	var otelCtx oteltrace.SpanContext
	if info.Otel != nil {
		// NOTE: The ugly starry expressions below can be simplified once/if direct
		// conversions from slices to arrays gets adopted:
		// https://github.com/golang/go/issues/46505
		traceID := *(*[16]byte)(info.Otel.TraceID)
		spanID := *(*[8]byte)(info.Otel.SpanID)
		otelCtx = otelCtx.WithRemote(true).WithTraceID(traceID).WithSpanID(spanID)
	}

	sm := SpanMeta{
		traceID: info.TraceID,
		spanID:  info.ParentSpanID,
		otelCtx: otelCtx,
		sterile: false,
	}
	switch info.RecordingMode {
	case tracingpb.TraceInfo_NONE:
		sm.recordingType = RecordingOff
	case tracingpb.TraceInfo_STRUCTURED:
		sm.recordingType = RecordingStructured
	case tracingpb.TraceInfo_VERBOSE:
		sm.recordingType = RecordingVerbose
	default:
		sm.recordingType = RecordingOff
	}
	return sm
}

// Structured is an opaque protobuf that can be attached to a trace via
// `Span.RecordStructured`.
type Structured interface {
	protoutil.Message
}
