// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracing

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/redact"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
	"golang.org/x/net/trace"
)

// spanInner contains all the span types we are recording to.
// Note that one of crdb, otelSpan or netTr should be non-nil.
type spanInner struct {
	tracer *Tracer // never nil

	// Internal trace Span; nil if not tracing to crdb.
	// When not-nil, allocated together with the surrounding Span for
	// performance.
	crdb *crdbSpan
	// x/net/trace.Trace instance; nil if not tracing to x/net/trace.
	netTr trace.Trace
	// otelSpan is the "shadow span" created for reporting to the OpenTelemetry
	// tracer (if an otel tracer was configured).
	otelSpan oteltrace.Span

	// sterile is set if this span does not want to have children spans. In that
	// case, trying to create a child span will result in the would-be child being
	// a root span. This is useful for span corresponding to long-running
	// operations that don't want to be associated with derived operations.
	sterile bool
}

func (s *spanInner) TraceID() tracingpb.TraceID {
	return s.crdb.TraceID()
}

func (s *spanInner) SpanID() tracingpb.SpanID {
	return s.crdb.SpanID()
}

func (s *spanInner) isSterile() bool {
	return s.sterile
}

func (s *spanInner) RecordingType() tracingpb.RecordingType {
	return s.crdb.recordingType()
}

func (s *spanInner) SetRecordingType(to tracingpb.RecordingType) {
	s.crdb.SetRecordingType(to)
}

// GetTraceRecording returns the span's recording as a Trace.
//
// See also GetRecording(), which returns it as a tracingpb.Recording.
func (s *spanInner) GetTraceRecording(recType tracingpb.RecordingType, finishing bool) Trace {
	return s.crdb.GetRecording(recType, finishing)
}

// GetRecording returns the span's recording.
//
// finishing indicates whether s is in the process of finishing. If it isn't,
// the recording will include an "_unfinished" tag.
func (s *spanInner) GetRecording(
	recType tracingpb.RecordingType, finishing bool,
) tracingpb.Recording {
	trace := s.GetTraceRecording(recType, finishing)
	return trace.ToRecording()
}

func (s *spanInner) ImportTrace(trace Trace) {
	s.crdb.recordFinishedChildren(trace)
}

func treeifyRecording(rec tracingpb.Recording) Trace {
	if len(rec) == 0 {
		return Trace{}
	}

	byParent := make(map[tracingpb.SpanID][]*tracingpb.RecordedSpan)
	for i := range rec {
		s := &rec[i]
		byParent[s.ParentSpanID] = append(byParent[s.ParentSpanID], s)
	}
	r := treeifyRecordingInner(rec[0], byParent)

	// Include the orphans under the root.
	orphans := rec.OrphanSpans()
	traces := make([]Trace, len(orphans))
	for i, sp := range orphans {
		traces[i] = treeifyRecordingInner(sp, byParent)
	}
	r.addChildren(traces, 0 /* maxSpans */, 0 /* maxStructuredBytes */)
	return r
}

func treeifyRecordingInner(
	sp tracingpb.RecordedSpan, byParent map[tracingpb.SpanID][]*tracingpb.RecordedSpan,
) Trace {
	r := MakeTrace(sp)
	children := make([]Trace, len(byParent[sp.SpanID]))
	for i, s := range byParent[sp.SpanID] {
		children[i] = treeifyRecordingInner(*s, byParent)
	}
	r.addChildren(children, 0 /* maxSpans */, 0 /* maxStructuredBytes */)
	return r
}

func (s *spanInner) Finish() {
	if s == nil {
		return
	}

	if !s.crdb.finish() {
		// Short-circuit because netTr.Finish does not tolerate double-finish.
		return
	}

	if s.otelSpan != nil {
		// Serialize the lazy tags.
		s.crdb.mu.Lock()
		defer s.crdb.mu.Unlock()
		for _, lazyTagGroup := range s.crdb.getLazyTagGroupsLocked() {
			for _, tag := range lazyTagGroup.Tags {
				key := attribute.Key(tag.Key)
				if lazyTagGroup.Name != tracingpb.AnonymousTagGroupName {
					key = attribute.Key(fmt.Sprintf("%s-%s", lazyTagGroup.Name, tag.Key))
				}
				s.otelSpan.SetAttributes(attribute.KeyValue{
					Key:   key,
					Value: attribute.StringValue(tag.Value),
				})
			}
		}

		s.otelSpan.End()
	}

	if s.netTr != nil {
		s.netTr.Finish()
	}
}

func (s *spanInner) Meta() SpanMeta {
	var traceID tracingpb.TraceID
	var spanID tracingpb.SpanID
	var recordingType tracingpb.RecordingType
	var sterile bool

	if s.crdb != nil {
		traceID, spanID = s.crdb.traceID, s.crdb.spanID
		recordingType = s.crdb.mu.recording.recordingType.load()
		sterile = s.isSterile()
	}

	var otelCtx oteltrace.SpanContext
	if s.otelSpan != nil {
		otelCtx = s.otelSpan.SpanContext()
	}

	if traceID == 0 &&
		spanID == 0 &&
		!otelCtx.TraceID().IsValid() &&
		recordingType == 0 &&
		!sterile {
		return SpanMeta{}
	}
	return SpanMeta{
		traceID:       traceID,
		spanID:        spanID,
		otelCtx:       otelCtx,
		recordingType: recordingType,
		sterile:       sterile,
	}
}

// OperationName returns the span's name. The name was specified at span
// creation time.
func (s *spanInner) OperationName() string {
	return s.crdb.operation
}

func (s *spanInner) SetTag(key string, value attribute.Value) *spanInner {
	if s.otelSpan != nil {
		s.otelSpan.SetAttributes(attribute.KeyValue{
			Key:   attribute.Key(key),
			Value: value,
		})
	}
	if s.netTr != nil {
		s.netTr.LazyPrintf("%s:%v", key, value)
	}
	s.crdb.mu.Lock()
	defer s.crdb.mu.Unlock()
	s.crdb.setTagLocked(key, value)
	return s
}

func (s *spanInner) SetLazyTag(key string, value interface{}) *spanInner {
	s.crdb.mu.Lock()
	defer s.crdb.mu.Unlock()
	s.crdb.setLazyTagLocked(key, value)
	return s
}

func (s *spanInner) setLazyTagLocked(key string, value interface{}) *spanInner {
	s.crdb.setLazyTagLocked(key, value)
	return s
}

// GetLazyTag returns the value of the tag with the given key. If that tag doesn't
// exist, the bool retval is false.
func (s *spanInner) GetLazyTag(key string) (interface{}, bool) {
	s.crdb.mu.Lock()
	defer s.crdb.mu.Unlock()
	return s.crdb.getLazyTagLocked(key)
}

func (s *spanInner) RecordStructured(item Structured) {
	s.crdb.recordStructured(item)
	if s.hasVerboseSink() {
		// Do not call .String() on the item, so that non-redactable bits
		// in its representation are properly preserved by Recordf() in
		// verbose recordings.
		s.Recordf("%v", item)
	}
}

func (s *spanInner) Record(msg string) {
	s.Recordf("%s", msg)
}

func (s *spanInner) Recordf(format string, args ...interface{}) {
	if !s.hasVerboseSink() {
		return
	}
	var str redact.RedactableString
	if s.Tracer().Redactable() {
		str = redact.Sprintf(format, args...)
	} else {
		// `fmt.Sprintf` when called on a logEntry will use the faster
		// `logEntry.String` method instead of `logEntry.SafeFormat`.
		// The additional use of `redact.Sprint(...)` is necessary
		// to wrap the result in redaction markers.
		str = redact.Sprint(fmt.Sprintf(format, args...))
	}
	s.recordRedactable(format, args, str)
}

func (s *spanInner) recordRedactable(
	format string, args []interface{}, str redact.RedactableString,
) {
	if s.otelSpan != nil {
		// TODO(obs-inf): depending on the situation it may be more appropriate to
		// redact the string here.
		// See:
		// https://github.com/cockroachdb/cockroach/issues/58610#issuecomment-926093901
		s.otelSpan.AddEvent(str.StripMarkers(), oteltrace.WithTimestamp(timeutil.Now()))
	}
	if s.netTr != nil {
		s.netTr.LazyPrintf(format, args)
	}
	s.crdb.record(str)
}

// hasVerboseSink returns false if there is no reason to even evaluate Record
// because the result wouldn't be used for anything.
func (s *spanInner) hasVerboseSink() bool {
	if s.netTr == nil && s.otelSpan == nil && s.RecordingType() != tracingpb.RecordingVerbose {
		return false
	}
	return true
}

// Tracer exports the tracer this span was created using.
func (s *spanInner) Tracer() *Tracer {
	return s.tracer
}
