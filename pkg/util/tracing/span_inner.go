// Copyright 2021 The Cockroach Authors.
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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
	"golang.org/x/net/trace"
)

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
}

func (s *spanInner) TraceID() uint64 {
	if s.isNoop() {
		return 0
	}
	return s.crdb.traceID
}

func (s *spanInner) isNoop() bool {
	return s.crdb == nil && s.netTr == nil && s.otelSpan == nil
}

func (s *spanInner) IsVerbose() bool {
	return s.crdb.recordingType() == RecordingVerbose
}

func (s *spanInner) SetVerbose(to bool) {
	// TODO(tbg): when always-on tracing is firmly established, we can remove the ugly
	// caveat that SetVerbose(true) is a panic on a noop span because there will be no
	// noop span.
	if s.isNoop() {
		panic(errors.AssertionFailedf("SetVerbose called on NoopSpan; use the WithForceRealSpan option for StartSpan"))
	}
	if to {
		s.crdb.enableRecording(RecordingVerbose)
	} else {
		s.crdb.disableRecording()
	}
}

func (s *spanInner) SetVerboseRecursively(to bool) {
	s.SetVerbose(to)
	s.crdb.setVerboseRecursively(to)
}

func (s *spanInner) ResetRecording() {
	s.crdb.resetRecording()
}

func (s *spanInner) GetRecording() Recording {
	if s.isNoop() {
		return nil
	}
	// If the span is not verbose, optimize by avoiding the tags.
	// This span is likely only used to carry payloads around.
	wantTags := s.crdb.recordingType() == RecordingVerbose
	return s.crdb.getRecording(wantTags)
}

func (s *spanInner) ImportRemoteSpans(remoteSpans []tracingpb.RecordedSpan) {
	s.crdb.importRemoteSpans(remoteSpans)
}

func (s *spanInner) Finish() {
	if s == nil {
		return
	}
	if s.isNoop() {
		return
	}
	finishTime := timeutil.Now()
	duration := finishTime.Sub(s.crdb.startTime)
	if duration == 0 {
		duration = time.Nanosecond
	}

	s.crdb.mu.Lock()
	if alreadyFinished := s.crdb.mu.duration >= 0; alreadyFinished {
		s.crdb.mu.Unlock()

		// External spans and net/trace are not always forgiving about spans getting
		// finished twice, but it may happen so let's be resilient to it.
		return
	}
	s.crdb.mu.duration = duration
	s.crdb.mu.Unlock()

	if s.otelSpan != nil {
		s.otelSpan.End()
	}
	if s.netTr != nil {
		s.netTr.Finish()
	}
	if s.crdb.rootSpan.spanID == s.crdb.spanID {
		s.tracer.activeSpans.Lock()
		delete(s.tracer.activeSpans.m, s.crdb.spanID)
		s.tracer.activeSpans.Unlock()
	}
}

func (s *spanInner) Meta() SpanMeta {
	var traceID uint64
	var spanID uint64
	var recordingType RecordingType
	var baggage map[string]string

	if s.crdb != nil {
		traceID, spanID = s.crdb.traceID, s.crdb.spanID
		s.crdb.mu.Lock()
		defer s.crdb.mu.Unlock()
		n := len(s.crdb.mu.baggage)
		// In the common case, we have no baggage, so avoid making an empty map.
		if n > 0 {
			baggage = make(map[string]string, n)
		}
		for k, v := range s.crdb.mu.baggage {
			baggage[k] = v
		}
		recordingType = s.crdb.mu.recording.recordingType.load()
	}

	var otelCtx oteltrace.SpanContext
	if s.otelSpan != nil {
		otelCtx = s.otelSpan.SpanContext()
	}

	if traceID == 0 &&
		spanID == 0 &&
		!otelCtx.TraceID().IsValid() &&
		recordingType == 0 &&
		baggage == nil {
		return SpanMeta{}
	}
	return SpanMeta{
		traceID:       traceID,
		spanID:        spanID,
		otelCtx:       otelCtx,
		recordingType: recordingType,
		Baggage:       baggage,
	}
}

func (s *spanInner) SetOperationName(operationName string) *spanInner {
	if s.isNoop() {
		return s
	}
	if s.otelSpan != nil {
		s.otelSpan.SetName(operationName)
	}
	s.crdb.mu.Lock()
	s.crdb.mu.operation = operationName
	s.crdb.mu.Unlock()
	return s
}

func (s *spanInner) SetTag(key string, value attribute.Value) *spanInner {
	if s.isNoop() {
		return s
	}
	return s.setTagInner(key, value, false /* locked */)
}

func (s *spanInner) setTagInner(key string, value attribute.Value, locked bool) *spanInner {
	if s.otelSpan != nil {
		s.otelSpan.SetAttributes(attribute.KeyValue{
			Key:   attribute.Key(key),
			Value: value,
		})
	}
	if s.netTr != nil {
		s.netTr.LazyPrintf("%s:%v", key, value)
	}
	// The internal tags will be used if we start a recording on this Span.
	if !locked {
		s.crdb.mu.Lock()
		defer s.crdb.mu.Unlock()
	}
	s.crdb.setTagLocked(key, value)
	return s
}

func (s *spanInner) RecordStructured(item Structured) {
	if s.isNoop() {
		return
	}
	s.crdb.recordStructured(item)
	if s.hasVerboseSink() {
		// NB: TrimSpace avoids the trailing whitespace generated by the
		// protobuf stringers.
		s.Record(strings.TrimSpace(item.String()))
	}
}

func (s *spanInner) Record(msg string) {
	s.Recordf("%s", msg)
}

func (s *spanInner) Recordf(format string, args ...interface{}) {
	if !s.hasVerboseSink() {
		return
	}
	str := redact.Sprintf(format, args...)
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
	if s.netTr == nil && s.otelSpan == nil && !s.IsVerbose() {
		return false
	}
	return true
}

func (s *spanInner) SetBaggageItem(restrictedKey, value string) *spanInner {
	if s.isNoop() {
		return s
	}
	s.crdb.setBaggageItemAndTag(restrictedKey, value)
	if s.otelSpan != nil {
		// In OpenTelemetry, baggage is stored directly in the context, separately
		// from the span. We don't go through the trouble. We'll set a tag on the
		// current span, however.
		s.otelSpan.SetAttributes(attribute.String(restrictedKey, value))
	}
	// NB: nothing to do for net/trace.

	return s
}

// Tracer exports the tracer this span was created using.
func (s *spanInner) Tracer() *Tracer {
	return s.tracer
}
