// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package tracing

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/net/trace"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
)

// spanMeta stores span information that is common to span and spanContext.
type spanMeta struct {
	// A probabilistically unique identifier for a [multi-span] trace.
	TraceID uint64

	// A probabilistically unique identifier for a span.
	SpanID uint64
}

type spanContext struct {
	spanMeta

	// Underlying lightstep span context, if using lightstep.
	lightstep opentracing.SpanContext

	// If set, all spans derived from this context are being recorded as a group.
	recordingGroup *spanGroup

	// The span's associated baggage.
	Baggage map[string]string
}

var _ opentracing.SpanContext = &spanContext{}

// ForeachBaggageItem is part of the opentracing.SpanContext interface.
func (sc *spanContext) ForeachBaggageItem(handler func(k, v string) bool) {
	for k, v := range sc.Baggage {
		if !handler(k, v) {
			break
		}
	}
}

type span struct {
	spanMeta

	parentSpanID uint64

	tracer *Tracer

	// x/net/trace.Trace instance; nil if not tracing to x/net/trace.
	netTr trace.Trace
	// "Shadow" lightstep span; nil if not using lightstep.
	lightstep opentracing.Span

	operation string
	startTime time.Time

	// Atomic flag used to avoid taking the mutex in the hot path.
	recording int32

	mu struct {
		syncutil.Mutex
		// duration is initialized to -1 and set on Finish().
		duration time.Duration

		recordingGroup *spanGroup
		recordedLogs   []opentracing.LogRecord
		// tags are only set when recording.
		// TODO(radu): perhaps we want a recording to capture all the tags (even
		// those that were set before recording started)?
		tags opentracing.Tags

		// The span's associated baggage.
		Baggage map[string]string
	}
}

var _ opentracing.Span = &span{}

func (s *span) isRecording() bool {
	return atomic.LoadInt32(&s.recording) != 0
}

func (s *span) enableRecording(group *spanGroup) {
	if group == nil {
		panic("no spanGroup")
	}
	s.mu.Lock()
	atomic.StoreInt32(&s.recording, 1)
	s.mu.recordingGroup = group
	// Clear any previously recorded logs.
	s.mu.recordedLogs = nil
	s.mu.Unlock()

	group.addSpan(s)
}

// StartRecording enables recording on the span. Events from this point forward
// are recorded; also, all direct and indirect child spans started from now on
// will be part of the same recording.
//
// Recording is not supported by noop spans; to ensure a real span is always
// created, use the Force option to StartSpan.
//
// If recording was already started on this span (either directly or because a
// parent span is recording), the old recording is lost.
func StartRecording(os opentracing.Span) {
	if IsNoopSpan(os) {
		panic("StartRecording called on NoopSpan; use the Force option for StartSpan")
	}
	os.(*span).enableRecording(new(spanGroup))
}

func (s *span) disableRecording() {
	s.mu.Lock()
	atomic.StoreInt32(&s.recording, 0)
	s.mu.recordingGroup = nil
	s.mu.Unlock()
}

// StopRecording disables recording on this span. Child spans that were created
// since recording was started will continue to record until they finish.
//
// Calling this after StartRecording is not required; the recording will go away
// when all the spans finish.
func StopRecording(os opentracing.Span) {
	os.(*span).disableRecording()
}

// GetRecording retrieves the current recording, if the span has
// recording enabled. This can be called while spans that are part of the
// record are still open; it can run concurrently with operations on those
// spans.
func GetRecording(os opentracing.Span) []basictracer.RawSpan {
	if IsNoopSpan(os) {
		return nil
	}
	s := os.(*span)
	if !s.isRecording() {
		return nil
	}
	s.mu.Lock()
	group := s.mu.recordingGroup
	s.mu.Unlock()
	if group == nil {
		return nil
	}
	return group.getSpans()
}

// ImportRemoteSpans adds raw span data to the recording of the given span;
// these spans will be part of the result of GetRecording.
// Used to import recorded traces from other nodes.
func ImportRemoteSpans(os opentracing.Span, rawSpans []basictracer.RawSpan) error {
	s := os.(*span)
	s.mu.Lock()
	group := s.mu.recordingGroup
	s.mu.Unlock()
	if group == nil {
		return errors.New("adding Raw Spans to a span that isn't recording")
	}
	group.Lock()
	group.rawSpans = append(group.rawSpans, rawSpans...)
	group.Unlock()
	return nil
}

// ClearRecordedLogs removes all logs (events) from the spans in the recording.
// This can be used to retrieve logs iteratively:
//    r1 := GetRecording(sp)
//    ClearRecordedLogs(sp)
//    // do more stuff, which triggers more events..
//    r2 := GetRecording(sp)  // contains the new events.
//
// TODO(radu): this API is inherently racy - we lose any events that may have
// happened between GetRecording and ClearRecordedLogs. This could be fixed (by
// integrating the two) but this is only temporary (it is used by the current
// current EXPLAIN (TRACE) implementation which is about to change).
func ClearRecordedLogs(os opentracing.Span) {
	s := os.(*span)
	s.mu.Lock()
	group := s.mu.recordingGroup
	s.mu.Unlock()
	if group != nil {
		group.clearLogs()
	}
}

// IsNoopSpan returns true if events for this span are just dropped. This is the
// case when tracing is disable and we're not recording.
func IsNoopSpan(s opentracing.Span) bool {
	_, noop := s.(*noopSpan)
	return noop
}

// Finish is part of the opentracing.Span interface.
func (s *span) Finish() {
	s.FinishWithOptions(opentracing.FinishOptions{})
}

// FinishWithOptions is part of the opentracing.Span interface.
func (s *span) FinishWithOptions(opts opentracing.FinishOptions) {
	finishTime := opts.FinishTime
	if finishTime.IsZero() {
		finishTime = time.Now()
	}
	s.mu.Lock()
	s.mu.duration = finishTime.Sub(s.startTime)
	s.mu.Unlock()
	if s.lightstep != nil {
		s.lightstep.Finish()
	}
	if s.netTr != nil {
		s.netTr.Finish()
	}
}

// Context is part of the opentracing.Span interface.
func (s *span) Context() opentracing.SpanContext {
	s.mu.Lock()
	defer s.mu.Unlock()
	baggageCopy := make(map[string]string, len(s.mu.Baggage))
	for k, v := range s.mu.Baggage {
		baggageCopy[k] = v
	}
	sc := &spanContext{
		spanMeta: s.spanMeta,
		Baggage:  baggageCopy,
	}
	if s.lightstep != nil {
		sc.lightstep = s.lightstep.Context()
	}

	if s.isRecording() {
		sc.recordingGroup = s.mu.recordingGroup
	}
	return sc
}

// SetOperationName is part of the opentracing.Span interface.
func (s *span) SetOperationName(operationName string) opentracing.Span {
	if s.lightstep != nil {
		s.lightstep.SetOperationName(operationName)
	}
	s.operation = operationName
	return s
}

// SetTag is part of the opentracing.Span interface.
func (s *span) SetTag(key string, value interface{}) opentracing.Span {
	if s.lightstep != nil {
		s.lightstep.SetTag(key, value)
	}
	if s.netTr != nil {
		s.netTr.LazyPrintf("%s:%v", key, value)
	}
	if s.isRecording() {
		s.mu.Lock()
		if s.mu.tags == nil {
			s.mu.tags = make(opentracing.Tags)
		}
		s.mu.tags[key] = value
		s.mu.Unlock()
	}
	return s
}

// LogFields is part of the opentracing.Span interface.
func (s *span) LogFields(fields ...otlog.Field) {
	if s.lightstep != nil {
		s.lightstep.LogFields(fields...)
	}
	if s.netTr != nil {
		// TODO(radu): when LightStep supports arbitrary fields, we should make
		// the formatting of the message consistent with that. Until then we treat
		// legacy events that just have an "event" key specially.
		if len(fields) == 1 && fields[0].Key() == "event" {
			s.netTr.LazyPrintf("%s", fields[0].Value())
		} else {
			var buf bytes.Buffer
			for i, f := range fields {
				if i > 0 {
					buf.WriteByte(' ')
				}
				fmt.Fprintf(&buf, "%s:%v", f.Key(), f.Value())
			}

			s.netTr.LazyPrintf("%s", buf.String())
		}
	}
	if s.isRecording() {
		s.mu.Lock()
		if len(s.mu.recordedLogs) < maxLogsPerSpan {
			s.mu.recordedLogs = append(s.mu.recordedLogs, opentracing.LogRecord{
				Timestamp: time.Now(),
				Fields:    fields,
			})
		}
		s.mu.Unlock()
	}
}

// LogKV is part of the opentracing.Span interface.
func (s *span) LogKV(alternatingKeyValues ...interface{}) {
	fields, err := otlog.InterleavedKVToFields(alternatingKeyValues...)
	if err != nil {
		s.LogFields(otlog.Error(err), otlog.String("function", "LogKV"))
		return
	}
	s.LogFields(fields...)
}

// SetBaggageItem is part of the opentracing.Span interface.
func (s *span) SetBaggageItem(restrictedKey, value string) opentracing.Span {
	s.mu.Lock()
	if s.mu.Baggage == nil {
		s.mu.Baggage = make(map[string]string)
	}
	s.mu.Baggage[restrictedKey] = value
	s.mu.Unlock()

	if s.lightstep != nil {
		s.lightstep.SetBaggageItem(restrictedKey, value)
	}
	// Also set a tag so it shows up in the Lightstep UI or x/net/trace.
	s.SetTag(restrictedKey, value)
	return s
}

// BaggageItem is part of the opentracing.Span interface.
func (s *span) BaggageItem(restrictedKey string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.Baggage[restrictedKey]
}

// Tracer is part of the opentracing.Span interface.
func (s *span) Tracer() opentracing.Tracer {
	return s.tracer
}

// LogEvent is part of the opentracing.Span interface. Deprecated.
func (s *span) LogEvent(event string) {
	s.LogFields(otlog.String("event", event))
}

// LogEventWithPayload is part of the opentracing.Span interface. Deprecated.
func (s *span) LogEventWithPayload(event string, payload interface{}) {
	s.LogFields(otlog.String("event", event), otlog.Object("payload", payload))
}

// Log is part of the opentracing.Span interface. Deprecated.
func (s *span) Log(data opentracing.LogData) {
	panic("unimplemented")
}

// spanGroup keeps track of all the spans that are being recorded as a group (i.e.
// the span for which recording was enabled and all direct or indirect child
// spans since then).
type spanGroup struct {
	syncutil.Mutex
	spans []*span
	// rawSpans stores spans obtained from another host that we want to associate
	// with the record for this group.
	rawSpans []basictracer.RawSpan
}

func (ss *spanGroup) addSpan(s *span) {
	ss.Lock()
	ss.spans = append(ss.spans, s)
	ss.Unlock()
}

func (ss *spanGroup) getSpans() []basictracer.RawSpan {
	ss.Lock()
	spans := ss.spans
	rawSpans := ss.rawSpans
	ss.Unlock()

	result := make([]basictracer.RawSpan, 0, len(spans)+len(rawSpans))
	for _, s := range spans {
		s.mu.Lock()
		rs := basictracer.RawSpan{
			Context: basictracer.SpanContext{
				TraceID: s.TraceID,
				SpanID:  s.SpanID,
			},
			ParentSpanID: s.parentSpanID,
			Operation:    s.operation,
			Start:        s.startTime,
			Duration:     s.mu.duration,
		}
		if rs.Duration < 0 {
			// Span not finished yet; set duration as if it finished just now.
			rs.Duration = time.Since(s.startTime)
		}

		if len(s.mu.Baggage) > 0 {
			rs.Context.Baggage = make(map[string]string)
			for k, v := range s.mu.Baggage {
				rs.Context.Baggage[k] = v
			}
		}
		if len(s.mu.tags) > 0 {
			rs.Tags = make(opentracing.Tags)
			for k, v := range s.mu.tags {
				rs.Tags[k] = v
			}
		}
		// It's safe to return the same slice as long as the caller doesn't modify
		// the spans. Limit its capacity just in case.
		rs.Logs = s.mu.recordedLogs[:len(s.mu.recordedLogs):len(s.mu.recordedLogs)]
		s.mu.Unlock()
		result = append(result, rs)
	}
	return append(result, rawSpans...)
}

func (ss *spanGroup) clearLogs() {
	ss.Lock()
	spans := ss.spans
	ss.rawSpans = nil
	ss.Unlock()

	for _, s := range spans {
		s.mu.Lock()
		s.mu.recordedLogs = nil
		s.mu.Unlock()
	}
}

type noopSpanContext struct{}

var _ opentracing.SpanContext = noopSpanContext{}

func (n noopSpanContext) ForeachBaggageItem(handler func(k, v string) bool) {}

type noopSpan struct {
	tracer *Tracer
}

var _ opentracing.Span = &noopSpan{}

func (n *noopSpan) Context() opentracing.SpanContext                       { return noopSpanContext{} }
func (n *noopSpan) BaggageItem(key string) string                          { return "" }
func (n *noopSpan) SetTag(key string, value interface{}) opentracing.Span  { return n }
func (n *noopSpan) Finish()                                                {}
func (n *noopSpan) FinishWithOptions(opts opentracing.FinishOptions)       {}
func (n *noopSpan) SetOperationName(operationName string) opentracing.Span { return n }
func (n *noopSpan) Tracer() opentracing.Tracer                             { return n.tracer }
func (n *noopSpan) LogFields(fields ...otlog.Field)                        {}
func (n *noopSpan) LogKV(keyVals ...interface{})                           {}
func (n *noopSpan) LogEvent(event string)                                  {}
func (n *noopSpan) LogEventWithPayload(event string, payload interface{})  {}
func (n *noopSpan) Log(data opentracing.LogData)                           {}

func (n *noopSpan) SetBaggageItem(key, val string) opentracing.Span {
	if key == Snowball {
		panic("attempting to set Snowball on a noop span; use the Force option to StartSpan")
	}
	return n
}
