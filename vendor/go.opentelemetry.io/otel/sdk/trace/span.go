// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package trace // import "go.opentelemetry.io/otel/sdk/trace"

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	rt "runtime/trace"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/internal"
	"go.opentelemetry.io/otel/sdk/resource"
)

// ReadOnlySpan allows reading information from the data structure underlying a
// trace.Span. It is used in places where reading information from a span is
// necessary but changing the span isn't necessary or allowed.
//
// Warning: methods may be added to this interface in minor releases.
type ReadOnlySpan interface {
	// Name returns the name of the span.
	Name() string
	// SpanContext returns the unique SpanContext that identifies the span.
	SpanContext() trace.SpanContext
	// Parent returns the unique SpanContext that identifies the parent of the
	// span if one exists. If the span has no parent the returned SpanContext
	// will be invalid.
	Parent() trace.SpanContext
	// SpanKind returns the role the span plays in a Trace.
	SpanKind() trace.SpanKind
	// StartTime returns the time the span started recording.
	StartTime() time.Time
	// EndTime returns the time the span stopped recording. It will be zero if
	// the span has not ended.
	EndTime() time.Time
	// Attributes returns the defining attributes of the span.
	Attributes() []attribute.KeyValue
	// Links returns all the links the span has to other spans.
	Links() []Link
	// Events returns all the events that occurred within in the spans
	// lifetime.
	Events() []Event
	// Status returns the spans status.
	Status() Status
	// InstrumentationLibrary returns information about the instrumentation
	// library that created the span.
	InstrumentationLibrary() instrumentation.Library
	// Resource returns information about the entity that produced the span.
	Resource() *resource.Resource
	// DroppedAttributes returns the number of attributes dropped by the span
	// due to limits being reached.
	DroppedAttributes() int
	// DroppedLinks returns the number of links dropped by the span due to
	// limits being reached.
	DroppedLinks() int
	// DroppedEvents returns the number of events dropped by the span due to
	// limits being reached.
	DroppedEvents() int
	// ChildSpanCount returns the count of spans that consider the span a
	// direct parent.
	ChildSpanCount() int

	// A private method to prevent users implementing the
	// interface and so future additions to it will not
	// violate compatibility.
	private()
}

// ReadWriteSpan exposes the same methods as trace.Span and in addition allows
// reading information from the underlying data structure.
// This interface exposes the union of the methods of trace.Span (which is a
// "write-only" span) and ReadOnlySpan. New methods for writing or reading span
// information should be added under trace.Span or ReadOnlySpan, respectively.
//
// Warning: methods may be added to this interface in minor releases.
type ReadWriteSpan interface {
	trace.Span
	ReadOnlySpan
}

// recordingSpan is an implementation of the OpenTelemetry Span API
// representing the individual component of a trace that is sampled.
type recordingSpan struct {
	// mu protects the contents of this span.
	mu sync.Mutex

	// parent holds the parent span of this span as a trace.SpanContext.
	parent trace.SpanContext

	// spanKind represents the kind of this span as a trace.SpanKind.
	spanKind trace.SpanKind

	// name is the name of this span.
	name string

	// startTime is the time at which this span was started.
	startTime time.Time

	// endTime is the time at which this span was ended. It contains the zero
	// value of time.Time until the span is ended.
	endTime time.Time

	// status is the status of this span.
	status Status

	// childSpanCount holds the number of child spans created for this span.
	childSpanCount int

	// resource contains attributes representing an entity that produced this
	// span.
	resource *resource.Resource

	// instrumentationLibrary defines the instrumentation library used to
	// provide instrumentation.
	instrumentationLibrary instrumentation.Library

	// spanContext holds the SpanContext of this span.
	spanContext trace.SpanContext

	// attributes are capped at configured limit. When the capacity is reached
	// an oldest entry is removed to create room for a new entry.
	attributes *attributesMap

	// events are stored in FIFO queue capped by configured limit.
	events *evictedQueue

	// links are stored in FIFO queue capped by configured limit.
	links *evictedQueue

	// executionTracerTaskEnd ends the execution tracer span.
	executionTracerTaskEnd func()

	// tracer is the SDK tracer that created this span.
	tracer *tracer

	// spanLimits holds the limits to this span.
	spanLimits SpanLimits
}

var _ ReadWriteSpan = (*recordingSpan)(nil)
var _ runtimeTracer = (*recordingSpan)(nil)

// SpanContext returns the SpanContext of this span.
func (s *recordingSpan) SpanContext() trace.SpanContext {
	if s == nil {
		return trace.SpanContext{}
	}
	return s.spanContext
}

// IsRecording returns if this span is being recorded. If this span has ended
// this will return false.
func (s *recordingSpan) IsRecording() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.endTime.IsZero()
}

// SetStatus sets the status of the Span in the form of a code and a
// description, overriding previous values set. The description is only
// included in the set status when the code is for an error. If this span is
// not being recorded than this method does nothing.
func (s *recordingSpan) SetStatus(code codes.Code, description string) {
	if !s.IsRecording() {
		return
	}

	status := Status{Code: code}
	if code == codes.Error {
		status.Description = description
	}

	s.mu.Lock()
	s.status = status
	s.mu.Unlock()
}

// SetAttributes sets attributes of this span.
//
// If a key from attributes already exists the value associated with that key
// will be overwritten with the value contained in attributes.
//
// If this span is not being recorded than this method does nothing.
func (s *recordingSpan) SetAttributes(attributes ...attribute.KeyValue) {
	if !s.IsRecording() {
		return
	}
	s.copyToCappedAttributes(attributes...)
}

// End ends the span. This method does nothing if the span is already ended or
// is not being recorded.
//
// The only SpanOption currently supported is WithTimestamp which will set the
// end time for a Span's life-cycle.
//
// If this method is called while panicking an error event is added to the
// Span before ending it and the panic is continued.
func (s *recordingSpan) End(options ...trace.SpanEndOption) {
	// Do not start by checking if the span is being recorded which requires
	// acquiring a lock. Make a minimal check that the span is not nil.
	if s == nil {
		return
	}

	// Store the end time as soon as possible to avoid artificially increasing
	// the span's duration in case some operation below takes a while.
	et := internal.MonotonicEndTime(s.startTime)

	// Do relative expensive check now that we have an end time and see if we
	// need to do any more processing.
	if !s.IsRecording() {
		return
	}

	config := trace.NewSpanEndConfig(options...)
	if recovered := recover(); recovered != nil {
		// Record but don't stop the panic.
		defer panic(recovered)
		opts := []trace.EventOption{
			trace.WithAttributes(
				semconv.ExceptionTypeKey.String(typeStr(recovered)),
				semconv.ExceptionMessageKey.String(fmt.Sprint(recovered)),
			),
		}

		if config.StackTrace() {
			opts = append(opts, trace.WithAttributes(
				semconv.ExceptionStacktraceKey.String(recordStackTrace()),
			))
		}

		s.addEvent(semconv.ExceptionEventName, opts...)
	}

	if s.executionTracerTaskEnd != nil {
		s.executionTracerTaskEnd()
	}

	s.mu.Lock()
	// Setting endTime to non-zero marks the span as ended and not recording.
	if config.Timestamp().IsZero() {
		s.endTime = et
	} else {
		s.endTime = config.Timestamp()
	}
	s.mu.Unlock()

	if sps, ok := s.tracer.provider.spanProcessors.Load().(spanProcessorStates); ok {
		if len(sps) == 0 {
			return
		}
		snap := s.snapshot()
		for _, sp := range sps {
			sp.sp.OnEnd(snap)
		}
	}
}

// RecordError will record err as a span event for this span. An additional call to
// SetStatus is required if the Status of the Span should be set to Error, this method
// does not change the Span status. If this span is not being recorded or err is nil
// than this method does nothing.
func (s *recordingSpan) RecordError(err error, opts ...trace.EventOption) {
	if s == nil || err == nil || !s.IsRecording() {
		return
	}

	opts = append(opts, trace.WithAttributes(
		semconv.ExceptionTypeKey.String(typeStr(err)),
		semconv.ExceptionMessageKey.String(err.Error()),
	))

	c := trace.NewEventConfig(opts...)
	if c.StackTrace() {
		opts = append(opts, trace.WithAttributes(
			semconv.ExceptionStacktraceKey.String(recordStackTrace()),
		))
	}

	s.addEvent(semconv.ExceptionEventName, opts...)
}

func typeStr(i interface{}) string {
	t := reflect.TypeOf(i)
	if t.PkgPath() == "" && t.Name() == "" {
		// Likely a builtin type.
		return t.String()
	}
	return fmt.Sprintf("%s.%s", t.PkgPath(), t.Name())
}

func recordStackTrace() string {
	stackTrace := make([]byte, 2048)
	n := runtime.Stack(stackTrace, false)

	return string(stackTrace[0:n])
}

// AddEvent adds an event with the provided name and options. If this span is
// not being recorded than this method does nothing.
func (s *recordingSpan) AddEvent(name string, o ...trace.EventOption) {
	if !s.IsRecording() {
		return
	}
	s.addEvent(name, o...)
}

func (s *recordingSpan) addEvent(name string, o ...trace.EventOption) {
	c := trace.NewEventConfig(o...)

	// Discard over limited attributes
	attributes := c.Attributes()
	var discarded int
	if len(attributes) > s.spanLimits.AttributePerEventCountLimit {
		discarded = len(attributes) - s.spanLimits.AttributePerEventCountLimit
		attributes = attributes[:s.spanLimits.AttributePerEventCountLimit]
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events.add(Event{
		Name:                  name,
		Attributes:            attributes,
		DroppedAttributeCount: discarded,
		Time:                  c.Timestamp(),
	})
}

// SetName sets the name of this span. If this span is not being recorded than
// this method does nothing.
func (s *recordingSpan) SetName(name string) {
	if !s.IsRecording() {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.name = name
}

// Name returns the name of this span.
func (s *recordingSpan) Name() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.name
}

// Name returns the SpanContext of this span's parent span.
func (s *recordingSpan) Parent() trace.SpanContext {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.parent
}

// SpanKind returns the SpanKind of this span.
func (s *recordingSpan) SpanKind() trace.SpanKind {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spanKind
}

// StartTime returns the time this span started.
func (s *recordingSpan) StartTime() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.startTime
}

// EndTime returns the time this span ended. For spans that have not yet
// ended, the returned value will be the zero value of time.Time.
func (s *recordingSpan) EndTime() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.endTime
}

// Attributes returns the attributes of this span.
func (s *recordingSpan) Attributes() []attribute.KeyValue {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attributes.evictList.Len() == 0 {
		return []attribute.KeyValue{}
	}
	return s.attributes.toKeyValue()
}

// Links returns the links of this span.
func (s *recordingSpan) Links() []Link {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.links.queue) == 0 {
		return []Link{}
	}
	return s.interfaceArrayToLinksArray()
}

// Events returns the events of this span.
func (s *recordingSpan) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.events.queue) == 0 {
		return []Event{}
	}
	return s.interfaceArrayToEventArray()
}

// Status returns the status of this span.
func (s *recordingSpan) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

// InstrumentationLibrary returns the instrumentation.Library associated with
// the Tracer that created this span.
func (s *recordingSpan) InstrumentationLibrary() instrumentation.Library {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.instrumentationLibrary
}

// Resource returns the Resource associated with the Tracer that created this
// span.
func (s *recordingSpan) Resource() *resource.Resource {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resource
}

func (s *recordingSpan) addLink(link trace.Link) {
	if !s.IsRecording() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var droppedAttributeCount int

	// Discard over limited attributes
	if len(link.Attributes) > s.spanLimits.AttributePerLinkCountLimit {
		droppedAttributeCount = len(link.Attributes) - s.spanLimits.AttributePerLinkCountLimit
		link.Attributes = link.Attributes[:s.spanLimits.AttributePerLinkCountLimit]
	}

	s.links.add(Link{link.SpanContext, link.Attributes, droppedAttributeCount})
}

// DroppedAttributes returns the number of attributes dropped by the span
// due to limits being reached.
func (s *recordingSpan) DroppedAttributes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attributes.droppedCount
}

// DroppedLinks returns the number of links dropped by the span due to limits
// being reached.
func (s *recordingSpan) DroppedLinks() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.links.droppedCount
}

// DroppedEvents returns the number of events dropped by the span due to
// limits being reached.
func (s *recordingSpan) DroppedEvents() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.events.droppedCount
}

// ChildSpanCount returns the count of spans that consider the span a
// direct parent.
func (s *recordingSpan) ChildSpanCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.childSpanCount
}

// TracerProvider returns a trace.TracerProvider that can be used to generate
// additional Spans on the same telemetry pipeline as the current Span.
func (s *recordingSpan) TracerProvider() trace.TracerProvider {
	return s.tracer.provider
}

// snapshot creates a read-only copy of the current state of the span.
func (s *recordingSpan) snapshot() ReadOnlySpan {
	var sd snapshot
	s.mu.Lock()
	defer s.mu.Unlock()

	sd.endTime = s.endTime
	sd.instrumentationLibrary = s.instrumentationLibrary
	sd.name = s.name
	sd.parent = s.parent
	sd.resource = s.resource
	sd.spanContext = s.spanContext
	sd.spanKind = s.spanKind
	sd.startTime = s.startTime
	sd.status = s.status
	sd.childSpanCount = s.childSpanCount

	if s.attributes.evictList.Len() > 0 {
		sd.attributes = s.attributes.toKeyValue()
		sd.droppedAttributeCount = s.attributes.droppedCount
	}
	if len(s.events.queue) > 0 {
		sd.events = s.interfaceArrayToEventArray()
		sd.droppedEventCount = s.events.droppedCount
	}
	if len(s.links.queue) > 0 {
		sd.links = s.interfaceArrayToLinksArray()
		sd.droppedLinkCount = s.links.droppedCount
	}
	return &sd
}

func (s *recordingSpan) interfaceArrayToLinksArray() []Link {
	linkArr := make([]Link, 0)
	for _, value := range s.links.queue {
		linkArr = append(linkArr, value.(Link))
	}
	return linkArr
}

func (s *recordingSpan) interfaceArrayToEventArray() []Event {
	eventArr := make([]Event, 0)
	for _, value := range s.events.queue {
		eventArr = append(eventArr, value.(Event))
	}
	return eventArr
}

func (s *recordingSpan) copyToCappedAttributes(attributes ...attribute.KeyValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range attributes {
		// Ensure attributes conform to the specification:
		// https://github.com/open-telemetry/opentelemetry-specification/blob/v1.0.1/specification/common/common.md#attributes
		if a.Valid() {
			s.attributes.add(a)
		}
	}
}

func (s *recordingSpan) addChild() {
	if !s.IsRecording() {
		return
	}
	s.mu.Lock()
	s.childSpanCount++
	s.mu.Unlock()
}

func (*recordingSpan) private() {}

// runtimeTrace starts a "runtime/trace".Task for the span and returns a
// context containing the task.
func (s *recordingSpan) runtimeTrace(ctx context.Context) context.Context {
	if !rt.IsEnabled() {
		// Avoid additional overhead if runtime/trace is not enabled.
		return ctx
	}
	nctx, task := rt.NewTask(ctx, s.name)

	s.mu.Lock()
	s.executionTracerTaskEnd = task.End
	s.mu.Unlock()

	return nctx
}

// nonRecordingSpan is a minimal implementation of the OpenTelemetry Span API
// that wraps a SpanContext. It performs no operations other than to return
// the wrapped SpanContext or TracerProvider that created it.
type nonRecordingSpan struct {
	// tracer is the SDK tracer that created this span.
	tracer *tracer
	sc     trace.SpanContext
}

var _ trace.Span = nonRecordingSpan{}

// SpanContext returns the wrapped SpanContext.
func (s nonRecordingSpan) SpanContext() trace.SpanContext { return s.sc }

// IsRecording always returns false.
func (nonRecordingSpan) IsRecording() bool { return false }

// SetStatus does nothing.
func (nonRecordingSpan) SetStatus(codes.Code, string) {}

// SetError does nothing.
func (nonRecordingSpan) SetError(bool) {}

// SetAttributes does nothing.
func (nonRecordingSpan) SetAttributes(...attribute.KeyValue) {}

// End does nothing.
func (nonRecordingSpan) End(...trace.SpanEndOption) {}

// RecordError does nothing.
func (nonRecordingSpan) RecordError(error, ...trace.EventOption) {}

// AddEvent does nothing.
func (nonRecordingSpan) AddEvent(string, ...trace.EventOption) {}

// SetName does nothing.
func (nonRecordingSpan) SetName(string) {}

// TracerProvider returns the trace.TracerProvider that provided the Tracer
// that created this span.
func (s nonRecordingSpan) TracerProvider() trace.TracerProvider { return s.tracer.provider }

func isRecording(s SamplingResult) bool {
	return s.Decision == RecordOnly || s.Decision == RecordAndSample
}

func isSampled(s SamplingResult) bool {
	return s.Decision == RecordAndSample
}

// Status is the classified state of a Span.
type Status struct {
	// Code is an identifier of a Spans state classification.
	Code codes.Code
	// Description is a user hint about why that status was set. It is only
	// applicable when Code is Error.
	Description string
}
