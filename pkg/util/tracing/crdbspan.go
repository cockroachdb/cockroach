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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/ring"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"github.com/gogo/protobuf/types"
	"go.opentelemetry.io/otel/attribute"
)

// crdbSpan is a span for internal crdb usage. This is used to power SQL session
// tracing.
type crdbSpan struct {
	testing *TracerTestingKnobs

	traceID      uint64 // probabilistically unique
	spanID       uint64 // probabilistically unique
	parentSpanID uint64
	goroutineID  uint64

	startTime time.Time

	// logTags are set to the log tags that were available when this Span was
	// created, so that there's no need to eagerly copy all of those log tags
	// into this Span's tags. If the Span's tags are actually requested, these
	// logTags will be copied out at that point.
	//
	// Note that these tags have not gone through the log tag -> Span tag
	// remapping procedure; tagName() needs to be called before exposing each
	// tag's key to a user.
	logTags *logtags.Buffer

	mu     crdbSpanMu
	parent *crdbSpan
}

type crdbSpanMu struct {
	syncutil.Mutex
	// duration is initialized to -1 and set on Finish().
	duration  time.Duration
	operation string // name of operation associated with the span

	recording struct {
		// recordingType is the recording type of the ongoing recording, if any.
		// Its 'load' method may be called without holding the surrounding mutex,
		// but its 'swap' method requires the mutex.
		recordingType atomicRecordingType

		logs       sizeLimitedBuffer // of *tracingpb.LogRecords
		structured sizeLimitedBuffer // of Structured events

		// dropped is true if the span has capped out it's memory limits for
		// logs and structured events, and has had to drop some. It's used to
		// annotate recordings with the _dropped tag, when applicable.
		dropped bool

		// openChildren contains the list of local child spans started after this
		// Span started recording, that haven't been Finish()ed yet. After Finish(),
		// the respective child moves to finishedChildren.
		openChildren []*crdbSpan
		// finishedChildren contains the recordings of finished children (and
		// grandchildren recursively). This includes remote child span recordings
		// that were manually imported, as well as recordings from local children
		// that Finish()ed.
		finishedChildren []tracingpb.RecordedSpan
	}

	// The Span's associated baggage.
	baggage map[string]string

	// tags are only captured when recording. These are tags that have been
	// added to this Span, and will be appended to the tags in logTags when
	// someone needs to actually observe the total set of tags that is a part of
	// this Span.
	// TODO(radu): perhaps we want a recording to capture all the tags (even
	// those that were set before recording started)?
	tags []attribute.KeyValue
}

// makeSizeLimitedBuffer creates a sizeLimitedBuffer.
//
// scratch, if not nil, represents pre-allocated space that the Buffer takes
// ownership of. The whole backing array of the provided slice is taken over,
// included elements and available capacity.
func makeSizeLimitedBuffer(limit int64, scratch []interface{}) sizeLimitedBuffer {
	return sizeLimitedBuffer{
		limit:  limit,
		Buffer: ring.MakeBuffer(scratch),
	}
}

type sizeLimitedBuffer struct {
	ring.Buffer
	size  int64 // in bytes
	limit int64 // in bytes
}

func (b *sizeLimitedBuffer) Reset() {
	b.Buffer.Reset()
	b.size = 0
}

// finish marks the span as finished. Further operations on the span are not
// allowed. Returns false if the span was already finished.
//
// The intention is for a span to be available for reuse (e.g. through a
// sync.Pool) after finish(), although we're not currently taking advantage of
// this.
func (s *crdbSpan) finish() bool {
	{
		s.mu.Lock()
		if s.mu.duration >= 0 {
			// Already finished.
			s.mu.Unlock()
			return false
		}

		finishTime := timeutil.Now()
		duration := finishTime.Sub(s.startTime)
		if duration == 0 {
			duration = time.Nanosecond
		}
		s.mu.duration = duration

		s.mu.Unlock()
	}

	if s.parent != nil {
		s.parent.childFinished(s)
	}

	// TODO(andrei): All the children that are still open are getting orphaned by
	// the finishing of this parent. We should make them all root spans and
	// register them with the active spans registry.
	return true
}

func (s *crdbSpan) recordingType() RecordingType {
	if s == nil {
		return RecordingOff
	}
	return s.mu.recording.recordingType.load()
}

// enableRecording start recording on the Span. From now on, log events and
// child spans will be stored.
func (s *crdbSpan) enableRecording(recType RecordingType) {
	if recType == RecordingOff || s.recordingType() == recType {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.recording.recordingType.swap(recType)
	if recType == RecordingVerbose {
		s.setBaggageItemLocked(verboseTracingBaggageKey, "1")
	}
}

// resetRecording clears any previously recorded info.
//
// NB: This is needed by SQL SessionTracing, who likes to start and stop
// recording repeatedly on the same Span, and collect the (separate) recordings
// every time.
func (s *crdbSpan) resetRecording() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.recording.logs.Reset()
	s.mu.recording.structured.Reset()
	s.mu.recording.dropped = false
	s.mu.recording.openChildren = s.mu.recording.openChildren[:0]
	s.mu.recording.finishedChildren = nil
}

func (s *crdbSpan) disableRecording() {
	if s.recordingType() == RecordingOff {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	oldRecType := s.mu.recording.recordingType.swap(RecordingOff)
	// We test the duration as a way to check if the Span has been finished. If it
	// has, we don't want to do the call below as it might crash (at least if
	// there's a netTr).
	if (s.mu.duration == -1) && (oldRecType == RecordingVerbose) {
		// Clear the verboseTracingBaggageKey baggage item, assuming that it was set by
		// enableRecording().
		s.setBaggageItemLocked(verboseTracingBaggageKey, "")
	}
}

// getRecording returns the Span's recording, including its children.
//
// When wantTags is false, no tags will be added. This is a performance
// optimization as stringifying the tag values can be expensive.
func (s *crdbSpan) getRecording(wantTags bool) Recording {
	if s == nil {
		return nil // noop span
	}

	s.mu.Lock()
	// The capacity here is approximate since we don't know how many
	// grandchildren there are.
	result := make(Recording, 0, 1+len(s.mu.recording.openChildren)+len(s.mu.recording.finishedChildren))
	result = append(result, s.getRecordingNoChildrenLocked(wantTags))
	result = append(result, s.mu.recording.finishedChildren...)
	children := make([]*crdbSpan, len(s.mu.recording.openChildren))
	copy(children, s.mu.recording.openChildren)
	s.mu.Unlock()

	for _, child := range children {
		result = append(result, child.getRecording(wantTags)...)
	}

	// Sort the spans by StartTime, except the first Span (the root of this
	// recording) which stays in place.
	toSort := sortPool.Get().(*Recording) // avoids allocations in sort.Sort
	*toSort = result[1:]
	sort.Sort(toSort)
	*toSort = nil
	sortPool.Put(toSort)
	return result
}

// recordFinishedChildren adds `children` to the receiver's recording.
func (s *crdbSpan) recordFinishedChildren(children []tracingpb.RecordedSpan) {
	if len(children) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordFinishedChildrenLocked(children)
}

func (s *crdbSpan) recordFinishedChildrenLocked(children []tracingpb.RecordedSpan) {
	if len(children) == 0 {
		return
	}

	// Change the root of the remote recording to be a child of this Span. This is
	// usually already the case, except with DistSQL traces where remote
	// processors run in spans that FollowFrom an RPC Span that we don't collect.
	children[0].ParentSpanID = s.spanID

	s.mu.recording.finishedChildren = append(s.mu.recording.finishedChildren, children...)
}

func (s *crdbSpan) setTagLocked(key string, value attribute.Value) {
	k := attribute.Key(key)
	for i := range s.mu.tags {
		if s.mu.tags[i].Key == k {
			s.mu.tags[i].Value = value
			return
		}
	}
	s.mu.tags = append(s.mu.tags, attribute.KeyValue{Key: k, Value: value})
}

func (s *crdbSpan) record(msg redact.RedactableString) {
	if s.recordingType() != RecordingVerbose {
		return
	}

	var now time.Time
	if clock := s.testing.Clock; clock != nil {
		now = clock.Now()
	} else {
		now = time.Now()
	}
	logRecord := &tracingpb.LogRecord{
		Time:    now,
		Message: msg,
		// Compatibility with 21.2.
		DeprecatedFields: []tracingpb.LogRecord_Field{
			{Key: tracingpb.LogMessageField, Value: msg},
		},
	}

	s.recordInternal(logRecord, &s.mu.recording.logs)
}

func (s *crdbSpan) recordStructured(item Structured) {
	p, err := types.MarshalAny(item)
	if err != nil {
		// An error here is an error from Marshal; these
		// are unlikely to happen.
		return
	}

	var now time.Time
	if clock := s.testing.Clock; clock != nil {
		now = clock.Now()
	} else {
		now = time.Now()
	}
	sr := &tracingpb.StructuredRecord{
		Time:    now,
		Payload: p,
	}
	s.recordInternal(sr, &s.mu.recording.structured)
}

// memorySizable is implemented by log records and structured events for
// exposing their in-memory size. This size is used to put caps on the payloads
// accumulated by a span.
//
// Note that, as opposed to the Size() method implemented by our
// protoutil.Messages, MemorySize() aims to represent the memory footprint, not
// the serialized length.
type memorySizable interface {
	MemorySize() int
}

func (s *crdbSpan) recordInternal(payload memorySizable, buffer *sizeLimitedBuffer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	size := int64(payload.MemorySize())
	if size > buffer.limit {
		// The incoming payload alone blows past the memory limit. Let's just
		// drop it.
		s.mu.recording.dropped = true
		return
	}

	buffer.size += size
	if buffer.size > buffer.limit {
		s.mu.recording.dropped = true
	}
	for buffer.size > buffer.limit {
		first := buffer.GetFirst().(memorySizable)
		buffer.RemoveFirst()
		buffer.size -= int64(first.MemorySize())
	}
	buffer.AddLast(payload)
}

func (s *crdbSpan) setBaggageItemAndTag(restrictedKey, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setBaggageItemLocked(restrictedKey, value)
	// Don't set the tag if this is the special cased baggage item indicating
	// span verbosity, as it is named nondescriptly and the recording knows
	// how to display its verbosity independently.
	if restrictedKey != verboseTracingBaggageKey {
		s.setTagLocked(restrictedKey, attribute.StringValue(value))
	}
}

func (s *crdbSpan) setBaggageItemLocked(restrictedKey, value string) {
	if oldVal, ok := s.mu.baggage[restrictedKey]; ok && oldVal == value {
		// No-op.
		return
	}
	if s.mu.baggage == nil {
		s.mu.baggage = make(map[string]string)
	}
	s.mu.baggage[restrictedKey] = value
}

// getRecordingNoChildrenLocked returns the Span's recording without including
// children.
//
// When wantTags is false, no tags will be added. This is a performance
// optimization as stringifying the tag values can be expensive.
func (s *crdbSpan) getRecordingNoChildrenLocked(wantTags bool) tracingpb.RecordedSpan {
	rs := tracingpb.RecordedSpan{
		TraceID:        s.traceID,
		SpanID:         s.spanID,
		ParentSpanID:   s.parentSpanID,
		GoroutineID:    s.goroutineID,
		Operation:      s.mu.operation,
		StartTime:      s.startTime,
		Duration:       s.mu.duration,
		RedactableLogs: true,
	}

	if rs.Duration == -1 {
		// -1 indicates an unfinished Span. For a recording it's better to put some
		// duration in it, otherwise tools get confused. For example, we export
		// recordings to Jaeger, and spans with a zero duration don't look nice.
		rs.Duration = timeutil.Now().Sub(rs.StartTime)
		rs.Finished = false
	} else {
		rs.Finished = true
	}

	addTag := func(k, v string) {
		if rs.Tags == nil {
			rs.Tags = make(map[string]string)
		}
		rs.Tags[k] = v
	}

	if wantTags {
		if s.mu.duration == -1 {
			addTag("_unfinished", "1")
		}
		if s.mu.recording.recordingType.load() == RecordingVerbose {
			addTag("_verbose", "1")
		}
		if s.mu.recording.dropped {
			addTag("_dropped", "1")
		}
	}

	if numEvents := s.mu.recording.structured.Len(); numEvents != 0 {
		rs.StructuredRecords = make([]tracingpb.StructuredRecord, numEvents)
		for i := 0; i < numEvents; i++ {
			event := s.mu.recording.structured.Get(i).(*tracingpb.StructuredRecord)
			rs.StructuredRecords[i] = *event
		}
	}

	if len(s.mu.baggage) > 0 {
		rs.Baggage = make(map[string]string)
		for k, v := range s.mu.baggage {
			rs.Baggage[k] = v
		}
	}
	if wantTags {
		if s.logTags != nil {
			setLogTags(s.logTags.Get(), func(remappedKey string, tag *logtags.Tag) {
				addTag(remappedKey, tag.ValueStr())
			})
		}
		for _, kv := range s.mu.tags {
			// We encode the tag values as strings.
			addTag(string(kv.Key), kv.Value.Emit())
		}
	}

	if numLogs := s.mu.recording.logs.Len(); numLogs != 0 {
		rs.Logs = make([]tracingpb.LogRecord, numLogs)
		for i := 0; i < numLogs; i++ {
			lr := s.mu.recording.logs.Get(i).(*tracingpb.LogRecord)
			rs.Logs[i] = *lr
		}
	}

	return rs
}

func (s *crdbSpan) addChild(child *crdbSpan) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.mu.recording.openChildren) < maxChildrenPerSpan {
		s.mu.recording.openChildren = append(s.mu.recording.openChildren, child)
	}
}

// childFinished atomically removes a child and replaces it with its recording.
// This allows the child span to be reused (since the parent no longer
// references it).
//
// child is the child span that just finished.
func (s *crdbSpan) childFinished(child *crdbSpan) {
	wantTags := s.recordingType() == RecordingVerbose
	rec := child.getRecording(wantTags)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.recordFinishedChildrenLocked(rec)

	l := len(s.mu.recording.openChildren)
	for i, c := range s.mu.recording.openChildren {
		if c != child {
			continue
		}
		s.mu.recording.openChildren[i] = s.mu.recording.openChildren[l-1]
		s.mu.recording.openChildren[l-1] = nil
		s.mu.recording.openChildren = s.mu.recording.openChildren[:l-1]
		break
	}
}

// setVerboseRecursively sets the verbosity of the crdbSpan appropriately and
// recurses on its list of children.
func (s *crdbSpan) setVerboseRecursively(to bool) {
	if to {
		s.enableRecording(RecordingVerbose)
	} else {
		s.disableRecording()
	}

	s.mu.Lock()
	children := make([]*crdbSpan, len(s.mu.recording.openChildren))
	copy(children, s.mu.recording.openChildren)
	s.mu.Unlock()

	for _, child := range children {
		child.setVerboseRecursively(to)
	}
}

var sortPool = sync.Pool{
	New: func() interface{} {
		return &Recording{}
	},
}

// Less implements sort.Interface.
func (r Recording) Less(i, j int) bool {
	return r[i].StartTime.Before(r[j].StartTime)
}

// Swap implements sort.Interface.
func (r Recording) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

// Len implements sort.Interface.
func (r Recording) Len() int {
	return len(r)
}

type atomicRecordingType RecordingType

// load returns the recording type.
func (art *atomicRecordingType) load() RecordingType {
	return RecordingType(atomic.LoadInt32((*int32)(art)))
}

// swap stores the new recording type and returns the old one.
func (art *atomicRecordingType) swap(recType RecordingType) RecordingType {
	return RecordingType(atomic.SwapInt32((*int32)(art), int32(recType)))
}
