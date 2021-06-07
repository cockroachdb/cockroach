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
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/ring"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/logtags"
	"github.com/gogo/protobuf/types"
	"github.com/opentracing/opentracing-go"
)

// crdbSpan is a span for internal crdb usage. This is used to power SQL session
// tracing.
type crdbSpan struct {
	rootSpan *crdbSpan // root span of the containing trace; could be itself
	// traceEmpty indicates whether or not the trace rooted at this span
	// (provided it is a root span) contains any recordings or baggage. All
	// spans hold a reference to the rootSpan; this field is accessed
	// through that reference.
	traceEmpty int32 // accessed atomically, through markTraceAsNonEmpty and inAnEmptyTrace

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

	mu      crdbSpanMu
	testing *testingKnob
}

type testingKnob struct {
	clock timeutil.TimeSource
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

		// children contains the list of child spans started after this Span
		// started recording.
		children childSpanRefs
		// remoteSpan contains the list of remote child span recordings that
		// were manually imported.
		remoteSpans []tracingpb.RecordedSpan
	}

	// tags are only captured when recording. These are tags that have been
	// added to this Span, and will be appended to the tags in logTags when
	// someone needs to actually observe the total set of tags that is a part of
	// this Span.
	// TODO(radu): perhaps we want a recording to capture all the tags (even
	// those that were set before recording started)?
	tags opentracing.Tags

	// The Span's associated baggage.
	baggage map[string]string
}

type childSpanRefs struct {
	refCount     int
	preAllocated [4]*crdbSpan
	overflow     []*crdbSpan
}

func (c *childSpanRefs) len() int {
	return c.refCount
}

func (c *childSpanRefs) add(ref *crdbSpan) {
	if c.refCount < len(c.preAllocated) {
		c.preAllocated[c.refCount] = ref
		c.refCount++
		return
	}

	// Only record the child if the parent still has room.
	if c.refCount < maxChildrenPerSpan {
		c.overflow = append(c.overflow, ref)
		c.refCount++
	}
}

func (c *childSpanRefs) get(idx int) *crdbSpan {
	if idx < len(c.preAllocated) {
		ref := c.preAllocated[idx]
		if ref == nil {
			panic(fmt.Sprintf("idx %d out of bounds", idx))
		}
		return ref
	}
	return c.overflow[idx-len(c.preAllocated)]
}

func (c *childSpanRefs) reset() {
	for i := 0; i < len(c.preAllocated); i++ {
		c.preAllocated[i] = nil
	}
	c.overflow = nil
	c.refCount = 0
}

func newSizeLimitedBuffer(limit int64) sizeLimitedBuffer {
	return sizeLimitedBuffer{
		limit: limit,
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

func (s *crdbSpan) recordingType() RecordingType {
	if s == nil {
		return RecordingOff
	}
	return s.mu.recording.recordingType.load()
}

// enableRecording start recording on the Span. From now on, log events and
// child spans will be stored.
//
// If parent != nil, the Span will be registered as a child of the respective
// parent. If nil, the parent's recording will not include this child.
func (s *crdbSpan) enableRecording(parent *crdbSpan, recType RecordingType) {
	if parent != nil {
		parent.addChild(s)
	}
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
	s.mu.recording.children.reset()
	s.mu.recording.remoteSpans = nil
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

func (s *crdbSpan) getRecording(everyoneIsV211 bool, wantTags bool) Recording {
	if s == nil {
		return nil // noop span
	}

	if !everyoneIsV211 {
		// The cluster may contain nodes that are running v20.2. Unfortunately that
		// version can easily crash when a peer returns a recording that that node
		// did not expect would get created. To circumvent this, retain the v20.2
		// behavior of eliding recordings when verbosity is off until we're sure
		// that v20.2 is not around any longer.
		//
		// TODO(tbg): remove this in the v21.2 cycle.
		if s.recordingType() == RecordingOff {
			return nil
		}
	}

	// Return early (without allocating) if the trace is empty, i.e. there are
	// no recordings or baggage. If the trace is verbose, we'll still recurse in
	// order to pick up all the operations that were part of the trace, despite
	// nothing having any actual data in them.
	if s.recordingType() != RecordingVerbose && s.inAnEmptyTrace() {
		return nil
	}

	s.mu.Lock()
	// The capacity here is approximate since we don't know how many
	// grandchildren there are.
	result := make(Recording, 0, 1+s.mu.recording.children.len()+len(s.mu.recording.remoteSpans))
	// Shallow-copy the children so we can process them without the lock.
	var children []*crdbSpan
	for i := 0; i < s.mu.recording.children.len(); i++ {
		children = append(children, s.mu.recording.children.get(i))
	}
	result = append(result, s.getRecordingLocked(wantTags))
	result = append(result, s.mu.recording.remoteSpans...)
	s.mu.Unlock()

	for _, child := range children {
		result = append(result, child.getRecording(everyoneIsV211, wantTags)...)
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

func (s *crdbSpan) importRemoteSpans(remoteSpans []tracingpb.RecordedSpan) {
	if len(remoteSpans) == 0 {
		return
	}

	s.markTraceAsNonEmpty()
	// Change the root of the remote recording to be a child of this Span. This is
	// usually already the case, except with DistSQL traces where remote
	// processors run in spans that FollowFrom an RPC Span that we don't collect.
	remoteSpans[0].ParentSpanID = s.spanID

	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.recording.remoteSpans = append(s.mu.recording.remoteSpans, remoteSpans...)
}

func (s *crdbSpan) setTagLocked(key string, value interface{}) {
	if s.recordingType() != RecordingVerbose {
		// Don't bother storing tags if we're unlikely to retrieve them.
		return
	}

	if s.mu.tags == nil {
		s.mu.tags = make(opentracing.Tags)
	}
	s.mu.tags[key] = value
}

func (s *crdbSpan) record(msg string) {
	if s.recordingType() != RecordingVerbose {
		return
	}

	var now time.Time
	if s.testing != nil {
		now = s.testing.clock.Now()
	} else {
		now = time.Now()
	}
	logRecord := &tracingpb.LogRecord{
		Time: now,
		Fields: []tracingpb.LogRecord_Field{
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
	sr := &tracingpb.StructuredRecord{
		Time:    time.Now(),
		Payload: p,
	}
	s.recordInternal(sr, &s.mu.recording.structured)
}

// sizable is a subset for protoutil.Message, for payloads (log records and
// structured events) that can be recorded.
type sizable interface {
	Size() int
}

// inAnEmptyTrace indicates whether or not the containing trace is "empty" (i.e.
// has any recordings or baggage).
func (s *crdbSpan) inAnEmptyTrace() bool {
	val := atomic.LoadInt32(&s.rootSpan.traceEmpty)
	return val == 0
}

func (s *crdbSpan) markTraceAsNonEmpty() {
	atomic.StoreInt32(&s.rootSpan.traceEmpty, 1)
}

func (s *crdbSpan) recordInternal(payload sizable, buffer *sizeLimitedBuffer) {
	s.markTraceAsNonEmpty()
	s.mu.Lock()
	defer s.mu.Unlock()
	size := int64(payload.Size())
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
		first := buffer.GetFirst().(sizable)
		buffer.RemoveFirst()
		buffer.size -= int64(first.Size())
	}
	buffer.AddLast(payload)
}

func (s *crdbSpan) setBaggageItemAndTag(restrictedKey, value string) {
	s.markTraceAsNonEmpty()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setBaggageItemLocked(restrictedKey, value)
	// Don't set the tag if this is the special cased baggage item indicating
	// span verbosity, as it is named nondescriptly and the recording knows
	// how to display its verbosity independently.
	if restrictedKey != verboseTracingBaggageKey {
		s.setTagLocked(restrictedKey, value)
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

// getRecordingLocked returns the Span's recording. This does not include
// children.
//
// When wantTags is false, no tags will be added. This is a performance
// optimization as stringifying the tag values can be expensive.
func (s *crdbSpan) getRecordingLocked(wantTags bool) tracingpb.RecordedSpan {
	rs := tracingpb.RecordedSpan{
		TraceID:      s.traceID,
		SpanID:       s.spanID,
		ParentSpanID: s.parentSpanID,
		GoroutineID:  s.goroutineID,
		Operation:    s.mu.operation,
		StartTime:    s.startTime,
		Duration:     s.mu.duration,
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
		// TODO(adityamaru): Stop writing to DeprecatedInternalStructured in 22.1.
		rs.DeprecatedInternalStructured = make([]*types.Any, numEvents)
		rs.StructuredRecords = make([]tracingpb.StructuredRecord, numEvents)
		for i := 0; i < numEvents; i++ {
			event := s.mu.recording.structured.Get(i).(*tracingpb.StructuredRecord)
			rs.StructuredRecords[i] = *event
			// Write the Structured payload stored in the StructuredRecord, since
			// nodes older than 21.2 expect a Structured event when they fetch
			// recordings.
			rs.DeprecatedInternalStructured[i] = event.Payload
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
		if len(s.mu.tags) > 0 {
			for k, v := range s.mu.tags {
				// We encode the tag values as strings.
				addTag(k, fmt.Sprint(v))
			}
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

	s.mu.recording.children.add(child)
}

// setVerboseRecursively sets the verbosity of the crdbSpan appropriately and
// recurses on its list of children.
func (s *crdbSpan) setVerboseRecursively(to bool) {
	if to {
		s.enableRecording(nil /* parent */, RecordingVerbose)
	} else {
		s.disableRecording()
	}

	s.mu.Lock()
	var children []*crdbSpan
	for i := 0; i < s.mu.recording.children.len(); i++ {
		children = append(children, s.mu.recording.children.get(i))
	}
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
