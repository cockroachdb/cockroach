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
	"github.com/cockroachdb/redact"
	"github.com/gogo/protobuf/types"
	"go.opentelemetry.io/otel/attribute"
)

// crdbSpan is a span for internal crdb usage. This is used to power SQL session
// tracing.
type crdbSpan struct {
	tracer *Tracer

	traceID      tracingpb.TraceID // probabilistically unique
	spanID       tracingpb.SpanID  // probabilistically unique
	parentSpanID tracingpb.SpanID
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

	// Locking rules: if locking both a parent and a child, the parent must be
	// locked first. In practice, children don't take the parent's lock.
	mu crdbSpanMu
}

type crdbSpanMu struct {
	syncutil.Mutex

	// parent if set, is the span's local parent. parent is not always set:
	// - not set if the span is a root or the parent is remote
	// - not set if the parent wasn't recording at the time when the child was
	//   created
	//
	// Note that parent is mutable; a span can start by having a parent but then,
	// if the parent finishes before the child does (which is uncommon), the
	// child's parent is set to nil.
	parent *crdbSpan

	finished bool
	// duration is initialized to -1 and set on Finish().
	duration  time.Duration
	operation string // name of operation associated with the span

	recording struct {
		// recordingType is the recording type of the ongoing recording, if any.
		// Its 'load' method may be called without holding the surrounding mutex,
		// but its 'swap' method requires the mutex.
		recordingType atomicRecordingType

		logs sizeLimitedBuffer // of *tracingpb.LogRecords
		// structured accumulates StructuredRecord's. It will contain the events
		// recorded on this span, and also the ones recorded on children that
		// finished while this parent span's recording was not verbose.
		structured sizeLimitedBuffer

		// dropped is true if the span has capped out it's memory limits for
		// logs and structured events, and has had to drop some. It's used to
		// annotate recordings with the _dropped tag, when applicable.
		dropped bool

		// openChildren contains the list of local child spans started after this
		// Span started recording, that haven't been Finish()ed yet. After Finish(),
		// the respective child moves to finishedChildren.
		//
		// The spans are not maintained in a particular order.
		openChildren []*crdbSpan
		// finishedChildren contains the recordings of finished children (and
		// grandchildren recursively). This includes remote child span recordings
		// that were manually imported, as well as recordings from local children
		// that Finish()ed.
		//
		// Only child spans that finished while this span was in the
		// RecordingVerbose mode are included here. For children finished while this
		// span is not in RecordingVerbose, only their structured events are copied
		// to structured above.
		//
		// The spans are not maintained in a particular order.
		finishedChildren []tracingpb.RecordedSpan
	}

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
// TODO(andrei): The intention is for a span to be available for reuse (e.g.
// through a sync.Pool) after finish(), although we're not currently taking
// advantage of this. I think there might be users that collect a span's
// recording after finish(), which should be illegal. For now, use-after-finish
// is generally tolerated - and that's also why this method returns false when
// called a second time.
func (s *crdbSpan) finish() bool {
	var children []*crdbSpan
	var parent *crdbSpan
	{
		s.mu.Lock()
		if s.mu.finished {
			// Already finished.
			s.mu.Unlock()
			return false
		}
		s.mu.finished = true

		finishTime := timeutil.Now()
		duration := finishTime.Sub(s.startTime)
		if duration == 0 {
			duration = time.Nanosecond
		}
		s.mu.duration = duration

		// Shallow-copy the children so they can be processed outside the lock.
		children = make([]*crdbSpan, len(s.mu.recording.openChildren))
		copy(children, s.mu.recording.openChildren)

		// We'll operate on the parent outside of the child's lock.
		parent = s.mu.parent

		s.mu.Unlock()
	}

	if parent != nil {
		parent.childFinished(s)
	}

	for _, c := range children {
		c.parentFinished()
	}

	// Atomically replace s in the registry with all of its still-open children.
	s.tracer.activeSpansRegistry.swap(s.spanID, children)

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
	s.mu.recording.finishedChildren = s.mu.recording.finishedChildren[:0]
}

func (s *crdbSpan) disableRecording() {
	if s.recordingType() == RecordingOff {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.recording.recordingType.swap(RecordingOff)
}

// TraceID is part of the RegistrySpan interface.
func (s *crdbSpan) TraceID() tracingpb.TraceID {
	return s.traceID
}

// GetRecording is part of the RegistrySpan interface.
func (s *crdbSpan) GetRecording(recType RecordingType) Recording {
	switch recType {
	case RecordingVerbose:
		return s.getVerboseRecording()
	case RecordingStructured:
		return s.getStructuredRecording()
	case RecordingOff:
		return nil
	default:
		panic("unreachable")
	}
}

// getVerboseRecording returns the Span's recording, including its children.
func (s *crdbSpan) getVerboseRecording() Recording {
	if s == nil {
		return nil // noop span
	}

	s.mu.Lock()
	// The capacity here is approximate since we don't know how many
	// grandchildren there are.
	result := make(Recording, 0, 1+len(s.mu.recording.openChildren)+len(s.mu.recording.finishedChildren))
	result = append(result, s.getRecordingNoChildrenLocked(RecordingVerbose))
	result = append(result, s.mu.recording.finishedChildren...)

	for _, child := range s.mu.recording.openChildren {
		result = append(result, child.getVerboseRecording()...)
	}
	s.mu.Unlock()

	// Sort the spans by StartTime, except the first Span (the root of this
	// recording) which stays in place.
	toSort := sortPool.Get().(*Recording) // avoids allocations in sort.Sort
	*toSort = result[1:]
	sort.Sort(toSort)
	*toSort = nil
	sortPool.Put(toSort)
	return result
}

// getStructuredRecording returns the structured events in this span and
// in all the children. The results are returned as a Recording for the caller's
// convenience (and for optimizing memory allocations). The Recording will be
// nil if there are no structured events. If not nil, the Recording will have
// exactly one span corresponding to the receiver, will all events handing from
// this span (even if the events had been recorded on different spans).
//
// The caller does not take ownership of the events.
func (s *crdbSpan) getStructuredRecording() Recording {
	s.mu.Lock()
	defer s.mu.Unlock()
	buffer := make([]*tracingpb.StructuredRecord, 0, 3)
	for _, c := range s.mu.recording.finishedChildren {
		for i := range c.StructuredRecords {
			buffer = append(buffer, &c.StructuredRecords[i])
		}
	}
	for _, c := range s.mu.recording.openChildren {
		buffer = c.getStructuredEventsRecursively(buffer)
	}

	if len(buffer) == 0 && s.mu.recording.structured.Len() == 0 {
		// Optimize out the allocations below.
		return nil
	}

	res := s.getRecordingNoChildrenLocked(RecordingOff)
	// If necessary, grow res.StructuredRecords to have space for buffer.
	var reservedSpace []tracingpb.StructuredRecord
	if cap(res.StructuredRecords)-len(res.StructuredRecords) < len(buffer) {
		// res.StructuredRecords does not have enough capacity to accommodate the
		// elements of buffer. We allocate a new, larger array and copy over the old
		// entries.
		old := res.StructuredRecords
		res.StructuredRecords = make([]tracingpb.StructuredRecord, len(old)+len(buffer))
		copy(res.StructuredRecords, old)
		reservedSpace = res.StructuredRecords[len(old):]
	} else {
		// res.StructuredRecords has enough capacity for buffer. We extend it in
		// place.
		oldLen := len(res.StructuredRecords)
		res.StructuredRecords = res.StructuredRecords[:oldLen+len(buffer)]
		reservedSpace = res.StructuredRecords[oldLen:]
	}
	for i, e := range buffer {
		reservedSpace[i] = *e
	}
	return Recording{res}
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

	// Depending on the type of recording, we either keep all the information
	// received, or only the structured events.
	if s.recordingType() == RecordingVerbose {
		// Change the root of the remote recording to be a child of this Span. This is
		// usually already the case, except with DistSQL traces where remote
		// processors run in spans that FollowFrom an RPC Span that we don't collect.
		children[0].ParentSpanID = s.spanID

		s.mu.recording.finishedChildren = append(s.mu.recording.finishedChildren, children...)
	} else {
		for _, c := range children {
			for _, e := range c.StructuredRecords {
				s.recordInternalLocked(&e, &s.mu.recording.structured)
			}
		}
	}
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

// record includes a log message in s' recording.
func (s *crdbSpan) record(msg redact.RedactableString) {
	if s.recordingType() != RecordingVerbose {
		return
	}

	var now time.Time
	if clock := s.tracer.testing.Clock; clock != nil {
		now = clock.Now()
	} else {
		now = timeutil.Now()
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

// recordStructured includes a structured event in s' recording.
func (s *crdbSpan) recordStructured(item Structured) {
	if s.recordingType() == RecordingOff {
		return
	}

	p, err := types.MarshalAny(item)
	if err != nil {
		// An error here is an error from Marshal; these
		// are unlikely to happen.
		return
	}

	var now time.Time
	if clock := s.tracer.testing.Clock; clock != nil {
		now = clock.Now()
	} else {
		now = timeutil.Now()
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
	s.recordInternalLocked(payload, buffer)
}

func (s *crdbSpan) recordInternalLocked(payload memorySizable, buffer *sizeLimitedBuffer) {
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

// getStructuredEventsRecursively returns the structured events accumulated by
// this span and its finished and still-open children.
func (s *crdbSpan) getStructuredEventsRecursively(
	buffer []*tracingpb.StructuredRecord,
) []*tracingpb.StructuredRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	buffer = s.getStructuredEventsLocked(buffer)
	for _, c := range s.mu.recording.openChildren {
		buffer = c.getStructuredEventsRecursively(buffer)
	}
	for _, c := range s.mu.recording.finishedChildren {
		for i := range c.StructuredRecords {
			buffer = append(buffer, &c.StructuredRecords[i])
		}
	}
	return buffer
}

func (s *crdbSpan) getStructuredEventsLocked(
	buffer []*tracingpb.StructuredRecord,
) []*tracingpb.StructuredRecord {
	numEvents := s.mu.recording.structured.Len()
	for i := 0; i < numEvents; i++ {
		event := s.mu.recording.structured.Get(i).(*tracingpb.StructuredRecord)
		buffer = append(buffer, event)
	}
	return buffer
}

// getRecordingNoChildrenLocked returns the Span's recording without including
// children.
//
// The tags are included in the result only if recordingType==RecordingVerbose.
// This is a performance optimization as stringifying the tag values can be
// expensive.
func (s *crdbSpan) getRecordingNoChildrenLocked(
	recordingType RecordingType,
) tracingpb.RecordedSpan {
	rs := tracingpb.RecordedSpan{
		TraceID:        s.traceID,
		SpanID:         s.spanID,
		ParentSpanID:   s.parentSpanID,
		GoroutineID:    s.goroutineID,
		Operation:      s.mu.operation,
		StartTime:      s.startTime,
		Duration:       s.mu.duration,
		RedactableLogs: true,
		Verbose:        s.recordingType() == RecordingVerbose,
	}

	if rs.Duration == -1 {
		// -1 indicates an unfinished Span. For a recording it's better to put some
		// duration in it, otherwise tools get confused. For example, we export
		// recordings to Jaeger, and spans with a zero duration don't look nice.
		rs.Duration = timeutil.Since(rs.StartTime)
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

	// If the span is not verbose, optimize by avoiding the tags.
	// This span is likely only used to carry payloads around.
	//
	// TODO(andrei): The optimization for avoiding the tags was done back when
	// stringifying a {NodeID,StoreID}Container (a very common tag) was expensive.
	// That has become cheap since, so this optimization might not be worth it any
	// more.
	wantTags := recordingType == RecordingVerbose
	if wantTags {
		if s.mu.duration == -1 {
			addTag("_unfinished", "1")
		}
		addTag("_verbose", "1")
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
	if s.recordingType() == RecordingOff {
		// We're not recording; there's nothing to do. The caller also checks the
		// recording status, but we want to also check it here under the lock
		// (double-checked locking).
		return
	}

	if len(s.mu.recording.openChildren) < maxChildrenPerSpan {
		s.mu.recording.openChildren = append(s.mu.recording.openChildren, child)
	}
}

// childFinished is called when a child is Finish()ed. Depending on the
// receiver's recording mode, the child is atomically removed and replaced it
// with its recording. This allows the child span to be reused (since the parent
// no longer references it).
//
// child is the child span that just finished.
//
// This is only called if the respective child had been linked to the parent -
// i.e. only if the parent was recording when the child started.
func (s *crdbSpan) childFinished(child *crdbSpan) {
	// Collect the recording outside of s' lock, to avoid locking the parent and
	// the child at the same time (to not have to think about deadlocks).
	var rec Recording
	var events []*tracingpb.StructuredRecord
	var verbose bool
	var structured bool
	switch s.recordingType() {
	case RecordingOff:
	case RecordingVerbose:
		verbose = true
		rec = child.GetRecording(RecordingVerbose)
	case RecordingStructured:
		structured = true
		events = make([]*tracingpb.StructuredRecord, 0, 3)
		events = child.getStructuredEventsRecursively(events)
	default:
		panic(fmt.Sprintf("unrecognized recording mode: %v", s.recordingType()))
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if verbose {
		s.recordFinishedChildrenLocked(rec)
	} else if structured {
		for i := range events {
			s.recordInternalLocked(events[i], &s.mu.recording.structured)
		}
	}

	// Unlink the child.
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

// parentFinished makes s a root.
func (s *crdbSpan) parentFinished() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.finished {
		return
	}
	s.mu.parent = nil
}

// SetVerbose is part of the RegistrySpan interface.
func (s *crdbSpan) SetVerbose(to bool) {
	if to {
		s.enableRecording(RecordingVerbose)
	} else {
		s.disableRecording()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, child := range s.mu.recording.openChildren {
		child.SetVerbose(to)
	}

	// TODO(andrei): The children that have started while this span was not
	// recording are not linked into openChildren. The children that are still
	// open can be found through the registry, so we could go spelunking in there
	// and link them into the parent.
}

// withLock calls f while holding s' lock.
func (s *crdbSpan) withLock(f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f()
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
