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
	operation    string // name of operation associated with the span

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

type childRef struct {
	*crdbSpan
	// collectRecording is set if this child's recording should be included in the
	// parent's recording. This is usually the case, except for children created
	// with the WithDetachedRecording() option.
	collectRecording bool
}

type crdbSpanMu struct {
	syncutil.Mutex

	// goroutineID is the ID of the goroutine that created this span, or the goroutine that
	// subsequently adopted it through Span.UpdateGoroutineIDToCurrent()).
	goroutineID uint64

	// parent is the span's local parent, if any. parent is not set if the span is
	// a root or the parent span is remote.
	//
	// Note that parent is mutable; a span can start by having a parent but then,
	// if the parent finishes before the child does (which is uncommon), the
	// child's parent is set to nil.
	parent *crdbSpan

	finished bool
	// duration is initialized to -1 and set on Finish().
	duration time.Duration

	// openChildren maintains the list of currently-open local children. These
	// children are part of the active spans registry only indirectly, through
	// this parent. When the parent finishes, any child that's still open will be
	// inserted into the registry directly.
	//
	// If this parent is recording at the time when a child Finish()es, and the
	// respective childRef indicates that the child is to be included in the
	// parent's recording, then the child's recording is collected in
	// recording.finishedChildren.
	//
	// The spans are not maintained in a particular order.
	openChildren []childRef

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
	var needRegistryChange bool
	{
		s.mu.Lock()
		if s.mu.finished {
			// Already finished.
			s.mu.Unlock()
			return false
		}
		s.mu.finished = true
		// If the span is not part of the registry now, it never will be. So, we'll
		// need to remove it from the registry only if it currently does not have a
		// parent. We'll also need to manipulate the registry if there are open
		// children (they'll need to be added to the registry).
		needRegistryChange = s.mu.parent == nil || len(s.mu.openChildren) > 0

		if s.recordingType() != RecordingOff {
			duration := timeutil.Since(s.startTime)
			if duration == 0 {
				duration = time.Nanosecond
			}
			s.mu.duration = duration
		}

		// Shallow-copy the children so they can be processed outside the lock.
		children = make([]*crdbSpan, len(s.mu.openChildren))
		for i, c := range s.mu.openChildren {
			children[i] = c.crdbSpan
		}

		// We'll operate on the parent outside of the child's lock.
		parent = s.mu.parent

		s.mu.Unlock()
	}

	if parent != nil {
		parent.childFinished(s)
	}

	// Deal with the orphaned children - make them roots.
	for _, c := range children {
		c.parentFinished()
	}
	if needRegistryChange {
		// Atomically replace s in the registry with all of its still-open children.
		s.tracer.activeSpansRegistry.swap(s.spanID, children)
	}

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
	return s.getRecordingImpl(recType, false /* includeDetachedChildren */)
}

func (s *crdbSpan) GetFullRecording(recType RecordingType) Recording {
	return s.getRecordingImpl(recType, true /* includeDetachedChildren */)
}

func (s *crdbSpan) getRecordingImpl(recType RecordingType, includeDetachedChildren bool) Recording {
	switch recType {
	case RecordingVerbose:
		return s.getVerboseRecording(includeDetachedChildren)
	case RecordingStructured:
		return s.getStructuredRecording(includeDetachedChildren)
	case RecordingOff:
		return nil
	default:
		panic("unreachable")
	}
}

// getVerboseRecording returns the Span's recording, including its children.
func (s *crdbSpan) getVerboseRecording(includeDetachedChildren bool) Recording {
	if s == nil {
		return nil // noop span
	}

	s.mu.Lock()
	// The capacity here is approximate since we don't know how many
	// grandchildren there are.
	result := make(Recording, 0, 1+len(s.mu.openChildren)+len(s.mu.recording.finishedChildren))
	result = append(result, s.getRecordingNoChildrenLocked(RecordingVerbose))
	result = append(result, s.mu.recording.finishedChildren...)

	for _, child := range s.mu.openChildren {
		if child.collectRecording || includeDetachedChildren {
			result = append(result, child.getVerboseRecording(includeDetachedChildren)...)
		}
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
func (s *crdbSpan) getStructuredRecording(includeDetachedChildren bool) Recording {
	s.mu.Lock()
	defer s.mu.Unlock()
	buffer := make([]*tracingpb.StructuredRecord, 0, 3)
	for _, c := range s.mu.recording.finishedChildren {
		for i := range c.StructuredRecords {
			buffer = append(buffer, &c.StructuredRecords[i])
		}
	}
	for _, c := range s.mu.openChildren {
		if c.collectRecording || includeDetachedChildren {
			buffer = c.getStructuredEventsRecursively(buffer, includeDetachedChildren)
		}
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

// clearTag removes a tag, if it exists.
func (s *crdbSpan) clearTag(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := attribute.Key(key)
	for i := range s.mu.tags {
		if s.mu.tags[i].Key == k {
			s.mu.tags[i] = s.mu.tags[len(s.mu.tags)-1]
			s.mu.tags = s.mu.tags[:len(s.mu.tags)-1]
			return
		}
	}
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
	buffer []*tracingpb.StructuredRecord, includeDetachedChildren bool,
) []*tracingpb.StructuredRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	buffer = s.getStructuredEventsLocked(buffer)
	for _, c := range s.mu.openChildren {
		if c.collectRecording || includeDetachedChildren {
			buffer = c.getStructuredEventsRecursively(buffer, includeDetachedChildren)
		}
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
		GoroutineID:    s.mu.goroutineID,
		Operation:      s.operation,
		StartTime:      s.startTime,
		Duration:       s.mu.duration,
		RedactableLogs: true,
		Verbose:        s.recordingType() == RecordingVerbose,
	}

	if rs.Duration == -1 {
		// -1 indicates an unfinished Span. For a recording it's better to put some
		// duration in it, otherwise tools get confused. For example, we export
		// recordings to Jaeger, and spans with a zero duration don't look nice.
		rs.Duration = time.Since(rs.StartTime)
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
		if !s.mu.finished {
			addTag("_unfinished", "1")
		}
		if s.recordingType() == RecordingVerbose {
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

// addChildLocked adds a child to the receiver.
//
// The receiver's lock must be held.
//
// The adding fails if the receiver has already Finish()ed. This should never
// happen, since using a Span after Finish() is illegal. But still, we
// defensively return false.
func (s *crdbSpan) addChildLocked(child *crdbSpan, collectChildRec bool) bool {
	s.mu.AssertHeld()

	if s.mu.finished {
		return false
	}

	s.mu.openChildren = append(
		s.mu.openChildren,
		childRef{crdbSpan: child, collectRecording: collectChildRec},
	)
	return true
}

// childFinished is called when a child is Finish()ed. Depending on the
// receiver's recording mode, the child is atomically removed and replaced it
// with its recording. This allows the child span to be reused (since the parent
// no longer references it).
//
// child is the child span that just finished.
func (s *crdbSpan) childFinished(child *crdbSpan) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var childIdx int
	found := false
	for i, c := range s.mu.openChildren {
		if c.crdbSpan == child {
			childIdx = i
			found = true
			break
		}
	}
	if !found {
		panic("child not present in parent")
	}

	collectChildRec := s.mu.openChildren[childIdx].collectRecording

	// Unlink the child.
	l := len(s.mu.openChildren)
	s.mu.openChildren[childIdx] = s.mu.openChildren[l-1]
	s.mu.openChildren[l-1].crdbSpan = nil // Make the child available to GC.
	s.mu.openChildren = s.mu.openChildren[:l-1]

	// Collect the child's recording.

	if s.recordingType() == RecordingOff || !collectChildRec {
		return
	}

	var rec Recording
	var events []*tracingpb.StructuredRecord
	var verbose bool
	switch s.recordingType() {
	case RecordingOff:
		panic("should have been handled above")
	case RecordingVerbose:
		rec = child.GetRecording(RecordingVerbose)
		if len(s.mu.recording.finishedChildren)+len(rec) <= maxRecordedSpansPerTrace {
			verbose = true
			break
		}
		// We don't have space for this recording. Let's collect just the structured
		// records by falling through.
		rec = nil
		fallthrough
	case RecordingStructured:
		events = make([]*tracingpb.StructuredRecord, 0, 3)
		events = child.getStructuredEventsRecursively(events, false /* includeDetachedChildren */)
	default:
		panic(fmt.Sprintf("unrecognized recording mode: %v", s.recordingType()))
	}

	if verbose {
		s.recordFinishedChildrenLocked(rec)
	} else {
		for i := range events {
			s.recordInternalLocked(events[i], &s.mu.recording.structured)
		}
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
	for _, child := range s.mu.openChildren {
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

// setGoroutineID updates the span's goroutine ID.
func (s *crdbSpan) setGoroutineID(gid int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.goroutineID = uint64(gid)
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
