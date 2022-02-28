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
	// tracer is the Tracer that created this span.
	tracer *Tracer
	// sp is Span that this crdbSpan is part of.
	sp *Span

	traceID tracingpb.TraceID // probabilistically unique
	spanID  tracingpb.SpanID  // probabilistically unique
	// parentSpanID indicates the parent at the time when this span was created. 0
	// if this span didn't have a parent. If crdbSpan.mu.parent is set,
	// parentSpanID corresponds to it. However, if the parent finishes, or if the
	// parent is a span from a remote node, crdbSpan.mu.parent will be nil.
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

	// Locking rules:
	// - If locking both a parent and a child, the parent must be locked first. In
	// practice, children don't take the parent's lock.
	// - The active spans registry's lock must be acquired before this lock.
	mu struct {
		syncutil.Mutex
		crdbSpanMu
	}
}

type childRef struct {
	spanRef
	// collectRecording is set if this child's recording should be included in the
	// parent's recording. This is usually the case, except for children created
	// with the WithDetachedRecording() option.
	collectRecording bool
}

type crdbSpanMu struct {
	// goroutineID is the ID of the goroutine that created this span, or the goroutine that
	// subsequently adopted it through Span.UpdateGoroutineIDToCurrent()).
	goroutineID uint64

	// parent is the span's local parent, if any. parent is not set if the span is
	// a root or the parent span is remote.
	//
	// Note that parent is mutable; a span can start by having a parent but then,
	// if the parent finishes before the child does (which is uncommon), the
	// child's parent is set to nil.
	//
	// While parent is set, this child is holding a reference in the parent's
	// reference counter. The parent's ref count is decremented when this child
	// Finish()es, or otherwise when this pointer is nil'ed (i.e. on parent
	// Finish()).
	parent spanRef

	// finished is set if finish() was called.
	finished bool
	// finishing is set while finish() is in the process of running. finish() has
	// two separate critical sections, and this finishing field is used to detect
	// when other calls have been interleaved between them.
	finishing bool

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

	recording recordingState

	// tags are only captured when recording. These are tags that have been
	// added to this Span, and will be appended to the tags in logTags when
	// someone needs to actually observe the total set of tags that is a part of
	// this Span.
	// TODO(radu): perhaps we want a recording to capture all the tags (even
	// those that were set before recording started)?
	tags []attribute.KeyValue
}

type recordingState struct {
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

// makeSizeLimitedBuffer creates a sizeLimitedBuffer.
//
// scratch, if not nil, represents pre-allocated space that the Buffer takes
// ownership of. The whole backing array of the provided slice is taken over,
// included elements and available capacity.
func makeSizeLimitedBuffer(limit int64, scratch []interface{}) sizeLimitedBuffer {
	return sizeLimitedBuffer{
		bytesLimit: limit,
		Buffer:     ring.MakeBuffer(scratch),
	}
}

// sizeLimitedBuffer is a wrapper on top of ring.Buffer that keeps track of the
// memory size of its elements.
type sizeLimitedBuffer struct {
	ring.Buffer
	bytesSize  int64
	bytesLimit int64
}

// Discard zeroes out *buf. If nobody else is referencing the backing storage
// for the buffer, or any of the elements, then this makes the backing storage
// is made available for GC.
//
// Note that Discard does not modify the backing storage (i.e. it does not nil
// out the elements). So, if anyone still has a reference to the storage, then
// the elements cannot be GCed.
func (buf *sizeLimitedBuffer) Discard() {
	*buf = sizeLimitedBuffer{}
}

// finish marks the span as finished. Further operations on the span are not
// allowed. Returns false if the span was already finished.
//
// Calling finish() a second time is illegal, as is any use-after-finish().
// Still, the Tracer can be configured to tolerate such uses. If the Tracer was
// configured to not tolerate use-after-Finish, we would have crashed before
// calling this.
func (s *crdbSpan) finish() bool {
	// Finishing involves the following steps:
	// 1) Take the lock and capture a reference to the parent.
	// 2) Operate on the parent outside of the lock.
	// 3) Take the lock again, operate on the children under the lock, and also
	//    capture references to the children for further operations outside of the
	//    lock.
	// 4) Insert the children into the active spans registry outside of the lock.
	//
	// We could reorder things such that the lock is only taken once, but it
	// results in more awkward code because operating on the s' parent expects to
	// find s' children in place, to collect their recordings.

	var parent spanRef
	var hasParent bool
	{
		s.mu.Lock()
		if s.mu.finished {
			// Already finished (or at least in the process of finish()ing). This
			// check ensures that only one caller performs cleanup for this span. We
			// don't want the span to be re-allocated while finish() is running.
			s.mu.Unlock()
			return false
		}
		s.mu.finished = true

		if s.recordingType() != RecordingOff {
			duration := timeutil.Since(s.startTime)
			if duration == 0 {
				duration = time.Nanosecond
			}
			s.mu.duration = duration
		}

		// If there is a parent, we'll operate on the parent below, outside the
		// child's lock, as per the lock ordering convention between parents and
		// children. The parent might get Finish()ed by the time we call
		// parent.childFinished(s) on it below; that's OK because we're going to
		// hold on taking a reference in the parent's reference counter. Notice that
		// we move the reference out of s.mu.parent; leaving it there would not work
		// because s.mu.parent can be released by s.parentFinished() after we drop
		// our lock.
		//
		// If there is no parent, we avoid releasing and then immediately
		// re-acquiring the lock, as a performance optimization.
		parent = s.mu.parent.move()
		hasParent = !parent.empty()
		if hasParent {
			s.mu.finishing = true
			s.mu.Unlock()
		}
	}

	// Operate on the parent outside the child (our current receiver) lock.
	// childFinished() might call back into the child (`s`) and acquire the
	// child's lock.
	if hasParent {
		// It's possible to race with parent.Finish(); if we lose the race, the
		// parent will not have any record of this child. childFinished() deals with
		// that possibility.
		parent.Span.i.crdb.childFinished(s)
		parent.release()
	}

	// Operate on children.
	var children []spanRef
	var needRegistryChange bool
	{
		// Re-acquire the lock if we dropped it above.
		if hasParent {
			s.mu.Lock()
			s.mu.finishing = false
		}

		// If the span was not part of the registry the first time the lock was
		// acquired, above, it never will be (because we marked it as finished). So,
		// we'll need to remove it from the registry only if it currently does not
		// have a parent. We'll also need to manipulate the registry if there are
		// open children (they'll need to be added to the registry).
		needRegistryChange = !hasParent || len(s.mu.openChildren) > 0

		// Deal with the orphaned children - make them roots. We call into the
		// children while holding the parent's lock. As per the span locking
		// convention, that's OK (but the reverse isn't).
		//
		// We also shallow-copy the children for operating on them outside the lock.
		children = make([]spanRef, len(s.mu.openChildren))
		for i := range s.mu.openChildren {
			c := &s.mu.openChildren[i]
			c.parentFinished()
			// Move ownership of the child reference, and also nil out the pointer to
			// the child, making it available for GC.
			children[i] = c.spanRef.move()
		}
		s.mu.openChildren = nil // The children were moved away.
		s.mu.Unlock()
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

// TraceID is part of the RegistrySpan interface.
func (s *crdbSpan) TraceID() tracingpb.TraceID {
	return s.traceID
}

func (s *crdbSpan) SpanID() tracingpb.SpanID {
	return s.spanID
}

// GetRecording returns the span's recording.
//
// finishing indicates whether s is in the process of finishing. If it isn't,
// the recording will include an "_unfinished" tag.
func (s *crdbSpan) GetRecording(recType RecordingType, finishing bool) Recording {
	return s.getRecordingImpl(recType, false /* includeDetachedChildren */, finishing)
}

// GetFullRecording is part of the RegistrySpan interface.
func (s *crdbSpan) GetFullRecording(recType RecordingType) Recording {
	return s.getRecordingImpl(recType, true /* includeDetachedChildren */, false /* finishing */)
}

// getRecordingImpl returns the span's recording.
//
// finishing indicates whether s is in the process of finishing. If it isn't,
// the recording will include an "_unfinished" tag.
func (s *crdbSpan) getRecordingImpl(
	recType RecordingType, includeDetachedChildren bool, finishing bool,
) Recording {
	switch recType {
	case RecordingVerbose:
		return s.getVerboseRecording(includeDetachedChildren, finishing)
	case RecordingStructured:
		return s.getStructuredRecording(includeDetachedChildren)
	case RecordingOff:
		return nil
	default:
		panic("unreachable")
	}
}

// getVerboseRecording returns the Span's recording, including its children.
//
// finishing indicates whether s is in the process of finishing. If it isn't,
// the recording will include an "_unfinished" tag.
func (s *crdbSpan) getVerboseRecording(includeDetachedChildren bool, finishing bool) Recording {
	if s == nil {
		return nil // noop span
	}

	s.mu.Lock()
	// The capacity here is approximate since we don't know how many
	// grandchildren there are.
	result := make(Recording, 0, 1+len(s.mu.openChildren)+len(s.mu.recording.finishedChildren))
	result = append(result, s.getRecordingNoChildrenLocked(RecordingVerbose, finishing))
	result = append(result, s.mu.recording.finishedChildren...)

	for _, child := range s.mu.openChildren {
		if child.collectRecording || includeDetachedChildren {
			sp := child.Span.i.crdb
			result = append(result, sp.getVerboseRecording(includeDetachedChildren, false /* finishing */)...)
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
			sp := c.Span.i.crdb
			buffer = sp.getStructuredEventsRecursively(buffer, includeDetachedChildren)
		}
	}

	if len(buffer) == 0 && s.mu.recording.structured.Len() == 0 {
		// Optimize out the allocations below.
		return nil
	}

	res := s.getRecordingNoChildrenLocked(
		RecordingStructured,
		false, // finishing - since we're only asking for the structured recording, the argument doesn't matter
	)
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

// recordFinishedChildren adds children to s' recording.
//
// s takes ownership of children; the caller is not allowed to use them anymore.
func (s *crdbSpan) recordFinishedChildren(children []tracingpb.RecordedSpan) {
	if len(children) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordFinishedChildrenLocked(children)
}

// s takes ownership of children; the caller is not allowed to use them anymore.
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
		for ci := range children {
			child := &children[ci]
			for i := range child.StructuredRecords {
				s.recordInternalLocked(&child.StructuredRecords[i], &s.mu.recording.structured)
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
	if size > buffer.bytesLimit {
		// The incoming payload alone blows past the memory limit. Let's just
		// drop it.
		s.mu.recording.dropped = true
		return
	}

	buffer.bytesSize += size
	if buffer.bytesSize > buffer.bytesLimit {
		s.mu.recording.dropped = true
	}
	for buffer.bytesSize > buffer.bytesLimit {
		first := buffer.GetFirst().(memorySizable)
		buffer.RemoveFirst()
		buffer.bytesSize -= int64(first.MemorySize())
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
			sp := c.Span.i.crdb
			buffer = sp.getStructuredEventsRecursively(buffer, includeDetachedChildren)
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
//
// finishing indicates whether s is in the process of finishing. If it isn't,
// the recording will include an "_unfinished" tag.
func (s *crdbSpan) getRecordingNoChildrenLocked(
	recordingType RecordingType, finishing bool,
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
		if !finishing && !s.mu.finished {
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

// addChildLocked adds a child to the receiver.
//
// The receiver's lock must be held.
//
// The adding fails if the receiver has already Finish()ed. This should never
// happen, since using a Span after Finish() is illegal. But still, we
// defensively return false.
func (s *crdbSpan) addChildLocked(child *Span, collectChildRec bool) bool {
	s.mu.AssertHeld()

	if s.mu.finished {
		return false
	}

	s.mu.openChildren = append(
		s.mu.openChildren,
		childRef{spanRef: makeSpanRef(child), collectRecording: collectChildRec},
	)
	return true
}

// childFinished is called when a child is Finish()ed. Depending on the
// receiver's recording mode, the child is atomically removed and replaced it
// with its recording.
//
// child is the child span that just finished.
//
// This method can be called while the parent (s) is still alive, is in the
// process of finishing (i.e. s.finish() is in the middle section where it has
// dropped the lock), or has completed finishing. In the first two cases, it is
// expected that child is part of s.mu.openChildren. In the last case, this will
// be a no-op.
func (s *crdbSpan) childFinished(child *crdbSpan) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var childIdx int
	found := false
	for i, c := range s.mu.openChildren {
		sp := c.Span.i.crdb
		if sp == child {
			childIdx = i
			found = true
			break
		}
	}
	if !found {
		// We haven't found the child, so the child must be calling into the parent
		// (s) after s.finish() completed. This can happen if finishing the child
		// races with finishing the parent (if s.finish() would have ran to
		// completion before c.finish() was called, c wouldn't have called
		// s.childFinished(c) because it would no longer have had a parent).
		if !s.mu.finished {
			panic("unexpectedly failed to find child of non-finished parent")
		}
		// Since s has been finished, there's nothing more to do.
		return
	}

	// Sanity check the situations when this method is called after s.finish() was
	// called. Running this after s.finish() was called indicates that s.finish()
	// and child.finish() were called concurrently - if s.finish() would have
	// finished before child.finish() was called, the child would not have had a
	// parent to call childFinished() on. Here, we assert that we understand the
	// races that can happen, as this concurrency is subtle.
	// - s.childFinished(c) can be called after s.finish() completed, in which
	//   case `found` will be false above, and we won't get here.
	// - s.childFinished(c) can also be called after s.finish() started, but
	//   before s.finish() completed. s.finish() has two critical sections, and
	//   drops its lock in between them.
	// This is the only case where the child is found, and we explicitly assert
	// that here.
	if s.mu.finished {
		if !s.mu.finishing {
			panic("child unexpectedly calling into finished parent")
		}
	}

	collectChildRec := s.mu.openChildren[childIdx].collectRecording
	// Drop the child's reference.
	if s.mu.openChildren[childIdx].decRef() {
		// We're going to use the child below, so we don't want it to be
		// re-allocated yet. It shouldn't be re-allocated, because each span holds a
		// reference to itself that's only dropped at the end of Span.Finish() (and
		// the child at this point is in the middle of its Finish() call).
		panic(fmt.Sprintf("span's reference count unexpectedly dropped to zero: %s", child.operation))
	}

	// Unlink the child.
	l := len(s.mu.openChildren)
	s.mu.openChildren[childIdx] = s.mu.openChildren[l-1]
	s.mu.openChildren[l-1] = childRef{}
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
		rec = child.GetRecording(RecordingVerbose, false /* finishing - the child is already finished */)
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
	s.mu.parent.release()
}

// SetRecordingType is part of the RegistrySpan interface.
func (s *crdbSpan) SetRecordingType(to RecordingType) {
	s.mu.recording.recordingType.swap(to)

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, child := range s.mu.openChildren {
		child.SetRecordingType(to)
	}
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
