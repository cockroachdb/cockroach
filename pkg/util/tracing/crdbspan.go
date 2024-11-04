// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracing

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
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

	// eventListeners is a list of registered EventListener's that are notified
	// whenever a Structured event is recorded by the span and its children.
	eventListeners []EventListener

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
	_spanRef spanRef
	// collectRecording is set if this child's recording should be included in the
	// parent's recording. This is usually the case, except for children created
	// with the WithDetachedRecording() option.
	collectRecording bool
}

func (c childRef) span() *crdbSpan {
	return c._spanRef.i.crdb
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

	// tags are a list of key/value pairs associated with the span through
	// SetTag(). They will be appended to the tags in logTags when someone needs
	// to actually observe the total set of tags that is a part of this Span.
	tags []attribute.KeyValue
	// lazyTags are tags whose values are only string-ified on demand. Each lazy
	// tag is expected to implement either fmt.Stringer or LazyTag.
	lazyTags []lazyTag
}

type lazyTag struct {
	Key   string
	Value interface{}
}

type recordingState struct {
	// recordingType is the recording type of the ongoing recording, if any.
	// Its 'load' method may be called without holding the surrounding mutex,
	// but its 'swap' method requires the mutex.
	recordingType atomicRecordingType

	logs sizeLimitedBuffer[*tracingpb.LogRecord]
	// structured accumulates StructuredRecord's.
	//
	// Note that structured events that originally belonged to child spans but
	// bubbled up to this parent in various ways (e.g. children finished while
	// this span was not recording verbosely, or children that were dropped from a
	// verbose recording because of the span limit) are not part of this buffer;
	// they're in finishedChildren.Root.StructuredRecords.
	structured sizeLimitedBuffer[*tracingpb.StructuredRecord]

	// notifyParentOnStructuredEvent is true if the span's parent has asked to be
	// notified of every StructuredEvent recording on this span.
	notifyParentOnStructuredEvent bool

	// droppedLogs is set if the span has capped out its memory limits for logs
	// and structured events, and had to drop some. It's used to annotate
	// recordings with the _dropped_logs tag, when applicable.
	//
	// NOTE: The _dropped_logs tag applies exclusively to logs directly pertaining
	// to this span. It does not apply to structured logs belonging to child spans
	// that were dropped because of the limit on the number of spans in the
	// recording. We move the structured logs from such spans into the parent if
	// there's room. If there isn't, they are silently dropped.
	droppedLogs bool

	// finishedChildren contains the recordings of finished children (and
	// grandchildren recursively). This includes remote child span recordings
	// that were manually imported, as well as recordings from local children
	// that Finish()ed.
	//
	// Only child spans that finished while this span was in the RecordingVerbose
	// mode are included here. Structured events from children finished while this
	// parent was in RecordingStructured mode are stored
	// finishedChildren.Root.Structured.
	//
	// It's convenient to store finishedChildren as a Trace, even though the
	// finishedChildren.Root will only be initialized when the span finishes
	// (other that Root.Structured, which can accumulate events from children).
	finishedChildren Trace

	// childrenMetadata is a mapping from operation to the aggregated metadata of
	// that operation.
	//
	// When a child of this span is Finish()ed, it updates the map with all the
	// children in its Recording. childrenMetadata therefore provides a bucketed
	// view of the various operations that are being traced as part of a span.
	childrenMetadata map[string]tracingpb.OperationMetadata
}

// makeSizeLimitedBuffer creates a sizeLimitedBuffer.
//
// scratch, if not nil, represents pre-allocated space that the Buffer takes
// ownership of. The whole backing array of the provided slice is taken over,
// included elements and available capacity.
func makeSizeLimitedBuffer[T any](limit int64, scratch []T) sizeLimitedBuffer[T] {
	return sizeLimitedBuffer[T]{
		bytesLimit: limit,
		Buffer:     ring.MakeBuffer(scratch),
	}
}

// sizeLimitedBuffer is a wrapper on top of ring.Buffer that keeps track of the
// memory size of its elements.
type sizeLimitedBuffer[T any] struct {
	ring.Buffer[T]
	bytesSize  int64
	bytesLimit int64
}

// Trace represents the recording of a span and all its descendents.
type Trace struct {
	Root tracingpb.RecordedSpan
	// Children are the traces of the child spans. The slice is kept sorted by
	// child start time.
	//
	// Children are added via addChildren(). Once a child is added to a Trace, the
	// Trace takes ownership; only the Trace can modify that child, since it needs
	// to maintain the NumSpans and StructuredRecordsSizeBytes bookkeeping.
	Children []Trace
	// NumSpans tracks the number of spans in the recording: 1 for the root plus
	// the size of the child traces recursively.
	NumSpans int
	// StructuredRecordsSizeBytes tracks the total size of structured logs in Root
	// and all the Children, recursively.
	StructuredRecordsSizeBytes int64

	// DroppedDirectChildren maintains info about whether any direct children were
	// omitted from Children because of recording limits.
	//
	// When set, DroppedDirectChildren and DroppedIndirectChildren are also
	// reflected in corresponding tags on Root.
	DroppedDirectChildren bool
	// DroppedIndirectChildren maintains info about whether any indirect children
	// were omitted from Children because of recording limits.
	DroppedIndirectChildren bool
}

// MakeTrace constructs a Trace.
func MakeTrace(root tracingpb.RecordedSpan) Trace {
	return Trace{
		Root:                       root,
		NumSpans:                   1,
		StructuredRecordsSizeBytes: root.StructuredRecordsSizeBytes,
	}
}

func (t *Trace) String() string {
	return tracingpb.Recording(t.Flatten()).String()
}

// trimSpans reduces the size of the trace to maxSpans. If t.NumSpans <= maxSpans,
// this is a no-op. Otherwise, spans will be dropped from the trace until the
// size is exactly maxSpans.
//
// Structured events from the dropped spans are moved to the first non-dropped
// parent. Note that trimStructuredEvents() can be used to reduce the size of
// events.
func (t *Trace) trimSpans(maxSpans int) {
	if t.NumSpans <= maxSpans {
		return
	}
	t.trimSpansRecursive(t.NumSpans-maxSpans, true /* isRoot */)
}

func (t *Trace) trimSpansRecursive(toDrop int, isRoot bool) {
	if toDrop <= 0 {
		toDrop := toDrop // copy escaping to the heap
		panic(errors.AssertionFailedf("invalid toDrop < 0: %d", toDrop))
	}
	if t.NumSpans <= toDrop {
		panic(errors.AssertionFailedf("NumSpans expected to be > toDrop; NumSpans: %d, toDrop: %d", t.NumSpans, toDrop))
	}

	// Look at the spans ordered by size descendingly, so that we drop the fewest children
	// possible.
	childrenIdx := make([]int, len(t.Children))
	for i := range t.Children {
		childrenIdx[i] = i
	}
	sort.Slice(childrenIdx, func(i, j int) bool {
		return t.Children[childrenIdx[i]].NumSpans >= t.Children[childrenIdx[j]].NumSpans
	})
	// We'll eliminate some children completely (the largest ones), and we'll
	// recurse into the next one.
	// Figure out how many spans we're going to drop completely (we might not have
	// to drop any).
	spansToDrop := 0
	total := 0
	recurseIdx := -1
	for _, idx := range childrenIdx {
		if total+t.Children[idx].NumSpans <= toDrop {
			spansToDrop++
			total += t.Children[idx].NumSpans
		} else {
			break
		}
	}
	// If we need to drop some more spans, but we don't have to completely drop
	// the next fattest child, recurse into the next child.
	toDropFromNextChild := toDrop - total
	if toDropFromNextChild > 0 {
		// Note: we know that childrenIdx[spansToDrop] is not out of bounds; if
		// toDropFromNextChild > 0, there must be at least one more child after
		// spansToDrop.
		recurseIdx = childrenIdx[spansToDrop]
		if t.Children[recurseIdx].NumSpans < toDropFromNextChild {
			panic(errors.AssertionFailedf("expected next child to have enough spans"))
		}
		t.Children[recurseIdx].trimSpansRecursive(toDropFromNextChild, false /* isRoot */)
		t.NumSpans -= toDropFromNextChild
		t.DroppedIndirectChildren = true
		if isRoot {
			// The original recursive root Trace `t`'s Root member is expected to have independently allocated
			// tags structures. However, child spans are potentially share memory for tags, logs, and stats
			// information across goroutines which are not protected by a mutex. See (*Trace).PartialClone for
			// more details - deep copying these structures is avoided to reduce allocations.
			//
			// To avoid multiple goroutines racing to add tags to child spans with unprotected tags structures,
			// we only indicate on the recursive root Trace's Root member that indirect children have been dropped.
			t.Root.EnsureTagGroup(tracingpb.AnonymousTagGroupName).AddTag("_dropped_indirect_children", "")
		}
	}

	if spansToDrop > 0 {
		t.DroppedDirectChildren = true
		if isRoot {
			// The original recursive root Trace `t`'s Root member is expected to have independently allocated
			// tags structures. However, child spans are potentially share memory for tags, logs, and stats
			// information across goroutines which are not protected by a mutex. See (*Trace).PartialClone for
			// more details - deep copying these structures is avoided to reduce allocations.
			//
			// To avoid multiple goroutines racing to add tags to child spans with unprotected tags structures,
			// we only indicate on the recursive root Trace's Root member that children have been dropped.
			t.Root.EnsureTagGroup(tracingpb.AnonymousTagGroupName).AddTag("_dropped_children", "")
		}
		// We're going to drop the fattest spansToDrop spans.
		childrenToDropIdx := childrenIdx[:spansToDrop]

		// Sort the indexes of the children to drop ascendingly, so that we can
		// remove them easily while maintaining the existing order for the children
		// that stay.
		sort.Ints(childrenToDropIdx)
		newChildren := make([]Trace, 0, len(t.Children)-spansToDrop)
		j := 0 // This will iterate over childrenToDropIdx
		for i := range t.Children {
			if j < len(childrenToDropIdx) && i == childrenToDropIdx[j] {
				// We need to drop this child.
				j++
				// Copy the structured events from the dropped child to the parent.
				// Note that t.StructuredRecordsSizeBytes doesn't change.
				buf := t.Children[i].appendStructuredEventsRecursively(nil /* buffer */)
				for i := range buf {
					t.Root.AddStructuredRecord(buf[i])
				}
			} else {
				// This child is not dropped; copy it over to newChildren.
				newChildren = append(newChildren, t.Children[i])
			}
		}
		t.Children = newChildren
		t.NumSpans -= total
	}
}

// trimStructuredEvents drops structured records as needed in order to make the total
// size of events in t <= maxBytes.
//
// The method works recursively. Events are dropped first from the spans with
// the largest events size.
func (t *Trace) trimStructuredEvents(maxBytes int64) int64 {
	toDrop := t.StructuredRecordsSizeBytes - maxBytes
	if toDrop <= 0 {
		return 0
	}

	// Look at the spans ordered by structured size descendingly; we'll drop
	// events from the fattest child first.
	childrenIdx := make([]int, len(t.Children)+1)
	for i := range t.Children {
		childrenIdx[i] = i
	}
	childrenIdx[len(t.Children)] = -1 // Represent the root.
	sort.Slice(childrenIdx, func(i, j int) bool {
		var leftSize, rightSize int64
		if childrenIdx[i] == -1 {
			leftSize = t.Root.StructuredRecordsSizeBytes
		} else {
			leftSize = t.Children[childrenIdx[i]].StructuredRecordsSizeBytes
		}
		if childrenIdx[j] == -1 {
			rightSize = t.Root.StructuredRecordsSizeBytes
		} else {
			rightSize = t.Children[childrenIdx[j]].StructuredRecordsSizeBytes
		}
		return leftSize >= rightSize
	})

	droppedTotal := int64(0)
	for _, idx := range childrenIdx {
		var dropped int64
		if idx == -1 {
			// This is the root.
			dropped = t.Root.TrimStructured(t.Root.StructuredRecordsSizeBytes - toDrop)
		} else {
			child := &t.Children[idx]
			dropped = child.trimStructuredEvents(child.StructuredRecordsSizeBytes - toDrop)
		}
		droppedTotal += dropped
		toDrop -= dropped
		if toDrop <= 0 {
			break
		}
	}

	t.StructuredRecordsSizeBytes -= droppedTotal
	return droppedTotal
}

// addChildren adds child traces to t. After adding the children, the trace is
// trimmed to maxSpans (if 0, no trimming occurs). If spans are dropped because
// of the maxSpans limit, the structured messages from dropped spans are copied
// into their parents. However, the total size of structured logs across t is
// limited to maxStructuredBytes (if not zero). If records need to be dropped
// because of this limit, they're dropped from the span with the largest size
// first.
//
// The list of children is kept sorted.
func (t *Trace) addChildren(children []Trace, maxSpans int, maxStructuredBytes int64) {

	// Figure out if we'll need to re-sort the children. We won't need to do it if
	// we're adding a single child that belongs in the last position.
	needSort := false
	if len(t.Children) > 0 {
		needSort = !(len(children) == 1 &&
			children[0].Root.StartTime.After(
				t.Children[len(t.Children)-1].Root.StartTime))
	}
	for i := range children {
		c := &children[i]
		t.NumSpans += c.NumSpans
		t.StructuredRecordsSizeBytes += c.StructuredRecordsSizeBytes
		t.Children = append(t.Children, *c)
	}
	if needSort {
		t.sortChildren()
	}
	if maxSpans > 0 {
		t.trimSpans(maxSpans)
	}
	if maxStructuredBytes > 0 {
		t.trimStructuredEvents(maxStructuredBytes)
	}
}

// sortChildren sorts the children in the trace by start time.
func (t *Trace) sortChildren() {
	toSort := sortPoolTraces.Get().(*[]Trace) // avoids allocations in sort.Sort
	*toSort = t.Children
	sort.Slice(*toSort, func(i, j int) bool {
		return (*toSort)[i].Root.StartTime.Before((*toSort)[j].Root.StartTime)
	})
	*toSort = nil
	sortPoolTraces.Put(toSort)
}

var sortPoolTraces = sync.Pool{
	New: func() interface{} {
		return &[]Trace{}
	},
}

// Empty returns true if the receiver is not initialized.
func (t *Trace) Empty() bool {
	return len(t.Children) == 0 && t.Root.StartTime == time.Time{}
}

func (t *Trace) appendStructuredEventsRecursively(
	buffer []tracingpb.StructuredRecord,
) []tracingpb.StructuredRecord {
	buffer = append(buffer, t.Root.StructuredRecords...)
	for i := range t.Children {
		buffer = t.Children[i].appendStructuredEventsRecursively(buffer)
	}
	return buffer
}

func (t *Trace) appendSpansRecursively(buffer []tracingpb.RecordedSpan) []tracingpb.RecordedSpan {
	buffer = append(buffer, t.Root)
	for i := range t.Children {
		buffer = t.Children[i].appendSpansRecursively(buffer)
	}
	return buffer
}

// Flatten flattens the trace into a slice of spans. The root is the first span,
// and parents come before children. Otherwise, the spans are not sorted.
//
// See SortSpans() for sorting the result in order to turn it into a
// tracingpb.Recording.
func (t *Trace) Flatten() []tracingpb.RecordedSpan {
	if t.Empty() {
		return nil
	}
	return t.appendSpansRecursively(nil /* buffer */)
}

// ToRecording converts the Trace to a tracingpb.Recording by flattening it and
// sorting the spans.
func (t *Trace) ToRecording() tracingpb.Recording {
	spans := t.Flatten()
	// sortSpans sorts the spans by StartTime, except the first Span (the root of
	// this recording) which stays in place.
	toSort := sortPoolRecordings.Get().(*tracingpb.Recording) // avoids allocations in sort.Sort
	*toSort = spans[1:]
	sort.Sort(toSort)
	*toSort = nil
	sortPoolRecordings.Put(toSort)
	return spans
}

var sortPoolRecordings = sync.Pool{
	New: func() interface{} {
		return &tracingpb.Recording{}
	},
}

// PartialClone performs a deep copy of the trace. The immutable slices are not
// copied: logs, tags and stats.
func (t *Trace) PartialClone() Trace {
	r := *t
	// The structured logs need copying since the slice is mutable: new logs can
	// be added to the source when spans are added to the trace but the trace is
	// over the span limit. Technically, simply adding to the slice is safe if we
	// made a shallow copy of the slice, but we're being defensive here as we
	// might go beyond simply adding logs in the future.
	r.Root.StructuredRecords = make([]tracingpb.StructuredRecord, len(t.Root.StructuredRecords))
	copy(r.Root.StructuredRecords, t.Root.StructuredRecords)
	r.Children = make([]Trace, len(t.Children))
	for i := range t.Children {
		r.Children[i] = t.Children[i].PartialClone()
	}
	return r
}

// Discard zeroes out *buf. If nobody else is referencing the backing storage
// for the buffer, or any of the elements, then this makes the backing storage
// is made available for GC.
//
// Note that Discard does not modify the backing storage (i.e. it does not nil
// out the elements). So, if anyone still has a reference to the storage, then
// the elements cannot be GCed.
func (buf *sizeLimitedBuffer[T]) Discard() {
	*buf = sizeLimitedBuffer[T]{}
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
		if s.recordingType() != tracingpb.RecordingOff {
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
			c.span().parentFinished()
			// Move ownership of the child reference, and also nil out the pointer to
			// the child, making it available for GC.
			children[i] = c._spanRef.move()
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

func (s *crdbSpan) recordingType() tracingpb.RecordingType {
	if s == nil {
		return tracingpb.RecordingOff
	}
	return s.mu.recording.recordingType.load()
}

// TraceID is part of the RegistrySpan interface.
func (s *crdbSpan) TraceID() tracingpb.TraceID {
	return s.traceID
}

// SpanID is part of the RegistrySpan interface.
func (s *crdbSpan) SpanID() tracingpb.SpanID {
	return s.spanID
}

// GetRecording returns the span's recording.
//
// finishing indicates whether s is in the process of finishing. If it isn't,
// the recording will include an "_unfinished" tag.
func (s *crdbSpan) GetRecording(recType tracingpb.RecordingType, finishing bool) Trace {
	return s.getRecordingImpl(recType, false /* includeDetachedChildren */, finishing)
}

// GetFullRecording is part of the RegistrySpan interface.
func (s *crdbSpan) GetFullRecording(recType tracingpb.RecordingType) Trace {
	return s.getRecordingImpl(recType, true /* includeDetachedChildren */, false /* finishing */)
}

// getRecordingImpl returns the span's recording.
//
// finishing indicates whether s is in the process of finishing. If it isn't,
// the recording will include an "_unfinished" tag.
func (s *crdbSpan) getRecordingImpl(
	recType tracingpb.RecordingType, includeDetachedChildren bool, finishing bool,
) Trace {
	switch recType {
	case tracingpb.RecordingVerbose:
		return s.getVerboseRecording(includeDetachedChildren, finishing)
	case tracingpb.RecordingStructured:
		return MakeTrace(s.getStructuredRecording(includeDetachedChildren))
	case tracingpb.RecordingOff:
		return Trace{}
	default:
		panic(errors.AssertionFailedf("unreachable"))
	}
}

// rollupChildrenMetadata combines the OperationMetadata in `from` into `to`.
func rollupChildrenMetadata(
	to map[string]tracingpb.OperationMetadata, from map[string]tracingpb.OperationMetadata,
) {
	for op, metadata := range from {
		to[op] = to[op].Combine(metadata)
	}
}

// getVerboseRecording returns the Span's recording, including its children.
//
// Each RecordedSpan in the Recording contains the ChildrenMetadata of all the
// children, both finished and open, in the spans' subtree.
//
// finishing indicates whether s is in the process of finishing. If it isn't,
// the recording will include an "_unfinished" tag.
func (s *crdbSpan) getVerboseRecording(includeDetachedChildren bool, finishing bool) Trace {
	if s == nil {
		return Trace{} // noop span
	}

	var result Trace
	var childrenMetadata map[string]tracingpb.OperationMetadata
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Make a clone of the finished children, to avoid working on and returning
		// a trace that aliases s.mu.recording.finishedChildren. We're going to
		// modify the trace below, when adding open children to it, and
		// s.mu.recording.finishedChildren will also evolve after we return the
		// copy, so we can't allow for aliases.
		result = s.mu.recording.finishedChildren.PartialClone()
		// result.Root.StructuredRecords might have accumulated entries from spans
		// that were trimmed from finishedChildren. Save them so we can re-insert
		// them below.
		oldEvents := result.Root.StructuredRecords
		result.StructuredRecordsSizeBytes -= result.Root.StructuredRecordsSizeBytes
		result.Root = s.getRecordingNoChildrenLocked(tracingpb.RecordingVerbose, finishing)
		result.StructuredRecordsSizeBytes += result.Root.StructuredRecordsSizeBytes
		for i := range oldEvents {
			size := int64(oldEvents[i].Size())
			if result.StructuredRecordsSizeBytes+size <= maxStructuredBytesPerTrace {
				result.Root.AddStructuredRecord(oldEvents[i])
				result.StructuredRecordsSizeBytes += size
			}
		}

		// Copy over the OperationMetadata collected from s' finished children.
		childrenMetadata = make(map[string]tracingpb.OperationMetadata)
		rollupChildrenMetadata(childrenMetadata, s.mu.recording.childrenMetadata)

		// We recurse on s' open children to get their verbose recordings, and to
		// aggregate OperationMetadata from their children, both finished and open.
		now := s.tracer.now()
		openRecordings := make([]Trace, 0, len(s.mu.openChildren))
		for _, openChild := range s.mu.openChildren {
			if openChild.collectRecording || includeDetachedChildren {
				openChildSp := openChild.span()
				openChildRecording := openChildSp.getVerboseRecording(includeDetachedChildren, false /* finishing */)
				openRecordings = append(openRecordings, openChildRecording)

				// Record an entry for openChilds' OperationMetadata.
				op := openChildSp.operation
				meta := childrenMetadata[op]
				meta.Count++
				meta.ContainsUnfinished = true
				meta.Duration += now.Sub(openChildSp.startTime)
				childrenMetadata[op] = meta

				// Copy over the OperationMetadata collected recursively from openChilds'
				// children.
				rollupChildrenMetadata(childrenMetadata, openChildRecording.Root.ChildrenMetadata)
			}
		}
		result.addChildren(openRecordings, maxRecordedSpansPerTrace, maxStructuredBytesPerTrace)
	}()

	// Copy over the OperationMetadata collected from s' children into the root of
	// the recording.
	if len(childrenMetadata) != 0 {
		result.Root.ChildrenMetadata = childrenMetadata
	}

	return result
}

// getStructuredRecording returns a shallow copy of the structured events in
// this span and in all the children. The returned span will contain all
// structured events across the receiver and all its children. The returned span
// will also have its `childrenMetadata` populated with data for all the
// children.
//
// The caller does not take ownership of the events; the event payloads must be
// treated as immutable since they're shared with the receiver.
func (s *crdbSpan) getStructuredRecording(includeDetachedChildren bool) tracingpb.RecordedSpan {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := s.getRecordingNoChildrenLocked(
		tracingpb.RecordingStructured,
		false, // finishing - since we're only asking for the structured recording, the argument doesn't matter
	)

	// Wipe the root's records. We're going to re-add them.
	res.StructuredRecordsSizeBytes = 0
	res.StructuredRecords = res.StructuredRecords[:0]
	// Recursively fetch the StructuredEvents for s' and its children, both
	// finished and open.
	buf := s.appendStructuredEventsRecursivelyLocked(res.StructuredRecords, includeDetachedChildren)
	for i := range buf {
		res.AddStructuredRecord(buf[i])
	}

	// Recursively fetch the OperationMetadata for s' children, both finished and
	// open.
	res.ChildrenMetadata = make(map[string]tracingpb.OperationMetadata)
	s.getChildrenMetadataRecursivelyLocked(res.ChildrenMetadata,
		false /* includeRootMetadata */, includeDetachedChildren)

	return res
}

// recordFinishedChildren adds the spans in childRecording to s' recording.
//
// s takes ownership of childRecording; the caller is not allowed to use them anymore.
func (s *crdbSpan) recordFinishedChildren(childRecording Trace) {
	if childRecording.Empty() {
		return
	}

	// Notify the event listeners registered with s of the StructuredEvents on the
	// children being added to s.
	events := childRecording.appendStructuredEventsRecursively(nil)
	for _, record := range events {
		var d types.DynamicAny
		if err := types.UnmarshalAny(record.Payload, &d); err != nil {
			continue
		}
		s.notifyEventListeners(d.Message.(protoutil.Message))
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordFinishedChildrenLocked(childRecording)
}

// s takes ownership of childRec; the caller is not allowed to use them
// anymore.
func (s *crdbSpan) recordFinishedChildrenLocked(childRec Trace) {
	if childRec.Empty() {
		return
	}

	// Depending on the type of recording, we either keep all the information
	// received, or only the structured events.
	switch s.recordingType() {
	case tracingpb.RecordingVerbose:
		// Change the root of the recording to be a child of this Span. This is
		// usually already the case, except with DistSQL traces where remote
		// processors run in spans that FollowFrom an RPC Span that we don't
		// collect.
		childRec.Root.ParentSpanID = s.spanID
		s.mu.recording.finishedChildren.addChildren([]Trace{childRec}, maxRecordedSpansPerTrace, maxStructuredBytesPerTrace)
	case tracingpb.RecordingStructured:
		fc := &s.mu.recording.finishedChildren
		num := len(fc.Root.StructuredRecords)
		fc.Root.StructuredRecords = childRec.appendStructuredEventsRecursively(fc.Root.StructuredRecords)
		// Account for the size of the structured records that were appended,
		// breaking out of the loop if we hit the byte limit. This incorporates
		// the byte size accounting logic from RecordedSpan.AddStructuredRecord.
		for ; num < len(fc.Root.StructuredRecords); num++ {
			size := int64(fc.Root.StructuredRecords[num].MemorySize())
			if fc.StructuredRecordsSizeBytes+size > maxStructuredBytesPerTrace {
				break
			}
			fc.Root.StructuredRecordsSizeBytes += size
			fc.StructuredRecordsSizeBytes += size
		}
		// Trim any remaining entries if we hit the byte limit.
		for i := num; i < len(fc.Root.StructuredRecords); i++ {
			fc.Root.StructuredRecords[i] = tracingpb.StructuredRecord{}
		}
		fc.Root.StructuredRecords = fc.Root.StructuredRecords[:num]
	case tracingpb.RecordingOff:
		break
	default:
		panic(errors.AssertionFailedf("unrecognized recording mode: %v", s.recordingType()))
	}

	// Update s' ChildrenMetadata to capture all the spans in childRec.
	//
	// As an example where we are done finishing `child`:
	//
	// parent
	//   child
	//     grandchild
	//
	// `parent` will have:
	// {child: 2s, grandchild: 1s}
	//
	// Record finished rootChilds' metadata.
	s.mu.recording.childrenMetadata[childRec.Root.Operation] =
		s.mu.recording.childrenMetadata[childRec.Root.Operation].Combine(
			tracingpb.OperationMetadata{
				Count:              1,
				Duration:           childRec.Root.Duration,
				ContainsUnfinished: false,
			})

	// Record the metadata of rootChilds' children, both finished and open.
	//
	// GetRecording(...) is responsible for recursively capturing the metadata for
	// rootChilds' open and finished children.
	rollupChildrenMetadata(s.mu.recording.childrenMetadata, childRec.Root.ChildrenMetadata)
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

// setLazyTagLocked sets a tag that's only stringified if s' recording is
// collected.
//
// value is expected to implement either Stringer or LazyTag.
//
// key is expected to not match the key of a non-lazy tag.
func (s *crdbSpan) setLazyTagLocked(key string, value interface{}) {
	for i := range s.mu.lazyTags {
		if s.mu.lazyTags[i].Key == key {
			s.mu.lazyTags[i].Value = value
			return
		}
	}
	s.mu.lazyTags = append(s.mu.lazyTags, lazyTag{Key: key, Value: value})
}

// getLazyTagLocked returns the value of the tag with the given key. If that tag
// doesn't exist, the bool retval is false.
func (s *crdbSpan) getLazyTagLocked(key string) (interface{}, bool) {
	for i := range s.mu.lazyTags {
		if s.mu.lazyTags[i].Key == key {
			return s.mu.lazyTags[i].Value, true
		}
	}
	return nil, false
}

// notifyEventListeners recursively notifies all the EventListeners registered
// with this span and any ancestor spans in the Recording, of a StructuredEvent.
//
// This span is notified _before_ its ancestors.
//
// If any of the span's EventListeners return EventConsumed status, then the
// ancestors are **not** notified about this Structured item.
//
// If s has a parent, then we notify the parent of the StructuredEvent outside
// the child (our current receiver) lock. This is as per the lock ordering
// convention between parents and children.
func (s *crdbSpan) notifyEventListeners(item Structured) {
	s.mu.Lock()
	var unlocked bool
	defer func() {
		if !unlocked {
			s.mu.Unlock()
		}
	}()

	// Check if the span has been finished concurrently with this notify call.
	// This can happen when the signal comes from a child span; in that case the
	// child calls into the parent without holding the child's lock, so the call
	// can race with parent.Finish().
	if s.mu.finished {
		return
	}

	// Notify s' eventListeners first, before passing the event to parent.
	for _, listener := range s.eventListeners {
		if listener.Notify(item) == EventConsumed {
			return
		}
	}

	if s.mu.recording.notifyParentOnStructuredEvent {
		parent := s.mu.parent.Span.i.crdb
		// Take a reference of s' parent before releasing the mutex. This ensures
		// that if the parent were to be Finish()ed concurrently then the span does
		// not get reused until we release the reference.
		parentRef := makeSpanRef(s.mu.parent.Span)
		defer parentRef.release()
		s.mu.Unlock()
		unlocked = true
		parent.notifyEventListeners(item)
	}
}

// record includes a log message in s' recording.
func (s *crdbSpan) record(msg redact.RedactableString) {
	if s.recordingType() != tracingpb.RecordingVerbose {
		return
	}

	recordInternal(s, &tracingpb.LogRecord{
		Time:    s.tracer.now(),
		Message: msg,
	}, &s.mu.recording.logs)
}

// recordStructured includes a structured event in s' recording.
func (s *crdbSpan) recordStructured(item Structured) {
	if s.recordingType() == tracingpb.RecordingOff {
		return
	}

	p, err := types.MarshalAny(item)
	if err != nil {
		// An error here is an error from Marshal; these
		// are unlikely to happen.
		return
	}

	sr := &tracingpb.StructuredRecord{
		Time:    s.tracer.now(),
		Payload: p,
	}
	recordInternal(s, sr, &s.mu.recording.structured)

	// If there are any listener's registered with this span, notify them of the
	// Structured event being recorded.
	s.notifyEventListeners(item)
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

func recordInternal[PL memorySizable](s *crdbSpan, payload PL, buffer *sizeLimitedBuffer[PL]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	size := int64(payload.MemorySize())
	if size > buffer.bytesLimit {
		// The incoming payload alone blows past the memory limit. Let's just
		// drop it.
		s.mu.recording.droppedLogs = true
		return
	}

	buffer.bytesSize += size
	if buffer.bytesSize > buffer.bytesLimit {
		s.mu.recording.droppedLogs = true
	}
	for buffer.bytesSize > buffer.bytesLimit {
		first := buffer.GetFirst()
		buffer.RemoveFirst()
		buffer.bytesSize -= int64(first.MemorySize())
	}
	buffer.AddLast(payload)
}

// appendStructuredEventsRecursivelyLocked appends the structured events
// accumulated by s' and its finished and still-open children to buffer, and
// returns the resulting slice.
func (s *crdbSpan) appendStructuredEventsRecursivelyLocked(
	buffer []tracingpb.StructuredRecord, includeDetachedChildren bool,
) []tracingpb.StructuredRecord {
	buffer = s.appendStructuredEventsLocked(buffer)
	for _, c := range s.mu.openChildren {
		if c.collectRecording || includeDetachedChildren {
			sp := c.span()
			buffer = func() []tracingpb.StructuredRecord {
				sp.mu.Lock()
				defer sp.mu.Unlock()
				return sp.appendStructuredEventsRecursivelyLocked(buffer, includeDetachedChildren)
			}()
		}
	}
	return s.mu.recording.finishedChildren.appendStructuredEventsRecursively(buffer)
}

// getChildrenMetadataRecursivelyLocked populates `childrenMetadata` with
// OperationMetadata entries for all of s' children (open and finished),
// recursively.
//
// The method also populates `childrenMetadata` with an entry for the receiver
// if `includeRootMetadata` is true.
func (s *crdbSpan) getChildrenMetadataRecursivelyLocked(
	childrenMetadata map[string]tracingpb.OperationMetadata,
	includeRootMetadata, includeDetachedChildren bool,
) {
	if includeRootMetadata {
		// Record an entry for s' metadata.
		prevMetadata := childrenMetadata[s.operation]
		prevMetadata.Count++
		if s.mu.duration == -1 {
			prevMetadata.Duration += timeutil.Since(s.startTime)
			prevMetadata.ContainsUnfinished = true
		} else {
			prevMetadata.Duration += s.mu.duration
		}
		childrenMetadata[s.operation] = prevMetadata
	}

	// Copy over s' Finish()ed children metadata.
	rollupChildrenMetadata(childrenMetadata, s.mu.recording.childrenMetadata)

	// For each of s' open children, recurse to collect their metadata.
	for _, c := range s.mu.openChildren {
		if c.collectRecording || includeDetachedChildren {
			sp := c.span()
			func() {
				sp.mu.Lock()
				defer sp.mu.Unlock()
				sp.getChildrenMetadataRecursivelyLocked(childrenMetadata,
					true /*includeRootMetadata */, includeDetachedChildren)
			}()
		}
	}
}

func (s *crdbSpan) appendStructuredEventsLocked(
	buffer []tracingpb.StructuredRecord,
) []tracingpb.StructuredRecord {
	numEvents := s.mu.recording.structured.Len()
	for i := 0; i < numEvents; i++ {
		event := s.mu.recording.structured.Get(i)
		buffer = append(buffer, *event)
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
	recordingType tracingpb.RecordingType, finishing bool,
) tracingpb.RecordedSpan {
	rs := tracingpb.RecordedSpan{
		TraceID:       s.traceID,
		SpanID:        s.spanID,
		ParentSpanID:  s.parentSpanID,
		GoroutineID:   s.mu.goroutineID,
		Operation:     s.operation,
		StartTime:     s.startTime,
		Duration:      s.mu.duration,
		Verbose:       s.recordingType() == tracingpb.RecordingVerbose,
		RecordingMode: s.recordingType().ToProto(),
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

	// If the span is not verbose, optimize by avoiding the tags.
	// This span is likely only used to carry payloads around.
	//
	// TODO(andrei): The optimization for avoiding the tags was done back when
	// stringifying a {NodeID,StoreID}Container (a very common tag) was expensive.
	// That has become cheap since, so this optimization might not be worth it any
	// more.
	wantTags := recordingType == tracingpb.RecordingVerbose
	if wantTags {
		if !finishing && !s.mu.finished {
			rs.EnsureTagGroup(tracingpb.AnonymousTagGroupName).AddTag("_unfinished", "1")
		}
		if s.recordingType() == tracingpb.RecordingVerbose {
			rs.EnsureTagGroup(tracingpb.AnonymousTagGroupName).AddTag("_verbose", "1")
		}
		if s.mu.recording.droppedLogs {
			rs.EnsureTagGroup(tracingpb.AnonymousTagGroupName).AddTag("_dropped_logs", "")
		}
		if s.mu.recording.finishedChildren.DroppedDirectChildren {
			rs.EnsureTagGroup(tracingpb.AnonymousTagGroupName).AddTag("_dropped_children", "")
		}
		if s.mu.recording.finishedChildren.DroppedIndirectChildren {
			rs.EnsureTagGroup(tracingpb.AnonymousTagGroupName).AddTag("_dropped_indirect_children", "")
		}
	}

	if numEvents := s.mu.recording.structured.Len(); numEvents != 0 {
		rs.StructuredRecords = make([]tracingpb.StructuredRecord, 0, numEvents)
		for i := 0; i < numEvents; i++ {
			event := s.mu.recording.structured.Get(i)
			rs.AddStructuredRecord(*event)
		}
	}

	if wantTags {
		if s.logTags != nil {
			setLogTags(s.logTags.Get(), func(remappedKey string, tag *logtags.Tag) {
				rs.EnsureTagGroup(tracingpb.AnonymousTagGroupName).AddTag(remappedKey, tag.ValueStr())
			})
		}
		for _, kv := range s.mu.tags {
			// We encode the tag values as strings.
			rs.EnsureTagGroup(tracingpb.AnonymousTagGroupName).AddTag(string(kv.Key), kv.Value.Emit())
		}
		for _, tg := range s.getLazyTagGroupsLocked() {
			if tg.Name == tracingpb.AnonymousTagGroupName {
				for _, tag := range tg.Tags {
					rs.EnsureTagGroup(tracingpb.AnonymousTagGroupName).AddTag(tag.Key, tag.Value)
				}
			} else {
				rs.TagGroups = append(rs.TagGroups, *tg)
			}
		}

		// Pull anonymous group to the front.
		for i := range rs.TagGroups {
			if rs.TagGroups[i].Name == tracingpb.AnonymousTagGroupName {
				rs.TagGroups[0], rs.TagGroups[i] = rs.TagGroups[i], rs.TagGroups[0]
			}
		}
	}

	if numLogs := s.mu.recording.logs.Len(); numLogs != 0 {
		rs.Logs = make([]tracingpb.LogRecord, numLogs)
		for i := 0; i < numLogs; i++ {
			lr := s.mu.recording.logs.Get(i)
			rs.Logs[i] = *lr
		}
	}

	return rs
}

// getLazyTagGroupsLocked returns a list of lazy tags as a slice of
// *tracingpb.TagGroup
func (s *crdbSpan) getLazyTagGroupsLocked() []*tracingpb.TagGroup {
	var lazyTags []*tracingpb.TagGroup
	addTagGroup := func(name string) *tracingpb.TagGroup {
		lazyTags = append(lazyTags,
			&tracingpb.TagGroup{
				Name: name,
			})
		return lazyTags[len(lazyTags)-1]
	}

	addTag := func(k, v string) {
		var tagGroup *tracingpb.TagGroup
		for _, tg := range lazyTags {
			if tg.Name == tracingpb.AnonymousTagGroupName {
				tagGroup = tg
				break
			}
		}
		if tagGroup == nil {
			tagGroup = addTagGroup(tracingpb.AnonymousTagGroupName)
		}
		tagGroup.Tags = append(tagGroup.Tags, tracingpb.Tag{
			Key:   k,
			Value: v,
		})
	}
	for _, kv := range s.mu.lazyTags {
		switch v := kv.Value.(type) {
		case LazyTag:
			var tagGroup *tracingpb.TagGroup
			for _, tag := range v.Render() {
				if tagGroup == nil {
					// Only create the tag group if we have at least one child tag.
					tagGroup = addTagGroup(kv.Key)
				}
				tagGroup.Tags = append(tagGroup.Tags,
					tracingpb.Tag{
						Key:   string(tag.Key),
						Value: tag.Value.Emit(),
					},
				)
			}
		case fmt.Stringer:
			addTag(kv.Key, v.String())
		default:
			addTag(kv.Key, fmt.Sprintf("<can't render %T>", kv.Value))
		}
	}
	return lazyTags
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
		childRef{_spanRef: makeSpanRef(child), collectRecording: collectChildRec},
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
		sp := c.span()
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
			panic(errors.AssertionFailedf("unexpectedly failed to find child of non-finished parent"))
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
			panic(errors.AssertionFailedf("child unexpectedly calling into finished parent"))
		}
	}

	collectChildRec := s.mu.openChildren[childIdx].collectRecording
	// Drop the child's reference.
	if s.mu.openChildren[childIdx]._spanRef.decRef() {
		// We're going to use the child below, so we don't want it to be
		// re-allocated yet. It shouldn't be re-allocated, because each span holds a
		// reference to itself that's only dropped at the end of Span.Finish() (and
		// the child at this point is in the middle of its Finish() call).
		panic(errors.AssertionFailedf("span's reference count unexpectedly dropped to zero: %s", child.operation))
	}

	// Unlink the child.
	l := len(s.mu.openChildren)
	s.mu.openChildren[childIdx] = s.mu.openChildren[l-1]
	s.mu.openChildren[l-1] = childRef{}
	s.mu.openChildren = s.mu.openChildren[:l-1]

	// Collect the child's recording.

	if s.recordingType() == tracingpb.RecordingOff || !collectChildRec {
		return
	}

	s.recordFinishedChildrenLocked(child.GetRecording(s.recordingType(),
		false /* finishing - the child is already finished */))
}

// parentFinished makes s a root.
func (s *crdbSpan) parentFinished() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.parent.release()
	// Reset `notifyParentOnStructuredEvent` since s no longer has a parent.
	s.mu.recording.notifyParentOnStructuredEvent = false
}

// visitOpenChildren calls the visitor for every open child. The receiver's lock
// is held for the duration of the iteration, so the visitor should be quick.
// The visitor is not allowed to hold on to children after it returns.
func (s *crdbSpan) visitOpenChildren(visitor func(child *crdbSpan)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.mu.openChildren {
		visitor(c.span())
	}
}

// SetRecordingType is part of the RegistrySpan interface.
func (s *crdbSpan) SetRecordingType(to tracingpb.RecordingType) {
	s.mu.recording.recordingType.swap(to)

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, child := range s.mu.openChildren {
		child.span().SetRecordingType(to)
	}
}

// RecordingType is part of the RegistrySpan interface.
func (s *crdbSpan) RecordingType() tracingpb.RecordingType {
	return s.recordingType()
}

// withLock calls f while holding s' lock.
func (s *crdbSpan) withLock(f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f()
}

// wantEventNotificationsLocked returns true if the span was created
// WithEventListeners(...) or the span has been configured to notify its parent
// span on a StructuredEvent recording.
func (s *crdbSpan) wantEventNotificationsLocked() bool {
	return len(s.eventListeners) != 0 || s.mu.recording.notifyParentOnStructuredEvent
}

// setGoroutineID updates the span's goroutine ID.
func (s *crdbSpan) setGoroutineID(gid int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.goroutineID = uint64(gid)
}

type atomicRecordingType tracingpb.RecordingType

// load returns the recording type.
func (art *atomicRecordingType) load() tracingpb.RecordingType {
	return tracingpb.RecordingType(atomic.LoadInt32((*int32)(art)))
}

// swap stores the new recording type and returns the old one.
func (art *atomicRecordingType) swap(recType tracingpb.RecordingType) tracingpb.RecordingType {
	return tracingpb.RecordingType(atomic.SwapInt32((*int32)(art), int32(recType)))
}
