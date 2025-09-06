// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package span

import (
	"fmt"
	"iter"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/container/heap"
	"github.com/cockroachdb/cockroach/pkg/util/container/heaputil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// A PartitionerFunc is a function that assigns a span a partition of type P.
// The returned partition must be stable for any given span (and any of its sub-spans).
//
// An example function would be a function that returns the table ID that
// a span belongs to or an error if the span overlaps multiple tables.
type PartitionerFunc[P comparable] func(roachpb.Span) (P, error)

// MultiFrontier is a Frontier that partitions its span space into
// multiple sub-frontiers using a provided PartitionerFunc[P] to determine
// the partition (of type P) for each span. It starts with no sub-frontiers
// and creates new ones lazily when spans that map to previously unseen
// partitions are added to the set of tracked spans.
//
// An example use case would be tracking spans belonging to different tables
// where we want to quickly see both overall and per-table progress.
type MultiFrontier[P comparable] struct {
	// TODO(yang): Consider adding a cache for the computed partitions.
	partitioner PartitionerFunc[P]

	mu struct {
		syncutil.Mutex
		frontiers *partitionedMultiFrontierHeap[P]
	}
}

var _ Frontier = (*MultiFrontier[int])(nil)

// NewMultiFrontier returns a new MultiFrontier with all spans initialized
// at the zero timestamp.
func NewMultiFrontier[P comparable](
	partitioner PartitionerFunc[P], spans ...roachpb.Span,
) (*MultiFrontier[P], error) {
	return NewMultiFrontierAt(partitioner, hlc.Timestamp{}, spans...)
}

// NewMultiFrontierAt returns a new MultiFrontier with all spans initialized
// at the provided timestamp.
func NewMultiFrontierAt[P comparable](
	partitioner PartitionerFunc[P], ts hlc.Timestamp, spans ...roachpb.Span,
) (*MultiFrontier[P], error) {
	f := &MultiFrontier[P]{
		partitioner: partitioner,
	}
	f.mu.frontiers = newPartitionedMultiFrontierHeap[P]()
	if err := f.AddSpansAt(ts, spans...); err != nil {
		f.Release()
		return nil, err
	}
	return f, nil
}

// AddSpansAt implements Frontier.
func (f *MultiFrontier[P]) AddSpansAt(startAt hlc.Timestamp, spans ...roachpb.Span) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, sp := range spans {
		partition, err := f.partitioner(sp)
		if err != nil {
			return errors.Wrapf(err, "got partitioner error when attempting to add spans")
		}
		frontier := f.mu.frontiers.get(partition)
		if frontier == nil {
			frontier = newFrontier()
			if err := f.mu.frontiers.pushUnordered(partition, frontier); err != nil {
				return err
			}
		}
		if err := frontier.AddSpansAt(startAt, sp); err != nil {
			return err
		}
	}

	// Re-initialize the heap because the act of adding new spans to the
	// sub-frontiers may have changed the sort order.
	f.mu.frontiers.heapify()

	if buildutil.CrdbTestBuild && !f.mu.frontiers.valid() {
		return errors.AssertionFailedf(
			"AddSpansAt: heap invariant violated after adding spans: %v", spans)
	}

	return nil
}

// Frontier implements Frontier.
func (f *MultiFrontier[P]) Frontier() hlc.Timestamp {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.mu.frontiers.len() == 0 {
		return hlc.Timestamp{}
	}

	if buildutil.CrdbTestBuild && !f.mu.frontiers.valid() {
		panic(errors.AssertionFailedf("Frontier: heap invariant violation detected"))
	}

	return f.mu.frontiers.min().Frontier()
}

// PeekFrontierSpan implements Frontier.
func (f *MultiFrontier[P]) PeekFrontierSpan() roachpb.Span {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.mu.frontiers.len() == 0 {
		return roachpb.Span{}
	}

	if buildutil.CrdbTestBuild && !f.mu.frontiers.valid() {
		panic(errors.AssertionFailedf("PeekFrontierSpan: heap invariant violation detected"))
	}

	return f.mu.frontiers.min().PeekFrontierSpan()
}

// Forward implements Frontier.
func (f *MultiFrontier[P]) Forward(span roachpb.Span, ts hlc.Timestamp) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	partition, err := f.partitioner(span)
	if err != nil {
		return false, errors.Wrapf(err, "got partitioner error when attempting to forward")
	}

	frontier := f.mu.frontiers.get(partition)
	if frontier == nil {
		return false, nil
	}

	prevMinFrontier := f.mu.frontiers.min().Frontier()
	forwarded, err := frontier.Forward(span, ts)
	if err != nil {
		return false, err
	}
	if forwarded {
		if err := f.mu.frontiers.fixup(partition); err != nil {
			return false, err
		}
	}

	if buildutil.CrdbTestBuild && !f.mu.frontiers.valid() {
		return false, errors.AssertionFailedf(
			"Forward: heap invariant violated after forwarding span: %v", span)
	}

	newMinFrontier := f.mu.frontiers.min().Frontier()
	if prevMinFrontier.Less(newMinFrontier) {
		return true, nil
	}
	return false, nil
}

// Release implements Frontier.
func (f *MultiFrontier[P]) Release() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.mu.frontiers.clear()
}

// Entries implements Frontier.
func (f *MultiFrontier[P]) Entries() iter.Seq2[roachpb.Span, hlc.Timestamp] {
	return func(yield func(roachpb.Span, hlc.Timestamp) bool) {
		f.mu.Lock()
		defer f.mu.Unlock()

		for _, frontier := range f.mu.frontiers.all() {
			for sp, ts := range frontier.Entries() {
				if !yield(sp, ts) {
					return
				}
			}
		}
	}
}

// SpanEntries implements Frontier.
func (f *MultiFrontier[P]) SpanEntries(span roachpb.Span) iter.Seq2[roachpb.Span, hlc.Timestamp] {
	return func(yield func(roachpb.Span, hlc.Timestamp) bool) {
		f.mu.Lock()
		defer f.mu.Unlock()

		for _, frontier := range f.mu.frontiers.all() {
			for sp, ts := range frontier.SpanEntries(span) {
				if !yield(sp, ts) {
					return
				}
			}
		}
	}
}

// Len implements Frontier.
func (f *MultiFrontier[P]) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	var l int
	for _, frontier := range f.mu.frontiers.all() {
		l += frontier.Len()
	}
	return l
}

// String implements Frontier.
func (f *MultiFrontier[P]) String() string {
	f.mu.Lock()
	defer f.mu.Unlock()

	var buf strings.Builder
	for partition, frontier := range f.mu.frontiers.all() {
		if buf.Len() != 0 {
			buf.WriteString(`, `)
		}
		buf.WriteString(fmt.Sprintf("%v: {%s}", partition, frontier.String()))
	}
	return buf.String()
}

// Frontiers returns an iterator over the sub-frontiers (with read-only access).
func (f *MultiFrontier[P]) Frontiers() iter.Seq2[P, ReadOnlyFrontier] {
	return func(yield func(P, ReadOnlyFrontier) bool) {
		f.mu.Lock()
		defer f.mu.Unlock()

		for partition, frontier := range f.mu.frontiers.all() {
			if !yield(partition, frontier) {
				return
			}
		}
	}
}

// partitionedMultiFrontierHeap is a wrapper around multiFrontierHeap
// that provides convenience methods so that frontiers can be
// accessed by their partition. It is not safe for concurrent use.
type partitionedMultiFrontierHeap[P comparable] struct {
	// h is a heap of frontiers.
	h multiFrontierHeap
	// partitions stores a map of partitions to heap nodes.
	partitions map[P]*multiFrontierHeapNode
}

// newPartitionedMultiFrontierHeap returns a new partitionedMultiFrontierHeap.
func newPartitionedMultiFrontierHeap[P comparable]() *partitionedMultiFrontierHeap[P] {
	return &partitionedMultiFrontierHeap[P]{
		partitions: make(map[P]*multiFrontierHeapNode),
	}
}

// pushUnordered adds a new frontier to the heap for the given partition
// without maintaining the heap invariant. Callers are responsible for
// calling fixup or heapify to restore the heap invariant after pushing.
func (h *partitionedMultiFrontierHeap[P]) pushUnordered(partition P, frontier Frontier) error {
	if _, ok := h.partitions[partition]; ok {
		return errors.AssertionFailedf("frontier for partition %v already exists", partition)
	}
	node := &multiFrontierHeapNode{frontier: frontier}
	h.h.Push(node)
	h.partitions[partition] = node
	return nil
}

// get returns the frontier for the given partition if it is present
// in the heap and nil otherwise.
func (h *partitionedMultiFrontierHeap[P]) get(partition P) Frontier {
	if node, ok := h.partitions[partition]; ok {
		return node.frontier
	}
	return nil
}

// min returns the frontier for the minimum node in the heap.
func (h *partitionedMultiFrontierHeap[P]) min() Frontier {
	if len(h.h) == 0 {
		return nil
	}
	return h.h[0].frontier
}

// fixup performs a fixup of the heap node for the given partition.
func (h *partitionedMultiFrontierHeap[P]) fixup(partition P) error {
	node, ok := h.partitions[partition]
	if !ok {
		return errors.AssertionFailedf("partition %v does not exist", partition)
	}
	heap.Fix(&h.h, node.index)
	return nil
}

// heapify reinitializes the heap.
func (h *partitionedMultiFrontierHeap[P]) heapify() {
	heap.Init(&h.h)
}

// clear releases and removes all the frontiers in the heap.
func (h *partitionedMultiFrontierHeap[P]) clear() {
	for _, frontier := range h.all() {
		frontier.Release()
	}
	clear(h.h)
	h.h = h.h[:0]
	clear(h.partitions)
}

// len returns the number of frontiers in the heap.
func (h *partitionedMultiFrontierHeap[P]) len() int {
	return len(h.h)
}

// all returns an iterator over the partitions and frontiers tracked
// by the heap.
func (h *partitionedMultiFrontierHeap[P]) all() iter.Seq2[P, Frontier] {
	return func(yield func(P, Frontier) bool) {
		for partition, node := range h.partitions {
			if !yield(partition, node.frontier) {
				return
			}
		}
	}
}

// valid returns whether the heap is valid (invariant is intact).
func (h *partitionedMultiFrontierHeap[P]) valid() bool {
	return heaputil.Valid(&h.h)
}

// multiFrontierHeap is a heap of frontiers that are sorted by their
// frontier timestamps. It is not safe for concurrent use.
type multiFrontierHeap []*multiFrontierHeapNode

var _ heap.Interface[*multiFrontierHeapNode] = (*multiFrontierHeap)(nil)

// Len implements sort.Interface.
func (h multiFrontierHeap) Len() int { return len(h) }

// Less implements sort.Interface.
func (h multiFrontierHeap) Less(i, j int) bool {
	return h[i].frontier.Frontier().Compare(h[j].frontier.Frontier()) < 0
}

// Swap implements sort.Interface.
func (h multiFrontierHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

// Push implements heap.Interface.
func (h *multiFrontierHeap) Push(x *multiFrontierHeapNode) {
	n := len(*h)
	x.index = n
	*h = append(*h, x)
}

// Pop implements heap.Interface.
func (h *multiFrontierHeap) Pop() *multiFrontierHeapNode {
	old := *h
	n := len(old)
	node := old[n-1]
	old[n-1] = nil
	node.index = -1
	*h = old[:n-1]
	return node
}

// A multiFrontierHeapNode is a multiFrontierHeap node.
type multiFrontierHeapNode struct {
	// frontier is the frontier tracked by the node.
	frontier Frontier
	// index is the index of this node in the heap.
	index int
}
