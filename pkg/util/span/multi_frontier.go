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
	"github.com/cockroachdb/cockroach/pkg/util/container/heap"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// A PartitionerFunc is a function that assigns a span a partition.
// The returned partition must be stable for any given span.
type PartitionerFunc[T comparable] func(roachpb.Span) (T, error)

// MultiFrontier is a Frontier that partitions its span space into
// multiple sub-frontiers using a given PartitionerFunc.
type MultiFrontier[T comparable] struct {
	partitioner PartitionerFunc[T]
	// TODO replacing this with the basic makefrontier call
	constructor func() Frontier
	// TODO consider storing sorted slice of partitions

	mu struct {
		// TODO consider replacing with syncutil.RWMutex
		syncutil.Mutex
		frontiers *multiFrontierHeap[T]
	}
}

var _ Frontier = (*MultiFrontier[int])(nil)
var _ PartitionedFrontier[int] = (*MultiFrontier[int])(nil)

// NewMultiFrontier returns a new MultiFrontier with all spans initialized
// at the zero timestamp.
func NewMultiFrontier[T comparable](
	partitioner PartitionerFunc[T], spans ...roachpb.Span,
) (*MultiFrontier[T], error) {
	return NewMultiFrontierAt(partitioner, hlc.Timestamp{}, spans...)
}

// NewMultiFrontierAt returns a new MultiFrontier with all spans initialized
// at the provided timestamp.
func NewMultiFrontierAt[T comparable](
	partitioner PartitionerFunc[T], ts hlc.Timestamp, spans ...roachpb.Span,
) (*MultiFrontier[T], error) {
	f := &MultiFrontier[T]{
		partitioner: partitioner,
		constructor: newBtreeFrontier,
	}
	f.mu.frontiers = newMultiFrontierHeap[T]()
	if err := f.AddSpansAt(ts, spans...); err != nil {
		f.Release()
		return nil, err
	}
	return f, nil
}

// AddSpansAt implements Frontier.
func (f *MultiFrontier[T]) AddSpansAt(startAt hlc.Timestamp, spans ...roachpb.Span) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, sp := range spans {
		partition, err := f.partitioner(sp)
		if err != nil {
			return err
		}
		frontier, ok := f.mu.frontiers.get(partition)
		if !ok {
			frontier = f.constructor()
			if err := f.mu.frontiers.add(partition, frontier); err != nil {
				return err
			}
		}
		if err := frontier.AddSpansAt(startAt, sp); err != nil {
			return err
		}
	}
	f.mu.frontiers.heapify()

	return nil
}

// Frontier implements Frontier.
func (f *MultiFrontier[T]) Frontier() hlc.Timestamp {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.mu.frontiers.Len() == 0 {
		return hlc.Timestamp{}
	}

	return f.mu.frontiers.min().Frontier()
}

// PeekFrontierSpan implements Frontier.
func (f *MultiFrontier[T]) PeekFrontierSpan() roachpb.Span {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.mu.frontiers.Len() == 0 {
		return roachpb.Span{}
	}

	return f.mu.frontiers.min().PeekFrontierSpan()
}

// Forward implements Frontier.
func (f *MultiFrontier[T]) Forward(span roachpb.Span, ts hlc.Timestamp) (bool, error) {
	partition, err := f.partitioner(span)
	if err != nil {
		return false, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	frontier, ok := f.mu.frontiers.get(partition)
	if !ok {
		return false, nil
	}

	forwarded, err := frontier.Forward(span, ts)
	if err != nil {
		return false, err
	}
	if forwarded {
		if err := f.mu.frontiers.fixup(partition); err != nil {
			return false, err
		}
	}
	return forwarded, nil
}

// Release implements Frontier.
func (f *MultiFrontier[T]) Release() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.mu.frontiers.clear()
}

// Entries implements Frontier.
func (f *MultiFrontier[T]) Entries() iter.Seq2[roachpb.Span, hlc.Timestamp] {
	// TODO consider returning in sorted order
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
func (f *MultiFrontier[T]) SpanEntries(span roachpb.Span) iter.Seq2[roachpb.Span, hlc.Timestamp] {
	// TODO consider returning in sorted order
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
func (f *MultiFrontier[T]) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	var l int
	for _, frontier := range f.mu.frontiers.all() {
		l += frontier.Len()
	}
	return l
}

// String implements Frontier.
func (f *MultiFrontier[T]) String() string {
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

// Partitions implements PartitionedFrontier.
func (f *MultiFrontier[T]) Partitions() iter.Seq2[T, Frontier] {
	return func(yield func(T, Frontier) bool) {
		f.mu.Lock()
		defer f.mu.Unlock()

		for partition, frontier := range f.mu.frontiers.all() {
			if !yield(partition, frontier) {
				return
			}
		}
	}
}

// FrontierFor implements PartitionedFrontier.
func (f *MultiFrontier[T]) FrontierFor(partition T) Frontier {
	f.mu.Lock()
	defer f.mu.Unlock()

	frontier, ok := f.mu.frontiers.get(partition)
	if ok {
		return frontier
	}
	return nil
}

// TODO consider renaming SubFrontiers
// Frontiers returns an iterator over the sub-frontiers.
func (f *MultiFrontier[T]) Frontiers() iter.Seq2[T, Frontier] {
	return func(yield func(T, Frontier) bool) {
		f.mu.Lock()
		defer f.mu.Unlock()

		for partition, frontier := range f.mu.frontiers.all() {
			if !yield(partition, frontier) {
				return
			}
		}
	}
}

type multiFrontierHeapElem[T comparable] struct {
	frontier  Frontier
	partition T
	index     int
}

type multiFrontierHeap[T comparable] struct {
	h          []*multiFrontierHeapElem[T]
	partitions map[T]*multiFrontierHeapElem[T]
}

var _ heap.Interface[*multiFrontierHeapElem[int]] = (*multiFrontierHeap[int])(nil)

func newMultiFrontierHeap[T comparable]() *multiFrontierHeap[T] {
	return &multiFrontierHeap[T]{
		partitions: make(map[T]*multiFrontierHeapElem[T]),
	}
}

// Len implements sort.Interface.
func (h *multiFrontierHeap[T]) Len() int { return len(h.h) }

// Less implements sort.Interface.
func (h *multiFrontierHeap[T]) Less(i, j int) bool {
	return h.h[i].frontier.Frontier().Compare(h.h[j].frontier.Frontier()) < 0
}

// Swap implements sort.Interface.
func (h *multiFrontierHeap[T]) Swap(i, j int) {
	h.h[i], h.h[j] = h.h[j], h.h[i]
	h.h[i].index, h.h[j].index = i, j
}

// Push implements heap.Interface.
func (h *multiFrontierHeap[T]) Push(x *multiFrontierHeapElem[T]) {
	n := len(h.h)
	x.index = n
	h.h = append(h.h, x)
	h.partitions[x.partition] = x
}

// Pop implements heap.Interface.
func (h *multiFrontierHeap[T]) Pop() *multiFrontierHeapElem[T] {
	n := len(h.h)
	elem := h.h[n-1]
	elem.index = -1
	h.h[n-1] = nil
	h.h = h.h[:n-1]
	delete(h.partitions, elem.partition)
	return elem
}

func (h *multiFrontierHeap[T]) add(partition T, frontier Frontier) error {
	if _, ok := h.partitions[partition]; ok {
		return errors.AssertionFailedf("frontier for partition %v already exists", partition)
	}
	elem := &multiFrontierHeapElem[T]{frontier: frontier, partition: partition}
	heap.Push(h, elem)
	return nil
}

func (h *multiFrontierHeap[T]) get(partition T) (Frontier, bool) {
	if elem, ok := h.partitions[partition]; ok {
		return elem.frontier, true
	}
	return nil, false
}

func (h *multiFrontierHeap[T]) min() Frontier {
	if len(h.h) == 0 {
		return nil
	}
	return h.h[0].frontier
}

func (h *multiFrontierHeap[T]) heapify() {
	heap.Init(h)
}

func (h *multiFrontierHeap[T]) fixup(partition T) error {
	elem, ok := h.partitions[partition]
	if !ok {
		return errors.AssertionFailedf("partition %v does not exist", partition)
	}
	heap.Fix(h, elem.index)
	return nil
}

func (h *multiFrontierHeap[T]) clear() {
	for _, frontier := range h.all() {
		frontier.Release()
	}
	clear(h.h)
	h.h = h.h[:0]
	clear(h.partitions)
}

func (h *multiFrontierHeap[T]) all() iter.Seq2[T, Frontier] {
	return func(yield func(T, Frontier) bool) {
		for partition, elem := range h.partitions {
			if !yield(partition, elem.frontier) {
				return
			}
		}
	}
}
