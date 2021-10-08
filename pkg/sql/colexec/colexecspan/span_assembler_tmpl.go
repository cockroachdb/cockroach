// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
// +build execgen_template
//
// This file is the execgen template for span_assembler.eg.go. It's formatted in
// a special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexecspan

import (
	"sync"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// NewColSpanAssembler returns a ColSpanAssembler operator that is able to
// generate lookup spans from input batches.
func NewColSpanAssembler(
	codec keys.SQLCodec,
	allocator *colmem.Allocator,
	table catalog.TableDescriptor,
	index catalog.Index,
	inputTypes []*types.T,
	neededCols util.FastIntSet,
) ColSpanAssembler {
	base := spanAssemblerPool.Get().(*spanAssemblerBase)
	base.colFamStartKeys, base.colFamEndKeys = getColFamilyEncodings(neededCols, table, index)
	keyPrefix := rowenc.MakeIndexKeyPrefix(codec, table, index.GetID())
	base.scratchKey = append(base.scratchKey[:0], keyPrefix...)
	base.prefixLength = len(keyPrefix)
	base.allocator = allocator

	// Add span encoders to encode each primary key column as bytes. The
	// ColSpanAssembler will later append these together to form valid spans.
	for i := 0; i < index.NumKeyColumns(); i++ {
		asc := index.GetKeyColumnDirection(i) == descpb.IndexDescriptor_ASC
		base.spanEncoders = append(base.spanEncoders, newSpanEncoder(allocator, inputTypes[i], asc, i))
	}
	if cap(base.spanCols) < len(base.spanEncoders) {
		base.spanCols = make([]*coldata.Bytes, len(base.spanEncoders))
	} else {
		base.spanCols = base.spanCols[:len(base.spanEncoders)]
	}

	// Account for the memory currently in use.
	usedMem := int64(cap(base.spans) * int(spanSize))
	base.allocator.AdjustMemoryUsage(usedMem)

	if len(base.colFamStartKeys) == 0 {
		return &spanAssemblerNoColFamily{spanAssemblerBase: *base}
	}
	return &spanAssemblerWithColFamily{spanAssemblerBase: *base}
}

var spanAssemblerPool = sync.Pool{
	New: func() interface{} {
		return &spanAssemblerBase{}
	},
}

// ColSpanAssembler is a utility operator that generates a series of spans from
// input batches which can be used to perform an index join.
type ColSpanAssembler interface {
	execinfra.Releasable

	// ConsumeBatch generates lookup spans from input batches and stores them to
	// later be returned by GetSpans. Spans are generated only for rows in the
	// range [startIdx, endIdx). If startIdx >= endIdx, ConsumeBatch will perform
	// no work.
	ConsumeBatch(batch coldata.Batch, startIdx, endIdx int)

	// GetSpans returns the set of spans that have been generated so far. The
	// returned Spans object is still owned by the SpanAssembler, so subsequent
	// calls to GetSpans will invalidate the spans returned by previous calls. A
	// caller that wishes to hold on to spans over the course of multiple calls
	// should perform a shallow copy of the Spans. GetSpans will return an empty
	// slice if it is called before ConsumeBatch.
	GetSpans() roachpb.Spans

	// Close closes the ColSpanAssembler operator.
	Close()
}

// spanAssemblerBase extracts common fields between the SpanAssembler operators.
type spanAssemblerBase struct {
	allocator *colmem.Allocator

	// keyBytes tracks the number of bytes that have been allocated for the span
	// keys since the last call to GetSpans. It is reset each time GetSpans is
	// called, since the SpanAssembler operator no longer owns the memory.
	keyBytes int

	// spans is the list of spans that have been assembled so far. spans is owned
	// and reset upon each call to GetSpans by the SpanAssembler operator.
	spans roachpb.Spans

	// scratchKey is a scratch space used to append the key prefix and the key
	// column encodings. It is reused for each span, and always contains at least
	// the key prefix.
	scratchKey roachpb.Key

	// prefixLength is the length in bytes of the key prefix.
	prefixLength int

	// spanEncoders is an ordered list of utility operators that encode each key
	// column in vectorized fashion.
	spanEncoders []spanEncoder

	// spanCols is used to iterate through the input columns that contain the
	// key encodings during span construction.
	spanCols []*coldata.Bytes

	// colFamStartKeys and colFamEndKeys is the list of start and end key suffixes
	// for the column families that should be scanned. The spans will be split to
	// scan over each family individually. Note that it is not necessarily
	// possible to break a span into family scans.
	colFamStartKeys, colFamEndKeys []roachpb.Key
}

// {{range .}}

type _OP_STRING struct {
	spanAssemblerBase
}

var _ ColSpanAssembler = &_OP_STRING{}

// ConsumeBatch implements the ColSpanAssembler interface.
func (op *_OP_STRING) ConsumeBatch(batch coldata.Batch, startIdx, endIdx int) {
	if startIdx >= endIdx {
		return
	}

	for i := range op.spanEncoders {
		op.spanCols[i] = op.spanEncoders[i].next(batch, startIdx, endIdx)
	}

	oldKeyBytes := op.keyBytes
	oldSpansCap := cap(op.spans)
	for i := 0; i < (endIdx - startIdx); i++ {
		op.scratchKey = op.scratchKey[:op.prefixLength]
		for j := range op.spanCols {
			// The encoding for each primary key column has previously been
			// calculated and stored in an input column.
			op.scratchKey = append(op.scratchKey, op.spanCols[j].Get(i)...)
		}
		// {{if .WithColFamilies}}
		constructSpans(true)
		// {{else}}
		constructSpans(false)
		// {{end}}
	}

	// Account for the memory allocated for the span slice and keys.
	keyBytesMem := op.keyBytes - oldKeyBytes
	spanSliceMem := (cap(op.spans) - oldSpansCap) * int(spanSize)
	op.allocator.AdjustMemoryUsage(int64(spanSliceMem + keyBytesMem))
}

// {{end}}

const spanSize = unsafe.Sizeof(roachpb.Span{})

// GetSpans implements the ColSpanAssembler interface.
func (b *spanAssemblerBase) GetSpans() roachpb.Spans {
	// Even though the memory allocated for the span keys probably can't be GC'd
	// yet, we release now because the memory will be owned by the caller.
	b.allocator.ReleaseMemory(int64(b.keyBytes))
	b.keyBytes = 0
	spans := b.spans
	b.spans = b.spans[:0]
	return spans
}

// Close implements the ColSpanAssembler interface.
func (b *spanAssemblerBase) Close() {
	for i := range b.spanEncoders {
		b.spanEncoders[i].close()
	}
}

// Release implements the ColSpanAssembler interface.
// TODO(drewk): we account for the memory that is owned by the SpanAssembler
// operator, but release it once the SpanAssembler loses its references even
// though it can still be referenced elsewhere. The two cases are the spans
// slice (it is put into a pool) and the underlying bytes for the keys (they are
// referenced by the kv fetcher code). Ideally we would hand off memory
// accounting to whoever ends up owning the memory, instead of just no longer
// tracking it.
func (b *spanAssemblerBase) Release() {
	for i := range b.spanCols {
		// Release references to input columns.
		b.spanCols[i] = nil
	}
	for i := range b.spanEncoders {
		// Release references to input operators.
		b.spanEncoders[i] = nil
	}
	b.spans = b.spans[:cap(b.spans)]
	for i := range b.spans {
		// Deeply reset all spans that were initialized during execution.
		b.spans[i] = roachpb.Span{}
	}
	*b = spanAssemblerBase{
		spans:        b.spans[:0],
		spanEncoders: b.spanEncoders[:0],
		spanCols:     b.spanCols[:0],
		scratchKey:   b.scratchKey[:0],
	}
	spanAssemblerPool.Put(b)
}

// execgen:inline
// execgen:template<hasFamilies>
func constructSpans(hasFamilies bool) {
	if hasFamilies {
		// The span for each row can be split into a series of column family spans,
		// which have the column family ID as a suffix. Individual column family
		// spans can be served as Get requests, which are more efficient than Scan
		// requests.
		for j := range op.colFamStartKeys {
			var span roachpb.Span
			span.Key = make(roachpb.Key, 0, len(op.scratchKey)+len(op.colFamStartKeys[j]))
			span.Key = append(span.Key, op.scratchKey...)
			span.Key = append(span.Key, op.colFamStartKeys[j]...)
			op.keyBytes += len(span.Key)
			// The end key may be nil, in which case the span is a point lookup.
			if len(op.colFamEndKeys[j]) > 0 {
				span.EndKey = make(roachpb.Key, 0, len(op.scratchKey)+len(op.colFamEndKeys[j]))
				span.EndKey = append(span.EndKey, op.scratchKey...)
				span.EndKey = append(span.EndKey, op.colFamEndKeys[j]...)
				op.keyBytes += len(span.EndKey)
			}
			op.spans = append(op.spans, span)
		}
	} else {
		// The spans cannot be split into column family spans, so there will be
		// exactly one span for each input row.
		var span roachpb.Span
		span.Key = make(roachpb.Key, 0, len(op.scratchKey))
		span.Key = append(span.Key, op.scratchKey...)
		op.keyBytes += len(span.Key)
		span.EndKey = make(roachpb.Key, 0, len(op.scratchKey)+1)
		span.EndKey = append(span.EndKey, op.scratchKey...)
		// TODO(drewk): change this to use PrefixEnd() when interleaved indexes are
		// permanently removed. Keep it this way for now to allow testing
		// against the row engine, even though the vectorized index joiner doesn't
		// allow interleaved indexes.
		span.EndKey = encoding.EncodeInterleavedSentinel(span.EndKey)
		op.keyBytes += len(span.EndKey)
		op.spans = append(op.spans, span)
	}
}

// getColFamilyEncodings returns two lists of keys of the same length. Each pair
// of keys at the same index corresponds to the suffixes of the start and end
// keys of a span over a specific column family (or adjacent column families).
// If the returned lists are empty, the spans cannot be split into separate
// family spans.
func getColFamilyEncodings(
	neededCols util.FastIntSet, table catalog.TableDescriptor, index catalog.Index,
) (startKeys, endKeys []roachpb.Key) {
	familyIDs := rowenc.NeededColumnFamilyIDs(neededCols, table, index)

	if !canSplitSpans(len(familyIDs), table, index) {
		return nil, nil
	}

	for i, familyID := range familyIDs {
		var key roachpb.Key
		key = keys.MakeFamilyKey(key, uint32(familyID))
		if i > 0 && familyID-1 == familyIDs[i-1] && endKeys != nil {
			// This column family is adjacent to the previous one. We can merge
			// the two spans into one.
			endKeys[len(endKeys)-1] = key.PrefixEnd()
		} else {
			startKeys = append(startKeys, key)
			endKeys = append(endKeys, nil)
		}
	}
	return startKeys, endKeys
}

// canSplitSpans returns true if the spans that will be generated by the
// SpanAssembler operator can be split into spans over individual column
// families. For index joins, either all spans can be split or none can because
// the lookup columns are never nullable (null values prevent the index key from
// being fully knowable).
func canSplitSpans(numNeededFamilies int, table catalog.TableDescriptor, index catalog.Index) bool {
	// We can only split a span into separate family specific point lookups if:
	//
	// * The table is not a special system table. (System tables claim to have
	//   column families, but actually do not, since they're written to with
	//   raw KV puts in a "legacy" way.)
	if table.GetID() > 0 && table.GetID() < keys.MaxReservedDescID {
		return false
	}

	// * The index either has just 1 family (so we'll make a GetRequest) or we
	//   need fewer than every column family in the table (otherwise we'd just
	//   make a big ScanRequest).
	numFamilies := len(table.GetFamilies())
	if numFamilies > 1 && numNeededFamilies == numFamilies {
		return false
	}

	// Other requirements that are always satisfied by index joins, and therefore
	// do not need to be checked:
	// * The index is unique.
	// * The index is fully constrained.
	// * If we're looking at a secondary index...
	//   * The index constraint must not contain null, since that would cause the
	//     index key to not be completely knowable.
	//   * The index cannot be inverted.
	//   * The index must store some columns.
	//   * The index is a new enough version.
	//
	// We've passed all the conditions, and should be able to safely split this
	// span into multiple column-family-specific spans.
	return true
}
