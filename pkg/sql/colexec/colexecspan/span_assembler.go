// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecspan

import (
	"sync"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// NewColSpanAssembler returns a ColSpanAssembler operator that is able to
// generate lookup spans from input batches.
// See JoinReaderSpec for more info on fetchSpec and splitFamilyIDs.
func NewColSpanAssembler(
	codec keys.SQLCodec,
	allocator *colmem.Allocator,
	fetchSpec *descpb.IndexFetchSpec,
	splitFamilyIDs []descpb.FamilyID,
	inputTypes []*types.T,
) ColSpanAssembler {
	sa := spanAssemblerPool.Get().(*spanAssembler)
	sa.colFamStartKeys, sa.colFamEndKeys = getColFamilyEncodings(splitFamilyIDs)
	keyPrefix := rowenc.MakeIndexKeyPrefix(codec, fetchSpec.TableID, fetchSpec.IndexID)
	sa.scratchKey = append(sa.scratchKey[:0], keyPrefix...)
	sa.prefixLength = len(keyPrefix)
	sa.allocator = allocator

	// Add span encoders to encode each primary key column as bytes. The
	// ColSpanAssembler will later append these together to form valid spans.
	keyColumns := fetchSpec.KeyColumns()
	for i := range keyColumns {
		asc := keyColumns[i].Direction == descpb.IndexDescriptor_ASC
		sa.spanEncoders = append(sa.spanEncoders, newSpanEncoder(allocator, inputTypes[i], asc, i))
	}
	if cap(sa.spanCols) < len(sa.spanEncoders) {
		sa.spanCols = make([]*coldata.Bytes, len(sa.spanEncoders))
	} else {
		sa.spanCols = sa.spanCols[:len(sa.spanEncoders)]
	}

	// Account for the memory currently in use.
	sa.spansBytes = int64(cap(sa.spans)) * spanSize
	sa.allocator.AdjustMemoryUsage(sa.spansBytes)

	return sa
}

var spanAssemblerPool = sync.Pool{
	New: func() interface{} {
		return &spanAssembler{}
	},
}

// ColSpanAssembler is a utility operator that generates a series of spans from
// input batches which can be used to perform an index join.
type ColSpanAssembler interface {
	execinfra.Releasable

	// ConsumeBatch generates lookup spans from input batches and stores them to
	// later be returned by GetSpans. Spans are generated only for rows in the
	// range [startIdx, endIdx). If startIdx >= endIdx, ConsumeBatch will
	// perform no work. The memory of the newly accumulated spans is accounted
	// for.
	//
	// Note: ConsumeBatch may invalidate the spans returned by the last call to
	// GetSpans.
	ConsumeBatch(batch coldata.Batch, startIdx, endIdx int)

	// GetSpans returns the set of spans that have been generated so far. The
	// subsequent calls to GetSpans will invalidate the spans returned by the
	// previous calls. A caller that wishes to hold on to spans over the course
	// of multiple calls should perform a shallow copy of the Spans. GetSpans
	// will return an empty slice if it is called before ConsumeBatch.
	//
	// The memory of the returned object is no longer accounted for by the
	// ColSpanAssembler, so it is the caller's responsibility to do so.
	GetSpans() roachpb.Spans

	// AccountForSpans notifies the ColSpanAssembler that it is now responsible
	// for accounting for the memory used by already allocated spans slice. This
	// should be called after the result of the last call to GetSpans() is no
	// longer accounted for by the caller.
	AccountForSpans()

	// Close closes the ColSpanAssembler operator.
	Close()
}

type spanAssembler struct {
	allocator *colmem.Allocator

	// keyBytes tracks the number of bytes that have been allocated for the span
	// keys since the last call to GetSpans. It is reset each time GetSpans is
	// called, since the SpanAssembler operator no longer owns the memory.
	keyBytes int

	// spansBytes tracks the number of bytes used by spans slice that we have
	// accounted for so far. It doesn't include any of the keys in the spans.
	spansBytes int64

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
	// possible to break a span into family scans (in which case these slices are
	// empty).
	colFamStartKeys, colFamEndKeys []roachpb.Key
}

var _ ColSpanAssembler = (*spanAssembler)(nil)

// ConsumeBatch implements the ColSpanAssembler interface.
func (sa *spanAssembler) ConsumeBatch(batch coldata.Batch, startIdx, endIdx int) {
	if startIdx >= endIdx {
		return
	}

	for i := range sa.spanEncoders {
		sa.spanCols[i] = sa.spanEncoders[i].next(batch, startIdx, endIdx)
	}

	oldKeyBytes := sa.keyBytes
	oldSpansBytes := sa.spansBytes

	if len(sa.colFamStartKeys) == 0 {
		// The spans cannot be split into column family spans, so there will be
		// exactly one span for each input row.
		for i := 0; i < (endIdx - startIdx); i++ {
			sa.scratchKey = sa.scratchKey[:sa.prefixLength]
			for j := range sa.spanCols {
				// The encoding for each primary key column has previously been
				// calculated and stored in an input column.
				sa.scratchKey = append(sa.scratchKey, sa.spanCols[j].Get(i)...)
			}
			var span roachpb.Span
			span.Key = make(roachpb.Key, 0, len(sa.scratchKey))
			span.Key = append(span.Key, sa.scratchKey...)
			sa.keyBytes += len(span.Key)
			span.EndKey = make(roachpb.Key, 0, len(sa.scratchKey)+1)
			span.EndKey = append(span.EndKey, sa.scratchKey...)
			span.EndKey = span.EndKey.PrefixEnd()
			sa.keyBytes += len(span.EndKey)
			sa.spans = append(sa.spans, span)
		}
	} else {
		// The span for each row can be split into a series of column family spans,
		// which have the column family ID as a suffix. Individual column family
		// spans can be served as Get requests, which are more efficient than Scan
		// requests.
		for i := 0; i < (endIdx - startIdx); i++ {
			sa.scratchKey = sa.scratchKey[:sa.prefixLength]
			for j := range sa.spanCols {
				// The encoding for each primary key column has previously been
				// calculated and stored in an input column.
				sa.scratchKey = append(sa.scratchKey, sa.spanCols[j].Get(i)...)
			}
			for j := range sa.colFamStartKeys {
				var span roachpb.Span
				span.Key = make(roachpb.Key, 0, len(sa.scratchKey)+len(sa.colFamStartKeys[j]))
				span.Key = append(span.Key, sa.scratchKey...)
				span.Key = append(span.Key, sa.colFamStartKeys[j]...)
				sa.keyBytes += len(span.Key)
				// The end key may be nil, in which case the span is a point lookup.
				if len(sa.colFamEndKeys[j]) > 0 {
					span.EndKey = make(roachpb.Key, 0, len(sa.scratchKey)+len(sa.colFamEndKeys[j]))
					span.EndKey = append(span.EndKey, sa.scratchKey...)
					span.EndKey = append(span.EndKey, sa.colFamEndKeys[j]...)
					sa.keyBytes += len(span.EndKey)
				}
				sa.spans = append(sa.spans, span)
			}
		}
	}

	// Account for the memory allocated for the span slice and keys.
	keyBytesMem := int64(sa.keyBytes - oldKeyBytes)
	sa.spansBytes = int64(cap(sa.spans)) * spanSize
	sa.allocator.AdjustMemoryUsage((sa.spansBytes - oldSpansBytes) + keyBytesMem)
}

const spanSize = int64(unsafe.Sizeof(roachpb.Span{}))

// GetSpans implements the ColSpanAssembler interface.
func (sa *spanAssembler) GetSpans() roachpb.Spans {
	// The caller takes ownership of the returned spans, so we release all the
	// memory.
	sa.allocator.ReleaseMemory(int64(sa.keyBytes) + sa.spansBytes)
	sa.keyBytes = 0
	sa.spansBytes = 0
	spans := sa.spans
	sa.spans = sa.spans[:0]
	return spans
}

// AccountForSpans implements the ColSpanAssembler interface.
func (sa *spanAssembler) AccountForSpans() {
	if sa.spansBytes != 0 {
		colexecerror.InternalError(errors.AssertionFailedf(
			"unexpectedly non-zero spans bytes in AccountForSpans",
		))
	}
	sa.spansBytes = int64(cap(sa.spans)) * spanSize
	sa.allocator.AdjustMemoryUsage(sa.spansBytes)
}

// Close implements the ColSpanAssembler interface.
func (sa *spanAssembler) Close() {
	for i := range sa.spanEncoders {
		sa.spanEncoders[i].close()
	}
}

// Release implements the ColSpanAssembler interface.
// TODO(yuzefovich): once we put the spanAssembler into the pool, we no longer
// account for the spans slice. Figure out how we can improve the accounting
// here.
func (sa *spanAssembler) Release() {
	for i := range sa.spanCols {
		// Release references to input columns.
		sa.spanCols[i] = nil
	}
	for i := range sa.spanEncoders {
		// Release references to input operators.
		sa.spanEncoders[i] = nil
	}
	sa.spans = sa.spans[:cap(sa.spans)]
	for i := range sa.spans {
		// Deeply reset all spans that were initialized during execution.
		sa.spans[i] = roachpb.Span{}
	}
	*sa = spanAssembler{
		spans:        sa.spans[:0],
		spanEncoders: sa.spanEncoders[:0],
		spanCols:     sa.spanCols[:0],
		scratchKey:   sa.scratchKey[:0],
	}
	spanAssemblerPool.Put(sa)
}

// getColFamilyEncodings returns two lists of keys of the same length. Each pair
// of keys at the same index corresponds to the suffixes of the start and end
// keys of a span over a specific column family (or adjacent column families).
// If the returned lists are empty, the spans cannot be split into separate
// family spans.
func getColFamilyEncodings(splitFamilyIDs []descpb.FamilyID) (startKeys, endKeys []roachpb.Key) {
	if len(splitFamilyIDs) == 0 {
		return nil, nil
	}
	for i, familyID := range splitFamilyIDs {
		var key roachpb.Key
		key = keys.MakeFamilyKey(key, uint32(familyID))
		if i > 0 && familyID-1 == splitFamilyIDs[i-1] && endKeys != nil {
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
