// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// We define a group to be a set of rows from a given source with the same
// group key, in this case the set of ordered columns. streamMerger emits
// batches of rows that are the cross-product of matching groups from each
// stream.
type streamMerger struct {
	left       streamGroupAccumulator
	right      streamGroupAccumulator
	leftGroup  []rowenc.EncDatumRow
	rightGroup []rowenc.EncDatumRow
	// nulLEquality indicates when NULL = NULL is truth-y. This is helpful
	// when we want NULL to be meaningful during equality, for example
	// during SCRUB secondary index checks.
	nullEquality bool
	datumAlloc   rowenc.DatumAlloc
}

func (sm *streamMerger) start(ctx context.Context) {
	sm.left.start(ctx)
	sm.right.start(ctx)
}

// NextBatch returns a set of rows from the left stream and a set of rows from
// the right stream, all matching on the equality columns. One of the sets can
// be empty.
func (sm *streamMerger) NextBatch(
	ctx context.Context, evalCtx *tree.EvalContext,
) ([]rowenc.EncDatumRow, []rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if sm.leftGroup == nil {
		var meta *execinfrapb.ProducerMetadata
		sm.leftGroup, meta = sm.left.nextGroup(ctx, evalCtx)
		if meta != nil {
			return nil, nil, meta
		}
	}
	if sm.rightGroup == nil {
		var meta *execinfrapb.ProducerMetadata
		sm.rightGroup, meta = sm.right.nextGroup(ctx, evalCtx)
		if meta != nil {
			return nil, nil, meta
		}
	}
	if sm.leftGroup == nil && sm.rightGroup == nil {
		return nil, nil, nil
	}

	var lrow, rrow rowenc.EncDatumRow
	if len(sm.leftGroup) > 0 {
		lrow = sm.leftGroup[0]
	}
	if len(sm.rightGroup) > 0 {
		rrow = sm.rightGroup[0]
	}

	cmp, err := CompareEncDatumRowForMerge(
		sm.left.types, lrow, rrow, sm.left.ordering, sm.right.ordering,
		sm.nullEquality, &sm.datumAlloc, evalCtx,
	)
	if err != nil {
		return nil, nil, &execinfrapb.ProducerMetadata{Err: err}
	}
	var leftGroup, rightGroup []rowenc.EncDatumRow
	if cmp <= 0 {
		leftGroup = sm.leftGroup
		sm.leftGroup = nil
	}
	if cmp >= 0 {
		rightGroup = sm.rightGroup
		sm.rightGroup = nil
	}
	return leftGroup, rightGroup, nil
}

// CompareEncDatumRowForMerge compares two EncDatumRows for merging.
// When merging two streams and preserving the order (as in a MergeSort or
// a MergeJoin) compare the head of the streams, emitting the one that sorts
// first. It allows for the EncDatumRow to be nil if one of the streams is
// exhausted (and hence nil). CompareEncDatumRowForMerge returns 0 when both
// rows are nil, and a nil row is considered greater than any non-nil row.
// CompareEncDatumRowForMerge assumes that the two rows have the same columns
// in the same orders, but can handle different ordering directions. It takes
// a DatumAlloc which is used for decoding if any underlying EncDatum is not
// yet decoded.
func CompareEncDatumRowForMerge(
	lhsTypes []*types.T,
	lhs, rhs rowenc.EncDatumRow,
	leftOrdering, rightOrdering colinfo.ColumnOrdering,
	nullEquality bool,
	da *rowenc.DatumAlloc,
	evalCtx *tree.EvalContext,
) (int, error) {
	if lhs == nil && rhs == nil {
		return 0, nil
	}
	if lhs == nil {
		return 1, nil
	}
	if rhs == nil {
		return -1, nil
	}
	if len(leftOrdering) != len(rightOrdering) {
		return 0, errors.Errorf(
			"cannot compare two EncDatumRow types that have different length ColumnOrderings",
		)
	}

	for i, ord := range leftOrdering {
		lIdx := ord.ColIdx
		rIdx := rightOrdering[i].ColIdx
		// If both datums are NULL, we need to follow SQL semantics where
		// they are not equal. This differs from our datum semantics where
		// they are equal. In the case where we want to consider NULLs to be
		// equal, we continue and skip to the next datums in the row.
		if lhs[lIdx].IsNull() && rhs[rIdx].IsNull() {
			if !nullEquality {
				// We can return either -1 or 1, it does not change the behavior.
				return -1, nil
			}
			continue
		}
		cmp, err := lhs[lIdx].Compare(lhsTypes[lIdx], da, evalCtx, &rhs[rIdx])
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			if leftOrdering[i].Direction == encoding.Descending {
				cmp = -cmp
			}
			return cmp, nil
		}
	}
	return 0, nil
}

func (sm *streamMerger) close(ctx context.Context) {
	sm.left.close(ctx)
	sm.right.close(ctx)
}

// makeStreamMerger creates a streamMerger, joining rows from leftSource with
// rows from rightSource.
//
// All metadata from the sources is forwarded to metadataSink.
func makeStreamMerger(
	leftSource execinfra.RowSource,
	leftOrdering colinfo.ColumnOrdering,
	rightSource execinfra.RowSource,
	rightOrdering colinfo.ColumnOrdering,
	nullEquality bool,
	memMonitor *mon.BytesMonitor,
) (streamMerger, error) {
	if len(leftOrdering) != len(rightOrdering) {
		return streamMerger{}, errors.Errorf(
			"ordering lengths don't match: %d and %d", len(leftOrdering), len(rightOrdering))
	}
	for i, ord := range leftOrdering {
		if ord.Direction != rightOrdering[i].Direction {
			return streamMerger{}, errors.New("ordering mismatch")
		}
	}

	return streamMerger{
		left:         makeStreamGroupAccumulator(leftSource, leftOrdering, memMonitor),
		right:        makeStreamGroupAccumulator(rightSource, rightOrdering, memMonitor),
		nullEquality: nullEquality,
	}, nil
}
