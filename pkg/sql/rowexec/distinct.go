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

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/cockroach/pkg/util/stringarena"
	"github.com/cockroachdb/errors"
)

// distinct is the physical processor implementation of the DISTINCT relational operator.
type distinct struct {
	execinfra.ProcessorBase

	input            execinfra.RowSource
	types            []*types.T
	haveLastGroupKey bool
	lastGroupKey     rowenc.EncDatumRow
	arena            stringarena.Arena
	seen             map[string]struct{}
	distinctCols     struct {
		// ordered and nonOrdered are such that their union determines the set
		// of distinct columns and their intersection is empty.
		ordered    []uint32
		nonOrdered []uint32
	}
	memAcc           mon.BoundAccount
	datumAlloc       tree.DatumAlloc
	scratch          []byte
	nullsAreDistinct bool
	nullCount        uint32
	errorOnDup       string
}

// sortedDistinct is a specialized distinct that can be used when all of the
// distinct columns are also ordered.
type sortedDistinct struct {
	*distinct
}

var _ execinfra.Processor = &distinct{}
var _ execinfra.RowSource = &distinct{}
var _ execinfra.OpNode = &distinct{}

const distinctProcName = "distinct"

var _ execinfra.Processor = &sortedDistinct{}
var _ execinfra.RowSource = &sortedDistinct{}
var _ execinfra.OpNode = &sortedDistinct{}

const sortedDistinctProcName = "sorted distinct"

// newDistinct instantiates a new Distinct processor.
func newDistinct(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.DistinctSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.RowSourcedProcessor, error) {
	if len(spec.DistinctColumns) == 0 {
		return nil, errors.AssertionFailedf("0 distinct columns specified for distinct processor")
	}

	nonOrderedCols := make([]uint32, 0, len(spec.DistinctColumns)-len(spec.OrderedColumns))
	for _, col := range spec.DistinctColumns {
		ordered := false
		for _, ordCol := range spec.OrderedColumns {
			if col == ordCol {
				ordered = true
				break
			}
		}
		if !ordered {
			nonOrderedCols = append(nonOrderedCols, col)
		}
	}

	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "distinct-mem")
	d := &distinct{
		input:            input,
		memAcc:           memMonitor.MakeBoundAccount(),
		types:            input.OutputTypes(),
		nullsAreDistinct: spec.NullsAreDistinct,
		errorOnDup:       spec.ErrorOnDup,
	}
	d.distinctCols.ordered = spec.OrderedColumns
	d.distinctCols.nonOrdered = nonOrderedCols

	var returnProcessor execinfra.RowSourcedProcessor = d
	if len(nonOrderedCols) == 0 {
		// We can use the faster sortedDistinct processor.
		sd := &sortedDistinct{distinct: d}
		returnProcessor = sd
	}

	if err := d.Init(
		d, post, d.types, flowCtx, processorID, output, memMonitor, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{d.input},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				d.close()
				return nil
			},
		}); err != nil {
		return nil, err
	}
	d.lastGroupKey = d.OutputHelper.RowAlloc.AllocRow(len(d.types))
	d.haveLastGroupKey = false
	// If we set up the arena when d is created, the pointer to the memAcc
	// will be changed because the sortedDistinct case makes a copy of d.
	// So we have to set up the account here.
	d.arena = stringarena.Make(&d.memAcc)

	if execinfra.ShouldCollectStats(ctx, flowCtx) {
		d.input = newInputStatCollector(d.input)
		d.ExecStatsForTrace = d.execStatsForTrace
	}

	return returnProcessor, nil
}

// Start is part of the RowSource interface.
func (d *distinct) Start(ctx context.Context) {
	ctx = d.StartInternal(ctx, distinctProcName)
	d.input.Start(ctx)
}

// Start is part of the RowSource interface.
func (d *sortedDistinct) Start(ctx context.Context) {
	ctx = d.StartInternal(ctx, sortedDistinctProcName)
	d.input.Start(ctx)
}

func (d *distinct) matchLastGroupKey(row rowenc.EncDatumRow) (bool, error) {
	if !d.haveLastGroupKey {
		return false, nil
	}
	for _, colIdx := range d.distinctCols.ordered {
		res, err := d.lastGroupKey[colIdx].Compare(
			d.types[colIdx], &d.datumAlloc, d.EvalCtx, &row[colIdx],
		)
		if res != 0 || err != nil {
			return false, err
		}

		// If null values are treated as distinct from one another, then a grouping
		// column with a NULL value means that the row should never match any other
		// row.
		if d.nullsAreDistinct && d.lastGroupKey[colIdx].IsNull() {
			return false, nil
		}
	}
	return true, nil
}

// encode appends the encoding of non-ordered columns, which we use as a key in
// our 'seen' set.
func (d *distinct) encode(appendTo []byte, row rowenc.EncDatumRow) ([]byte, error) {
	var err error
	foundNull := false
	for _, colIdx := range d.distinctCols.nonOrdered {
		datum := row[colIdx]
		// We might allocate tree.Datums when hashing the row, so we'll ask the
		// fingerprint to account for them. Note that even though we're losing
		// the references to the row (and to the newly allocated datums)
		// shortly, it'll likely take some time before GC reclaims that memory,
		// so we choose the over-accounting route to be safe.
		appendTo, err = datum.Fingerprint(d.Ctx, d.types[colIdx], &d.datumAlloc, appendTo, &d.memAcc)
		if err != nil {
			return nil, err
		}

		// If null values are treated as distinct from one another, then append
		// a unique identifier to the end of the encoding, so that the row will
		// always be in its own distinct group.
		if d.nullsAreDistinct && datum.IsNull() {
			foundNull = true
		}
	}

	if foundNull {
		appendTo = encoding.EncodeUint32Ascending(appendTo, d.nullCount)
		d.nullCount++
	}

	return appendTo, nil
}

func (d *distinct) close() {
	if d.InternalClose() {
		d.memAcc.Close(d.Ctx)
		d.MemMonitor.Stop(d.Ctx)
	}
}

// Next is part of the RowSource interface.
func (d *distinct) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for d.State == execinfra.StateRunning {
		row, meta := d.input.Next()
		if meta != nil {
			if meta.Err != nil {
				d.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			d.MoveToDraining(nil /* err */)
			break
		}

		// If we are processing DISTINCT(x, y) and the input stream is ordered
		// by x, we define x to be our group key. Our seen set at any given time
		// is only the set of all rows with the same group key. The encoding of
		// the row is the key we use in our 'seen' set.
		encoding, err := d.encode(d.scratch, row)
		if err != nil {
			d.MoveToDraining(err)
			break
		}
		d.scratch = encoding[:0]

		// The 'seen' set is reset whenever we find consecutive rows differing on the
		// group key thus avoiding the need to store encodings of all rows.
		matched, err := d.matchLastGroupKey(row)
		if err != nil {
			d.MoveToDraining(err)
			break
		}

		if !matched {
			// Since the sorted distinct columns have changed, we know that all the
			// distinct keys in the 'seen' set will never be seen again. This allows
			// us to keep the current arena block and overwrite strings previously
			// allocated on it, which implies that UnsafeReset() is safe to call here.
			copy(d.lastGroupKey, row)
			d.haveLastGroupKey = true
			if err := d.arena.UnsafeReset(d.Ctx); err != nil {
				d.MoveToDraining(err)
				break
			}
			d.seen = make(map[string]struct{})
		}

		// Check whether row is distinct.
		if _, ok := d.seen[string(encoding)]; ok {
			if d.errorOnDup != "" {
				// Row is a duplicate input to an Upsert operation, so raise
				// an error.
				//
				// TODO(knz): errorOnDup could be passed via redact.Safe() if
				// there was a guarantee that it does not contain PII. Or
				// better yet, the caller would construct an `error` object to
				// return here instead of a string.
				// See: https://github.com/cockroachdb/cockroach/issues/48166
				err = pgerror.Newf(pgcode.CardinalityViolation, "%s", d.errorOnDup)
				d.MoveToDraining(err)
				break
			}
			continue
		}
		s, err := d.arena.AllocBytes(d.Ctx, encoding)
		if err != nil {
			d.MoveToDraining(err)
			break
		}
		d.seen[s] = struct{}{}

		if outRow := d.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, d.DrainHelper()
}

// Next is part of the RowSource interface.
//
// sortedDistinct is simpler than distinct. All it has to do is keep track
// of the last row it saw, emitting if the new row is different.
func (d *sortedDistinct) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for d.State == execinfra.StateRunning {
		row, meta := d.input.Next()
		if meta != nil {
			if meta.Err != nil {
				d.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			d.MoveToDraining(nil /* err */)
			break
		}
		matched, err := d.matchLastGroupKey(row)
		if err != nil {
			d.MoveToDraining(err)
			break
		}
		if matched {
			if d.errorOnDup != "" {
				// Row is a duplicate input to an Upsert operation, so raise an error.
				// TODO(knz): errorOnDup could be passed via redact.Safe() if
				// there was a guarantee that it does not contain PII.
				err = pgerror.Newf(pgcode.CardinalityViolation, "%s", d.errorOnDup)
				d.MoveToDraining(err)
				break
			}
			continue
		}

		d.haveLastGroupKey = true
		copy(d.lastGroupKey, row)

		if outRow := d.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, d.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (d *distinct) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	d.close()
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (d *distinct) execStatsForTrace() *execinfrapb.ComponentStats {
	is, ok := getInputStats(d.input)
	if !ok {
		return nil
	}
	return &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{is},
		Exec: execinfrapb.ExecStats{
			MaxAllocatedMem: optional.MakeUint(uint64(d.MemMonitor.MaximumBytes())),
		},
		Output: d.OutputHelper.Stats(),
	}
}

// ChildCount is part of the execinfra.OpNode interface.
func (d *distinct) ChildCount(verbose bool) int {
	if _, ok := d.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (d *distinct) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := d.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to distinct is not an execinfra.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}
