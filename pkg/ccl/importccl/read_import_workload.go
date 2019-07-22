// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"context"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	sqltypes "github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/pkg/errors"
)

type workloadReader struct {
	evalCtx *tree.EvalContext
	table   *sqlbase.TableDescriptor
	kvCh    chan []roachpb.KeyValue
}

var _ inputConverter = &workloadReader{}

func newWorkloadReader(
	kvCh chan []roachpb.KeyValue, table *sqlbase.TableDescriptor, evalCtx *tree.EvalContext,
) *workloadReader {
	return &workloadReader{evalCtx: evalCtx, table: table, kvCh: kvCh}
}

func (w *workloadReader) start(ctx ctxgroup.Group) {
}

func (w *workloadReader) inputFinished(ctx context.Context) {
	close(w.kvCh)
}

// makeDatumFromColOffset tries to fast-path a few workload-generated types into
// directly datums, to dodge making a string and then the parsing it.
func makeDatumFromColOffset(
	alloc *sqlbase.DatumAlloc,
	hint *sqltypes.T,
	evalCtx *tree.EvalContext,
	col coldata.Vec,
	rowIdx int,
) (tree.Datum, error) {
	if col.Nulls().NullAt64(uint64(rowIdx)) {
		return tree.DNull, nil
	}
	switch col.Type() {
	case types.Bool:
		return tree.MakeDBool(tree.DBool(col.Bool()[rowIdx])), nil
	case types.Int64:
		switch hint.Family() {
		case sqltypes.IntFamily:
			return alloc.NewDInt(tree.DInt(col.Int64()[rowIdx])), nil
		case sqltypes.DecimalFamily:
			d := *apd.New(col.Int64()[rowIdx], 0)
			return alloc.NewDDecimal(tree.DDecimal{Decimal: d}), nil
		}
	case types.Float64:
		switch hint.Family() {
		case sqltypes.FloatFamily:
			return alloc.NewDFloat(tree.DFloat(col.Float64()[rowIdx])), nil
		case sqltypes.DecimalFamily:
			var d apd.Decimal
			if _, err := d.SetFloat64(col.Float64()[rowIdx]); err != nil {
				return nil, err
			}
			return alloc.NewDDecimal(tree.DDecimal{Decimal: d}), nil
		}
	case types.Bytes:
		switch hint.Family() {
		case sqltypes.BytesFamily:
			return alloc.NewDBytes(tree.DBytes(col.Bytes()[rowIdx])), nil
		case sqltypes.StringFamily:
			data := col.Bytes()[rowIdx]
			str := *(*string)(unsafe.Pointer(&data))
			return alloc.NewDString(tree.DString(str)), nil
		default:
			data := col.Bytes()[rowIdx]
			str := *(*string)(unsafe.Pointer(&data))
			return tree.ParseDatumStringAs(hint, str, evalCtx)
		}
	}
	return nil, errors.Errorf(
		`don't know how to interpret %s column as %s`, col.Type().GoTypeName(), hint)
}

func (w *workloadReader) readFiles(
	ctx context.Context,
	dataFiles map[int32]string,
	_ roachpb.IOFileFormat,
	progressFn func(float32) error,
	_ *cluster.Settings,
) error {
	progress := jobs.ProgressUpdateBatcher{Report: func(ctx context.Context, pct float32) error {
		return progressFn(pct)
	}}

	var numTotalBatches int64
	var finishedBatchesAtomic int64
	const batchesPerProgress = 1000
	finishedBatchFn := func() {
		finishedBatches := atomic.AddInt64(&finishedBatchesAtomic, 1)
		if finishedBatches%batchesPerProgress == 0 {
			progressDelta := float32(batchesPerProgress) / float32(numTotalBatches)
			// TODO(dan): (*ProgressUpdateBatcher).Add also has logic to only report
			// job progress periodically. See if we can unify them (potentially by
			// making Add cheap enough to call every time inside `finishedBatchFn`, if
			// it's not already). It also seems to me that it would be more natural
			// for Add to take the overall progress than a delta.
			if err := progress.Add(ctx, progressDelta); err != nil {
				log.Warningf(ctx, "failed to update progress: %+v", err)
			}
		}
	}

	wcs := make([]*WorkloadKVConverter, 0, len(dataFiles))
	for _, fileName := range dataFiles {
		file, err := url.Parse(fileName)
		if err != nil {
			return err
		}
		conf, err := storageccl.ParseWorkloadConfig(file)
		if err != nil {
			return err
		}
		meta, err := workload.Get(conf.Generator)
		if err != nil {
			return err
		}
		// Different versions of the workload could generate different data, so
		// disallow this.
		if meta.Version != conf.Version {
			return errors.Errorf(
				`expected %s version "%s" but got "%s"`, meta.Name, conf.Version, meta.Version)
		}
		gen := meta.New()
		if f, ok := gen.(workload.Flagser); ok {
			if err := f.Flags().Parse(conf.Flags); err != nil {
				return errors.Wrapf(err, `parsing parameters %s`, strings.Join(conf.Flags, ` `))
			}
		}
		var t workload.Table
		for _, tbl := range gen.Tables() {
			if tbl.Name == conf.Table {
				t = tbl
				break
			}
		}
		if t.Name == `` {
			return errors.Wrapf(err, `unknown table %s for generator %s`, conf.Table, meta.Name)
		}

		numTotalBatches += conf.BatchEnd - conf.BatchBegin
		wc := NewWorkloadKVConverter(
			w.table, t.InitialRows, int(conf.BatchBegin), int(conf.BatchEnd), w.kvCh)
		wcs = append(wcs, wc)
	}
	for _, wc := range wcs {
		if err := ctxgroup.GroupWorkers(ctx, runtime.NumCPU(), func(ctx context.Context) error {
			evalCtx := w.evalCtx.Copy()
			return wc.Worker(ctx, evalCtx, finishedBatchFn)
		}); err != nil {
			return err
		}
	}
	if err := progress.Done(ctx); err != nil {
		log.Warningf(ctx, "failed to update progress: %+v", err)
	}
	return nil
}

// WorkloadKVConverter converts workload.BatchedTuples to []roachpb.KeyValues.
type WorkloadKVConverter struct {
	tableDesc      *sqlbase.TableDescriptor
	rows           workload.BatchedTuples
	batchIdxAtomic int64
	batchEnd       int
	kvCh           chan []roachpb.KeyValue
}

// NewWorkloadKVConverter returns a WorkloadKVConverter for the given table and
// range of batches, emitted converted kvs to the given channel.
func NewWorkloadKVConverter(
	tableDesc *sqlbase.TableDescriptor,
	rows workload.BatchedTuples,
	batchStart, batchEnd int,
	kvCh chan []roachpb.KeyValue,
) *WorkloadKVConverter {
	return &WorkloadKVConverter{
		tableDesc:      tableDesc,
		rows:           rows,
		batchIdxAtomic: int64(batchStart) - 1,
		batchEnd:       batchEnd,
		kvCh:           kvCh,
	}
}

// Worker can be called concurrently to create multiple workers to process
// batches in order. This keeps concurrently running workers ~adjacent batches
// at any given moment (as opposed to handing large ranges of batches to each
// worker, e.g. 0-999 to worker 1, 1000-1999 to worker 2, etc). This property is
// relevant when ordered workload batches produce ordered PK data, since the
// workers feed into a shared kvCH so then contiguous blocks of PK data will
// usually be buffered together and thus batched together in the SST builder,
// minimzing the amount of overlapping SSTs ingested.
//
// This worker needs its own EvalContext and DatumAlloc.
func (w *WorkloadKVConverter) Worker(
	ctx context.Context, evalCtx *tree.EvalContext, finishedBatchFn func(),
) error {
	conv, err := row.NewDatumRowConverter(w.tableDesc, nil /* targetColNames */, evalCtx, w.kvCh)
	if err != nil {
		return err
	}

	var alloc sqlbase.DatumAlloc
	var a bufalloc.ByteAllocator
	cb := coldata.NewMemBatchWithSize(nil, 0)

	for {
		batchIdx := int(atomic.AddInt64(&w.batchIdxAtomic, 1))
		if batchIdx >= w.batchEnd {
			break
		}
		a = a[:0]
		w.rows.FillBatch(batchIdx, cb, &a)
		for rowIdx, numRows := 0, int(cb.Length()); rowIdx < numRows; rowIdx++ {
			for colIdx, col := range cb.ColVecs() {
				// TODO(dan): This does a type switch once per-datum. Reduce this to
				// a one-time switch per column.
				converted, err := makeDatumFromColOffset(
					&alloc, conv.VisibleColTypes[colIdx], evalCtx, col, rowIdx)
				if err != nil {
					return err
				}
				conv.Datums[colIdx] = converted
			}
			// `conv.Row` uses these as arguments to GenerateUniqueID to generate
			// hidden primary keys, when necessary. We want them to be ascending per
			// batch (to reduce overlap in the resulting kvs) and non-conflicting
			// (because of primary key uniqueness). The ids that come out of
			// GenerateUniqueID are sorted by (fileIdx, timestamp) and unique as long
			// as the two inputs are a unique combo, so using the index of the batch
			// within the table and the index of the row within the batch should do
			// what we want.
			fileIdx, timestamp := int32(batchIdx), int64(rowIdx)
			if err := conv.Row(ctx, fileIdx, timestamp); err != nil {
				return err
			}
		}
		finishedBatchFn()
	}
	return conv.SendBatch(ctx)
}
