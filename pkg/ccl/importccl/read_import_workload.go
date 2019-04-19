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
	"fmt"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/pkg/errors"
)

type workloadReader struct {
	newEvalCtx func() *tree.EvalContext
	table      *sqlbase.TableDescriptor
	kvCh       chan []roachpb.KeyValue
}

var _ inputConverter = &workloadReader{}

func newWorkloadReader(
	kvCh chan []roachpb.KeyValue, table *sqlbase.TableDescriptor, newEvalCtx func() *tree.EvalContext,
) *workloadReader {
	return &workloadReader{newEvalCtx: newEvalCtx, table: table, kvCh: kvCh}
}

func (w *workloadReader) start(ctx ctxgroup.Group) {
}

func (w *workloadReader) inputFinished(ctx context.Context) {
	close(w.kvCh)
}

// makeDatumFromRaw tries to fast-path a few workload-generated types into
// directly datums, to dodge making a string and then the parsing it.
func makeDatumFromRaw(
	alloc *sqlbase.DatumAlloc, datum interface{}, hint *types.T, evalCtx *tree.EvalContext,
) (tree.Datum, error) {
	if datum == nil {
		return tree.DNull, nil
	}
	switch t := datum.(type) {
	case int:
		return alloc.NewDInt(tree.DInt(t)), nil
	case int64:
		return alloc.NewDInt(tree.DInt(t)), nil
	case []byte:
		return alloc.NewDBytes(tree.DBytes(t)), nil
	case time.Time:
		switch hint.Family() {
		case types.TimestampTZFamily:
			return tree.MakeDTimestampTZ(t, time.Microsecond), nil
		case types.TimestampFamily:
			return tree.MakeDTimestamp(t, time.Microsecond), nil
		}
	case tree.DString:
		return alloc.NewDString(t), nil
	case string:
		return tree.ParseDatumStringAs(hint, t, evalCtx)
	}
	return tree.ParseDatumStringAs(hint, fmt.Sprint(datum), evalCtx)
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

		wc := NewWorkloadKVConverter(
			w.table, t.InitialRows, int(conf.BatchBegin), int(conf.BatchEnd), w.kvCh)
		if err := ctxgroup.GroupWorkers(ctx, runtime.NumCPU(), func(ctx context.Context) error {
			evalCtx := w.newEvalCtx()
			var alloc sqlbase.DatumAlloc
			return wc.Worker(ctx, evalCtx, &alloc, finishedBatchFn)
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
	ctx context.Context, evalCtx *tree.EvalContext, alloc *sqlbase.DatumAlloc, finishedBatchFn func(),
) error {
	conv, err := newRowConverter(w.tableDesc, evalCtx, w.kvCh)
	if err != nil {
		return err
	}

	for {
		batchIdx := int(atomic.AddInt64(&w.batchIdxAtomic, 1))
		if batchIdx >= w.batchEnd {
			break
		}
		for rowIdx, rows := range w.rows.Batch(batchIdx) {
			for colIdx, datum := range rows {
				converted, err := makeDatumFromRaw(alloc, datum, conv.visibleColTypes[colIdx], evalCtx)
				if err != nil {
					return err
				}
				conv.datums[colIdx] = converted
			}
			// `conv.row` uses these as arguments to GenerateUniqueID to generate
			// hidden primary keys, when necessary. We want them to be ascending per
			// batch (to reduce overlap in the resulting kvs) and non-conflicting
			// (because of primary key uniqueness). The ids that come out of
			// GenerateUniqueID are sorted by (fileIdx, timestamp) and unique as long
			// as the two inputs are a unique combo, so using the index of the batch
			// within the table and the index of the row within the batch should do
			// what we want.
			fileIdx, timestamp := int32(batchIdx), int64(rowIdx)
			if err := conv.row(ctx, fileIdx, timestamp); err != nil {
				return err
			}
		}
		finishedBatchFn()
	}
	return conv.sendBatch(ctx)
}
