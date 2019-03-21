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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/pkg/errors"
)

type workloadReader struct {
	newEvalCtx func() *tree.EvalContext
	table      *sqlbase.TableDescriptor
	kvCh       chan kvBatch
}

type workloadReaderWorker struct {
	workloadTable    workload.Table
	w                *workloadReader
	progress         *jobs.ProgressUpdateBatcher
	progressPerBatch float32
	fileNum          int32 // used for generating unique row-ids
	endBatch         int64
	curBatchAtomic   int64
}

var _ inputConverter = &workloadReader{}

func newWorkloadReader(
	kvCh chan kvBatch, table *sqlbase.TableDescriptor, newEvalCtx func() *tree.EvalContext,
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
	alloc *sqlbase.DatumAlloc, datum interface{}, hint types.T, evalCtx *tree.EvalContext,
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
		switch hint {
		case types.TimestampTZ:
			return tree.MakeDTimestampTZ(t, time.Microsecond), nil
		case types.Timestamp:
			return tree.MakeDTimestamp(t, time.Microsecond), nil
		}
	case string:
		return tree.ParseDatumStringAs(hint, t, evalCtx)
	}
	return tree.ParseDatumStringAs(hint, fmt.Sprint(datum), evalCtx)
}

func (w *workloadReader) readFiles(
	ctx context.Context,
	dataFiles map[int32]string,
	format roachpb.IOFileFormat,
	progressFn func(float32) error,
	settings *cluster.Settings,
) error {

	progress := jobs.ProgressUpdateBatcher{
		Report: func(ctx context.Context, pct float32) error {
			return progressFn(pct)
		}}

	for inputIdx, fileName := range dataFiles {
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

		workers := workloadReaderWorker{
			w:                w,
			workloadTable:    t,
			progress:         &progress,
			progressPerBatch: (1.0 / float32(conf.BatchEnd-conf.BatchBegin)) / float32(len(dataFiles)),
			fileNum:          inputIdx,
			curBatchAtomic:   conf.BatchBegin - 1,
			endBatch:         conf.BatchEnd,
		}

		return ctxgroup.GroupWorkers(ctx, runtime.NumCPU(), workers.run)
	}
	if err := progress.Done(ctx); err != nil {
		log.Warningf(ctx, "failed to update progress: %+v", err)
	}
	return nil
}

// run can be called concurrently to create multiple workers that coordinate via
// w.curBatchAtomic to process batches in order. This keeps concurrently running
// workers ~adjacent batches at any given moment (as opposed to handing large
// ranges of batches to each worker, e.g. 0-999 to worker 1, 1000-1999 to worker
// 2, etc). This property is relevant when ordered workload batches produce
// ordered PK data, since the workers feed into a shared kvCH so then contiguous
// blocks of PK data will usually be buffered together and thus batched together
// in the SST builder, minimzing the amount of overlapping SSTs ingested.
func (w *workloadReaderWorker) run(ctx context.Context) error {
	evalCtx := w.w.newEvalCtx()
	conv, err := newRowConverter(w.w.table, evalCtx, w.w.kvCh)
	if err != nil {
		return err
	}
	var alloc sqlbase.DatumAlloc

	var pendingProgressBatches int
	var rowIdx int64
	for {
		b := atomic.AddInt64(&w.curBatchAtomic, 1)
		if b >= w.endBatch {
			break
		}
		pendingProgressBatches++
		for _, values := range w.workloadTable.InitialRows.Batch(int(b)) {
			rowIdx++
			for i, value := range values {
				converted, err := makeDatumFromRaw(&alloc, value, conv.visibleColTypes[i], evalCtx)
				if err != nil {
					return err
				}
				conv.datums[i] = converted
			}
			if err := conv.row(ctx, w.fileNum, rowIdx); err != nil {
				return err
			}
		}
		if pendingProgressBatches > 1000 {
			if err := w.progress.Add(ctx, w.progressPerBatch*float32(pendingProgressBatches)); err != nil {
				log.Warningf(ctx, "failed to update progress: %+v", err)
			}
			pendingProgressBatches = 0
		}
	}
	return conv.sendBatch(ctx)
}
