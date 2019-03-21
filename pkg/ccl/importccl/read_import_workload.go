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
	evalCtx *tree.EvalContext
	table   *sqlbase.TableDescriptor
	kvCh    chan kvBatch
}

var _ inputConverter = &workloadReader{}

func newWorkloadReader(
	kvCh chan kvBatch, table *sqlbase.TableDescriptor, evalCtx *tree.EvalContext,
) *workloadReader {
	return &workloadReader{evalCtx: evalCtx, table: table, kvCh: kvCh}
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

		batches := float32(conf.BatchEnd - conf.BatchBegin)
		begin, end := int(conf.BatchBegin), int(conf.BatchEnd)
		files := float32(len(dataFiles))
		workers := runtime.NumCPU()

		g := ctxgroup.WithContext(ctx)
		// Each worker thread will process every i'th batch, e.g. with 4 workers,
		// worker 0 will process batches 0, 4, 8, etc. This keeps the concurrently
		// running workers, which are all feeding into the same kvCh which is
		// buffered, batched and sorted into SSTs, working on ~adjacent batches at
		// any given moment (as opposed to handing large ranges of batches to each
		// thread, e.g. 0-999 to thread 1, 1000-1999 to thread 2, etc). This
		// property is relevant when ordered workload batches produce ordered PK
		// data, since then contiguous blocks of PK data will usually be buffered
		// together and then ingested together in the same SST, minimzing the amount
		// of overlapping SSTs.
		// TODO(dt): on very long imports, these could drift. We might want to check
		// a shared low-watermark periodically and call runtime.Gosched() if we see
		// that we're significantly above it.
		for i := 0; i < workers; i++ {
			thread := i
			g.GoCtx(func(ctx context.Context) error {
				conv, err := newRowConverter(w.table, w.evalCtx, w.kvCh)
				if err != nil {
					return err
				}
				var alloc sqlbase.DatumAlloc

				var pendingBatches int
				var rowIdx int64
				for b := begin + thread; b < end; b += workers {
					// log.Infof(ctx, "%s thread %d of %d importing batch %d", t.Name, thread, workers, b)

					pendingBatches++
					for _, values := range t.InitialRows.Batch(b) {
						rowIdx++
						for i, value := range values {
							converted, err := makeDatumFromRaw(&alloc, value, conv.visibleColTypes[i], w.evalCtx)
							if err != nil {
								return err
							}
							conv.datums[i] = converted
						}
						if err := conv.row(ctx, inputIdx, rowIdx); err != nil {
							return err
						}
					}
					if pendingBatches > 1000 {
						fileProgressDelta := float32(pendingBatches) / batches
						procProgressDelta := fileProgressDelta / files
						if err := progress.Add(ctx, procProgressDelta); err != nil {
							log.Warningf(ctx, "failed to update progress: %+v", err)
						}
						pendingBatches = 0
					}
				}
				return conv.sendBatch(ctx)
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
	}
	if err := progress.Done(ctx); err != nil {
		log.Warningf(ctx, "failed to update progress: %+v", err)
	}
	return nil
}
