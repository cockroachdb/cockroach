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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/pkg/errors"
)

type workloadReader struct {
	conv *rowConverter
	kvCh chan kvBatch
}

var _ inputConverter = &workloadReader{}

func newWorkloadReader(
	kvCh chan kvBatch, table *sqlbase.TableDescriptor, evalCtx *tree.EvalContext,
) (*workloadReader, error) {
	conv, err := newRowConverter(table, evalCtx, kvCh)
	if err != nil {
		return nil, err
	}
	return &workloadReader{kvCh: kvCh, conv: conv}, nil
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
	var alloc sqlbase.DatumAlloc

	numFiles := float32(len(dataFiles))
	var filesCompleted int
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

		// how is this reader, before starting this file.
		initialProgress := float32(filesCompleted) / numFiles
		numBatches := conf.BatchEnd - conf.BatchBegin
		var rows int64
		lastProgress := rows
		for b := conf.BatchBegin; b < conf.BatchEnd; b++ {
			if rows-lastProgress > 10000 {
				// how far we are on this file
				fileProgress := float32(b-conf.BatchBegin) / float32(numBatches)
				progress := initialProgress + fileProgress/numFiles
				if err := progressFn(progress); err != nil {
					return err
				}
				lastProgress = rows
			}
			for _, row := range t.InitialRows.Batch(int(b)) {
				rows++
				for i, value := range row {
					converted, err := makeDatumFromRaw(&alloc, value, w.conv.visibleColTypes[i], w.conv.evalCtx)
					if err != nil {
						return err
					}
					w.conv.datums[i] = converted
				}
				if err := w.conv.row(ctx, inputIdx, rows); err != nil {
					return err
				}
			}
		}
		filesCompleted++
	}
	return w.conv.sendBatch(ctx)
}
