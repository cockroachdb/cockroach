// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importer

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
)

type workloadReader struct {
	semaCtx     *tree.SemaContext
	evalCtx     *tree.EvalContext
	table       catalog.TableDescriptor
	kvCh        chan row.KVBatch
	parallelism int
}

var _ inputConverter = &workloadReader{}

func newWorkloadReader(
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	table catalog.TableDescriptor,
	kvCh chan row.KVBatch,
	parallelism int,
) *workloadReader {
	return &workloadReader{semaCtx: semaCtx, evalCtx: evalCtx, table: table, kvCh: kvCh, parallelism: parallelism}
}

func (w *workloadReader) start(ctx ctxgroup.Group) {
}

// makeDatumFromColOffset tries to fast-path a few workload-generated types into
// directly datums, to dodge making a string and then the parsing it.
func makeDatumFromColOffset(
	alloc *tree.DatumAlloc, hint *types.T, evalCtx *tree.EvalContext, col coldata.Vec, rowIdx int,
) (tree.Datum, error) {
	if col.Nulls().NullAt(rowIdx) {
		return tree.DNull, nil
	}
	switch t := col.Type(); col.CanonicalTypeFamily() {
	case types.BoolFamily:
		return tree.MakeDBool(tree.DBool(col.Bool()[rowIdx])), nil
	case types.IntFamily:
		switch t.Width() {
		case 0, 64:
			switch hint.Family() {
			case types.IntFamily:
				return alloc.NewDInt(tree.DInt(col.Int64()[rowIdx])), nil
			case types.DecimalFamily:
				d := *apd.New(col.Int64()[rowIdx], 0)
				return alloc.NewDDecimal(tree.DDecimal{Decimal: d}), nil
			case types.DateFamily:
				date, err := pgdate.MakeDateFromUnixEpoch(col.Int64()[rowIdx])
				if err != nil {
					return nil, err
				}
				return alloc.NewDDate(tree.DDate{Date: date}), nil
			}
		case 16:
			switch hint.Family() {
			case types.IntFamily:
				return alloc.NewDInt(tree.DInt(col.Int16()[rowIdx])), nil
			}
		}
	case types.FloatFamily:
		switch hint.Family() {
		case types.FloatFamily:
			return alloc.NewDFloat(tree.DFloat(col.Float64()[rowIdx])), nil
		case types.DecimalFamily:
			var d apd.Decimal
			if _, err := d.SetFloat64(col.Float64()[rowIdx]); err != nil {
				return nil, err
			}
			return alloc.NewDDecimal(tree.DDecimal{Decimal: d}), nil
		}
	case types.BytesFamily:
		switch hint.Family() {
		case types.BytesFamily:
			return alloc.NewDBytes(tree.DBytes(col.Bytes().Get(rowIdx))), nil
		case types.StringFamily:
			data := col.Bytes().Get(rowIdx)
			str := *(*string)(unsafe.Pointer(&data))
			return alloc.NewDString(tree.DString(str)), nil
		default:
			data := col.Bytes().Get(rowIdx)
			str := *(*string)(unsafe.Pointer(&data))
			return rowenc.ParseDatumStringAs(hint, str, evalCtx)
		}
	}
	return nil, errors.Errorf(
		`don't know how to interpret %s column as %s`, col.Type(), hint)
}

func (w *workloadReader) readFiles(
	ctx context.Context,
	dataFiles map[int32]string,
	_ map[int32]int64,
	_ roachpb.IOFileFormat,
	_ cloud.ExternalStorageFactory,
	_ security.SQLUsername,
) error {

	wcs := make([]*WorkloadKVConverter, 0, len(dataFiles))
	for fileID, fileName := range dataFiles {
		conf, err := parseWorkloadConfig(fileName)
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
			flags := f.Flags()
			if err := flags.Parse(conf.Flags); err != nil {
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
			return errors.Errorf(`unknown table %s for generator %s`, conf.Table, meta.Name)
		}

		wc := NewWorkloadKVConverter(
			fileID, w.table, t.InitialRows, int(conf.BatchBegin), int(conf.BatchEnd), w.kvCh)
		wcs = append(wcs, wc)
	}

	for _, wc := range wcs {
		if err := ctxgroup.GroupWorkers(ctx, w.parallelism, func(ctx context.Context, _ int) error {
			evalCtx := w.evalCtx.Copy()
			return wc.Worker(ctx, evalCtx, w.semaCtx)
		}); err != nil {
			return err
		}
	}
	return nil
}

// WorkloadKVConverter converts workload.BatchedTuples to []roachpb.KeyValues.
type WorkloadKVConverter struct {
	tableDesc      catalog.TableDescriptor
	rows           workload.BatchedTuples
	batchIdxAtomic int64
	batchEnd       int
	kvCh           chan row.KVBatch

	// For progress reporting
	fileID                int32
	totalBatches          float32
	finishedBatchesAtomic int64
}

// NewWorkloadKVConverter returns a WorkloadKVConverter for the given table and
// range of batches, emitted converted kvs to the given channel.
func NewWorkloadKVConverter(
	fileID int32,
	tableDesc catalog.TableDescriptor,
	rows workload.BatchedTuples,
	batchStart, batchEnd int,
	kvCh chan row.KVBatch,
) *WorkloadKVConverter {
	return &WorkloadKVConverter{
		tableDesc:      tableDesc,
		rows:           rows,
		batchIdxAtomic: int64(batchStart) - 1,
		batchEnd:       batchEnd,
		kvCh:           kvCh,
		totalBatches:   float32(batchEnd - batchStart),
		fileID:         fileID,
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
	ctx context.Context, evalCtx *tree.EvalContext, semaCtx *tree.SemaContext,
) error {
	conv, err := row.NewDatumRowConverter(ctx, semaCtx, w.tableDesc, nil, /* targetColNames */
		evalCtx, w.kvCh, nil /* seqChunkProvider */, nil /* metrics */)
	if err != nil {
		return err
	}
	conv.KvBatch.Source = w.fileID
	conv.FractionFn = func() float32 {
		return float32(atomic.LoadInt64(&w.finishedBatchesAtomic)) / w.totalBatches
	}
	var alloc tree.DatumAlloc
	var a bufalloc.ByteAllocator
	cb := coldata.NewMemBatchWithCapacity(nil /* typs */, 0 /* capacity */, coldata.StandardColumnFactory)

	for {
		batchIdx := int(atomic.AddInt64(&w.batchIdxAtomic, 1))
		if batchIdx >= w.batchEnd {
			break
		}
		a = a.Truncate()
		w.rows.FillBatch(batchIdx, cb, &a)
		for rowIdx, numRows := 0, cb.Length(); rowIdx < numRows; rowIdx++ {
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
		atomic.AddInt64(&w.finishedBatchesAtomic, 1)
	}
	return conv.SendBatch(ctx)
}

var errNotWorkloadURI = errors.New("not a workload URI")

// parseWorkloadConfig parses a workload config URI to a config.
func parseWorkloadConfig(fileName string) (workloadConfig, error) {
	c := workloadConfig{}

	uri, err := url.Parse(fileName)
	if err != nil {
		return c, err
	}

	if uri.Scheme != "workload" {
		return c, errNotWorkloadURI
	}
	pathParts := strings.Split(strings.Trim(uri.Path, `/`), `/`)
	if len(pathParts) != 3 {
		return c, errors.Errorf(
			`path must be of the form /<format>/<generator>/<table>: %s`, uri.Path)
	}
	c.Format, c.Generator, c.Table = pathParts[0], pathParts[1], pathParts[2]
	q := uri.Query()
	if _, ok := q[`version`]; !ok {
		return c, errors.New(`parameter version is required`)
	}
	c.Version = q.Get(`version`)
	q.Del(`version`)
	if s := q.Get(`row-start`); len(s) > 0 {
		q.Del(`row-start`)
		var err error
		if c.BatchBegin, err = strconv.ParseInt(s, 10, 64); err != nil {
			return c, err
		}
	}
	if e := q.Get(`row-end`); len(e) > 0 {
		q.Del(`row-end`)
		var err error
		if c.BatchEnd, err = strconv.ParseInt(e, 10, 64); err != nil {
			return c, err
		}
	}
	for k, vs := range q {
		for _, v := range vs {
			c.Flags = append(c.Flags, `--`+k+`=`+v)
		}
	}
	return c, nil
}

type workloadConfig struct {
	Generator  string
	Version    string
	Table      string
	Flags      []string
	Format     string
	BatchBegin int64
	BatchEnd   int64
}
