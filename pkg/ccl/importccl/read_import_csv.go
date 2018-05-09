// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"context"
	"encoding/csv"
	"io"
	"runtime"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

type csvInputReader struct {
	expectedCols int
	recordCh     chan csvRecord
	opts         roachpb.CSVOptions
	tableDesc    *sqlbase.TableDescriptor
}

func newCSVInputReader(
	ctx context.Context,
	opts roachpb.CSVOptions,
	tableDesc *sqlbase.TableDescriptor,
	expectedCols int,
) *csvInputReader {
	return &csvInputReader{
		opts:         opts,
		expectedCols: expectedCols,
		tableDesc:    tableDesc,
		recordCh:     make(chan csvRecord),
	}
}

func (c *csvInputReader) start(
	ctx context.Context, group *errgroup.Group, kvCh chan []roachpb.KeyValue,
) {
	group.Go(func() error {
		sCtx, span := tracing.ChildSpan(ctx, "convertcsv")
		defer tracing.FinishSpan(span)

		defer close(kvCh)
		return groupWorkers(sCtx, runtime.NumCPU(), func(ctx context.Context) error {
			return c.convertRecord(ctx, kvCh)
		})
	})
}

func (c *csvInputReader) inputFinished() {
	close(c.recordCh)
}

func (c *csvInputReader) readFile(
	ctx context.Context,
	input io.Reader,
	inputIdx int32,
	inputName string,
	progressFn func(finished bool) error,
) error {
	done := ctx.Done()
	cr := csv.NewReader(input)
	if c.opts.Comma != 0 {
		cr.Comma = c.opts.Comma
	}
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	cr.Comment = c.opts.Comment

	const batchSize = 500

	batch := csvRecord{
		file:      inputName,
		fileIndex: inputIdx,
		rowOffset: 1,
		r:         make([][]string, 0, batchSize),
	}

	var count int64
	for i := 1; ; i++ {
		record, err := cr.Read()
		if err == io.EOF || len(batch.r) >= batchSize {
			// if the batch isn't empty, we need to flush it.
			if len(batch.r) > 0 {
				select {
				case <-done:
					return ctx.Err()
				case c.recordCh <- batch:
					count += int64(len(batch.r))
				}
			}
			if progressErr := progressFn(err == io.EOF); progressErr != nil {
				return progressErr
			}
			if err == io.EOF {
				break
			}
			batch.rowOffset = i
			batch.r = make([][]string, 0, batchSize)
		}
		if err != nil {
			return errors.Wrapf(err, "row %d: reading CSV record", i)
		}
		// Ignore the first N lines.
		if uint32(i) <= c.opts.Skip {
			continue
		}
		if len(record) == c.expectedCols {
			// Expected number of columns.
		} else if len(record) == c.expectedCols+1 && record[c.expectedCols] == "" {
			// Line has the optional trailing comma, ignore the empty field.
			record = record[:c.expectedCols]
		} else {
			return errors.Errorf("row %d: expected %d fields, got %d", i, c.expectedCols, len(record))
		}
		batch.r = append(batch.r, record)
	}
	return nil
}

type csvRecord struct {
	r         [][]string
	file      string
	fileIndex int32
	rowOffset int
}

// convertRecord converts CSV records into KV pairs and sends them on the
// kvCh chan.
func (c *csvInputReader) convertRecord(ctx context.Context, kvCh chan<- []roachpb.KeyValue) error {
	done := ctx.Done()

	const kvBatchSize = 1000
	padding := 2 * (len(c.tableDesc.Indexes) + len(c.tableDesc.Families))
	visibleCols := c.tableDesc.VisibleColumns()

	ri, err := sqlbase.MakeRowInserter(nil /* txn */, c.tableDesc, nil, /* fkTables */
		c.tableDesc.Columns, false /* checkFKs */, &sqlbase.DatumAlloc{})
	if err != nil {
		return errors.Wrap(err, "make row inserter")
	}

	var txCtx transform.ExprTransformContext
	evalCtx := tree.EvalContext{SessionData: &sessiondata.SessionData{Location: time.UTC}}
	// Although we don't yet support DEFAULT expressions on visible columns,
	// we do on hidden columns (which is only the default _rowid one). This
	// allows those expressions to run.
	cols, defaultExprs, err := sqlbase.ProcessDefaultColumns(c.tableDesc.Columns, c.tableDesc, &txCtx, &evalCtx)
	if err != nil {
		return errors.Wrap(err, "process default columns")
	}

	datums := make([]tree.Datum, len(visibleCols), len(cols))

	// Check for a hidden column. This should be the unique_rowid PK if present.
	hidden := -1
	for i, col := range cols {
		if col.Hidden {
			if col.DefaultExpr == nil || *col.DefaultExpr != "unique_rowid()" || hidden != -1 {
				return errors.New("unexpected hidden column")
			}
			hidden = i
			datums = append(datums, nil)
		}
	}
	if len(datums) != len(cols) {
		return errors.New("unexpected hidden column")
	}

	kvBatch := make([]roachpb.KeyValue, 0, kvBatchSize+padding)

	computedIVarContainer := sqlbase.RowIndexedVarContainer{
		Mapping: ri.InsertColIDtoRowIndex,
		Cols:    c.tableDesc.Columns,
	}

	for batch := range c.recordCh {
		for batchIdx, record := range batch.r {
			rowNum := batch.rowOffset + batchIdx
			for i, v := range record {
				col := visibleCols[i]
				if c.opts.NullEncoding != nil && v == *c.opts.NullEncoding {
					datums[i] = tree.DNull
				} else {
					var err error
					datums[i], err = tree.ParseDatumStringAs(col.Type.ToDatumType(), v, &evalCtx)
					if err != nil {
						return errors.Wrapf(err, "%s: row %d: parse %q as %s", batch.file, rowNum, col.Name, col.Type.SQLString())
					}
				}
			}
			if hidden >= 0 {
				// We don't want to call unique_rowid() for the hidden PK column because
				// it is not idempotent. The sampling from the first stage will be useless
				// during the read phase, producing a single range split with all of the
				// data. Instead, we will call our own function that mimics that function,
				// but more-or-less guarantees that it will not interfere with the numbers
				// that will be produced by it. The lower 15 bits mimic the node id, but as
				// the CSV file number. The upper 48 bits are the line number and mimic the
				// timestamp. It would take a file with many more than 2**32 lines to even
				// begin approaching what unique_rowid would return today, so we assume it
				// to be safe. Since the timestamp is won't overlap, it is safe to use any
				// number in the node id portion. The 15 bits in that portion should account
				// for up to 32k CSV files in a single IMPORT. In the case of > 32k files,
				// the data is xor'd so the final bits are flipped instead of set.
				datums[hidden] = tree.NewDInt(builtins.GenerateUniqueID(batch.fileIndex, uint64(rowNum)))
			}

			// TODO(justin): we currently disallow computed columns in import statements.
			var computeExprs []tree.TypedExpr
			var computedCols []sqlbase.ColumnDescriptor

			row, err := sql.GenerateInsertRow(
				defaultExprs, computeExprs, cols, computedCols, evalCtx, c.tableDesc, datums, &computedIVarContainer)
			if err != nil {
				return errors.Wrapf(err, "generate insert row: %s: row %d", batch.file, rowNum)
			}
			if err := ri.InsertRow(
				ctx,
				inserter(func(kv roachpb.KeyValue) {
					kv.Value.InitChecksum(kv.Key)
					kvBatch = append(kvBatch, kv)
				}),
				row,
				true, /* ignoreConflicts */
				sqlbase.SkipFKs,
				false, /* traceKV */
			); err != nil {
				return errors.Wrapf(err, "insert row: %s: row %d", batch.file, rowNum)
			}
			if len(kvBatch) >= kvBatchSize {
				select {
				case kvCh <- kvBatch:
				case <-done:
					return ctx.Err()
				}
				kvBatch = make([]roachpb.KeyValue, 0, kvBatchSize+padding)
			}
		}
	}
	select {
	case kvCh <- kvBatch:
	case <-done:
		return ctx.Err()
	}
	return nil
}
