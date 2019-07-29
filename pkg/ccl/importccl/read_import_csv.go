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
	"io"
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

type csvInputReader struct {
	evalCtx      *tree.EvalContext
	kvCh         chan []roachpb.KeyValue
	recordCh     chan csvRecord
	batchSize    int
	batch        csvRecord
	opts         roachpb.CSVOptions
	walltime     int64
	tableDesc    *sqlbase.TableDescriptor
	targetCols   tree.NameList
	expectedCols int
}

var _ inputConverter = &csvInputReader{}

func newCSVInputReader(
	kvCh chan []roachpb.KeyValue,
	opts roachpb.CSVOptions,
	walltime int64,
	tableDesc *sqlbase.TableDescriptor,
	targetCols tree.NameList,
	evalCtx *tree.EvalContext,
) *csvInputReader {
	return &csvInputReader{
		evalCtx:      evalCtx,
		opts:         opts,
		walltime:     walltime,
		kvCh:         kvCh,
		expectedCols: len(tableDesc.VisibleColumns()),
		tableDesc:    tableDesc,
		targetCols:   targetCols,
		recordCh:     make(chan csvRecord),
		batchSize:    500,
	}
}

func (c *csvInputReader) start(group ctxgroup.Group) {
	group.GoCtx(func(ctx context.Context) error {
		ctx, span := tracing.ChildSpan(ctx, "convertcsv")
		defer tracing.FinishSpan(span)

		defer close(c.kvCh)
		return ctxgroup.GroupWorkers(ctx, runtime.NumCPU(), func(ctx context.Context) error {
			return c.convertRecordWorker(ctx)
		})
	})
}

func (c *csvInputReader) inputFinished(_ context.Context) {
	close(c.recordCh)
}

func (c *csvInputReader) readFiles(
	ctx context.Context,
	dataFiles map[int32]string,
	format roachpb.IOFileFormat,
	progressFn func(float32) error,
	settings *cluster.Settings,
) error {
	return readInputFiles(ctx, dataFiles, format, c.readFile, progressFn, settings)
}

func (c *csvInputReader) flushBatch(ctx context.Context, finished bool, progFn progressFn) error {
	// if the batch isn't empty, we need to flush it.
	if len(c.batch.r) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case c.recordCh <- c.batch:
		}
	}
	if progressErr := progFn(finished); progressErr != nil {
		return progressErr
	}
	if !finished {
		c.batch.r = make([][]string, 0, c.batchSize)
	}
	return nil
}

func (c *csvInputReader) readFile(
	ctx context.Context, input io.Reader, inputIdx int32, inputName string, progressFn progressFn,
) error {
	cr := csv.NewReader(input)
	if c.opts.Comma != 0 {
		cr.Comma = c.opts.Comma
	}
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	cr.Comment = c.opts.Comment

	c.batch = csvRecord{
		file:      inputName,
		fileIndex: inputIdx,
		rowOffset: 1,
		r:         make([][]string, 0, c.batchSize),
	}

	for i := 1; ; i++ {
		record, err := cr.Read()
		finished := err == io.EOF
		if finished || len(c.batch.r) >= c.batchSize {
			if err := c.flushBatch(ctx, finished, progressFn); err != nil {
				return err
			}
			c.batch.rowOffset = i
		}
		if finished {
			break
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
		c.batch.r = append(c.batch.r, record)
	}
	return nil
}

type csvRecord struct {
	r         [][]string
	file      string
	fileIndex int32
	rowOffset int
}

// convertRecordWorker converts CSV records into KV pairs and sends them on the
// kvCh chan.
func (c *csvInputReader) convertRecordWorker(ctx context.Context) error {
	// Create a new evalCtx per converter so each go routine gets its own
	// collationenv, which can't be accessed in parallel.
	evalCtx := c.evalCtx.Copy()
	conv, err := row.NewDatumRowConverter(c.tableDesc, c.targetCols, evalCtx, c.kvCh)
	if err != nil {
		return err
	}
	if conv.EvalCtx.SessionData == nil {
		panic("uninitialized session data")
	}

	const precision = uint64(10 * time.Microsecond)
	timestamp := uint64(c.walltime) / precision

	for batch := range c.recordCh {
		for batchIdx, record := range batch.r {
			rowNum := int64(batch.rowOffset + batchIdx)
			datumIdx := 0
			for i, v := range record {
				// Skip over record entries corresponding to columns not in the target
				// columns specified by the user.
				if _, ok := conv.IsTargetCol[i]; !ok {
					continue
				}
				col := conv.VisibleCols[i]
				if c.opts.NullEncoding != nil && v == *c.opts.NullEncoding {
					conv.Datums[datumIdx] = tree.DNull
				} else {
					var err error
					conv.Datums[datumIdx], err = tree.ParseDatumStringAs(conv.VisibleColTypes[i], v, conv.EvalCtx)
					if err != nil {
						return wrapRowErr(err, batch.file, rowNum, pgcode.Syntax,
							"parse %q as %s", col.Name, col.Type.SQLString())
					}
				}
				datumIdx++
			}

			rowIndex := int64(timestamp) + rowNum
			if err := conv.Row(ctx, batch.fileIndex, rowIndex); err != nil {
				return wrapRowErr(err, batch.file, rowNum, pgcode.Uncategorized, "")
			}
		}
	}
	return conv.SendBatch(ctx)
}
