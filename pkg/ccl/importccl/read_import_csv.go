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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

type csvInputReader struct {
	evalCtx      *tree.EvalContext
	kvCh         chan row.KVBatch
	recordCh     chan csvRecord
	batchSize    int
	batch        csvRecord
	opts         roachpb.CSVOptions
	walltime     int64
	tableDesc    *sqlbase.TableDescriptor
	targetCols   tree.NameList
	expectedCols int
	parallelism  int
}

var _ inputConverter = &csvInputReader{}

func newCSVInputReader(
	kvCh chan row.KVBatch,
	opts roachpb.CSVOptions,
	walltime int64,
	parallelism int,
	tableDesc *sqlbase.TableDescriptor,
	targetCols tree.NameList,
	evalCtx *tree.EvalContext,
) *csvInputReader {
	if parallelism <= 0 {
		parallelism = runtime.NumCPU()
	}

	return &csvInputReader{
		evalCtx:      evalCtx,
		opts:         opts,
		walltime:     walltime,
		kvCh:         kvCh,
		expectedCols: len(tableDesc.VisibleColumns()),
		tableDesc:    tableDesc,
		targetCols:   targetCols,
		batchSize:    500,
		parallelism:  parallelism,
	}
}

func (c *csvInputReader) start(group ctxgroup.Group) {
}

func (c *csvInputReader) readFiles(
	ctx context.Context,
	dataFiles map[int32]string,
	resumePos map[int32]int64,
	format roachpb.IOFileFormat,
	makeExternalStorage cloud.ExternalStorageFactory,
) error {
	return readInputFiles(ctx, dataFiles, resumePos, format, c.readFile, makeExternalStorage)
}

func (c *csvInputReader) flushBatch(ctx context.Context, finished bool) error {
	// if the batch isn't empty, we need to flush it.
	if len(c.batch.r) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case c.recordCh <- c.batch:
		}
	}
	if !finished {
		c.batch.r = make([][]string, 0, c.batchSize)
	}
	return nil
}

func (c *csvInputReader) readFile(
	ctx context.Context,
	input *fileReader,
	inputIdx int32,
	inputName string,
	resumePos int64,
	rejected chan string,
) error {
	cr := csv.NewReader(input)
	if c.opts.Comma != 0 {
		cr.Comma = c.opts.Comma
	}
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = !c.opts.StrictQuotes
	cr.Comment = c.opts.Comment

	c.recordCh = make(chan csvRecord)
	c.batch = csvRecord{
		file:      inputName,
		fileIndex: inputIdx,
		rowOffset: 1,
		r:         make([][]string, 0, c.batchSize),
		rejected:  rejected,
	}

	group := ctxgroup.WithContext(ctx)
	group.GoCtx(func(ctx context.Context) error {
		ctx, span := tracing.ChildSpan(ctx, "convertcsv")
		defer tracing.FinishSpan(span)
		return ctxgroup.GroupWorkers(ctx, c.parallelism, func(ctx context.Context) error {
			return c.convertRecordWorker(ctx)
		})
	})

	group.GoCtx(func(ctx context.Context) error {
		defer close(c.recordCh)
		for i := int64(1); ; i++ {
			record, err := cr.Read()
			finished := err == io.EOF
			if finished || len(c.batch.r) >= c.batchSize {
				c.batch.progress = input.ReadFraction()
				if err := c.flushBatch(ctx, finished); err != nil {
					return err
				}
				c.batch.rowOffset = i
			}
			if finished {
				break
			}
			if err != nil {
				// TODO(spaskob): Find a way to report this row to rejected. The difficulty
				// is that we can't really know how to get the line since it is being parsed
				// internally by the csv reader `cr`.
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
				err := makeRowErr(inputName, i, pgcode.Syntax, "expected %d fields, got %d: %#v", c.expectedCols, len(record), record)
				if rejected != nil {
					log.Error(ctx, err)
					rejected <- strings.Join(record, string(cr.Comma)) + "\n"
					continue
				} else {
					return err
				}
			}
			c.batch.r = append(c.batch.r, record)
		}
		return nil
	})
	return group.Wait()
}

type csvRecord struct {
	r         [][]string
	file      string
	fileIndex int32
	rowOffset int64
	progress  float32
	// Channel on which to report bad rows.
	rejected chan string
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

	epoch := time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	const precision = uint64(10 * time.Microsecond)
	timestamp := uint64(c.walltime-epoch) / precision

	for batch := range c.recordCh {
		if conv.KvBatch.Source != batch.fileIndex {
			if err := conv.SendBatch(ctx); err != nil {
				return err
			}
			conv.KvBatch.Source = batch.fileIndex
		}
		conv.KvBatch.Progress = batch.progress
	ROW_LOOP:
		for batchIdx, record := range batch.r {
			rowNum := batch.rowOffset + int64(batchIdx)
			datumIdx := 0
			for i, field := range record {
				// Skip over record entries corresponding to columns not in the target
				// columns specified by the user.
				if _, ok := conv.IsTargetCol[i]; !ok {
					continue
				}
				col := conv.VisibleCols[i]
				if c.opts.NullEncoding != nil && field == *c.opts.NullEncoding {
					conv.Datums[datumIdx] = tree.DNull
				} else {
					var err error
					conv.Datums[datumIdx], err = tree.ParseDatumStringAs(conv.VisibleColTypes[i], field, conv.EvalCtx)
					if err != nil {
						err = wrapRowErr(err, batch.file, rowNum, pgcode.Syntax,
							"parse %q as %s", col.Name, col.Type.SQLString())
						if batch.rejected != nil {
							log.Error(ctx, err)
							batch.rejected <- strings.Join(record, string(c.opts.Comma)) + "\n"
							continue ROW_LOOP
						}
						return err
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
