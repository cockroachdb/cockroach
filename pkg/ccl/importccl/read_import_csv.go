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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type csvInputReader struct {
	evalCtx      *tree.EvalContext
	kvCh         chan row.KVBatch
	batchSize    int
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
		batchSize:    parallelImporterReaderBatchSize,
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

func (c *csvInputReader) readFile(
	ctx context.Context,
	input *fileReader,
	inputIdx int32,
	inputName string,
	resumePos int64,
	rejected chan string,
) error {
	scanner := newCSVInputScanner(c, input, inputName, inputIdx, rejected)

	if resumePos < int64(c.opts.Skip) {
		resumePos = int64(c.opts.Skip)
	}

	return runParallelImport(
		ctx,
		parallelImportOptions{
			skip:       resumePos,
			walltime:   c.walltime,
			numWorkers: c.parallelism,
			batchSize:  c.batchSize,
		},
		scanner,
		func(ctx context.Context) (*row.DatumRowConverter, error) {
			return row.NewDatumRowConverter(ctx, c.tableDesc, c.targetCols, c.evalCtx.Copy(), c.kvCh)
		},
	)
}

type csvScanner struct {
	expectedCols int
	input        *namedInput
	opts         roachpb.CSVOptions
	csv          *csv.Reader
	err          error
	record       []string
	rejected     chan string
}

func (s *csvScanner) Input() *namedInput {
	return s.input
}

func (s *csvScanner) Scan() bool {
	s.record, s.err = s.csv.Read()

	if s.err == io.EOF {
		s.err = nil
		return false
	}

	return s.err == nil
}

func (s *csvScanner) Err() error {
	return s.err
}

func (s *csvScanner) Skip() error {
	// No-op
	return nil
}

func (s *csvScanner) Row() (interface{}, error) {
	if len(s.record) == s.expectedCols {
		// Expected number of columns.
	} else if len(s.record) == s.expectedCols+1 && s.record[s.expectedCols] == "" {
		// Line has the optional trailing comma, ignore the empty field.
		s.record = s.record[:s.expectedCols]
	} else {
		return nil, errors.Errorf("expected %d fields, got %d: %#v", s.expectedCols, len(s.record), s.record)
	}
	return s.record, nil
}

func (s *csvScanner) HandleCorruptRow(
	ctx context.Context, row interface{}, rowNum int64, err error,
) bool {
	log.Error(ctx, err)
	if s.rejected == nil {
		return false
	}
	s.rejected <- strings.Join(row.([]string), string(s.opts.Comma)) + "\n"
	return true
}

func (s *csvScanner) StreamProgress() float32 {
	return s.input.reader.ReadFraction()
}

// convertRecordWorker converts CSV records into KV pairs and sends them on the
// kvCh chan.
func (s *csvScanner) FillDatums(row interface{}, rowNum int64, conv *row.DatumRowConverter) error {
	record := row.([]string)
	datumIdx := 0
	for i, field := range record {
		// Skip over record entries corresponding to columns not in the target
		// columns specified by the user.
		if _, ok := conv.IsTargetCol[i]; !ok {
			continue
		}

		if s.opts.NullEncoding != nil && field == *s.opts.NullEncoding {
			conv.Datums[datumIdx] = tree.DNull
		} else {
			var err error
			conv.Datums[datumIdx], err = sqlbase.ParseDatumStringAs(conv.VisibleColTypes[i], field, conv.EvalCtx)
			if err != nil {
				col := conv.VisibleCols[i]
				err = wrapRowErr(err, s.input.name, rowNum, pgcode.Syntax,
					"parse %q as %s", col.Name, col.Type.SQLString())
				return err
			}
		}
		datumIdx++
	}
	return nil
}

var _ importRowStream = &csvScanner{}

func newCSVInputScanner(
	c *csvInputReader, input *fileReader, inputName string, inputIdx int32, rejected chan string,
) *csvScanner {
	cr := csv.NewReader(input)
	if c.opts.Comma != 0 {
		cr.Comma = c.opts.Comma
	}
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = !c.opts.StrictQuotes
	cr.Comment = c.opts.Comment

	return &csvScanner{
		input:        &namedInput{reader: input, name: inputName, idx: inputIdx},
		opts:         c.opts,
		csv:          cr,
		expectedCols: c.expectedCols,
		rejected:     rejected,
	}
}
