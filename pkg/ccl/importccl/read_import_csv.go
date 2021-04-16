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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/errors"
)

type csvInputReader struct {
	importCtx *parallelImportContext
	// The number of columns that we expect in the CSV data file.
	numExpectedDataCols int
	opts                roachpb.CSVOptions
}

var _ inputConverter = &csvInputReader{}

func newCSVInputReader(
	kvCh chan row.KVBatch,
	opts roachpb.CSVOptions,
	walltime int64,
	parallelism int,
	tableDesc catalog.TableDescriptor,
	targetCols tree.NameList,
	evalCtx *tree.EvalContext,
	seqChunkProvider *row.SeqChunkProvider,
) *csvInputReader {
	numExpectedDataCols := len(targetCols)
	if numExpectedDataCols == 0 {
		numExpectedDataCols = len(tableDesc.VisibleColumns())
	}

	return &csvInputReader{
		importCtx: &parallelImportContext{
			walltime:         walltime,
			numWorkers:       parallelism,
			evalCtx:          evalCtx,
			tableDesc:        tableDesc,
			targetCols:       targetCols,
			kvCh:             kvCh,
			seqChunkProvider: seqChunkProvider,
		},
		numExpectedDataCols: numExpectedDataCols,
		opts:                opts,
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
	user security.SQLUsername,
) error {
	return readInputFiles(ctx, dataFiles, resumePos, format, c.readFile, makeExternalStorage, user)
}

func (c *csvInputReader) readFile(
	ctx context.Context, input *fileReader, inputIdx int32, resumePos int64, rejected chan string,
) error {
	producer, consumer := newCSVPipeline(c, input)

	if resumePos < int64(c.opts.Skip) {
		resumePos = int64(c.opts.Skip)
	}

	fileCtx := &importFileContext{
		source:   inputIdx,
		skip:     resumePos,
		rejected: rejected,
		rowLimit: c.opts.RowLimit,
	}

	return runParallelImport(ctx, c.importCtx, fileCtx, producer, consumer)
}

type csvRowProducer struct {
	importCtx          *parallelImportContext
	opts               *roachpb.CSVOptions
	csv                *csv.Reader
	rowNum             int64
	err                error
	record             []string
	progress           func() float32
	numExpectedColumns int
}

var _ importRowProducer = &csvRowProducer{}

// Scan() implements importRowProducer interface.
func (p *csvRowProducer) Scan() bool {
	p.record, p.err = p.csv.Read()

	if p.err == io.EOF {
		p.err = nil
		return false
	}

	return p.err == nil
}

// Err() implements importRowProducer interface.
func (p *csvRowProducer) Err() error {
	return p.err
}

// Skip() implements importRowProducer interface.
func (p *csvRowProducer) Skip() error {
	// No-op
	return nil
}

func strRecord(record []string, sep rune) string {
	csvSep := ","
	if sep != 0 {
		csvSep = string(sep)
	}
	return strings.Join(record, csvSep)
}

// Row() implements importRowProducer interface.
func (p *csvRowProducer) Row() (interface{}, error) {
	p.rowNum++
	expectedColsLen := p.numExpectedColumns

	if len(p.record) == expectedColsLen {
		// Expected number of columns.
	} else if len(p.record) == expectedColsLen+1 && p.record[expectedColsLen] == "" {
		// Line has the optional trailing comma, ignore the empty field.
		p.record = p.record[:expectedColsLen]
	} else {
		return nil, newImportRowError(
			errors.Errorf("expected %d fields, got %d", expectedColsLen, len(p.record)),
			strRecord(p.record, p.opts.Comma),
			p.rowNum)

	}
	return p.record, nil
}

// Progress() implements importRowProducer interface.
func (p *csvRowProducer) Progress() float32 {
	return p.progress()
}

type csvRowConsumer struct {
	importCtx *parallelImportContext
	opts      *roachpb.CSVOptions
}

var _ importRowConsumer = &csvRowConsumer{}

// FillDatums() implements importRowConsumer interface
func (c *csvRowConsumer) FillDatums(
	row interface{}, rowNum int64, conv *row.DatumRowConverter,
) error {
	record := row.([]string)
	datumIdx := 0

	for i, field := range record {
		// Skip over record entries corresponding to columns not in the target
		// columns specified by the user.
		if !conv.TargetColOrds.Contains(i) {
			continue
		}

		if c.opts.NullEncoding != nil &&
			field == *c.opts.NullEncoding {
			conv.Datums[datumIdx] = tree.DNull
		} else {
			var err error
			conv.Datums[datumIdx], err = rowenc.ParseDatumStringAs(conv.VisibleColTypes[i], field, conv.EvalCtx)
			if err != nil {
				col := conv.VisibleCols[i]
				return newImportRowError(
					errors.Wrapf(err, "parse %q as %s", col.GetName(), col.GetType().SQLString()),
					strRecord(record, c.opts.Comma),
					rowNum)
			}
		}
		datumIdx++
	}
	return nil
}

func newCSVPipeline(c *csvInputReader, input *fileReader) (*csvRowProducer, *csvRowConsumer) {
	cr := csv.NewReader(input)
	if c.opts.Comma != 0 {
		cr.Comma = c.opts.Comma
	}
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = !c.opts.StrictQuotes
	cr.Comment = c.opts.Comment

	producer := &csvRowProducer{
		importCtx:          c.importCtx,
		opts:               &c.opts,
		csv:                cr,
		progress:           func() float32 { return input.ReadFraction() },
		numExpectedColumns: c.numExpectedDataCols,
	}
	consumer := &csvRowConsumer{
		importCtx: c.importCtx,
		opts:      &c.opts,
	}

	return producer, consumer
}
