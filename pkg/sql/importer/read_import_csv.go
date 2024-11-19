// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"context"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	semaCtx *tree.SemaContext,
	kvCh chan row.KVBatch,
	opts roachpb.CSVOptions,
	walltime int64,
	parallelism int,
	tableDesc catalog.TableDescriptor,
	targetCols tree.NameList,
	evalCtx *eval.Context,
	seqChunkProvider *row.SeqChunkProvider,
	db *kv.DB,
) *csvInputReader {
	numExpectedDataCols := len(targetCols)
	if numExpectedDataCols == 0 {
		numExpectedDataCols = len(tableDesc.VisibleColumns())
	}

	return &csvInputReader{
		importCtx: &parallelImportContext{
			semaCtx:          semaCtx,
			walltime:         walltime,
			numWorkers:       parallelism,
			evalCtx:          evalCtx,
			tableDesc:        tableDesc,
			targetCols:       targetCols,
			kvCh:             kvCh,
			seqChunkProvider: seqChunkProvider,
			db:               db,
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
	user username.SQLUsername,
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
	record             []csv.Record
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

func strRecord(record []csv.Record, sep rune) string {
	csvSep := ","
	if sep != 0 {
		csvSep = string(sep)
	}
	strs := make([]string, len(record))
	for i := range record {
		if record[i].Quoted {
			strs[i] = "\"" + record[i].Val + "\""
		} else {
			strs[i] = record[i].Val
		}
	}
	return strings.Join(strs, csvSep)
}

// Row() implements importRowProducer interface.
func (p *csvRowProducer) Row() (interface{}, error) {
	p.rowNum++
	expectedColsLen := p.numExpectedColumns

	if len(p.record) == expectedColsLen {
		// Expected number of columns.
	} else if len(p.record) == expectedColsLen+1 &&
		p.record[expectedColsLen].Val == "" &&
		!p.record[expectedColsLen].Quoted {
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
	ctx context.Context, row interface{}, rowNum int64, conv *row.DatumRowConverter,
) error {
	record := row.([]csv.Record)
	datumIdx := 0

	for i, field := range record {
		// Skip over record entries corresponding to columns not in the target
		// columns specified by the user.
		if !conv.TargetColOrds.Contains(i) {
			continue
		}

		// NullEncoding is stored as a *string historically, from before we wanted
		// it to default to "". Rather than changing the proto, we just set the
		// default here.
		nullEncoding := ""
		if c.opts.NullEncoding != nil {
			nullEncoding = *c.opts.NullEncoding
		}
		if (!field.Quoted || c.opts.AllowQuotedNull) && field.Val == nullEncoding {
			// To match COPY, the default behavior is to only treat the field as NULL
			// if it was not quoted (and if it matches the configured NullEncoding).
			// The AllowQuotedNull option can be used to get the old behavior where
			// even a quoted value is treated as NULL.
			conv.Datums[datumIdx] = tree.DNull
		} else {
			var err error
			conv.Datums[datumIdx], err = rowenc.ParseDatumStringAs(ctx, conv.VisibleColTypes[i], field.Val, conv.EvalCtx, conv.SemaCtx)
			if err != nil {
				// Fallback to parsing as a string literal. This allows us to support
				// both array expressions (like `ARRAY[1, 2, 3]`) and literals (like
				// `{1, 2, 3}`).
				var err2 error
				conv.Datums[datumIdx], _, err2 = tree.ParseAndRequireString(conv.VisibleColTypes[i], field.Val, conv.EvalCtx)
				if err2 != nil {
					col := conv.VisibleCols[i]
					parseRowErr := newImportRowError(
						errors.Wrapf(errors.CombineErrors(err, err2), "parse %q as %s", col.GetName(), col.GetType().SQLString()),
						strRecord(record, c.opts.Comma),
						rowNum)
					if field.Quoted && !c.opts.AllowQuotedNull && field.Val == nullEncoding {
						return errors.WithHint(parseRowErr, "null value is quoted but allow_quoted_null option is not set")
					}
					if strings.TrimSpace(field.Val) == nullEncoding {
						return errors.WithHint(parseRowErr, "null value must not have extra whitespace")
					}
					return parseRowErr
				}
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
