// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/errors"
)

type mysqloutfileReader struct {
	importCtx *parallelImportContext
	opts      roachpb.MySQLOutfileOptions
}

var _ inputConverter = &mysqloutfileReader{}

func newMysqloutfileReader(
	opts roachpb.MySQLOutfileOptions,
	kvCh chan row.KVBatch,
	walltime int64,
	parallelism int,
	tableDesc *sqlbase.TableDescriptor,
	evalCtx *tree.EvalContext,
) (*mysqloutfileReader, error) {
	return &mysqloutfileReader{
		importCtx: &parallelImportContext{
			walltime:   walltime,
			numWorkers: parallelism,
			evalCtx:    evalCtx,
			tableDesc:  tableDesc,
			kvCh:       kvCh,
		},
		opts: opts,
	}, nil
}

func (d *mysqloutfileReader) start(ctx ctxgroup.Group) {
}

func (d *mysqloutfileReader) readFiles(
	ctx context.Context,
	dataFiles map[int32]string,
	resumePos map[int32]int64,
	format roachpb.IOFileFormat,
	makeExternalStorage cloud.ExternalStorageFactory,
) error {
	return readInputFiles(ctx, dataFiles, resumePos, format, d.readFile, makeExternalStorage)
}

type delimitedProducer struct {
	importCtx *parallelImportContext
	opts      *roachpb.MySQLOutfileOptions
	input     *fileReader
	reader    *bufio.Reader
	row       []rune
	err       error
}

func (d *delimitedProducer) Scan() bool {
	// Returns true if the rune we are about to add to d.row is escaped.
	isCurrentRuneEscaped := func() bool {
		return d.opts.HasEscape && len(d.row) > 0 && d.row[len(d.row)-1] == d.opts.Escape
	}

	d.row = nil
	for {
		c, w, err := d.reader.ReadRune()
		if err == io.EOF {
			if isCurrentRuneEscaped() {
				d.err = io.ErrUnexpectedEOF
			}
			return false
		}

		if err != nil {
			d.err = err
			return false
		}

		if c == unicode.ReplacementChar && w == 1 {
			if d.err = d.reader.UnreadRune(); d.err != nil {
				return false
			}
			var raw byte
			raw, d.err = d.reader.ReadByte()
			if err != nil {
				return false
			}
			c = rune(raw)
		}

		if c == d.opts.RowSeparator && !isCurrentRuneEscaped() {
			return true
		}
		d.row = append(d.row, c)
	}
}

func (d *delimitedProducer) Err() error {
	return d.err
}

func (d *delimitedProducer) Skip() error {
	return nil // no-op
}

func (d *delimitedProducer) Row() (interface{}, error) {
	return d.row, d.err
}

func (d *delimitedProducer) Progress() float32 {
	return d.input.ReadFraction()
}

var _ importRowProducer = &delimitedProducer{}

// helper for parsing and setting datums.
type datumSetter struct {
	opts *roachpb.MySQLOutfileOptions
	conv *row.DatumRowConverter
	next int
}

func (ds *datumSetter) setDatum(d tree.Datum) error {
	if ds.next >= len(ds.conv.VisibleCols) {
		return fmt.Errorf("got more than %d expected columns", len(ds.conv.VisibleCols))
	}

	ds.conv.Datums[ds.next] = d
	ds.next++
	return nil
}

func (ds *datumSetter) parseAndSetDatum(data []rune) error {
	if ds.next >= len(ds.conv.VisibleCols) {
		return fmt.Errorf("got more than %d expected columns", len(ds.conv.VisibleCols))
	}

	// If enclosing is not disabled, check for the encloser.
	// Technically when it is not optional, we could _require_ it to start and
	// end fields, but for the purposes of decoding, we don't actually care --
	// we'll handle it if we see it either way.
	if ds.opts.Enclose != roachpb.MySQLOutfileOptions_Never &&
		len(data) > 0 &&
		data[0] == ds.opts.Encloser {
		// Field is enclosed.  Trim it.
		if len(data) < 2 || data[len(data)-1] != ds.opts.Encloser {
			return fmt.Errorf("unmatched field enclosure at end of field %s", string(data))
		}
		data = data[1 : len(data)-1]
	}

	field := string(data)
	if (!ds.opts.HasEscape && field == "NULL") ||
		(ds.opts.NullEncoding != nil && field == *ds.opts.NullEncoding) {
		return ds.setDatum(tree.DNull)
	}

	// This uses ParseDatumStringAsWithRawBytes instead of ParseDatumStringAs
	// since mysql emits raw byte strings that do not use the same escaping as
	// our ParseBytes function expects, and the difference between ParseStringAs
	// and ParseDatumStringAs is whether or not it attempts to parse bytes.
	datum, err := sqlbase.ParseDatumStringAsWithRawBytes(
		ds.conv.VisibleColTypes[ds.next], field, ds.conv.EvalCtx)

	if err != nil {
		col := ds.conv.VisibleCols[ds.next]
		return errors.Wrapf(err, "parse %q as %s", col.Name, col.Type.SQLString())
	}

	ds.conv.Datums[ds.next] = datum
	ds.next++
	return nil
}

type delimitedConsumer struct {
	opts *roachpb.MySQLOutfileOptions
}

func (d *delimitedConsumer) FillDatums(
	input interface{}, rowNum int64, conv *row.DatumRowConverter,
) error {
	ds := &datumSetter{opts: d.opts, conv: conv}
	data := input.([]rune)
	var field []rune

	for i := 0; i < len(data); i++ {
		r := data[i]

		if d.opts.HasEscape && r == d.opts.Escape {
			// See https://dev.mysql.com/doc/refman/8.0/en/load-data.html.
			i++
			if i == len(data) {
				return newImportRowError(io.ErrUnexpectedEOF, string(data), rowNum)
			}

			r = data[i]
			switch r {
			case '0':
				field = append(field, rune(0))
			case 'b':
				field = append(field, rune('\b'))
			case 'n':
				field = append(field, rune('\n'))
			case 'r':
				field = append(field, rune('\r'))
			case 't':
				field = append(field, rune('\t'))
			case 'Z':
				field = append(field, rune(byte(26)))
			case 'N':
				// NULL value.  Peek ahead to make sure that NULL is the
				// only data that's part of this field.
				if len(field) != 0 || (i+1 < len(data) && data[i+1] != d.opts.FieldSeparator) {
					return newImportRowError(
						errors.Errorf("unexpected data in NULL field along with null encoding: %v", field),
						string(data), rowNum)
				}
				if err := ds.setDatum(tree.DNull); err != nil {
					return newImportRowError(err, string(data), rowNum)
				}
			default:
				field = append(field, r)
			}
			continue
		}

		if r == d.opts.FieldSeparator {
			if err := ds.parseAndSetDatum(field); err != nil {
				return newImportRowError(err, string(data), rowNum)
			}
			field = field[:0]
		} else {
			field = append(field, r)
		}
	}

	if len(field) > 0 {
		if err := ds.parseAndSetDatum(field); err != nil {
			return newImportRowError(err, string(data), rowNum)
		}
	}
	return nil
}

var _ importRowConsumer = &delimitedConsumer{}

func (d *mysqloutfileReader) readFile(
	ctx context.Context,
	input *fileReader,
	inputIdx int32,
	inputName string,
	resumePos int64,
	rejected chan string,
) error {
	producer := &delimitedProducer{
		importCtx: d.importCtx,
		opts:      &d.opts,
		input:     input,
		reader:    bufio.NewReaderSize(input, 64*1024),
	}
	consumer := &delimitedConsumer{opts: &d.opts}

	if resumePos < int64(d.opts.Skip) {
		resumePos = int64(d.opts.Skip)
	}

	fileCtx := &importFileContext{
		source:   inputIdx,
		skip:     resumePos,
		rejected: rejected,
	}

	return runParallelImport(ctx, d.importCtx, fileCtx, producer, consumer)
}
