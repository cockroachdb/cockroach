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
	"io"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type mysqloutfileReader struct {
	csvInputReader
	opts roachpb.MySQLOutfileOptions
}

var _ inputConverter = &mysqloutfileReader{}

func newMysqloutfileReader(
	kvCh chan kvBatch,
	opts roachpb.MySQLOutfileOptions,
	tableDesc *sqlbase.TableDescriptor,
	evalCtx *tree.EvalContext,
) *mysqloutfileReader {
	null := "NULL"
	if opts.HasEscape {
		null = `\N`
	}
	csvOpts := roachpb.CSVOptions{NullEncoding: &null}
	return &mysqloutfileReader{
		csvInputReader: *newCSVInputReader(kvCh, csvOpts, tableDesc, evalCtx),
		opts:           opts,
	}
}

func (d *mysqloutfileReader) readFile(
	ctx context.Context, input io.Reader, inputIdx int32, inputName string, progressFn progressFn,
) error {

	var count int

	// The current row being read.
	var row []string
	// the current field being read.
	var field []byte

	// If we have an escaping char defined, seeing it means the next char is to be
	// treated as escaped -- usually that means literal but has some specific
	// mappings defined as well.
	var nextLiteral bool

	// If we have an enclosing char defined, seeing it begins reading a field --
	// which means we do not look for separators until we see the end of the field
	// as indicated by the matching enclosing char.
	var readingField bool

	reader := bufio.NewReaderSize(input, 1024*64)

	for {
		c, w, err := reader.ReadRune()
		finished := err == io.EOF

		// First check that if we're done and everything looks good.
		if finished {
			if nextLiteral {
				return makeRowErr(inputName, int64(count)+1, "unmatched literal")
			}
			if readingField {
				return makeRowErr(inputName, int64(count)+1, "unmatched field enclosure")
			}
			// flush the last row if we have one.
			if len(row) > 0 {
				d.csvInputReader.batch.r = append(d.csvInputReader.batch.r, row)
			}
		}

		// Check if we need to flush due to capacity or finished.
		if finished || len(d.csvInputReader.batch.r) > d.csvInputReader.batchSize {
			if err := d.csvInputReader.flushBatch(ctx, finished, progressFn); err != nil {
				return err
			}
			d.csvInputReader.batch.rowOffset = count
		}

		if finished {
			break
		}

		if err != nil {
			return err
		}
		if c == unicode.ReplacementChar && w == 1 {
			if err := reader.UnreadRune(); err != nil {
				return err
			}
			raw, err := reader.ReadByte()
			if err != nil {
				return err
			}
			field = append(field, raw)
			continue
		}

		// Do we need to check for escaping?
		if d.opts.HasEscape {
			if nextLiteral {
				nextLiteral = false
				// See https://dev.mysql.com/doc/refman/8.0/en/load-data.html.
				switch c {
				case '0':
					field = append(field, byte(0))
				case 'b':
					field = append(field, '\b')
				case 'n':
					field = append(field, '\n')
				case 'r':
					field = append(field, '\r')
				case 't':
					field = append(field, '\t')
				case 'Z':
					field = append(field, byte(26))
				case 'N':
					field = append(field, '\\', 'N')
				default:
					field = append(field, string(c)...)
				}
				continue
			}

			if c == d.opts.Escape {
				nextLiteral = true
				continue
			}
		}

		// If enclosing is not disabled, check for the encloser.
		// Technically when it is not optional, we could _require_ it to start and
		// end fields, but for the purposes of decoding, we don't actually care --
		// we'll handle it if we see it either way.
		if d.opts.Enclose != roachpb.MySQLOutfileOptions_Never && c == d.opts.Encloser {
			readingField = !readingField
			continue
		}

		// Are we done with the field, or even the whole row?
		if !readingField && (c == d.opts.FieldSeparator || c == d.opts.RowSeparator) {
			row = append(row, string(field))
			field = field[:0]

			if c == d.opts.RowSeparator {
				if expected := d.csvInputReader.expectedCols; expected != len(row) {
					return makeRowErr(inputName, int64(count)+1, "expected %d columns, got %d: %#v", expected, len(row), row)
				}
				d.csvInputReader.batch.r = append(d.csvInputReader.batch.r, row)
				count++
				row = make([]string, 0, len(row))
			}
			continue
		}

		field = append(field, string(c)...)
	}

	return nil
}
