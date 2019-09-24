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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
)

type mysqloutfileReader struct {
	conv row.DatumRowConverter
	opts roachpb.MySQLOutfileOptions
}

var _ inputConverter = &mysqloutfileReader{}

func newMysqloutfileReader(
	kvCh chan row.KVBatch,
	opts roachpb.MySQLOutfileOptions,
	tableDesc *sqlbase.TableDescriptor,
	evalCtx *tree.EvalContext,
) (*mysqloutfileReader, error) {
	conv, err := row.NewDatumRowConverter(tableDesc, nil /* targetColNames */, evalCtx, kvCh)
	if err != nil {
		return nil, err
	}
	return &mysqloutfileReader{
		conv: *conv,
		opts: opts,
	}, nil
}

func (d *mysqloutfileReader) start(ctx ctxgroup.Group) {
}

func (d *mysqloutfileReader) inputFinished(ctx context.Context) {
	close(d.conv.KvCh)
}

func (d *mysqloutfileReader) readFiles(
	ctx context.Context,
	dataFiles map[int32]string,
	format roachpb.IOFileFormat,
	progressFn func(float32) error,
	settings *cluster.Settings,
) error {
	return readInputFiles(ctx, dataFiles, format, d.readFile, progressFn, settings)
}

func (d *mysqloutfileReader) readFile(
	ctx context.Context, input *fileReader, inputIdx int32, inputName string, progressFn progressFn,
) error {
	d.conv.KvBatch.Source = inputIdx
	d.conv.FractionFn = input.ReadFraction
	var count int64 = 1

	var row []tree.Datum
	// The current field being read needs to be a list to be able to undo
	// field enclosures at end of field.
	var fieldParts []rune

	// If we have an escaping char defined, seeing it means the next char is to be
	// treated as escaped -- usually that means literal but has some specific
	// mappings defined as well.
	var nextLiteral bool

	// If we have an enclosing char defined, seeing it begins reading a field --
	// which means we do not look for separators until we see the end of the field
	// as indicated by the matching enclosing char.
	var readingField bool
	// If we have just encountered a potential encloser symbol.
	// That means if a end of field or line is next we should honor it.
	var gotEncloser bool

	var gotNull bool

	reader := bufio.NewReaderSize(input, 1024*64)
	addField := func() error {
		field := string(fieldParts)
		if len(row) >= len(d.conv.VisibleCols) {
			return makeRowErr(inputName, count, pgcode.Syntax,
				"too many columns, expected %d: %#v", len(d.conv.VisibleCols), row)
		}
		if gotNull {
			if len(field) != 0 {
				return makeRowErr(inputName, count, pgcode.Syntax,
					"unexpected data after null encoding: %v", row)
			}
			row = append(row, tree.DNull)
			gotNull = false
		} else if !d.opts.HasEscape && field == "NULL" {
			row = append(row, tree.DNull)
		} else {
			datum, err := tree.ParseStringAs(d.conv.VisibleColTypes[len(row)], field, d.conv.EvalCtx)
			if err != nil {
				col := d.conv.VisibleCols[len(row)]
				return wrapRowErr(err, inputName, count, pgcode.Syntax,
					"parse %q as %s", col.Name, col.Type.SQLString())
			}

			row = append(row, datum)
		}
		fieldParts = fieldParts[:0]
		return nil
	}
	addRow := func() error {
		if len(row) != len(d.conv.VisibleCols) {
			return makeRowErr(inputName, count, pgcode.Syntax,
				"unexpected number of columns, expected %d got %d: %#v", len(d.conv.VisibleCols), len(row), row)
		}
		copy(d.conv.Datums, row)
		if err := d.conv.Row(ctx, inputIdx, count); err != nil {
			return wrapRowErr(err, inputName, count, pgcode.Uncategorized, "")
		}
		count++

		row = row[:0]
		return nil
	}

	for {
		c, w, err := reader.ReadRune()
		finished := err == io.EOF

		// First check that if we're done and everything looks good.
		if finished {
			if nextLiteral {
				return makeRowErr(inputName, count, pgcode.Syntax, "unmatched literal")
			}
			// If previous symbol was field encloser it should be
			// dropped as it only marks end of field. Otherwise
			// throw an error since we don;t expect unmatched encloser.
			if gotEncloser {
				if readingField {
					fieldParts = fieldParts[:len(fieldParts)-1]
				} else {
					return makeRowErr(inputName, count, pgcode.Syntax,
						"unmatched field enclosure at end of field")
				}
			} else if readingField {
				return makeRowErr(inputName, count, pgcode.Syntax,
					"unmatched field enclosure at start of field")
			}
			if len(fieldParts) > 0 {
				if err := addField(); err != nil {
					return err
				}
			}
			// flush the last row if we have one.
			if len(row) > 0 {
				if err := addRow(); err != nil {
					return err
				}
			}
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
			fieldParts = append(fieldParts, rune(raw))
			gotEncloser = false
			continue
		}

		// Do we need to check for escaping?
		if d.opts.HasEscape {
			if nextLiteral {
				nextLiteral = false
				// See https://dev.mysql.com/doc/refman/8.0/en/load-data.html.
				switch c {
				case '0':
					fieldParts = append(fieldParts, rune(0))
				case 'b':
					fieldParts = append(fieldParts, rune('\b'))
				case 'n':
					fieldParts = append(fieldParts, rune('\n'))
				case 'r':
					fieldParts = append(fieldParts, rune('\r'))
				case 't':
					fieldParts = append(fieldParts, rune('\t'))
				case 'Z':
					fieldParts = append(fieldParts, rune(byte(26)))
				case 'N':
					if gotNull {
						return makeRowErr(inputName, count, pgcode.Syntax, "unexpected null encoding")
					}
					gotNull = true
				default:
					fieldParts = append(fieldParts, c)
				}
				gotEncloser = false
				continue
			}

			if c == d.opts.Escape {
				nextLiteral = true
				gotEncloser = false
				continue
			}
		}

		// Are we done with the field, or even the whole row?
		if (!readingField || gotEncloser) &&
			(c == d.opts.FieldSeparator || c == d.opts.RowSeparator) {
			if gotEncloser {
				// If the encloser marked end of field
				// drop it.
				if readingField {
					fieldParts = fieldParts[:len(fieldParts)-1]
				} else {
					// Unexpected since we did not see one at start of field.
					return makeRowErr(inputName, count, pgcode.Syntax,
						"unmatched field enclosure at end of field")
				}
			}
			if err := addField(); err != nil {
				return err
			}
			if c == d.opts.RowSeparator {
				if err := addRow(); err != nil {
					return err
				}
			}
			readingField = false
			gotEncloser = false
			continue
		}

		if gotEncloser {
			gotEncloser = false
		}

		// If enclosing is not disabled, check for the encloser.
		// Technically when it is not optional, we could _require_ it to start and
		// end fields, but for the purposes of decoding, we don't actually care --
		// we'll handle it if we see it either way.
		if d.opts.Enclose != roachpb.MySQLOutfileOptions_Never && c == d.opts.Encloser {
			if !readingField && len(fieldParts) == 0 {
				readingField = true
				continue
			}
			gotEncloser = true
		}

		fieldParts = append(fieldParts, c)
	}
	return d.conv.SendBatch(ctx)
}
