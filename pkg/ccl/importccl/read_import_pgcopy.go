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
	"strconv"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
)

type pgCopyReader struct {
	conv rowConverter
	opts roachpb.PgCopyOptions
}

var _ inputConverter = &pgCopyReader{}

func newPgCopyReader(
	kvCh chan kvBatch,
	opts roachpb.PgCopyOptions,
	tableDesc *sqlbase.TableDescriptor,
	evalCtx *tree.EvalContext,
) (*pgCopyReader, error) {
	conv, err := newRowConverter(tableDesc, evalCtx, kvCh)
	if err != nil {
		return nil, err
	}
	return &pgCopyReader{
		conv: *conv,
		opts: opts,
	}, nil
}

func (d *pgCopyReader) start(ctx ctxgroup.Group) {
}

func (d *pgCopyReader) inputFinished(ctx context.Context) {
	close(d.conv.kvCh)
}

func (d *pgCopyReader) readFile(
	ctx context.Context, input io.Reader, inputIdx int32, inputName string, progressFn progressFn,
) error {
	var count int64 = 1

	var row []tree.Datum
	// the current field being read.
	var field []byte

	var sawBackslash bool

	reader := bufio.NewReaderSize(input, 1024*64)
	addField := func() error {
		if len(row) >= len(d.conv.visibleCols) {
			return makeRowErr(inputName, count, "too many columns, expected %d: %#v", len(d.conv.visibleCols), row)
		}
		// COPY does its backslash removal after the entire field has been extracted
		// so it can compare to the NULL setting earlier.
		if string(field) == d.opts.Null {
			row = append(row, tree.DNull)
		} else {
			// Use the same backing store since we are guaranteed to only remove
			// characters.
			nf := field[:0]
			for i := 0; i < len(field); i++ {
				if field[i] == '\\' {
					i++
					if len(field) <= i {
						return makeRowErr(inputName, count, "unmatched escape")
					}
					// See https://www.postgresql.org/docs/current/static/sql-copy.html
					c := field[i]
					switch c {
					case 'b':
						nf = append(nf, '\b')
					case 'f':
						nf = append(nf, '\f')
					case 'n':
						nf = append(nf, '\n')
					case 'r':
						nf = append(nf, '\r')
					case 't':
						nf = append(nf, '\t')
					case 'v':
						nf = append(nf, '\v')
					default:
						if c == 'x' || (c >= '0' && c <= '9') {
							// Handle \xNN and \NNN hex and octal escapes.)
							if len(field) <= i+2 {
								return makeRowErr(inputName, count, "unsupported escape sequence: \\%s", field[i:])
							}
							base := 8
							idx := 0
							if c == 'x' {
								base = 16
								idx = 1
							}
							v, err := strconv.ParseInt(string(field[i+idx:i+3]), base, 8)
							if err != nil {
								return makeRowErr(inputName, count, err.Error())
							}
							i += 2
							nf = append(nf, byte(v))
						} else {
							nf = append(nf, string(c)...)
						}
					}
				} else {
					nf = append(nf, field[i])
				}
			}

			datum, err := tree.ParseStringAs(d.conv.visibleColTypes[len(row)], string(nf), d.conv.evalCtx)
			if err != nil {
				col := d.conv.visibleCols[len(row)]
				return makeRowErr(inputName, count, "parse %q as %s: %s:", col.Name, col.Type.SQLString(), err)
			}

			row = append(row, datum)
		}
		field = field[:0]
		return nil
	}
	addRow := func() error {
		copy(d.conv.datums, row)
		if err := d.conv.row(ctx, inputIdx, count); err != nil {
			return makeRowErr(inputName, count, "%s", err)
		}
		count++

		row = row[:0]
		return nil
	}

	// Start by finding field delimiters.
	for {
		c, w, err := reader.ReadRune()
		finished := err == io.EOF

		// First check that if we're done and everything looks good.
		if finished {
			if sawBackslash {
				return makeRowErr(inputName, count, "unmatched escape")
			}
			if len(field) > 0 {
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
		// We only care about backslashes here if they are followed by a field
		// delimiter. Otherwise we pass them through. They will be escaped later on.
		if sawBackslash {
			sawBackslash = false
			if c == d.opts.Delimiter {
				field = append(field, string(d.opts.Delimiter)...)
			} else {
				field = append(field, '\\')
				field = append(field, string(c)...)
			}
			continue
		} else if c == '\\' {
			sawBackslash = true
			continue
		}

		const rowSeparator = '\n'
		// Are we done with the field, or even the whole row?
		if c == d.opts.Delimiter || c == rowSeparator {
			if err := addField(); err != nil {
				return err
			}
			if c == rowSeparator {
				if err := addRow(); err != nil {
					return err
				}
			}
			continue
		}

		field = append(field, string(c)...)
	}

	return d.conv.sendBatch(ctx)
}
