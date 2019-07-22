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
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/errors"
)

// defaultScanBuffer is the default max row size of the PGCOPY and PGDUMP
// scanner.
const defaultScanBuffer = 1024 * 1024 * 4

type pgCopyReader struct {
	conv row.DatumRowConverter
	opts roachpb.PgCopyOptions
}

var _ inputConverter = &pgCopyReader{}

func newPgCopyReader(
	kvCh chan []roachpb.KeyValue,
	opts roachpb.PgCopyOptions,
	tableDesc *sqlbase.TableDescriptor,
	evalCtx *tree.EvalContext,
) (*pgCopyReader, error) {
	conv, err := row.NewDatumRowConverter(tableDesc, nil /* targetColNames */, evalCtx, kvCh)
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
	close(d.conv.KvCh)
}

func (d *pgCopyReader) readFiles(
	ctx context.Context,
	dataFiles map[int32]string,
	format roachpb.IOFileFormat,
	progressFn func(float32) error,
	settings *cluster.Settings,
) error {
	return readInputFiles(ctx, dataFiles, format, d.readFile, progressFn, settings)
}

type postgreStreamCopy struct {
	s         *bufio.Scanner
	delimiter rune
	null      string
}

// newPostgreStreamCopy streams COPY data rows from s with specified delimiter
// and null string. If readEmptyLine is true, the first line is expected to
// be empty as the newline following the previous COPY statement.
//
// s must be an existing bufio.Scanner configured to split on
// bufio.ScanLines. It must be done outside of this function because we cannot
// change the split func mid scan. And in order to use the same scanner as
// the SQL parser, which can change the splitter using a closure.
func newPostgreStreamCopy(s *bufio.Scanner, delimiter rune, null string) *postgreStreamCopy {
	return &postgreStreamCopy{
		s:         s,
		delimiter: delimiter,
		null:      null,
	}
}

var errCopyDone = errors.New("COPY done")

// Next returns the next row. io.EOF is the returned error upon EOF. The
// errCopyDone error is returned if "\." is encountered, indicating the end
// of the COPY section.
func (p *postgreStreamCopy) Next() (copyData, error) {
	var row copyData
	// the current field being read.
	var field []byte

	addField := func() error {
		// COPY does its backslash removal after the entire field has been extracted
		// so it can compare to the NULL setting earlier.
		if string(field) == p.null {
			row = append(row, nil)
		} else {
			// Use the same backing store since we are guaranteed to only remove
			// characters.
			nf := field[:0]
			for i := 0; i < len(field); i++ {
				if field[i] == '\\' {
					i++
					if len(field) <= i {
						return errors.New("unmatched escape")
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
								return errors.Errorf("unsupported escape sequence: \\%s", field[i:])
							}
							base := 8
							idx := 0
							if c == 'x' {
								base = 16
								idx = 1
							}
							v, err := strconv.ParseInt(string(field[i+idx:i+3]), base, 8)
							if err != nil {
								return err
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
			ns := string(nf)
			row = append(row, &ns)
		}
		field = field[:0]
		return nil
	}

	// Attempt to read an entire line.
	scanned := p.s.Scan()
	if err := p.s.Err(); err != nil {
		if err == bufio.ErrTooLong {
			err = errors.New("line too long")
		}
		return nil, err
	}
	if !scanned {
		return nil, io.EOF
	}
	// Check for the copy done marker.
	if bytes.Equal(p.s.Bytes(), []byte(`\.`)) {
		return nil, errCopyDone
	}
	reader := bytes.NewReader(p.s.Bytes())

	var sawBackslash bool
	// Start by finding field delimiters.
	for {
		c, w, err := reader.ReadRune()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if c == unicode.ReplacementChar && w == 1 {
			return nil, errors.New("error decoding UTF-8 Rune")
		}

		// We only care about backslashes here if they are followed by a field
		// delimiter. Otherwise we pass them through. They will be escaped later on.
		if sawBackslash {
			sawBackslash = false
			if c == p.delimiter {
				field = append(field, string(p.delimiter)...)
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
		// Are we done with the field?
		if c == p.delimiter || c == rowSeparator {
			if err := addField(); err != nil {
				return nil, err
			}
		} else {
			field = append(field, string(c)...)
		}
	}
	if sawBackslash {
		return nil, errors.Errorf("unmatched escape")
	}
	// We always want to call this because there's at least 1 field per row. If
	// the row is empty we should return a row with a single, empty field.
	if err := addField(); err != nil {
		return nil, err
	}
	return row, nil
}

const (
	copyDefaultDelimiter = '\t'
	copyDefaultNull      = `\N`
)

type copyData []*string

func (c copyData) String() string {
	var buf bytes.Buffer
	for i, s := range c {
		if i > 0 {
			buf.WriteByte(copyDefaultDelimiter)
		}
		if s == nil {
			buf.WriteString(copyDefaultNull)
		} else {
			// TODO(mjibson): this isn't correct COPY syntax, but it's only used in tests.
			fmt.Fprintf(&buf, "%q", *s)
		}
	}
	return buf.String()
}

func (d *pgCopyReader) readFile(
	ctx context.Context, input io.Reader, inputIdx int32, inputName string, progressFn progressFn,
) error {
	s := bufio.NewScanner(input)
	s.Split(bufio.ScanLines)
	s.Buffer(nil, int(d.opts.MaxRowSize))
	c := newPostgreStreamCopy(
		s,
		d.opts.Delimiter,
		d.opts.Null,
	)

	for count := int64(1); ; count++ {
		row, err := c.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return wrapRowErr(err, inputName, count, pgcode.Uncategorized, "")
		}
		if len(row) != len(d.conv.VisibleColTypes) {
			return makeRowErr(inputName, count, pgcode.Syntax,
				"expected %d values, got %d", len(d.conv.VisibleColTypes), len(row))
		}
		for i, s := range row {
			if s == nil {
				d.conv.Datums[i] = tree.DNull
			} else {
				d.conv.Datums[i], err = tree.ParseDatumStringAs(d.conv.VisibleColTypes[i], *s, d.conv.EvalCtx)
				if err != nil {
					col := d.conv.VisibleCols[i]
					return wrapRowErr(err, inputName, count, pgcode.Syntax,
						"parse %q as %s", col.Name, col.Type.SQLString())
				}
			}
		}

		if err := d.conv.Row(ctx, inputIdx, count); err != nil {
			return wrapRowErr(err, inputName, count, pgcode.Uncategorized, "")
		}
	}

	return d.conv.SendBatch(ctx)
}
