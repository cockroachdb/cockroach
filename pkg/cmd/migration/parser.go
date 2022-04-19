// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"unicode"

	"github.com/cockroachdb/errors"
)

type postgreStreamCopy struct {
	s         *bufio.Scanner
	delimiter rune
	null      string
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
