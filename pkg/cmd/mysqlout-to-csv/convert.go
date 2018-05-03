// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"io"
	"unicode/utf8"

	"github.com/pkg/errors"
)

type dumpReader struct {
	rows  int
	width int

	f func([]string) error

	fieldSep rune // default \t
	rowSep   rune // default \n

	hasEscapeChar  bool
	escapeChar     rune
	hasEncloseChar bool
	encloseChar    rune

	bufSizeKb int
}

func (d *dumpReader) row(row []string) error {
	if d.width != 0 && d.width != len(row) {
		return errors.Errorf("row %d: mismatched row size %d, previously %d: %#v", d.rows+1, len(row), d.width, row)
	}
	d.width = len(row)
	if err := d.f(row); err != nil {
		return err
	}
	d.rows++

	return nil
}

func (d *dumpReader) Process(in io.Reader) error {
	start := 0
	buf := make([]byte, 1024*d.bufSizeKb)

	var row []string
	var field []rune
	var nextLiteral bool
	var readingField bool

	for {
		n, err := in.Read(buf[start:])
		total := n + start
		start = 0

		for i := 0; i < total; {
			c, w := utf8.DecodeRune(buf[i:])
			if c == utf8.RuneError && w == 1 {
				remaining := total - i
				//				fmt.Printf("\n\n i %d n %d r total %d remaining %d\n\n", i, n, total, remaining)
				if remaining < 4 {
					copy(buf, buf[i:])
					start = remaining
					break
				}
				return errors.Errorf("row %d, chunk byte %d of %d: invalid %d byte utf8 in %v", d.rows+1, i, total, w, buf[i:i+4])
			}
			i += w

			// Do we need to check for escaping?
			if d.hasEscapeChar {
				if nextLiteral {
					// See https://dev.mysql.com/doc/refman/8.0/en/load-data.html.
					switch c {
					case '0':
						field = append(field, rune(0))
					case 'b':
						field = append(field, '\b')
					case 'n':
						field = append(field, '\n')
					case 'r':
						field = append(field, '\r')
					case 't':
						field = append(field, '\t')
					case 'Z':
						field = append(field, rune(26))
					case 'N':
						field = append(field, '\\', 'N')
					default:
						field = append(field, c)
					}
					nextLiteral = false
					continue
				}

				if c == d.escapeChar {
					nextLiteral = true
					continue
				}
			}

			if d.hasEncloseChar && c == d.encloseChar {
				readingField = !readingField
				continue
			}

			// Are we done with the field, or even the whole row?
			if !readingField && (c == d.fieldSep || c == d.rowSep) {
				row = append(row, string(field))
				field = field[:0]
				if c == d.rowSep {
					if err := d.row(row); err != nil {
						return err
					}
					row = make([]string, 0, len(row))
				}
				continue
			}

			field = append(field, c)
		}
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

	}

	if nextLiteral {
		return errors.New("unmatched literal")
	}

	if readingField {
		return errors.New("unmatched field enclosure")
	}

	if len(row) > 0 {
		if err := d.row(row); err != nil {
			return err
		}
	}

	return nil
}
