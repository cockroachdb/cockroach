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
	"bufio"
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
	// The current row being read.
	var row []string
	// the current field being read.
	var field []rune

	// If we have an escaping char defined, seeing it means the next char is to be
	// treated as escaped -- usually that means literal but has some specific
	// mappings defined as well.
	var nextLiteral bool

	// If we have an enclosing char defined, seeing it begins reading a field --
	// which means we do not look for separators until we see the end of the field
	// as indicated by the matching enclosing char.
	var readingField bool

	reader := bufio.NewReaderSize(in, 1024*64)

	for {
		c, w, err := reader.ReadRune()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if c == utf8.RuneError && w == 1 {
			return errors.New("Invalid UTF8")
		}

		// Do we need to check for escaping?
		if d.hasEscapeChar {
			if nextLiteral {
				nextLiteral = false
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
				continue
			}

			if c == d.escapeChar {
				nextLiteral = true
				continue
			}
		}

		// If we have a defined enclose char, check if we're starting or stopping
		// an enclosed field. Technically, the enclose char can be made mandatory,
		// so potentially we could have a flag to enforce that, returning an error
		// if it is missing, but we don't need to care for simply decoding.
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

	if nextLiteral {
		return errors.New("unmatched literal")
	}
	if readingField {
		return errors.New("unmatched field enclosure")
	}

	// flush the last row if we have one.
	if len(row) > 0 {
		if err := d.row(row); err != nil {
			return err
		}
	}
	return nil
}
