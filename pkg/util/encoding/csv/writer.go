// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

package csv

import (
	"bufio"
	"bytes"
	"io"
	"unicode"
)

// A Writer writes records to a CSV encoded file.
//
// As returned by NewWriter, a Writer writes records terminated by a
// newline and uses ',' as the field delimiter. The exported fields can be
// changed to customize the details before the first call to Write or WriteAll.
//
// Comma is the field delimiter.
//
// If UseCRLF is true, the Writer ends each record with \r\n instead of \n.
type Writer struct {
	Comma                    rune // Field delimiter (set to ',' by NewWriter)
	Escape                   rune
	UseCRLF                  bool // True to use \r\n as the line terminator
	SkipNewline              bool // True to skip \n as the line terminator
	w                        *bufio.Writer
	scratch                  *bytes.Buffer
	i                        int
	midRow                   bool
	currentRecordNeedsQuotes bool
	maybeTerminatorString    bool
}

// NewWriter returns a new Writer that writes to w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		Comma:   ',',
		Escape:  '"',
		w:       bufio.NewWriter(w),
		scratch: new(bytes.Buffer),
	}
}

// Writer writes a single CSV record to w along with any necessary quoting.
// A record is a slice of strings with each string being one field.
func (w *Writer) Write(record []string) error {
	if !validDelim(w.Comma) {
		return errInvalidDelim
	}

	for _, field := range record {
		if err := w.WriteField(bytes.NewBufferString(field)); err != nil {
			return err
		}
	}
	return w.FinishRecord()
}

// FinishRecord writes the newline at the end of a record.
// Only call FinishRecord in conjunction with WriteField,
// not Write.
func (w *Writer) FinishRecord() (err error) {
	if !w.SkipNewline {
		if w.UseCRLF {
			_, err = w.w.WriteString("\r\n")
		} else {
			err = w.w.WriteByte('\n')
		}
	}
	w.midRow = false
	return err
}

// WriteField writes an individual field.
func (w *Writer) WriteField(field *bytes.Buffer) (e error) {
	if w.midRow {
		if _, err := w.w.WriteRune(w.Comma); err != nil {
			return err
		}
	}
	w.midRow = true
	w.i = 0
	w.currentRecordNeedsQuotes = false
	w.scratch.Reset()
	w.maybeTerminatorString = true
	// Iterate through the input rune by rune, escaping where needed,
	// modifying linebreaks as configured by w.UseCRLF, and tracking
	// whether the string as a whole needs to be enclosed in quotes.
	// We write to a scratch buffer instead of directly to w since we
	// don't know yet if the first byte needs to be '"'.
	var r rune
	for ; e == nil; w.i++ {
		r, _, e = field.ReadRune()
		if e != nil {
			break
		}
		// Check if the string exactly equals the Postgres terminator string \.
		w.maybeTerminatorString = w.maybeTerminatorString && ((w.i == 0 && r == '\\') || (w.i == 1 && r == '.'))
		switch r {
		case '"', w.Escape:
			w.currentRecordNeedsQuotes = true
			_, e = w.scratch.WriteRune(w.Escape)
			if e == nil {
				_, e = w.scratch.WriteRune(r)
			}
		case w.Comma:
			w.currentRecordNeedsQuotes = true
			_, e = w.scratch.WriteRune(r)
		case '\r':
			// TODO: This is copying how the previous implementation behaved,
			// even though it looks wrong: if we're omitting the return, why
			// do we still need to quote the field?
			w.currentRecordNeedsQuotes = true
			if !w.UseCRLF {
				e = w.scratch.WriteByte('\r')
			}
		case '\n':
			w.currentRecordNeedsQuotes = true
			if w.UseCRLF {
				_, e = w.scratch.WriteString("\r\n")
			} else {
				e = w.scratch.WriteByte('\n')
			}
		default:
			if w.i == 0 {
				w.currentRecordNeedsQuotes = unicode.IsSpace(r)
			}
			_, e = w.scratch.WriteRune(r)
		}
	}

	if e != io.EOF {
		return e
	}

	w.maybeTerminatorString = w.maybeTerminatorString && w.i == 2
	w.currentRecordNeedsQuotes = w.currentRecordNeedsQuotes || w.maybeTerminatorString

	// By now we know whether or not the entire field needs to be quoted.
	// Fields with a Comma, fields with a quote or newline, and
	// fields which start with a space must be enclosed in quotes.
	// We used to quote empty strings, but we do not anymore (as of Go 1.4).
	// The two representations should be equivalent, but Postgres distinguishes
	// quoted vs non-quoted empty string during database imports, and it has
	// an option to force the quoted behavior for non-quoted CSV but it has
	// no option to force the non-quoted behavior for quoted CSV, making
	// CSV with quoted empty strings strictly less useful.
	// Not quoting the empty string also makes this package match the behavior
	// of Microsoft Excel and Google Drive.
	// For Postgres, quote the data terminating string `\.`.
	if w.currentRecordNeedsQuotes {
		e = w.w.WriteByte('"')
		if e != nil {
			return e
		}
	}
	_, e = w.scratch.WriteTo(w.w)
	if w.currentRecordNeedsQuotes {
		e = w.w.WriteByte('"')
	}

	return e
}

func (w *Writer) ForceEmptyField() error {
	if w.midRow {
		if _, err := w.w.WriteRune(w.Comma); err != nil {
			return err
		}
	}
	w.midRow = true
	w.i = 0
	w.currentRecordNeedsQuotes = false
	w.scratch.Reset()
	w.maybeTerminatorString = true
	_, err := w.w.WriteString(`""`)
	return err
}

// Flush writes any buffered data to the underlying io.Writer.
// To check if an error occurred during the Flush, call Error.
func (w *Writer) Flush() {
	w.w.Flush()
}

// Error reports any error that has occurred during a previous Write or Flush.
func (w *Writer) Error() error {
	_, err := w.w.Write(nil)
	return err
}

// WriteAll writes multiple CSV records to w using Write and then calls Flush.
func (w *Writer) WriteAll(records [][]string) error {
	for _, record := range records {
		err := w.Write(record)
		if err != nil {
			return err
		}
	}
	return w.w.Flush()
}
