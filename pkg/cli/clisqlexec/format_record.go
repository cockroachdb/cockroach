// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlexec

import (
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/util"
)

type recordReporter struct {
	cols        []string
	colRest     [][]string
	maxColWidth int
}

func (p *recordReporter) describe(w io.Writer, cols []string) error {
	for _, col := range cols {
		parts := strings.Split(col, "\n")
		for _, part := range parts {
			colLen := utf8.RuneCountInString(part)
			if colLen > p.maxColWidth {
				p.maxColWidth = colLen
			}
		}
		p.cols = append(p.cols, parts[0])
		p.colRest = append(p.colRest, parts[1:])
	}
	return nil
}

func (p *recordReporter) iter(w, _ io.Writer, rowIdx int, row []string) error {
	if len(p.cols) == 0 {
		// No record details; a summary will be printed at the end.
		return nil
	}
	fmt.Fprintf(w, "-[ RECORD %d ]\n", rowIdx+1)
	for j, r := range row {
		lines := strings.Split(r, "\n")
		for l, line := range lines {
			colLabel := p.cols[j]
			if l > 0 {
				colLabel = ""
			}
			lineCont := "+"
			if l == len(lines)-1 {
				lineCont = ""
			}
			// Note: special characters, including a vertical bar, in
			// the colLabel are not escaped here. This is in accordance
			// with the same behavior in PostgreSQL. However there is
			// special behavior for newlines.
			contChar := " "
			if len(p.colRest[j]) > 0 {
				contChar = "+"
			}
			fmt.Fprintf(w, "%-*s%s| %s%s\n", p.maxColWidth, colLabel, contChar, line, lineCont)
			for k, cont := range p.colRest[j] {
				if k == len(p.colRest[j])-1 {
					contChar = " "
				}
				fmt.Fprintf(w, "%-*s%s|\n", p.maxColWidth, cont, contChar)
			}
		}
	}
	return nil
}

func (p *recordReporter) doneRows(w io.Writer, seenRows int) error {
	if len(p.cols) == 0 {
		fmt.Fprintf(w, "(%d row%s)\n", seenRows, util.Pluralize(int64(seenRows)))
	}
	return nil
}

func (p *recordReporter) beforeFirstRow(_ io.Writer, _ RowStrIter) error { return nil }
func (p *recordReporter) doneNoRows(_ io.Writer) error                   { return nil }
