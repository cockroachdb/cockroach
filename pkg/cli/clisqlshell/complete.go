// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlshell

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/knz/bubbline"
	"github.com/knz/bubbline/computil"
	"github.com/knz/bubbline/editline"
)

// getCompletions implements the editline AutoComplete interface.
func (b *bubblineReader) getCompletions(
	v [][]rune, line, col int,
) (msg string, comps bubbline.Completions) {
	// In COPY mode, we can't complete anything.
	if b.sql.inCopy() {
		return "", comps
	}

	sql, offset := computil.Flatten(v, line, col)

	if col > 1 && v[line][col-1] == '?' && v[line][col-2] == '?' {
		// This is a syntax check.
		sql = strings.TrimSuffix(sql[:offset], "\n")
		helpText, err := b.sql.serverSideParse(sql)
		if helpText != "" {
			// We have a completion suggestion. Use that.
			msg = fmt.Sprintf("\nSuggestion:\n%s\n", helpText)
		} else if err != nil {
			// Some other error. Display it.
			var buf strings.Builder
			clierror.OutputError(&buf, err, true /*showSeverity*/, false /*verbose*/)
			msg = buf.String()
		}
		return msg, comps
	}

	// TODO(knz): do not read all the rows - stop after a maximum.
	rows, err := b.sql.runShowCompletions(sql, offset)
	if err != nil {
		var buf strings.Builder
		clierror.OutputError(&buf, err, true /*showSeverity*/, false /*verbose*/)
		msg = buf.String()
		return msg, comps
	}
	// TODO(knz): Extend this logic once the advanced completion engine
	// is finalized: https://github.com/cockroachdb/cockroach/pull/87606
	candidates := make([]string, 0, len(rows))
	for _, row := range rows {
		candidates = append(candidates, row[0])
	}
	_, wstart, wend := computil.FindWord(v, line, col)
	return msg, editline.SimpleWordsCompletion(candidates, "keywords", col, wstart, wend)
}

// runeOffset converts the 2D rune cursor to a 1D offset from the
// start. The result can be used by the byteOffsetToRuneOffset
// conversion function.
func runeOffset(v [][]rune, line, col int) int {
	roffset := 0
	for i, l := range v {
		if i > 0 {
			// Increment for newline from previous line.
			roffset++
		}
		if line != i {
			roffset += len(l)
			continue
		}
		if col > len(l) {
			col = len(l)
		}
		roffset += col
		break
	}
	return roffset
}
