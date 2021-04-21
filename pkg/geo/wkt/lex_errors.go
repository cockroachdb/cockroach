// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package wkt

import (
	"fmt"
	"strings"
)

// SyntaxError is an error that occurs during parsing of a WKT string.
type SyntaxError struct {
	wkt       string
	problem   string
	lineNum   int
	lineStart int
	linePos   int
	hint      string
}

// Error generates a detailed syntax error message with line and pos numbers as well as a snippet of
// the erroneous input.
func (e *SyntaxError) Error() string {
	// These constants define the maximum number of characters of the line to show on each side of the cursor.
	const (
		leftPadding  = 30
		rightPadding = 30
	)

	// Print the problem along with line and pos number.
	err := fmt.Sprintf("syntax error: %s at line %d, pos %d\n", e.problem, e.lineNum, e.linePos)

	// Find the position of the end of the line.
	lineEnd := strings.IndexRune(e.wkt[e.lineStart:], '\n')
	if lineEnd == -1 {
		lineEnd = len(e.wkt)
	} else {
		lineEnd += e.lineStart
	}

	// Prepend the line with the line number.
	strLinePrefix := fmt.Sprintf("LINE %d: ", e.lineNum)
	strLineSuffix := "\n"

	// Trim the start and end of the line as needed.
	snipPos := e.linePos
	snipStart := e.lineStart
	leftMin := e.lineStart + e.linePos - leftPadding
	if snipStart < leftMin {
		snipPos -= leftMin - snipStart
		snipStart = leftMin
		strLinePrefix += "..."
	}
	snipEnd := lineEnd
	rightMax := e.lineStart + e.linePos + rightPadding
	if snipEnd > rightMax {
		snipEnd = rightMax
		strLineSuffix = "..." + strLineSuffix
	}

	// Print a cursor pointing to the token where the problem occurred.
	snippet := e.wkt[snipStart:snipEnd]
	snippet = strings.ReplaceAll(snippet, "\t", " ")
	err += strLinePrefix + snippet + strLineSuffix
	err += fmt.Sprintf("%s^", strings.Repeat(" ", len(strLinePrefix)+snipPos))

	// Print a hint, if applicable.
	if e.hint != "" {
		err += fmt.Sprintf("\nHINT: %s", e.hint)
	}

	return err
}
