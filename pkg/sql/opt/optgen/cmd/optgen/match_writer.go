// Copyright 2018 The Cockroach Authors.
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
	"fmt"
	"io"
	"strings"
)

// matchWriter keeps track of the indentation level so that callers can nest
// and unnest code in whatever pattern they choose.
type matchWriter struct {
	writer  io.Writer
	nesting int
}

// marker opaquely stores a nesting level. The nest methods return the marker
// for the nesting level that existed before the nest call, and the
// unnestToMarker method returns to that level given the marker.
type marker int

// nest writes a formatted string with no identation and then increases the
// indentation. It returns a marker for the indentation level before the
// increase. This marker can be passed to unnestToMarker to return to that
// level.
func (w *matchWriter) nest(format string, args ...interface{}) marker {
	w.write(format, args...)
	w.nesting++
	return marker(w.nesting - 1)
}

// nestIndent writes an indented formatted string and then increases the
// indentation. It returns a marker for the indentation level before the
// increase. This marker can be passed to unnestToMarker to return to that
// level.
func (w *matchWriter) nestIndent(format string, args ...interface{}) marker {
	w.writeIndent(format, args...)
	w.nesting++
	return marker(w.nesting - 1)
}

// marker returns the a marker for the current nesting level, which can be
// passed to unnestToMarker in order to return to this level.
func (w *matchWriter) marker() marker {
	return marker(w.nesting)
}

func (w *matchWriter) write(format string, args ...interface{}) {
	fmt.Fprintf(w.writer, format, args...)
}

func (w *matchWriter) writeIndent(format string, args ...interface{}) {
	fmt.Fprint(w.writer, strings.Repeat("  ", w.nesting))
	fmt.Fprintf(w.writer, format, args...)
}

func (w *matchWriter) newline() {
	fmt.Fprintf(w.writer, "\n")
}

func (w *matchWriter) unnest(suffix string) {
	w.unnestToMarker(marker(w.nesting-1), suffix)
}

func (w *matchWriter) unnestToMarker(marker marker, suffix string) {
	for w.nesting > int(marker) {
		w.nesting--
		fmt.Fprint(w.writer, strings.Repeat("  ", w.nesting))
		fmt.Fprint(w.writer, suffix)
	}
}
