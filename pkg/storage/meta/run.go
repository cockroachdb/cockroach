// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package meta provides facilities for running metamorphic, property-based
// testing. By running logically equivalent operations with different
// conditions, metamorphic tests can identify bugs without requiring an oracle.
//
// Package meta will be moved to github.com/cockroachdb/metamorphic once it's
// more stable.
package meta

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// ItemWeight holds an item and its corresponding weight.
type ItemWeight[I any] struct {
	Item   I
	Weight int
}

// Weighted takes a slice of items and their weights, producing a function that
// randomly picks items from the slice using the distribution indicated by the
// weights.
func Weighted[I any](items []ItemWeight[I]) func(rng *rand.Rand) I {
	var total int
	for i := 0; i < len(items); i++ {
		total += items[i].Weight
	}
	return func(rng *rand.Rand) I {
		w := rng.Intn(total)
		for i := 0; i < len(items); i++ {
			w -= items[i].Weight
			if w < 0 {
				return items[i].Item
			}
		}
		panic("unreachable")
	}
}

// Generate generates a sequence of n items, calling fn(rng)(rng) to produce
// each item. It's intended to be used with a function returned by Weighted,
// whhere the item itself is func(rng *rand.Rand).
func Generate[I any](rng *rand.Rand, n int, fn func(*rand.Rand) func(*rand.Rand) I) []I {
	items := make([]I, n)
	for i := 0; i < n; i++ {
		items[i] = fn(rng)(rng)
	}
	return items
}

// RunOne runs the provided operations, using the provided initial state.
func RunOne[S any](t *testing.T, initial S, ops []Op[S]) {
	l := &Logger{t: t}

	// TODO(jackson): Support teeing to additional sink(s), eg, a file.
	l.w = &l.history
	l.wIndent = newlineIndentingWriter{Writer: l.w, indent: []byte("  ")}

	// Ensure pancis result in printing the history.
	defer func() {
		if r := recover(); r != nil {
			l.Fatal(r)
		}
	}()

	s := initial
	for i := 0; i < len(ops); i++ {
		// Set Logger's per-Op context.
		l.opNumber = i
		l.op = ops[i]
		l.logged = false

		fmt.Fprintf(l, "op %6d: %s = ", l.opNumber, l.op)
		ops[i].Run(l, s)

		if !l.logged {
			fmt.Fprint(l, "-")
		}
		fmt.Fprintln(l)
	}
	fmt.Fprintln(l, "done")
	if t.Failed() {
		t.Logf("History:\n\n%s", l.history.String())
	}
}

// Op represents a single operation within a metamorphic test.
type Op[S any] interface {
	fmt.Stringer

	// Run runs the operation, logging its outcome to Logger.
	Run(*Logger, S)
}

// Logger logs test operation's outputs and errors, maintaining a cumulative
// history of the test. Logger may be used analogously to the standard library's
// testing.TB.
type Logger struct {
	w        io.Writer
	wIndent  newlineIndentingWriter
	t        testing.TB
	history  bytes.Buffer
	lastByte byte

	// op context; updated before each operation is run
	opNumber int
	op       fmt.Stringer
	logged   bool
}

// Assert that *Logger implements require.TestingT.
var _ require.TestingT = (*Logger)(nil)

// Commentf writes a comment to the log file. Commentf always appends a newline
// after the comment. Commentf may prepend a newline before the message if there
// isn't already one.
func (l *Logger) Commentf(format string, args ...any) {
	if l.lastByte != '\n' {
		fmt.Fprintln(&l.wIndent)
	}
	l.Logf("// "+format+"\n", args...)
}

// Error fails the test run, logging the provided error.
func (l *Logger) Error(err error) {
	l.Log("error: ", err)
	l.t.Error(err)
}

// Errorf fails the test run, logging the provided message.
func (l *Logger) Errorf(format string, args ...any) {
	l.Logf("error: "+format, args...)
	l.t.Errorf(format, args...)
}

// FailNow marks the function as having failed and stops its execution by
// calling runtime.Goexit. FailNow is implemented by calling through to the
// underlying *testing.T's FailNow.
func (l *Logger) FailNow() {
	l.t.Logf("History:\n\n%s", l.history.String())
	l.t.FailNow()
}

// Fatal is equivalent to Log followed by FailNow.
func (l *Logger) Fatal(args ...any) {
	l.Errorf("%s", fmt.Sprint(args...))
	l.FailNow()
}

// Log formats its arguments using default formatting, analogous to Print, and
// records the text in the test's recorded history.
func (l *Logger) Log(args ...any) {
	l.logged = true
	fmt.Fprint(&l.wIndent, args...)
}

// Logf formats its arguments according to the format, analogous to Printf, and
// records the text in the test's recorded history.
func (l *Logger) Logf(format string, args ...interface{}) {
	l.logged = true
	fmt.Fprintf(&l.wIndent, format, args...)
}

// Write implements io.Writer.
func (l *Logger) Write(b []byte) (int, error) {
	n, err := l.w.Write(b)
	if n > 0 {
		l.lastByte = b[n-1]
	}
	return n, err
}

// newlineIndentingWriter wraps a Writer. Whenever a '\n' is written, the
// newlineIndentingWriter writes the '\n' and the configured `indent` byte slice
// to the writer. All other bytes written are written to the underlying Writer
// verbatim.
type newlineIndentingWriter struct {
	io.Writer
	indent []byte
}

func (w *newlineIndentingWriter) Write(b []byte) (n int, err error) {
	for len(b) > 0 {
		if i := bytes.IndexByte(b, '\n'); i >= 0 {
			n2, err := w.Writer.Write(b[:i+1])
			n += n2
			if err != nil {
				return n, err
			}
			b = b[i+1:]
			n2, err = w.Writer.Write(w.indent)
			n += n2
			if err != nil {
				return n, err
			}
			continue
		}

		n2, err := w.Writer.Write(b)
		n += n2
		return n, err
	}
	return n, err
}
