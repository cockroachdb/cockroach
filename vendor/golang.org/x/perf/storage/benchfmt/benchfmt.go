// Copyright 2016 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package benchfmt provides readers and writers for the Go benchmark format.
//
// The format is documented at https://golang.org/design/14313-benchmark-format
package benchfmt

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// Reader reads benchmark results from an io.Reader.
// Use Next to advance through the results.
//
//   br := benchfmt.NewReader(r)
//   for br.Next() {
//     res := br.Result()
//     ...
//   }
//   err = br.Err() // get any error encountered during iteration
//   ...
type Reader struct {
	s      *bufio.Scanner
	labels Labels
	// permLabels are permanent labels read from the start of the
	// file or provided by AddLabels. They cannot be overridden.
	permLabels Labels
	lineNum    int
	// cached from last call to newResult, to save on allocations
	lastName       string
	lastNameLabels Labels
	// cached from the last call to Next
	result *Result
	err    error
}

// NewReader creates a BenchmarkReader that reads from r.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		s:      bufio.NewScanner(r),
		labels: make(Labels),
	}
}

// AddLabels adds additional labels as if they had been read from the header of a file.
// It must be called before the first call to r.Next.
func (r *Reader) AddLabels(labels Labels) {
	r.permLabels = labels.Copy()
	for k, v := range labels {
		r.labels[k] = v
	}
}

// Result represents a single line from a benchmark file.
// All information about that line is self-contained in the Result.
// A Result is immutable once created.
type Result struct {
	// Labels is the set of persistent labels that apply to the result.
	// Labels must not be modified.
	Labels Labels
	// NameLabels is the set of ephemeral labels that were parsed
	// from the benchmark name/line.
	// NameLabels must not be modified.
	NameLabels Labels
	// LineNum is the line number on which the result was found
	LineNum int
	// Content is the verbatim input line of the benchmark file, beginning with the string "Benchmark".
	Content string
}

// SameLabels reports whether r and b have the same labels.
func (r *Result) SameLabels(b *Result) bool {
	return r.Labels.Equal(b.Labels) && r.NameLabels.Equal(b.NameLabels)
}

// Labels is a set of key-value strings.
type Labels map[string]string

// String returns the labels formatted as a comma-separated
// list enclosed in braces.
func (l Labels) String() string {
	var out bytes.Buffer
	out.WriteString("{")
	for k, v := range l {
		fmt.Fprintf(&out, "%q: %q, ", k, v)
	}
	if out.Len() > 1 {
		// Remove extra ", "
		out.Truncate(out.Len() - 2)
	}
	out.WriteString("}")
	return out.String()
}

// Keys returns a sorted list of the keys in l.
func (l Labels) Keys() []string {
	var out []string
	for k := range l {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Equal reports whether l and b have the same keys and values.
func (l Labels) Equal(b Labels) bool {
	if len(l) != len(b) {
		return false
	}
	for k := range l {
		if l[k] != b[k] {
			return false
		}
	}
	return true
}

// A Printer prints a sequence of benchmark results.
type Printer struct {
	w      io.Writer
	labels Labels
}

// NewPrinter constructs a BenchmarkPrinter writing to w.
func NewPrinter(w io.Writer) *Printer {
	return &Printer{w: w}
}

// Print writes the lines necessary to recreate r.
func (p *Printer) Print(r *Result) error {
	var keys []string
	// Print removed keys first.
	for k := range p.labels {
		if r.Labels[k] == "" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		if _, err := fmt.Fprintf(p.w, "%s:\n", k); err != nil {
			return err
		}
	}
	// Then print new or changed keys.
	keys = keys[:0]
	for k, v := range r.Labels {
		if v != "" && p.labels[k] != v {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		if _, err := fmt.Fprintf(p.w, "%s: %s\n", k, r.Labels[k]); err != nil {
			return err
		}
	}
	// Finally print the actual line itself.
	if _, err := fmt.Fprintf(p.w, "%s\n", r.Content); err != nil {
		return err
	}
	p.labels = r.Labels
	return nil
}

// parseNameLabels extracts extra labels from a benchmark name and sets them in labels.
func parseNameLabels(name string, labels Labels) {
	dash := strings.LastIndex(name, "-")
	if dash >= 0 {
		// Accept -N as an alias for /gomaxprocs=N
		_, err := strconv.Atoi(name[dash+1:])
		if err == nil {
			labels["gomaxprocs"] = name[dash+1:]
			name = name[:dash]
		}
	}
	parts := strings.Split(name, "/")
	labels["name"] = parts[0]
	for i, sub := range parts[1:] {
		equals := strings.Index(sub, "=")
		var key string
		if equals >= 0 {
			key, sub = sub[:equals], sub[equals+1:]
		} else {
			key = fmt.Sprintf("sub%d", i+1)
		}
		labels[key] = sub
	}
}

// newResult parses a line and returns a Result object for the line.
func (r *Reader) newResult(labels Labels, lineNum int, name, content string) *Result {
	res := &Result{
		Labels:  labels,
		LineNum: lineNum,
		Content: content,
	}
	if r.lastName != name {
		r.lastName = name
		r.lastNameLabels = make(Labels)
		parseNameLabels(name, r.lastNameLabels)
	}
	res.NameLabels = r.lastNameLabels
	return res
}

// Copy returns a new copy of the labels map, to protect against
// future modifications to labels.
func (l Labels) Copy() Labels {
	new := make(Labels)
	for k, v := range l {
		new[k] = v
	}
	return new
}

// TODO(quentin): How to represent and efficiently group multiple lines?

// Next returns the next benchmark result from the file. If there are
// no further results, it returns nil, io.EOF.
func (r *Reader) Next() bool {
	if r.err != nil {
		return false
	}
	copied := false
	havePerm := r.permLabels != nil
	for r.s.Scan() {
		r.lineNum++
		line := r.s.Text()
		if key, value, ok := parseKeyValueLine(line); ok {
			if _, ok := r.permLabels[key]; ok {
				continue
			}
			if !copied {
				copied = true
				r.labels = r.labels.Copy()
			}
			// TODO(quentin): Spec says empty value is valid, but
			// we need a way to cancel previous labels, so we'll
			// treat an empty value as a removal.
			if value == "" {
				delete(r.labels, key)
			} else {
				r.labels[key] = value
			}
			continue
		}
		// Blank line delimits the header. If we find anything else, the file must not have a header.
		if !havePerm {
			if line == "" {
				r.permLabels = r.labels.Copy()
			} else {
				r.permLabels = Labels{}
			}
		}
		if fullName, ok := parseBenchmarkLine(line); ok {
			r.result = r.newResult(r.labels, r.lineNum, fullName, line)
			return true
		}
	}
	if err := r.s.Err(); err != nil {
		r.err = err
		return false
	}
	r.err = io.EOF
	return false
}

// Result returns the most recent result generated by a call to Next.
func (r *Reader) Result() *Result {
	return r.result
}

// Err returns the error state of the reader.
func (r *Reader) Err() error {
	if r.err == io.EOF {
		return nil
	}
	return r.err
}

// parseKeyValueLine attempts to parse line as a key: value pair. ok
// indicates whether the line could be parsed.
func parseKeyValueLine(line string) (key, val string, ok bool) {
	for i, c := range line {
		if i == 0 && !unicode.IsLower(c) {
			return
		}
		if unicode.IsSpace(c) || unicode.IsUpper(c) {
			return
		}
		if i > 0 && c == ':' {
			key = line[:i]
			val = line[i+1:]
			break
		}
	}
	if key == "" {
		return
	}
	if val == "" {
		ok = true
		return
	}
	for len(val) > 0 && (val[0] == ' ' || val[0] == '\t') {
		val = val[1:]
		ok = true
	}
	return
}

// parseBenchmarkLine attempts to parse line as a benchmark result. If
// successful, fullName is the name of the benchmark with the
// "Benchmark" prefix stripped, and ok is true.
func parseBenchmarkLine(line string) (fullName string, ok bool) {
	space := strings.IndexFunc(line, unicode.IsSpace)
	if space < 0 {
		return
	}
	name := line[:space]
	if !strings.HasPrefix(name, "Benchmark") {
		return
	}
	return name[len("Benchmark"):], true
}
