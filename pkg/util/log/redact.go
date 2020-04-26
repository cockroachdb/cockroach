// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// EditSensitiveData describes how the messages in log entries should
// be edited through the API.
type EditSensitiveData int

const (
	invalidEditMode EditSensitiveData = iota
	// WithMarkedSensitiveData is the "raw" log with sensitive data markers included.
	WithMarkedSensitiveData
	// WithFlattenedSensitiveData is the log with markers stripped.
	WithFlattenedSensitiveData
	// WithoutSensitiveData is the log with the sensitive data redacted.
	WithoutSensitiveData
)

// SelectEditMode returns an EditSensitiveData value that's suitable
// for use with NewDecoder depending on client-side desired
// "redact" and "keep redactable" flags.
// (See the documentation for the Logs and LogFile RPCs
// and that of the 'merge-logs' CLI command.)
func SelectEditMode(redact, keepRedactable bool) EditSensitiveData {
	editMode := WithMarkedSensitiveData
	if redact {
		editMode = WithoutSensitiveData
	}
	if !keepRedactable && !redact {
		editMode = WithFlattenedSensitiveData
	}
	return editMode
}

type redactEditor func(redactablePackage) redactablePackage

func getEditor(editMode EditSensitiveData) redactEditor {
	switch editMode {
	case WithMarkedSensitiveData:
		return func(r redactablePackage) redactablePackage {
			if !r.redactable {
				r.msg = reStripMarkers.ReplaceAll(r.msg, escapeBytes)
				enclosed := make([]byte, 0, len(r.msg)+len(startRedactBytes)+len(endRedactBytes))
				enclosed = append(enclosed, startRedactBytes...)
				enclosed = append(enclosed, r.msg...)
				enclosed = append(enclosed, endRedactBytes...)
				r.msg = enclosed
				r.redactable = true
			}
			return r
		}
	case WithFlattenedSensitiveData:
		return func(r redactablePackage) redactablePackage {
			if r.redactable {
				r.msg = reStripMarkers.ReplaceAll(r.msg, nil)
				r.redactable = false
			}
			return r
		}
	case WithoutSensitiveData:
		return func(r redactablePackage) redactablePackage {
			if r.redactable {
				r.msg = reStripSensitive.ReplaceAll(r.msg, redacted)
			} else {
				r.msg = redacted
				r.redactable = true
			}
			return r
		}
	case invalidEditMode:
		fallthrough
	default:
		panic(errors.AssertionFailedf("unrecognized mode: %v", editMode))
	}
}

// SafeMessager aliases a type definition.
type SafeMessager = errors.SafeMessager

// Safe constructs a SafeMessager.
var Safe = errors.Safe

// SafeFormatter is like SafeMessager but it's a format function.
// TODO(knz): Make this part of the errors package.
// TODO(knz): consider using richer arguments, like fmt.Formatter.
type SafeFormatter interface {
	SafeFormat(w io.Writer)
}

type redactablePackage struct {
	msg        []byte
	redactable bool
}

const startRedactable = "‹"
const endRedactable = "›"
const escapeMark = "?"
const redactableIndicator = "⋮"

var startRedactBytes = []byte(startRedactable)
var endRedactBytes = []byte(endRedactable)
var escapeBytes = []byte(escapeMark)
var redactableIndicatorBytes = []byte(redactableIndicator)

var redacted = []byte(startRedactable + "×" + endRedactable)
var reStripSensitive = regexp.MustCompile(startRedactable + "[^" + startRedactable + endRedactable + "]*" + endRedactable)
var reStripMarkers = regexp.MustCompile("[" + startRedactable + endRedactable + "]")

func redactTags(ctx context.Context, buf *strings.Builder) {
	tags := logtags.FromContext(ctx)
	if tags == nil {
		return
	}
	comma := ""
	var r redactable
	for _, t := range tags.Get() {
		buf.WriteString(comma)
		buf.WriteString(t.Key())
		if v := t.Value(); v != nil && v != "" {
			if len(t.Key()) > 1 {
				buf.WriteByte('=')
			}
			switch m := v.(type) {
			case SafeMessager:
				fmt.Fprint(buf, m.SafeMessage())
			case SafeFormatter:
				m.SafeFormat(buf)
			default:
				r.arg = v
				fmt.Fprint(buf, &r)
			}
		}
		comma = ","
	}
}

func annotateUnsafe(args []interface{}) {
	for i := range args {
		args[i] = annotateUnsafeValue(args[i])
	}
}

func annotateUnsafeValue(a interface{}) interface{} {
	switch m := a.(type) {
	case SafeMessager:
		a = m.SafeMessage()
	case SafeFormatter:
		a = &redactFormatRedirect{m}
	default:
		a = &redactable{a}
	}
	return a
}

type redactFormatRedirect struct {
	arg SafeFormatter
}

func (r *redactFormatRedirect) Format(s fmt.State, verb rune) {
	// TODO(knz): do something with s.Flag() and verb here.
	r.arg.SafeFormat(s)
}

type redactable struct {
	arg interface{}
}

func (r *redactable) Format(s fmt.State, verb rune) {
	rs := redactStream{s: s}
	fmt.Fprintf(&rs, MakeFormat(s, verb), r.arg)
}

type redactStream struct {
	s    io.Writer
	done bool
}

func (rs *redactStream) Write(b []byte) (int, error) {
	if rs.done {
		panic("Write called more than once")
	}
	// Trim final newlines/spaces, for convenience.
	end := len(b)
	for i := end - 1; i >= 0; i-- {
		if b[i] == '\n' || b[i] == ' ' {
			end = i
		} else {
			break
		}
	}
	b = b[:end]

	// Here we could choose to omit the output
	// entirely if there was nothing but empty space:
	// if len(b) == 0 { return 0, nil }

	// Now write the string.
	rs.doWrite(startRedactBytes)
	k := 0
	ls := len(startRedactBytes)
	le := len(endRedactBytes)
	for i := 0; i < len(b); i++ {
		// Ensure that occurrences of the delimiter inside the string get
		// escaped.
		if i+ls <= len(b) && bytes.Equal(b[i:i+ls], startRedactBytes) {
			rs.doWrite(b[k:i])
			rs.doWrite(escapeBytes)
			k = i + ls
			i += ls - 1
		} else if i+le <= len(b) && bytes.Equal(b[i:i+le], endRedactBytes) {
			rs.doWrite(b[k:i])
			rs.doWrite(escapeBytes)
			k = i + le
			i += le - 1
		}
	}
	rs.doWrite(b[k:])
	rs.doWrite(endRedactBytes)
	rs.done = true
	// NB: we return a zero length because we know that fmt.Fprintf
	// discards if. If this was not the case, we'd need to spend the
	// effort to accumulate the length and error in doWrite() and return
	// it here.
	return 0, nil
}

func (rs *redactStream) doWrite(b []byte) {
	_, _ = rs.s.Write(b)
}

// MakeFormat reproduces the format currently active in fmt.State and
// verb.
// TODO(knz): Move this into the errors package or some other exported
// string utility package.
func MakeFormat(s fmt.State, verb rune) string {
	var f strings.Builder
	f.WriteByte('%')
	if s.Flag('+') {
		f.WriteByte('+')
	}
	if s.Flag('-') {
		f.WriteByte('-')
	}
	if s.Flag('#') {
		f.WriteByte('#')
	}
	if s.Flag(' ') {
		f.WriteByte(' ')
	}
	if s.Flag('0') {
		f.WriteByte('0')
	}
	if w, wp := s.Width(); wp {
		f.WriteString(strconv.Itoa(w))
	}
	if p, pp := s.Precision(); pp {
		f.WriteByte('.')
		f.WriteString(strconv.Itoa(p))
	}
	f.WriteRune(verb)
	return f.String()
}

// TestingSetRedactable sets the redactable flag for usage in a test.
// The caller is responsible for calling the cleanup function.  This
// is exported for use in tests only -- it causes the logging
// configuration to be at risk of leaking unsafe information due to
// asynchronous direct writes to fd 2 / os.Stderr.
//
// See the discussion on SetupRedactionAndStderrRedirects() for
// details.
func TestingSetRedactable(redactableLogs bool) (cleanup func()) {
	prev := mainLog.redactableLogs.Swap(redactableLogs)
	return func() {
		mainLog.redactableLogs.Set(prev)
	}
}
