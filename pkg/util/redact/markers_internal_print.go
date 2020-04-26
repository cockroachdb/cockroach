// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package redact

import "fmt"

// annotateArgs wraps the arguments to one of the print functions with
// an indirect formatter which either encloses the result of the
// display between redaction markers, or not.
func annotateArgs(args []interface{}) {
	for i, arg := range args {
		switch v := arg.(type) {
		case RedactableString:
			// Already formatted as redactable. Include as-is.
			// We need an intermediate struct because we don't
			// want the fmt machinery to re-format the string
			// object by adding quotes, expanding the byte slice, etc.
			args[i] = &passthrough{arg: []byte(v)}
		case RedactableBytes:
			args[i] = &passthrough{arg: v}

		case SafeFormatter:
			// calls to Format() by fmt.Print will be redirected to
			// v.SafeFormat(). This delegates the task of adding markers to
			// the object itself.
			args[i] = &redactFormatRedirect{v}

		case SafeValue:
			// calls to Format() by fmt.Print will be redirected to a
			// display of v without redaction markers.
			//
			// Note that we can't let the value be displayed as-is because
			// we must prevent any marker inside the value from leaking into
			// the result. (We want to avoid mismatched markers.)
			args[i] = &escapeArg{arg: arg, enclose: false}

		case SafeMessager:
			// Obsolete interface.
			// TODO(knz): Remove this.
			args[i] = &escapeArg{arg: v.SafeMessage(), enclose: false}

		default:
			// calls to Format() by fmt.Print will be redirected to a
			// display of v within redaction markers if the type is
			// considered unsafe, without markers otherwise. In any case,
			// occurrences of delimiters within are escaped.
			args[i] = &escapeArg{arg: v, enclose: !isSafeValue(v)}
		}
	}
}

// redactFormatRedirect wraps a SafeFormatter object and uses its
// SafeFormat method as-is to implement fmt.Formatter.
type redactFormatRedirect struct {
	arg SafeFormatter
}

// Format implements fmt.Formatter.
func (r *redactFormatRedirect) Format(s fmt.State, verb rune) {
	defer func() {
		if p := recover(); p != nil {
			e := escapeWriter{w: s}
			fmt.Fprintf(&e, "%%!%c(PANIC=SafeFormatter method: %v)", verb, p)
		}
	}()
	p := &printer{}
	p.escapeState = makeEscapeState(s, &p.buf)
	r.arg.SafeFormat(p, verb)
	_, _ = s.Write(p.buf.Bytes())
}

// passthrough passes a pre-formatted string through.
type passthrough struct{ arg []byte }

// Format implements fmt.Formatter.
func (p *passthrough) Format(s fmt.State, _ rune) {
	_, _ = s.Write(p.arg)
}

// escapeArg wraps an arbitrary value and ensures that any occurrence
// of the redaction markers in its representation are escaped.
//
// The result of printing out the value is enclosed within markers or
// not depending on the value of the enclose bool.
type escapeArg struct {
	arg     interface{}
	enclose bool
}

func (r *escapeArg) Format(s fmt.State, verb rune) {
	switch t := r.arg.(type) {
	case fmt.Formatter:
		// This is a special case from the default case below, which
		// allows a shortcut through the layers of the fmt package.
		p := &escapeState{
			State: s,
			w: escapeWriter{
				w:       s,
				enclose: r.enclose,
				strip:   r.enclose,
			}}
		defer func() {
			if recovered := recover(); recovered != nil {
				fmt.Fprintf(p, "%%!%c(PANIC=Format method: %v)", verb, recovered)
			}
		}()
		t.Format(p, verb)

	default:
		// TODO(knz): It would be possible to implement struct formatting
		// with conditional redaction based on field tag annotations here.
		p := &escapeWriter{w: s, enclose: r.enclose, strip: r.enclose}
		reproducePrintf(p, s, verb, r.arg)
	}
}

// printerfn is a helper struct for use by Sprintfn.
type printerfn struct {
	fn func(SafePrinter)
}

// SafeFormat implements the SafeFormatter interface.
func (p printerfn) SafeFormat(w SafePrinter, _ rune) {
	p.fn(w)
}
