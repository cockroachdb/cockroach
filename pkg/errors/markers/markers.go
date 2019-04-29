// Copyright 2019 The Cockroach Authors.
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
// permissions and limitations under the License.

package markers

import (
	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/errors/errorspb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// Is determines whether one of the causes of the given error or any
// of its causes is equivalent to some reference error.
func Is(err, reference error) bool {
	if err == nil {
		return err == reference
	}

	// Direct reference comparison is the fastest, and most
	// likely to be true, so do this first.
	for c := err; c != nil; c = errbase.UnwrapOnce(c) {
		if c == reference {
			return true
		}
	}

	// Not directly equal. Try harder, using error marks. We don't this
	// during the loop above as it may be more expensive.
	//
	// Note: there is a more effective recursive algorithm that ensures
	// that any pair of string only gets compared once. Should the
	// following code become a performance bottleneck, that algorithm
	// can be considered instead.
	refMark := getMark(reference)
	for c := err; c != nil; c = errbase.UnwrapOnce(c) {
		if equalMarks(getMark(c), refMark) {
			return true
		}
	}
	return false
}

// If returns a predicate's return value the first time the predicate returns true.
func If(err error, pred func(err error) (interface{}, bool)) (interface{}, bool) {
	for c := err; c != nil; c = errbase.UnwrapOnce(c) {
		if v, ok := pred(c); ok {
			return v, ok
		}
	}
	return nil, false
}

// IsAny is like Is except that multiple references are compared.
func IsAny(err error, references ...error) bool {
	// First try using direct reference comparison.
	for c := err; c != nil; c = errbase.UnwrapOnce(c) {
		for _, refErr := range references {
			if err == refErr {
				return true
			}
		}
	}

	// Try harder with marks.
	// Note: there is a more effective recursive algorithm that ensures
	// that any pair of string only gets compared once. Should this
	// become a performance bottleneck, that algorithm can be considered
	// instead.
	refMarks := make([]errorMark, len(references))
	for i, refErr := range references {
		refMarks[i] = getMark(refErr)
	}
	for c := err; c != nil; c = errbase.UnwrapOnce(c) {
		errMark := getMark(err)
		for _, refMark := range refMarks {
			if equalMarks(errMark, refMark) {
				return true
			}
		}
	}
	return false
}

type errorMark struct {
	msg   string
	types []string
}

// equalMarks compares two error markers.
func equalMarks(m1, m2 errorMark) bool {
	if m1.msg != m2.msg {
		return false
	}
	for i, t := range m1.types {
		if t != m2.types[i] {
			return false
		}
	}
	return true
}

// getMark computes a marker for the given error.
func getMark(err error) errorMark {
	if m, ok := err.(*withMark); ok {
		return m.mark
	}
	m := errorMark{msg: err.Error(), types: []string{errbase.FullTypeName(err)}}
	for c := errbase.UnwrapOnce(err); c != nil; c = errbase.UnwrapOnce(c) {
		m.types = append(m.types, errbase.FullTypeName(c))
	}
	return m
}

// Mark creates an explicit mark for the given error, using
// the same mark as some reference error.
func Mark(err error, reference error) error {
	if err == nil {
		return nil
	}
	refMark := getMark(reference)
	return &withMark{cause: err, mark: refMark}
}

// withMark carries an explicit mark.
type withMark struct {
	cause error
	mark  errorMark
}

func (m *withMark) Error() string { return m.cause.Error() }
func (m *withMark) Cause() error  { return m.cause }
func (m *withMark) Unwrap() error { return m.cause }

func encodeMark(err error) (msg string, _ []string, payload protoutil.SimpleMessage) {
	m := err.(*withMark)
	payload = &errorspb.MarkPayload{Msg: m.mark.msg, Types: m.mark.types}
	return "", nil, payload
}

func decodeMark(cause error, _ string, _ []string, payload protoutil.SimpleMessage) error {
	m := payload.(*errorspb.MarkPayload)
	return &withMark{cause: cause, mark: errorMark{msg: m.Msg, types: m.Types}}
}

func init() {
	errbase.RegisterWrapperEncoder(errbase.FullTypeName(&withMark{}), encodeMark)
	errbase.RegisterWrapperDecoder(errbase.FullTypeName(&withMark{}), decodeMark)
}
