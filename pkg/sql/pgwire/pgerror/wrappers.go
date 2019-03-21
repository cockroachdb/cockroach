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
//

package pgerror

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Causer is a public definition for the interface provided
// by the package github.com/pkg/errors.
type Causer interface {
	error
	Cause() error
}

var _ Causer = &withDefaultCode{}
var _ Causer = &withMessage{}
var _ Causer = &withDetail{}
var _ Causer = &withHint{}
var _ Causer = &withTelemetryKey{}
var _ Causer = &withSource{}
var _ Causer = &withSafeDetail{}
var _ Causer = &withInternalError{}
var _ Causer = &withUnknownErrorPayload{}

func (e *withDefaultCode) Cause() error         { return e.cause }
func (e *withDetail) Cause() error              { return e.cause }
func (e *withHint) Cause() error                { return e.cause }
func (e *withInternalError) Cause() error       { return e.cause }
func (e *withMessage) Cause() error             { return e.cause }
func (e *withSafeDetail) Cause() error          { return e.cause }
func (e *withSource) Cause() error              { return e.cause }
func (e *withTelemetryKey) Cause() error        { return e.cause }
func (e *withUnknownErrorPayload) Cause() error { return e.cause }

func (e *withDefaultCode) Error() string         { return e.cause.Error() }
func (e *withDetail) Error() string              { return e.cause.Error() }
func (e *withHint) Error() string                { return e.cause.Error() }
func (e *withInternalError) Error() string       { return e.cause.Error() }
func (e *withMessage) Error() string             { return fmt.Sprintf("%s: %s", e.msg, e.cause.Error()) }
func (e *withSafeDetail) Error() string          { return e.cause.Error() }
func (e *withSource) Error() string              { return e.cause.Error() }
func (e *withTelemetryKey) Error() string        { return e.cause.Error() }
func (e *withUnknownErrorPayload) Error() string { return e.cause.Error() }

var _ fmt.Formatter = &withInternalError{}
var _ fmt.Formatter = &withUnknownErrorPayload{}
var _ fmt.Formatter = &withDefaultCode{}
var _ fmt.Formatter = &withDetail{}
var _ fmt.Formatter = &withHint{}
var _ fmt.Formatter = &withMessage{}
var _ fmt.Formatter = &withSafeDetail{}
var _ fmt.Formatter = &withSource{}
var _ fmt.Formatter = &withTelemetryKey{}

func (e *withInternalError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v\n-- internal error code: %s", e.cause, e.internalErrorCode)
			return
		}
		fallthrough
	case 's', 'q':
		fmtErr(s, verb, e.cause)
	}
}

func (e *withUnknownErrorPayload) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v\n-- unknown payload: %s", e.cause, e.payloadType)
			return
		}
		fallthrough
	case 's', 'q':
		fmtErr(s, verb, e.cause)
	}
}

func (e *withDefaultCode) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v\n-- code: %s", e.cause, e.code)
			return
		}
		fallthrough
	case 's', 'q':
		fmtErr(s, verb, e.cause)
	}
}

func (e *withDetail) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v\n-- detail:\n%s", e.cause, e.detail)
			return
		}
		fallthrough
	case 's', 'q':
		fmtErr(s, verb, e.cause)
	}
}

func (e *withHint) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v\n-- hint:\n%s", e.cause, e.hint)
			return
		}
		fallthrough
	case 's', 'q':
		fmtErr(s, verb, e.cause)
	}
}

func (e *withMessage) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v\n-- msg: %s", e.cause, e.msg)
			return
		}
		fallthrough
	case 's':
		fmt.Fprintf(s, "%s: ", e.msg)
		fmtErr(s, verb, e.cause)
	case 'q':
		fmt.Fprintf(s, "%q", e.Error())
	}
}

func (e *withSafeDetail) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') && e.detail != nil {
			fmt.Fprintf(s, "%+v", e.cause)
			if e.detail.Source != nil {
				src := e.detail.Source
				fmt.Fprintf(s, "\n-- at:\n%s:%d %s()", src.File, src.Line, src.Function)
			}
			fmt.Fprintf(s, "\n-- reportable: %s", e.detail.SafeMessage)
			if e.detail.EncodedStackTrace != "" {
				if st, ok := log.DecodeStackTrace(e.detail.EncodedStackTrace); ok {
					fmt.Fprintf(s, "\n%s", log.PrintStackTrace(st))
				} else {
					fmt.Fprintf(s, "\n%s", e.detail.EncodedStackTrace)
				}
			}
			return
		}
		fallthrough
	case 's', 'q':
		fmtErr(s, verb, e.cause)
	}
}

func (e *withSource) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') && e.source != nil {
			src := e.source
			fmt.Fprintf(s, "%+v\n-- at:\n%s:%d %s()", e.cause, src.File, src.Line, src.Function)
			return
		}
		fallthrough
	case 's', 'q':
		fmtErr(s, verb, e.cause)
	}
}

func (e *withTelemetryKey) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v\n-- key:\n%s", e.cause, e.key)
			return
		}
		fallthrough
	case 's', 'q':
		fmtErr(s, verb, e.cause)
	}
}

func fmtErr(s fmt.State, verb rune, err error) {
	flags := ""
	if s.Flag('+') {
		flags += "+"
	}
	fmtString := fmt.Sprintf("%%%s%c", flags, verb)
	fmt.Fprintf(s, fmtString, err)
}
