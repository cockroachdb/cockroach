// Copyright 2016 The Cockroach Authors.
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
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package pgerror

import (
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/pkg/errors"
)

func (pg *Error) Error() string {
	return pg.Message
}

// NewError creates an Error.
func NewError(code string, msg string) error {
	return newErrorf(code, msg)
}

// NewErrorf creates an Error.
func NewErrorf(code string, msg string, args ...interface{}) error {
	return newErrorf(code, msg, args...)
}

func newErrorf(code string, msg string, args ...interface{}) error {
	srcCtx := makeSrcCtx(2)
	return &Error{
		Message: fmt.Sprintf(msg, args...),
		Code:    code,
		Source:  &srcCtx,
	}
}

// makeSrcCtx creates a Error_Source value with contextual information
// about the caller at the requested depth.
func makeSrcCtx(depth int) Error_Source {
	f, l, fun := caller.Lookup(depth + 1)
	return Error_Source{File: f, Line: int32(l), Function: fun}
}

// WithHint annotates err with a hint for users to resolve the
// issue in the future.
// If err is not of type *Error, WithHint returns the error passed in.
func WithHint(err error, hint string) error {
	if pgErr, ok := err.(*Error); ok {
		pgErr.Hint = hint
		return pgErr
	}
	return err
}

// WithDetail annotates err with a detail.
// If err is not of type *Error, WithDetail returns the error passed in.
func WithDetail(err error, detail string) error {
	if pgErr, ok := err.(*Error); ok {
		pgErr.Detail = detail
		return pgErr
	}
	return err
}

// PGError returns an unwrapped Error.
func PGError(err error) (*Error, bool) {
	switch pgErr := errors.Cause(err).(type) {
	case *Error:
		return pgErr, true

	default:
		return nil, false
	}
}

// Format implements the Formatter interface.
func (pg *Error) Format(s fmt.State, verb rune) { formatError(pg, s, verb) }

func formatError(pgErr *Error, s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v%s", pgErr.Message, pgErr.verboseFormat())
			return
		}
		fallthrough
	case 's', 'q':
		if _, err := io.WriteString(s, pgErr.Error()); err != nil {
			panic(err)
		}
	}
}

func (pg *Error) verboseFormat() string {
	var ret string
	if pg.Code != "" {
		ret += "\ncode: " + pg.Code
	}
	if pg.Detail != "" {
		ret += "\ndetail: " + pg.Detail
	}
	if pg.Hint != "" {
		ret += "\nhint: " + pg.Hint
	}
	if s := pg.Source; s != nil {
		ret += fmt.Sprintf("\nlocation: %s, %s:%d", s.Function, s.File, s.Line)
	}
	return ret
}
