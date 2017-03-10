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
)

// pgError is an error that contains optional Postgres structured error
// fields. The fields can then be retrieved from the error using the
// functions like Hint and Detail below.
//
// See https://www.postgresql.org/docs/current/static/protocol-error-fields.html
// for a list of all Postgres error fields, most of which are optional and can
// be used to provide auxiliary error information.
//
// pgError also implement the errors.causer interface, meaning that errors.Cause
// will work correctly with it.
type pgError struct {
	error
	e PGWireError
}

// from github.com/pkg/errors package.
type causer interface {
	Cause() error
}

func (pg *pgError) Cause() error {
	return pg.error
}

func (pg *pgError) Format(s fmt.State, verb rune) { formatPGError(pg, s, verb) }

func (pg *pgError) verboseFormat() string {
	var ret string
	if pg.e.Code != "" {
		ret += "\ncode: " + pg.e.Code
	}
	if pg.e.Detail != "" {
		ret += "\ndetail: " + pg.e.Detail
	}
	if pg.e.Hint != "" {
		ret += "\nhint: " + pg.e.Hint
	}
	if s := pg.e.Source; s != nil {
		ret += fmt.Sprintf("\nlocation: %s, %s:%d", s.Function, s.File, s.Line)
	}
	return ret
}

// WithPGCode annotates err with a Postgres error code.
// If err is nil, WithPGCode returns nil.
func WithPGCode(err error, code string) error {
	if err == nil || code == "" {
		return err
	}

	if pgErr, ok := err.(*pgError); ok {
		pgErr.e.Code = code
		return pgErr
	}

	return &pgError{
		error: err,
		e: PGWireError{
			Message: err.Error(),
			Code:    code,
		},
	}
}

// WithDetail annotates err with a detail message.
// If err is nil, WithDetail returns nil.
func WithDetail(err error, detail string) error {
	if err == nil || detail == "" {
		return err
	}

	if pgErr, ok := err.(*pgError); ok {
		pgErr.e.Detail = detail
		return pgErr
	}

	return &pgError{
		error: err,
		e: PGWireError{
			Message: err.Error(),
			Detail:  detail,
		},
	}
}

// WithHint annotates err with a hint for users to resolve the
// issue in the future.
// If err is nil, WithHint returns nil.
func WithHint(err error, hint string) error {
	if err == nil || hint == "" {
		return err
	}

	if pgErr, ok := err.(*pgError); ok {
		pgErr.e.Hint = hint
		return pgErr
	}

	return &pgError{
		error: err,
		e: PGWireError{
			Message: err.Error(),
			Hint:    hint,
		},
	}
}

// makeSrcCtx creates a PGWireError_Source value with contextual information
// about the caller at the requested depth.
func makeSrcCtx(depth int) PGWireError_Source {
	f, l, fun := caller.Lookup(depth + 1)
	return PGWireError_Source{File: f, Line: int32(l), Function: fun}
}

// WithSourceContext annotates err with contextual information about the
// caller at the requested depth.
// If err is nil, WithSourceContext returns nil.
func WithSourceContext(err error, depth int) error {
	if err == nil {
		return nil
	}

	srcCtx := makeSrcCtx(depth + 1)

	if pgErr, ok := err.(*pgError); ok {
		pgErr.e.Source = &srcCtx
		return pgErr
	}

	return &pgError{
		error: err,
		e: PGWireError{
			Message: err.Error(),
			Source:  &srcCtx,
		},
	}
}

// PGCode returns the Postgres error code of the error, if possible.
//
// If a code was never annotated onto the error, no code will be
// returned and a "found" flag will be returned as false.
func PGCode(err error) (string, bool) {
	if pgErr := unwrapToPGError(err); pgErr != nil {
		code := pgErr.e.Code
		return code, code != ""
	}
	return "", false
}

// Detail returns the detail message of the error, if possible.
//
// If a detail was never annotated onto the error, no detail will
// be returned and a "found" flag will be returned as false.
func Detail(err error) (string, bool) {
	if pgErr := unwrapToPGError(err); pgErr != nil {
		detail := pgErr.e.Detail
		return detail, detail != ""
	}
	return "", false
}

// Hint returns the hint message of the error, if possible.
//
// If a hint was never annotated onto the error, no hint will be
// returned and a "found" flag will be returned as false.
func Hint(err error) (string, bool) {
	if pgErr := unwrapToPGError(err); pgErr != nil {
		hint := pgErr.e.Hint
		return hint, hint != ""
	}
	return "", false
}

// SourceContext returns contextual information about the source of
// an error, if possible.
//
// If contextual information was never annotated onto the error, no
// SrcCtx will be returned and a "found" flag will be returned as false.
func SourceContext(err error) (PGWireError_Source, bool) {
	if pgErr := unwrapToPGError(err); pgErr != nil {
		source := pgErr.e.Source
		if source != nil {
			return *source, true
		}
	}
	return PGWireError_Source{}, false
}

// unwrapToPGError returns an unwrapped pgError annotation, if possible.
// The method behaves similarly to errors.Cause, but will only unwrap (call
// Cause on) non-pgError implementations. This allows us to strip errors
// added by errors.Wrap, and find the next pgError.
func unwrapToPGError(err error) *pgError {
	for err != nil {
		if pgErr, ok := err.(*pgError); ok {
			return pgErr
		}
		cause, ok := err.(causer)
		if !ok {
			break
		}
		err = cause.Cause()
	}
	return nil
}

func formatPGError(pgErr *pgError, s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v\n%s", pgErr.Cause(), pgErr.verboseFormat())
			return
		}
		fallthrough
	case 's', 'q':
		if _, err := io.WriteString(s, pgErr.Error()); err != nil {
			panic(err)
		}
	}
}

func (pg *PGWireError) message() string {
	return pg.Message
}
