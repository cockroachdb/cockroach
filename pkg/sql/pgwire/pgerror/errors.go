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
// fields. Implementations of the interface annotate these fields onto
// standard errors by wrapping them. The fields can then be retrieved from
// the errors using the functions like Hint and Detail below.
//
// See https://www.postgresql.org/docs/current/static/protocol-error-fields.html
// for a list of all Postgres error fields, most of which are optional and can
// be used to provide auxiliary error information.
//
// The style for the implementation is inspired by github.com/pkg/errors,
// and all pgError implementations are made to play nicely with that package.
// For instance, a pgError can be wrapped by errors.Wrap and its annotations
// can still be retrieved. pgErrors also implement the errors.causer interface,
// meaning that errors.Cause will work correctly with them.
type pgError interface {
	error
	causer

	// structured Postgres error fields. Each method attempts to return the
	// optional field and a boolean signifying if the optional field was
	// present.
	PGCode() (string, bool)
	Detail() (string, bool)
	Hint() (string, bool)
	SourceContext() (SrcCtx, bool)

	// formatting methods.
	fmt.Formatter
	verboseFormat() string
}

// from github.com/pkg/errors package.
type causer interface {
	Cause() error
}

var _ pgError = &withPGCode{}
var _ pgError = &withDetail{}
var _ pgError = &withHint{}
var _ pgError = &withSrcCtx{}

// pgErrorCause is embedded by each pgError implementation to eliminate
// method duplication.
type pgErrorCause struct {
	error
}

func (ec pgErrorCause) Cause() error                  { return ec.error }
func (ec pgErrorCause) PGCode() (string, bool)        { return PGCode(ec.error) }
func (ec pgErrorCause) Detail() (string, bool)        { return Detail(ec.error) }
func (ec pgErrorCause) Hint() (string, bool)          { return Hint(ec.error) }
func (ec pgErrorCause) SourceContext() (SrcCtx, bool) { return SourceContext(ec.error) }

// WithPGCode annotates err with a Postgres error code.
// If err is nil, WithPGCode returns nil.
func WithPGCode(err error, code string) error {
	if err == nil || code == "" {
		return err
	}
	return &withPGCode{
		pgErrorCause: pgErrorCause{err},
		code:         code,
	}
}

type withPGCode struct {
	pgErrorCause
	code string
}

func (w *withPGCode) PGCode() (string, bool)        { return w.code, true }
func (w *withPGCode) verboseFormat() string         { return "code: " + w.code }
func (w *withPGCode) Format(s fmt.State, verb rune) { formatPGError(w, s, verb) }

// WithDetail annotates err with a detail message.
// If err is nil, WithDetail returns nil.
func WithDetail(err error, detail string) error {
	if err == nil || detail == "" {
		return err
	}
	return &withDetail{
		pgErrorCause: pgErrorCause{err},
		detail:       detail,
	}
}

type withDetail struct {
	pgErrorCause
	detail string
}

func (w *withDetail) Detail() (string, bool)        { return w.detail, true }
func (w *withDetail) verboseFormat() string         { return "detail: " + w.detail }
func (w *withDetail) Format(s fmt.State, verb rune) { formatPGError(w, s, verb) }

// WithHint annotates err with a hint for users to resolve the
// issue in the future.
// If err is nil, WithHint returns nil.
func WithHint(err error, hint string) error {
	if err == nil || hint == "" {
		return err
	}
	return &withHint{
		pgErrorCause: pgErrorCause{err},
		hint:         hint,
	}
}

type withHint struct {
	pgErrorCause
	hint string
}

func (w *withHint) Hint() (string, bool)          { return w.hint, true }
func (w *withHint) verboseFormat() string         { return "hint: " + w.hint }
func (w *withHint) Format(s fmt.State, verb rune) { formatPGError(w, s, verb) }

// SrcCtx contains contextual information about the source of an error.
type SrcCtx struct {
	File     string
	Line     int
	Function string
}

// makeSrcCtx creates a SrcCtx value with contextual information about the
// caller at the requested depth.
func makeSrcCtx(depth int) SrcCtx {
	f, l, fun := caller.Lookup(depth + 1)
	return SrcCtx{File: f, Line: l, Function: fun}
}

// WithSourceContext annotates err with contextual information about the
// caller at the requested depth.
// If err is nil, WithSourceContext returns nil.
func WithSourceContext(err error, depth int) error {
	if err == nil {
		return nil
	}
	return &withSrcCtx{
		pgErrorCause: pgErrorCause{err},
		ctx:          makeSrcCtx(depth + 1),
	}
}

type withSrcCtx struct {
	pgErrorCause
	ctx SrcCtx
}

func (w *withSrcCtx) SourceContext() (SrcCtx, bool) { return w.ctx, true }
func (w *withSrcCtx) verboseFormat() string {
	return fmt.Sprintf("location: %s, %s:%d", w.ctx.Function, w.ctx.File, w.ctx.Line)
}
func (w *withSrcCtx) Format(s fmt.State, verb rune) { formatPGError(w, s, verb) }

// PGCode returns the Postgres error code of the error, if possible.
//
// If a code was never annotated onto the error, no code will be
// returned and a "found" flag will be returned as false.
func PGCode(err error) (string, bool) {
	if pgErr := unwrapToPGError(err); pgErr != nil {
		return pgErr.PGCode()
	}
	return "", false
}

// Detail returns the detail message of the error, if possible.
//
// If a detail was never annotated onto the error, no detail will
// be returned and a "found" flag will be returned as false.
func Detail(err error) (string, bool) {
	if pgErr := unwrapToPGError(err); pgErr != nil {
		return pgErr.Detail()
	}
	return "", false
}

// Hint returns the hint message of the error, if possible.
//
// If a hint was never annotated onto the error, no hint will be
// returned and a "found" flag will be returned as false.
func Hint(err error) (string, bool) {
	if pgErr := unwrapToPGError(err); pgErr != nil {
		return pgErr.Hint()
	}
	return "", false
}

// SourceContext returns contextual information about the source of
// an error, if possible.
//
// If contextual information was never annotated onto the error, no
// SrcCtx will be returned and a "found" flag will be returned as false.
func SourceContext(err error) (SrcCtx, bool) {
	if pgErr := unwrapToPGError(err); pgErr != nil {
		return pgErr.SourceContext()
	}
	return SrcCtx{}, false
}

// unwrapToPGError returns an unwrapped pgError annotation, if possible.
// The method behaves similarly to errors.Cause, but will only unwrap (call
// Cause on) non-pgError implementations. This allows us to strip errors
// added by errors.Wrap, and find the next pgError.
func unwrapToPGError(err error) pgError {
	for err != nil {
		if pgErr, ok := err.(pgError); ok {
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

func formatPGError(pgErr pgError, s fmt.State, verb rune) {
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
