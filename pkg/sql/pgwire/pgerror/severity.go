// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgerror

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// DefaultSeverity is the default severity for decoding severity of errors.
const DefaultSeverity = "ERROR"

// WithSeverity decorates the error with a severity.
func WithSeverity(err error, severity string) error {
	if err == nil {
		return nil
	}

	return &withSeverity{cause: err, severity: severity}
}

// GetSeverity attempts to unwrap and find a Severity.
func GetSeverity(err error) string {
	if c := (*withSeverity)(nil); errors.As(err, &c) {
		return c.severity
	}
	return DefaultSeverity
}

// withSeverity decorates an error with a given severity.
type withSeverity struct {
	cause    error
	severity string
}

var _ error = (*withSeverity)(nil)
var _ errors.SafeDetailer = (*withSeverity)(nil)
var _ fmt.Formatter = (*withSeverity)(nil)
var _ errors.Formatter = (*withSeverity)(nil)

func (w *withSeverity) Error() string         { return w.cause.Error() }
func (w *withSeverity) Cause() error          { return w.cause }
func (w *withSeverity) Unwrap() error         { return w.cause }
func (w *withSeverity) SafeDetails() []string { return []string{w.severity} }

func (w *withSeverity) Format(s fmt.State, verb rune) { errors.FormatError(w, s, verb) }

func (w *withSeverity) FormatError(p errors.Printer) (next error) {
	if p.Detail() {
		p.Printf("severity: %s", w.severity)
	}
	return w.cause
}

// decodeWithSeverity is a custom decoder that will be used when decoding
// withSeverity error objects.
// Note that as the last argument it takes proto.Message (and not
// protoutil.Message which is required by linter) because the latter brings in
// additional dependencies into this package and the former is sufficient here.
func decodeWithSeverity(
	_ context.Context, cause error, _ string, details []string, _ proto.Message,
) error {
	severity := DefaultSeverity
	if len(details) > 0 {
		severity = details[0]
	}
	return &withSeverity{cause: cause, severity: severity}
}

func init() {
	errors.RegisterWrapperDecoder(errors.GetTypeKey((*withSeverity)(nil)), decodeWithSeverity)
}
