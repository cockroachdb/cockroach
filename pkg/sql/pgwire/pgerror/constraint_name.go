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

// WithConstraintName decorates the error with a severity.
func WithConstraintName(err error, constraint string) error {
	if err == nil {
		return nil
	}

	return &withConstraintName{cause: err, constraint: constraint}
}

// GetConstraintName attempts to unwrap and find a Severity.
func GetConstraintName(err error) string {
	if c := (*withConstraintName)(nil); errors.As(err, &c) {
		return c.constraint
	}
	return ""
}

type withConstraintName struct {
	cause      error
	constraint string
}

var _ error = (*withConstraintName)(nil)
var _ errors.SafeDetailer = (*withConstraintName)(nil)
var _ fmt.Formatter = (*withConstraintName)(nil)
var _ errors.SafeFormatter = (*withConstraintName)(nil)

func (w *withConstraintName) Error() string { return w.cause.Error() }
func (w *withConstraintName) Cause() error  { return w.cause }
func (w *withConstraintName) Unwrap() error { return w.cause }
func (w *withConstraintName) SafeDetails() []string {
	// The constraint name is considered PII.
	return nil
}

func (w *withConstraintName) Format(s fmt.State, verb rune) { errors.FormatError(w, s, verb) }

func (w *withConstraintName) SafeFormatError(p errors.Printer) (next error) {
	if p.Detail() {
		p.Printf("constraint name: %s", w.constraint)
	}
	return w.cause
}

// decodeWithConstraintName is a custom decoder that will be used when decoding
// withConstraintName error objects.
// Note that as the last argument it takes proto.Message (and not
// protoutil.Message which is required by linter) because the latter brings in
// additional dependencies into this package and the former is sufficient here.
func decodeWithConstraintName(
	_ context.Context, cause error, _ string, details []string, _ proto.Message,
) error {
	var constraint string
	if len(details) > 0 {
		constraint = details[0]
	}
	return &withConstraintName{cause: cause, constraint: constraint}
}

func init() {
	errors.RegisterWrapperDecoder(
		errors.GetTypeKey((*withConstraintName)(nil)),
		decodeWithConstraintName,
	)
}
