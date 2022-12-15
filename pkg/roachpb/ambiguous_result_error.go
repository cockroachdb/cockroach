// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// NewAmbiguousResultErrorf initializes a new AmbiguousResultError with
// an explanatory format and set of arguments.
func NewAmbiguousResultErrorf(format string, args ...interface{}) *AmbiguousResultError {
	return NewAmbiguousResultError(errors.NewWithDepthf(1, format, args...))
}

// NewAmbiguousResultError returns an AmbiguousResultError wrapping (via
// errors.Wrapper) the supplied error.
func NewAmbiguousResultError(err error) *AmbiguousResultError {
	return &AmbiguousResultError{
		EncodedError: errors.EncodeError(context.Background(), err),
	}
}

var _ errors.SafeFormatter = (*AmbiguousResultError)(nil)
var _ fmt.Formatter = (*AmbiguousResultError)(nil)
var _ = func() errors.Wrapper {
	aErr := (*AmbiguousResultError)(nil)
	typeKey := errors.GetTypeKey(aErr)
	errors.RegisterWrapperEncoder(typeKey, func(ctx context.Context, err error) (msgPrefix string, safeDetails []string, payload proto.Message) {
		errors.As(err, &payload)
		return "", nil, payload
	})
	errors.RegisterWrapperDecoder(typeKey, func(ctx context.Context, cause error, msgPrefix string, safeDetails []string, payload proto.Message) error {
		return payload.(*AmbiguousResultError)
	})

	return aErr
}()

// SafeFormatError implements errors.SafeFormatter.
func (e *AmbiguousResultError) SafeFormatError(p errors.Printer) error {
	p.Printf("result is ambiguous: %s", e.unwrapOrDefault())
	return nil
}

// Format implements fmt.Formatter.
func (e *AmbiguousResultError) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

// Error implements error.
func (e *AmbiguousResultError) Error() string {
	return fmt.Sprint(e)
}

// Unwrap implements errors.Wrapper.
func (e *AmbiguousResultError) Unwrap() error {
	if e.EncodedError.Error == nil {
		return nil
	}
	return errors.DecodeError(context.Background(), e.EncodedError)
}

func (e *AmbiguousResultError) unwrapOrDefault() error {
	cause := e.Unwrap()
	if cause == nil {
		return errors.New("unknown cause") // can be removed in 22.2
	}
	return cause
}

// Type is part of the ErrorDetailInterface.
func (e *AmbiguousResultError) Type() ErrorDetailType {
	return AmbiguousResultErrType
}
