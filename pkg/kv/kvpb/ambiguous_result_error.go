// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvpb

import (
	"context"
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

func init() {
	// Register the migration of the error that used to be in the roachpb
	// package and is now in the kv/kvpb package.
	roachpbPath := reflect.TypeOf(roachpb.Key("")).PkgPath()
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.AmbiguousResultError", &AmbiguousResultError{})
	// Note that it is important that these wrapper methods are registered
	// _after_ the type migration above.
	aErr := (*AmbiguousResultError)(nil)
	typeKey := errors.GetTypeKey(aErr)
	errors.RegisterWrapperEncoder(typeKey, func(ctx context.Context, err error) (msgPrefix string, safeDetails []string, payload proto.Message) {
		errors.As(err, &payload)
		return "", nil, payload
	})
	errors.RegisterWrapperDecoder(typeKey, func(ctx context.Context, cause error, msgPrefix string, safeDetails []string, payload proto.Message) error {
		return payload.(*AmbiguousResultError)
	})
}

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
