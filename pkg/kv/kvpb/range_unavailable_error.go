// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvpb

import (
	context "context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// NewRangeUnavailableError initializes a new *RangeUnavailableError. It is
// provided with the relevant range descriptor.
func NewRangeUnavailableError(cause error, desc *roachpb.RangeDescriptor) error {
	return &RangeUnavailableError{
		Desc:  *desc,
		Cause: errors.EncodeError(context.Background(), cause),
	}
}

var _ errors.SafeFormatter = (*RangeUnavailableError)(nil)
var _ fmt.Formatter = (*RangeUnavailableError)(nil)
var _ errors.Wrapper = (*RangeUnavailableError)(nil)

// SafeFormatError implements errors.SafeFormatter.
func (e *RangeUnavailableError) SafeFormatError(p errors.Printer) error {
	p.Printf("range unavailable: unable to serve request to %s: %s", e.Desc, e.Unwrap())
	return nil
}

// Format implements fmt.Formatter.
func (e *RangeUnavailableError) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

// Error implements error.
func (e *RangeUnavailableError) Error() string {
	return fmt.Sprint(e)
}

// Unwrap implements errors.Wrapper.
func (e *RangeUnavailableError) Unwrap() error {
	return errors.DecodeError(context.Background(), e.Cause)
}

func init() {
	encode := func(ctx context.Context, err error) (msgPrefix string, safeDetails []string, payload proto.Message) {
		errors.As(err, &payload) // payload = err.(proto.Message)
		return "", nil, payload
	}
	decode := func(ctx context.Context, cause error, msgPrefix string, safeDetails []string, payload proto.Message) error {
		return payload.(*RangeUnavailableError)
	}
	typeName := errors.GetTypeKey((*RangeUnavailableError)(nil))
	errors.RegisterWrapperEncoder(typeName, encode)
	errors.RegisterWrapperDecoder(typeName, decode)
}
