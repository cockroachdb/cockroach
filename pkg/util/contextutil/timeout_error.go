// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package contextutil

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errorspb"
	"github.com/gogo/protobuf/proto"
)

// TimeoutError is a wrapped ContextDeadlineExceeded error. It indicates that
// an operation didn't complete within its designated timeout.
type TimeoutError struct {
	// The operation that timed out.
	operation string
	// The configured timeout.
	timeout time.Duration
	// The duration of the operation. This is usually expected to be the same as
	// the timeout, but can be longer if the timeout was not observed expediently
	// (because the ctx was not checked sufficiently often).
	took  time.Duration
	cause error
}

var _ error = (*TimeoutError)(nil)
var _ fmt.Formatter = (*TimeoutError)(nil)
var _ errors.Formatter = (*TimeoutError)(nil)

// We implement net.Error the same way that context.DeadlineExceeded does, so
// that people looking for net.Error attributes will still find them.
var _ net.Error = (*TimeoutError)(nil)

// Operation returns the name of the operation that timed out.
func (t *TimeoutError) Operation() string {
	return t.operation
}

func (t *TimeoutError) Error() string { return fmt.Sprintf("%v", t) }

// Format implements fmt.Formatter.
func (t *TimeoutError) Format(s fmt.State, verb rune) { errors.FormatError(t, s, verb) }

// FormatError implements errors.Formatter.
func (t *TimeoutError) FormatError(p errors.Printer) error {
	// NB: With RunWithTimeout(), it is possible for both the caller and the
	// callee to have set their own context timeout that is smaller than the
	// timeout set by RunWithTimeout. It is also possible for the operation to run
	// for much longer than the timeout, e.g. if the callee does not check the
	// context in a timely manner. The error message must make this clear.
	p.Printf("operation %q timed out", t.operation)
	if t.took != 0 {
		p.Printf(" after %s", t.took.Round(time.Millisecond))
	}
	p.Printf(" (given timeout %s)", t.timeout)
	return t.cause
}

// Timeout implements net.Error.
func (*TimeoutError) Timeout() bool { return true }

// Temporary implements net.Error.
func (*TimeoutError) Temporary() bool { return true }

// Cause implements Causer.
func (t *TimeoutError) Cause() error {
	return t.cause
}

// encodeTimeoutError serializes a TimeoutError.
// We cannot include the operation in the safe strings because
// we currently have plenty of uses where the operation is constructed
// from unsafe/sensitive data.
func encodeTimeoutError(
	_ context.Context, err error,
) (msgPrefix string, safe []string, details proto.Message) {
	t := err.(*TimeoutError)
	details = &errorspb.StringsPayload{
		Details: []string{t.operation, t.timeout.String(), t.took.String()},
	}
	msgPrefix = fmt.Sprintf("operation %q timed out after %s", t.operation, t.timeout)
	return msgPrefix, nil, details
}

func decodeTimeoutError(
	ctx context.Context, cause error, msgPrefix string, safeDetails []string, payload proto.Message,
) error {
	m, ok := payload.(*errorspb.StringsPayload)
	if !ok || len(m.Details) < 2 {
		// If this ever happens, this means some version of the library
		// (presumably future) changed the payload type, and we're
		// receiving this here. In this case, give up and let
		// DecodeError use the opaque type.
		return nil
	}
	op := m.Details[0]
	timeout, decodeErr := time.ParseDuration(m.Details[1])
	if decodeErr != nil {
		// Not encoded by our encode function. Bail out.
		return nil //nolint:returnerrcheck
	}
	var took time.Duration
	if len(m.Details) >= 3 {
		took, decodeErr = time.ParseDuration(m.Details[2])
		if decodeErr != nil {
			// Not encoded by our encode function. Bail out.
			return nil //nolint:returnerrcheck
		}
	}
	return &TimeoutError{
		operation: op,
		timeout:   timeout,
		took:      took,
		cause:     cause,
	}
}

func init() {
	pKey := errors.GetTypeKey(&TimeoutError{})
	errors.RegisterWrapperEncoder(pKey, encodeTimeoutError)
	errors.RegisterWrapperDecoder(pKey, decodeTimeoutError)
}
