// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwirebase

import (
	"strconv"

	"github.com/cockroachdb/errors"
)

// withMessageTooBig decorates an error when a read would overflow the ReadBuffer.
type withMessageTooBig struct {
	cause error
	size  int
}

var _ error = (*withMessageTooBig)(nil)
var _ errors.SafeDetailer = (*withMessageTooBig)(nil)

func (w *withMessageTooBig) Error() string         { return w.cause.Error() }
func (w *withMessageTooBig) Unwrap() error         { return w.cause }
func (w *withMessageTooBig) SafeDetails() []string { return []string{strconv.Itoa(w.size)} }

// withMessageTooBigError decorates the error with a severity.
func withMessageTooBigError(err error, size int) error {
	if err == nil {
		return nil
	}

	return &withMessageTooBig{cause: err, size: size}
}

// IsMessageTooBigError denotes whether a message is too big.
func IsMessageTooBigError(err error) bool {
	var c withMessageTooBig
	return errors.HasType(err, &c)
}

// GetMessageTooBigSize attempts to unwrap and find a MessageTooBig.
func GetMessageTooBigSize(err error) int {
	if c := (*withMessageTooBig)(nil); errors.As(err, &c) {
		return c.size
	}
	return -1
}
