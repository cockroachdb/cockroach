// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package poison

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// NewPoisonedError instantiates a *PoisonedError referencing a poisoned latch
// (as identified by span and timestamp).
func NewPoisonedError(span roachpb.Span, ts hlc.Timestamp) *PoisonedError {
	return &PoisonedError{Span: span, Timestamp: ts}
}

var _ errors.SafeFormatter = (*PoisonedError)(nil)
var _ fmt.Formatter = (*PoisonedError)(nil)

// SafeFormatError implements errors.SafeFormatter.
func (e *PoisonedError) SafeFormatError(p errors.Printer) error {
	p.Printf("encountered poisoned latch %s@%s", e.Span, e.Timestamp)
	return nil
}

// Format implements fmt.Formatter.
func (e *PoisonedError) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

// Error implements error.
func (e *PoisonedError) Error() string {
	return fmt.Sprint(e)
}
