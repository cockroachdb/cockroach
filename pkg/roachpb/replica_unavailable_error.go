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
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// NewReplicaUnavailableError initializes a new *ReplicaUnavailableError. It is
// provided with the range descriptor known to the replica, and the relevant
// replica descriptor within.
func NewReplicaUnavailableError(
	desc *RangeDescriptor, replDesc ReplicaDescriptor,
) *ReplicaUnavailableError {
	return &ReplicaUnavailableError{
		Desc:    *desc,
		Replica: replDesc,
	}
}

var _ errors.SafeFormatter = (*ReplicaUnavailableError)(nil)
var _ fmt.Formatter = (*ReplicaUnavailableError)(nil)

// SafeFormatError implements errors.SafeFormatter.
func (e *ReplicaUnavailableError) SafeFormatError(p errors.Printer) error {
	p.Printf("replica %s unable to serve request to %s", e.Replica, e.Desc)
	return nil
}

// Format implements fmt.Formatter.
func (e *ReplicaUnavailableError) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

// Error implements error.
func (e *ReplicaUnavailableError) Error() string {
	return `
How do I express this in terms of SafeFormatError without causing infinite recursion?
Redact seems to call Error to get the error mark, so isn't this impossible?
It also doesn't seem like the output of Error is ever printed neither through fmt nor redact,
so it's almost as though I could just return anything here, but of course that's not correct
either because someone could manually invoke Error().
`
}

func (e *ReplicaUnavailableError) String() string {
	return redact.Sprint(e).StripMarkers()
}
