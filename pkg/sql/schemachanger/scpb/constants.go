// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scpb

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const (
	// PlaceHolderRoleName placeholder string for non-fetched role names.
	PlaceHolderRoleName string = "__placeholder_role_name__"
)

// TargetStatus is the subset of Status values which serve as valid target
// statuses.
type TargetStatus Status

var _ redact.SafeValue = Status(0)
var _ redact.SafeValue = TargetStatus(0)

// SafeValue implements the redact.SafeValue interface.
func (t TargetStatus) SafeValue() {}

// SafeValue implements the redact.SafeValue interface.
func (s Status) SafeValue() {}

const (
	// InvalidTarget indicates that the element doesn't have a target status
	// for the current schema change.
	InvalidTarget TargetStatus = TargetStatus(Status_UNKNOWN)

	// ToAbsent indicates that the element should have status ABSENT after
	// completion of the current schema change, meaning it should no longer exist.
	ToAbsent TargetStatus = TargetStatus(Status_ABSENT)

	// ToPublic indicates that the element should have status PUBLIC after
	// completion of the current schema change, meaning it should exist and not
	// in any kind of transitory state.
	ToPublic TargetStatus = TargetStatus(Status_PUBLIC)

	// Transient is like ToAbsent in that the element should no longer exist once
	// the schema change is done. The element is also not assumed to exist prior
	// to the schema change, so this target status is used to ensure it comes into
	// existence before disappearing again. Otherwise, an element whose current
	// and target statuses are both ABSENT won't experience any state transitions.
	Transient TargetStatus = TargetStatus(Status_TRANSIENT_ABSENT)
)

// Status returns the TargetStatus as a Status.
func (t TargetStatus) String() string {
	return Status(t).String()
}

// Status returns the TargetStatus as a Status.
func (t TargetStatus) Status() Status {
	return Status(t)
}

// InitialStatus returns the initial status for this TargetStatus.
func (t TargetStatus) InitialStatus() Status {
	switch t {
	case ToAbsent:
		return Status_PUBLIC
	case ToPublic, Transient:
		return Status_ABSENT
	default:
		panic(errors.AssertionFailedf("unknown target status %v", t.Status()))
	}
}

// AsTargetStatus returns the Status as a TargetStatus.
func AsTargetStatus(s Status) TargetStatus {
	switch s {
	case Status_ABSENT:
		return ToAbsent
	case Status_PUBLIC:
		return ToPublic
	case Status_TRANSIENT_ABSENT:
		return Transient
	default:
		return InvalidTarget
	}
}
