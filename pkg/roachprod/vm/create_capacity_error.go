// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vm

import "fmt"

// CreateCapacityClass identifies the create input dimension constrained by
// provider capacity.
type CreateCapacityClass string

const (
	// CreateCapacityClassZone indicates that the provider could not satisfy the
	// request in one or more zones.
	CreateCapacityClassZone CreateCapacityClass = "zone"
)

// CreateCapacityError is returned by VM providers when cluster creation fails
// due to provider capacity in one or more zones. Providers may include
// SuggestedZones when the cloud provider reports zones that can satisfy the
// request.
type CreateCapacityError struct {
	CapacityClass  CreateCapacityClass
	Provider       string
	MachineType    string
	FailedZones    []string
	SuggestedZones []string
	Cause          error
}

func (e *CreateCapacityError) Error() string {
	msg := fmt.Sprintf("%s create failed due to capacity", e.Provider)
	if e.MachineType != "" {
		msg += fmt.Sprintf(" for machine type %s", e.MachineType)
	}
	if len(e.FailedZones) > 0 {
		msg += fmt.Sprintf(" in zone(s) %v", e.FailedZones)
	}
	if len(e.SuggestedZones) > 0 {
		msg += fmt.Sprintf("; suggested zone(s): %v", e.SuggestedZones)
	}
	if e.Cause != nil {
		msg += fmt.Sprintf(": %v", e.Cause)
	}
	return msg
}

// Unwrap returns the original provider error.
func (e *CreateCapacityError) Unwrap() error {
	return e.Cause
}
