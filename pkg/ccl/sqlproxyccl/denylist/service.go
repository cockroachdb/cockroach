// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

//go:generate mockgen -package=denylist -destination=mocks.go -source=service.go . Service
package denylist

// Entry records the reason for putting an item on the denylist.
// TODO(spaskob): add codes for different denial reasons.
type Entry struct {
	Reason string
}

// Service provides an interface for checking if an id has been denied access.
type Service interface {
	// Denied returns a non-nil Entry if the id is denied. The reason for the
	// denial will be in Entry.
	Denied(id string) (*Entry, error)

	// TODO(spaskob): add API for registering listeners to be notified of any
	// updates (inclusion/exclusion) to the denylist.
}
