// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scpb

// GetTransientEquivalent returns the equivalent TRANSIENT_ Status for a given
// status. If false is returned, there is no equivalent for the provided
// Status.
func GetTransientEquivalent(s Status) (Status, bool) {
	equiv, ok := transientEquivalent[s]
	return equiv, ok
}

var transientEquivalent = map[Status]Status{
	Status_DELETE_ONLY: Status_TRANSIENT_DELETE_ONLY,
	Status_WRITE_ONLY:  Status_TRANSIENT_WRITE_ONLY,
	Status_ABSENT:      Status_TRANSIENT_ABSENT,
}
