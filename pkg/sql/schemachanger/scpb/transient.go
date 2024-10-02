// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scpb

// GetTransientEquivalent returns the equivalent TRANSIENT_ Status for a given
// status. If false is returned, there is no equivalent for the provided
// Status.
func GetTransientEquivalent(s Status) (Status, bool) {
	equiv, ok := transientEquivalent[s]
	return equiv, ok
}

// GetNonTransientEquivalent is the reciprocal of GetTransientEquivalent
// and therefore take a TRANSIENT_ Status as input.
func GetNonTransientEquivalent(s Status) (Status, bool) {
	for k, v := range transientEquivalent {
		if v == s {
			return k, true
		}
	}
	return Status_UNKNOWN, false
}

var transientEquivalent = map[Status]Status{
	Status_DELETE_ONLY:   Status_TRANSIENT_DELETE_ONLY,
	Status_WRITE_ONLY:    Status_TRANSIENT_WRITE_ONLY,
	Status_ABSENT:        Status_TRANSIENT_ABSENT,
	Status_PUBLIC:        Status_TRANSIENT_PUBLIC,
	Status_BACKFILL_ONLY: Status_TRANSIENT_BACKFILL_ONLY,
	Status_BACKFILLED:    Status_TRANSIENT_BACKFILLED,
	Status_MERGE_ONLY:    Status_TRANSIENT_MERGE_ONLY,
	Status_MERGED:        Status_TRANSIENT_MERGED,
	Status_VALIDATED:     Status_TRANSIENT_VALIDATED,
	Status_DROPPED:       Status_TRANSIENT_DROPPED,
}
