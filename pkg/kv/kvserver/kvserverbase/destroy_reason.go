// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserverbase

import "fmt"

// DestroyReason indicates if a replica is alive, destroyed, corrupted or pending destruction.
type DestroyReason int

const (
	// The replica is alive.
	DestroyReasonAlive DestroyReason = iota
	// The replica has been GCed or is in the process of being synchronously
	// removed.
	DestroyReasonRemoved
	// The replica has been merged into its left-hand neighbor, but its left-hand
	// neighbor hasn't yet subsumed it.
	DestroyReasonMergePending
)

type DestroyStatus struct {
	Reason DestroyReason
	Err    error
}

func (s DestroyStatus) String() string {
	return fmt.Sprintf("{%v %d}", s.Err, s.Reason)
}

func (s *DestroyStatus) Set(err error, reason DestroyReason) {
	s.Err = err
	s.Reason = reason
}

// IsAlive returns true when a replica is alive.
func (s DestroyStatus) IsAlive() bool {
	return s.Reason == DestroyReasonAlive
}

// Removed returns whether the replica has been removed.
func (s DestroyStatus) Removed() bool {
	return s.Reason == DestroyReasonRemoved
}
