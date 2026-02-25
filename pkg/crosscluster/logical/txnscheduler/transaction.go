// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnscheduler

import "github.com/cockroachdb/cockroach/pkg/util/hlc"

// Transaction contains all of the information about a transaction needed to
// identify its dependencies.
type Transaction struct {
	CommitTime hlc.Timestamp
	Locks      []Lock
}

// TODO(jeffswenson): replace with txnlock.LockHash once it merges.
type LockHash uint64

// TODO(jeffswenson): replace with txnlock.Lock once it merges.
type Lock struct {
	Hash   LockHash
	IsRead bool
}
