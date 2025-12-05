package scheduler

import "github.com/cockroachdb/cockroach/pkg/util/hlc"

type Transaction struct {
	CommitTime hlc.Timestamp
	// NOTE: there is a hazard when constructing the Locks slice, we may end up
	// with duplicates. The scheduler should tolerate duplicates.
	Locks []Lock
}

type Lock struct {
	Hash   LockHash
	IsRead bool
}
