// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opt

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// Locking represents the row-level locking properties of a relational operator.
// Each relational operator clause consist of two different row-level locking
// properties.
type Locking struct {
	// The first property is locking strength (see tree.LockingStrength). Locking
	// strength represents the degree of protection that a row-level lock provides.
	// The stronger the lock, the more protection it provides for the lock holder
	// but the more restrictive it is to concurrent transactions attempting to
	// access the same row. In order from weakest to strongest, the lock strength
	// variants are:
	//
	//   FOR KEY SHARE
	//   FOR SHARE
	//   FOR NO KEY UPDATE
	//   FOR UPDATE
	//
	Strength tree.LockingStrength

	// The second property is the locking wait policy (see tree.LockingWaitPolicy).
	// A locking wait policy represents the policy a table scan uses to interact
	// with row-level locks held by other transactions. Unlike locking strength,
	// locking wait policy is optional to specify in a locking clause. If not
	// specified, the policy defaults to blocking and waiting for locks to become
	// available. The non-standard policies instruct scans to take other approaches
	// to handling locks held by other transactions. These non-standard policies
	// are:
	//
	//   SKIP LOCKED
	//   NOWAIT
	//
	WaitPolicy tree.LockingWaitPolicy
}

// IsLocking returns whether the receiver is configured to use a row-level
// locking mode.
func (l Locking) IsLocking() bool {
	return l.Strength != tree.ForNone
}
