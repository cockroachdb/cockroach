// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opt

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// Locking represents the row-level locking properties of a relational operator.
// Each relational operator clause consists of four different row-level locking
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
	// Note that SKIP LOCKED can be requested without a locking strength, which
	// signifies skipping over locks without taking any additional locks.
	WaitPolicy tree.LockingWaitPolicy

	// The third property is the form of locking, either record locking or
	// predicate locking (see tree.LockingForm). Record locking prevents
	// modification of existing rows, but does not prevent insertion of new
	// rows. Predicate locking prevents both modification of existing rows and
	// insertion of new rows. Unlike locking strength, locking form is optional to
	// specify in a locking clause. If not specified, the form defaults to record
	// locking. We currently only use predicate locking for uniqueness checks
	// under snapshot and read committed isolation, and only support predicate
	// locking on single-key spans.
	Form tree.LockingForm

	// The fourth property is the durability of the locking (see
	// tree.LockingDurability). A guaranteed-durable lock always persists until
	// commit time, while a best-effort lock may sometimes be lost before commit
	// (for example, during a lease transfer). Unlike locking strength, locking
	// durability is optional to specify in a locking clause. If not specified,
	// the durability defaults to best-effort. We currently only require
	// guaranteed-durable locks for SELECT FOR UPDATE statements and
	// system-maintained constraint checks (e.g. FK checks) under snapshot and
	// read commited isolation. Other locking statements, such as UPDATE, rely on
	// the durability of intents for correctness, rather than the durability of
	// locks.
	Durability tree.LockingDurability
}

// Max returns a new set of locking properties where each property is the max of
// the respective property in the two inputs.
func (l Locking) Max(l2 Locking) Locking {
	return Locking{
		Strength:   l.Strength.Max(l2.Strength),
		WaitPolicy: l.WaitPolicy.Max(l2.WaitPolicy),
		Form:       l.Form.Max(l2.Form),
		Durability: l.Durability.Max(l2.Durability),
	}
}

// IsLocking returns whether the receiver is configured to use row-level
// locking.
func (l Locking) IsLocking() bool {
	return l.Strength != tree.ForNone
}

// IsNoOp returns true if none of the locking properties are set. It differs
// from IsLocking in that it considers all of the locking properties, instead of
// only Strength. Currently, the only locking property that can be set when
// Strength=ForNone is WaitPolicy=LockWaitSkipLocked or
// WaitPolicy=LockWaitError. So we can say: IsNoOp returns false if IsLocking
// returns true OR the SKIP LOCKED wait policy is in effect OR the NOWAIT wait
// policy is in effect.
func (l Locking) IsNoOp() bool {
	return l == Locking{}
}
