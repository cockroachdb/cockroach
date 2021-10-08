// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import "github.com/cockroachdb/errors"

// Volatility indicates whether the result of a function is dependent *only*
// on the values of its explicit arguments, or can change due to outside factors
// (such as parameter variables or table contents).
//
// The values are ordered with smaller values being strictly more restrictive
// than larger values.
//
// NOTE: functions having side-effects, such as setval(),
// must be labeled volatile to ensure they will not get optimized away,
// even if the actual return value is not changeable.
type Volatility int8

const (
	// VolatilityLeakProof means that the operator cannot modify the database, the
	// transaction state, or any other state. It cannot depend on configuration
	// settings and is guaranteed to return the same results given the same
	// arguments in any context. In addition, no information about the arguments
	// is conveyed except via the return value. Any function that might throw an
	// error depending on the values of its arguments is not leak-proof.
	//
	// USE THIS WITH CAUTION! The optimizer might call operators that are leak
	// proof on inputs that they wouldn't normally be called on (e.g. pulling
	// expressions out of a CASE). In the future, they may even run on rows that
	// the user doesn't have permission to access.
	//
	// Note: VolatilityLeakProof is strictly stronger than VolatilityImmutable. In
	// principle it could be possible to have leak-proof stable or volatile
	// functions (perhaps now()); but this is not useful in practice as very few
	// operators are marked leak-proof.
	// Examples: integer comparison.
	VolatilityLeakProof Volatility = 1 + iota
	// VolatilityImmutable means that the operator cannot modify the database, the
	// transaction state, or any other state. It cannot depend on configuration
	// settings and is guaranteed to return the same results given the same
	// arguments in any context. ImmutableCopy operators can be constant folded.
	// Examples: log, from_json.
	VolatilityImmutable
	// VolatilityStable means that the operator cannot modify the database or the
	// transaction state and is guaranteed to return the same results given the
	// same arguments whenever it is evaluated within the same statement. Multiple
	// calls to a stable operator can be optimized to a single call.
	// Examples: current_timestamp, current_date.
	VolatilityStable
	// VolatilityVolatile means that the operator can do anything, including
	// modifying database state.
	// Examples: random, crdb_internal.force_error, nextval.
	VolatilityVolatile
)

// String returns the byte representation of Volatility as a string.
func (v Volatility) String() string {
	switch v {
	case VolatilityLeakProof:
		return "leak-proof"
	case VolatilityImmutable:
		return "immutable"
	case VolatilityStable:
		return "stable"
	case VolatilityVolatile:
		return "volatile"
	default:
		return "invalid"
	}
}

// ToPostgres returns the postgres "provolatile" string ("i" or "s" or "v") and
// the "proleakproof" flag.
func (v Volatility) ToPostgres() (provolatile string, proleakproof bool) {
	switch v {
	case VolatilityLeakProof:
		return "i", true
	case VolatilityImmutable:
		return "i", false
	case VolatilityStable:
		return "s", false
	case VolatilityVolatile:
		return "v", false
	default:
		panic(errors.AssertionFailedf("invalid volatility %s", v))
	}
}

// VolatilityFromPostgres returns a Volatility that matches the postgres
// provolatile/proleakproof settings.
func VolatilityFromPostgres(provolatile string, proleakproof bool) (Volatility, error) {
	switch provolatile {
	case "i":
		if proleakproof {
			return VolatilityLeakProof, nil
		}
		return VolatilityImmutable, nil
	case "s":
		return VolatilityStable, nil
	case "v":
		return VolatilityVolatile, nil
	default:
		return 0, errors.AssertionFailedf("invalid provolatile %s", provolatile)
	}
}
