// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package volatility

import "github.com/cockroachdb/errors"

// V indicates whether the result of a function is dependent *only*
// on the values of its explicit arguments, or can change due to outside factors
// (such as parameter variables or table contents).
//
// The values are ordered with smaller values being strictly more restrictive
// than larger values.
//
// NOTE: functions having side-effects, such as setval(),
// must be labeled volatile to ensure they will not get optimized away,
// even if the actual return value is not changeable.
type V int8

const (
	// Leakproof means that the operator cannot modify the database, the
	// transaction state, or any other state. It cannot depend on configuration
	// settings and is guaranteed to return the same results given the same
	// arguments in any context. In addition, no information about the arguments
	// is conveyed except via the return value. Any function that might throw an
	// error depending on the values of its arguments is not leakproof.
	//
	// USE THIS WITH CAUTION! The optimizer might call operators that are leak
	// proof on inputs that they wouldn't normally be called on (e.g. pulling
	// expressions out of a CASE). In the future, they may even run on rows that
	// the user doesn't have permission to access.
	//
	// Note: Leakproof is strictly stronger than Immutable. In
	// principle it could be possible to have leakproof stable or volatile
	// functions (perhaps now()); but this is not useful in practice as very few
	// operators are marked leakproof.
	// Examples: integer comparison.
	Leakproof V = 1 + iota
	// Immutable means that the operator cannot modify the database, the
	// transaction state, or any other state. It cannot depend on configuration
	// settings and is guaranteed to return the same results given the same
	// arguments in any context. Immutable operators can be constant folded.
	// Examples: log, from_json.
	Immutable
	// Stable means that the operator cannot modify the database or the
	// transaction state and is guaranteed to return the same results given the
	// same arguments whenever it is evaluated within the same statement.
	// Multiple calls to a stable operator can be optimized to a single call.
	// Examples: current_timestamp, current_date.
	Stable
	// Volatile means that the operator can do anything, including
	// modifying database state.
	// Examples: random, crdb_internal.force_error, nextval.
	Volatile
)

// String returns the byte representation of Volatility as a string.
func (v V) String() string {
	switch v {
	case Leakproof:
		return "leakproof"
	case Immutable:
		return "immutable"
	case Stable:
		return "stable"
	case Volatile:
		return "volatile"
	default:
		return "invalid"
	}
}

// TitleString returns the byte representation of Volatility as a title-cased
// string.
func (v V) TitleString() string {
	switch v {
	case Leakproof:
		return "Leakproof"
	case Immutable:
		return "Immutable"
	case Stable:
		return "Stable"
	case Volatile:
		return "Volatile"
	default:
		return "Invalid"
	}
}

// ToPostgres returns the postgres "provolatile" string ("i" or "s" or "v") and
// the "proleakproof" flag.
func (v V) ToPostgres() (provolatile string, proleakproof bool) {
	switch v {
	case Leakproof:
		return "i", true
	case Immutable:
		return "i", false
	case Stable:
		return "s", false
	case Volatile:
		return "v", false
	default:
		panic(errors.AssertionFailedf("invalid volatility %s", v))
	}
}

// FromPostgres returns a Volatility that matches the postgres
// provolatile/proleakproof settings.
func FromPostgres(provolatile string, proleakproof bool) (V, error) {
	switch provolatile {
	case "i":
		if proleakproof {
			return Leakproof, nil
		}
		return Immutable, nil
	case "s":
		return Stable, nil
	case "v":
		return Volatile, nil
	default:
		return 0, errors.AssertionFailedf("invalid provolatile %s", provolatile)
	}
}
