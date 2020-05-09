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

// Volatility indicates whether the result of a function is dependent *only*
// on the values of its explicit arguments, or can change due to outside factors
// (such as parameter variables or table contents).
//
// NOTE: functions having side-effects, such as setval(),
// must be labeled volatile to ensure they will not get optimized away,
// even if the actual return value is not changeable.
//
// This matches the PostgreSQL definition of `provolatile` in `pg_proc`.
type Volatility byte

// String returns the byte representation of Volatility as a string.
func (v Volatility) String() string {
	return string([]byte{byte(v)})
}

const (
	// VolatilityImmutable means the operator cannot modify the database, the transaction state,
	// or any other state. It cannot depend on configuration settings and is
	// guaranteed to return the same results given the same arguments forever.
	// Immutable operators can be constant folded.
	// Examples: log, from_json.
	VolatilityImmutable Volatility = 'i'
	// VolatilityStable means the operator cannot modify the database or the transaction state
	// and is guaranteed to return the same results given the same arguments
	// whenever it is evaluated within the same statement. Multiple calls to a
	// stable operator can be optimized to a single call.
	// Examples: current_timestamp, current_date.
	VolatilityStable Volatility = 's'
	// VolatilityVolatile means the operator can do anything, including modifying database state.
	// Examples: random, crdb_internal.force_error, nextval.
	VolatilityVolatile Volatility = 'v'
)
