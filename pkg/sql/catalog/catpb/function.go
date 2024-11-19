// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catpb

// DefaultFunctionVolatility is the default volatility assigned to a user defined
// function if there is no user provided volatility through CREATE (OR REPLACE)
// FUNCTION.
const DefaultFunctionVolatility = Function_VOLATILE

// DefaultFunctionLeakProof is the LeakProof flag assigned to a user defined
// function if there is no user provided value through CREATE (OR REPLACE)
// FUNCTION.
const DefaultFunctionLeakProof = false
