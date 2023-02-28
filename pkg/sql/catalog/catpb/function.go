// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catpb

// DefaultFunctionVolatility is the default volatility assigned to a user defined
// function if there is no user provided volatility through CREATE (OR REPLACE)
// FUNCTION.
const DefaultFunctionVolatility = Function_VOLATILE

// DefaultFunctionLeakProof is the LeakProof flag assigned to a user defined
// function if there is no user provided value through CREATE (OR REPLACE)
// FUNCTION.
const DefaultFunctionLeakProof = false
