// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import "github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"

// MakeStateLoader creates a StateLoader for the EvalContext.
func MakeStateLoader(rec EvalContext) stateloader.StateLoader {
	return stateloader.Make(rec.GetRangeID())
}
