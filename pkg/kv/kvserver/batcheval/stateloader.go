// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import "github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"

// MakeStateLoader creates a StateLoader for the EvalContext.
func MakeStateLoader(rec EvalContext) stateloader.StateLoader {
	return stateloader.Make(rec.GetRangeID())
}
