// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import "github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"

// MakeStateLoader creates a StateLoader for the EvalContext.
func MakeStateLoader(rec EvalContext) kvstorage.StateLoader {
	return kvstorage.MakeStateLoader(rec.GetRangeID())
}
