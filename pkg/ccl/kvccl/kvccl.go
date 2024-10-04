// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvccl

import (
	// ccl init hooks.
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvfollowerreadsccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
)
