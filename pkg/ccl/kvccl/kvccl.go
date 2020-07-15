// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvccl

import (
	// ccl init hooks.
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvfollowerreadsccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
)
