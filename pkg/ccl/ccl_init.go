// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package ccl

// We import each of the CCL packages that use init hooks below, so a single
// import of this package enables building a binary with CCL features.

import (
	// ccl init hooks
	_ "github.com/cockroachdb/cockroach/pkg/ccl/buildccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/sqlccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/utilccl/intervalccl"
)
