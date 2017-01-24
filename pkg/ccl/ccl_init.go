// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package ccl

// We import each of the CCL packages that use init hooks below, so a single
// import of this package enables building a binary with CCL features.

import (
	_ "github.com/cockroachdb/cockroach/pkg/ccl/buildccl"   // ccl init hooks
	_ "github.com/cockroachdb/cockroach/pkg/ccl/sqlccl"     // ccl init hooks
	_ "github.com/cockroachdb/cockroach/pkg/ccl/storageccl" // ccl init hooks
)
