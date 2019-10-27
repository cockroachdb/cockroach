// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package ccl

// We import each of the CCL packages that use init hooks below, so a single
// import of this package enables building a binary with CCL features.

import (
	// ccl init hooks. Don't include cliccl here, it pulls in pkg/cli, which
	// does weird things at init time.
	_ "github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/buildccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/followerreadsccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/gssapiccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/importccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/roleccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/workloadccl"
)
