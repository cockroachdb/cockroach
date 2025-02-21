// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestBulkMergeProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tc := testcluster.NewTestCluster(t, 1, base.TestClusterArgs{})
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)

	// Just testing that the package is set up correctly.
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE DATABASE test`)

	// _, err := newBulkMergeProcessor(ctx, nil, 0, execinfrapb.BulkMergeSpec{}, nil, nil)
	// require.ErrorContains(t, err, "unimplemented")
}
