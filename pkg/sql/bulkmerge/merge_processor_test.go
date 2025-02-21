// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestBulkMergeProcessor(t *testing.T) {
	ctx := context.Background()
	defer leaktest.AfterTest(t)()

	tc := testcluster.NewTestCluster(t, 1, base.TestClusterArgs{})
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)

	// Just testing that the package is set up correctly.
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE DATABASE test`)

	_, err := newBulkMergeProcessor(ctx, nil, 0, execinfrapb.StreamIngestionDataSpec{}, nil)
	require.ErrorContains(t, err, "unimplemented")
}
