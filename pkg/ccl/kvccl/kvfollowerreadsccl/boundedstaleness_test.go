// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvfollowerreadsccl_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestBoundedStalenessEnterpriseLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	queries := []string{
		`SELECT with_max_staleness('10s')`,
		`SELECT with_min_timestamp(statement_timestamp())`,
	}

	defer utilccl.TestingDisableEnterprise()()
	t.Run("disabled", func(t *testing.T) {
		for _, q := range queries {
			t.Run(q, func(t *testing.T) {
				_, err := tc.Conns[0].QueryContext(ctx, q)
				require.Error(t, err)
				require.Contains(t, err.Error(), "use of bounded staleness requires an enterprise license")
			})
		}
	})

	t.Run("enabled", func(t *testing.T) {
		defer utilccl.TestingEnableEnterprise()()
		for _, q := range queries {
			t.Run(q, func(t *testing.T) {
				r, err := tc.Conns[0].QueryContext(ctx, q)
				require.NoError(t, err)
				require.NoError(t, r.Close())
			})
		}
	})
}
