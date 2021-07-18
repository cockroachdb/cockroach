// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvboundedstalenessccl_test

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

func TestNoEnterpriseLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer utilccl.TestingDisableEnterprise()()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	for _, q := range []string{
		`SELECT with_max_staleness('10s')`,
		`SELECT with_min_timestamp(now())`,
	} {
		t.Run(q, func(t *testing.T) {
			_, err := tc.Conns[0].QueryContext(ctx, q)
			require.Error(t, err)
			require.Contains(t, err.Error(), "use of bounded staleness requires an enterprise license")
		})
	}
}
