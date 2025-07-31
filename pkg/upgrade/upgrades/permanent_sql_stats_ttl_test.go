// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLStatsTTLChange is testing the permanent upgrade associated with
// Permanent_V23_1ChangeSQLStatsTTL. We no longer support versions this old, but
// we still need to test that the upgrade happens as expected when creating a
// new cluster.
func TestSQLStatsTTLChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderRace(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)
	defer db.Close()

	tables := []string{
		"system.public.statement_statistics",
		"system.public.transaction_statistics",
		"system.public.statement_activity",
		"system.public.transaction_activity",
	}

	var target string
	var rawConfigSql string
	for _, table := range tables {
		row := db.QueryRow(fmt.Sprintf("SHOW ZONE CONFIGURATION FROM TABLE %s", table))
		err := row.Scan(&target, &rawConfigSql)
		require.NoError(t, err)

		assert.Equal(t, target, fmt.Sprintf("TABLE %s", table))
		assert.Contains(t, rawConfigSql, "gc.ttlseconds = 3600")
	}
}
