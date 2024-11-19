// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

// TestCreateActivityUpdateJobMigration is testing the permanent upgrade
// associated Permanent_V23_1CreateSystemActivityUpdateJob. We no longer support
// versions this old, but we still need to test that the upgrade happens as
// expected when creating a new cluster.
func TestCreateActivityUpdateJobMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderRace(t)
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()

	row := db.QueryRow("SELECT count(*) FROM system.public.jobs WHERE id = 103")
	assert.NotNil(t, row)
	assert.NoError(t, row.Err())
	var count int
	err := row.Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}
