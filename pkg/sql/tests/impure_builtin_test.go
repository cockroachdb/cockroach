// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func TestClusterID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())
	expected := tc.ApplicationLayer(0).RPCContext().LogicalClusterID.Get()

	for i := 0; i < 3; i++ {
		conn := tc.ApplicationLayer(i).SQLConn(t, serverutils.DBName("system"))
		db := sqlutils.MakeSQLRunner(conn)
		var clusterID uuid.UUID
		db.QueryRow(t, "SELECT crdb_internal.cluster_id()").Scan(&clusterID)
		if expected != clusterID {
			t.Fatalf("expected %v, got %v", expected, clusterID)
		}
	}
}
