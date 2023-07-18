// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	expected := tc.TenantOrServer(0).RPCContext().LogicalClusterID.Get()

	for i := 0; i < 3; i++ {
		conn := serverutils.OpenDBConn(t, tc.TenantOrServer(i).SQLAddr(), "system", false, tc.Stopper())
		db := sqlutils.MakeSQLRunner(conn)
		var clusterID uuid.UUID
		db.QueryRow(t, "SELECT crdb_internal.cluster_id()").Scan(&clusterID)
		if expected != clusterID {
			t.Fatalf("expected %v, got %v", expected, clusterID)
		}
	}
}
