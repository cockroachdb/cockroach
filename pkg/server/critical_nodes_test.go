// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestCriticalNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{Server: &TestingKnobs{
				DefaultSystemZoneConfigOverride: zonepb.DefaultZoneConfigRef(),
			}},
		},
	})
	s := testCluster.Server(0).TenantStatusServer().(serverpb.StatusServer)
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	// Add a table, and alter its zone config to something
	// that can't be satisfied, given this is a 3-node cluster.
	db := testCluster.ServerConn(0)
	_, err := db.Exec("CREATE TABLE test (x int PRIMARY KEY)")
	require.NoError(t, err)
	_, err = db.Exec("ALTER TABLE test CONFIGURE ZONE USING num_replicas = 5;")
	require.NoError(t, err)

	testutils.SucceedsWithin(t, func() error {
		res, err := s.CriticalNodes(ctx, &serverpb.CriticalNodesRequest{})
		if err != nil {
			return err
		}
		if res.Report.IsEmpty() {
			return errors.Errorf(
				"expected report to not be empty, got: {over: %d, under: %d, violating: %d, unavailable: %d}",
				len(res.Report.OverReplicated),
				len(res.Report.UnderReplicated),
				len(res.Report.ViolatingConstraints),
				len(res.Report.Unavailable),
			)
		}

		// We should expect all 3 nodes to be critical, because they all contain
		// a replica for the under-replicated range.
		require.Equal(t, 3, len(res.CriticalNodes))
		return nil
	}, 2*time.Minute)
}
