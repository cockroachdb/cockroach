// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

// TestResourceGroupIDPropagation verifies that the id bound to a session via
// SET resource_group reaches KV on the admission header of the session's
// BatchRequests, through two paths:
//
//  1. Transaction -> KV BatchRequest admission header, on the gateway's own
//     requests (the batch_request subtest).
//  2. DistSQL remote flows -> leaf transaction, so requests issued by a leaf
//     txn on a remote node also carry the id (the distsql_remote subtest).
//     This is the path that NewLeafTxn's admission-header copy enables.
//
// Propagation is independent of admission.cpu_time_tokens.mode: the gateway
// always stamps the header. The mode only governs whether the server routes
// on the id, which is covered by the unit tests in pkg/util/admission.
func TestResourceGroupIDPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// SET resource_group = 'low' resolves to this id.
	const lowID = uint64(admissionpb.LowResourceGroupID)

	makeFilter := func(seen *map[uint64]struct{}, mu *syncutil.Mutex) kvserverbase.ReplicaRequestFilter {
		return func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
			if id := ba.AdmissionHeader.ResourceGroupID; id != 0 {
				mu.Lock()
				(*seen)[id] = struct{}{}
				mu.Unlock()
			}
			return nil
		}
	}

	t.Run("batch_request", func(t *testing.T) {
		var mu syncutil.Mutex
		seen := make(map[uint64]struct{})

		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					TestingRequestFilter: makeFilter(&seen, &mu),
				},
			},
		})
		defer s.Stopper().Stop(ctx)

		// Pin the session to a single connection so the SET persists across
		// statements.
		db.SetMaxOpenConns(1)
		runner := sqlutils.MakeSQLRunner(db)
		runner.Exec(t, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
		runner.Exec(t, "SET resource_group = 'crdb_internal_resource_group_low'")

		mu.Lock()
		seen = make(map[uint64]struct{})
		mu.Unlock()

		runner.Exec(t, "INSERT INTO t VALUES (1, 10)")

		mu.Lock()
		_, sawLow := seen[lowID]
		mu.Unlock()
		require.True(t, sawLow,
			"expected a BatchRequest stamped with the low resource group id (%d)", lowID)
	})

	t.Run("distsql_remote", func(t *testing.T) {
		var mu syncutil.Mutex
		seen := make(map[uint64]struct{})

		tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingRequestFilter: makeFilter(&seen, &mu),
					},
				},
			},
		})
		defer tc.Stopper().Stop(ctx)

		if tc.DefaultTenantDeploymentMode().IsExternal() {
			tc.GrantTenantCapabilities(ctx, t, serverutils.TestTenantID(),
				map[tenantcapabilitiespb.ID]string{
					tenantcapabilitiespb.CanAdminRelocateRange: "true",
				})
		}

		db := tc.ServerConn(0)
		db.SetMaxOpenConns(1)
		runner := sqlutils.MakeSQLRunner(db)
		runner.Exec(t, "CREATE TABLE td (id INT PRIMARY KEY, v INT)")
		runner.Exec(t, "INSERT INTO td SELECT generate_series(1, 30), generate_series(1, 30)")

		// Split td into three ranges and place them on different nodes so that a
		// distributed scan creates leaf txns on the remote nodes.
		runner.Exec(t, "ALTER TABLE td SPLIT AT VALUES (10), (20)")
		runner.ExecSucceedsSoon(t, fmt.Sprintf(
			"ALTER TABLE td EXPERIMENTAL_RELOCATE VALUES (ARRAY[%d], 1), (ARRAY[%d], 10), (ARRAY[%d], 20)",
			tc.Server(0).GetFirstStoreID(),
			tc.Server(1).GetFirstStoreID(),
			tc.Server(2).GetFirstStoreID(),
		))
		// Populate the range cache so the gateway plans remote flows.
		runner.Exec(t, "SELECT count(*) FROM td")

		runner.Exec(t, "SET distsql = always")
		runner.Exec(t, "SET resource_group = 'crdb_internal_resource_group_low'")

		mu.Lock()
		seen = make(map[uint64]struct{})
		mu.Unlock()

		runner.Exec(t, "SELECT * FROM td WHERE v > 0")

		mu.Lock()
		_, sawLow := seen[lowID]
		mu.Unlock()
		require.True(t, sawLow,
			"expected a leaf-issued BatchRequest stamped with the low resource group id (%d)", lowID)
	})
}
