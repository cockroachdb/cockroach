// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package intentresolver_test

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func forceScanOnAllReplicationQueues(tc *testcluster.TestCluster) (err error) {
	for _, s := range tc.Servers {
		err = s.Stores().VisitStores(func(store *kvserver.Store) error {
			return store.ForceReplicationScanAndProcess()
		})
	}
	return err
}

func TestIntentResolutionUnavailableRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	const numNodes = 3
	const intentResolverSendMaxTimeout = 5 * time.Second
	serverArgs := make(map[int]base.TestServerArgs)
	for i := 1; i <= numNodes; i++ {
		serverArgs[i-1] = base.TestServerArgs{
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{
						Key: "rack", Value: strconv.Itoa(i),
					},
				},
			},
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					IntentResolverKnobs: kvserverbase.IntentResolverTestingKnobs{
						InFlightBackpressureLimit:    1,
						IntentResolverSendMaxTimeout: intentResolverSendMaxTimeout,
					},
				},
			},
		}
	}

	// Start test cluster.
	clusterArgs := base.TestClusterArgs{
		ReplicationMode:   base.ReplicationAuto,
		ServerArgsPerNode: serverArgs,
	}
	tc := testcluster.StartTestCluster(t, numNodes, clusterArgs)
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)

	numTables := 2
	for i := 1; i <= numTables; i++ {
		_, err := db.Exec(fmt.Sprintf("CREATE TABLE t%d (i INT PRIMARY KEY, j INT)", i))
		require.NoError(t, err)
		_, err = db.Exec(fmt.Sprintf("ALTER TABLE t%d CONFIGURE ZONE USING num_replicas = 1, constraints = '{\"+rack=%d\": 1}'", i, i))
		require.NoError(t, err)
	}
	testutils.SucceedsSoon(t, func() error {
		if err := forceScanOnAllReplicationQueues(tc); err != nil {
			return err
		}
		for i := 1; i <= numTables; i++ {
			r := db.QueryRow(fmt.Sprintf("select replicas from [show ranges from table t%d]", i))
			var repl string
			if err := r.Scan(&repl); err != nil {
				return err
			}
			if repl != fmt.Sprintf("{%d}", i) {
				return errors.Newf("Expected replicas {%d} for table t%d, got %s", i, i, repl)
			}
		}
		return nil
	})

	_, err := db.Exec("BEGIN")
	require.NoError(t, err)
	for i := 1; i <= numTables; i++ {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO t%d (i, j) VALUES (0, 0)", i))
		require.NoError(t, err)
	}
	_, err = db.Exec("COMMIT")
	require.NoError(t, err)

	tc.Servers[1].Stopper().Stop(ctx)
	time.Sleep(time.Second)

	_, err = db.Exec("BEGIN")
	require.NoError(t, err)
	_, err = db.Exec("UPDATE t1 SET j = 1 WHERE i = 0")
	require.NoError(t, err)

	go func() {
		time.Sleep(time.Second)
		_, err = db.Exec("COMMIT")
		require.NoError(t, err)
	}()

	db2 := tc.ServerConn(2)
	_, err = db2.Exec("BEGIN")
	require.NoError(t, err)
	var done int32
	go func() {
		_, _ = db2.Exec("SELECT * FROM t1")
		atomic.StoreInt32(&done, 1)
	}()
	testutils.SucceedsSoon(t, func() error {
		if atomic.LoadInt32(&done) != 1 {
			return errors.New("SELECT * FROM t1 did not complete because intent resolution has been clogged up")
		}
		return nil
	})
}
