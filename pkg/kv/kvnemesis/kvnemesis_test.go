// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemesis

import (
	"context"
	gosql "database/sql"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

var numSteps int

func init() {
	numSteps = envutil.EnvOrDefaultInt("COCKROACH_KVNEMESIS_STEPS", 50)
}

func TestKVNemesisSingleNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					// Drop the clock MaxOffset to reduce commit-wait time for
					// transactions that write to global_read ranges.
					MaxOffset: 10 * time.Millisecond,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)
	db := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)
	sqlutils.MakeSQLRunner(sqlDB).Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)

	config := NewDefaultConfig()
	config.NumNodes, config.NumReplicas = 1, 1
	rng, _ := randutil.NewPseudoRand()
	env := &Env{sqlDBs: []*gosql.DB{sqlDB}}
	failures, err := RunNemesis(ctx, rng, env, config, numSteps, db)
	require.NoError(t, err, `%+v`, err)

	for _, failure := range failures {
		t.Errorf("failure:\n%+v", failure)
	}
}

func TestKVNemesisMultiNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t)

	// 4 nodes so we have somewhere to move 3x replicated ranges to.
	const numNodes = 4
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					// Drop the clock MaxOffset to reduce commit-wait time for
					// transactions that write to global_read ranges.
					MaxOffset: 10 * time.Millisecond,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)
	dbs, sqlDBs := make([]*kv.DB, numNodes), make([]*gosql.DB, numNodes)
	for i := 0; i < numNodes; i++ {
		dbs[i] = tc.Server(i).DB()
		sqlDBs[i] = tc.ServerConn(i)
	}
	sqlutils.MakeSQLRunner(sqlDBs[0]).Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	// Turn net/trace on, which results in real trace spans created throughout.
	// This gives kvnemesis a chance to hit NPEs related to tracing.
	sqlutils.MakeSQLRunner(sqlDBs[0]).Exec(t, `SET CLUSTER SETTING trace.debug.enable = true`)

	config := NewDefaultConfig()
	config.NumNodes, config.NumReplicas = numNodes, 3
	rng, _ := randutil.NewPseudoRand()
	env := &Env{sqlDBs: sqlDBs}
	failures, err := RunNemesis(ctx, rng, env, config, numSteps, dbs...)
	require.NoError(t, err, `%+v`, err)

	for _, failure := range failures {
		t.Errorf("failure:\n%+v", failure)
	}
}
