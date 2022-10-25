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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvnemesis/kvnemesisutil"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

var numSteps int

func init() {
	numSteps = envutil.EnvOrDefaultInt("COCKROACH_KVNEMESIS_STEPS", 50)
}

func testClusterArgs(tr *SeqTracker) base.TestClusterArgs {
	storeKnobs := &kvserver.StoreTestingKnobs{
		// Drop the clock MaxOffset to reduce commit-wait time for
		// transactions that write to global_read ranges.
		MaxOffset: 10 * time.Millisecond,
		// Make sure we know the seq for each of our writes when they come out of
		// the rangefeed. We do this via an interceptor to avoid having to change
		// RangeFeed's APIs.
		RangefeedValueHeaderFilter: func(key, endKey roachpb.Key, ts hlc.Timestamp, vh enginepb.MVCCValueHeader) {
			seq := kvnemesisutil.Seq(vh.Seq)
			if false && len(endKey) > 0 {
				fmt.Printf("XXX rangefeed intercept ranged write [%s,%s), seq %s\n", key, endKey, seq)
			}
			if seq > 0 {
				tr.Add(key, endKey, ts, seq)
			}
		},
	}

	return base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: storeKnobs,
				KVClient: &kvcoord.ClientTestingKnobs{
					// Don't let DistSender split DeleteRangeUsingTombstone across range boundaries.
					// This does happen in real CRDB, but leads to separate atomic subunits, which
					// would add complexity to kvnemesis that isn't worth it. Instead, the operation
					// generator for the most part tries to avoid range-spanning requests, and the
					// ones that do end up happening get a hard error.
					OnRangeSpanningNonTxnalBatch: func(ba roachpb.BatchRequest) *roachpb.Error {
						for _, req := range ba.Requests {
							if req.GetInner().Method() != roachpb.DeleteRange {
								continue
							}
							if req.GetDeleteRange().UseRangeTombstone == true {
								return roachpb.NewErrorf("DeleteRangeUsingTombstone can not straddle range boundary")
							}
						}
						return nil
					},
				},
			},
		},
	}
}

func TestKVNemesisSingleNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t)

	ctx := context.Background()

	var seed int64 = 0 //5408881829841458374 // easy to set a seed here from a failing invocation
	var rng *rand.Rand
	if seed > 0 {
		rng = rand.New(rand.NewSource(seed))
		rand.Seed(seed)
	} else {
		rng, seed = randutil.NewTestRand()
	}
	t.Logf("seed: %d", seed)
	// TODO(tbg): why doesn't the t.Logf show up under `./dev test --stress` even with `--test-args='-show-logs''?
	// At least not if the test panics.
	log.Infof(ctx, "seed: %d", seed)

	tr := &SeqTracker{}
	tc := testcluster.StartTestCluster(t, 1, testClusterArgs(tr))
	defer tc.Stopper().Stop(ctx)
	db := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)
	sqlutils.MakeSQLRunner(sqlDB).Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)

	config := NewDefaultConfig()
	config.Ops.Split = SplitConfig{}
	config.Ops.Merge = MergeConfig{}
	config.Ops.ChangeZone = ChangeZoneConfig{}
	config.Ops.ChangeLease = ChangeLeaseConfig{}

	c := config.Ops.ClosureTxn.TxnBatchOps.Ops
	c.GetMissing = 0
	c.GetMissingForUpdate = 0
	c.GetExisting = 0
	c.GetExistingForUpdate = 0
	c.PutMissing = 0
	c.PutExisting = 0
	c.Scan = 0
	c.ScanForUpdate = 0
	// c.ReverseScan = 0
	c.ReverseScanForUpdate = 0
	//c.DeleteMissing = 0
	//c.DeleteExisting = 0
	c.DeleteRange = 0
	c.DeleteRangeUsingTombstone = 0
	config.Ops.ClosureTxn.TxnClientOps = c
	//config.Ops.DB = c
	config.Ops.ClosureTxn.TxnBatchOps.Batch = 0
	// config.Ops.ClosureTxn.TxnBatchOps.Ops = ClientOperationConfig{}
	config.Ops.ClosureTxn.CommitInBatch = 0
	// config.Ops.DB.DeleteRangeUsingTombstone = 0 // HACK

	// config.Ops.DB.ScanForUpdate = 0
	// config.Ops.DB.ReverseScan = 0
	// config.Ops.DB.ReverseScanForUpdate = 0
	// config.Ops.DB.GetExisting = 0
	// config.Ops.DB.GetMissing = 0
	// config.Ops.DB.GetExistingForUpdate = 0
	// config.Ops.DB.GetMissingForUpdate = 0

	config.NumNodes, config.NumReplicas = 1, 1

	env := &Env{SQLDBs: []*gosql.DB{sqlDB}, Tracker: tr, L: t}
	// NB: when a failure is observed, it can be helpful to try to reproduce it
	// with concurrency 1, as then determinism is much more likely once a suitable
	// seed has been identified.
	const concurrency = 1 // HACK
	//numSteps = 5          // HACK
	failures, err := RunNemesis(ctx, rng, env, config, concurrency, numSteps, db)
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
	tr := &SeqTracker{}
	tc := testcluster.StartTestCluster(t, numNodes, testClusterArgs(tr))
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
	rng, _ := randutil.NewTestRand()
	env := &Env{SQLDBs: sqlDBs, Tracker: tr, L: t}
	const concurrency = 5
	failures, err := RunNemesis(ctx, rng, env, config, concurrency, numSteps, dbs...)
	require.NoError(t, err, `%+v`, err)

	for _, failure := range failures {
		t.Errorf("failure:\n%+v", failure)
	}
}
