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
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
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
			if seq := kvnemesisutil.Seq(vh.KVNemesisSeq.Get()); seq > 0 {
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
					OnRangeSpanningNonTxnalBatch: func(ba *roachpb.BatchRequest) *roachpb.Error {
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

func randWithSeed(
	t interface {
		Logf(string, ...interface{})
		Helper()
	}, seedOrZero int64,
) *rand.Rand {
	t.Helper()
	var rng *rand.Rand
	if seedOrZero > 0 {
		rng = rand.New(rand.NewSource(seedOrZero))
	} else {
		rng, seedOrZero = randutil.NewTestRand()
	}
	t.Logf("seed: %d", seedOrZero)
	return rng
}

func TestKVNemesisSingleNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t)

	if !buildutil.CrdbTestBuild {
		// `roachpb.RequestHeader` and `MVCCValueHeader` have a KVNemesisSeq field
		// that is zero-sized outside of test builds. We could revisit that should
		// a need arise to run kvnemesis against production binaries.
		skip.IgnoreLint(t, "kvnemesis must be run with the crdb_test build tag")
	}

	ctx := context.Background()

	// Can set a seed here for determinism. This works best when the seed was
	// obtained with concurrency=1.
	const concurrency = 5
	rng := randWithSeed(t, 0)

	tr := &SeqTracker{}
	tc := testcluster.StartTestCluster(t, 1, testClusterArgs(tr))
	defer tc.Stopper().Stop(ctx)
	db := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)
	sqlutils.MakeSQLRunner(sqlDB).Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)

	config := NewDefaultConfig()
	config.NumNodes, config.NumReplicas = 1, 1

	env := &Env{SQLDBs: []*gosql.DB{sqlDB}, Tracker: tr, L: t}
	// NB: when a failure is observed, it can be helpful to try to reproduce it
	// with concurrency 1, as then determinism is much more likely once a suitable
	// seed has been identified.
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

	if !buildutil.CrdbTestBuild {
		// `roachpb.RequestHeader` and `MVCCValueHeader` have a KVNemesisSeq field
		// that is zero-sized outside test builds. We could revisit that should
		// a need arise to run kvnemesis against production binaries.
		skip.IgnoreLint(t, "kvnemesis must be run with the crdb_test build tag")
	}

	// Can set a seed here for determinism. This works best when the seed was
	// obtained with concurrency=1.
	const concurrency = 5
	rng := randWithSeed(t, 0)

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
	env := &Env{SQLDBs: sqlDBs, Tracker: tr, L: t}
	failures, err := RunNemesis(ctx, rng, env, config, concurrency, numSteps, dbs...)
	require.NoError(t, err, `%+v`, err)

	for _, failure := range failures {
		t.Errorf("failure:\n%+v", failure)
	}
}

// TestRunReproductionSteps is a helper that allows quickly running a kvnemesis
// history.
func TestRunReproductionSteps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.IgnoreLint(t, "test unskipped only on demand")
	ctx := context.Background()

	const n = 1 // number of nodes

	tc := testcluster.StartTestCluster(t, n, base.TestClusterArgs{})
	db0 := tc.Server(0).DB()
	_, _ = db0, ctx

	// Paste a repro as printed by kvnemesis here.
}
