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
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
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

var defaultNumSteps = envutil.EnvOrDefaultInt("COCKROACH_KVNEMESIS_STEPS", 50)

func testClusterArgs(tr *SeqTracker) base.TestClusterArgs {
	storeKnobs := &kvserver.StoreTestingKnobs{
		// Drop the clock MaxOffset to reduce commit-wait time for
		// transactions that write to global_read ranges.
		MaxOffset: 10 * time.Millisecond,
		// Make sure we know the seq for each of our writes when they come out of
		// the rangefeed. We do this via an interceptor to avoid having to change
		// RangeFeed's APIs.
		RangefeedValueHeaderFilter: func(key, endKey roachpb.Key, ts hlc.Timestamp, vh enginepb.MVCCValueHeader) {
			if seq := vh.KVNemesisSeq.Get(); seq > 0 {
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
					OnRangeSpanningNonTxnalBatch: func(ba *kvpb.BatchRequest) *kvpb.Error {
						for _, req := range ba.Requests {
							if req.GetInner().Method() != kvpb.DeleteRange {
								continue
							}
							if req.GetDeleteRange().UseRangeTombstone == true {
								return kvpb.NewError(errDelRangeUsingTombstoneStraddlesRangeBoundary)
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

type ti interface {
	Helper()
	Logf(string, ...interface{})
}

type tBridge struct {
	ti
	ll logLogger
}

func newTBridge(t *testing.T) *tBridge {
	// NB: we're not using t.TempDir() because we want these to survive
	// on failure.
	td, err := os.MkdirTemp("", "kvnemesis")
	if err != nil {
		td = os.TempDir()
	}
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		_ = os.RemoveAll(td)
	})
	t.Logf("kvnemesis logging to %s", td)
	return &tBridge{
		ti: t,
		ll: logLogger{
			dir: td,
		},
	}
}

func (t *tBridge) WriteFile(basename string, contents string) string {
	return t.ll.WriteFile(basename, contents)
}

type kvnemesisTestCfg struct {
	numNodes     int
	numSteps     int
	concurrency  int
	seedOverride int64
}

func TestKVNemesisSingleNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testKVNemesisImpl(t, kvnemesisTestCfg{
		numNodes:     1,
		numSteps:     defaultNumSteps,
		concurrency:  5,
		seedOverride: 0,
	})
}

func TestKVNemesisMultiNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testKVNemesisImpl(t, kvnemesisTestCfg{
		numNodes:     4,
		numSteps:     defaultNumSteps,
		concurrency:  5,
		seedOverride: 0,
	})
}

func testKVNemesisImpl(t *testing.T, cfg kvnemesisTestCfg) {
	skip.UnderRace(t)

	if !buildutil.CrdbTestBuild {
		// `kvpb.RequestHeader` and `MVCCValueHeader` have a KVNemesisSeq field
		// that is zero-sized outside test builds. We could revisit that should
		// a need arise to run kvnemesis against production binaries.
		skip.IgnoreLint(t, "kvnemesis must be run with the crdb_test build tag")
	}

	// Can set a seed here for determinism. This works best when the seed was
	// obtained with cfg.concurrency=1.
	rng := randWithSeed(t, cfg.seedOverride)

	// 4 nodes so we have somewhere to move 3x replicated ranges to.
	ctx := context.Background()
	tr := &SeqTracker{}
	tc := testcluster.StartTestCluster(t, cfg.numNodes, testClusterArgs(tr))
	defer tc.Stopper().Stop(ctx)
	dbs, sqlDBs := make([]*kv.DB, cfg.numNodes), make([]*gosql.DB, cfg.numNodes)
	for i := 0; i < cfg.numNodes; i++ {
		dbs[i] = tc.Server(i).DB()
		sqlDBs[i] = tc.ServerConn(i)
	}
	sqlutils.MakeSQLRunner(sqlDBs[0]).Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	// Turn net/trace on, which results in real trace spans created throughout.
	// This gives kvnemesis a chance to hit NPEs related to tracing.
	sqlutils.MakeSQLRunner(sqlDBs[0]).Exec(t, `SET CLUSTER SETTING trace.debug.enable = true`)

	config := NewDefaultConfig()
	config.NumNodes = cfg.numNodes
	config.NumReplicas = 3
	if config.NumReplicas > cfg.numNodes {
		config.NumReplicas = cfg.numNodes
	}
	logger := newTBridge(t)
	env := &Env{SQLDBs: sqlDBs, Tracker: tr, L: logger}
	failures, err := RunNemesis(ctx, rng, env, config, cfg.concurrency, cfg.numSteps, dbs...)
	require.NoError(t, err, `%+v`, err)
	require.Zero(t, len(failures), "kvnemesis detected failures") // they've been logged already
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
