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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var defaultNumSteps = envutil.EnvOrDefaultInt("COCKROACH_KVNEMESIS_STEPS", 50)

func (cfg kvnemesisTestCfg) testClusterArgs(tr *SeqTracker) base.TestClusterArgs {
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

	isOurCommand := func(ba *kvpb.BatchRequest) (string, uint64, bool) {
		key := ba.Requests[0].GetInner().Header().Key
		n, err := fkE(string(key))
		if err != nil {
			return "", 0, false
		}
		return string(key), n, true
	}

	shouldInject := func(baseProb float64, key uint64, attempt int) bool {
		// Example: baseProb = 0.8
		// On attempt 1, 0.8/1 = 80% chance of catching retry.
		// On attempt 2, 0.8/2 = 40%.
		// On attempt 3, 0.8/3 = 27%.
		// And so on.
		thresh := baseProb / float64(attempt)
		// NB: it's important to include "attempt" in here so that a write to a key
		// that is unlucky enough to map to, say, 1E-9, eventually gets to
		// successfully go through.
		return rand.New(rand.NewSource(int64(attempt)+int64(key))).Float64() < thresh
	}

	storeKnobs.LeaseIndexFilter = nil
	storeKnobs.InjectReproposalError = nil

	if p := cfg.injectReproposalErrorProb; p > 0 {
		var mu syncutil.Mutex
		seen := map[string]int{}
		storeKnobs.InjectReproposalError = func(pd *kvserver.ProposalData) error {
			key, n, ok := isOurCommand(pd.Request)
			if !ok {
				return nil
			}

			mu.Lock()
			defer mu.Unlock()
			seen[key]++
			if !shouldInject(p, n, seen[key]) {
				return nil
			}
			log.Infof(context.Background(), "inserting reproposal error for %s (seen %d times)", roachpb.Key(key), seen[key])
			err := errInjected // special error that kvnemesis accepts
			return errors.Wrapf(err, "on %s at %s", pd.Request.Summary(), roachpb.Key(key))
		}
	}

	if p := cfg.injectReproposalErrorProb; p > 0 {
		var mu syncutil.Mutex
		seen := map[string]int{}
		storeKnobs.LeaseIndexFilter = func(pd *kvserver.ProposalData) kvpb.LeaseAppliedIndex {
			key, n, ok := isOurCommand(pd.Request)
			if !ok {
				return 0
			}

			mu.Lock()
			defer mu.Unlock()
			seen[key]++
			if !shouldInject(p, n, seen[key]) {
				return 0
			}
			log.Infof(context.Background(), "inserting illegal lease index for %s (seen %d times)", roachpb.Key(key), seen[key])
			// LAI 1 is always going to fail because the LAI is initialized when the lease
			// comes into existence. (It's important that we pick one here that reliably
			// fails because otherwise we may accidentally regress the closed timestamp[^1].
			//
			// [^1]: https://github.com/cockroachdb/cockroach/issues/70894#issuecomment-1433244880
			return 1
		}
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
	// The two knobs below inject illegal lease index errors and, for the
	// resulting reproposals, reproposal errors. The injection is stateful and
	// remembers the keys on which the commands operated, and, per key, the
	// probability is scaled down linearly based on the number of times we've
	// injected an error. In other words, this can be set to 1.0 and some amount
	// of progress would still be made.
	//
	// NB: to at least directionally preserve determinism, the rand for each dice
	// roll is seeded from the uint64 represented by the key, so this shouldn't be
	// considered truly random, but is random enough for the desired purpose.
	invalidLeaseAppliedIndexProb float64 // [0,1)
	injectReproposalErrorProb    float64 // [0,1)
}

func TestKVNemesisSingleNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testKVNemesisImpl(t, kvnemesisTestCfg{
		numNodes:                     1,
		numSteps:                     defaultNumSteps,
		concurrency:                  5,
		seedOverride:                 0,
		invalidLeaseAppliedIndexProb: 0.1,
		injectReproposalErrorProb:    0.1,
	})
}

func TestKVNemesisSingleNode_ReproposalChaos(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testKVNemesisImpl(t, kvnemesisTestCfg{
		numNodes:                     1,
		numSteps:                     defaultNumSteps,
		concurrency:                  5,
		seedOverride:                 0,
		invalidLeaseAppliedIndexProb: 0.9,
		injectReproposalErrorProb:    0.5,
	})
}

func TestKVNemesisMultiNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testKVNemesisImpl(t, kvnemesisTestCfg{
		numNodes:                     4,
		numSteps:                     defaultNumSteps,
		concurrency:                  5,
		seedOverride:                 0,
		invalidLeaseAppliedIndexProb: 0.1,
		injectReproposalErrorProb:    0.1,
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
	tc := testcluster.StartTestCluster(t, cfg.numNodes, cfg.testClusterArgs(tr))
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
