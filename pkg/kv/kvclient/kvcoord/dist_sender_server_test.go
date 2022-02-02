// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord_test

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/kvclientutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// NOTE: these tests are in package kv_test to avoid a circular
// dependency between the server and kv packages. These tests rely on
// starting a TestServer, which creates a "real" node and employs a
// distributed sender server-side.

func startNoSplitMergeServer(t *testing.T) (*server.TestServer, *kv.DB) {
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableSplitQueue: true,
				DisableMergeQueue: true,
			},
		},
	})
	return s.(*server.TestServer), db
}

// TestRangeLookupWithOpenTransaction verifies that range lookups are
// done in such a way (e.g. using inconsistent reads) that they
// proceed in the event that a write intent is extant at the meta
// index record being read.
func TestRangeLookupWithOpenTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _ := startNoSplitMergeServer(t)
	defer s.Stopper().Stop(context.Background())

	// Create an intent on the meta1 record by writing directly to the
	// engine.
	key := testutils.MakeKey(keys.Meta1Prefix, roachpb.KeyMax)
	now := s.Clock().Now()
	txn := roachpb.MakeTransaction("txn", roachpb.Key("foobar"), 0, now, 0, int32(s.SQLInstanceID()))
	if err := storage.MVCCPutProto(
		context.Background(), s.Engines()[0],
		nil, key, now, &txn, &roachpb.RangeDescriptor{}); err != nil {
		t.Fatal(err)
	}

	// Create a new DistSender and client.DB so that the Get below is guaranteed
	// to not hit in the range descriptor cache forcing a RangeLookup operation.
	ambient := s.AmbientCtx()
	ds := kvcoord.NewDistSender(kvcoord.DistSenderConfig{
		AmbientCtx:         ambient,
		Settings:           cluster.MakeTestingClusterSettings(),
		Clock:              s.Clock(),
		NodeDescs:          s.Gossip(),
		RPCContext:         s.RPCContext(),
		NodeDialer:         nodedialer.New(s.RPCContext(), gossip.AddressResolver(s.Gossip())),
		FirstRangeProvider: s.Gossip(),
	})
	tsf := kvcoord.NewTxnCoordSenderFactory(
		kvcoord.TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			Clock:      s.Clock(),
			Stopper:    s.Stopper(),
		},
		ds,
	)
	db := kv.NewDB(ambient, tsf, s.Clock(), s.Stopper())

	// Now, with an intent pending, attempt (asynchronously) to read
	// from an arbitrary key. This will cause the distributed sender to
	// do a range lookup, which will encounter the intent. We're
	// verifying here that the range lookup doesn't fail with a write
	// intent error. If it did, it would go into a deadloop attempting
	// to push the transaction, which in turn requires another range
	// lookup, etc, ad nauseam.
	if _, err := db.Get(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
}

// setupMultipleRanges creates a database client to the supplied test
// server and splits the key range at the given keys. Returns the DB
// client.
func setupMultipleRanges(ctx context.Context, db *kv.DB, splitAt ...string) error {
	// Split the keyspace at the given keys.
	for _, key := range splitAt {
		if err := db.AdminSplit(ctx, key /* splitKey */, hlc.MaxTimestamp /* expirationTime */); err != nil {
			return err
		}
	}
	return nil
}

type checkResultsMode int

const (
	// Strict means that the expected results must be passed exactly.
	Strict checkResultsMode = iota
	// AcceptPrefix means that a superset of the expected results may be passed.
	// The actual results for each scan must be a prefix of the passed-in values.
	AcceptPrefix
)

type checkOptions struct {
	mode     checkResultsMode
	expCount int
}

// checks the keys returned from a Scan/ReverseScan.
//
// Args:
// expSatisfied: A set of indexes into spans representing the scans that
// 	have been completed and don't need a ResumeSpan. For these scans, having no
// 	results and also no resume span is acceptable by this function.
// resultsMode: Specifies how strict the result checking is supposed to be.
// 	expCount
// expCount: If resultsMode == AcceptPrefix, this is the total number of
// 	expected results. Ignored for resultsMode == Strict.
func checkSpanResults(
	t *testing.T, spans [][]string, results []kv.Result, expResults [][]string, opt checkOptions,
) {
	t.Helper()
	require.Equal(t, len(expResults), len(results), "unexpected number of results")

	// Ensure all the keys returned align properly with what is expected.
	count := 0
	for i, res := range results {
		resKeys := []string{}
		for _, kv := range res.Rows {
			resKeys = append(resKeys, string(kv.Key))
			count++
		}
		if expResults[i] == nil {
			expResults[i] = []string{}
		}
		switch opt.mode {
		case Strict:
			require.Equal(t, expResults[i], resKeys, "scan %d (%s): unexpected result", i, spans[i])
		case AcceptPrefix:
			require.Equal(t, expResults[i][:len(resKeys)], resKeys, "scan %d (%s): unexpected result", i, spans[i])
		}
	}
	if opt.mode == AcceptPrefix {
		require.Equal(t, opt.expCount, count)
	}
}

// checks ResumeSpan returned in a ScanResponse.
func checkResumeSpanScanResults(
	t *testing.T,
	spans [][]string,
	results []kv.Result,
	reverse bool,
	expResults [][]string,
	expSatisfied map[int]struct{},
	expReason roachpb.ResumeReason,
	opt checkOptions,
) {
	t.Helper()
	for i, res := range results {
		// Check that satisfied scans don't have resume spans.
		if _, satisfied := expSatisfied[i]; satisfied {
			require.Nil(t, res.ResumeSpan, "satisfied scan %d (%s) has ResumeSpan: %v",
				i, spans[i], res.ResumeSpan)
			require.Zero(t, res.ResumeReason, "satisfied scan %d (%s) has ResumeReason: %v",
				i, spans[i], res.ResumeReason)
			continue
		}

		// The scan is not expected to be satisfied, so there must be a resume span.
		// The resume span should be identical to the original request if no
		// results have been produced, or should continue after the last result
		// otherwise.
		require.NotNil(t, res.ResumeSpan, "scan %d (%s): no resume span", i, spans[i])
		require.NotZero(t, res.ResumeReason, "scan %d (%s): no resume reason. resume span: %+v",
			i, spans[i], res.ResumeSpan)
		require.Equal(t, expReason, res.ResumeReason,
			"scan %d (%s): unexpected resume reason", i, spans[i])

		if !reverse {
			if len(res.Rows) == 0 {
				require.GreaterOrEqual(t, string(res.ResumeSpan.Key), spans[i][0],
					"scan %d (%s): expected resume span %s to be at or above scan start", i, spans[i], res.ResumeSpan)
				require.Less(t, string(res.ResumeSpan.Key), spans[i][1],
					"scan %d (%s): expected resume span %s to be below scan end", i, spans[i], res.ResumeSpan)
			} else {
				require.Greater(t, string(res.ResumeSpan.Key), expResults[i][len(res.Rows)-1],
					"scan %d (%s): expected resume span %s to be above last result", i, spans[i], res.ResumeSpan)
			}
			require.Equal(t, spans[i][1], string(res.ResumeSpan.EndKey),
				"scan %d (%s): expected resume span %s to have same end key", i, spans[i], res.ResumeSpan)

		} else {
			if len(res.Rows) == 0 {
				require.Greater(t, string(res.ResumeSpan.EndKey), spans[i][0],
					"scan %d (%s): expected resume span %s to be above scan start", i, spans[i], res.ResumeSpan)
				require.LessOrEqual(t, string(res.ResumeSpan.EndKey), spans[i][1],
					"scan %d (%s): expected resume span %s to be at or below scan end", i, spans[i], res.ResumeSpan)
			} else {
				require.Less(t, string(res.ResumeSpan.EndKey), expResults[i][len(res.Rows)-1],
					"scan %d (%s): expected resume span %s to be below last result", i, spans[i], res.ResumeSpan)
			}
			require.Equal(t, spans[i][0], string(res.ResumeSpan.Key),
				"scan %d (%s): expected resume span %s to have same start key", i, spans[i], res.ResumeSpan)
		}
	}
}

// check an entire scan result including the ResumeSpan.
func checkScanResults(
	t *testing.T,
	spans [][]string,
	results []kv.Result,
	expResults [][]string,
	expSatisfied map[int]struct{},
	expReason roachpb.ResumeReason,
	opt checkOptions,
) {
	t.Helper()
	checkSpanResults(t, spans, results, expResults, opt)
	checkResumeSpanScanResults(t, spans, results, false /* reverse */, expResults, expSatisfied, expReason, opt)
}

func checkReverseScanResults(
	t *testing.T,
	spans [][]string,
	results []kv.Result,
	expResults [][]string,
	expSatisfied map[int]struct{},
	expReason roachpb.ResumeReason,
	opt checkOptions,
) {
	t.Helper()
	checkSpanResults(t, spans, results, expResults, opt)
	checkResumeSpanScanResults(t, spans, results, true /* reverse */, expResults, expSatisfied, expReason, opt)
}

// Tests limited scan requests across many ranges with multiple bounds.
func TestMultiRangeBoundedBatchScanSimple(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db := startNoSplitMergeServer(t)
	defer s.Stopper().Stop(ctx)

	require.NoError(t, setupMultipleRanges(ctx, db, "a", "b", "c", "d", "e", "f", "g", "h"))
	for _, key := range []string{"a1", "a2", "a3", "b1", "b2", "c1", "c2", "d1", "f1", "f2", "f3", "g1", "g2", "h1"} {
		require.NoError(t, db.Put(ctx, key, "value"))
	}

	expResultsWithoutBound := [][]string{
		{"a1", "a2", "a3", "b1", "b2"},
		{"c1", "c2", "d1"},
		{"g1", "g2"},
	}

	for bound := 1; bound <= 20; bound++ {
		t.Run(fmt.Sprintf("bound=%d", bound), func(t *testing.T) {

			b := &kv.Batch{}
			b.Header.MaxSpanRequestKeys = int64(bound)
			spans := [][]string{{"a", "c"}, {"c", "e"}, {"g", "h"}}
			for _, span := range spans {
				b.Scan(span[0], span[1])
			}
			require.NoError(t, db.Run(ctx, b))

			require.Equal(t, len(expResultsWithoutBound), len(b.Results))

			expResults := make([][]string, len(expResultsWithoutBound))
			expSatisfied := make(map[int]struct{})
			var count int
		Loop:
			for i, expRes := range expResultsWithoutBound {
				for _, key := range expRes {
					if count == bound {
						break Loop
					}
					expResults[i] = append(expResults[i], key)
					count++
				}
				// NB: only works because requests are sorted and non-overlapping.
				expSatisfied[i] = struct{}{}
			}

			checkScanResults(t, spans, b.Results, expResults, expSatisfied, roachpb.RESUME_KEY_LIMIT, checkOptions{mode: Strict})
		})
	}
}

// Tests multiple scans, forward and reverse, across many ranges with multiple
// bounds, with or without partial results on range boundaries.
func TestMultiRangeBoundedBatchScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db := startNoSplitMergeServer(t)
	defer s.Stopper().Stop(ctx)

	splits := []string{"a", "b", "c", "d", "e", "f"}
	require.NoError(t, setupMultipleRanges(ctx, db, splits...))
	keys := []string{"a1", "a2", "a3", "b1", "b2", "c1", "c2", "d1", "f1", "f2", "f3"}
	for _, key := range keys {
		require.NoError(t, db.Put(ctx, key, "value"))
	}

	scans := [][]string{{"a", "c"}, {"b", "c2"}, {"c", "g"}, {"f1a", "f2a"}}
	// These are the expected results if there is no bound.
	expResults := [][]string{
		{"a1", "a2", "a3", "b1", "b2"},
		{"b1", "b2", "c1"},
		{"c1", "c2", "d1", "f1", "f2", "f3"},
		{"f2"},
	}
	var expResultsReverse [][]string
	for _, res := range expResults {
		var rres []string
		for i := len(res) - 1; i >= 0; i-- {
			rres = append(rres, res[i])
		}
		expResultsReverse = append(expResultsReverse, rres)
	}

	maxExpCount := 0
	for _, res := range expResults {
		maxExpCount += len(res)
	}

	// Compute the `bound` at which each scan is satisfied. We take advantage
	// that, in this test, each scan is satisfied as soon as its last expected
	// results is generated; in other words, the last result for each scan is in
	// the same range as the scan's end key.
	// This loopy code "simulates" the way the DistSender operates in the face of
	// overlapping spans that cross ranges and have key limits: the batch run
	// range by range and, within a range, scans are satisfied in the order in
	// which they appear in the batch.
	satisfiedBoundThreshold := make([]int, len(expResults))
	satisfiedBoundThresholdReverse := make([]int, len(expResults))
	rangeBoundaryThreshold := -1
	rangeBoundaryThresholdReverse := -1
	remaining := make(map[int]int)
	for i := range expResults {
		remaining[i] = len(expResults[i])
	}
	const maxBound int = 20
	var r int
	splits = append([]string{""}, splits...)
	splits = append(splits, "zzzzzz")
	for s := 1; s < len(splits)-1; s++ {
		firstK := sort.SearchStrings(keys, splits[s])
		lastK := sort.SearchStrings(keys, splits[s+1]) - 1
		for j, res := range expResults {
			for _, expK := range res {
				for k := firstK; k <= lastK; k++ {
					if keys[k] == expK {
						r++
						remaining[j]--
						if remaining[j] == 0 {
							satisfiedBoundThreshold[j] = r
						}
						break
					}
				}
			}
		}
		if rangeBoundaryThreshold < 0 {
			rangeBoundaryThreshold = r
		}
	}
	// Compute the thresholds for the reverse scans.
	r = 0
	for i := range expResults {
		remaining[i] = len(expResults[i])
	}
	for s := len(splits) - 1; s > 0; s-- {
		// The split contains keys [lastK..firstK].
		firstK := sort.SearchStrings(keys, splits[s]) - 1
		lastK := sort.SearchStrings(keys, splits[s-1])
		for j, res := range expResultsReverse {
			for expIdx := len(res) - 1; expIdx >= 0; expIdx-- {
				expK := res[expIdx]
				for k := firstK; k >= lastK; k-- {
					if keys[k] == expK {
						r++
						remaining[j]--
						if remaining[j] == 0 {
							satisfiedBoundThresholdReverse[j] = r
						}
						break
					}
				}
			}
		}
		if rangeBoundaryThresholdReverse < 0 {
			rangeBoundaryThresholdReverse = r
		}
	}

	for _, returnOnRangeBoundary := range []bool{false, true} {
		for _, reverse := range []bool{false, true} {
			for bound := 1; bound <= maxBound; bound++ {
				name := fmt.Sprintf("returnOnRangeBoundary=%t, reverse=%t,bound=%d", returnOnRangeBoundary, reverse, bound)
				t.Run(name, func(t *testing.T) {
					b := &kv.Batch{}
					b.Header.MaxSpanRequestKeys = int64(bound)
					b.Header.ReturnOnRangeBoundary = returnOnRangeBoundary

					for _, span := range scans {
						if !reverse {
							b.Scan(span[0], span[1])
						} else {
							b.ReverseScan(span[0], span[1])
						}
					}
					require.NoError(t, db.Run(ctx, b))

					// Compute the range boundary.
					expReason := roachpb.RESUME_KEY_LIMIT
					expCount := maxExpCount
					if bound < maxExpCount {
						expCount = bound
					}
					if returnOnRangeBoundary {
						var threshold int
						if !reverse {
							threshold = rangeBoundaryThreshold
						} else {
							threshold = rangeBoundaryThresholdReverse
						}
						if threshold < expCount {
							expCount = threshold
							expReason = roachpb.RESUME_RANGE_BOUNDARY
						}
					}
					// Compute the satisfied scans.
					expSatisfied := make(map[int]struct{})
					for i := range b.Results {
						var threshold int
						if !reverse {
							threshold = satisfiedBoundThreshold[i]
						} else {
							threshold = satisfiedBoundThresholdReverse[i]
						}
						if expCount >= threshold {
							expSatisfied[i] = struct{}{}
						}
					}
					opt := checkOptions{mode: AcceptPrefix, expCount: expCount}
					if !reverse {
						checkScanResults(
							t, scans, b.Results, expResults, expSatisfied, expReason, opt)
					} else {
						checkReverseScanResults(
							t, scans, b.Results, expResultsReverse, expSatisfied, expReason, opt)
					}

					// Re-query using the resume spans that were returned; check that all
					// spans are read properly.
					if bound < maxExpCount {
						newB := &kv.Batch{}
						for _, res := range b.Results {
							if res.ResumeSpan != nil {
								if !reverse {
									newB.Scan(res.ResumeSpan.Key, res.ResumeSpan.EndKey)
								} else {
									newB.ReverseScan(res.ResumeSpan.Key, res.ResumeSpan.EndKey)
								}
							}
						}
						require.NoError(t, db.Run(ctx, newB))
						// Add the results to the previous results.
						j := 0
						for i, res := range b.Results {
							if res.ResumeSpan != nil {
								b.Results[i].Rows = append(b.Results[i].Rows, newB.Results[j].Rows...)
								b.Results[i].ResumeSpan = newB.Results[j].ResumeSpan
								b.Results[i].ResumeReason = newB.Results[j].ResumeReason
								j++
							}
						}
						for i := range b.Results {
							expSatisfied[i] = struct{}{}
						}
						// Check that the scan results contain all the expected results.
						opt = checkOptions{mode: Strict}
						if !reverse {
							checkScanResults(
								t, scans, b.Results, expResults, expSatisfied, expReason, opt)
						} else {
							checkReverseScanResults(
								t, scans, b.Results, expResultsReverse, expSatisfied, expReason, opt)
						}
					}
				})
			}
		}
	}
}

// TestMultiRangeBoundedBatchScanPartialResponses runs multiple scan requests
// either out-of-order or over overlapping key spans and shows how the batch
// responses can contain partial responses.
func TestMultiRangeBoundedBatchScanPartialResponses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db := startNoSplitMergeServer(t)
	defer s.Stopper().Stop(ctx)

	require.NoError(t, setupMultipleRanges(ctx, db, "a", "b", "c", "d", "e", "f", "g"))
	for _, key := range []string{"a1", "a2", "a3", "b1", "b2", "b3", "c1", "c2", "c3", "e1", "e2", "e3", "f1"} {
		require.NoError(t, db.Put(ctx, key, "value"))
	}

	for _, tc := range []struct {
		name                  string
		bound                 int64
		spans                 [][]string
		returnOnRangeBoundary bool
		expResults            [][]string
		expSatisfied          []int
		expReason             roachpb.ResumeReason
	}{
		{
			name:  "unsorted, non-overlapping, neither satisfied",
			bound: 6,
			spans: [][]string{
				{"b1", "d"}, {"a", "b1"},
			},
			expResults: [][]string{
				{"b1", "b2", "b3"}, {"a1", "a2", "a3"},
			},
		},
		{
			name:  "unsorted, non-overlapping, first satisfied",
			bound: 6,
			spans: [][]string{
				{"b1", "c"}, {"a", "b1"},
			},
			expResults: [][]string{
				{"b1", "b2", "b3"}, {"a1", "a2", "a3"},
			},
			expSatisfied: []int{0},
		},
		{
			name:  "unsorted, non-overlapping, second satisfied",
			bound: 6,
			spans: [][]string{
				{"b1", "d"}, {"a", "b"},
			},
			expResults: [][]string{
				{"b1", "b2", "b3"}, {"a1", "a2", "a3"},
			},
			expSatisfied: []int{1},
		},
		{
			name:  "unsorted, non-overlapping, both satisfied",
			bound: 6,
			spans: [][]string{
				{"b1", "c"}, {"a", "b"},
			},
			expResults: [][]string{
				{"b1", "b2", "b3"}, {"a1", "a2", "a3"},
			},
			expSatisfied: []int{0, 1},
		},
		{
			name:  "sorted, overlapping, neither satisfied",
			bound: 7,
			spans: [][]string{
				{"a", "d"}, {"b", "g"},
			},
			expResults: [][]string{
				{"a1", "a2", "a3", "b1", "b2", "b3"}, {"b1"},
			},
		},
		{
			name:  "sorted, overlapping, first satisfied",
			bound: 7,
			spans: [][]string{
				{"a", "c"}, {"b", "g"},
			},
			expResults: [][]string{
				{"a1", "a2", "a3", "b1", "b2", "b3"}, {"b1"},
			},
			expSatisfied: []int{0},
		},
		{
			name:  "sorted, overlapping, second satisfied",
			bound: 9,
			spans: [][]string{
				{"a", "d"}, {"b", "c"},
			},
			expResults: [][]string{
				{"a1", "a2", "a3", "b1", "b2", "b3"}, {"b1", "b2", "b3"},
			},
			expSatisfied: []int{1},
		},
		{
			name:  "sorted, overlapping, both satisfied",
			bound: 9,
			spans: [][]string{
				{"a", "c"}, {"b", "c"},
			},
			expResults: [][]string{
				{"a1", "a2", "a3", "b1", "b2", "b3"}, {"b1", "b2", "b3"},
			},
			expSatisfied: []int{0, 1},
		},
		{
			name:  "unsorted, overlapping, neither satisfied",
			bound: 7,
			spans: [][]string{
				{"b", "g"}, {"a", "d"},
			},
			expResults: [][]string{
				{"b1", "b2", "b3"}, {"a1", "a2", "a3", "b1"},
			},
		},
		{
			name:  "unsorted, overlapping, first satisfied",
			bound: 7,
			spans: [][]string{
				{"b", "c"}, {"a", "d"},
			},
			expResults: [][]string{
				{"b1", "b2", "b3"}, {"a1", "a2", "a3", "b1"},
			},
			expSatisfied: []int{0},
		},
		{
			name:  "unsorted, overlapping, second satisfied",
			bound: 7,
			spans: [][]string{
				{"b", "g"}, {"a", "b2"},
			},
			expResults: [][]string{
				{"b1", "b2", "b3"}, {"a1", "a2", "a3", "b1"},
			},
			expSatisfied: []int{1},
		},
		{
			name:  "unsorted, overlapping, both satisfied",
			bound: 7,
			spans: [][]string{
				{"b", "c"}, {"a", "b2"},
			},
			expResults: [][]string{
				{"b1", "b2", "b3"}, {"a1", "a2", "a3", "b1"},
			},
			expSatisfied: []int{0, 1},
		},
		{
			name:  "unsorted, overlapping, unreached",
			bound: 7,
			spans: [][]string{
				{"b", "g"}, {"c", "f"}, {"a", "d"},
			},
			expResults: [][]string{
				{"b1", "b2", "b3"}, {}, {"a1", "a2", "a3", "b1"},
			},
		},
		{
			name: "range boundary, overlapping, unbounded",
			spans: [][]string{
				{"d", "g"}, {"e2", "g"},
			},
			returnOnRangeBoundary: true,
			expResults: [][]string{
				{"e1", "e2", "e3"}, {"e2", "e3"},
			},
			expReason: roachpb.RESUME_RANGE_BOUNDARY,
		},
		{
			name:  "range boundary, overlapping, neither satisfied",
			bound: 10,
			spans: [][]string{
				{"d", "f2"}, {"e2", "g"},
			},
			returnOnRangeBoundary: true,
			expResults: [][]string{
				{"e1", "e2", "e3"}, {"e2", "e3"},
			},
			expReason: roachpb.RESUME_RANGE_BOUNDARY,
		},
		{
			name:  "range boundary, overlapping, neither satisfied, bounded below boundary",
			bound: 4,
			spans: [][]string{
				{"d", "f2"}, {"e2", "g"},
			},
			returnOnRangeBoundary: true,
			expResults: [][]string{
				{"e1", "e2", "e3"}, {"e2"},
			},
			expReason: roachpb.RESUME_KEY_LIMIT,
		},
		{
			name:  "range boundary, overlapping, neither satisfied, bounded at boundary",
			bound: 5,
			spans: [][]string{
				{"d", "f2"}, {"e2", "g"},
			},
			returnOnRangeBoundary: true,
			expResults: [][]string{
				{"e1", "e2", "e3"}, {"e2", "e3"},
			},
			expReason: roachpb.RESUME_KEY_LIMIT,
		},
		{
			name:  "range boundary, overlapping, neither satisfied, bounded above boundary",
			bound: 6,
			spans: [][]string{
				{"d", "f2"}, {"e2", "g"},
			},
			returnOnRangeBoundary: true,
			expResults: [][]string{
				{"e1", "e2", "e3"}, {"e2", "e3"},
			},
			expReason: roachpb.RESUME_RANGE_BOUNDARY,
		},
		{
			name:  "range boundary, non-overlapping, first satisfied",
			bound: 10,
			spans: [][]string{
				{"d", "e3"}, {"f", "g"},
			},
			returnOnRangeBoundary: true,
			expResults: [][]string{
				{"e1", "e2"}, {},
			},
			expSatisfied: []int{0},
			expReason:    roachpb.RESUME_RANGE_BOUNDARY,
		},
		{
			name:  "range boundary, non-overlapping, second satisfied",
			bound: 10,
			spans: [][]string{
				{"e3", "g"}, {"e", "e2"},
			},
			returnOnRangeBoundary: true,
			expResults: [][]string{
				{"e3"}, {"e1"},
			},
			expSatisfied: []int{1},
			expReason:    roachpb.RESUME_RANGE_BOUNDARY,
		},
		{
			name:  "range boundary, non-overlapping, both satisfied",
			bound: 10,
			spans: [][]string{
				{"e2", "f"}, {"d", "e2"},
			},
			returnOnRangeBoundary: true,
			expResults: [][]string{
				{"e2", "e3"}, {"e1"},
			},
			expSatisfied: []int{0, 1},
		},
		{
			name:  "range boundary, separate ranges, none satisfied",
			bound: 10,
			spans: [][]string{
				{"c", "e"}, {"e", "f"},
			},
			returnOnRangeBoundary: true,
			expResults: [][]string{
				{"c1", "c2", "c3"}, {},
			},
			expReason: roachpb.RESUME_RANGE_BOUNDARY,
		},
		{
			name:  "range boundary, separate ranges, first satisfied",
			bound: 10,
			spans: [][]string{
				{"c", "d"}, {"e", "f"},
			},
			returnOnRangeBoundary: true,
			expResults: [][]string{
				{"c1", "c2", "c3"}, {},
			},
			expSatisfied: []int{0},
			expReason:    roachpb.RESUME_RANGE_BOUNDARY,
		},
		{
			name:  "range boundary, separate ranges, second satisfied",
			bound: 10,
			spans: [][]string{
				{"e", "f"}, {"c2", "d"},
			},
			returnOnRangeBoundary: true,
			expResults: [][]string{
				{}, {"c2", "c3"},
			},
			expSatisfied: []int{1},
			expReason:    roachpb.RESUME_RANGE_BOUNDARY,
		},
		{
			name:  "range boundary, separate ranges, first empty",
			bound: 10,
			spans: [][]string{
				{"d", "e"}, {"f", "h"},
			},
			returnOnRangeBoundary: true,
			expResults: [][]string{
				{}, {"f1"},
			},
			expSatisfied: []int{0},
			expReason:    roachpb.RESUME_RANGE_BOUNDARY,
		},
		{
			name:  "range boundary, separate ranges, second empty",
			bound: 10,
			spans: [][]string{
				{"f", "h"}, {"d", "e"},
			},
			returnOnRangeBoundary: true,
			expResults: [][]string{
				{"f1"}, {},
			},
			expSatisfied: []int{1},
			expReason:    roachpb.RESUME_RANGE_BOUNDARY,
		},
		{
			name:  "range boundary, separate ranges, all empty",
			bound: 10,
			spans: [][]string{
				{"d", "d3"}, {"f3", "h"}, {"a", "a1"}, {"c4", "e"},
			},
			returnOnRangeBoundary: true,
			expResults: [][]string{
				{}, {}, {}, {},
			},
			expSatisfied: []int{0, 1, 2, 3, 4},
		},
		{
			name:  "range boundary, separate ranges, most empty",
			bound: 10,
			spans: [][]string{
				{"d", "d3"}, {"f3", "g"}, {"a", "a1"}, {"c4", "e"}, {"f", "h"}, {"e4", "g"},
			},
			returnOnRangeBoundary: true,
			expResults: [][]string{
				{}, {}, {}, {}, {"f1"}, {"f1"},
			},
			expSatisfied: []int{0, 1, 2, 3, 5},
			expReason:    roachpb.RESUME_RANGE_BOUNDARY,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &kv.Batch{}
			b.Header.MaxSpanRequestKeys = tc.bound
			b.Header.ReturnOnRangeBoundary = tc.returnOnRangeBoundary
			for _, span := range tc.spans {
				b.Scan(span[0], span[1])
			}
			require.NoError(t, db.Run(ctx, b))

			expSatisfied := make(map[int]struct{})
			for _, exp := range tc.expSatisfied {
				expSatisfied[exp] = struct{}{}
			}
			opts := checkOptions{mode: Strict}
			expReason := roachpb.RESUME_KEY_LIMIT
			if tc.expReason != roachpb.RESUME_UNKNOWN {
				expReason = tc.expReason
			}
			checkScanResults(t, tc.spans, b.Results, tc.expResults, expSatisfied, expReason, opts)
		})
	}
}

// Test that a bounded range delete request that gets terminated at a range
// boundary uses the range boundary as the start key in the response ResumeSpan.
func TestMultiRangeBoundedBatchScanBoundary(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _ := startNoSplitMergeServer(t)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	db := s.DB()
	if err := setupMultipleRanges(ctx, db, "a", "b"); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a1", "a2", "a3", "b1", "b2"} {
		if err := db.Put(ctx, key, "value"); err != nil {
			t.Fatal(err)
		}
	}

	b := &kv.Batch{}
	b.Header.MaxSpanRequestKeys = 3
	b.Scan("a", "c")
	if err := db.Run(ctx, b); err != nil {
		t.Fatal(err)
	}
	if len(b.Results) != 1 {
		t.Fatalf("%d results returned", len(b.Results))
	}
	if string(b.Results[0].ResumeSpan.Key) != "b" || string(b.Results[0].ResumeSpan.EndKey) != "c" {
		t.Fatalf("received ResumeSpan %+v", b.Results[0].ResumeSpan)
	}

	b = &kv.Batch{}
	b.Header.MaxSpanRequestKeys = 1
	b.Scan("b", "c")
	if err := db.Run(ctx, b); err != nil {
		t.Fatal(err)
	}
	if len(b.Results) != 1 {
		t.Fatalf("%d results returned", len(b.Results))
	}
	if string(b.Results[0].ResumeSpan.Key) != "b2" || string(b.Results[0].ResumeSpan.EndKey) != "c" {
		t.Fatalf("received ResumeSpan %+v", b.Results[0].ResumeSpan)
	}
}

// TestMultiRangeEmptyAfterTruncate exercises a code path in which a
// multi-range request deals with a range without any active requests after
// truncation. In that case, the request is skipped.
func TestMultiRangeEmptyAfterTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _ := startNoSplitMergeServer(t)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	db := s.DB()
	if err := setupMultipleRanges(ctx, db, "c", "d"); err != nil {
		t.Fatal(err)
	}

	// Delete the keys within a transaction. The range [c,d) doesn't have
	// any active requests.
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		b.DelRange("a", "b", false /* returnKeys */)
		b.DelRange("e", "f", false /* returnKeys */)
		return txn.CommitInBatch(ctx, b)
	}); err != nil {
		t.Fatalf("unexpected error on transactional DeleteRange: %s", err)
	}
}

// TestMultiRequestBatchWithFwdAndReverseRequests are disallowed.
func TestMultiRequestBatchWithFwdAndReverseRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _ := startNoSplitMergeServer(t)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	db := s.DB()
	if err := setupMultipleRanges(ctx, db, "a", "b"); err != nil {
		t.Fatal(err)
	}
	b := &kv.Batch{}
	b.Header.MaxSpanRequestKeys = 100
	b.Scan("a", "b")
	b.ReverseScan("a", "b")
	if err := db.Run(ctx, b); !testutils.IsError(
		err, "batch with limit contains both forward and reverse scans",
	) {
		t.Fatal(err)
	}
}

// TestMultiRangeScanReverseScanDeleteResolve verifies that Scan, ReverseScan,
// DeleteRange and ResolveIntentRange work across ranges.
func TestMultiRangeScanReverseScanDeleteResolve(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _ := startNoSplitMergeServer(t)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	db := s.DB()
	if err := setupMultipleRanges(ctx, db, "b"); err != nil {
		t.Fatal(err)
	}

	// Write keys before, at, and after the split key.
	for _, key := range []string{"a", "b", "c"} {
		if err := db.Put(ctx, key, "value"); err != nil {
			t.Fatal(err)
		}
	}
	// Scan to retrieve the keys just written.
	if rows, err := db.Scan(ctx, "a", "q", 0); err != nil {
		t.Fatalf("unexpected error on Scan: %s", err)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}

	// Scan in reverse order to retrieve the keys just written.
	if rows, err := db.ReverseScan(ctx, "a", "q", 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}

	// Delete the keys within a transaction. Implicitly, the intents are
	// resolved via ResolveIntentRange upon completion.
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		b.DelRange("a", "d", false /* returnKeys */)
		return txn.CommitInBatch(ctx, b)
	}); err != nil {
		t.Fatalf("unexpected error on transactional DeleteRange: %s", err)
	}

	// Scan consistently to make sure the intents are gone.
	if rows, err := db.Scan(ctx, "a", "q", 0); err != nil {
		t.Fatalf("unexpected error on Scan: %s", err)
	} else if l := len(rows); l != 0 {
		t.Errorf("expected 0 rows; got %d", l)
	}

	// ReverseScan consistently to make sure the intents are gone.
	if rows, err := db.ReverseScan(ctx, "a", "q", 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 0 {
		t.Errorf("expected 0 rows; got %d", l)
	}
}

// TestMultiRangeScanReverseScanInconsistent verifies that a Scan/ReverseScan
// across ranges that doesn't require read consistency will set a timestamp
// using the clock local to the distributed sender.
func TestMultiRangeScanReverseScanInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, rc := range []roachpb.ReadConsistencyType{
		roachpb.READ_UNCOMMITTED,
		roachpb.INCONSISTENT,
	} {
		t.Run(rc.String(), func(t *testing.T) {
			s, _ := startNoSplitMergeServer(t)
			ctx := context.Background()
			defer s.Stopper().Stop(ctx)
			db := s.DB()
			if err := setupMultipleRanges(ctx, db, "b"); err != nil {
				t.Fatal(err)
			}

			// Write keys "a" and "b", the latter of which is the first key in the
			// second range.
			keys := [2]string{"a", "b"}
			ts := [2]hlc.Timestamp{}
			for i, key := range keys {
				b := &kv.Batch{}
				b.Put(key, "value")
				if err := db.Run(ctx, b); err != nil {
					t.Fatal(err)
				}
				ts[i] = s.Clock().Now()
				log.Infof(ctx, "%d: %s %s", i, key, ts[i])
				if i == 0 {
					testutils.SucceedsSoon(t, func() error {
						// Enforce that when we write the second key, it's written
						// with a strictly higher timestamp. We're dropping logical
						// ticks and the clock may just have been pushed into the
						// future, so that's necessary. See #3122.
						if ts[0].WallTime >= s.Clock().Now().WallTime {
							return errors.New("time stands still")
						}
						return nil
					})
				}
			}

			// Do an inconsistent Scan/ReverseScan from a new DistSender and verify
			// it does the read at its local clock and doesn't receive an
			// OpRequiresTxnError. We set the local clock to the timestamp of
			// just above the first key to verify it's used to read only key "a".
			for i, request := range []roachpb.Request{
				roachpb.NewScan(roachpb.Key("a"), roachpb.Key("c"), false),
				roachpb.NewReverseScan(roachpb.Key("a"), roachpb.Key("c"), false),
			} {
				manual := hlc.NewManualClock(ts[0].WallTime + 1)
				clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
				ds := kvcoord.NewDistSender(kvcoord.DistSenderConfig{
					AmbientCtx:         s.AmbientCtx(),
					Settings:           cluster.MakeTestingClusterSettings(),
					Clock:              clock,
					NodeDescs:          s.Gossip(),
					RPCContext:         s.RPCContext(),
					NodeDialer:         nodedialer.New(s.RPCContext(), gossip.AddressResolver(s.Gossip())),
					FirstRangeProvider: s.Gossip(),
				})

				reply, err := kv.SendWrappedWith(context.Background(), ds, roachpb.Header{
					ReadConsistency: rc,
				}, request)
				if err != nil {
					t.Fatal(err)
				}

				var rows []roachpb.KeyValue
				switch r := reply.(type) {
				case *roachpb.ScanResponse:
					rows = r.Rows
				case *roachpb.ReverseScanResponse:
					rows = r.Rows
				default:
					t.Fatalf("unexpected response %T: %v", reply, reply)
				}

				if l := len(rows); l != 1 {
					t.Fatalf("%d: expected 1 row; got %d\n%v", i, l, rows)
				}
				if key := string(rows[0].Key); keys[0] != key {
					t.Errorf("expected key %q; got %q", keys[0], key)
				}
			}
		})
	}
}

// TestMultiRangeScanDeleteRange tests that commands which access multiple
// ranges are carried out properly.
func TestMultiRangeScanDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tds := db.NonTransactionalSender()

	if err := db.AdminSplit(ctx, "m", hlc.MaxTimestamp /* expirationTime */); err != nil {
		t.Fatal(err)
	}
	writes := []roachpb.Key{roachpb.Key("a"), roachpb.Key("z")}
	get := &roachpb.GetRequest{
		RequestHeader: roachpb.RequestHeader{Key: writes[0]},
	}
	get.EndKey = writes[len(writes)-1]
	if _, err := kv.SendWrapped(ctx, tds, get); err == nil {
		t.Errorf("able to call Get with a key range: %v", get)
	}
	var delTS hlc.Timestamp
	for i, k := range writes {
		put := roachpb.NewPut(k, roachpb.MakeValueFromBytes(k))
		if _, err := kv.SendWrapped(ctx, tds, put); err != nil {
			t.Fatal(err)
		}
		scan := roachpb.NewScan(writes[0], writes[len(writes)-1].Next(), false)
		reply, err := kv.SendWrapped(ctx, tds, scan)
		if err != nil {
			t.Fatal(err)
		}
		sr := reply.(*roachpb.ScanResponse)
		if sr.Txn != nil {
			// This was the other way around at some point in the past.
			// Same below for Delete, etc.
			t.Errorf("expected no transaction in response header")
		}
		if rows := sr.Rows; len(rows) != i+1 {
			t.Fatalf("expected %d rows, but got %d", i+1, len(rows))
		}
	}

	del := &roachpb.DeleteRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    writes[0],
			EndKey: writes[len(writes)-1].Next(),
		},
		ReturnKeys: true,
	}
	reply, err := kv.SendWrappedWith(ctx, tds, roachpb.Header{Timestamp: delTS}, del)
	if err != nil {
		t.Fatal(err)
	}
	dr := reply.(*roachpb.DeleteRangeResponse)
	if dr.Txn != nil {
		t.Errorf("expected no transaction in response header")
	}
	if !reflect.DeepEqual(dr.Keys, writes) {
		t.Errorf("expected %d keys to be deleted, but got %d instead", writes, dr.Keys)
	}

	now := s.Clock().NowAsClockTimestamp()
	txnProto := roachpb.MakeTransaction("MyTxn", nil, 0, now.ToTimestamp(), 0, int32(s.SQLInstanceID()))
	txn := kv.NewTxnFromProto(ctx, db, s.NodeID(), now, kv.RootTxn, &txnProto)

	scan := roachpb.NewScan(writes[0], writes[len(writes)-1].Next(), false)
	ba := roachpb.BatchRequest{}
	ba.Header = roachpb.Header{Txn: &txnProto}
	ba.Add(scan)
	br, pErr := txn.Send(ctx, ba)
	if pErr != nil {
		t.Fatal(err)
	}
	replyTxn := br.Txn
	if replyTxn == nil || replyTxn.Name != "MyTxn" {
		t.Errorf("wanted Txn to persist, but it changed to %v", txn)
	}
	sr := br.Responses[0].GetInner().(*roachpb.ScanResponse)
	if rows := sr.Rows; len(rows) > 0 {
		t.Fatalf("scan after delete returned rows: %v", rows)
	}
}

// TestMultiRangeScanWithPagination tests that specifying MaxSpanResultKeys
// and/or TargetBytes to break up result sets works properly, even across
// ranges.
func TestMultiRangeScanWithPagination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	keys := func(strs ...string) []roachpb.Key {
		r := make([]roachpb.Key, len(strs))
		for i, s := range strs {
			r[i] = roachpb.Key(s)
		}
		return r
	}

	scan := func(startKey, endKey string) roachpb.Span {
		return roachpb.Span{
			Key:    roachpb.Key(startKey),
			EndKey: roachpb.Key(endKey).Next(),
		}
	}

	get := func(key string) roachpb.Span {
		return roachpb.Span{
			Key: roachpb.Key(key),
		}
	}

	// Each testcase defines range split keys, a set of keys, and a set of
	// operations (scans / gets) that together should return all the keys in
	// order.
	testCases := []struct {
		splitKeys []roachpb.Key
		keys      []roachpb.Key
		// An operation is either a Scan or a Get, depending if the span EndKey is
		// set.
		operations []roachpb.Span
	}{
		{
			splitKeys:  keys(),
			keys:       keys("a", "j", "z"),
			operations: []roachpb.Span{scan("a", "z")},
		},

		{
			splitKeys:  keys("m"),
			keys:       keys("a", "z"),
			operations: []roachpb.Span{scan("a", "z")},
		},

		{
			splitKeys:  keys("h", "q"),
			keys:       keys("b", "f", "k", "r", "w", "y"),
			operations: []roachpb.Span{scan("b", "y")},
		},

		{
			splitKeys:  keys("h", "q"),
			keys:       keys("b", "f", "k", "r", "w", "y"),
			operations: []roachpb.Span{scan("b", "k"), scan("p", "z")},
		},

		{
			// This test mixes Scans and Gets, used to make sure that the ResumeSpan
			// is set correctly for Gets that were not performed (see #74736).
			splitKeys:  keys("c", "f"),
			keys:       keys("a", "b", "c", "d", "e", "f", "g", "h", "i"),
			operations: []roachpb.Span{scan("a", "d"), get("d1"), get("e"), scan("f", "h"), get("i")},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			ctx := context.Background()
			s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
			defer s.Stopper().Stop(ctx)
			tds := db.NonTransactionalSender()

			for _, sk := range tc.splitKeys {
				if err := db.AdminSplit(ctx, sk, hlc.MaxTimestamp /* expirationTime */); err != nil {
					t.Fatal(err)
				}
			}

			for _, k := range tc.keys {
				put := roachpb.NewPut(k, roachpb.MakeValueFromBytes(k))
				if _, err := kv.SendWrapped(ctx, tds, put); err != nil {
					t.Fatal(err)
				}
			}

			// The maximum TargetBytes to use in this test. We use the bytes in
			// all kvs in this test case as a ceiling. Nothing interesting
			// happens above this.
			var maxTargetBytes int64
			{
				scan := roachpb.NewScan(tc.keys[0], tc.keys[len(tc.keys)-1].Next(), false)
				resp, pErr := kv.SendWrapped(ctx, tds, scan)
				require.Nil(t, pErr)
				require.Nil(t, resp.Header().ResumeSpan)
				require.EqualValues(t, len(tc.keys), resp.Header().NumKeys)
				maxTargetBytes = resp.Header().NumBytes
			}

			testutils.RunTrueAndFalse(t, "reverse", func(t *testing.T, reverse bool) {
				testutils.RunTrueAndFalse(t, "returnOnRangeBoundary", func(t *testing.T, returnOnRangeBoundary bool) {
					// Iterate through MaxSpanRequestKeys=1..n and TargetBytes=1..m
					// and (where n and m are chosen to reveal the full result set
					// in one page). At each(*) combination, paginate both the
					// forward and reverse scan and make sure we get the right
					// result.
					//
					// (*) we don't increase the limits when there's only one page,
					// but short circuit to something more interesting instead.
					msrq := int64(1)
					for targetBytes := int64(1); ; targetBytes++ {
						var numPages int
						t.Run(fmt.Sprintf("targetBytes=%d,maxSpanRequestKeys=%d", targetBytes, msrq), func(t *testing.T) {

							// Paginate.
							operations := tc.operations
							if reverse {
								operations = make([]roachpb.Span, len(operations))
								for i := range operations {
									operations[i] = tc.operations[len(operations)-i-1]
								}
							}
							var keys []roachpb.Key
							for {
								numPages++

								// Build the batch.
								var ba roachpb.BatchRequest
								for _, span := range operations {
									var req roachpb.Request
									switch {
									case span.EndKey == nil:
										req = roachpb.NewGet(span.Key, false /* forUpdate */)
									case reverse:
										req = roachpb.NewReverseScan(span.Key, span.EndKey, false /* forUpdate */)
									default:
										req = roachpb.NewScan(span.Key, span.EndKey, false /* forUpdate */)
									}
									ba.Add(req)
								}

								ba.Header.TargetBytes = targetBytes
								ba.Header.MaxSpanRequestKeys = msrq
								ba.Header.ReturnOnRangeBoundary = returnOnRangeBoundary
								br, pErr := tds.Send(ctx, ba)
								require.Nil(t, pErr)
								for i := range operations {
									resp := br.Responses[i]
									if getResp := resp.GetGet(); getResp != nil {
										if getResp.Value != nil {
											keys = append(keys, operations[i].Key)
										}
										continue
									}
									var rows []roachpb.KeyValue
									if reverse {
										rows = resp.GetReverseScan().Rows
									} else {
										rows = resp.GetScan().Rows
									}
									for _, kv := range rows {
										keys = append(keys, kv.Key)
									}
								}
								operations = nil
								for _, resp := range br.Responses {
									if resumeSpan := resp.GetInner().Header().ResumeSpan; resumeSpan != nil {
										operations = append(operations, *resumeSpan)
									}
								}
								if len(operations) == 0 {
									// Done with this pagination.
									break
								}
							}
							if reverse {
								for i, n := 0, len(keys); i < n-i-1; i++ {
									keys[i], keys[n-i-1] = keys[n-i-1], keys[i]
								}
							}
							require.Equal(t, tc.keys, keys)
							if returnOnRangeBoundary {
								// Definitely more pages than splits.
								require.Greater(t, numPages, len(tc.splitKeys))
							}
							if targetBytes == 1 || msrq < int64(len(tc.keys)) {
								// Definitely more than one page in this case.
								require.Greater(t, numPages, 1)
							}
							if !returnOnRangeBoundary && targetBytes >= maxTargetBytes && msrq >= int64(len(tc.keys)) {
								// Definitely one page if limits are larger than result set.
								require.Equal(t, 1, numPages)
							}
						})
						if targetBytes >= maxTargetBytes || numPages == 1 {
							if msrq >= int64(len(tc.keys)) {
								return
							}
							targetBytes = 0
							msrq++
						}
					}
				})
			})
		})
	}
}

// TestParallelSender splits the keyspace 10 times and verifies that a
// scan across all and 10 puts to each range both use parallelizing
// dist sender.
func TestParallelSender(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := startNoSplitMergeServer(t)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	// Split into multiple ranges.
	splitKeys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	for _, key := range splitKeys {
		if err := db.AdminSplit(context.Background(), key, hlc.MaxTimestamp /* expirationTime */); err != nil {
			t.Fatal(err)
		}
	}

	getPSCount := func() int64 {
		return s.DistSenderI().(*kvcoord.DistSender).Metrics().AsyncSentCount.Count()
	}
	psCount := getPSCount()

	// Batch writes to each range.
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		for _, key := range splitKeys {
			b.Put(key, "val")
		}
		return txn.CommitInBatch(ctx, b)
	}); err != nil {
		t.Errorf("unexpected error on batch put: %s", err)
	}
	newPSCount := getPSCount()
	if c := newPSCount - psCount; c < 9 {
		t.Errorf("expected at least 9 parallel sends; got %d", c)
	}
	psCount = newPSCount

	// Scan across all rows.
	if rows, err := db.Scan(context.Background(), "a", "z", 0); err != nil {
		t.Fatalf("unexpected error on Scan: %s", err)
	} else if l := len(rows); l != len(splitKeys) {
		t.Fatalf("expected %d rows; got %d", len(splitKeys), l)
	}
	newPSCount = getPSCount()
	if c := newPSCount - psCount; c < 9 {
		t.Errorf("expected at least 9 parallel sends; got %d", c)
	}
}

func initReverseScanTestEnv(s serverutils.TestServerInterface, t *testing.T) *kv.DB {
	db := s.DB()

	// Set up multiple ranges:
	// ["", "b"),["b", "e") ,["e", "g") and ["g", "\xff\xff").
	for _, key := range []string{"b", "e", "g"} {
		// Split the keyspace at the given key.
		if err := db.AdminSplit(context.Background(), key, hlc.MaxTimestamp /* expirationTime */); err != nil {
			t.Fatal(err)
		}
	}
	// Write keys before, at, and after the split key.
	for _, key := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		if err := db.Put(context.Background(), key, "value"); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// TestSingleRangeReverseScan verifies that ReverseScan gets the right results
// on a single range.
func TestSingleRangeReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _ := startNoSplitMergeServer(t)
	defer s.Stopper().Stop(context.Background())
	db := initReverseScanTestEnv(s, t)
	ctx := context.Background()

	// Case 1: Request.EndKey is in the middle of the range.
	if rows, err := db.ReverseScan(ctx, "b", "d", 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 2 {
		t.Errorf("expected 2 rows; got %d", l)
	}
	if rows, err := db.ReverseScan(ctx, "b", "d", 1); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 1 {
		t.Errorf("expected 1 rows; got %d", l)
	}

	// Case 2: Request.EndKey is equal to the EndKey of the range.
	if rows, pErr := db.ReverseScan(ctx, "e", "g", 0); pErr != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", pErr)
	} else if l := len(rows); l != 2 {
		t.Errorf("expected 2 rows; got %d", l)
	}
	// Case 3: Test roachpb.TableDataMin. Expected to return "g" and "h".
	wanted := 2
	if rows, pErr := db.ReverseScan(ctx, "g", keys.TableDataMin, 0); pErr != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", pErr)
	} else if l := len(rows); l != wanted {
		t.Errorf("expected %d rows; got %d", wanted, l)
	}
	// Case 4: Test keys.SystemMax
	// This span covers the system DB keys. Note sql.GetInitialSystemValues
	// returns one key before keys.SystemMax, but our scan is including one key
	// (\xffa) created for the test.
	if rows, pErr := db.ReverseScan(ctx, keys.SystemMax, "b", 0); pErr != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", pErr)
	} else if l := len(rows); l != 1 {
		t.Errorf("expected 1 row; got %d", l)
	}
}

// TestMultiRangeReverseScan verifies that ReverseScan gets the right results
// across multiple ranges.
func TestMultiRangeReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _ := startNoSplitMergeServer(t)
	defer s.Stopper().Stop(context.Background())
	db := initReverseScanTestEnv(s, t)
	ctx := context.Background()

	// Case 1: Request.EndKey is in the middle of the range.
	if rows, pErr := db.ReverseScan(ctx, "a", "d", 0); pErr != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", pErr)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}
	if rows, pErr := db.ReverseScan(ctx, "a", "d", 2); pErr != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", pErr)
	} else if l := len(rows); l != 2 {
		t.Errorf("expected 2 rows; got %d", l)
	}
	// Case 2: Request.EndKey is equal to the EndKey of the range.
	if rows, pErr := db.ReverseScan(ctx, "d", "g", 0); pErr != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", pErr)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}
}

// TestBatchPutWithConcurrentSplit creates a batch with a series of put
// requests and splits the middle of the range in order to trigger
// reentrant invocation of DistSender.divideAndSendBatchToRanges. See
// #12603 for more details.
func TestBatchPutWithConcurrentSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := startNoSplitMergeServer(t)
	defer s.Stopper().Stop(context.Background())

	// Split first using the default client and scan to make sure that
	// the range descriptor cache reflects the split.
	for _, key := range []string{"b", "f"} {
		if err := db.AdminSplit(context.Background(), key, hlc.MaxTimestamp /* expirationTime */); err != nil {
			t.Fatal(err)
		}
	}
	if rows, err := db.Scan(context.Background(), "a", "z", 0); err != nil {
		t.Fatal(err)
	} else if l := len(rows); l != 0 {
		t.Fatalf("expected empty keyspace; got %d rows", l)
	}

	// Now, split further at the given keys, but use a new dist sender so
	// we don't update the caches on the default dist sender-backed client.
	ds := kvcoord.NewDistSender(kvcoord.DistSenderConfig{
		AmbientCtx:         s.AmbientCtx(),
		Clock:              s.Clock(),
		NodeDescs:          s.Gossip(),
		RPCContext:         s.RPCContext(),
		NodeDialer:         nodedialer.New(s.RPCContext(), gossip.AddressResolver(s.Gossip())),
		Settings:           cluster.MakeTestingClusterSettings(),
		FirstRangeProvider: s.Gossip(),
	})
	for _, key := range []string{"c"} {
		req := &roachpb.AdminSplitRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: roachpb.Key(key),
			},
			SplitKey:       roachpb.Key(key),
			ExpirationTime: hlc.MaxTimestamp,
		}
		if _, err := kv.SendWrapped(context.Background(), ds, req); err != nil {
			t.Fatal(err)
		}
	}

	// Execute a batch on the default sender. Since its cache will not
	// have been updated to reflect the new splits, it will discover
	// them partway through and need to reinvoke divideAndSendBatchToRanges.
	b := &kv.Batch{}
	for i, key := range []string{"a1", "b1", "c1", "d1", "f1"} {
		b.Put(key, fmt.Sprintf("value-%d", i))
	}
	if err := db.Run(context.Background(), b); err != nil {
		t.Fatal(err)
	}
}

// TestReverseScanWithSplitAndMerge verifies that ReverseScan gets the right results
// across multiple ranges while range splits and merges happen.
func TestReverseScanWithSplitAndMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _ := startNoSplitMergeServer(t)
	defer s.Stopper().Stop(context.Background())
	db := initReverseScanTestEnv(s, t)

	// Case 1: An encounter with a range split.
	// Split the range ["b", "e") at "c".
	if err := db.AdminSplit(context.Background(), "c", hlc.MaxTimestamp /* expirationTime */); err != nil {
		t.Fatal(err)
	}

	// The ReverseScan will run into a stale descriptor.
	if rows, err := db.ReverseScan(context.Background(), "a", "d", 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}

	// Case 2: encounter with range merge .
	// Merge the range ["e", "g") and ["g", "\xff\xff") .
	if err := db.AdminMerge(context.Background(), "e"); err != nil {
		t.Fatal(err)
	}
	if rows, err := db.ReverseScan(context.Background(), "d", "g", 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}
}

func TestBadRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db := startNoSplitMergeServer(t)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	// Write key "a".
	if err := db.Put(ctx, "a", "value"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Scan(ctx, "a", "a", 0); !testutils.IsError(err, "must be greater than start") {
		t.Fatalf("unexpected error on scan with startkey == endkey: %v", err)
	}

	if _, err := db.ReverseScan(ctx, "a", "a", 0); !testutils.IsError(err, "must be greater than start") {
		t.Fatalf("unexpected error on reverse scan with startkey == endkey: %v", err)
	}

	if _, err := db.DelRange(ctx, "x", "a", false); !testutils.IsError(err, "must be greater than start") {
		t.Fatalf("unexpected error on deletion on [x, a): %v", err)
	}

	// To make the last check fail we need to search the replica that starts at
	// KeyMin i.e. typically it's Range(1).
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	repl := store.LookupReplica(roachpb.RKeyMin)
	if _, err := db.DelRange(ctx, "", repl.Desc().EndKey, false); !testutils.IsError(err, "must be greater than LocalMax") {
		t.Fatalf("unexpected error on deletion on [KeyMin, %s): %v", repl.Desc().EndKey, err)
	}
}

// TestPropagateTxnOnError verifies that DistSender.Send properly propagates the
// txn data to a next iteration. The test uses the txn.ObservedTimestamps field
// to verify that.
func TestPropagateTxnOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Inject these two observed timestamps into the parts of the batch's
	// response that does not result in an error. Even though the batch as a
	// whole results in an error, the transaction should still propagate this
	// information.
	ot1 := roachpb.ObservedTimestamp{NodeID: 7, Timestamp: hlc.ClockTimestamp{WallTime: 15}}
	ot2 := roachpb.ObservedTimestamp{NodeID: 8, Timestamp: hlc.ClockTimestamp{WallTime: 16}}
	containsObservedTSs := func(txn *roachpb.Transaction) bool {
		contains := func(ot roachpb.ObservedTimestamp) bool {
			for _, ts := range txn.ObservedTimestamps {
				if ts == ot {
					return true
				}
			}
			return false
		}
		return contains(ot1) && contains(ot2)
	}

	// Set up a filter to so that the first CPut operation will
	// get a ReadWithinUncertaintyIntervalError and so that the
	// Put operations on either side of the CPut will each return
	// with the new observed timestamp.
	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")
	var numCPuts int32
	var storeKnobs kvserver.StoreTestingKnobs
	storeKnobs.EvalKnobs.TestingEvalFilter =
		func(fArgs kvserverbase.FilterArgs) *roachpb.Error {
			k := fArgs.Req.Header().Key
			switch fArgs.Req.(type) {
			case *roachpb.PutRequest:
				if k.Equal(keyA) {
					fArgs.Hdr.Txn.UpdateObservedTimestamp(ot1.NodeID, ot1.Timestamp)
				} else if k.Equal(keyC) {
					fArgs.Hdr.Txn.UpdateObservedTimestamp(ot2.NodeID, ot2.Timestamp)
				}
			case *roachpb.ConditionalPutRequest:
				if k.Equal(keyB) {
					if atomic.AddInt32(&numCPuts, 1) == 1 {
						pErr := roachpb.NewReadWithinUncertaintyIntervalError(hlc.Timestamp{}, hlc.Timestamp{}, hlc.Timestamp{}, nil)
						return roachpb.NewErrorWithTxn(pErr, fArgs.Hdr.Txn)
					}
				}
			}
			return nil
		}

	s, _, _ := serverutils.StartServer(t,
		base.TestServerArgs{Knobs: base.TestingKnobs{Store: &storeKnobs}})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	db := s.DB()
	if err := setupMultipleRanges(ctx, db, "b", "c"); err != nil {
		t.Fatal(err)
	}

	// Set the initial value on the target key "b".
	origVal := roachpb.MakeValueFromString("val")
	if err := db.Put(ctx, keyB, &origVal); err != nil {
		t.Fatal(err)
	}
	origBytes := origVal.TagAndDataBytes()

	// The following txn creates a batch request that is split into three
	// requests: Put, CPut, and Put. The CPut operation will get a
	// ReadWithinUncertaintyIntervalError and the txn will be retried.
	epoch := 0
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Observe the commit timestamp to prevent refreshes.
		_ = txn.CommitTimestamp()

		epoch++
		proto := txn.TestingCloneTxn()
		if epoch >= 2 {
			// ObservedTimestamps must contain the timestamp returned from the
			// Put operation.
			if !containsObservedTSs(proto) {
				t.Errorf("expected observed timestamp, found: %v", proto.ObservedTimestamps)
			}
		} else {
			// ObservedTimestamps must not contain the timestamp returned from
			// the Put operation.
			if containsObservedTSs(proto) {
				t.Errorf("unexpected observed timestamp, found: %v", proto.ObservedTimestamps)
			}
		}

		b := txn.NewBatch()
		b.Put(keyA, "val")
		b.CPut(keyB, "new_val", origBytes)
		b.Put(keyC, "val2")
		err := txn.CommitInBatch(ctx, b)
		if epoch == 1 {
			if retErr := (*roachpb.TransactionRetryWithProtoRefreshError)(nil); errors.As(err, &retErr) {
				if !testutils.IsError(retErr, "ReadWithinUncertaintyIntervalError") {
					t.Errorf("expected ReadWithinUncertaintyIntervalError, but got: %v", retErr)
				}
			} else {
				t.Errorf("expected a retryable error, but got: %v", err)
			}
		}
		return err
	}); err != nil {
		t.Errorf("unexpected error on transactional Puts: %s", err)
	}

	if epoch != 2 {
		t.Errorf("unexpected epoch; the txn must be retried exactly once, but got %d", epoch)
	}
}

// TestTxnStarvation pits a transaction against an adversarial
// concurrent writer which will continually cause write-too-old
// errors unless the transaction is able to lay down intents on
// retry.
func TestTxnStarvation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	haveWritten := make(chan struct{})
	txnDone := make(chan struct{})
	errCh := make(chan error)

	// Busy write new values to the same key.
	go func() {
		for i := 0; ; i++ {
			if err := s.DB().Put(ctx, "key", fmt.Sprintf("%10d", i)); err != nil {
				errCh <- err
				return
			}
			// Signal after the first write.
			if i == 0 {
				close(haveWritten)
			}
			select {
			case <-txnDone:
				errCh <- nil
				return
			default:
			}
		}
	}()

	epoch := 0
	if err := s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		epoch++
		<-haveWritten
		time.Sleep(1 * time.Millisecond)
		b := txn.NewBatch()
		b.Put("key", "txn-value")
		return txn.CommitInBatch(ctx, b)
	}); err != nil {
		t.Fatal(err)
	}
	close(txnDone)

	if epoch > 2 {
		t.Fatalf("expected at most two epochs; got %d", epoch)
	}

	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

// Test that, if the TxnCoordSender gets a TransactionAbortedError, it sends an
// EndTxn with Poison=true (the poisoning is so that concurrent readers don't
// miss their writes).
func TestAsyncAbortPoisons(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Add a testing request filter which pauses a get request for the
	// key until after the signal channel is closed.
	var storeKnobs kvserver.StoreTestingKnobs
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	commitCh := make(chan error, 1)
	storeKnobs.TestingRequestFilter = func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		for _, req := range ba.Requests {
			switch r := req.GetInner().(type) {
			case *roachpb.EndTxnRequest:
				if r.Key.Equal(keyA) {
					if r.Poison {
						close(commitCh)
					} else {
						commitCh <- errors.New("EndTxn didn't have expected Poison flag")
					}
				}
			}
		}
		return nil
	}
	s, _, db := serverutils.StartServer(t,
		base.TestServerArgs{Knobs: base.TestingKnobs{Store: &storeKnobs}})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// Write values to key "a".
	txn := kv.NewTxn(ctx, db, 0 /* gatewayNodeID */)
	b := txn.NewBatch()
	b.Put(keyA, []byte("value"))
	require.NoError(t, txn.Run(ctx, b))

	// Run a high-priority txn that will abort the previous one.
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
			return err
		}
		// Write to keyB first to locate this txn's record on a different key
		// than the initial txn's record. This allows the request filter to
		// trivially ignore this transaction.
		if err := txn.Put(ctx, keyB, []byte("value2")); err != nil {
			return err
		}
		return txn.Put(ctx, keyA, []byte("value2"))
	}))

	_, err := txn.Get(ctx, keyA)
	require.Error(t, err)
	require.IsType(t, err, &roachpb.TransactionRetryWithProtoRefreshError{})
	require.Contains(t, err.Error(), "TransactionAbortedError")
	require.NoError(t, <-commitCh)
}

// TestTxnCoordSenderRetries verifies that the txn coord sender
// can automatically retry transactions in many different cases,
// but still fail in others, depending on different conditions.
func TestTxnCoordSenderRetries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var filterFn atomic.Value
	var storeKnobs kvserver.StoreTestingKnobs
	storeKnobs.EvalKnobs.TestingEvalFilter =
		func(fArgs kvserverbase.FilterArgs) *roachpb.Error {
			fnVal := filterFn.Load()
			if fn, ok := fnVal.(func(kvserverbase.FilterArgs) *roachpb.Error); ok && fn != nil {
				return fn(fArgs)
			}
			return nil
		}

	var refreshSpansCondenseFilter atomic.Value
	s, _, _ := serverutils.StartServer(t,
		base.TestServerArgs{Knobs: base.TestingKnobs{
			Store: &storeKnobs,
			KVClient: &kvcoord.ClientTestingKnobs{
				CondenseRefreshSpansFilter: func() bool {
					fnVal := refreshSpansCondenseFilter.Load()
					if fn, ok := fnVal.(func() bool); ok {
						return fn()
					}
					return true
				},
			}}})

	disableCondensingRefreshSpans := func() bool { return false }

	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	newUncertaintyFilter := func(key roachpb.Key) func(kvserverbase.FilterArgs) *roachpb.Error {
		var count int32
		return func(fArgs kvserverbase.FilterArgs) *roachpb.Error {
			if (fArgs.Req.Header().Key.Equal(key) ||
				fArgs.Req.Header().Span().ContainsKey(key)) && fArgs.Hdr.Txn != nil {
				if atomic.AddInt32(&count, 1) > 1 {
					return nil
				}
				err := roachpb.NewReadWithinUncertaintyIntervalError(
					fArgs.Hdr.Timestamp, s.Clock().Now(), hlc.Timestamp{}, fArgs.Hdr.Txn)
				return roachpb.NewErrorWithTxn(err, fArgs.Hdr.Txn)
			}
			return nil
		}
	}

	// Setup two userspace ranges: /Min-b, b-/Max.
	db := s.DB()
	if err := setupMultipleRanges(ctx, db, "b"); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name                       string
		beforeTxnStart             func(context.Context, *kv.DB) error  // called before the txn starts
		afterTxnStart              func(context.Context, *kv.DB) error  // called after the txn chooses a timestamp
		retryable                  func(context.Context, *kv.Txn) error // called during the txn; may be retried
		filter                     func(kvserverbase.FilterArgs) *roachpb.Error
		refreshSpansCondenseFilter func() bool
		priorReads                 bool
		tsLeaked                   bool
		// If both of these are false, no retries.
		txnCoordRetry bool
		clientRetry   bool
		expFailure    string // regexp pattern to match on error if not empty
	}{
		{
			name: "forwarded timestamp with get and put",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // read key to set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.Put(ctx, "a", "put") // put to advance txn ts
			},
			// No retry, preemptive refresh before commit.
		},
		{
			name: "forwarded timestamp with get and put after timestamp leaked",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // read key to set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.Put(ctx, "a", "put") // put to advance txn ts
			},
			tsLeaked:    true,
			clientRetry: true,
		},
		{
			name: "forwarded timestamp with get and initput",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // read key to set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.InitPut(ctx, "a", "put", false /* failOnTombstones */) // put to advance txn ts
			},
		},
		{
			name: "forwarded timestamp with get and cput",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // read key to set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.CPut(ctx, "a", "cput", kvclientutils.StrToCPutExistingValue("put")) // cput to advance txn ts, set update span
			},
		},
		{
			name: "forwarded timestamp with get and cput after timestamp leaked",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "put")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // read key to set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.CPut(ctx, "a", "cput", kvclientutils.StrToCPutExistingValue("put")) // cput to advance txn ts, set update span
			},
			tsLeaked:    true,
			clientRetry: true,
		},
		{
			name: "forwarded timestamp with scan and cput",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Scan(ctx, "a", "az", 0) // scan sets ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.CPut(ctx, "ab", "cput", nil) // cput advances, sets update span
			},
		},
		{
			name: "forwarded timestamp with delete range",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // read key to set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				_, err := txn.DelRange(ctx, "a", "b", false /* returnKeys */)
				return err
			},
			// No retry, preemptive refresh before commit.
		},
		{
			name: "forwarded timestamp with put in batch commit",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				b.Put("a", "put")
				return txn.CommitInBatch(ctx, b)
			},
			// No retries, server-side refresh, 1pc commit.
		},
		{
			name: "forwarded timestamp with cput in batch commit",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "orig")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				b.CPut("a", "cput", kvclientutils.StrToCPutExistingValue("orig"))
				return txn.CommitInBatch(ctx, b)
			},
			// No retries, server-side refresh, 1pc commit.
		},
		{
			name: "forwarded timestamp with get before commit",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				// Advance timestamp.
				if err := txn.Put(ctx, "a", "put"); err != nil {
					return err
				}
				_, err := txn.Get(ctx, "a2")
				return err
			},
			// No retry, preemptive refresh before get.
		},
		{
			name: "forwarded timestamp with scan before commit",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				// Advance timestamp.
				if err := txn.Put(ctx, "a", "put"); err != nil {
					return err
				}
				_, err := txn.Scan(ctx, "a2", "a3", 0)
				return err
			},
			// No retry, preemptive refresh before scan.
		},
		{
			name: "forwarded timestamp with get in batch commit",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				// Advance timestamp.
				if err := txn.Put(ctx, "a", "put"); err != nil {
					return err
				}
				b := txn.NewBatch()
				b.Get("a2")
				return txn.CommitInBatch(ctx, b)
			},
			// No retry, preemptive refresh before commit.
		},
		{
			name: "forwarded timestamp with scan in batch commit",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				// Advance timestamp.
				if err := txn.Put(ctx, "a", "put"); err != nil {
					return err
				}
				b := txn.NewBatch()
				b.Scan("a2", "a3")
				return txn.CommitInBatch(ctx, b)
			},
			// No retry, preemptive refresh before commit.
		},
		{
			name: "forwarded timestamp with put and get in batch commit",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				b.Get("a2")
				b.Put("a", "put") // advance timestamp
				return txn.CommitInBatch(ctx, b)
			},
			// Read-only request (Get) prevents server-side refresh.
			txnCoordRetry: true,
		},
		{
			name: "forwarded timestamp with put and scan in batch commit",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				b.Scan("a2", "a3")
				b.Put("a", "put") // advance timestamp
				return txn.CommitInBatch(ctx, b)
			},
			// Read-only request (Scan) prevents server-side refresh.
			txnCoordRetry: true,
		},
		{
			// If we've exhausted the limit for tracking refresh spans but we
			// already refreshed, keep running the txn.
			name:                       "forwarded timestamp with too many refreshes, read only",
			refreshSpansCondenseFilter: disableCondensingRefreshSpans,
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "value")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				// Make the batch large enough such that when we accounted for
				// all of its spans then we exceed the limit on refresh spans.
				// This is not an issue because we refresh before tracking their
				// spans.
				keybase := strings.Repeat("a", 1024)
				maxRefreshBytes := kvcoord.MaxTxnRefreshSpansBytes.Get(&s.ClusterSettings().SV)
				scanToExceed := int(maxRefreshBytes) / len(keybase)
				b := txn.NewBatch()
				// Hit the uncertainty error at the beginning of the batch.
				b.Get("a")
				for i := 0; i < scanToExceed; i++ {
					key := roachpb.Key(fmt.Sprintf("%s%10d", keybase, i))
					b.Scan(key, key.Next())
				}
				return txn.Run(ctx, b)
			},
			filter: newUncertaintyFilter(roachpb.Key("a")),
			// We expect the request to succeed after a server-side retry.
			txnCoordRetry: false,
		},
		{
			// Even if accounting for the refresh spans would have exhausted the
			// limit for tracking refresh spans and our transaction's timestamp
			// has been pushed, if we successfully commit then we won't hit an
			// error. This is the case even if the final batch itself causes a
			// no-op refresh because the txn has no refresh spans.
			name: "forwarded timestamp with too many refreshes in batch commit " +
				"with no-op refresh",
			refreshSpansCondenseFilter: disableCondensingRefreshSpans,
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				// Advance timestamp.
				if err := txn.Put(ctx, "a", "put"); err != nil {
					return err
				}
				// Make the final batch large enough such that if we accounted
				// for all of its spans then we would exceed the limit on
				// refresh spans. This is not an issue because we never need to
				// account for them. The txn has no refresh spans, so it can
				// forward its timestamp while committing.
				keybase := strings.Repeat("a", 1024)
				maxRefreshBytes := kvcoord.MaxTxnRefreshSpansBytes.Get(&s.ClusterSettings().SV)
				scanToExceed := int(maxRefreshBytes) / len(keybase)
				b := txn.NewBatch()
				for i := 0; i < scanToExceed; i++ {
					key := roachpb.Key(fmt.Sprintf("%s%10d", keybase, i))
					b.Scan(key, key.Next())
				}
				return txn.CommitInBatch(ctx, b)
			},
			// No retry, preemptive refresh before commit.
		},
		{
			// Even if accounting for the refresh spans would have exhausted the
			// limit for tracking refresh spans and our transaction's timestamp
			// has been pushed, if we successfully commit then we won't hit an
			// error. This is the case even if the final batch itself causes a
			// real refresh.
			name: "forwarded timestamp with too many refreshes in batch commit " +
				"with refresh",
			refreshSpansCondenseFilter: disableCondensingRefreshSpans,
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				// Advance timestamp. This also creates a refresh span which
				// will prevent the txn from committing without a refresh.
				if _, err := txn.DelRange(ctx, "a", "b", false /* returnKeys */); err != nil {
					return err
				}
				// Make the final batch large enough such that if we accounted
				// for all of its spans then we would exceed the limit on
				// refresh spans. This is not an issue because we never need to
				// account for them until the final batch, at which time we
				// perform a span refresh and successfully commit.
				keybase := strings.Repeat("a", 1024)
				maxRefreshBytes := kvcoord.MaxTxnRefreshSpansBytes.Get(&s.ClusterSettings().SV)
				scanToExceed := int(maxRefreshBytes) / len(keybase)
				b := txn.NewBatch()
				for i := 0; i < scanToExceed; i++ {
					key := roachpb.Key(fmt.Sprintf("%s%10d", keybase, i))
					b.Scan(key, key.Next())
				}
				return txn.CommitInBatch(ctx, b)
			},
			// No retry, preemptive refresh before commit.
		},
		{
			name: "write too old with put",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.Put(ctx, "a", "put")
			},
		},
		{
			name: "write too old with put after prior read",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.Put(ctx, "a", "put")
			},
			priorReads:    true,
			txnCoordRetry: true,
		},
		{
			name: "write too old with put after timestamp leaked",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.Put(ctx, "a", "put")
			},
			tsLeaked:    true,
			clientRetry: true,
		},
		{
			name: "write too old with get in the clear",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				if _, err := txn.Get(ctx, "b"); err != nil {
					return err
				}
				return txn.Put(ctx, "a", "put")
			},
			txnCoordRetry: true,
		},
		{
			name: "write too old with get conflict",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				if _, err := txn.Get(ctx, "a"); err != nil {
					return err
				}
				return txn.Put(ctx, "a", "put")
			},
			clientRetry: true,
		},
		{
			name: "write too old with multiple puts to same key",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "value1")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				// Get so we must refresh when txn timestamp moves forward.
				if _, err := txn.Get(ctx, "a"); err != nil {
					return err
				}
				// Now, Put a new value to "a" out of band from the txn.
				if err := txn.DB().Put(ctx, "a", "value2"); err != nil {
					return err
				}
				// On the first txn Put, we will get a WriteTooOld flag set,
				// but lay down the intent and continue the txn.
				if err := txn.Put(ctx, "a", "txn-value1"); err != nil {
					return err
				}
				// Write again to make sure the timestamp of the second intent
				// is correctly set to the txn's advanced timestamp. There was
				// previously a bug where the txn's original timestamp would be used
				// and so on the txn refresh caused by the WriteTooOld flag, the
				// out-of-band Put's value would be missed (see #23032).
				return txn.Put(ctx, "a", "txn-value2")
			},
			clientRetry: true, // expect a client-side retry as refresh should fail
		},
		{
			name: "write too old with cput matching newer value",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "value")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.CPut(ctx, "a", "cput", kvclientutils.StrToCPutExistingValue("put"))
			},
			txnCoordRetry: false,              // fails on first attempt at cput
			expFailure:    "unexpected value", // the failure we get is a condition failed error
		},
		{
			name: "write too old with cput matching older value",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "value")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.CPut(ctx, "a", "cput", kvclientutils.StrToCPutExistingValue("value"))
			},
			txnCoordRetry: false,              // non-matching value means we fail txn coord retry
			expFailure:    "unexpected value", // the failure we get is a condition failed error
		},
		{
			name: "write too old with cput matching older and newer values",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "value")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "value")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.CPut(ctx, "a", "cput", kvclientutils.StrToCPutExistingValue("value"))
			},
		},
		{
			name: "write too old with cput matching older and newer values after prior read",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "value")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "value")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.CPut(ctx, "a", "cput", kvclientutils.StrToCPutExistingValue("value"))
			},
			priorReads:    true,
			txnCoordRetry: true,
		},
		{
			name: "write too old with increment",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Inc(ctx, "inc1", 1)
				return err
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Inc(ctx, "inc1", 1)
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				val, err := txn.Inc(ctx, "inc1", 1)
				if err != nil {
					return err
				}
				if vInt := val.ValueInt(); vInt != 3 {
					return errors.Errorf("expected val=3; got %d", vInt)
				}
				return nil
			},
		},
		{
			name: "write too old with increment after prior read",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Inc(ctx, "inc2", 1)
				return err
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Inc(ctx, "inc2", 1)
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				val, err := txn.Inc(ctx, "inc2", 1)
				if err != nil {
					return err
				}
				if vInt := val.ValueInt(); vInt != 3 {
					return errors.Errorf("expected val=3; got %d", vInt)
				}
				return nil
			},
			priorReads:    true,
			txnCoordRetry: true,
		},
		{
			name: "write too old with initput",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "iput", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.InitPut(ctx, "iput", "put", false)
			},
		},
		{
			name: "write too old with initput after prior read",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "iput", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.InitPut(ctx, "iput", "put", false)
			},
			priorReads:    true,
			txnCoordRetry: true, // fails on first attempt at cput with write too old
			// Succeeds on second attempt.
		},
		{
			name: "write too old with initput matching older and newer values",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "iput", "put")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "iput", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.InitPut(ctx, "iput", "put", false)
			},
		},
		{
			name: "write too old with initput matching older and newer values after prior read",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "iput", "put")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "iput", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.InitPut(ctx, "iput", "put", false)
			},
			priorReads: true,
			// Expect a transaction coord retry, which should succeed.
			txnCoordRetry: true,
		},
		{
			name: "write too old with initput matching older value",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "iput", "put1")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "iput", "put2")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.InitPut(ctx, "iput", "put1", false)
			},
			txnCoordRetry: false,              // non-matching value means we fail txn coord retry
			expFailure:    "unexpected value", // the failure we get is a condition failed error
		},
		{
			name: "write too old with initput matching newer value",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "iput", "put1")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "iput", "put2")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.InitPut(ctx, "iput", "put2", false)
			},
			// No txn coord retry as we get condition failed error.
			expFailure: "unexpected value", // the failure we get is a condition failed error
		},
		{
			name: "write too old with initput failing on tombstone before",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Del(ctx, "iput")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "iput", "put2")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.InitPut(ctx, "iput", "put2", true)
			},
			expFailure: "unexpected value", // condition failed error when failing on tombstones
		},
		{
			name: "write too old with initput failing on tombstone after",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "iput", "put")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Del(ctx, "iput")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.InitPut(ctx, "iput", "put", true)
			},
			txnCoordRetry: false,              // non-matching value means we fail txn coord retry
			expFailure:    "unexpected value", // condition failed error when failing on tombstones
		},
		{
			name: "write too old with locking read",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				_, err := txn.ScanForUpdate(ctx, "a", "a\x00", 0)
				return err
			},
		},
		{
			name: "write too old with locking read after prior read",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				_, err := txn.ScanForUpdate(ctx, "a", "a\x00", 0)
				return err
			},
			priorReads:    true,
			txnCoordRetry: true,
		},
		{
			name: "write too old with multi-range locking read (err on first range)",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				_, err := txn.ScanForUpdate(ctx, "a", "c", 0)
				return err
			},
			txnCoordRetry: true,
		},
		{
			name: "write too old with multi-range locking read (err on second range)",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "b", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				_, err := txn.ScanForUpdate(ctx, "a", "c", 0)
				return err
			},
			txnCoordRetry: true,
		},
		{
			name: "write too old with multi-range batch of locking reads",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				b.ScanForUpdate("a", "a\x00")
				b.ScanForUpdate("b", "b\x00")
				return txn.Run(ctx, b)
			},
			txnCoordRetry: true,
		},
		{
			name: "write too old with delete range after prior read on other key",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "put")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.DelRange(ctx, "a", "b", false /* returnKeys */)
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				if _, err := txn.Get(ctx, "c"); err != nil {
					return err
				}
				_, err := txn.DelRange(ctx, "a", "b", false /* returnKeys */)
				return err
			},
			txnCoordRetry: true, // can refresh
		},
		{
			name: "write too old with delete range after prior read on same key",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "put")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.DelRange(ctx, "a", "b", false /* returnKeys */)
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				if _, err := txn.Get(ctx, "a"); err != nil {
					return err
				}
				_, err := txn.DelRange(ctx, "a", "b", false /* returnKeys */)
				return err
			},
			clientRetry: true, // can't refresh
		},
		{
			// This test sends a 1PC batch with Put+EndTxn.
			// The Put gets a write too old error but, since there's no refresh spans,
			// the commit succeeds.
			name: "write too old with put in batch commit",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				b.Put("a", "new-put")
				return txn.CommitInBatch(ctx, b) // will be a 1PC, won't get auto retry
			},
			// No retries, server-side refresh, 1pc commit.
		},
		{
			// This test is like the previous one in that the commit batch succeeds at
			// an updated timestamp, but this time the EndTxn puts the
			// transaction in the STAGING state instead of COMMITTED because there had
			// been previous write in a different batch. Like above, the commit is
			// successful since there are no refresh spans (the request will succeed
			// after a server-side refresh).
			name: "write too old in staging commit",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "orig")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				if err := txn.Put(ctx, "another", "another put"); err != nil {
					return err
				}
				b := txn.NewBatch()
				b.Put("a", "final value")
				return txn.CommitInBatch(ctx, b)
			},
			// The request will succeed after a server-side refresh.
			txnCoordRetry: false,
		},
		{
			name: "write too old with cput in batch commit",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "orig")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				b.CPut("a", "cput", kvclientutils.StrToCPutExistingValue("put"))
				return txn.CommitInBatch(ctx, b) // will be a 1PC, won't get auto retry
			},
			// No client-side retries, 1PC commit. On the server-side, the batch is
			// evaluated twice: once at the original timestamp, where it gets a
			// WriteTooOldError, and then once at the pushed timestamp. The
			// server-side retry is enabled by the fact that there have not been any
			// previous reads and so the transaction can commit at a pushed timestamp.
		},
		{
			// This test is like the previous one, except the 1PC batch cannot commit
			// at the updated timestamp.
			name: "write too old with failed cput in batch commit",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "orig")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				b.CPut("a", "cput", kvclientutils.StrToCPutExistingValue("orig"))
				return txn.CommitInBatch(ctx, b) // will be a 1PC, won't get auto retry
			},
			expFailure: "unexpected value", // The CPut cannot succeed.
		},
		{
			name: "multi-range batch with forwarded timestamp",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "c") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				b.Put("a", "put")
				b.Put("c", "put")
				return txn.CommitInBatch(ctx, b)
			},
			txnCoordRetry: true,
		},
		{
			name: "multi-range batch with forwarded timestamp and cput",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "value")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				b.CPut("a", "cput", kvclientutils.StrToCPutExistingValue("value"))
				b.Put("c", "put")
				return txn.CommitInBatch(ctx, b) // both puts will succeed, no retry
			},
		},
		{
			name: "multi-range batch with forwarded timestamp and cput and get",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "value")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				if _, err := txn.Get(ctx, "b"); err != nil { // Get triggers retry
					return err
				}
				b := txn.NewBatch()
				b.CPut("a", "cput", kvclientutils.StrToCPutExistingValue("value"))
				b.Put("c", "put")
				return txn.CommitInBatch(ctx, b) // both puts will succeed, et will retry from get
			},
			txnCoordRetry: true,
		},
		{
			name: "multi-range batch with forwarded timestamp and cput and delete range",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "c", "value")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				b.DelRange("a", "b", false /* returnKeys */)
				b.CPut("c", "cput", kvclientutils.StrToCPutExistingValue("value"))
				return txn.CommitInBatch(ctx, b) // both puts will succeed, et will retry
			},
			txnCoordRetry: true,
		},
		{
			name: "multi-range batch with write too old",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "c", "value")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				b.Put("a", "put")
				b.Put("c", "put")
				return txn.CommitInBatch(ctx, b) // put to c will return WriteTooOldError
			},
			txnCoordRetry: true,
		},
		{
			name: "multi-range batch with write too old and failed cput",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "orig")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "value")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				b.CPut("a", "cput", kvclientutils.StrToCPutExistingValue("orig"))
				b.Put("c", "put")
				return txn.CommitInBatch(ctx, b)
			},
			txnCoordRetry: false,              // non-matching value means we fail txn coord retry
			expFailure:    "unexpected value", // the failure we get is a condition failed error
		},
		{
			name: "multi-range batch with write too old and successful cput",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "orig")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "orig")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				b.CPut("a", "cput", kvclientutils.StrToCPutExistingValue("orig"))
				b.Put("c", "put")
				return txn.CommitInBatch(ctx, b)
			},
			// We expect the request to succeed after a server-side retry.
			txnCoordRetry: false,
		},
		{
			// This test checks the behavior of batches that were split by the
			// DistSender. We'll check that the whole batch is retried after a
			// successful refresh, and that previously-successful prefix sub-batches
			// are not refreshed (but are retried instead).
			name: "multi-range with scan getting updated results after refresh",
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				// Write to "a". This value will not be seen by the Get the first time
				// it's evaluated, but it will be see when it's retried at a bumped
				// timestamp. In particular, this verifies that the get is not
				// refreshed, for this would fail (and lead to a client-side retry
				// instead of one at the txn coord sender).
				if err := db.Put(ctx, "a", "newval"); err != nil {
					return err
				}
				// "b" is on a different range, so this put will cause a
				// WriteTooOldError on the 2nd sub-batch. The error will cause a
				// refresh.
				return db.Put(ctx, "b", "newval2")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				b.Get("a")
				b.Put("b", "put2")
				err := txn.Run(ctx, b)
				if err != nil {
					return err
				}
				gr := b.RawResponse().Responses[0].GetGet()
				if b, err := gr.Value.GetBytes(); err != nil {
					return err
				} else if !bytes.Equal(b, []byte("newval")) {
					return fmt.Errorf("expected \"newval\", got: %v", b)
				}
				return txn.Commit(ctx)
			},
			txnCoordRetry: true,
		},
		{
			name: "cput within uncertainty interval",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "value")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.CPut(ctx, "a", "cput", kvclientutils.StrToCPutExistingValue("value"))
			},
			filter: newUncertaintyFilter(roachpb.Key("a")),
			// We expect the request to succeed after a server-side retry.
			txnCoordRetry: false,
		},
		{
			name: "cput within uncertainty interval after timestamp leaked",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "value")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				return txn.CPut(ctx, "a", "cput", kvclientutils.StrToCPutExistingValue("value"))
			},
			filter:      newUncertaintyFilter(roachpb.Key("a")),
			clientRetry: true,
			tsLeaked:    true,
		},
		{
			name: "reads within uncertainty interval",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "value")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				if _, err := txn.Get(ctx, "aa"); err != nil {
					return err
				}
				if _, err := txn.Get(ctx, "ab"); err != nil {
					return err
				}
				if _, err := txn.Get(ctx, "ac"); err != nil {
					return err
				}
				return txn.CPut(ctx, "a", "cput", kvclientutils.StrToCPutExistingValue("value"))
			},
			filter:        newUncertaintyFilter(roachpb.Key("ac")),
			txnCoordRetry: true,
		},
		{
			name: "reads within uncertainty interval and violating concurrent put",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "value")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "ab", "value")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				if _, err := txn.Get(ctx, "aa"); err != nil {
					return err
				}
				if _, err := txn.Get(ctx, "ab"); err != nil {
					return err
				}
				if _, err := txn.Get(ctx, "ac"); err != nil {
					return err
				}
				return nil
			},
			filter:      newUncertaintyFilter(roachpb.Key("ac")),
			clientRetry: true, // note this txn is read-only but still restarts
		},
		{
			name: "multi-range batch with uncertainty interval error",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "c", "value")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				if err := txn.Put(ctx, "a", "put"); err != nil {
					return err
				}
				b := txn.NewBatch()
				b.CPut("c", "cput", kvclientutils.StrToCPutExistingValue("value"))
				return txn.CommitInBatch(ctx, b)
			},
			filter:        newUncertaintyFilter(roachpb.Key("c")),
			txnCoordRetry: true,
		},
		{
			name: "multi-range batch with uncertainty interval error and get conflict",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "a", "init")
			},
			afterTxnStart: func(ctx context.Context, db *kv.DB) error {
				if err := db.Put(ctx, "b", "value"); err != nil {
					return err
				}
				return db.Put(ctx, "a", "value")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				if _, err := txn.Get(ctx, "b"); err != nil {
					return err
				}
				b := txn.NewBatch()
				b.CPut("a", "cput", kvclientutils.StrToCPutExistingValue("value"))
				return txn.CommitInBatch(ctx, b)
			},
			filter:      newUncertaintyFilter(roachpb.Key("a")),
			clientRetry: true, // will fail because of conflict on refresh span for the Get
		},
		{
			name: "multi-range batch with uncertainty interval error and mixed success",
			beforeTxnStart: func(ctx context.Context, db *kv.DB) error {
				return db.Put(ctx, "c", "value")
			},
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				b.Put("a", "put")
				b.CPut("c", "cput", kvclientutils.StrToCPutExistingValue("value"))
				return txn.CommitInBatch(ctx, b)
			},
			filter: newUncertaintyFilter(roachpb.Key("c")),
			// Expect a transaction coord retry, which should succeed.
			txnCoordRetry: true,
		},
		{
			name: "multi-range scan with uncertainty interval error",
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				_, err := txn.Scan(ctx, "a", "d", 0)
				return err
			},
			filter: newUncertaintyFilter(roachpb.Key("c")),
			// Expect a transaction coord retry, which should succeed.
			txnCoordRetry: true,
		},
		{
			name: "multi-range delete range with uncertainty interval error",
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				_, err := txn.DelRange(ctx, "a", "d", false /* returnKeys */)
				return err
			},
			filter: newUncertaintyFilter(roachpb.Key("c")),
			// Expect a transaction coord retry, which should succeed.
			txnCoordRetry: true,
		},
		{
			name: "missing pipelined write caught on chain",
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				if err := txn.Put(ctx, "a", "put"); err != nil {
					return err
				}
				// Simulate a failed intent write by resolving the intent
				// directly. This should be picked up by the transaction's
				// QueryIntent when chaining on to the pipelined write to
				// key "a".
				var ba roachpb.BatchRequest
				ba.Add(&roachpb.ResolveIntentRequest{
					RequestHeader: roachpb.RequestHeader{
						Key: roachpb.Key("a"),
					},
					IntentTxn: txn.TestingCloneTxn().TxnMeta,
					Status:    roachpb.ABORTED,
				})
				if _, pErr := txn.DB().NonTransactionalSender().Send(ctx, ba); pErr != nil {
					return pErr.GoError()
				}
				_, err := txn.Get(ctx, "a")
				return err
			},
			// The missing intent write results in a RETRY_ASYNC_WRITE_FAILURE error.
			clientRetry: true,
		},
		{
			name: "missing pipelined write caught on commit",
			retryable: func(ctx context.Context, txn *kv.Txn) error {
				if err := txn.Put(ctx, "a", "put"); err != nil {
					return err
				}
				// Simulate a failed intent write by resolving the intent
				// directly. This should be picked up by the transaction's
				// pre-commit QueryIntent for the pipelined write to key "a".
				var ba roachpb.BatchRequest
				ba.Add(&roachpb.ResolveIntentRequest{
					RequestHeader: roachpb.RequestHeader{
						Key: roachpb.Key("a"),
					},
					IntentTxn: txn.TestingCloneTxn().TxnMeta,
					Status:    roachpb.ABORTED,
				})
				if _, pErr := txn.DB().NonTransactionalSender().Send(ctx, ba); pErr != nil {
					return pErr.GoError()
				}
				return nil // commit
			},
			// The missing intent write results in a RETRY_ASYNC_WRITE_FAILURE error.
			clientRetry: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.beforeTxnStart != nil {
				if err := tc.beforeTxnStart(ctx, db); err != nil {
					t.Fatalf("failed beforeTxnStart: %s", err)
				}
			}

			if tc.filter != nil {
				filterFn.Store(tc.filter)
				defer filterFn.Store((func(kvserverbase.FilterArgs) *roachpb.Error)(nil))
			}
			if tc.refreshSpansCondenseFilter != nil {
				refreshSpansCondenseFilter.Store(tc.refreshSpansCondenseFilter)
				defer refreshSpansCondenseFilter.Store((func() bool)(nil))
			}

			var metrics kvcoord.TxnMetrics
			var lastAutoRetries int64
			var hadClientRetry bool
			epoch := 0
			if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				if tc.priorReads {
					_, err := txn.Get(ctx, "prior read")
					if err != nil {
						t.Fatalf("unexpected error during prior read: %v", err)
					}
				}
				if tc.tsLeaked {
					// Read the commit timestamp so the expectation is that
					// this transaction cannot be restarted internally.
					_ = txn.CommitTimestamp()
				}
				if epoch > 0 {
					if !tc.clientRetry {
						t.Fatal("expected txn coord sender to retry, but got client-side retry")
					}
					hadClientRetry = true
					// We expected a new epoch and got it; return success.
					return nil
				}
				defer func() { epoch++ }()

				if tc.afterTxnStart != nil {
					if err := tc.afterTxnStart(ctx, db); err != nil {
						t.Fatalf("failed afterTxnStart: %s", err)
					}
				}

				metrics = txn.Sender().(*kvcoord.TxnCoordSender).TxnCoordSenderFactory.Metrics()
				lastAutoRetries = metrics.RefreshAutoRetries.Count()

				return tc.retryable(ctx, txn)
			}); err != nil {
				if len(tc.expFailure) == 0 || !testutils.IsError(err, tc.expFailure) {
					t.Fatal(err)
				}
			} else {
				if len(tc.expFailure) > 0 {
					t.Errorf("expected failure %q", tc.expFailure)
				}
			}
			// Verify auto retry metric. Because there's a chance that splits
			// from the cluster setup are still ongoing and can experience
			// their own retries, this might increase by more than one, so we
			// can only check here that it's >= 1.
			autoRetries := metrics.RefreshAutoRetries.Count() - lastAutoRetries
			if tc.txnCoordRetry && autoRetries == 0 {
				t.Errorf("expected [at least] one txn coord sender auto retry; got %d", autoRetries)
			} else if !tc.txnCoordRetry && autoRetries != 0 {
				t.Errorf("expected no txn coord sender auto retries; got %d", autoRetries)
			}
			if tc.clientRetry && !hadClientRetry {
				t.Errorf("expected but did not experience client retry")
			} else if !tc.clientRetry && hadClientRetry {
				t.Errorf("did not expect but experienced client retry")
			}
		})
	}
}

// Test that, even though at the kvserver level requests are not idempotent
// across an EndTxn, a TxnCoordSender retry of the final batch after a refresh
// still works fine. We check that a transaction is not considered implicitly
// committed through a combination of writes from a previous attempt of the
// EndTxn batch and a STAGING txn record written by a newer attempt of that
// batch.
// Namely, the scenario is as follows:
// 1. client sends CPut(a) + CPut(b) + EndTxn. The CPut(a) is split by the
//    DistSender from the rest. Note that the parallel commit mechanism is in
//    effect here.
// 2. One of the two sides gets a WriteTooOldError, the other succeeds.
//    The client needs to refresh.
// 3. The refresh succeeds.
// 4. The client resends the whole batch (note that we don't keep track of the
//    previous partial success).
// 5. The batch is split again, and one of the two sides fails.
//
// This tests checks that, for the different combinations of failures across the
// two attempts of the request, the transaction is not erroneously considered to
// be committed. We don't want an intent laid down by the first attempt to
// satisfy a STAGING record from the 2nd attempt, or the other way around (an
// intent written in the 2nd attempt satisfying a STAGING record written on the
// first attempt). See subtests for more details.
func TestTxnCoordSenderRetriesAcrossEndTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var filterFn atomic.Value
	var storeKnobs kvserver.StoreTestingKnobs
	storeKnobs.EvalKnobs.TestingEvalFilter =
		func(fArgs kvserverbase.FilterArgs) *roachpb.Error {
			fnVal := filterFn.Load()
			if fn, ok := fnVal.(func(kvserverbase.FilterArgs) *roachpb.Error); ok && fn != nil {
				return fn(fArgs)
			}
			return nil
		}

	// The left side is CPut(a), the right side is CPut(b)+EndTxn(STAGING).
	type side int
	const (
		left side = iota
		right
	)

	testCases := []struct {
		// sidePushedOnFirstAttempt controls which sub-batch will return a
		// WriteTooOldError on the first attempt.
		sidePushedOnFirstAttempt    side
		sideRejectedOnSecondAttempt side
		txnRecExpectation           kvclientutils.PushExpectation
	}{
		{
			// On the first attempt, the left side succeeds in laying down an intent,
			// while the right side fails. On the 2nd attempt, the right side succeeds
			// while the left side fails.
			//
			// The point of this test is to check that the txn is not considered to be
			// implicitly committed at this point. Handling this scenario requires
			// special care. If we didn't do anything, then we'd end up with a STAGING
			// txn record (from the second attempt of the request) and an intent on
			// "a" from the first attempt. That intent would have a lower timestamp
			// than the txn record and so the txn would be considered explicitly
			// committed. If the txn were to be considered implicitly committed, and
			// the intent on "a" was resolved, then write on a (when it eventually
			// evaluates) might return wrong results, or be pushed, or generally get
			// very confused about how its own transaction got committed already.
			//
			// We handle this scenario by disabling the parallel commit on the
			// request's 2nd attempt. Thus, the EndTxn will be split from all the
			// other requests, and the txn record is never written if anything fails.
			sidePushedOnFirstAttempt:    right,
			sideRejectedOnSecondAttempt: left,
			// The first attempt of right side contains a parallel commit (i.e. an
			// EndTxn), but fails. The 2nd attempt of the right side will no longer
			// contain an EndTxn, as explained above. So we expect the txn record to
			// not exist.
			txnRecExpectation: kvclientutils.ExpectPusheeTxnRecordNotFound,
		},
		{
			// On the first attempt, the right side succeed in writing a STAGING txn
			// record, but the left side fails. On the second attempt, the right side
			// is rejected.
			//
			// The point of this test is to check that the txn is not considered
			// implicitly committed at this point. All the intents are in place for
			// the txn to be considered committed, but we rely on the fact that the
			// intent on "a" has a timestamp that's too high (it gets the timestamp
			// from the 2nd attempt, after a refresh, but the STAGING txn record has
			// an older timestamp). If the txn were to be considered implicitly
			// committed, it'd be bad as we are returning an error to the client
			// telling it that the EndTxn failed.
			sidePushedOnFirstAttempt:    left,
			sideRejectedOnSecondAttempt: right,
			// The first attempt of the right side writes a STAGING txn record, so we
			// expect to perform txn recovery.
			txnRecExpectation: kvclientutils.ExpectPusheeTxnRecovery,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			si, _, db := serverutils.StartServer(t,
				base.TestServerArgs{Knobs: base.TestingKnobs{Store: &storeKnobs}})
			ctx := context.Background()
			defer si.Stopper().Stop(ctx)
			s := si.(*server.TestServer)

			keyA, keyA1, keyB, keyB1 := roachpb.Key("a"), roachpb.Key("a1"), roachpb.Key("b"), roachpb.Key("b1")
			require.NoError(t, setupMultipleRanges(ctx, db, string(keyB)))

			origValA := roachpb.MakeValueFromString("initA")
			require.NoError(t, db.Put(ctx, keyA, &origValA))
			origValB := roachpb.MakeValueFromString("initA")
			require.NoError(t, db.Put(ctx, keyB, &origValB))

			txn := db.NewTxn(ctx, "test txn")

			// Do a write to anchor the txn on b's range.
			require.NoError(t, txn.Put(ctx, keyB1, "b1"))

			// Take a snapshot of the txn early. We'll use it when verifying if the txn is
			// implicitly committed. If we didn't use this early snapshot and, instead,
			// used the transaction with a bumped timestamp, then the push code would
			// infer that the txn is not implicitly committed without actually running the
			// recovery procedure. Using this snapshot mimics a pusher that ran into an
			// old intent.
			origTxn := txn.TestingCloneTxn()

			// Do a read to prevent the txn for performing server-side refreshes.
			_, err := txn.Get(ctx, keyA1)
			require.NoError(t, err)

			// After the txn started, do a conflicting read. This will cause one of
			// the txn's upcoming CPuts to return a WriteTooOldError on the first
			// attempt, causing in turn to refresh and a retry. Note that, being
			// CPuts, the pushed writes don't defer the error by returning the
			// WriteTooOld flag instead of a WriteTooOldError.
			var readKey roachpb.Key
			if tc.sidePushedOnFirstAttempt == left {
				readKey = keyA
			} else {
				readKey = keyB
			}
			_, err = db.Get(ctx, readKey)
			require.NoError(t, err)

			b := txn.NewBatch()
			b.CPut(keyA, "a", origValA.TagAndDataBytes())
			b.CPut(keyB, "b", origValB.TagAndDataBytes())

			var secondAttemptRejectKey roachpb.Key
			if tc.sideRejectedOnSecondAttempt == left {
				secondAttemptRejectKey = keyA
			} else {
				secondAttemptRejectKey = keyB
			}

			// Install a filter which will reject requests touching
			// secondAttemptRejectKey on the retry.
			var count int32
			filterFn.Store(func(args kvserverbase.FilterArgs) *roachpb.Error {
				put, ok := args.Req.(*roachpb.ConditionalPutRequest)
				if !ok {
					return nil
				}
				if !put.Key.Equal(secondAttemptRejectKey) {
					return nil
				}
				count++
				// Reject the right request on the 2nd attempt.
				if count == 2 {
					return roachpb.NewErrorf("injected error; test rejecting request")
				}
				return nil
			})

			require.Regexp(t, "injected", txn.CommitInBatch(ctx, b))
			tr := s.Tracer()
			err = kvclientutils.CheckPushResult(
				ctx, db, tr, *origTxn, kvclientutils.ExpectAborted, tc.txnRecExpectation)
			require.NoError(t, err)
		})
	}
}

// Test that we're being smart about the timestamp ranges that need to be
// refreshed: when span are refreshed, they only need to be checked for writes
// above the previous time when they've been refreshed, not from the
// transaction's original read timestamp. To wit, the following scenario should
// NOT result in a failed refresh:
// - txn starts at ts 100
// - someone else writes "a" @ 200
// - txn attempts to write "a" and is pushed to (200,1). The refresh succeeds.
// - txn reads something that has a value in [100,200]. For example, "a", which
//   it just wrote.
// - someone else writes "b" @ 300
// - txn attempts to write "b" and is pushed to (300,1). This refresh must also
//   succeed. If this Refresh request would check for values in the range
//   [100-300], it would fail (as it would find a@200). But since it only checks
//   for values in the range [200-300] (i.e. values written beyond the timestamp
//   that was refreshed before), we're good.
func TestRefreshNoFalsePositive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	txn := db.NewTxn(ctx, "test")
	origTimestamp := txn.ReadTimestamp()
	log.Infof(ctx, "test txn starting @ %s", origTimestamp)
	require.NoError(t, db.Put(ctx, "a", "test"))
	// Attempt to overwrite b, which will result in a push.
	require.NoError(t, txn.Put(ctx, "a", "test2"))
	afterPush := txn.ReadTimestamp()
	require.True(t, origTimestamp.Less(afterPush))
	log.Infof(ctx, "txn pushed to %s", afterPush)

	// Read a so that we have to refresh it when we're pushed again.
	_, err := txn.Get(ctx, "a")
	require.NoError(t, err)

	require.NoError(t, db.Put(ctx, "b", "test"))

	// Attempt to overwrite b, which will result in another push. The point of the
	// test is to check that this push succeeds in refreshing "a".
	log.Infof(ctx, "test txn writing b")
	require.NoError(t, txn.Put(ctx, "b", "test2"))
	require.True(t, afterPush.Less(txn.ReadTimestamp()))
	log.Infof(ctx, "txn pushed to %s", txn.ReadTimestamp())

	require.NoError(t, txn.Commit(ctx))
}

func BenchmarkReturnOnRangeBoundary(b *testing.B) {
	const (
		Ranges                = 10   // number of ranges to create
		KeysPerRange          = 9    // number of keys to write per range
		MaxSpanRequestKeys    = 10   // max number of keys in each scan response
		ReturnOnRangeBoundary = true // if true, enable ReturnOnRangeBoundary
		Latency               = 0    // RPC request latency
	)

	b.StopTimer()

	require.Less(b, Ranges, 26) // a-z

	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	type scanKey struct{}
	ctx := context.Background()
	scanCtx := context.WithValue(ctx, scanKey{}, "scan")

	reqFilter := func(ctx context.Context, _ roachpb.BatchRequest) *roachpb.Error {
		if ctx.Value(scanKey{}) != nil && Latency > 0 {
			time.Sleep(Latency)
		}
		return nil
	}

	s, _, db := serverutils.StartServer(b, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: reqFilter,
				DisableSplitQueue:    true,
				DisableMergeQueue:    true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	for r := 0; r < Ranges; r++ {
		rangeKey := string(rune('a' + r))
		require.NoError(b, db.AdminSplit(ctx, rangeKey, hlc.MaxTimestamp))

		for k := 0; k < KeysPerRange; k++ {
			key := fmt.Sprintf("%s%d", rangeKey, k)
			require.NoError(b, db.Put(ctx, key, "value"))
		}
	}

	b.StartTimer()

	for n := 0; n < b.N; n++ {
		txn := db.NewTxn(ctx, "scanner")
		span := &roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}
		for span != nil {
			ba := &kv.Batch{}
			ba.Header.MaxSpanRequestKeys = MaxSpanRequestKeys
			ba.Header.ReturnOnRangeBoundary = ReturnOnRangeBoundary
			ba.Scan(span.Key, span.EndKey)

			require.NoError(b, txn.Run(scanCtx, ba))
			span = ba.Results[0].ResumeSpan
		}
		require.NoError(b, txn.Commit(ctx))
	}
}
