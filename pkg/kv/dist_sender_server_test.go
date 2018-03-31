// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package kv_test

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// NOTE: these tests are in package kv_test to avoid a circular
// dependency between the server and kv packages. These tests rely on
// starting a TestServer, which creates a "real" node and employs a
// distributed sender server-side.
// Addendum: I don't think the rationale above applies ever since
// TestServerInterface was introduced and all packages became able to create
// TestServers.

func startNoSplitServer(t *testing.T) (serverutils.TestServerInterface, *client.DB) {
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &storage.StoreTestingKnobs{
				DisableSplitQueue: true,
			},
		},
	})
	return s, db
}

// TestRangeLookupWithOpenTransaction verifies that range lookups are
// done in such a way (e.g. using inconsistent reads) that they
// proceed in the event that a write intent is extant at the meta
// index record being read.
func TestRangeLookupWithOpenTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _ := startNoSplitServer(t)
	defer s.Stopper().Stop(context.TODO())

	// Create an intent on the meta1 record by writing directly to the
	// engine.
	key := testutils.MakeKey(keys.Meta1Prefix, roachpb.KeyMax)
	now := s.Clock().Now()
	txn := roachpb.MakeTransaction("txn", roachpb.Key("foobar"), 0, enginepb.SERIALIZABLE, now, 0)
	if err := engine.MVCCPutProto(
		context.Background(), s.(*server.TestServer).Engines()[0],
		nil, key, now, &txn, &roachpb.RangeDescriptor{}); err != nil {
		t.Fatal(err)
	}

	// Create a new DistSender and client.DB so that the Get below is guaranteed
	// to not hit in the range descriptor cache forcing a RangeLookup operation.
	ds := kv.NewDistSender(
		kv.DistSenderConfig{
			AmbientCtx: log.AmbientContext{Tracer: s.ClusterSettings().Tracer},
			Clock:      s.Clock(),
			RPCContext: s.RPCContext(),
		},
		s.(*server.TestServer).Gossip(),
	)
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	tsf := kv.NewTxnCoordSenderFactory(
		ambient,
		cluster.MakeTestingClusterSettings(),
		ds,
		s.Clock(),
		false, /* linearizable */
		s.Stopper(),
		kv.MakeTxnMetrics(metric.TestSampleInterval),
	)
	db := client.NewDB(tsf, s.Clock())

	// Now, with an intent pending, attempt (asynchronously) to read
	// from an arbitrary key. This will cause the distributed sender to
	// do a range lookup, which will encounter the intent. We're
	// verifying here that the range lookup doesn't fail with a write
	// intent error. If it did, it would go into a deadloop attempting
	// to push the transaction, which in turn requires another range
	// lookup, etc, ad nauseam.
	if _, err := db.Get(context.TODO(), "a"); err != nil {
		t.Fatal(err)
	}
}

// setupMultipleRanges creates a database client to the supplied test
// server and splits the key range at the given keys. Returns the DB
// client.
func setupMultipleRanges(ctx context.Context, db *client.DB, splitAt ...string) error {
	// Split the keyspace at the given keys.
	for _, key := range splitAt {
		if err := db.AdminSplit(ctx, key /* spanKey */, key /* splitKey */); err != nil {
			return err
		}
	}
	return nil
}

var errInfo = testutils.MakeCaller(3, 2)

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
	// If set, this represents the point where scanning stopped because
	// StopAtRangeBoundary was set on the BatchRequest and the scanned range was
	// exhausted (ResumeReason == RESUME_RANGE_BOUNDARY) as opposed to the scan
	// being interrupted by MaxSpanRequestKeys).
	// Ignored if all scans are supposed to have been satisfied.
	nextRangeStart string
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
	t *testing.T,
	spans [][]string,
	results []client.Result,
	expResults [][]string,
	expSatisfied map[int]struct{},
	opt checkOptions,
) {
	if len(expResults) != len(results) {
		t.Fatalf("only got %d results, wanted %d", len(expResults), len(results))
	}
	// Ensure all the keys returned align properly with what is expected.
	count := 0
	for i, res := range results {
		count += len(res.Rows)
		if opt.mode == Strict {
			if len(res.Rows) != len(expResults[i]) {
				t.Fatalf("%s: scan %d (%s): expected %d rows, got %d (%s)",
					errInfo(), i, spans[i], len(expResults[i]), len(res.Rows), res)
			}
		}
		for j, kv := range res.Rows {
			if key, expKey := string(kv.Key), expResults[i][j]; key != expKey {
				t.Fatalf("%s: scan %d (%s) expected result %d to be %q; got %q",
					errInfo(), i, spans[i], j, expKey, key)
			}
		}
	}
	if opt.mode == AcceptPrefix && count != opt.expCount {
		// Check that the bound was respected.
		t.Errorf("count = %d, expCount = %d", count, opt.expCount)
	}
}

// checks ResumeSpan returned in a ScanResponse.
func checkResumeSpanScanResults(
	t *testing.T,
	spans [][]string,
	results []client.Result,
	expResults [][]string,
	expSatisfied map[int]struct{},
	opt checkOptions,
) {
	for i, res := range results {
		rowLen := len(res.Rows)
		// Check that satisfied scans don't have resume spans.
		if _, satisfied := expSatisfied[i]; satisfied {
			if res.ResumeSpan.Key != nil || res.ResumeSpan.EndKey != nil {
				t.Fatalf("%s: satisfied scan %d (%s) has ResumeSpan: %v",
					errInfo(), i, spans[i], res.ResumeSpan)
			}
			continue
		}

		if res.ResumeReason == roachpb.RESUME_UNKNOWN {
			t.Fatalf("%s: scan %d (%s): no resume reason. resume span: %+v",
				errInfo(), i, spans[i], res.ResumeSpan)
		}

		// The scan is not expected to be satisfied, so there must be a resume span.
		// If opt.nextRangeStart is set (and so the scan is supposed to have been
		// interrupted by a range boundary), then the resume span should start there
		// or further. If it's not set (and so the scan was interrupted because of
		// MaxSpanRequestKeys), then the resume span should be identical to
		// the original request if no results have been produced, or should continue
		// after the last result otherwise.
		resumeKey := string(res.ResumeSpan.Key)
		if opt.nextRangeStart != "" {
			if res.ResumeReason != roachpb.RESUME_RANGE_BOUNDARY {
				t.Fatalf("%s: scan %d (%s): expected resume reason RANGE_BOUNDARY but got: %s",
					errInfo(), i, spans[i], res.ResumeReason)
			}
			var expResume string
			if opt.nextRangeStart < spans[i][0] {
				expResume = spans[i][0]
			} else {
				expResume = opt.nextRangeStart
			}
			if resumeKey != expResume {
				t.Fatalf("%s: scan %d (%s): expected resume %q, got: %q",
					errInfo(), i, spans[i], expResume, resumeKey)
			}
		} else {
			if res.ResumeReason != roachpb.RESUME_KEY_LIMIT {
				t.Fatalf("%s: scan %d (%s): expected resume reason RANGE_BOUNDARY but got: %s",
					errInfo(), i, spans[i], res.ResumeReason)
			}
			if rowLen == 0 {
				if resumeKey != spans[i][0] {
					t.Fatalf("%s: scan %d: expected resume %s, got: %s",
						errInfo(), i, spans[i][0], resumeKey)
				}
			} else {
				lastRes := expResults[i][rowLen-1]
				if resumeKey <= lastRes {
					t.Fatalf("%s: scan %d: expected resume %s to be above last result %s",
						errInfo(), i, resumeKey, lastRes)
				}
			}
		}

		// The EndKey must be untouched.
		if key, expKey := string(res.ResumeSpan.EndKey), spans[i][1]; key != expKey {
			t.Errorf("%s: expected resume endkey %d to be %q; got %q", errInfo(), i, expKey, key)
		}
	}
}

// check ResumeSpan returned in a ReverseScanResponse.
func checkResumeSpanReverseScanResults(
	t *testing.T,
	spans [][]string,
	results []client.Result,
	expResults [][]string,
	expSatisfied map[int]struct{},
	opt checkOptions,
) {
	for i, res := range results {
		rowLen := len(res.Rows)
		// Check that satisfied scans don't have resume spans.
		if _, satisfied := expSatisfied[i]; satisfied {
			if res.ResumeSpan.Key != nil || res.ResumeSpan.EndKey != nil {
				t.Fatalf("%s: satisfied scan %d has ResumeSpan: %v", errInfo(), i, res.ResumeSpan)
			}
			continue
		}

		// The scan is not expected to be satisfied, so there must be a resume span.
		// If opt.nextRangeStart is set (and so the scan is supposed to have been
		// interrupted by a range boundary), then the resume span should start there
		// or further. If it's not set (and so the scan was interrupted because of
		// MaxSpanRequestKeys), then the resume span should be identical to
		// the original request if no results have been produced, or should continue
		// after the last result otherwise.
		resumeKey := string(res.ResumeSpan.EndKey)
		if opt.nextRangeStart != "" {
			if res.ResumeReason != roachpb.RESUME_RANGE_BOUNDARY {
				t.Fatalf("%s: scan %d (%s): expected resume reason RANGE_BOUNDARY but got: %s",
					errInfo(), i, spans[i], res.ResumeReason)
			}
			var expResume string
			if opt.nextRangeStart > spans[i][1] {
				expResume = spans[i][1]
			} else {
				expResume = opt.nextRangeStart
			}
			if resumeKey != expResume {
				t.Fatalf("%s: scan %d (%s): expected resume %q, got: %q",
					errInfo(), i, spans[i], expResume, resumeKey)
			}
		} else {
			if res.ResumeReason != roachpb.RESUME_KEY_LIMIT {
				t.Fatalf("%s: scan %d (%s): expected resume reason RANGE_BOUNDARY but got: %s",
					errInfo(), i, spans[i], res.ResumeReason)
			}
			if rowLen == 0 {
				if resumeKey != spans[i][1] {
					t.Fatalf("%s: scan %d (%s) expected resume %s, got: %s",
						errInfo(), i, spans[i], spans[i][1], resumeKey)
				}
			} else {
				lastRes := expResults[i][rowLen-1]
				if resumeKey >= lastRes {
					t.Fatalf("%s: scan %d: expected resume %s to be below last result %s",
						errInfo(), i, resumeKey, lastRes)
				}
			}
		}

		// The Key must be untouched.
		if key, expKey := string(res.ResumeSpan.Key), spans[i][0]; key != expKey {
			t.Errorf("%s: expected resume key %d to be %q; got %q", errInfo(), i, expKey, key)
		}
	}
}

// check an entire scan result including the ResumeSpan.
func checkScanResults(
	t *testing.T,
	spans [][]string,
	results []client.Result,
	expResults [][]string,
	expSatisfied map[int]struct{},
	opt checkOptions,
) {
	checkSpanResults(t, spans, results, expResults, expSatisfied, opt)
	checkResumeSpanScanResults(t, spans, results, expResults, expSatisfied, opt)
}

// check an entire reverse scan result including the ResumeSpan.
func checkReverseScanResults(
	t *testing.T,
	spans [][]string,
	results []client.Result,
	expResults [][]string,
	expSatisfied map[int]struct{},
	opt checkOptions,
) {
	checkSpanResults(t, spans, results, expResults, expSatisfied, opt)
	checkResumeSpanReverseScanResults(t, spans, results, expResults, expSatisfied, opt)
}

// Tests multiple scans, forward and reverse, across many ranges with multiple
// bounds.
func TestMultiRangeBoundedBatchScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _ := startNoSplitServer(t)
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	db := s.DB()
	splits := []string{"a", "b", "c", "d", "e", "f"}
	if err := setupMultipleRanges(ctx, db, splits...); err != nil {
		t.Fatal(err)
	}
	keys := []string{"a1", "a2", "a3", "b1", "b2", "c1", "c2", "d1", "f1", "f2", "f3"}
	for _, key := range keys {
		if err := db.Put(ctx, key, "value"); err != nil {
			t.Fatal(err)
		}
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
	}

	for _, reverse := range []bool{false, true} {
		for bound := 1; bound <= maxBound; bound++ {
			t.Run(fmt.Sprintf("reverse=%t,bound=%d", reverse, bound), func(t *testing.T) {
				b := &client.Batch{}
				b.Header.MaxSpanRequestKeys = int64(bound)

				for _, span := range scans {
					if !reverse {
						b.Scan(span[0], span[1])
					} else {
						b.ReverseScan(span[0], span[1])
					}
				}
				if err := db.Run(ctx, b); err != nil {
					t.Fatal(err)
				}

				expCount := maxExpCount
				if bound < maxExpCount {
					expCount = bound
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
					if bound >= threshold {
						expSatisfied[i] = struct{}{}
					}
				}
				opt := checkOptions{mode: AcceptPrefix, expCount: expCount}
				if !reverse {
					checkScanResults(
						t, scans, b.Results, expResults, expSatisfied, opt)
				} else {
					checkReverseScanResults(
						t, scans, b.Results, expResultsReverse, expSatisfied, opt)
				}

				// Re-query using the resume spans that were returned; check that all
				// spans are read properly.
				if bound < maxExpCount {
					newB := &client.Batch{}
					for _, res := range b.Results {
						if res.ResumeSpan.Key != nil {
							if !reverse {
								newB.Scan(res.ResumeSpan.Key, res.ResumeSpan.EndKey)
							} else {
								newB.ReverseScan(res.ResumeSpan.Key, res.ResumeSpan.EndKey)
							}
						}
					}
					if err := db.Run(ctx, newB); err != nil {
						t.Fatal(err)
					}
					// Add the results to the previous results.
					j := 0
					for i, res := range b.Results {
						if res.ResumeSpan.Key != nil {
							b.Results[i].Rows = append(b.Results[i].Rows, newB.Results[j].Rows...)
							b.Results[i].ResumeSpan = newB.Results[j].ResumeSpan
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
							t, scans, b.Results, expResults, expSatisfied, opt)
					} else {
						checkReverseScanResults(
							t, scans, b.Results, expResultsReverse, expSatisfied, opt)
					}
				}
			})
		}
	}
}

// TestMultiRangeBoundedBatchScanUnsortedOrder runs two non-overlapping
// scan requests out of order and shows how the batch response can
// contain two partial responses.
func TestMultiRangeBoundedBatchScanUnsortedOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _ := startNoSplitServer(t)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	db := s.DB()
	if err := setupMultipleRanges(ctx, db, "a", "b", "c", "d", "e", "f"); err != nil {
		t.Fatal(err)
	}

	for _, key := range []string{"a1", "a2", "a3", "b1", "b2", "b3", "b4", "b5", "c1", "c2", "d1", "f1", "f2", "f3"} {
		if err := db.Put(ctx, key, "value"); err != nil {
			t.Fatal(err)
		}
	}

	bound := 6
	b := &client.Batch{}
	b.Header.MaxSpanRequestKeys = int64(bound)
	// Two non-overlapping requests out of order.
	spans := [][]string{{"b3", "c2"}, {"a", "b3"}}
	for _, span := range spans {
		b.Scan(span[0], span[1])
	}
	if err := db.Run(ctx, b); err != nil {
		t.Fatal(err)
	}
	// See incomplete results for the two requests.
	expResults := [][]string{
		{"b3", "b4", "b5"},
		{"a1", "a2", "a3"},
	}
	checkScanResults(t, spans, b.Results, expResults, nil /* satisfied */, checkOptions{mode: Strict})
}

// TestMultiRangeBoundedBatchScanSortedOverlapping runs two overlapping
// ordered (by start key) scan requests, and shows how the batch response can
// contain two partial responses.
func TestMultiRangeBoundedBatchScanSortedOverlapping(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _ := startNoSplitServer(t)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	db := s.DB()
	if err := setupMultipleRanges(ctx, db, "a", "b", "c", "d", "e", "f"); err != nil {
		t.Fatal(err)
	}

	for _, key := range []string{"a1", "a2", "a3", "b1", "b2", "c1", "c2", "d1", "f1", "f2", "f3"} {
		if err := db.Put(ctx, key, "value"); err != nil {
			t.Fatal(err)
		}
	}

	bound := 6
	b := &client.Batch{}
	b.Header.MaxSpanRequestKeys = int64(bound)
	// Two ordered overlapping requests.
	spans := [][]string{{"a", "d"}, {"b", "g"}}
	for _, span := range spans {
		b.Scan(span[0], span[1])
	}
	if err := db.Run(ctx, b); err != nil {
		t.Fatal(err)
	}
	// See incomplete results for the two requests.
	expResults := [][]string{
		{"a1", "a2", "a3", "b1", "b2"},
		{"b1"},
	}
	checkScanResults(t, spans, b.Results, expResults, nil /* satisfied */, checkOptions{mode: Strict})
}

// check ResumeSpan in the DelRange results.
func checkResumeSpanDelRangeResults(
	t *testing.T, spans [][]string, results []client.Result, expResults [][]string, expCount int,
) {
	for i, res := range results {
		// Check ResumeSpan when request has been processed.
		rowLen := len(res.Keys)
		if rowLen > 0 {
			// The key can be empty once the entire span has been deleted.
			if rowLen == len(expResults[i]) {
				if res.ResumeSpan.Key == nil && res.ResumeSpan.EndKey == nil {
					// Done.
					continue
				}
			}
			// The next start key is always greater than the last key seen.
			if key, expKey := string(res.ResumeSpan.Key), expResults[i][rowLen-1]; key <= expKey {
				t.Errorf("%s: expected resume key %d, %d to be %q; got %q", errInfo(), i, expCount, expKey, key)
			}
		} else {
			// The request was not processed; the resume span key <= first seen key.
			if key, expKey := string(res.ResumeSpan.Key), expResults[i][0]; key > expKey {
				t.Errorf("%s: expected resume key %d, %d to be %q; got %q", errInfo(), i, expCount, expKey, key)
			}
		}
		// The EndKey is untouched.
		if key, expKey := string(res.ResumeSpan.EndKey), spans[i][1]; key != expKey {
			t.Errorf("%s: expected resume endkey %d, %d to be %q; got %q", errInfo(), i, expCount, expKey, key)
		}
	}
}

// Tests a batch of bounded DelRange() requests.
func TestMultiRangeBoundedBatchDelRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _ := startNoSplitServer(t)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	db := s.DB()
	if err := setupMultipleRanges(ctx, db, "a", "b", "c", "d", "e", "f", "g", "h"); err != nil {
		t.Fatal(err)
	}

	// These are the expected results if there is no bound.
	expResults := [][]string{
		{"a1", "a2", "a3", "b1", "b2"},
		{"c1", "c2", "d1"},
		{"g1", "g2"},
	}
	maxExpCount := 0
	for _, res := range expResults {
		maxExpCount += len(res)
	}

	for bound := 1; bound <= 20; bound++ {
		// Initialize all keys.
		for _, key := range []string{"a1", "a2", "a3", "b1", "b2", "c1", "c2", "d1", "f1", "f2", "f3", "g1", "g2", "h1"} {
			if err := db.Put(ctx, key, "value"); err != nil {
				t.Fatal(err)
			}
		}

		b := &client.Batch{}
		b.Header.MaxSpanRequestKeys = int64(bound)
		spans := [][]string{{"a", "c"}, {"c", "f"}, {"g", "h"}}
		for _, span := range spans {
			b.DelRange(span[0], span[1], true)
		}
		if err := db.Run(ctx, b); err != nil {
			t.Fatal(err)
		}

		if len(expResults) != len(b.Results) {
			t.Fatalf("bound: %d, only got %d results, wanted %d", bound, len(expResults), len(b.Results))
		}
		expCount := maxExpCount
		if bound < maxExpCount {
			expCount = bound
		}
		rem := expCount
		for i, res := range b.Results {
			// Verify that the KeyValue slice contains the given keys.
			rem -= len(res.Keys)
			for j, key := range res.Keys {
				if expKey := expResults[i][j]; string(key) != expKey {
					t.Errorf("%s: expected scan key %d, %d to be %q; got %q", errInfo(), i, j, expKey, key)
				}
			}
		}
		if rem != 0 {
			t.Errorf("expected %d keys, got %d", bound, expCount-rem)
		}
		checkResumeSpanDelRangeResults(t, spans, b.Results, expResults, expCount)
	}

}

// Test that a bounded DelRange() request that gets terminated at a range
// boundary uses the range boundary as the start key in the response
// ResumeSpan.
func TestMultiRangeBoundedBatchDelRangeBoundary(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _ := startNoSplitServer(t)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	db := s.DB()
	if err := setupMultipleRanges(ctx, db, "a", "b"); err != nil {
		t.Fatal(err)
	}
	// Check that a
	for _, key := range []string{"a1", "a2", "a3", "b1", "b2"} {
		if err := db.Put(ctx, key, "value"); err != nil {
			t.Fatal(err)
		}
	}

	b := &client.Batch{}
	b.Header.MaxSpanRequestKeys = 3
	b.DelRange("a", "c", true)
	if err := db.Run(ctx, b); err != nil {
		t.Fatal(err)
	}
	if len(b.Results) != 1 {
		t.Fatalf("%d results returned", len(b.Results))
	}
	if string(b.Results[0].ResumeSpan.Key) != "b" || string(b.Results[0].ResumeSpan.EndKey) != "c" {
		t.Fatalf("received ResumeSpan %+v", b.Results[0].ResumeSpan)
	}

	b = &client.Batch{}
	b.Header.MaxSpanRequestKeys = 1
	b.DelRange("b", "c", true)
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

// Tests a batch of bounded DelRange() requests deleting key ranges that
// overlap.
func TestMultiRangeBoundedBatchDelRangeOverlappingKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _ := startNoSplitServer(t)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	db := s.DB()
	if err := setupMultipleRanges(ctx, db, "a", "b", "c", "d", "e", "f"); err != nil {
		t.Fatal(err)
	}

	expResults := [][]string{
		{"a1", "a2", "a3", "b1", "b2"},
		{"b3", "c1", "c2"},
		{"d1", "f1", "f2"},
		{"f3"},
	}
	maxExpCount := 0
	for _, res := range expResults {
		maxExpCount += len(res)
	}

	for bound := 1; bound <= 20; bound++ {
		for _, key := range []string{"a1", "a2", "a3", "b1", "b2", "b3", "c1", "c2", "d1", "f1", "f2", "f3"} {
			if err := db.Put(ctx, key, "value"); err != nil {
				t.Fatal(err)
			}
		}

		b := &client.Batch{}
		b.Header.MaxSpanRequestKeys = int64(bound)
		spans := [][]string{{"a", "b3"}, {"b", "d"}, {"c", "f2a"}, {"f1a", "g"}}
		for _, span := range spans {
			b.DelRange(span[0], span[1], true)
		}
		if err := db.Run(ctx, b); err != nil {
			t.Fatal(err)
		}

		if len(expResults) != len(b.Results) {
			t.Fatalf("bound: %d, only got %d results, wanted %d", bound, len(expResults), len(b.Results))
		}
		expCount := maxExpCount
		if bound < maxExpCount {
			expCount = bound
		}
		rem := expCount
		for i, res := range b.Results {
			// Verify that the KeyValue slice contains the given keys.
			rem -= len(res.Keys)
			for j, key := range res.Keys {
				if expKey := expResults[i][j]; string(key) != expKey {
					t.Errorf("%s: expected scan key %d, %d to be %q; got %q", errInfo(), i, j, expKey, key)
				}
			}
		}
		if rem != 0 {
			t.Errorf("expected %d keys, got %d", bound, expCount-rem)
		}
		checkResumeSpanDelRangeResults(t, spans, b.Results, expResults, expCount)
	}
}

// TestMultiRangeEmptyAfterTruncate exercises a code path in which a
// multi-range request deals with a range without any active requests after
// truncation. In that case, the request is skipped.
func TestMultiRangeEmptyAfterTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _ := startNoSplitServer(t)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	db := s.DB()
	if err := setupMultipleRanges(ctx, db, "c", "d"); err != nil {
		t.Fatal(err)
	}

	// Delete the keys within a transaction. The range [c,d) doesn't have
	// any active requests.
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()
		b.DelRange("a", "b", false)
		b.DelRange("e", "f", false)
		return txn.CommitInBatch(ctx, b)
	}); err != nil {
		t.Fatalf("unexpected error on transactional DeleteRange: %s", err)
	}
}

// TestMultiRequestBatchWithFwdAndReverseRequests are disallowed.
func TestMultiRequestBatchWithFwdAndReverseRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _ := startNoSplitServer(t)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	db := s.DB()
	if err := setupMultipleRanges(ctx, db, "a", "b"); err != nil {
		t.Fatal(err)
	}
	b := &client.Batch{}
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
	s, _ := startNoSplitServer(t)
	ctx := context.TODO()
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
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()
		b.DelRange("a", "d", false)
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

	for _, rc := range []roachpb.ReadConsistencyType{
		roachpb.READ_UNCOMMITTED,
		roachpb.INCONSISTENT,
	} {
		t.Run(rc.String(), func(t *testing.T) {
			s, _ := startNoSplitServer(t)
			ctx := context.TODO()
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
				b := &client.Batch{}
				b.Put(key, "value")
				if err := db.Run(ctx, b); err != nil {
					t.Fatal(err)
				}
				ts[i] = s.Clock().Now()
				log.Infof(ctx, "%d: %s %d", i, key, ts[i])
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
				roachpb.NewScan(roachpb.Key("a"), roachpb.Key("c")),
				roachpb.NewReverseScan(roachpb.Key("a"), roachpb.Key("c")),
			} {
				manual := hlc.NewManualClock(ts[0].WallTime + 1)
				clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
				ds := kv.NewDistSender(
					kv.DistSenderConfig{
						AmbientCtx: log.AmbientContext{Tracer: s.ClusterSettings().Tracer},
						Clock:      clock, RPCContext: s.RPCContext(),
					},
					s.(*server.TestServer).Gossip(),
				)

				reply, err := client.SendWrappedWith(context.Background(), ds, roachpb.Header{
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
					t.Fatalf("%d: expected 1 row; got %d\n%s", i, l, rows)
				}
				if key := string(rows[0].Key); keys[0] != key {
					t.Errorf("expected key %q; got %q", keys[0], key)
				}
			}
		})
	}
}

// TestParallelSender splits the keyspace 10 times and verifies that a
// scan across all and 10 puts to each range both use parallelizing
// dist sender.
func TestParallelSender(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := startNoSplitServer(t)
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	// Split into multiple ranges.
	splitKeys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	for _, key := range splitKeys {
		if err := db.AdminSplit(context.TODO(), key, key); err != nil {
			t.Fatal(err)
		}
	}

	psCount := s.DistSender().GetParallelSendCount()

	// Batch writes to each range.
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()
		for _, key := range splitKeys {
			b.Put(key, "val")
		}
		return txn.CommitInBatch(ctx, b)
	}); err != nil {
		t.Errorf("unexpected error on batch put: %s", err)
	}
	newPSCount := s.DistSender().GetParallelSendCount()
	if c := newPSCount - psCount; c < 9 {
		t.Errorf("expected at least 9 parallel sends; got %d", c)
	}
	psCount = newPSCount

	// Scan across all rows.
	if rows, err := db.Scan(context.TODO(), "a", "z", 0); err != nil {
		t.Fatalf("unexpected error on Scan: %s", err)
	} else if l := len(rows); l != len(splitKeys) {
		t.Fatalf("expected %d rows; got %d", len(splitKeys), l)
	}
	newPSCount = s.DistSender().GetParallelSendCount()
	if c := newPSCount - psCount; c < 9 {
		t.Errorf("expected at least 9 parallel sends; got %d", c)
	}
}

func initReverseScanTestEnv(s serverutils.TestServerInterface, t *testing.T) *client.DB {
	db := s.DB()

	// Set up multiple ranges:
	// ["", "b"),["b", "e") ,["e", "g") and ["g", "\xff\xff").
	for _, key := range []string{"b", "e", "g"} {
		// Split the keyspace at the given key.
		if err := db.AdminSplit(context.TODO(), key, key); err != nil {
			t.Fatal(err)
		}
	}
	// Write keys before, at, and after the split key.
	for _, key := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		if err := db.Put(context.TODO(), key, "value"); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// TestSingleRangeReverseScan verifies that ReverseScan gets the right results
// on a single range.
func TestSingleRangeReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _ := startNoSplitServer(t)
	defer s.Stopper().Stop(context.TODO())
	db := initReverseScanTestEnv(s, t)
	ctx := context.TODO()

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
	s, _ := startNoSplitServer(t)
	defer s.Stopper().Stop(context.TODO())
	db := initReverseScanTestEnv(s, t)
	ctx := context.TODO()

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

func TestStopAtRangeBoundary(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _ := startNoSplitServer(t)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	db := s.DB()
	if err := setupMultipleRanges(ctx, db, "a", "b", "c", "d", "e", "f"); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a1", "a2", "a3", "b1", "b2", "c1", "c2", "e1", "f1", "f2", "f3"} {
		if err := db.Put(ctx, key, "value"); err != nil {
			t.Fatal(err)
		}
	}

	scans := [][]string{
		{"a", "c"},
		{"a", "b11"},
		// An empty span that will always be satisfied by the first scan.
		{"a00", "a01"},
		{"d", "g"},
	}

	type iterationExpRes struct {
		// expResults is a map of scan index to expected results. The indexes are
		// relative to the original scans that the test started with; an iteration
		// might contain less scans than that if some of the original scans have
		// been satisfied by previous iterations.
		expResults map[int][]string
		// satisfied contains the indexes of scans that have been satisfied by the
		// current iteration.
		satisfied []int
		// expResume is set when the scan is expected to have been stopped because a
		// range boundary was hit (i.e. ResumeReason == RESUME_RANGE_BOUNDARY) and
		// it represents the expected resume key.
		expResume string
	}

	testCases := []struct {
		name       string
		reverse    bool
		limit      int64
		minResults int64
		// expResults has an entry per scans iteration (i.e. per range if minResults
		// is not set).
		iterResults []iterationExpRes
	}{
		// No limit, no minResults.
		{
			name:       "reverse=false,limit=0,minResults=0",
			limit:      0,
			minResults: 0,
			iterResults: []iterationExpRes{
				// scanning [a,b)
				{
					expResults: map[int][]string{
						0: {"a1", "a2", "a3"},
						1: {"a1", "a2", "a3"},
						2: {},
						3: {},
					},
					satisfied: []int{2},
					expResume: "b",
				},
				// scanning [b,c)
				{
					expResults: map[int][]string{
						0: {"b1", "b2"},
						1: {"b1"},
						3: {},
					},
					satisfied: []int{0, 1},
					expResume: "c",
				},
				// scanning [d,e). Notice that [c,d) is expected to be skipped since no
				// Scan needs it.
				{
					expResults: map[int][]string{
						3: {},
					},
					expResume: "e",
				},
				// scanning [e,f)
				{
					expResults: map[int][]string{
						3: {"e1"},
					},
					expResume: "f",
				},
				// scanning [f,g)
				{
					expResults: map[int][]string{
						3: {"f1", "f2", "f3"},
					},
					satisfied: []int{3},
				},
			},
		},
		// No limit, minResults = 7.
		{
			name:       "reverse=false,limit=0,minResults=7",
			limit:      0,
			minResults: 7,
			iterResults: []iterationExpRes{
				// scanning [a,c)
				{
					expResults: map[int][]string{
						0: {"a1", "a2", "a3", "b1", "b2"},
						1: {"a1", "a2", "a3", "b1"},
						2: {},
						3: {},
					},
					satisfied: []int{0, 1, 2},
					expResume: "c",
				},
				// scanning [c,inf)
				{
					expResults: map[int][]string{
						3: {"e1", "f1", "f2", "f3"},
					},
					satisfied: []int{3},
				},
			},
		},
		// limit = 8, minResults = 7.
		{
			name:       "reverse=false,limit=8,minResults=7",
			limit:      8,
			minResults: 7,
			iterResults: []iterationExpRes{
				// scanning [a, <somewhere in between b anc c>)
				{
					expResults: map[int][]string{
						0: {"a1", "a2", "a3", "b1", "b2"},
						1: {"a1", "a2", "a3"},
						2: {},
						3: {},
					},
					satisfied: []int{0, 2},
				},
				// scanning [<where we left off>,inf)
				{
					expResults: map[int][]string{
						1: {"b1"},
						3: {"e1", "f1", "f2", "f3"},
					},
					satisfied: []int{1, 3},
				},
			},
		},
		// Reverse; no limit, no minResults.
		{
			name:       "reverse=true,limit=0,minResults=0",
			reverse:    true,
			limit:      0,
			minResults: 0,
			iterResults: []iterationExpRes{
				// scanning [f,g)
				{
					expResults: map[int][]string{
						3: {"f3", "f2", "f1"},
					},
					expResume: "f",
				},
				// scanning [e,f)
				{
					expResults: map[int][]string{
						3: {"e1"},
					},
					expResume: "e",
				},
				// scanning [d,e).
				{
					expResults: map[int][]string{
						3: {},
					},
					satisfied: []int{3},
					expResume: "d",
				},
				// scanning [b,c). Notice that [c,d) is expected to be skipped.
				{
					expResults: map[int][]string{
						0: {"b2", "b1"},
						1: {"b1"},
					},
					expResume: "b",
				},
				// scanning [a,b)
				{
					expResults: map[int][]string{
						0: {"a3", "a2", "a1"},
						1: {"a3", "a2", "a1"},
						2: {},
					},
					satisfied: []int{0, 1, 2},
				},
			},
		},
		// Reverse; no limit, minResults=4.
		{
			name:       "reverse=true,limit=0,minResults=4",
			reverse:    true,
			limit:      0,
			minResults: 4,
			iterResults: []iterationExpRes{
				// scanning [e,f)
				{
					expResults: map[int][]string{
						3: {"f3", "f2", "f1", "e1"},
					},
					expResume: "e",
				},
				// scanning [a,e).
				{
					expResults: map[int][]string{
						0: {"b2", "b1", "a3", "a2", "a1"},
						1: {"b1", "a3", "a2", "a1"},
						2: {},
						3: {},
					},
					satisfied: []int{0, 1, 2, 3},
				},
			},
		},
		// Reverse; limit=5, minResults=4.
		{
			name:       "reverse=true,limit=5,minResults=4",
			reverse:    true,
			limit:      5,
			minResults: 4,
			iterResults: []iterationExpRes{
				// scanning [e,f)
				{
					expResults: map[int][]string{
						3: {"f3", "f2", "f1", "e1"},
					},
					expResume: "e",
				},
				// scanning [<somewhere between a and b>,e).
				{
					expResults: map[int][]string{
						0: {"b2", "b1", "a3", "a2"},
						1: {"b1"},
					},
					satisfied: []int{3},
				},
				// scanning [a,b).
				{
					expResults: map[int][]string{
						0: {"a1"},
						1: {"a3", "a2", "a1"},
						2: {},
					},
					satisfied: []int{0, 1, 2},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			remaining := make(map[int]roachpb.Span)
			for i, span := range scans {
				remaining[i] = roachpb.Span{
					Key:    roachpb.Key(span[0]),
					EndKey: roachpb.Key(span[1]),
				}
			}

			// Iterate until all scans are satisfied.
			for iterIdx, iter := range tc.iterResults {
				log.Infof(context.TODO(), "iteration: %d", iterIdx)
				if len(remaining) == 0 {
					t.Fatalf("scans were satisfied prematurely. Expected results: %+v", iter)
				}

				b := &client.Batch{}
				b.Header.ScanOptions = &roachpb.ScanOptions{
					StopAtRangeBoundary: true,
					MinResults:          tc.minResults,
				}
				b.Header.MaxSpanRequestKeys = tc.limit

				// Map indexes of scans in the current batch to indexes of original
				// scans.
				batchToOrig := make(map[int]int)
				origToBatch := make(map[int]int)
				var batchIdx int
				var curScans [][]string
				for i := 0; i < len(scans); i++ {
					if _, ok := remaining[i]; !ok {
						continue
					}
					span := remaining[i]
					if !tc.reverse {
						b.Scan(span.Key, span.EndKey)
					} else {
						b.ReverseScan(span.Key, span.EndKey)
					}
					curScans = append(curScans, []string{string(span.Key), string(span.EndKey)})
					batchToOrig[batchIdx] = i
					origToBatch[i] = batchIdx
					batchIdx++
				}
				if err := db.Run(ctx, b); err != nil {
					t.Fatal(err)
				}

				// Build the expected results.
				var expResults [][]string
				for i := 0; i < batchIdx; i++ {
					expResults = append(expResults, iter.expResults[batchToOrig[i]])
				}
				expSatisfied := make(map[int]struct{})
				for _, origIdx := range iter.satisfied {
					expSatisfied[origToBatch[origIdx]] = struct{}{}
				}
				if !tc.reverse {
					checkScanResults(t, curScans, b.Results, expResults, expSatisfied,
						checkOptions{mode: Strict, nextRangeStart: iter.expResume})
				} else {
					checkReverseScanResults(t, curScans, b.Results, expResults, expSatisfied,
						checkOptions{mode: Strict, nextRangeStart: iter.expResume})
				}

				// Update remaining for the next iteration; remove satisfied scans.
				for batchIdx, res := range b.Results {
					if res.ResumeSpan.Key == nil && res.ResumeSpan.EndKey == nil {
						delete(remaining, batchToOrig[batchIdx])
					} else {
						remaining[batchToOrig[batchIdx]] = res.ResumeSpan
					}
				}
			}
			if len(remaining) != 0 {
				t.Fatalf("not all scans were satisfied. Remaining: %+v", remaining)
			}
		})
	}
}

// TestBatchPutWithConcurrentSplit creates a batch with a series of put
// requests and splits the middle of the range in order to trigger
// reentrant invocation of DistSender.divideAndSendBatchToRanges. See
// #12603 for more details.
func TestBatchPutWithConcurrentSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := startNoSplitServer(t)
	defer s.Stopper().Stop(context.TODO())

	// Split first using the default client and scan to make sure that
	// the range descriptor cache reflects the split.
	for _, key := range []string{"b", "f"} {
		if err := db.AdminSplit(context.TODO(), key, key); err != nil {
			t.Fatal(err)
		}
	}
	if rows, err := db.Scan(context.TODO(), "a", "z", 0); err != nil {
		t.Fatal(err)
	} else if l := len(rows); l != 0 {
		t.Fatalf("expected empty keyspace; got %d rows", l)
	}

	// Now, split further at the given keys, but use a new dist sender so
	// we don't update the caches on the default dist sender-backed client.
	ds := kv.NewDistSender(
		kv.DistSenderConfig{
			AmbientCtx: log.AmbientContext{Tracer: s.ClusterSettings().Tracer},
			Clock:      s.Clock(), RPCContext: s.RPCContext(),
		}, s.(*server.TestServer).Gossip(),
	)
	for _, key := range []string{"c"} {
		req := &roachpb.AdminSplitRequest{
			Span: roachpb.Span{
				Key: roachpb.Key(key),
			},
			SplitKey: roachpb.Key(key),
		}
		if _, err := client.SendWrapped(context.Background(), ds, req); err != nil {
			t.Fatal(err)
		}
	}

	// Execute a batch on the default sender. Since its cache will not
	// have been updated to reflect the new splits, it will discover
	// them partway through and need to reinvoke divideAndSendBatchToRanges.
	b := &client.Batch{}
	for i, key := range []string{"a1", "b1", "c1", "d1", "f1"} {
		b.Put(key, fmt.Sprintf("value-%d", i))
	}
	if err := db.Run(context.TODO(), b); err != nil {
		t.Fatal(err)
	}
}

// TestReverseScanWithSplitAndMerge verifies that ReverseScan gets the right results
// across multiple ranges while range splits and merges happen.
func TestReverseScanWithSplitAndMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _ := startNoSplitServer(t)
	defer s.Stopper().Stop(context.TODO())
	db := initReverseScanTestEnv(s, t)

	// Case 1: An encounter with a range split.
	// Split the range ["b", "e") at "c".
	if err := db.AdminSplit(context.TODO(), "c", "c"); err != nil {
		t.Fatal(err)
	}

	// The ReverseScan will run into a stale descriptor.
	if rows, err := db.ReverseScan(context.TODO(), "a", "d", 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}

	// Case 2: encounter with range merge .
	// Merge the range ["e", "g") and ["g", "\xff\xff") .
	if err := db.AdminMerge(context.TODO(), "e"); err != nil {
		t.Fatal(err)
	}
	if rows, err := db.ReverseScan(context.TODO(), "d", "g", 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}
}

func TestBadRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := startNoSplitServer(t)
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

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

	if err := db.DelRange(ctx, "x", "a"); !testutils.IsError(err, "must be greater than start") {
		t.Fatalf("unexpected error on deletion on [x, a): %v", err)
	}

	if err := db.DelRange(ctx, "", "z"); !testutils.IsError(err, "must be greater than LocalMax") {
		t.Fatalf("unexpected error on deletion on [KeyMin, z): %v", err)
	}
}

// TestPropagateTxnOnError verifies that DistSender.sendBatch properly
// propagates the txn data to a next iteration. Use txn.Writing field to
// verify that.
func TestPropagateTxnOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var storeKnobs storage.StoreTestingKnobs
	// Set up a filter to so that the first CPut operation will
	// get a ReadWithinUncertaintyIntervalError.
	targetKey := roachpb.Key("b")
	var numGets int32
	storeKnobs.EvalKnobs.TestingEvalFilter =
		func(fArgs storagebase.FilterArgs) *roachpb.Error {
			_, ok := fArgs.Req.(*roachpb.ConditionalPutRequest)
			if ok && fArgs.Req.Header().Key.Equal(targetKey) {
				if atomic.AddInt32(&numGets, 1) == 1 {
					pErr := roachpb.NewReadWithinUncertaintyIntervalError(hlc.Timestamp{}, hlc.Timestamp{}, nil)
					return roachpb.NewErrorWithTxn(pErr, fArgs.Hdr.Txn)
				}
			}
			return nil
		}
	s, _, _ := serverutils.StartServer(t,
		base.TestServerArgs{Knobs: base.TestingKnobs{Store: &storeKnobs}})
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	db := s.DB()
	if err := setupMultipleRanges(ctx, db, "b"); err != nil {
		t.Fatal(err)
	}

	// Set the initial value on the target key "b".
	origVal := "val"
	if err := db.Put(ctx, targetKey, origVal); err != nil {
		t.Fatal(err)
	}

	// The following txn creates a batch request that is split
	// into two requests: Put and CPut. The CPut operation will
	// get a ReadWithinUncertaintyIntervalError and the txn will be
	// retried.
	epoch := 0
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		epoch++
		if epoch >= 2 {
			// Writing must be true since we ran the BeginTransaction command.
			if !txn.Proto().Writing {
				t.Errorf("unexpected non-writing txn")
			}
		} else {
			// Writing must be false since we haven't run any write command.
			if txn.Proto().Writing {
				t.Errorf("unexpected writing txn")
			}
		}

		b := txn.NewBatch()
		b.Put("a", "val")
		b.CPut(targetKey, "new_val", origVal)
		err := txn.CommitInBatch(ctx, b)
		if epoch == 1 {
			if retErr, ok := err.(*roachpb.HandledRetryableTxnError); ok {
				if !testutils.IsError(retErr, "ReadWithinUncertaintyIntervalError") {
					t.Errorf("expected ReadWithinUncertaintyIntervalError, but got: %s", retErr)
				}
			} else {
				t.Errorf("expected a retryable error, but got: %s", err)
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
	if err := s.DB().Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
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

// TestTxnCoordSenderHeartbeatFailurePostSplit verifies that on
// heartbeat timeout, the transaction is aborted asynchronously,
// leaving abort span entries which cause concurrent reads to fail
// with txn aborted errors on both the range the transaction started
// on and a separate range involved in the same transaction.
//
// Note that this is a post-split version of TestTxnCoordSenderGCTimeout.
func TestTxnCoordSenderHeartbeatFailurePostSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Add a testing request filter which pauses a get request for the
	// key until after the signal channel is closed.
	var storeKnobs storage.StoreTestingKnobs
	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")
	signal := make(chan struct{})
	storeKnobs.TestingRequestFilter = func(ba roachpb.BatchRequest) *roachpb.Error {
		for _, req := range ba.Requests {
			switch r := req.GetInner().(type) {
			case *roachpb.GetRequest:
				if r.Key.Equal(keyA) || r.Key.Equal(keyB) {
					log.VEventf(context.TODO(), 1, "waiting on read of key %s", r.Key)
					<-signal
				}
			case *roachpb.HeartbeatTxnRequest:
				if bytes.Equal(ba.Txn.Key, keyA) {
					log.VEventf(context.TODO(), 1, "failing heartbeat of txn %s", r.Key)
					return roachpb.NewErrorf("induced heartbeat failure")
				}
			}
		}
		return nil
	}
	s, _, _ := serverutils.StartServer(t,
		base.TestServerArgs{Knobs: base.TestingKnobs{Store: &storeKnobs}})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// Setup two userspace ranges: /Min-b, b-/Max.
	db := s.DB()
	if err := setupMultipleRanges(ctx, db, "b"); err != nil {
		t.Fatal(err)
	}

	// Write values to keys "a" and "b", on separate ranges.
	txn := client.NewTxn(db, 0 /* gatewayNodeID */, client.RootTxn)
	b := txn.NewBatch()
	b.Put(keyA, []byte("value"))
	b.Put(keyB, []byte("value"))
	if err := txn.Run(context.TODO(), b); err != nil {
		t.Fatal(err)
	}

	startReader := func(key roachpb.Key) chan error {
		errCh := make(chan error)
		go func() {
			if _, err := txn.Get(context.TODO(), key); err != nil {
				log.Infof(context.TODO(), "read of key %s: %s", key, err)
				errCh <- err
			} else {
				errCh <- errors.New("expected error")
			}
		}()
		return errCh
	}
	errChA := startReader(keyA)
	errChB := startReader(keyB)

	stores := s.GetStores().(*storage.Stores)
	store, err := stores.GetStore(1)
	if err != nil {
		t.Fatal(err)
	}
	// Wait for the transaction to fail the heartbeat and cleanup
	// intents (and poison the abort span for the txn ID).
	testutils.SucceedsSoon(t, func() error {
		for _, key := range []roachpb.Key{keyA, keyB} {
			meta := &enginepb.MVCCMetadata{}
			ok, _, _, err := store.Engine().GetProto(engine.MakeMVCCMetadataKey(key), meta)
			if err != nil {
				return fmt.Errorf("error getting MVCC metadata: %s", err)
			}
			if ok && meta.Txn != nil {
				return fmt.Errorf("found unexpected write intent: %s", meta)
			}
		}
		return nil
	})

	// Now signal the inflight readers to continue; they should witness
	// abort span entries.
	close(signal)
	if err := <-errChA; !testutils.IsError(err, "txn aborted") {
		t.Errorf("expected transaction aborted error reading %s; got %s", keyA, err)
	}
	if err := <-errChB; !testutils.IsError(err, "txn aborted") {
		t.Errorf("expected transaction aborted error reading %s; got %s", keyB, err)
	}
}

// TestTxnCoordSenderRetries verifies that the txn coord sender
// can automatically retry transactions in many different cases,
// but still fail in others, depending on different conditions.
func TestTxnCoordSenderRetries(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var filterFn atomic.Value
	var storeKnobs storage.StoreTestingKnobs
	storeKnobs.EvalKnobs.TestingEvalFilter =
		func(fArgs storagebase.FilterArgs) *roachpb.Error {
			fnVal := filterFn.Load()
			if fn, ok := fnVal.(func(storagebase.FilterArgs) *roachpb.Error); ok && fn != nil {
				return fn(fArgs)
			}
			return nil
		}
	s, _, _ := serverutils.StartServer(t,
		base.TestServerArgs{Knobs: base.TestingKnobs{Store: &storeKnobs}})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	newUncertaintyFilter := func(key roachpb.Key) func(storagebase.FilterArgs) *roachpb.Error {
		var count int32
		return func(fArgs storagebase.FilterArgs) *roachpb.Error {
			if (fArgs.Req.Header().Key.Equal(key) || fArgs.Req.Header().ContainsKey(key)) && fArgs.Hdr.Txn != nil {
				if atomic.AddInt32(&count, 1) > 1 {
					return nil
				}
				pErr := roachpb.NewReadWithinUncertaintyIntervalError(
					fArgs.Hdr.Timestamp, s.Clock().Now(), fArgs.Hdr.Txn)
				return roachpb.NewErrorWithTxn(pErr, fArgs.Hdr.Txn)
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
		name           string
		beforeTxnStart func(context.Context, *client.DB) error  // called before the txn starts
		afterTxnStart  func(context.Context, *client.DB) error  // called after the txn chooses a timestamp
		retryable      func(context.Context, *client.Txn) error // called during the txn; may be retried
		filter         func(storagebase.FilterArgs) *roachpb.Error
		tsLeaked       bool
		// If both of these are false, no retries.
		txnCoordRetry bool
		clientRetry   bool
		expFailure    string // regexp pattern to match on error if not empty
	}{
		{
			name: "forwarded timestamp with get and put",
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				_, err := db.Get(ctx, "a") // read key to set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.Put(ctx, "a", "put") // put to advance txn ts
			},
		},
		{
			name: "forwarded timestamp with get and put timestamp leaked",
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				_, err := db.Get(ctx, "a") // read key to set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.Put(ctx, "a", "put") // put to advance txn ts
			},
			tsLeaked:    true,
			clientRetry: true,
		},
		{
			name: "forwarded timestamp with get and initput",
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				_, err := db.Get(ctx, "a") // read key to set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.InitPut(ctx, "a", "put", false) // put to advance txn ts
			},
		},
		{
			name: "forwarded timestamp with get and initput value exists",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "put")
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				_, err := db.Get(ctx, "a") // read key to set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				err := txn.InitPut(ctx, "a", "init-put", false) // init-put to advance txn ts
				// Swallow expected condition failed error.
				if _, ok := err.(*roachpb.ConditionFailedError); !ok {
					if err != nil {
						return errors.New("expected condition failed error")
					}
					return err
				}
				log.Infof(ctx, "Swallowed error")
				return nil
			},
			// No retries, this is a straight failure.
		},
		{
			name: "forwarded timestamp with get and cput",
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				_, err := db.Get(ctx, "a") // read key to set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.CPut(ctx, "a", "cput", "put") // cput to advance txn ts, set update span
			},
		},
		{
			name: "forwarded timestamp with get and cput timestamp leaked",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "put")
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				_, err := db.Get(ctx, "a") // read key to set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.CPut(ctx, "a", "cput", "put") // cput to advance txn ts, set update span
			},
			tsLeaked:    true,
			clientRetry: true,
		},
		{
			name: "forwarded timestamp with scan and cput",
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				_, err := db.Scan(ctx, "a", "az", 0) // scan sets ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.CPut(ctx, "ab", "cput", nil) // cput advances, sets update span
			},
		},
		{
			name: "forwarded timestamp with delete range",
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				_, err := db.Get(ctx, "a") // read key to set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.DelRange(ctx, "a", "b") // del range sets RetryOnPush, but only for SNAPSHOT
			},
		},
		{
			name: "forwarded timestamp with put in batch commit",
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				b := txn.NewBatch()
				b.Put("a", "put")
				return txn.CommitInBatch(ctx, b)
			},
			// No retries, 1pc commit.
		},
		{
			name: "forwarded timestamp with cput in batch commit",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "orig")
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				b := txn.NewBatch()
				b.CPut("a", "cput", "orig")
				return txn.CommitInBatch(ctx, b)
			},
			// No retries, 1pc commit.
		},
		{
			// If there are suitable retry conditions but we've exhausted the limit
			// for tracking refresh spans, we'll exit with an error before getting
			// to the end transaction.
			name: "forwarded timestamp with too many refreshes",
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				// Advance timestamp for retry.
				if err := txn.Put(ctx, "a", "put"); err != nil {
					return err
				}
				// Scan sufficient times to exceed the limit on refresh spans. This
				// will propagate a failure because our timestamp has been pushed.
				keybase := strings.Repeat("a", 1024)
				for i := 0; ; i++ {
					key := roachpb.Key(fmt.Sprintf("%s%10d", keybase, i))
					if _, err := txn.Scan(ctx, key, key.Next(), 0); err != nil {
						return err
					}
				}
			},
			expFailure: "transaction is too large to complete; try splitting into pieces",
		},
		{
			name: "write too old with put",
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.Put(ctx, "a", "put")
			},
			// This trivially succeeds as there are no refresh spans.
		},
		{
			name: "write too old with put timestamp leaked",
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.Put(ctx, "a", "put")
			},
			tsLeaked:    true,
			clientRetry: true,
		},
		{
			name: "write too old with get in the clear",
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				if _, err := txn.Get(ctx, "b"); err != nil {
					return nil
				}
				return txn.Put(ctx, "a", "put")
			},
			txnCoordRetry: true,
		},
		{
			name: "write too old with get conflict",
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				if _, err := txn.Get(ctx, "a"); err != nil {
					return nil
				}
				return txn.Put(ctx, "a", "put")
			},
			clientRetry: true,
		},
		{
			name: "write too old with multiple puts to same key",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "value1")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				// Get so we must refresh when txn timestamp moves forward.
				if _, err := txn.Get(ctx, "a"); err != nil {
					return nil
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
				// previously a bug where the txn's OrigTimestamp would be used
				// and so on the txn refresh caused by the WriteTooOld flag, the
				// out-of-band Put's value would be missed (see #23032).
				return txn.Put(ctx, "a", "txn-value2")
			},
			clientRetry: true, // expect a client-side retry as refresh should fail
		},
		{
			name: "write too old with cput matching newer value",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "value")
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.CPut(ctx, "a", "cput", "put")
			},
			txnCoordRetry: false,              // fails on first attempt at cput
			expFailure:    "unexpected value", // the failure we get is a condition failed error
		},
		{
			name: "write too old with cput matching older value",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "value")
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.CPut(ctx, "a", "cput", "value")
			},
			txnCoordRetry: false,              // non-matching value means we fail txn coord retry
			expFailure:    "unexpected value", // the failure we get is a condition failed error
		},
		{
			name: "write too old with cput matching older and newer values",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "value")
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "value")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.CPut(ctx, "a", "cput", "value")
			},
			txnCoordRetry: true,
		},
		{
			name: "write too old with increment",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				_, err := db.Inc(ctx, "inc", 1)
				return err
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				_, err := db.Inc(ctx, "inc", 1)
				return err
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				val, err := txn.Inc(ctx, "inc", 1)
				if err != nil {
					return err
				}
				if vInt := val.ValueInt(); vInt != 3 {
					return errors.Errorf("expected val=3; got %d", vInt)
				}
				return nil
			},
			txnCoordRetry: true,
		},
		{
			name: "write too old with initput",
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "iput", "put")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.InitPut(ctx, "iput", "put", false)
			},
			txnCoordRetry: true, // fails on first attempt at cput with write too old
			// Succeeds on second attempt.
		},
		{
			name: "write too old with initput matching older and newer values",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "iput", "put")
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "iput", "put")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.InitPut(ctx, "iput", "put", false)
			},
			// No retries.
		},
		{
			name: "write too old with initput matching older value",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "iput", "put1")
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "iput", "put2")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.InitPut(ctx, "iput", "put1", false)
			},
			// No retries, no failure as init put short circuits if it matches older value.
		},
		{
			name: "write too old with initput matching newer value",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "iput", "put1")
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "iput", "put2")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.InitPut(ctx, "iput", "put2", false)
			},
			// No txn coord retry as we get condition failed error.
			expFailure: "unexpected value", // the failure we get is a condition failed error
		},
		{
			name: "write too old with initput failing on tombstone before",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Del(ctx, "iput")
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "iput", "put2")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.InitPut(ctx, "iput", "put2", true)
			},
			expFailure: "unexpected value", // condition failed error when failing on tombstones
		},
		{
			name: "write too old with initput failing on tombstone after",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "iput", "put")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				// The first time through, this will be a noop, as put is the existing value.
				if err := txn.InitPut(ctx, "iput", "put", true); err != nil {
					return err
				}
				// Create an out-of-band tombstone on the iput, which must be refreshed
				// when the put below experiences a write-too-old error.
				if err := txn.DB().Del(ctx, "iput"); err != nil {
					return err
				}
				// Write the version of "a" which triggers write-too-old
				// *after* the tombstone at the "iput" key, to ensure we see the
				// tombstone when refreshing the iput span.
				if err := txn.DB().Put(ctx, "a", "value"); err != nil {
					return err
				}
				// This command will get a write too old and refresh the init
				// put, forcing a client-retry. On the retry, the init put
				// will fail with a condition failed error.
				return txn.Put(ctx, "a", "value")
			},
			clientRetry: true,
			// Would get a condition failed error when failing on
			// tombstones, but the retryable is not re-executed in the
			// test fixture.
		},
		{
			name: "write too old with put in batch commit",
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				b := txn.NewBatch()
				b.Put("a", "new-put")
				return txn.CommitInBatch(ctx, b) // will be a 1PC, won't get auto retry
			},
			// No retries, 1pc commit.
		},
		{
			name: "write too old with cput in batch commit",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "orig")
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "put")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				b := txn.NewBatch()
				b.CPut("a", "cput", "put")
				return txn.CommitInBatch(ctx, b) // will be a 1PC, won't get auto retry
			},
			// No retries, 1pc commit.
		},
		{
			name: "multi-range batch with forwarded timestamp",
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				_, err := db.Get(ctx, "c") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				b := txn.NewBatch()
				b.Put("a", "put")
				b.Put("c", "put")
				return txn.CommitInBatch(ctx, b) // both puts will succeed, no retry
			},
		},
		{
			name: "multi-range batch with forwarded timestamp and cput",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "value")
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				b := txn.NewBatch()
				b.CPut("a", "cput", "value")
				b.Put("c", "put")
				return txn.CommitInBatch(ctx, b) // both puts will succeed, no retry
			},
		},
		{
			name: "multi-range batch with forwarded timestamp and cput and get",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "value")
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				if _, err := txn.Get(ctx, "b"); err != nil { // Get triggers txn coord retry
					return err
				}
				b := txn.NewBatch()
				b.CPut("a", "cput", "value")
				b.Put("c", "put")
				return txn.CommitInBatch(ctx, b) // both puts will succeed, et will retry from get
			},
			txnCoordRetry: true,
		},
		{
			name: "multi-range batch with forwarded timestamp and cput and delRange",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "c", "value")
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				_, err := db.Get(ctx, "a") // set ts cache
				return err
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				b := txn.NewBatch()
				b.DelRange("a", "b", false)
				b.CPut("c", "cput", "value")
				return txn.CommitInBatch(ctx, b) // both puts will succeed, et will retry
			},
			txnCoordRetry: true,
		},
		{
			name: "multi-range batch with write too old",
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "c", "value")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				b := txn.NewBatch()
				b.Put("a", "put")
				b.Put("c", "put")
				return txn.CommitInBatch(ctx, b) // both puts will succeed, no retry
			},
		},
		{
			name: "multi-range batch with write too old and failed cput",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "orig")
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "value")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				b := txn.NewBatch()
				b.CPut("a", "cput", "orig")
				b.Put("c", "put")
				return txn.CommitInBatch(ctx, b)
			},
			clientRetry: true, // cput with write too old requires restart
		},
		{
			name: "multi-range batch with write too old and successful cput",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "orig")
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "orig")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				b := txn.NewBatch()
				b.CPut("a", "cput", "orig")
				b.Put("c", "put")
				return txn.CommitInBatch(ctx, b)
			},
			clientRetry: true, // successful cput will still retry because of mixed success
		},
		{
			name: "cput within uncertainty interval",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "value")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.CPut(ctx, "a", "cput", "value")
			},
			filter:        newUncertaintyFilter(roachpb.Key([]byte("a"))),
			txnCoordRetry: true,
		},
		{
			name: "cput within uncertainty interval timestamp leaked",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "value")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.CPut(ctx, "a", "cput", "value")
			},
			filter:      newUncertaintyFilter(roachpb.Key([]byte("a"))),
			clientRetry: true,
			tsLeaked:    true,
		},
		{
			name: "reads within uncertainty interval",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "value")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				if _, err := txn.Get(ctx, "aa"); err != nil {
					return err
				}
				if _, err := txn.Get(ctx, "ab"); err != nil {
					return err
				}
				if _, err := txn.Get(ctx, "ac"); err != nil {
					return err
				}
				return txn.CPut(ctx, "a", "cput", "value")
			},
			filter:        newUncertaintyFilter(roachpb.Key([]byte("ac"))),
			txnCoordRetry: true,
		},
		{
			name: "reads within uncertainty interval and violating concurrent put",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "value")
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "ab", "value")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
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
			filter:      newUncertaintyFilter(roachpb.Key([]byte("ac"))),
			clientRetry: true, // note this txn is read-only but still restarts
		},
		{
			name: "multi range batch with uncertainty interval error",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "c", "value")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				if err := txn.Put(ctx, "a", "put"); err != nil {
					return err
				}
				b := txn.NewBatch()
				b.CPut("c", "cput", "value")
				return txn.CommitInBatch(ctx, b)
			},
			filter:        newUncertaintyFilter(roachpb.Key([]byte("c"))),
			txnCoordRetry: true, // will succeed because no mixed success
		},
		{
			name: "multi range batch with uncertainty interval error and get conflict",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "a", "init")
			},
			afterTxnStart: func(ctx context.Context, db *client.DB) error {
				if err := db.Put(ctx, "b", "value"); err != nil {
					return err
				}
				return db.Put(ctx, "a", "value")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				if _, err := txn.Get(ctx, "b"); err != nil {
					return err
				}
				b := txn.NewBatch()
				b.CPut("a", "cput", "value")
				return txn.CommitInBatch(ctx, b)
			},
			filter:      newUncertaintyFilter(roachpb.Key([]byte("a"))),
			clientRetry: true, // will fail because of conflict on refresh span for the Get
		},
		{
			name: "multi range batch with uncertainty interval error and mixed success",
			beforeTxnStart: func(ctx context.Context, db *client.DB) error {
				return db.Put(ctx, "c", "value")
			},
			retryable: func(ctx context.Context, txn *client.Txn) error {
				b := txn.NewBatch()
				b.Put("a", "put")
				b.CPut("c", "cput", "value")
				return txn.CommitInBatch(ctx, b)
			},
			filter:      newUncertaintyFilter(roachpb.Key([]byte("c"))),
			clientRetry: true, // client-side retry required as this will be an mixed success
		},
		{
			name: "multi range scan with uncertainty interval error",
			retryable: func(ctx context.Context, txn *client.Txn) error {
				_, err := txn.Scan(ctx, "a", "d", 0)
				return err
			},
			filter:        newUncertaintyFilter(roachpb.Key([]byte("c"))),
			txnCoordRetry: true, // can restart at higher timestamp despite mixed success because read-only
		},
		{
			name: "multi range DelRange with uncertainty interval error",
			retryable: func(ctx context.Context, txn *client.Txn) error {
				return txn.DelRange(ctx, "a", "d")
			},
			filter:      newUncertaintyFilter(roachpb.Key([]byte("c"))),
			clientRetry: true, // can't restart because of mixed success and write batch
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
				defer filterFn.Store((func(storagebase.FilterArgs) *roachpb.Error)(nil))
			}

			var metrics kv.TxnMetrics
			var lastAutoRetries int64
			var hadClientRetry bool
			epoch := 0
			if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
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

				metrics = txn.Sender().(*kv.TxnCoordSender).TxnCoordSenderFactory.Metrics()
				lastAutoRetries = metrics.AutoRetries.Count()

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
			autoRetries := metrics.AutoRetries.Count() - lastAutoRetries
			if tc.txnCoordRetry && autoRetries == 0 {
				t.Errorf("expected [at least] one txn coord sender auto retry; got %d", autoRetries)
			}
			if tc.clientRetry && !hadClientRetry {
				t.Errorf("expected but did not experience client retry")
			}
		})
	}
}
