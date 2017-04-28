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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// NOTE: these tests are in package kv_test to avoid a circular
// dependency between the server and kv packages. These tests rely on
// starting a TestServer, which creates a "real" node and employs a
// distributed sender server-side.

// TestRangeLookupWithOpenTransaction verifies that range lookups are
// done in such a way (e.g. using inconsistent reads) that they
// proceed in the event that a write intent is extant at the meta
// index record being read.
func TestRangeLookupWithOpenTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := createTestClient(t, s)

	// Create an intent on the meta1 record by writing directly to the
	// engine.
	key := testutils.MakeKey(keys.Meta1Prefix, roachpb.KeyMax)
	now := s.Clock().Now()
	txn := roachpb.NewTransaction("txn", roachpb.Key("foobar"), 0, enginepb.SERIALIZABLE, now, 0)
	if err := engine.MVCCPutProto(
		context.Background(), s.(*server.TestServer).Engines()[0],
		nil, key, now, txn, &roachpb.RangeDescriptor{}); err != nil {
		t.Fatal(err)
	}

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
func setupMultipleRanges(
	t *testing.T, s serverutils.TestServerInterface, splitAt ...string,
) *client.DB {
	db := createTestClient(t, s)

	// Split the keyspace at the given keys.
	for _, key := range splitAt {
		if err := db.AdminSplit(context.TODO(), key); err != nil {
			// Don't leak server goroutines.
			t.Fatal(err)
		}
	}

	return db
}

var errInfo = testutils.MakeCaller(3, 2)

// checks the keys returned from a Scan/ReverseScan.
func checkSpanResults(
	t *testing.T, spans [][]string, results []client.Result, expResults [][]string, expCount int,
) {
	if len(expResults) != len(results) {
		t.Fatalf("only got %d results, wanted %d", len(expResults), len(results))
	}
	// Ensure all the keys returned align properly with what is expected.
	count := 0
	for i, res := range results {
		count += len(res.Rows)
		for j, kv := range res.Rows {
			if key, expKey := string(kv.Key), expResults[i][j]; key != expKey {
				t.Errorf("%s: expected scan key (%d, %d) to be %q; got %q", errInfo(), i, j, expKey, key)
			}
		}
		// Check ResumeSpan if the request hasn't been processed.
		if len(res.Rows) == 0 {
			// The row was not read; the resume span == request span.
			if key, expKey := string(res.ResumeSpan.Key), spans[i][0]; key != expKey {
				t.Errorf("%s: expected resume key %d, %d to be %q; got %q", errInfo(), i, expCount, expKey, key)
			}
			if key, expKey := string(res.ResumeSpan.EndKey), spans[i][1]; key != expKey {
				t.Errorf("%s: expected resume endkey %d, %d to be %q; got %q", errInfo(), i, expCount, expKey, key)
			}
		}
	}
	// The bound was respected.
	if count != expCount {
		t.Errorf("count = %d, expCount = %d", count, expCount)
	}
}

// checks ResumeSpan returned in a ScanResponse.
func checkResumeSpanScanResults(
	t *testing.T, spans [][]string, results []client.Result, expResults [][]string, expCount int,
) {
	for i, res := range results {
		rowLen := len(res.Rows)
		// Check ResumeSpan when request has been processed.
		if rowLen > 0 {
			if rowLen == len(expResults[i]) {
				// The key can be empty once the entire response is seen. It
				// is not guaranteed to be empty.
				if res.ResumeSpan.Key == nil {
					continue
				}
			}
			// The next start key is always greater than or equal to the last
			// key.Next() seen. It is either set to last key.Next, or the end
			// key of the range that the last key was a part of.
			if key, expKey := string(res.ResumeSpan.Key), string(roachpb.Key(expResults[i][rowLen-1]).Next()); key < expKey {
				t.Errorf("%s: expected resume key %d, %d to be %q; got %q", errInfo(), i, expCount, expKey, key)
			}
		} else {
			// The row was not read; the resume span key <= first seen key
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

// check ResumeSpan returned in a ReverseScanResponse.
func checkResumeSpanReverseScanResults(
	t *testing.T, spans [][]string, results []client.Result, expResults [][]string, expCount int,
) {
	for i, res := range results {
		// Check ResumeSpan when request has been processed.
		rowLen := len(res.Rows)
		if rowLen > 0 {
			// The key can be empty once the entire response is seen.
			if rowLen == len(expResults[i]) {
				if res.ResumeSpan.Key == nil {
					continue
				}
			}
			// The next end key is always less than or equal to the last key
			// seen.
			if key, expKey := string(res.ResumeSpan.EndKey), expResults[i][rowLen-1]; key > expKey {
				t.Errorf("%s: expected resume key %d, %d to be %q; got %q", errInfo(), i, expCount, expKey, key)
			}
		} else {
			// The row was not read; the resume span endkey >= first seen key.
			if key, expKey := string(res.ResumeSpan.EndKey), expResults[i][0]; key < expKey {
				t.Errorf("%s: expected resume endkey %d, %d to be %q; got %q", errInfo(), i, expCount, expKey, key)
			}
		}
		// The Key is untouched.
		if key, expKey := string(res.ResumeSpan.Key), spans[i][0]; key != expKey {
			t.Errorf("%s: expected resume key %d, %d to be %q; got %q", errInfo(), i, expCount, expKey, key)
		}
	}
}

// check an entire scan result including the ResumeSpan.
func checkScanResults(
	t *testing.T, spans [][]string, results []client.Result, expResults [][]string, expCount int,
) {
	checkSpanResults(t, spans, results, expResults, expCount)
	checkResumeSpanScanResults(t, spans, results, expResults, expCount)
}

// check an entire reverse scan result including the ResumeSpan.
func checkReverseScanResults(
	t *testing.T, spans [][]string, results []client.Result, expResults [][]string, expCount int,
) {
	checkSpanResults(t, spans, results, expResults, expCount)
	checkResumeSpanReverseScanResults(t, spans, results, expResults, expCount)
}

// Tests multiple scans across many ranges with multiple bounds.
func TestMultiRangeBoundedBatchScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	db := setupMultipleRanges(t, s, "a", "b", "c", "d", "e", "f")
	for _, key := range []string{"a1", "a2", "a3", "b1", "b2", "c1", "c2", "d1", "f1", "f2", "f3"} {
		if err := db.Put(ctx, key, "value"); err != nil {
			t.Fatal(err)
		}
	}

	// These are the expected results if there is no bound.
	expResults := [][]string{
		{"a1", "a2", "a3", "b1", "b2"},
		{"b1", "b2", "c1"},
		{"c1", "c2", "d1", "f1", "f2", "f3"},
		{"f2"},
	}
	maxExpCount := 0
	for _, res := range expResults {
		maxExpCount += len(res)
	}

	for bound := 1; bound <= 20; bound++ {
		b := &client.Batch{}
		b.Header.MaxSpanRequestKeys = int64(bound)

		spans := [][]string{{"a", "c"}, {"b", "c2"}, {"c", "g"}, {"f1a", "f2a"}}
		for _, span := range spans {
			b.Scan(span[0], span[1])
		}
		if err := db.Run(ctx, b); err != nil {
			t.Fatal(err)
		}

		expCount := maxExpCount
		if bound < maxExpCount {
			expCount = bound
		}
		checkScanResults(t, spans, b.Results, expResults, expCount)

		// Re-query using the resume spans that were returned; check that all
		// spans are read properly.
		if bound < maxExpCount {
			newB := &client.Batch{}
			for _, res := range b.Results {
				if res.ResumeSpan.Key != nil {
					newB.Scan(res.ResumeSpan.Key, res.ResumeSpan.EndKey)
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
			// Check that the Scan results contain all the expected results.
			checkScanResults(t, spans, b.Results, expResults, maxExpCount)
		}
	}
}

// Tests multiple reverse scans across many ranges with multiple bounds.
func TestMultiRangeBoundedBatchReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	db := setupMultipleRanges(t, s, "a", "b", "c", "d", "e", "f")
	for _, key := range []string{"a1", "a2", "a3", "b1", "b2", "c1", "c2", "d1", "f1", "f2", "f3"} {
		if err := db.Put(ctx, key, "value"); err != nil {
			t.Fatal(err)
		}
	}

	// These are the expected results if there is no bound
	expResults := [][]string{
		{"b2", "b1", "a3", "a2", "a1"},
		{"c1", "b2", "b1"},
		{"f3", "f2", "f1", "d1", "c2", "c1"},
		{"f2"},
	}
	maxExpCount := 0
	for _, res := range expResults {
		maxExpCount += len(res)
	}

	for bound := 1; bound <= 20; bound++ {
		b := &client.Batch{}
		b.Header.MaxSpanRequestKeys = int64(bound)

		spans := [][]string{{"a", "c"}, {"b", "c2"}, {"c", "g"}, {"f1a", "f2a"}}
		for _, span := range spans {
			b.ReverseScan(span[0], span[1])
		}
		if err := db.Run(ctx, b); err != nil {
			t.Fatal(err)
		}

		expCount := maxExpCount
		if bound < maxExpCount {
			expCount = bound
		}
		checkReverseScanResults(t, spans, b.Results, expResults, expCount)

		// Re-query using the resume spans that were returned; check that all
		// spans are read properly.
		if bound < maxExpCount {
			newB := &client.Batch{}
			for _, res := range b.Results {
				if res.ResumeSpan.Key != nil {
					newB.ReverseScan(res.ResumeSpan.Key, res.ResumeSpan.EndKey)
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
			// Check that the ReverseScan results contain all the expected
			// results.
			checkReverseScanResults(t, spans, b.Results, expResults, maxExpCount)
		}
	}
}

// TestMultiRangeBoundedBatchScanUnsortedOrder runs two non-overlapping
// scan requests out of order and shows how the batch response can
// contain two partial responses.
func TestMultiRangeBoundedBatchScanUnsortedOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	db := setupMultipleRanges(t, s, "a", "b", "c", "d", "e", "f")
	for _, key := range []string{"a1", "a2", "a3", "b1", "b2", "b3", "b4", "b5", "c1", "c2", "d1", "f1", "f2", "f3"} {
		if err := db.Put(context.TODO(), key, "value"); err != nil {
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
	if err := db.Run(context.TODO(), b); err != nil {
		t.Fatal(err)
	}
	// See incomplete results for the two requests.
	expResults := [][]string{
		{"b3", "b4", "b5"},
		{"a1", "a2", "a3"},
	}
	checkScanResults(t, spans, b.Results, expResults, bound)
}

// TestMultiRangeBoundedBatchScanSortedOverlapping runs two overlapping
// ordered (by start key) scan requests, and shows how the batch response can
// contain two partial responses.
func TestMultiRangeBoundedBatchScanSortedOverlapping(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	db := setupMultipleRanges(t, s, "a", "b", "c", "d", "e", "f")
	for _, key := range []string{"a1", "a2", "a3", "b1", "b2", "c1", "c2", "d1", "f1", "f2", "f3"} {
		if err := db.Put(context.TODO(), key, "value"); err != nil {
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
	if err := db.Run(context.TODO(), b); err != nil {
		t.Fatal(err)
	}
	// See incomplete results for the two requests.
	expResults := [][]string{
		{"a1", "a2", "a3", "b1", "b2"},
		{"b1"},
	}
	checkScanResults(t, spans, b.Results, expResults, bound)
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	db := setupMultipleRanges(t, s, "a", "b", "c", "d", "e", "f", "g", "h")

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
			if err := db.Put(context.TODO(), key, "value"); err != nil {
				t.Fatal(err)
			}
		}

		b := &client.Batch{}
		b.Header.MaxSpanRequestKeys = int64(bound)
		spans := [][]string{{"a", "c"}, {"c", "f"}, {"g", "h"}}
		for _, span := range spans {
			b.DelRange(span[0], span[1], true)
		}
		if err := db.Run(context.TODO(), b); err != nil {
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	db := setupMultipleRanges(t, s, "a", "b")
	// Check that a
	for _, key := range []string{"a1", "a2", "a3", "b1", "b2"} {
		if err := db.Put(context.TODO(), key, "value"); err != nil {
			t.Fatal(err)
		}
	}

	b := &client.Batch{}
	b.Header.MaxSpanRequestKeys = 3
	b.DelRange("a", "c", true)
	if err := db.Run(context.TODO(), b); err != nil {
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
	if err := db.Run(context.TODO(), b); err != nil {
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	db := setupMultipleRanges(t, s, "a", "b", "c", "d", "e", "f")

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
			if err := db.Put(context.TODO(), key, "value"); err != nil {
				t.Fatal(err)
			}
		}

		b := &client.Batch{}
		b.Header.MaxSpanRequestKeys = int64(bound)
		spans := [][]string{{"a", "b3"}, {"b", "d"}, {"c", "f2a"}, {"f1a", "g"}}
		for _, span := range spans {
			b.DelRange(span[0], span[1], true)
		}
		if err := db.Run(context.TODO(), b); err != nil {
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := setupMultipleRanges(t, s, "c", "d")

	// Delete the keys within a transaction. The range [c,d) doesn't have
	// any active requests.
	if err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := setupMultipleRanges(t, s, "a", "b")
	b := &client.Batch{}
	b.Header.MaxSpanRequestKeys = 100
	b.Scan("a", "b")
	b.ReverseScan("a", "b")
	if err := db.Run(context.TODO(), b); !testutils.IsError(
		err, "batch with limit contains both forward and reverse scans",
	) {
		t.Fatal(err)
	}
}

// TestMultiRangeScanReverseScanDeleteResolve verifies that Scan, ReverseScan,
// DeleteRange and ResolveIntentRange work across ranges.
func TestMultiRangeScanReverseScanDeleteResolve(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := setupMultipleRanges(t, s, "b")

	// Write keys before, at, and after the split key.
	for _, key := range []string{"a", "b", "c"} {
		if err := db.Put(context.TODO(), key, "value"); err != nil {
			t.Fatal(err)
		}
	}
	// Scan to retrieve the keys just written.
	if rows, err := db.Scan(context.TODO(), "a", "q", 0); err != nil {
		t.Fatalf("unexpected error on Scan: %s", err)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}

	// Scan in reverse order to retrieve the keys just written.
	if rows, err := db.ReverseScan(context.TODO(), "a", "q", 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}

	// Delete the keys within a transaction. Implicitly, the intents are
	// resolved via ResolveIntentRange upon completion.
	if err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()
		b.DelRange("a", "d", false)
		return txn.CommitInBatch(ctx, b)
	}); err != nil {
		t.Fatalf("unexpected error on transactional DeleteRange: %s", err)
	}

	// Scan consistently to make sure the intents are gone.
	if rows, err := db.Scan(context.TODO(), "a", "q", 0); err != nil {
		t.Fatalf("unexpected error on Scan: %s", err)
	} else if l := len(rows); l != 0 {
		t.Errorf("expected 0 rows; got %d", l)
	}

	// ReverseScan consistently to make sure the intents are gone.
	if rows, err := db.ReverseScan(context.TODO(), "a", "q", 0); err != nil {
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

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := setupMultipleRanges(t, s, "b")

	// Write keys "a" and "b", the latter of which is the first key in the
	// second range.
	keys := [2]string{"a", "b"}
	ts := [2]hlc.Timestamp{}
	for i, key := range keys {
		b := &client.Batch{}
		b.Put(key, "value")
		if err := db.Run(context.TODO(), b); err != nil {
			t.Fatal(err)
		}
		ts[i] = s.Clock().Now()
		log.Infof(context.TODO(), "%d: %s %d", i, key, ts[i])
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
			kv.DistSenderConfig{Clock: clock, RPCContext: s.RPCContext()},
			s.(*server.TestServer).Gossip(),
		)

		reply, err := client.SendWrappedWith(context.Background(), ds, roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
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
}

// TestParallelSender splits the keyspace 10 times and verifies that a
// scan across all and 10 puts to each range both use parallelizing
// dist sender.
func TestParallelSender(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	db := createTestClient(t, s)

	// Split into multiple ranges.
	splitKeys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	for _, key := range splitKeys {
		if err := db.AdminSplit(context.TODO(), key); err != nil {
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
	db := createTestClient(t, s)

	// Set up multiple ranges:
	// ["", "b"),["b", "e") ,["e", "g") and ["g", "\xff\xff").
	for _, key := range []string{"b", "e", "g"} {
		// Split the keyspace at the given key.
		if err := db.AdminSplit(context.TODO(), key); err != nil {
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
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

// TestBatchPutWithConcurrentSplit creates a batch with a series of put
// requests and splits the middle of the range in order to trigger
// reentrant invocation of DistSender.divideAndSendBatchToRanges. See
// #12603 for more details.
func TestBatchPutWithConcurrentSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	db := createTestClient(t, s)

	// Split first using the default client and scan to make sure that
	// the range descriptor cache reflects the split.
	for _, key := range []string{"b", "f"} {
		if err := db.AdminSplit(context.TODO(), key); err != nil {
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
		kv.DistSenderConfig{Clock: s.Clock(), RPCContext: s.RPCContext()}, s.(*server.TestServer).Gossip(),
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := initReverseScanTestEnv(s, t)

	// Case 1: An encounter with a range split.
	// Split the range ["b", "e") at "c".
	if err := db.AdminSplit(context.TODO(), "c"); err != nil {
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := createTestClient(t, s)
	ctx := context.TODO()

	// Write key "a".
	if err := db.Put(ctx, "a", "value"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Scan(ctx, "a", "a", 0); !testutils.IsError(err, "truncation resulted in empty batch") {
		t.Fatalf("unexpected error on scan with startkey == endkey: %v", err)
	}

	if _, err := db.ReverseScan(ctx, "a", "a", 0); !testutils.IsError(err, "truncation resulted in empty batch") {
		t.Fatalf("unexpected error on reverse scan with startkey == endkey: %v", err)
	}

	if err := db.DelRange(ctx, "x", "a"); !testutils.IsError(err, "truncation resulted in empty batch") {
		t.Fatalf("unexpected error on deletion on [x, a): %v", err)
	}

	if err := db.DelRange(ctx, "", "z"); !testutils.IsError(err, "must be greater than LocalMax") {
		t.Fatalf("unexpected error on deletion on [KeyMin, z): %v", err)
	}
}

// TestNoSequenceCachePutOnRangeMismatchError verifies that the
// sequence cache is not updated with RangeKeyMismatchError. This is a
// higher-level version of TestSequenceCacheShouldCache.
func TestNoSequenceCachePutOnRangeMismatchError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := setupMultipleRanges(t, s, "b", "c")

	// The requests in the transaction below will be chunked and
	// sent to replicas in the following way:
	// 1) A batch request containing a BeginTransaction and a
	//    put on "a" are sent to a replica owning range ["a","b").
	// 2) A next batch request containing a put on "b" and a put
	//    on "c" are sent to a replica owning range ["b","c").
	//   (The range cache has a stale range descriptor.)
	// 3) The put request on "c" causes a RangeKeyMismatchError.
	// 4) The dist sender re-sends a request to the same replica.
	//    This time the request contains only the put on "b" to the
	//    same replica.
	// 5) The command succeeds since the sequence cache has not yet been updated.
	epoch := 0
	if err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		epoch++
		b := txn.NewBatch()
		b.Put("a", "val")
		b.Put("b", "val")
		b.Put("c", "val")
		return txn.CommitInBatch(ctx, b)
	}); err != nil {
		t.Errorf("unexpected error on transactional Puts: %s", err)
	}

	if epoch != 1 {
		t.Errorf("unexpected epoch; the txn must not be retried, but got %d retries", epoch)
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
	storeKnobs.TestingEvalFilter =
		func(fArgs storagebase.FilterArgs) *roachpb.Error {
			_, ok := fArgs.Req.(*roachpb.ConditionalPutRequest)
			if ok && fArgs.Req.Header().Key.Equal(targetKey) {
				if atomic.AddInt32(&numGets, 1) == 1 {
					pErr := roachpb.NewReadWithinUncertaintyIntervalError(hlc.Timestamp{}, hlc.Timestamp{})
					return roachpb.NewErrorWithTxn(pErr, fArgs.Hdr.Txn)
				}
			}
			return nil
		}
	s, _, _ := serverutils.StartServer(t,
		base.TestServerArgs{Knobs: base.TestingKnobs{Store: &storeKnobs}})
	defer s.Stopper().Stop(context.TODO())

	db := setupMultipleRanges(t, s, "b")

	// Set the initial value on the target key "b".
	origVal := "val"
	if err := db.Put(context.TODO(), targetKey, origVal); err != nil {
		t.Fatal(err)
	}

	// The following txn creates a batch request that is split
	// into two requests: Put and CPut. The CPut operation will
	// get a ReadWithinUncertaintyIntervalError and the txn will be
	// retried.
	epoch := 0
	if err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
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
