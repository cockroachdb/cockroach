// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// TestSingleKey stresses the transaction retry machinery by starting
// up an N node cluster and running N workers that are all
// incrementing the value associated with a single key.
func TestSingleKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t)

	const num = 3
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
		})
	defer tc.Stopper().Stop(context.Background())
	ctx := context.Background()

	// Initialize the value for our test key to zero.
	const key = "test-key"
	initDB := tc.Servers[0].DB()
	if err := initDB.Put(ctx, key, 0); err != nil {
		t.Fatal(err)
	}

	type result struct {
		err        error
		maxLatency time.Duration
	}

	resultCh := make(chan result, num)
	deadline := timeutil.Now().Add(5 * time.Second)
	var expected int64

	// Start up num workers each reading and writing the same
	// key. Each worker is configured to talk to a different node in the
	// cluster.
	for i := 0; i < num; i++ {
		db := tc.Servers[i].DB()
		go func() {
			var r result
			for timeutil.Now().Before(deadline) {
				start := timeutil.Now()
				err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					minExp := atomic.LoadInt64(&expected)
					r, err := txn.Get(ctx, key)
					if err != nil {
						return err
					}
					b := txn.NewBatch()
					v := r.ValueInt()
					b.Put(key, v+1)
					err = txn.CommitInBatch(ctx, b)
					// Atomic updates after the fact mean that we should read
					// exp or larger (since concurrent writers might have
					// committed but not yet performed their atomic update).
					if err == nil && v < minExp {
						return errors.Errorf("unexpected read: %d, expected >= %d", v, minExp)
					}
					return err
				})
				if err != nil {
					resultCh <- result{err: err}
					return
				}
				atomic.AddInt64(&expected, 1)
				latency := timeutil.Since(start)
				if r.maxLatency < latency {
					r.maxLatency = latency
				}
			}
			resultCh <- r
		}()
	}

	// Verify that none of the workers encountered an error.
	var results []result
	for len(results) < num {
		select {
		case <-tc.Stopper().ShouldQuiesce():
			t.Fatalf("interrupted")
		case r := <-resultCh:
			if r.err != nil {
				t.Fatal(r.err)
			}
			results = append(results, r)
		case <-time.After(1 * time.Second):
			// Periodically print out progress so that we know the test is still
			// running.
			log.Infof(ctx, "%d", atomic.LoadInt64(&expected))
		}
	}

	// Verify the resulting value stored at the key is what we expect.
	r, err := initDB.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	v := r.ValueInt()
	if expected != v {
		t.Fatalf("expected %d, but found %d", expected, v)
	}
	var maxLatency []time.Duration
	for _, r := range results {
		maxLatency = append(maxLatency, r.maxLatency)
	}
	log.Infof(ctx, "%d increments: %s", v, maxLatency)
}
