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

package storage_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TestPut starts up an N node cluster and runs N workers that write
// to independent keys.
func TestPut(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if testing.Short() {
		t.Skip("short flag")
	}

	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
		})
	defer tc.Stopper().Stop(context.TODO())
	ctx := context.Background()

	db := tc.Servers[0].DB()

	errs := make(chan error, len(tc.Servers))
	start := timeutil.Now()
	deadline := start.Add(5 * time.Second)
	var count int64
	for i := 0; i < len(tc.Servers); i++ {
		go func() {
			r, _ := randutil.NewPseudoRand()
			value := randutil.RandBytes(r, 8192)

			for timeutil.Now().Before(deadline) {
				k := atomic.AddInt64(&count, 1)
				v := value[:r.Intn(len(value))]
				if err := db.Put(ctx, fmt.Sprintf("%08d", k), v); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
	}

	for i := 0; i < len(tc.Servers); {
		baseCount := atomic.LoadInt64(&count)
		select {
		case err := <-errs:
			if err != nil {
				t.Fatal(err)
			}
			i++
		case <-time.After(1 * time.Second):
			// Periodically print out progress so that we know the test is still
			// running.
			loadedCount := atomic.LoadInt64(&count)
			log.Infof(ctx, "%d (%d/s)", loadedCount, loadedCount-baseCount)
			if err := db.CheckConsistency(
				ctx, keys.LocalMax, keys.MaxKey, false /* withDiff*/); err != nil {
				t.Fatal(err)
			}
		}
	}

	elapsed := timeutil.Since(start)
	log.Infof(ctx, "%d %.1f/sec", count, float64(count)/elapsed.Seconds())
}
