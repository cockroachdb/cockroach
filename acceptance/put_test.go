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
// Author: Peter Mattis (peter@cockroachlabs.com)

package acceptance

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

// TestPut starts up an N node cluster and runs N workers that write
// to independent keys.
func TestPut(t *testing.T) {
	runTestOnConfigs(t, testPutInner)
}

func testPutInner(t *testing.T, c cluster.Cluster, cfg cluster.TestConfig) {
	db, dbStopper := c.NewClient(t, 0)
	defer dbStopper.Stop()

	errs := make(chan error, c.NumNodes())
	start := timeutil.Now()
	deadline := start.Add(cfg.Duration)
	var count int64
	for i := 0; i < c.NumNodes(); i++ {
		go func() {
			r, _ := randutil.NewPseudoRand()
			value := randutil.RandBytes(r, 8192)

			for timeutil.Now().Before(deadline) {
				k := atomic.AddInt64(&count, 1)
				v := value[:r.Intn(len(value))]
				if pErr := db.Put(fmt.Sprintf("%08d", k), v); pErr != nil {
					errs <- pErr.GoError()
					return
				}
			}
			errs <- nil
		}()
	}

	for i := 0; i < c.NumNodes(); {
		baseCount := atomic.LoadInt64(&count)
		select {
		case <-stopper:
			t.Fatalf("interrupted")
		case err := <-errs:
			if err != nil {
				t.Fatal(err)
			}
			i++
		case <-time.After(1 * time.Second):
			// Periodically print out progress so that we know the test is still
			// running.
			loadedCount := atomic.LoadInt64(&count)
			log.Infof("%d (%d/s)", loadedCount, loadedCount-baseCount)
			c.Assert(t)
			cluster.Consistent(t, c)
		}
	}

	elapsed := time.Since(start)
	log.Infof("%d %.1f/sec", count, float64(count)/elapsed.Seconds())
}
