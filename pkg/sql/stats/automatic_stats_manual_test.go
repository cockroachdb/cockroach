// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stats_test

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var runManual = flag.Bool(
	"run-manual", false,
	"run manual tests",
)

// TestAdaptiveThrottling is a "manual" test: it runs automatic statistics with
// varying load on the system and prints out the times. It should be run on a
// lightly loaded system using:
//
//   make test PKG=./pkg/sql/stats TESTS=AdaptiveThrottling TESTFLAGS='-v --run-manual -logtostderr NONE'
//
// Sample output:
//
// --- PASS: TestAdaptiveThrottling (114.51s)
//     automatic_stats_manual_test.go:72: Populate table took 7.639067726s
//     automatic_stats_manual_test.go:72: --- Load 0% ---
//     automatic_stats_manual_test.go:72: Create stats took 1.198634729s
//     automatic_stats_manual_test.go:72: --- Load 30% ---
//     automatic_stats_manual_test.go:72: Create stats took 2.270165784s
//     automatic_stats_manual_test.go:72: --- Load 50% ---
//     automatic_stats_manual_test.go:72: Create stats took 7.324599981s
//     automatic_stats_manual_test.go:72: --- Load 70% ---
//     automatic_stats_manual_test.go:72: Create stats took 15.886412857s
//
func TestAdaptiveThrottling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	if !*runManual {
		skip.IgnoreLint(t, "manual test with no --run-manual")
	}
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	r := sqlutils.MakeSQLRunner(sqlDB)
	r.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false")
	r.Exec(t, "CREATE TABLE xyz (x INT, y INT, z INT)")

	// log prints the message to stdout and to the test log.
	log := func(msg string) {
		fmt.Println(msg)
		t.Log(msg)
	}

	step := func(msg string, fn func()) {
		fmt.Println(msg)
		before := timeutil.Now()
		fn()
		log(fmt.Sprintf("%s took %s", msg, timeutil.Now().Sub(before)))
	}

	step("Populate table", func() {
		for i := 0; i < 200; i++ {
			r.Exec(t, "INSERT INTO xyz SELECT x, 2*x, 3*x FROM generate_series(1, 1000) AS g(x)")
		}
	})

	for _, load := range []int{0, 3, 5, 7} {
		log(fmt.Sprintf("--- Load %d%% ---", load*10))
		// Set up a load on each CPU.
		cancel := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < runtime.GOMAXPROCS(0); i++ {
			wg.Add(1)
			go func() {
				runLoad(load, cancel)
				wg.Done()
			}()
		}

		// Sleep for 2 * DefaultMetricsSampleInterval, to make sure the runtime
		// stats reflect the load we want.
		sleep := 2 * base.DefaultMetricsSampleInterval
		fmt.Printf("Sleeping for %s\n", sleep)
		time.Sleep(sleep)
		step("Create stats", func() {
			r.Exec(t, "CREATE STATISTICS __auto__ FROM xyz")
		})
		close(cancel)
		wg.Wait()
	}
}

// runLoad runs a goroutine that runs a load for (<load> / 10) of the time.
// It stops when the cancel channel is closed.
func runLoad(load int, cancel chan struct{}) {
	if load == 0 {
		<-cancel
		return
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for idx := 0; ; idx = (idx + 1) % 10 {
		if idx >= load {
			// No work this time slice; just wait until the ticker fires.
			select {
			case <-cancel:
				return
			case <-ticker.C:
			}
			continue
		}
		for done := false; !done; {
			select {
			case <-cancel:
				return
			case <-ticker.C:
				done = true
			default:
				// Do some work: find the first 100 prime numbers.
				for x, count := 2, 0; count < 100; x++ {
					prime := true
					for i := 2; i*i < x; i++ {
						if x%i == 0 {
							prime = false
							break
						}
					}
					if prime {
						count++
					}
				}
			}
		}
	}
}
