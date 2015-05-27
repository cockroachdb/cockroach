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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// +build acceptance

package acceptance

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// TestPut starts up an N node cluster and runs N workers that write
// to independent keys.
func TestPut(t *testing.T) {
	l := localcluster.Create(*numNodes, stopper)
	l.Start()
	defer l.Stop()

	checkRangeReplication(t, l, 20*time.Second)

	c, err := makeDBClient(l, 0)
	if err != nil {
		t.Fatal(err)
	}

	r, _ := util.NewPseudoRand()
	value := util.RandBytes(r, 8192)

	errs := make(chan error, *numNodes)
	start := time.Now()
	deadline := start.Add(*duration)
	var count int64
	for i := 0; i < *numNodes; i++ {
		go func() {
			for time.Now().Before(deadline) {
				k := atomic.AddInt64(&count, 1)
				v := value[:r.Intn(len(value))]
				if _, err := c.Put(fmt.Sprintf("%08d", k), v); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
	}

	for i := 0; i < *numNodes; {
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
			log.Infof("%d", atomic.LoadInt64(&count))
		}
	}

	elapsed := time.Since(start)
	log.Infof("%d %.1f/sec", count, float64(count)/elapsed.Seconds())
}
