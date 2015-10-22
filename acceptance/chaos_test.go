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
// Author: Tobias Schottdorf (tobias@cockroachlabs.com)

// +build acceptance

package acceptance

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/stop"
)

// TestChaos starts up a cluster and, for each node, a worker writing to
// independent keys, while nodes are being killed and restarted continuously.
// The test measures not write performance, but cluster recovery.
func TestChaos(t *testing.T) {
	l := localcluster.Create(*numNodes, stopper)
	l.Start()
	defer l.AssertAndStop(t)

	checkRangeReplication(t, l, 20*time.Second)

	errs := make(chan error, *numNodes)
	start := time.Now()
	deadline := start.Add(*duration)
	var count int64
	counts := make([]int64, *numNodes)
	clients := make([]struct {
		sync.RWMutex
		db      *client.DB
		stopper *stop.Stopper
	}, *numNodes)

	initClient := func(i int) {
		db, dbStopper := makeDBClient(t, l, i)
		if clients[i].stopper != nil {
			clients[i].stopper.Stop()
		}
		clients[i].db, clients[i].stopper = db, dbStopper
	}

	for i := 0; i < *numNodes; i++ {
		initClient(i)
		go func(i int) {
			r, _ := randutil.NewPseudoRand()
			value := randutil.RandBytes(r, 8192)

			for time.Now().Before(deadline) {
				clients[i].RLock()
				k := atomic.AddInt64(&count, 1)
				atomic.AddInt64(&counts[i], 1)
				v := value[:r.Intn(len(value))]
				if err := clients[i].db.Put(fmt.Sprintf("%08d", k), v); err != nil {
					// These originate from DistSender when, for example, the
					// leader is down. With more realistic retry options, we
					// should probably not see them.
					if _, ok := err.(*roachpb.SendError); ok {
						log.Warning(err)
					} else {
						errs <- err
						clients[i].RUnlock()
						return
					}
				}
				clients[i].RUnlock()
			}
			errs <- nil
		}(i)
	}

	teardown := make(chan struct{})
	defer func() {
		<-teardown
		for i := range clients {
			clients[i].stopper.Stop()
			clients[i].stopper = nil
		}
	}()

	// Chaos monkey.
	go func() {
		defer close(teardown)
		rnd, seed := randutil.NewPseudoRand()
		log.Warningf("monkey starts (seed %d)", seed)
		for round := 1; time.Now().Before(deadline); round++ {
			select {
			case <-stopper:
				return
			default:
			}
			nodes := rnd.Perm(*numNodes)[:rnd.Intn(*numNodes)+1]

			log.Infof("round %d: restarting nodes %v", round, nodes)
			for _, i := range nodes {
				clients[i].Lock()
			}
			for _, i := range nodes {
				log.Infof("restarting %v", i)
				l.Nodes[i].Kill()
				l.Nodes[i].Restart(5)
				initClient(i)
				clients[i].Unlock()
			}
			for cur := atomic.LoadInt64(&count); time.Now().Before(deadline) &&
				atomic.LoadInt64(&count) == cur; time.Sleep(time.Second) {
				l.Assert(t)
				log.Warningf("monkey sleeping while cluster recovers...")
			}
		}
	}()

	for i := 0; i < *numNodes; {
		select {
		case <-teardown:
		case <-stopper:
			t.Fatal("interrupted")
		case err := <-errs:
			if err != nil {
				t.Error(err)
			}
			i++
		case <-time.After(1 * time.Second):
			// Periodically print out progress so that we know the test is still
			// running.
			cur := make([]string, *numNodes)
			for i := range cur {
				cur[i] = fmt.Sprintf("%d", atomic.LoadInt64(&counts[i]))
			}
			log.Infof("%d (%s)", atomic.LoadInt64(&count), strings.Join(cur, ", "))
		}
	}

	elapsed := time.Since(start)
	log.Infof("%d %.1f/sec", count, float64(count)/elapsed.Seconds())
}
