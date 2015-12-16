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
	"net/rpc"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/stop"
)

// TestChaos starts up a cluster and, for each node, a worker writing to
// independent keys, while nodes are being killed and restarted continuously.
// The test measures not write performance, but cluster recovery.
func TestChaos(t *testing.T) {
	c := StartCluster(t)
	defer c.AssertAndStop(t)

	num := c.NumNodes()

	errs := make(chan error, num)
	start := time.Now()
	deadline := start.Add(*duration)
	var count int64
	var round int64
	counts := make([]int64, num)
	clients := make([]struct {
		sync.RWMutex
		db      *client.DB
		stopper *stop.Stopper
	}, num)

	// initClient requires that the caller holds the client's write lock.
	initClient := func(i int) {
		db, dbStopper := makeClient(t, c.ConnString(i))
		if clients[i].stopper != nil {
			clients[i].stopper.Stop()
		}
		clients[i].db, clients[i].stopper = db, dbStopper
	}

	for i := 0; i < num; i++ {
		clients[i].Lock()
		initClient(i)
		clients[i].Unlock()
		go func(i int) {
			r, _ := randutil.NewPseudoRand()
			value := randutil.RandBytes(r, 8192)
			k := atomic.LoadInt64(&count)

			for time.Now().Before(deadline) {
				clients[i].RLock()
				v := value[:r.Intn(len(value))]
				// TODO(bram): fix the retry options so the puts don't block
				// occasionally for a full min during shutdown.
				if err := clients[i].db.Put(fmt.Sprintf("%08d", k), v); err != nil {
					// These originate from DistSender when, for example, the
					// leader is down. With more realistic retry options, we
					// should probably not see them.
					if _, ok := err.(*roachpb.SendError); ok || testutils.IsError(err, rpc.ErrShutdown.Error()) || testutils.IsError(err, "client is unhealthy") {
						log.Warning(err)
					} else {
						errs <- err
						clients[i].RUnlock()
						return
					}
				}
				k = atomic.AddInt64(&count, 1)
				atomic.AddInt64(&counts[i], 1)
				clients[i].RUnlock()
			}
			log.Infof("client %d shutting down", i)
			errs <- nil
		}(i)
	}

	teardown := make(chan struct{})
	defer func() {
		<-teardown
		for i := range clients {
			clients[i].RLock()
			clients[i].stopper.Stop()
			clients[i].stopper = nil
			clients[i].RUnlock()
		}
	}()

	// Chaos monkey.
	go func() {
		defer close(teardown)
		rnd, seed := randutil.NewPseudoRand()
		log.Warningf("monkey starts (seed %d)", seed)
		for atomic.StoreInt64(&round, 1); time.Now().Before(deadline); atomic.AddInt64(&round, 1) {
			curRound := atomic.LoadInt64(&round)
			select {
			case <-stopper:
				return
			default:
			}
			nodes := rnd.Perm(num)[:rnd.Intn(num)+1]
			log.Infof("round %d: restarting nodes %v", curRound, nodes)
			for i := 0; i < num; i++ {
				clients[i].Lock()
			}
			for _, i := range nodes {
				// Two early exit conditions.
				select {
				case <-stopper:
					break
				default:
				}
				if time.Now().After(deadline) {
					break
				}
				log.Infof("round %d: restarting %d", curRound, i)
				c.Kill(i)
				c.Restart(i)
				initClient(i)
			}
			for i := 0; i < num; i++ {
				clients[i].Unlock()
			}
			for cur := atomic.LoadInt64(&count); time.Now().Before(deadline) &&
				atomic.LoadInt64(&count) == cur; time.Sleep(time.Second) {
				c.Assert(t)
				log.Warningf("round %d: monkey sleeping while cluster recovers...", curRound)
			}
		}
	}()

	prevRound := atomic.LoadInt64(&round)
	stallTime := time.Now().Add(*stall)
	var prevOutput string
	for i := 0; i < num; {
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
			var newOutput string
			if time.Now().Before(deadline) {
				curRound := atomic.LoadInt64(&round)
				if curRound == prevRound {
					if time.Now().After(stallTime) {
						t.Fatalf("Stall detected, no forward progress for %s", *stall)
					}
				} else {
					prevRound = curRound
					stallTime = time.Now().Add(*stall)
				}
				// Periodically print out progress so that we know the test is
				// still running and making progress.
				cur := make([]string, num)
				for j := range cur {
					cur[j] = fmt.Sprintf("%d", atomic.LoadInt64(&counts[j]))
				}
				newOutput = fmt.Sprintf("round %d: %d (%s)", curRound, atomic.LoadInt64(&count), strings.Join(cur, ", "))
			} else {
				newOutput = fmt.Sprintf("test finished, waiting for shutdown of %d clients", num-i)
			}
			// This just stops the logs from being a bit too spammy.
			if newOutput != prevOutput {
				log.Infof(newOutput)
				prevOutput = newOutput
			}
		}
	}

	elapsed := time.Since(start)
	log.Infof("%d %.1f/sec", count, float64(count)/elapsed.Seconds())
}
