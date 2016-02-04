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

	// One error sent by each client. A successful client sends a nil error.
	errs := make(chan error, num)
	start := time.Now()
	deadline := start.Add(*flagDuration)
	// The number of successful writes (puts) to the database.
	var count int64
	// The number of times chaos monkey has run.
	var round int64
	// Set to 1 if chaos monkey has stalled the writes.
	var stalled int32
	clients := make([]struct {
		sync.RWMutex
		db      *client.DB
		stopper *stop.Stopper
		count   int64
	}, num)

	// initClient requires that the caller holds the client's write lock.
	initClient := func(i int) {
		db, dbStopper := makeClient(t, c.ConnString(i))
		if clients[i].stopper != nil {
			clients[i].stopper.Stop()
		}
		clients[i].db, clients[i].stopper = db, dbStopper
	}

	done := func() bool {
		return !time.Now().Before(deadline) || atomic.LoadInt32(&stalled) == 1
	}

	for i := 0; i < num; i++ {
		clients[i].Lock()
		initClient(i)
		clients[i].Unlock()
		go func(i int) {
			r, _ := randutil.NewPseudoRand()
			value := randutil.RandBytes(r, 8192)
			k := atomic.LoadInt64(&count)
			var prevErrorOutput string
			for !done() {
				if err := func() error {
					clients[i].RLock()
					defer clients[i].RUnlock()
					v := value[:r.Intn(len(value))]
					if pErr := clients[i].db.Put(fmt.Sprintf("%08d", k), v); pErr != nil {
						err := pErr.GoError()
						if _, ok := err.(*roachpb.SendError); ok || testutils.IsError(err, rpc.ErrShutdown.Error()) || testutils.IsError(err, "client is unhealthy") {
							// Common errors we can ignore. Also suppress the
							// log messages so they don't get spammy.
							curErrorOutput := fmt.Sprintf("client %d: %s", i, err)
							if prevErrorOutput != curErrorOutput {
								log.Warning(curErrorOutput)
								prevErrorOutput = curErrorOutput
							}
						} else {
							// Unknown error.
							return err
						}
					} else {
						// Only advance the counts on a successful put.
						k = atomic.AddInt64(&count, 1)
						atomic.AddInt64(&clients[i].count, 1)
					}
					return nil
				}(); err != nil {
					// Report the err and terminate.
					errs <- err
					return
				}
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
		for atomic.StoreInt64(&round, 1); !done(); atomic.AddInt64(&round, 1) {
			curRound := atomic.LoadInt64(&round)
			select {
			case <-stopper:
				return
			default:
			}
			nodes := rnd.Perm(num)[:rnd.Intn(num)+1]
			// Prevent all clients from writing while nodes are being restarted.
			for i := 0; i < num; i++ {
				clients[i].Lock()
			}
			log.Infof("round %d: restarting nodes %v", curRound, nodes)
			for _, i := range nodes {
				// Two early exit conditions.
				select {
				case <-stopper:
					break
				default:
				}
				if done() {
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
			// Sleep until at least one client is writing successfully.
			first := true
			for cur := atomic.LoadInt64(&count); !done() && atomic.LoadInt64(&count) == cur; time.Sleep(time.Second) {
				c.Assert(t)
				if first {
					first = false
					log.Warningf("round %d: monkey sleeping while cluster recovers...", curRound)
				}
			}
		}
	}()

	prevRound := atomic.LoadInt64(&round)
	stallTime := time.Now().Add(*flagStall)
	var prevOutput string
	// Spin until all clients are shut.
	for numShutClients := 0; numShutClients < num; {
		select {
		case <-teardown:
		case <-stopper:
			t.Fatal("interrupted")

		case err := <-errs:
			if err != nil {
				t.Error(err)
			}
			numShutClients++

		case <-time.After(time.Second):
			var newOutput string
			if time.Now().Before(deadline) {
				curRound := atomic.LoadInt64(&round)
				if curRound == prevRound {
					if time.Now().After(stallTime) {
						atomic.StoreInt32(&stalled, 1)
						t.Fatalf("Stall detected at round %d, no forward progress for %s", curRound, *flagStall)
					}
				} else {
					prevRound = curRound
					stallTime = time.Now().Add(*flagStall)
				}
				// Periodically print out progress so that we know the test is
				// still running and making progress.
				cur := make([]string, num)
				for j := range cur {
					cur[j] = fmt.Sprintf("%d", atomic.LoadInt64(&clients[j].count))
				}
				newOutput = fmt.Sprintf("round %d: %d (%s)", curRound, atomic.LoadInt64(&count), strings.Join(cur, ", "))
			} else {
				newOutput = fmt.Sprintf("test finished, waiting for shutdown of %d clients", num-numShutClients)
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
