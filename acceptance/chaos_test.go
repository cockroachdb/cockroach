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
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/stop"
)

var maxTransfer = flag.Int("max-transfer", 999, "Maximum amount to transfer in one transaction.")
var numAccounts = flag.Int("num-accounts", 999, "Number of accounts.")

// TestChaos starts up a cluster with an "accounts" table.
// It starts transferring money between accounts, while nodes are
// being killed and restarted continuously.
// The test doesn't measure write performance, but cluster recovery.
// TODO(vivek): Expand this test to check that write performance
// is unaffected by chaos.
func TestChaos(t *testing.T) {
	c := StartCluster(t)
	defer c.AssertAndStop(t)

	num := c.NumNodes()
	if num <= 0 {
		t.Fatalf("%d nodes in cluster", num)
	}

	// One error sent by each client. A successful client sends a nil error.
	errs := make(chan error, num)
	// The number of times chaos monkey has run.
	var round uint64
	// Set to 1 if chaos monkey has stalled the writes.
	var stalled int32
	// One client for each node.
	clients := make([]struct {
		sync.RWMutex
		db      *sql.DB
		stopper *stop.Stopper
		count   uint64
	}, num)

	// initClient initializes the client talking to node "i".
	// It requires that the caller hold the client's write lock.
	initClient := func(i int) {
		if clients[i].stopper != nil {
			clients[i].stopper.Stop()
		}
		clients[i].db = makePGClient(t, c.PGUrl(i))
		clients[i].stopper = stop.NewStopper()
	}

	// Initialize the "accounts" table.
	db := makePGClient(t, c.PGUrl(0))

	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS bank`); err != nil {
		t.Fatal(err)
	}

	// Delete table created by a prior instance of a test.
	if _, err := db.Exec(`DROP TABLE IF EXISTS bank.accounts`); err != nil {
		t.Fatal(err)
	}

	schema := `
CREATE TABLE bank.accounts (
  id INT PRIMARY KEY,
  balance INT NOT NULL
)`
	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}

	var placeholders bytes.Buffer
	var values []interface{}
	for i := 0; i < *numAccounts; i++ {
		if i > 0 {
			placeholders.WriteString(", ")
		}
		fmt.Fprintf(&placeholders, "($%d, 0)", i+1)
		values = append(values, i)
	}
	stmt := `INSERT INTO bank.accounts (id, balance) VALUES ` + placeholders.String()
	if _, err := db.Exec(stmt, values...); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	deadline := start.Add(*flagDuration)
	done := func() bool {
		return !time.Now().Before(deadline) || atomic.LoadInt32(&stalled) == 1
	}

	for i := 0; i < num; i++ {
		clients[i].Lock()
		initClient(i)
		clients[i].Unlock()
		go func(i int) {
			for !done() {
				if err := func() error {
					clients[i].RLock()
					defer clients[i].RUnlock()
					from := rand.Intn(*numAccounts)
					to := rand.Intn(*numAccounts - 1)
					if from == to {
						to = *numAccounts - 1
					}
					amount := rand.Intn(*maxTransfer)

					const update = `
									UPDATE bank.accounts
									  SET balance = CASE id WHEN $1 THEN balance-$3 WHEN $2 THEN balance+$3 END
										  WHERE id IN ($1, $2) AND (SELECT balance >= $3 FROM bank.accounts WHERE id = $1)`
					if _, err := clients[i].db.Exec(update, from, to, amount); err != nil {
						// Ignore some errors.
						if testutils.IsError(err, "connection refused") {
							return nil
						}
						return err
					}

					// Only advance the counts on a successful update.
					atomic.AddUint64(&clients[i].count, 1)
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
			clients[i].Lock()
			clients[i].stopper.Stop()
			clients[i].stopper = nil
			clients[i].Unlock()
		}
	}()

	// Chaos monkey.
	go func() {
		defer close(teardown)
		rnd, seed := randutil.NewPseudoRand()
		log.Warningf("monkey starts (seed %d)", seed)
		for atomic.StoreUint64(&round, 1); !done(); atomic.AddUint64(&round, 1) {
			curRound := atomic.LoadUint64(&round)
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

			preCount := make([]uint64, len(clients))
			for i, client := range clients {
				preCount[i] = atomic.LoadUint64(&client.count)
			}

			// Sleep until at least one client is writing successfully.
			log.Warningf("round %d: monkey sleeping while cluster recovers...", curRound)
		recoverWait:
			for {
				c.Assert(t)
				for i, client := range clients {
					if atomic.LoadUint64(&client.count) > preCount[i] {
						break recoverWait
					}
				}
				time.Sleep(time.Second)
			}
			log.Warningf("round %d: cluster recovered", curRound)
		}
	}()

	prevRound := atomic.LoadUint64(&round)
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
				curRound := atomic.LoadUint64(&round)
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
				preCount := make([]string, len(clients))
				for i, client := range clients {
					preCount[i] = strconv.FormatUint(atomic.LoadUint64(&client.count), 10)
				}
				newOutput = fmt.Sprintf("round %d: client counts: (%s)", curRound, strings.Join(preCount, ", "))
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

	// Verify accounts.

	// Hold the read lock on node 0 to prevent it being restarted by
	// chaos monkey.
	var sum int
	clients[0].RLock()
	if err := clients[0].db.QueryRow("SELECT SUM(balance) FROM bank.accounts").Scan(&sum); err != nil {
		t.Fatal(err)
	}
	clients[0].RUnlock()
	if sum != 0 {
		t.Fatalf("The bank is not in good order. Total value: %d", sum)
	}

	elapsed := time.Since(start)
	var count uint64
	for _, client := range clients {
		count += atomic.LoadUint64(&client.count)
	}
	log.Infof("%d %.1f/sec", count, float64(count)/elapsed.Seconds())
}
