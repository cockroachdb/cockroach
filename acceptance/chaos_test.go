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

// Initialize the "accounts" table.
func initBank(t *testing.T, url string) {
	// Connect to the cluster.
	db := makePGClient(t, url)

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
}

func transferMoney(client *sql.DB) error {
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
	_, err := client.Exec(update, from, to, amount)
	return err
}

// Verify accounts.
func verifyAccounts(t *testing.T, client *testClient) {
	// Hold the read lock on the client to prevent it being restarted by
	// chaos monkey.
	var sum int
	client.RLock()
	if err := client.db.QueryRow("SELECT SUM(balance) FROM bank.accounts").Scan(&sum); err != nil {
		t.Fatal(err)
	}
	client.RUnlock()
	if sum != 0 {
		t.Fatalf("The bank is not in good order. Total value: %d", sum)
	}
}

func transferMoneyLoop(client *testClient, done func() bool, errs chan error) {
	for !done() {
		if err := func() error {
			client.RLock()
			defer client.RUnlock()
			if err := transferMoney(client.db); err != nil {
				// Ignore some errors.
				if testutils.IsError(err, "connection refused") {
					return nil
				}
				return err
			}
			// Only advance the counts on a successful update.
			atomic.AddUint64(&client.count, 1)
			return nil
		}(); err != nil {
			// Report the err and terminate.
			errs <- err
			return
		}
	}
	log.Infof("client %s shutting down", client.id)
	errs <- nil
}

func waitClientsStop(num int, t *testing.T, round *uint64, stalled *int32, teardown chan struct{}, clientCounts func() string, deadline time.Time, errs chan error) {
	prevRound := atomic.LoadUint64(round)
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
				curRound := atomic.LoadUint64(round)
				if curRound == prevRound {
					if time.Now().After(stallTime) {
						atomic.StoreInt32(stalled, 1)
						t.Fatalf("Stall detected at round %d, no forward progress for %s", curRound, *flagStall)
					}
				} else {
					prevRound = curRound
					stallTime = time.Now().Add(*flagStall)
				}
				// Periodically print out progress so that we know the test is
				// still running and making progress.
				newOutput = fmt.Sprintf("round %d: client counts: (%s)", curRound, clientCounts())
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
}

type testClient struct {
	sync.RWMutex
	id      string
	db      *sql.DB
	stopper *stop.Stopper
	count   uint64
}

// TestClusterRecovery starts up a cluster with an "accounts" table.
// It starts transferring money between accounts, while nodes are
// being killed and restarted continuously. The test doesn't measure write
// performance, but cluster recovery.
func TestClusterRecovery(t *testing.T) {
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
	clients := make([]testClient, num)
	initBank(t, c.PGUrl(0))

	start := time.Now()
	deadline := start.Add(*flagDuration)

	// initClient initializes the client talking to node "i".
	// It requires that the caller hold the client's write lock.
	initClient := func(i int) {
		clients[i].db = makePGClient(t, c.PGUrl(i))
	}

	done := func() bool {
		return !time.Now().Before(deadline) || atomic.LoadInt32(&stalled) == 1
	}

	for i := 0; i < num; i++ {
		clients[i].Lock()
		initClient(i)
		clients[i].Unlock()
		go transferMoneyLoop(&clients[i], done, errs)
	}

	teardown := make(chan struct{})
	defer func() {
		<-teardown
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
				if err := c.Kill(i); err != nil {
					t.Fatal(err)
				}
				if err := c.Restart(i); err != nil {
					t.Fatal(err)
				}
				initClient(i)
			}
			for i := 0; i < num; i++ {
				clients[i].Unlock()
			}

			preCount := make([]uint64, len(clients))
			for i, client := range clients {
				preCount[i] = atomic.LoadUint64(&client.count)
			}

			madeProgress := func() bool {
				c.Assert(t)
				for i, client := range clients {
					if atomic.LoadUint64(&client.count) > preCount[i] {
						return true
					}
				}
				return false
			}

			// Sleep until at least one client is writing successfully.
			log.Warningf("round %d: monkey sleeping while cluster recovers...", curRound)
			for !done() && !madeProgress() {
				time.Sleep(time.Second)
			}
			log.Warningf("round %d: cluster recovered", curRound)
		}
	}()

	counts := func() string {
		preCount := make([]string, len(clients))
		for i, client := range clients {
			preCount[i] = strconv.FormatUint(atomic.LoadUint64(&client.count), 10)
		}
		return strings.Join(preCount, ", ")
	}
	waitClientsStop(num, t, &round, &stalled, teardown, counts, deadline, errs)

	// Verify accounts.
	verifyAccounts(t, &clients[0])

	elapsed := time.Since(start)
	var count uint64
	for _, client := range clients {
		count += atomic.LoadUint64(&client.count)
	}
	log.Infof("%d %.1f/sec", count, float64(count)/elapsed.Seconds())
}

// TestNodeRestart starts up a cluster with an "accounts" table.
// It starts transferring money between accounts, while a single node is
// being killed and restarted continuously. The test measures read/write
// performance in the middle of restarts.
// TODO(vivek): Add latency metric to test.
func TestNodeRestart(t *testing.T) {
	c := StartCluster(t)
	defer c.AssertAndStop(t)

	num := c.NumNodes()
	if num <= 0 {
		t.Fatalf("%d nodes in cluster", num)
	}

	// One error sent by each client. A successful client sends a nil error.
	errs := make(chan error, 1)
	// The number of times chaos monkey has run.
	var round uint64
	// Set to 1 if chaos monkey has stalled the writes.
	var stalled int32
	// One client for each node.
	client := testClient{}
	initBank(t, c.PGUrl(0))

	start := time.Now()
	deadline := start.Add(*flagDuration)

	done := func() bool {
		return !time.Now().Before(deadline) || atomic.LoadInt32(&stalled) == 1
	}

	client.Lock()
	client.db = makePGClient(t, c.PGUrl(num-1))
	client.Unlock()
	go transferMoneyLoop(&client, done, errs)

	teardown := make(chan struct{})
	defer func() {
		<-teardown
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
			node := rnd.Intn(num - 1)
			log.Infof("round %d: restarting node %v", curRound, node)
			if err := c.Kill(node); err != nil {
				t.Fatal(err)
			}
			if err := c.Restart(node); err != nil {
				t.Fatal(err)
			}

			preCount := atomic.LoadUint64(&client.count)

			madeProgress := func() bool {
				c.Assert(t)
				if atomic.LoadUint64(&client.count) > preCount {
					return true
				}
				return false
			}

			// Sleep until at least one client is writing successfully.
			log.Warningf("round %d: monkey sleeping while cluster recovers...", curRound)
			for !done() && !madeProgress() {
				time.Sleep(time.Second)
			}
			log.Warningf("round %d: cluster recovered", curRound)
		}
	}()

	counts := func() string {
		return strconv.FormatUint(atomic.LoadUint64(&client.count), 10)
	}
	waitClientsStop(1, t, &round, &stalled, teardown, counts, deadline, errs)

	// Verify accounts.
	verifyAccounts(t, &client)

	elapsed := time.Since(start)
	count := atomic.LoadUint64(&client.count)
	log.Infof("%d %.1f/sec", count, float64(count)/elapsed.Seconds())
}
