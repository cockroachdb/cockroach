// Copyright 2018 The Cockroach Authors.
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

package main

import (
	"bytes"
	"context"
	gosql "database/sql"
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var maxTransfer = flag.Int("max-transfer", 999, "Maximum amount to transfer in one transaction.")
var numAccounts = flag.Int("num-accounts", 999, "Number of accounts.")

type testClient struct {
	syncutil.RWMutex
	db    *gosql.DB
	count uint64
}

type testState struct {
	// One error sent by each client. A successful client sends a nil error.
	errChan  chan error
	teardown chan struct{}
	// The number of times chaos monkey has run.
	monkeyIteration uint64
	// Set to 1 if chaos monkey has stalled the writes.
	stalled  int32
	deadline time.Time
	clients  []testClient
}

func (state *testState) done() bool {
	return !timeutil.Now().Before(state.deadline) || atomic.LoadInt32(&state.stalled) == 1
}

// initClient initializes the client talking to node "i".
// It requires that the caller hold the client's write lock.
func (state *testState) initClient(ctx context.Context, c *cluster, i int) {
	state.clients[i].db = c.Conn(ctx, i+1)
}

// Returns counts from all the clients.
func (state *testState) counts() []uint64 {
	counts := make([]uint64, len(state.clients))
	for i := range state.clients {
		counts[i] = atomic.LoadUint64(&state.clients[i].count)
	}
	return counts
}

// Initialize the "accounts" table.
func initBank(ctx context.Context, t *test, c *cluster) {
	db := c.Conn(ctx, 1)
	defer db.Close()

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

func transferMoney(client *testClient, numAccounts, maxTransfer int) error {
	from := rand.Intn(numAccounts)
	to := rand.Intn(numAccounts - 1)
	if from == to {
		to = numAccounts - 1
	}
	amount := rand.Intn(maxTransfer)

	const update = `
UPDATE bank.accounts
   SET balance = CASE id WHEN $1 THEN balance-$3 WHEN $2 THEN balance+$3 END
 WHERE id IN ($1, $2) AND (SELECT balance >= $3 FROM bank.accounts WHERE id = $1)
`
	client.RLock()
	defer client.RUnlock()
	_, err := client.db.Exec(update, from, to, amount)
	if err == nil {
		// Do all increments under the read lock so that grabbing a write lock in
		// chaosMonkey below guarantees no more increments could be incoming.
		atomic.AddUint64(&client.count, 1)
	}
	return err
}

// Continuously transfers money until done().
func transferMoneyLoop(
	ctx context.Context, idx int, state *testState, numAccounts, maxTransfer int,
) {
	client := &state.clients[idx]
	for !state.done() {
		if err := transferMoney(client, numAccounts, maxTransfer); err != nil {
			// Ignore some errors.
			if !testutils.IsSQLRetryableError(err) {
				// Report the err and terminate.
				state.errChan <- err
				break
			}
		}
	}
	log.Infof(ctx, "client %d shutting down", idx)
	state.errChan <- nil
}

// Verify accounts.
func verifyAccounts(t *test, client *testClient) {
	var sum int
	err := retry.ForDuration(30*time.Second, func() error {
		// Hold the read lock on the client to prevent it being restarted by
		// chaos monkey.
		client.RLock()
		defer client.RUnlock()
		err := client.db.QueryRow("SELECT sum(balance) FROM bank.accounts").Scan(&sum)
		if err != nil && !testutils.IsSQLRetryableError(err) {
			t.Fatal(err)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if sum != 0 {
		t.Fatalf("the bank is not in good order, total value: %d", sum)
	}
}

// chaosMonkey picks a set of nodes and restarts them. If stopClients is set
// all the clients are locked before the nodes are restarted.
func chaosMonkey(
	ctx context.Context,
	t *test,
	state *testState,
	c *cluster,
	stopClients bool,
	pickNodes func() []int,
	consistentIdx int,
) {
	defer close(state.teardown)
	for curRound := uint64(1); !state.done(); curRound++ {
		atomic.StoreUint64(&state.monkeyIteration, curRound)
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Pick nodes to be restarted.
		nodes := pickNodes()

		if stopClients {
			// Prevent all clients from writing while nodes are being restarted.
			for i := 0; i < len(state.clients); i++ {
				state.clients[i].Lock()
			}
		}
		c.l.Printf("round %d: restarting nodes %v\n", curRound, nodes)
	outer:
		for _, i := range nodes {
			// Two early exit conditions.
			select {
			case <-ctx.Done():
				break outer
			default:
			}
			if state.done() {
				break
			}
			c.l.Printf("round %d: restarting %d", curRound, i)
			c.Stop(ctx, c.Node(i+1))
			c.Start(ctx, c.Node(i+1))
			if stopClients {
				// Reinitialize the client talking to the restarted node.
				state.initClient(ctx, c, i)
			}
		}
		if stopClients {
			for i := 0; i < len(state.clients); i++ {
				state.clients[i].Unlock()
			}
		}

		preCount := state.counts()

		progressIdx := 0
		madeProgress := func() bool {
			newCounts := state.counts()
			for i := range newCounts {
				if newCounts[i] > preCount[i] {
					c.l.Printf("round %d: progress made by client %d", curRound, i)
					progressIdx = i
					return true
				}
			}
			return false
		}

		// Sleep until at least one client is writing successfully.
		c.l.Printf("round %d: monkey sleeping while cluster recovers...\n", curRound)
		for !state.done() && !madeProgress() {
			time.Sleep(time.Second)
		}
		if state.done() {
			c.l.Printf("round %d: not waiting for recovery due to signal that we're done\n", curRound)
			return
		}

		// If a particular node index wasn't specified, use the index that informed
		// us about progress having been made.
		idx := consistentIdx
		if idx < 0 {
			idx = progressIdx
		}

		c.l.Printf("round %d: cluster recovered\n", curRound)
	}
}

// Wait until all clients have stopped.
func waitClientsStop(ctx context.Context, t *test, c *cluster, state *testState, stallDuration time.Duration) {
	prevRound := atomic.LoadUint64(&state.monkeyIteration)
	stallTime := timeutil.Now().Add(stallDuration)
	var prevOutput string
	// Spin until all clients are shut.
	for doneClients := 0; doneClients < len(state.clients); {
		select {
		case <-state.teardown:
		case <-ctx.Done():
			t.Fatal(ctx.Err())

		case err := <-state.errChan:
			if err != nil {
				t.Fatal(err)
			}
			doneClients++

		case <-time.After(time.Second):
			var newOutput string
			if timeutil.Now().Before(state.deadline) {
				curRound := atomic.LoadUint64(&state.monkeyIteration)
				if curRound == prevRound {
					if timeutil.Now().After(stallTime) {
						atomic.StoreInt32(&state.stalled, 1)
						t.Fatalf("stall detected at round %d, no forward progress for %s", curRound, stallDuration)
					}
				} else {
					prevRound = curRound
					stallTime = timeutil.Now().Add(stallDuration)
				}
				// Periodically print out progress so that we know the test is
				// still running and making progress.
				counts := state.counts()
				strCounts := make([]string, len(counts))
				for i := range counts {
					strCounts[i] = strconv.FormatUint(counts[i], 10)
				}
				newOutput = fmt.Sprintf("round %d: client counts: (%s)", curRound, strings.Join(strCounts, ", "))
			} else {
				newOutput = fmt.Sprintf("test finished, waiting for shutdown of %d clients", c.nodes-doneClients)
			}
			// This just stops the logs from being a bit too spammy.
			if newOutput != prevOutput {
				c.l.Printf("%s\n", newOutput)
				prevOutput = newOutput
			}
		}
	}
}

func runBankClusterRecovery(ctx context.Context, t *test, c *cluster) {
	c.Put(ctx, cockroach, "./cockroach")
	c.Wipe(ctx)
	c.Start(ctx)

	initBank(ctx, t, c)

	start := timeutil.Now()
	state := &testState{
		errChan:  make(chan error, c.nodes),
		teardown: make(chan struct{}),
		deadline: start.Add(time.Minute),
		clients:  make([]testClient, c.nodes),
	}

	for i := 0; i < c.nodes; i++ {
		state.clients[i].Lock()
		state.initClient(ctx, c, i)
		state.clients[i].Unlock()
		go transferMoneyLoop(ctx, i, state, *numAccounts, *maxTransfer)
	}

	defer func() {
		<-state.teardown
	}()

	// Chaos monkey.
	rnd, seed := randutil.NewPseudoRand()
	c.l.Printf("monkey starts (seed %d)\n", seed)
	pickNodes := func() []int {
		return rnd.Perm(c.nodes)[:rnd.Intn(c.nodes)+1]
	}
	go chaosMonkey(ctx, t, state, c, true, pickNodes, -1)

	waitClientsStop(ctx, t, c, state, 30*time.Second)

	// Verify accounts.
	verifyAccounts(t, &state.clients[0])

	elapsed := timeutil.Since(start)
	var count uint64
	counts := state.counts()
	for _, c := range counts {
		count += c
	}
	c.l.Printf("%d %.1f/sec over %s\n", count, float64(count)/elapsed.Seconds(), elapsed)
}

func runBankNodeRestart(ctx context.Context, t *test, c *cluster) {
	c.Put(ctx, cockroach, "./cockroach", c.All())
	c.Wipe(ctx)
	c.Start(ctx)

	initBank(ctx, t, c)

	start := timeutil.Now()
	state := &testState{
		errChan:  make(chan error, 1),
		teardown: make(chan struct{}),
		deadline: start.Add(time.Minute),
		clients:  make([]testClient, 1),
	}

	clientIdx := c.nodes
	client := &state.clients[0]
	client.Lock()
	client.db = c.Conn(ctx, clientIdx)
	client.Unlock()
	go transferMoneyLoop(ctx, 0, state, *numAccounts, *maxTransfer)

	defer func() {
		<-state.teardown
	}()

	// Chaos monkey.
	rnd, seed := randutil.NewPseudoRand()
	log.Warningf(ctx, "monkey starts (seed %d)", seed)
	pickNodes := func() []int {
		return []int{rnd.Intn(clientIdx)}
	}
	go chaosMonkey(ctx, t, state, c, false, pickNodes, clientIdx)

	waitClientsStop(ctx, t, c, state, 30*time.Second)

	// Verify accounts.
	verifyAccounts(t, client)

	elapsed := timeutil.Since(start)
	count := atomic.LoadUint64(&client.count)
	log.Infof(ctx, "%d %.1f/sec", count, float64(count)/elapsed.Seconds())
}
