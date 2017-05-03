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

package acceptance

import (
	"bytes"
	gosql "database/sql"
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const stall = 2 * time.Minute

var maxTransfer = flag.Int("max-transfer", 999, "Maximum amount to transfer in one transaction.")
var numAccounts = flag.Int("num-accounts", 999, "Number of accounts.")

type testClient struct {
	syncutil.RWMutex
	db    *gosql.DB
	count uint64
}

type testState struct {
	t *testing.T
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
func (state *testState) initClient(ctx context.Context, t *testing.T, c cluster.Cluster, i int) {
	state.clients[i].db = makePGClient(t, c.PGUrl(ctx, i))
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
										  WHERE id IN ($1, $2) AND (SELECT balance >= $3 FROM bank.accounts WHERE id = $1)`
	client.RLock()
	defer client.RUnlock()
	_, err := client.db.Exec(update, from, to, amount)
	return err
}

// Verify accounts.
func verifyAccounts(t *testing.T, client *testClient) {
	var sum int
	testutils.SucceedsSoon(t, func() error {
		// Hold the read lock on the client to prevent it being restarted by
		// chaos monkey.
		client.RLock()
		defer client.RUnlock()
		err := client.db.QueryRow("SELECT SUM(balance) FROM bank.accounts").Scan(&sum)
		if err != nil && !testutils.IsSQLRetryableError(err) {
			t.Fatal(err)
		}
		return err
	})
	if sum != 0 {
		t.Fatalf("The bank is not in good order. Total value: %d", sum)
	}

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
		} else {
			// Only advance the counts on a successful update.
			atomic.AddUint64(&client.count, 1)
		}
	}
	log.Infof(ctx, "client %d shutting down", idx)
	state.errChan <- nil
}

// chaosMonkey picks a set of nodes and restarts them. If stopClients is set
// all the clients are locked before the nodes are restarted.
func chaosMonkey(
	ctx context.Context,
	state *testState,
	c cluster.Cluster,
	stopClients bool,
	pickNodes func() []int,
	consistentIdx int,
) {
	defer close(state.teardown)
	for curRound := uint64(1); !state.done(); curRound++ {
		atomic.StoreUint64(&state.monkeyIteration, curRound)
		select {
		case <-stopper.ShouldStop():
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
		log.Infof(ctx, "round %d: restarting nodes %v", curRound, nodes)
	outer:
		for _, i := range nodes {
			// Two early exit conditions.
			select {
			case <-stopper.ShouldStop():
				break outer
			default:
			}
			if state.done() {
				break
			}
			log.Infof(ctx, "round %d: restarting %d", curRound, i)
			if err := c.Kill(ctx, i); err != nil {
				state.t.Error(err)
			}
			if err := c.Restart(ctx, i); err != nil {
				state.t.Error(err)
			}
			if stopClients {
				// Reinitialize the client talking to the restarted node.
				state.initClient(ctx, state.t, c, i)
			}
		}
		if stopClients {
			for i := 0; i < len(state.clients); i++ {
				state.clients[i].Unlock()
			}
		}

		preCount := state.counts()

		madeProgress := func() bool {
			newCounts := state.counts()
			for i := range newCounts {
				if newCounts[i] > preCount[i] {
					return true
				}
			}
			return false
		}

		// Sleep until at least one client is writing successfully.
		log.Warningf(ctx, "round %d: monkey sleeping while cluster recovers...", curRound)
		for !state.done() && !madeProgress() {
			time.Sleep(time.Second)
		}
		c.Assert(ctx, state.t)

		if err := cluster.Consistent(ctx, c, consistentIdx); err != nil {
			state.t.Error(err)
		}
		log.Warningf(ctx, "round %d: cluster recovered", curRound)
	}
}

// Wait until all clients have stopped.
func waitClientsStop(ctx context.Context, num int, state *testState, stallDuration time.Duration) {
	prevRound := atomic.LoadUint64(&state.monkeyIteration)
	stallTime := timeutil.Now().Add(stallDuration)
	var prevOutput string
	// Spin until all clients are shut.
	for numShutClients := 0; numShutClients < num; {
		select {
		case <-state.teardown:
		case <-stopper.ShouldStop():
			state.t.Fatal("interrupted")

		case err := <-state.errChan:
			if err != nil {
				state.t.Error(err)
			}
			numShutClients++

		case <-time.After(time.Second):
			var newOutput string
			if timeutil.Now().Before(state.deadline) {
				curRound := atomic.LoadUint64(&state.monkeyIteration)
				if curRound == prevRound {
					if timeutil.Now().After(stallTime) {
						atomic.StoreInt32(&state.stalled, 1)
						state.t.Fatalf("Stall detected at round %d, no forward progress for %s", curRound, stallDuration)
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
				newOutput = fmt.Sprintf("test finished, waiting for shutdown of %d clients", num-numShutClients)
			}
			// This just stops the logs from being a bit too spammy.
			if newOutput != prevOutput {
				log.Infof(ctx, newOutput)
				prevOutput = newOutput
			}
		}
	}
}

// TestClusterRecovery starts up a cluster with an "accounts" table.
// It starts transferring money between accounts, while nodes are
// being killed and restarted continuously. The test doesn't measure write
// performance, but cluster recovery.
func TestClusterRecovery(t *testing.T) {
	t.Skip("#15620")

	s := log.Scope(t)
	defer s.Close(t)

	runTestOnConfigs(t, testClusterRecoveryInner)
}

func testClusterRecoveryInner(
	ctx context.Context, t *testing.T, c cluster.Cluster, cfg cluster.TestConfig,
) {
	num := c.NumNodes()

	// One client for each node.
	initBank(t, c.PGUrl(ctx, 0))

	start := timeutil.Now()
	state := testState{
		t:        t,
		errChan:  make(chan error, num),
		teardown: make(chan struct{}),
		deadline: start.Add(cfg.Duration),
		clients:  make([]testClient, num),
	}

	for i := 0; i < num; i++ {
		state.clients[i].Lock()
		state.initClient(ctx, t, c, i)
		state.clients[i].Unlock()
		go transferMoneyLoop(ctx, i, &state, *numAccounts, *maxTransfer)
	}

	defer func() {
		<-state.teardown
	}()

	// Chaos monkey.
	rnd, seed := randutil.NewPseudoRand()
	log.Warningf(ctx, "monkey starts (seed %d)", seed)
	pickNodes := func() []int {
		return rnd.Perm(num)[:rnd.Intn(num)+1]
	}
	go chaosMonkey(ctx, &state, c, true, pickNodes, 0)

	waitClientsStop(ctx, num, &state, stall)

	// Verify accounts.
	verifyAccounts(t, &state.clients[0])

	elapsed := timeutil.Since(start)
	var count uint64
	counts := state.counts()
	for _, c := range counts {
		count += c
	}
	log.Infof(ctx, "%d %.1f/sec", count, float64(count)/elapsed.Seconds())
}

// TestNodeRestart starts up a cluster with an "accounts" table.
// It uses a client connected to a single node in the cluster to issue SQL
// commands to transferring money between accounts, while a random node other
// than the one the client is connected to is being restarted periodically.
// The test measures read/write performance in the presence of restarts.
func TestNodeRestart(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	runTestOnConfigs(t, testNodeRestartInner)
}

func testNodeRestartInner(
	ctx context.Context, t *testing.T, c cluster.Cluster, cfg cluster.TestConfig,
) {
	num := c.NumNodes()
	if minNum := 3; num < minNum {
		t.Skipf("need at least %d nodes, got %d", minNum, num)
	}

	// One client for each node.
	initBank(t, c.PGUrl(ctx, 0))

	start := timeutil.Now()
	state := testState{
		t:        t,
		errChan:  make(chan error, 1),
		teardown: make(chan struct{}),
		deadline: start.Add(cfg.Duration),
		clients:  make([]testClient, 1),
	}

	clientIdx := num - 1
	client := &state.clients[0]
	client.Lock()
	client.db = makePGClient(t, c.PGUrl(ctx, clientIdx))
	client.Unlock()
	go transferMoneyLoop(ctx, 0, &state, *numAccounts, *maxTransfer)

	defer func() {
		<-state.teardown
	}()

	// Chaos monkey.
	rnd, seed := randutil.NewPseudoRand()
	log.Warningf(ctx, "monkey starts (seed %d)", seed)
	pickNodes := func() []int {
		return []int{rnd.Intn(clientIdx)}
	}
	go chaosMonkey(ctx, &state, c, false, pickNodes, clientIdx)

	waitClientsStop(ctx, 1, &state, stall)

	// Verify accounts.
	verifyAccounts(t, client)

	elapsed := timeutil.Since(start)
	count := atomic.LoadUint64(&client.count)
	log.Infof(ctx, "%d %.1f/sec", count, float64(count)/elapsed.Seconds())
}
