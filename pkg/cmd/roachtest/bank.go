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
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	bankMaxTransfer = 999
	bankNumAccounts = 999
)

type bankClient struct {
	syncutil.RWMutex
	db    *gosql.DB
	count uint64
}

func (client *bankClient) transferMoney(numAccounts, maxTransfer int) error {
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

type bankState struct {
	// One error sent by each client. A successful client sends a nil error.
	errChan  chan error
	teardown chan struct{}
	// The number of times chaos monkey has run.
	monkeyIteration uint64
	// Set to 1 if chaos monkey has stalled the writes.
	stalled  int32
	deadline time.Time
	clients  []bankClient
}

func (s *bankState) done(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}
	return !timeutil.Now().Before(s.deadline) || atomic.LoadInt32(&s.stalled) == 1
}

// initClient initializes the client talking to node "i".
// It requires that the caller hold the client's write lock.
func (s *bankState) initClient(ctx context.Context, c *cluster, i int) {
	s.clients[i-1].db = c.Conn(ctx, i)
}

// Returns counts from all the clients.
func (s *bankState) counts() []uint64 {
	counts := make([]uint64, len(s.clients))
	for i := range s.clients {
		counts[i] = atomic.LoadUint64(&s.clients[i].count)
	}
	return counts
}

// Initialize the "accounts" table.
func (s *bankState) initBank(ctx context.Context, t *test, c *cluster) {
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
	for i := 0; i < bankNumAccounts; i++ {
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

// Continuously transfers money until done().
func (s *bankState) transferMoney(
	ctx context.Context, c *cluster, idx, numAccounts, maxTransfer int,
) {
	client := &s.clients[idx-1]
	for !s.done(ctx) {
		if err := client.transferMoney(numAccounts, maxTransfer); err != nil {
			// Ignore some errors.
			if !testutils.IsSQLRetryableError(err) {
				// Report the err and terminate.
				s.errChan <- err
				break
			}
		}
	}
	c.l.Printf("client %d shutting down\n", idx)
	s.errChan <- nil
}

// Verify accounts.
func (s *bankState) verifyAccounts(ctx context.Context, t *test) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	client := &s.clients[0]

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
func (s *bankState) chaosMonkey(
	ctx context.Context,
	t *test,
	c *cluster,
	stopClients bool,
	pickNodes func() []int,
	consistentIdx int,
) {
	defer close(s.teardown)
	for curRound := uint64(1); !s.done(ctx); curRound++ {
		atomic.StoreUint64(&s.monkeyIteration, curRound)

		// Pick nodes to be restarted.
		nodes := pickNodes()

		if stopClients {
			// Prevent all clients from writing while nodes are being restarted.
			for i := 0; i < len(s.clients); i++ {
				s.clients[i].Lock()
			}
		}
		c.l.Printf("round %d: restarting nodes %v\n", curRound, nodes)
		for _, i := range nodes {
			if s.done(ctx) {
				break
			}
			c.l.Printf("round %d: restarting %d\n", curRound, i)
			c.Stop(ctx, c.Node(i))
			c.Start(ctx, c.Node(i))
			if stopClients {
				// Reinitialize the client talking to the restarted node.
				s.initClient(ctx, c, i)
			}
		}
		if stopClients {
			for i := 0; i < len(s.clients); i++ {
				s.clients[i].Unlock()
			}
		}

		preCount := s.counts()

		madeProgress := func() bool {
			newCounts := s.counts()
			for i := range newCounts {
				if newCounts[i] > preCount[i] {
					c.l.Printf("round %d: progress made by client %d\n", curRound, i)
					return true
				}
			}
			return false
		}

		// Sleep until at least one client is writing successfully.
		c.l.Printf("round %d: monkey sleeping while cluster recovers...\n", curRound)
		for !s.done(ctx) && !madeProgress() {
			time.Sleep(time.Second)
		}
		if s.done(ctx) {
			c.l.Printf("round %d: not waiting for recovery due to signal that we're done\n",
				curRound)
			return
		}

		c.l.Printf("round %d: cluster recovered\n", curRound)
	}
}

// Wait until all clients have stopped.
func (s *bankState) waitClientsStop(
	ctx context.Context, t *test, c *cluster, stallDuration time.Duration,
) {
	prevRound := atomic.LoadUint64(&s.monkeyIteration)
	stallTime := timeutil.Now().Add(stallDuration)
	var prevOutput string
	// Spin until all clients are shut.
	for doneClients := 0; doneClients < len(s.clients); {
		select {
		case <-s.teardown:
		case <-ctx.Done():
			t.Fatal(ctx.Err())

		case err := <-s.errChan:
			if err != nil {
				t.Fatal(err)
			}
			doneClients++

		case <-time.After(time.Second):
			var newOutput string
			if timeutil.Now().Before(s.deadline) {
				curRound := atomic.LoadUint64(&s.monkeyIteration)
				if curRound == prevRound {
					if timeutil.Now().After(stallTime) {
						atomic.StoreInt32(&s.stalled, 1)
						t.Fatalf("stall detected at round %d, no forward progress for %s",
							curRound, stallDuration)
					}
				} else {
					prevRound = curRound
					stallTime = timeutil.Now().Add(stallDuration)
				}
				// Periodically print out progress so that we know the test is
				// still running and making progress.
				counts := s.counts()
				strCounts := make([]string, len(counts))
				for i := range counts {
					strCounts[i] = strconv.FormatUint(counts[i], 10)
				}
				newOutput = fmt.Sprintf("round %d: client counts: (%s)",
					curRound, strings.Join(strCounts, ", "))
			} else {
				newOutput = fmt.Sprintf("test finished, waiting for shutdown of %d clients",
					c.nodes-doneClients)
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
	c.Start(ctx)

	// TODO(peter): Run for longer when !local.
	start := timeutil.Now()
	s := &bankState{
		errChan:  make(chan error, c.nodes),
		teardown: make(chan struct{}),
		deadline: start.Add(time.Minute),
		clients:  make([]bankClient, c.nodes),
	}
	s.initBank(ctx, t, c)

	for i := 0; i < c.nodes; i++ {
		s.clients[i].Lock()
		s.initClient(ctx, c, i+1)
		s.clients[i].Unlock()
		go s.transferMoney(ctx, c, i+1, bankNumAccounts, bankMaxTransfer)
	}

	defer func() {
		<-s.teardown
	}()

	// Chaos monkey.
	rnd, seed := randutil.NewPseudoRand()
	c.l.Printf("monkey starts (seed %d)\n", seed)
	pickNodes := func() []int {
		nodes := rnd.Perm(c.nodes)[:rnd.Intn(c.nodes)+1]
		for i := range nodes {
			nodes[i]++
		}
		return nodes
	}
	go s.chaosMonkey(ctx, t, c, true, pickNodes, -1)

	s.waitClientsStop(ctx, t, c, 30*time.Second)

	// Verify accounts.
	s.verifyAccounts(ctx, t)

	elapsed := timeutil.Since(start).Seconds()
	var count uint64
	counts := s.counts()
	for _, c := range counts {
		count += c
	}
	c.l.Printf("%d transfers (%.1f/sec) in %.1fs\n", count, float64(count)/elapsed, elapsed)
}

func runBankNodeRestart(ctx context.Context, t *test, c *cluster) {
	c.Put(ctx, cockroach, "./cockroach")
	c.Start(ctx)

	// TODO(peter): Run for longer when !local.
	start := timeutil.Now()
	s := &bankState{
		errChan:  make(chan error, 1),
		teardown: make(chan struct{}),
		deadline: start.Add(time.Minute),
		clients:  make([]bankClient, 1),
	}
	s.initBank(ctx, t, c)

	clientIdx := c.nodes
	client := &s.clients[0]
	client.db = c.Conn(ctx, clientIdx)
	go s.transferMoney(ctx, c, 1, bankNumAccounts, bankMaxTransfer)

	defer func() {
		<-s.teardown
	}()

	// Chaos monkey.
	rnd, seed := randutil.NewPseudoRand()
	c.l.Printf("monkey starts (seed %d)\n", seed)
	pickNodes := func() []int {
		return []int{1 + rnd.Intn(clientIdx)}
	}
	go s.chaosMonkey(ctx, t, c, false, pickNodes, clientIdx)

	s.waitClientsStop(ctx, t, c, 30*time.Second)

	// Verify accounts.
	s.verifyAccounts(ctx, t)

	elapsed := timeutil.Since(start).Seconds()
	count := atomic.LoadUint64(&client.count)
	c.l.Printf("%d transfers (%.1f/sec) in %.1fs\n", count, float64(count)/elapsed, elapsed)
}
