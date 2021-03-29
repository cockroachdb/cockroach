// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	bankStartingAmount = 999
	bankMaxTransfer    = 999
	bankNumAccounts    = 999
)

type bankClient struct {
	syncutil.RWMutex
	db    *gosql.DB
	count uint64
}

func (client *bankClient) transferMoney(ctx context.Context, numAccounts, maxTransfer int) error {
	from := rand.Intn(numAccounts)
	to := rand.Intn(numAccounts - 1)
	if from == to {
		to = numAccounts - 1
	}
	amount := rand.Intn(maxTransfer)

	tBegin := timeutil.Now()

	// If this statement gets stuck, the test harness will get stuck. Run with a
	// statement timeout, which unfortunately precludes the use of prepared
	// statements.
	q := fmt.Sprintf(`
SET statement_timeout = '30s';
UPDATE bank.accounts
   SET balance = CASE id WHEN %[1]d THEN balance-%[3]d WHEN %[2]d THEN balance+%[3]d END
 WHERE id IN (%[1]d, %[2]d) AND (SELECT balance >= %[3]d FROM bank.accounts WHERE id = %[1]d);
`, from, to, amount)

	client.RLock()
	defer client.RUnlock()
	_, err := client.db.ExecContext(ctx, q)
	if err == nil {
		// Do all increments under the read lock so that grabbing a write lock in
		// startChaosMonkey below guarantees no more increments could be incoming.
		atomic.AddUint64(&client.count, 1)
	}
	return errors.Wrapf(err, "after %.1fs", timeutil.Since(tBegin).Seconds())
}

type bankState struct {
	// One error sent by each client. A successful client sends a nil error.
	errChan chan error
	// Set to 1 if chaos monkey has stalled the writes.
	stalled  int32
	deadline time.Time
	clients  []bankClient

	cancel context.CancelFunc
	// monkeyWaitGroup tracks the running of the split and chaos monkeys.
	monkeyWaitGroup sync.WaitGroup
	// Monkey errors can be read after waiting on monkeyWaitGroup.
	chaosMonkeyErr error
	splitMonkeyErr error
	// The number of times chaos monkey has run.
	monkeyIteration uint64
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

// initBannk initializes the "accounts" table and some other test state. Returns
// a context that should be used by all of the test's operations and a cleanup
// function. The context will be canceled by the splitMonkey or chaosMonkey on
// errors. The cleanup function synchronizes with the monkeys and might call
// t.Fatal() on monkey errors. For this reason, the cleanup function must be
// called on the test's main goroutine.
func (s *bankState) initBank(
	ctx context.Context, t *test, c *cluster,
) (_ context.Context, cleanup func()) {
	db := c.Conn(ctx, 1)
	defer db.Close()

	if _, err := db.ExecContext(ctx, `CREATE DATABASE IF NOT EXISTS bank`); err != nil {
		t.Fatal(err)
	}

	// Delete table created by a prior instance of a test.
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS bank.accounts`); err != nil {
		t.Fatal(err)
	}

	schema := `
CREATE TABLE bank.accounts (
  id INT PRIMARY KEY,
  balance INT NOT NULL
)`
	if _, err := db.ExecContext(ctx, schema); err != nil {
		t.Fatal(err)
	}

	var placeholders bytes.Buffer
	var values []interface{}
	for i := 0; i < bankNumAccounts; i++ {
		if i > 0 {
			placeholders.WriteString(", ")
		}
		fmt.Fprintf(&placeholders, "($%d, %d)", i+1, bankStartingAmount)
		values = append(values, i)
	}
	stmt := `INSERT INTO bank.accounts (id, balance) VALUES ` + placeholders.String()
	if _, err := db.ExecContext(ctx, stmt, values...); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(ctx)
	return ctx, func() {
		cancel()
		s.monkeyWaitGroup.Wait()
		// Fail the test is any of the monkeys failed. Note that, on error,
		// the monkeys also call cancel() to stop the test.
		// TODO(andrei): The monkeys should have a way to register their error
		// as the test failure's cause, even though they don't run on the main
		// goroutine. The FatalIfErr here are not ideal because, if a monkey failed,
		// the context cancelation likely leads to another error being passed first
		// to t.Fatal(). Unfortunately t.Errorf() is just an alias for t.Fatalf().
		FatalIfErr(t, s.chaosMonkeyErr)
		FatalIfErr(t, s.splitMonkeyErr)
	}
}

// Continuously transfers money until done().
func (s *bankState) transferMoney(
	ctx context.Context, l *logger, c *cluster, idx, numAccounts, maxTransfer int,
) {
	defer l.Printf("client %d shutting down\n", idx)
	client := &s.clients[idx-1]
	for !s.done(ctx) {
		if err := client.transferMoney(ctx, numAccounts, maxTransfer); err != nil {
			// Ignore some errors.
			if !pgerror.IsSQLRetryableError(err) {
				// Report the err and terminate.
				s.errChan <- err
				return
			}
		}
	}
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
	var numAccounts uint64
	err := retry.ForDuration(30*time.Second, func() error {
		// Hold the read lock on the client to prevent it being restarted by
		// chaos monkey.
		client.RLock()
		defer client.RUnlock()
		err := client.db.QueryRowContext(ctx, "SELECT count(*), sum(balance) FROM bank.accounts").Scan(&numAccounts, &sum)
		if err != nil && !pgerror.IsSQLRetryableError(err) {
			t.Fatal(err)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if expected := bankStartingAmount * bankNumAccounts; sum != expected {
		t.Fatalf("the bank is not in good order, total value: %d, expected: %d", sum, expected)
	}

	if numAccounts != bankNumAccounts {
		t.Fatalf("the bank is not in good order, total num accounts: %d, expected: %d", numAccounts, bankNumAccounts)
	}
}

// startChaosMonkey picks a set of nodes and restarts them.
func (s *bankState) startChaosMonkey(
	ctx context.Context, t *test, c *cluster, pickNodes func() []int, consistentIdx int,
) {
	s.monkeyWaitGroup.Add(1)
	go func() {
		defer s.monkeyWaitGroup.Done()

		onErr := func(err error) {
			t.l.Errorf("chaos monkey failed; stopping test: %s", err)
			s.cancel()
			s.chaosMonkeyErr = errors.Wrap(err, "chaos monkey failed")
		}

		// Don't begin the chaos monkey until all nodes are serving SQL connections.
		// This ensures that we don't test cluster initialization under chaos.
		for i := 1; i <= c.spec.NodeCount; i++ {
			db := c.Conn(ctx, i)
			var res int
			err := db.QueryRowContext(ctx, `SELECT 1`).Scan(&res)
			if err != nil {
				onErr(err)
				return
			}
			err = db.Close()
			if err != nil {
				onErr(err)
				return
			}
		}

		for curRound := uint64(1); !s.done(ctx); curRound++ {
			atomic.StoreUint64(&s.monkeyIteration, curRound)

			// Pick nodes to be restarted.
			nodes := pickNodes()

			t.l.Printf("round %d: restarting nodes %v\n", curRound, nodes)
			for _, i := range nodes {
				if s.done(ctx) {
					break
				}
				t.l.Printf("round %d: restarting %d\n", curRound, i)
				if err := c.Restart(ctx, c.Node(i)); err != nil {
					onErr(err)
					return
				}
			}

			preCount := s.counts()

			madeProgress := func() bool {
				newCounts := s.counts()
				for i := range newCounts {
					if newCounts[i] > preCount[i] {
						t.l.Printf("round %d: progress made by client %d\n", curRound, i)
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

			t.l.Printf("round %d: cluster recovered\n", curRound)
		}
	}()
}

func (s *bankState) startSplitMonkey(ctx context.Context, t *test, d time.Duration, c *cluster) {
	s.monkeyWaitGroup.Add(1)
	go func() {
		defer s.monkeyWaitGroup.Done()

		onErr := func(err error) {
			t.l.Errorf("chaos monkey failed; stopping test: %s", err)
			s.cancel()
			s.splitMonkeyErr = errors.Wrap(err, "split monkey failed")
		}

		r := newRand()
		nodes := make([]string, c.spec.NodeCount)

		for i := 0; i < c.spec.NodeCount; i++ {
			nodes[i] = strconv.Itoa(i + 1)
		}

		for curRound := uint64(1); !s.done(ctx); curRound++ {
			atomic.StoreUint64(&s.monkeyIteration, curRound)
			time.Sleep(time.Duration(rand.Float64() * float64(d)))

			client := &s.clients[c.All().randNode()[0]-1]

			switch r.Intn(2) {
			case 0:
				client.RLock()
				zipF := accountDistribution(r)
				key := zipF.Uint64()
				c.l.Printf("round %d: splitting key %v\n", curRound, key)
				_, err := client.db.ExecContext(ctx,
					fmt.Sprintf(`ALTER TABLE bank.accounts SPLIT AT VALUES (%d)`, key))
				if err != nil && !(pgerror.IsSQLRetryableError(err) || kv.IsExpectedRelocateError(err)) {
					onErr(err)
				}
				client.RUnlock()
			case 1:
				for i := 0; i < len(s.clients); i++ {
					s.clients[i].Lock()
				}
				zipF := accountDistribution(r)
				key := zipF.Uint64()

				rand.Shuffle(len(nodes), func(i, j int) {
					nodes[i], nodes[j] = nodes[j], nodes[i]
				})

				const relocateQueryFormat = `ALTER TABLE bank.accounts EXPERIMENTAL_RELOCATE VALUES (ARRAY[%s], %d);`
				relocateQuery := fmt.Sprintf(relocateQueryFormat, strings.Join(nodes[1:], ", "), key)
				c.l.Printf("round %d: relocating key %d to nodes %s\n",
					curRound, key, nodes[1:])

				_, err := client.db.ExecContext(ctx, relocateQuery)
				if err != nil && !(pgerror.IsSQLRetryableError(err) || kv.IsExpectedRelocateError(err)) {
					onErr(err)
				}
				for i := 0; i < len(s.clients); i++ {
					s.clients[i].Unlock()
				}
			}
		}
	}()
}

func accountDistribution(r *rand.Rand) *rand.Zipf {
	// We use a Zipf distribution for selecting accounts.
	return rand.NewZipf(r, 1.1, float64(bankNumAccounts/10), uint64(bankNumAccounts-1))
}

func newRand() *rand.Rand {
	return rand.New(rand.NewSource(timeutil.Now().UnixNano()))
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
					c.spec.NodeCount-doneClients)
			}
			// This just stops the logs from being a bit too spammy.
			if newOutput != prevOutput {
				t.l.Printf("%s\n", newOutput)
				prevOutput = newOutput
			}
		}
	}
}

func runBankClusterRecovery(ctx context.Context, t *test, c *cluster) {
	c.Put(ctx, cockroach, "./cockroach")
	c.Start(ctx, t)

	// TODO(peter): Run for longer when !local.
	start := timeutil.Now()
	s := &bankState{
		errChan:  make(chan error, c.spec.NodeCount),
		deadline: start.Add(time.Minute),
		clients:  make([]bankClient, c.spec.NodeCount),
	}
	ctx, cleanup := s.initBank(ctx, t, c)
	defer cleanup()

	for i := 0; i < c.spec.NodeCount; i++ {
		s.clients[i].Lock()
		s.initClient(ctx, c, i+1)
		s.clients[i].Unlock()
		go s.transferMoney(ctx, t.l, c, i+1, bankNumAccounts, bankMaxTransfer)
	}

	// Chaos monkey.
	rnd, seed := randutil.NewPseudoRand()
	t.l.Printf("monkey starts (seed %d)\n", seed)
	pickNodes := func() []int {
		nodes := rnd.Perm(c.spec.NodeCount)[:rnd.Intn(c.spec.NodeCount)+1]
		for i := range nodes {
			nodes[i]++
		}
		return nodes
	}
	s.startChaosMonkey(ctx, t, c, pickNodes, -1)
	s.waitClientsStop(ctx, t, c, 45*time.Second)

	// Verify accounts.
	s.verifyAccounts(ctx, t)

	elapsed := timeutil.Since(start).Seconds()
	var count uint64
	counts := s.counts()
	for _, c := range counts {
		count += c
	}
	t.l.Printf("%d transfers (%.1f/sec) in %.1fs\n", count, float64(count)/elapsed, elapsed)
}

func runBankNodeRestart(ctx context.Context, t *test, c *cluster) {
	c.Put(ctx, cockroach, "./cockroach")
	c.Start(ctx, t)

	// TODO(peter): Run for longer when !local.
	start := timeutil.Now()
	s := &bankState{
		errChan:  make(chan error, 1),
		deadline: start.Add(time.Minute),
		clients:  make([]bankClient, 1),
	}
	ctx, cleanup := s.initBank(ctx, t, c)
	defer cleanup()

	clientIdx := c.spec.NodeCount
	client := &s.clients[0]
	client.db = c.Conn(ctx, clientIdx)

	go s.transferMoney(ctx, t.l, c, 1, bankNumAccounts, bankMaxTransfer)

	// Chaos monkey.
	rnd, seed := randutil.NewPseudoRand()
	t.l.Printf("monkey starts (seed %d)\n", seed)
	pickNodes := func() []int {
		return []int{1 + rnd.Intn(clientIdx)}
	}
	s.startChaosMonkey(ctx, t, c, pickNodes, clientIdx)
	s.waitClientsStop(ctx, t, c, 45*time.Second)

	// Verify accounts.
	s.verifyAccounts(ctx, t)

	elapsed := timeutil.Since(start).Seconds()
	count := atomic.LoadUint64(&client.count)
	t.l.Printf("%d transfers (%.1f/sec) in %.1fs\n", count, float64(count)/elapsed, elapsed)
}

func runBankNodeZeroSum(ctx context.Context, t *test, c *cluster) {
	c.Put(ctx, cockroach, "./cockroach")
	c.Start(ctx, t)

	start := timeutil.Now()
	s := &bankState{
		errChan:  make(chan error, c.spec.NodeCount),
		deadline: start.Add(time.Minute),
		clients:  make([]bankClient, c.spec.NodeCount),
	}
	ctx, cleanup := s.initBank(ctx, t, c)
	defer cleanup()

	for i := 0; i < c.spec.NodeCount; i++ {
		s.clients[i].Lock()
		s.initClient(ctx, c, i+1)
		s.clients[i].Unlock()
		go s.transferMoney(ctx, t.l, c, i+1, bankNumAccounts, bankMaxTransfer)
	}

	s.startSplitMonkey(ctx, t, 2*time.Second, c)
	s.waitClientsStop(ctx, t, c, 45*time.Second)

	s.verifyAccounts(ctx, t)

	elapsed := timeutil.Since(start).Seconds()
	var count uint64
	counts := s.counts()
	for _, c := range counts {
		count += c
	}
	c.l.Printf("%d transfers (%.1f/sec) in %.1fs\n", count, float64(count)/elapsed, elapsed)
}

var _ = runBankZeroSumRestart

func runBankZeroSumRestart(ctx context.Context, t *test, c *cluster) {
	c.Put(ctx, cockroach, "./cockroach")
	c.Start(ctx, t)

	start := timeutil.Now()
	s := &bankState{
		errChan:  make(chan error, c.spec.NodeCount),
		deadline: start.Add(time.Minute),
		clients:  make([]bankClient, c.spec.NodeCount),
	}
	ctx, cleanup := s.initBank(ctx, t, c)
	defer cleanup()

	for i := 0; i < c.spec.NodeCount; i++ {
		s.clients[i].Lock()
		s.initClient(ctx, c, i+1)
		s.clients[i].Unlock()
		go s.transferMoney(ctx, t.l, c, i+1, bankNumAccounts, bankMaxTransfer)
	}

	rnd, seed := randutil.NewPseudoRand()
	c.l.Printf("monkey starts (seed %d)\n", seed)
	pickNodes := func() []int {
		nodes := rnd.Perm(c.spec.NodeCount)[:rnd.Intn(c.spec.NodeCount)+1]
		for i := range nodes {
			nodes[i]++
		}
		return nodes
	}

	// Starting up the goroutines that restart and do splits and lease moves.
	s.startChaosMonkey(ctx, t, c, pickNodes, -1)
	s.startSplitMonkey(ctx, t, 2*time.Second, c)
	s.waitClientsStop(ctx, t, c, 45*time.Second)

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
