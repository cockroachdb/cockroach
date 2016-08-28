// Copyright 2016 The Cockroach Authors.
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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package acceptance

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/pkg/errors"
)

func TestPartitionNemesis(t *testing.T) {
	t.Skip("only enabled for manually playing with the partitioning agent")
	SkipUnlessLocal(t)
	runTestOnConfigs(t, func(t *testing.T, c cluster.Cluster, cfg cluster.TestConfig) {
		s := stop.NewStopper()
		defer s.Stop()
		s.RunWorker(func() {
			BidirectionalPartitionNemesis(t, s.ShouldQuiesce(), c)
		})
		select {
		case <-time.After(*flagDuration):
		case <-stopper:
		}
	})
}

func TestPartitionBank(t *testing.T) {
	t.Skip("#7978")
	SkipUnlessPrivileged(t)
	runTestOnConfigs(t, testBankWithNemesis(BidirectionalPartitionNemesis))
}

type Bank struct {
	cluster.Cluster
	*testing.T
	accounts, initialBalance int
}

func (b *Bank) must(err error) {
	if err != nil {
		f, l, _ := caller.Lookup(1)
		b.Fatal(errors.Wrapf(err, "%s:%d", f, l))
	}
}

// NewBank creates a Bank.
// TODO(tamird,tschottdorf): share this code with other bank test(s).
func NewBank(t *testing.T, c cluster.Cluster) *Bank {
	return &Bank{Cluster: c, T: t}
}

func (b *Bank) exec(query string, vars ...interface{}) error {
	db := makePGClient(b.T, b.PGUrl(0))
	defer db.Close()
	_, err := db.Exec(query, vars...)
	return err
}

// Init sets up the bank for the given number of accounts, each of which
// receiving a deposit of the given amount.
// This should be called before any nemeses are active; it will fail the test
// if unsuccessful.
func (b *Bank) Init(numAccounts, initialBalance int) {
	b.accounts = numAccounts
	b.initialBalance = initialBalance

	b.must(b.exec(`CREATE DATABASE IF NOT EXISTS bank`))
	b.must(b.exec(`DROP TABLE IF EXISTS bank.accounts`))
	const schema = `CREATE TABLE bank.accounts (id INT PRIMARY KEY, balance INT NOT NULL)`
	b.must(b.exec(schema))
	for i := 0; i < numAccounts; i++ {
		b.must(b.exec(`INSERT INTO bank.accounts (id, balance) VALUES ($1, $2)`,
			i, initialBalance))
	}
}

// Verify makes sure that the total amount of money in the system has not
// changed.
func (b *Bank) Verify() {
	exp := b.accounts * b.initialBalance
	db := makePGClient(b.T, b.PGUrl(0))
	defer db.Close()
	r := db.QueryRow(`SELECT SUM(balance) FROM bank.accounts`)
	var act int
	b.must(r.Scan(&act))
	if act != exp {
		b.Fatalf("bank is worth $%d, should be $%d", act, exp)
	}
}

func (b *Bank) logFailed(i int, v interface{}) {
	log.Warningf(context.Background(), "%d: %v", i, v)
}
func (b *Bank) logBegin(i int, from, to, amount int) {
	log.Warningf(context.Background(), "%d: %d trying to give $%d to %d", i, from, amount, to)
}
func (b *Bank) logSuccess(i int, from, to, amount int) {
	log.Warningf(context.Background(), "%d: %d gave $%d to %d", i, from, amount, to)
}

// Invoke transfers a random amount of money between random accounts.
func (b *Bank) Invoke(i int) {
	handle := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	var from, to int
	{
		p := rand.Perm(b.accounts)
		from, to = p[0], p[1]
	}
	amount := rand.Intn(b.initialBalance)
	b.logBegin(i, from, to, amount)
	defer func() {
		if r := recover(); r != nil {
			b.logFailed(i, r)
		} else {
			b.logSuccess(i, from, to, amount)
		}
	}()

	db := makePGClient(b.T, b.PGUrl(i%b.NumNodes()))
	defer db.Close()
	txn, err := db.Begin()
	handle(err)
	// The following SQL queries are intentionally unoptimized.
	var bFrom, bTo int
	{
		rFrom := txn.QueryRow(`SELECT balance FROM bank.accounts WHERE id = $1`, from)
		handle(rFrom.Scan(&bFrom))
		rTo := txn.QueryRow(`SELECT balance FROM bank.accounts WHERE id = $1`, to)
		handle(rTo.Scan(&bTo))
	}
	if diff := bFrom - amount; diff < 0 {
		handle(fmt.Errorf("%d is %d short to pay $%d", bFrom, -diff, amount))
	}
	_, err = txn.Exec(`UPDATE bank.accounts SET balance = $1 WHERE id = $2`, bFrom-amount, from)
	handle(err)
	_, err = txn.Exec(`UPDATE bank.accounts SET balance = $1 WHERE id = $2`, bTo+amount, to)
	handle(err)
}

func testBankWithNemesis(nemeses ...NemesisFn) configTestRunner {
	return func(t *testing.T, c cluster.Cluster, cfg cluster.TestConfig) {
		const (
			concurrency = 5
			accounts    = 10
		)
		deadline := timeutil.Now().Add(cfg.Duration)
		s := stop.NewStopper()
		defer s.Stop()
		b := NewBank(t, c)
		b.Init(accounts, 10)
		for _, nemesis := range nemeses {
			s.RunWorker(func() {
				nemesis(t, s.ShouldQuiesce(), c)
			})
		}
		for i := 0; i < concurrency; i++ {
			localI := i
			if err := s.RunAsyncTask(func() {
				for timeutil.Now().Before(deadline) {
					select {
					case <-s.ShouldQuiesce():
						return
					default:
					}
					b.Invoke(localI)
				}
			}); err != nil {
				t.Fatal(err)
			}
		}
		select {
		case <-stopper:
		case <-time.After(cfg.Duration):
		}
		log.Warningf(context.Background(), "finishing test")
		b.Verify()
	}
}
