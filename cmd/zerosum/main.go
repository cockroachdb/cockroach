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
// Author: Peter Mattis (peter@cockroachlabs.com)

package main

import (
	gosql "database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/cockroachdb/cockroach/util/timeutil"

	"golang.org/x/net/context"
)

var workers = flag.Int("w", 2*runtime.NumCPU(), "number of workers")
var monkeys = flag.Int("m", 3, "number of monkeys")
var numNodes = flag.Int("n", 4, "number of nodes")
var numAccounts = flag.Int("a", 1e5, "number of accounts")

// zeroSum is a bank-like simulation that tests correctness in the face of
// aggressive splits and lease transfers. A pool of workers chooses two random
// accounts and increments the balance in one while decrementing the balance in
// the other (leaving the total balance as zero, hence the name). A pool of
// monkeys splits ranges and moves leases every second or so. Periodically, we
// perform full cluster consistency checks as well as verify that the total
// balance in the accounts table is zero.
//
// The account IDs used by workers and chosen as split points are selected from
// a zipf distribution which tilts towards smaller IDs (and hence more
// contention).
type zeroSum struct {
	*cluster
	numAccounts int
	accounts    struct {
		syncutil.Mutex
		m map[uint64]struct{}
	}
	stats struct {
		ops       uint64
		errors    uint64
		splits    uint64
		transfers uint64
	}
}

func newZeroSum(c *cluster, numAccounts int) *zeroSum {
	z := &zeroSum{
		cluster:     c,
		numAccounts: numAccounts,
	}
	z.accounts.m = make(map[uint64]struct{})
	return z
}

func (z *zeroSum) run(workers, monkeys int) {
	tableID := z.setup()
	for i := 0; i < workers; i++ {
		go z.worker()
	}
	for i := 0; i < monkeys; i++ {
		go z.monkey(tableID, 2*time.Second*time.Duration(monkeys))
	}
	go z.check(20 * time.Second)
	go z.verify(10 * time.Second)
	z.monitor(time.Second)
}

func (z *zeroSum) setup() uint32 {
	db := z.db[0]
	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS zerosum"); err != nil {
		log.Fatal(context.Background(), err)
	}

	accounts := `
CREATE TABLE IF NOT EXISTS accounts (
  id INT PRIMARY KEY,
  balance INT NOT NULL
)
`
	if _, err := db.Exec(accounts); err != nil {
		log.Fatal(context.Background(), err)
	}

	tableIDQuery := `
SELECT tables.id FROM system.namespace tables
  JOIN system.namespace dbs ON dbs.id = tables.parentid
  WHERE dbs.name = $1 AND tables.name = $2
`
	var tableID uint32
	if err := db.QueryRow(tableIDQuery, "zerosum", "accounts").Scan(&tableID); err != nil {
		log.Fatal(context.Background(), err)
	}
	return tableID
}

func (z *zeroSum) accountDistribution(r *rand.Rand) *rand.Zipf {
	// We use a Zipf distribution for selecting accounts.
	return rand.NewZipf(r, 1.1, float64(z.numAccounts/10), uint64(z.numAccounts-1))
}

func (z *zeroSum) accountsLen() int {
	z.accounts.Lock()
	defer z.accounts.Unlock()
	return len(z.accounts.m)
}

func (z *zeroSum) worker() {
	r := rand.New(rand.NewSource(int64(timeutil.Now().UnixNano())))
	zipf := z.accountDistribution(r)

	for {
		from := zipf.Uint64()
		to := zipf.Uint64()
		if from == to {
			continue
		}

		db := z.db[r.Intn(len(z.db))]
		err := crdb.ExecuteTx(db, func(tx *gosql.Tx) error {
			rows, err := tx.Query(`SELECT id, balance FROM accounts WHERE id IN ($1, $2)`, from, to)
			if err != nil {
				return err
			}

			var fromBalance, toBalance int64
			for rows.Next() {
				var id uint64
				var balance int64
				if err = rows.Scan(&id, &balance); err != nil {
					log.Fatal(context.Background(), err)
				}
				switch id {
				case from:
					fromBalance = balance
				case to:
					toBalance = balance
				default:
					panic(fmt.Sprintf("got unexpected account %d", id))
				}
			}

			upsert := `UPSERT INTO accounts VALUES ($1, $3), ($2, $4)`
			_, err = tx.Exec(upsert, to, from, toBalance+1, fromBalance-1)
			return err
		})
		if err != nil {
			log.Error(context.Background(), err)
			atomic.AddUint64(&z.stats.errors, 1)
		} else {
			atomic.AddUint64(&z.stats.ops, 1)
			z.accounts.Lock()
			z.accounts.m[from] = struct{}{}
			z.accounts.m[to] = struct{}{}
			z.accounts.Unlock()
		}
	}
}

func (z *zeroSum) monkey(tableID uint32, d time.Duration) {
	r := rand.New(rand.NewSource(int64(timeutil.Now().UnixNano())))
	zipf := z.accountDistribution(r)

	for {
		time.Sleep(time.Duration(rand.Float64() * float64(d)))

		key := keys.MakeTablePrefix(tableID)
		key = encoding.EncodeVarintAscending(key, int64(zipf.Uint64()))
		key = keys.MakeRowSentinelKey(key)

		switch r.Intn(2) {
		case 0:
			if err := z.split(r.Intn(len(z.nodes)), key); err != nil {
				log.Error(context.Background(), err)
				atomic.AddUint64(&z.stats.errors, 1)
			} else {
				atomic.AddUint64(&z.stats.splits, 1)
			}
		case 1:
			if transferred, err := z.transferLease(r.Intn(len(z.nodes)), r, key); err != nil {
				log.Error(context.Background(), err)
				atomic.AddUint64(&z.stats.errors, 1)
			} else if transferred {
				atomic.AddUint64(&z.stats.transfers, 1)
			}
		}
	}
}

func (z *zeroSum) check(d time.Duration) {
	for {
		time.Sleep(d)

		client := z.clients[rand.Intn(len(z.clients))]
		if err := client.CheckConsistency(keys.LocalMax, keys.MaxKey, false); err != nil {
			log.Fatal(context.Background(), err)
		}
	}
}

func (z *zeroSum) verify(d time.Duration) {
	for {
		time.Sleep(d)

		// Grab the count of accounts from committed transactions first. The number
		// of accounts found by the SELECT should be at least this number.
		committedAccounts := uint64(z.accountsLen())

		q := `SELECT count(*), sum(balance) FROM accounts`
		var accounts uint64
		var total int64
		db := z.db[rand.Intn(len(z.db))]
		if err := db.QueryRow(q).Scan(&accounts, &total); err != nil {
			log.Error(context.Background(), err)
			atomic.AddUint64(&z.stats.errors, 1)
			continue
		}
		if total != 0 {
			log.Fatalf(context.Background(), "unexpected total balance %d", total)
		}
		if accounts < committedAccounts {
			log.Fatalf(context.Background(), "expected at least %d accounts, but found %d",
				committedAccounts, accounts)
		}
	}
}

func (z *zeroSum) replicaInfo() (int, string) {
	replicas := make([]int, len(z.nodes))
	client := z.clients[rand.Intn(len(z.clients))]
	rows, err := client.Scan(keys.Meta2Prefix, keys.Meta2KeyMax, 0)
	if err != nil {
		log.Error(context.Background(), err)
		atomic.AddUint64(&z.stats.errors, 1)
		return -1, ""
	}
	for _, row := range rows {
		desc := &roachpb.RangeDescriptor{}
		if err := row.ValueProto(desc); err != nil {
			log.Errorf(context.Background(), "%s: unable to unmarshal range descriptor", row.Key)
			atomic.AddUint64(&z.stats.errors, 1)
			continue
		}
		for _, replica := range desc.Replicas {
			replicas[replica.NodeID-1]++
		}
	}
	s := fmt.Sprint(replicas)
	return len(rows), s[1 : len(s)-1]
}

func (z *zeroSum) monitor(d time.Duration) {
	var timer timeutil.Timer
	defer timer.Stop()
	timer.Reset(d)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	start := timeutil.Now()
	lastTime := start
	var lastOps uint64

	for ticks := 0; true; ticks++ {
		select {
		case s := <-signalCh:
			log.Infof(context.Background(), "signal received: %v", s)
			return
		case <-timer.C:
			if ticks%20 == 0 {
				fmt.Printf("_elapsed__accounts_________ops__ops/sec___errors___splits____xfers___ranges_____________replicas\n")
			}

			now := timeutil.Now()
			elapsed := now.Sub(lastTime).Seconds()
			ops := atomic.LoadUint64(&z.stats.ops)
			ranges, replicas := z.replicaInfo()

			fmt.Printf("%8s %9d %11d %8.1f %8d %8d %8d %8d %20s\n",
				time.Duration(now.Sub(start).Seconds()+0.5)*time.Second,
				z.accountsLen(), ops, float64(ops-lastOps)/elapsed,
				atomic.LoadUint64(&z.stats.errors),
				atomic.LoadUint64(&z.stats.splits),
				atomic.LoadUint64(&z.stats.transfers),
				ranges, replicas)
			lastTime = now
			lastOps = ops

			timer.Read = true
			timer.Reset(d)
		}
	}
}

func main() {
	flag.Parse()

	c := newCluster(*numNodes)
	defer c.close()

	c.start("zerosum", flag.Args())
	c.waitForFullReplication()

	z := newZeroSum(c, *numAccounts)
	z.run(*workers, *monkeys)
}
