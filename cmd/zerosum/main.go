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
	"bytes"
	gosql "database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strings"
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
var chaosType = flag.String("c", "simple", "chaos type [none|simple|flappy|freeze]")

func newRand() *rand.Rand {
	return rand.New(rand.NewSource(timeutil.Now().UnixNano()))
}

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
	chaosType   string
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
	ranges struct {
		syncutil.Mutex
		count    int
		replicas []int
	}
}

func newZeroSum(c *cluster, numAccounts int, chaosType string) *zeroSum {
	z := &zeroSum{
		cluster:     c,
		numAccounts: numAccounts,
		chaosType:   chaosType,
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
		go z.monkey(tableID, 2*time.Second)
	}
	if workers > 0 || monkeys > 0 {
		z.chaos()
		go z.check(20 * time.Second)
		go z.verify(10 * time.Second)
	}
	go z.rangeStats(time.Second)
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

func (z *zeroSum) maybeLogError(err error) {
	if strings.Contains(err.Error(), "range is frozen") {
		return
	}
	log.Error(context.Background(), err)
	atomic.AddUint64(&z.stats.errors, 1)
}

func (z *zeroSum) worker() {
	r := newRand()
	zipf := z.accountDistribution(r)

	for {
		from := zipf.Uint64()
		to := zipf.Uint64()
		if from == to {
			continue
		}

		db := z.db[z.randNode(r.Intn)]
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
			z.maybeLogError(err)
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
	r := newRand()
	zipf := z.accountDistribution(r)

	for {
		time.Sleep(time.Duration(rand.Float64() * float64(d)))

		key := keys.MakeTablePrefix(tableID)
		key = encoding.EncodeVarintAscending(key, int64(zipf.Uint64()))
		key = keys.MakeRowSentinelKey(key)

		switch r.Intn(2) {
		case 0:
			if err := z.split(z.randNode(r.Intn), key); err != nil {
				if strings.Contains(err.Error(), "range is already split at key") ||
					strings.Contains(err.Error(), "conflict updating range descriptors") {
					continue
				}
				z.maybeLogError(err)
			} else {
				atomic.AddUint64(&z.stats.splits, 1)
			}
		case 1:
			if transferred, err := z.transferLease(z.randNode(r.Intn), r, key); err != nil {
				z.maybeLogError(err)
			} else if transferred {
				atomic.AddUint64(&z.stats.transfers, 1)
			}
		}
	}
}

func (z *zeroSum) chaosSimple() {
	d := 15 * time.Second
	fmt.Printf("chaos(simple): first event in %s\n", d)
	time.Sleep(d)

	nodeIdx := 0
	node := z.nodes[nodeIdx]
	d = 20 * time.Second
	fmt.Printf("chaos: killing node %d for %s\n", nodeIdx+1, d)
	node.kill()

	time.Sleep(d)
	fmt.Printf("chaos: starting node %d\n", nodeIdx+1)
	node.start()
}

func (z *zeroSum) chaosFlappy() {
	r := newRand()
	d := time.Duration(15+r.Intn(30)) * time.Second
	fmt.Printf("chaos(flappy): first event in %s\n", d)

	for i := 1; true; i++ {
		time.Sleep(d)

		nodeIdx := z.randNode(r.Intn)
		node := z.nodes[nodeIdx]
		d = time.Duration(15+r.Intn(30)) * time.Second
		fmt.Printf("chaos %d: killing node %d for %s\n", i, nodeIdx+1, d)
		node.kill()

		time.Sleep(d)

		d = time.Duration(15+r.Intn(30)) * time.Second
		fmt.Printf("chaos %d: starting node %d, next event in %s\n", i, nodeIdx+1, d)
		node.start()
	}
}

func (z *zeroSum) chaosFreeze() {
	r := newRand()
	d := time.Duration(10+r.Intn(10)) * time.Second
	fmt.Printf("chaos(freeze): first event in %s\n", d)

	for i := 1; true; i++ {
		time.Sleep(d)

		d = time.Duration(10+r.Intn(10)) * time.Second
		fmt.Printf("chaos %d: freezing cluster for %s\n", i, d)
		z.freeze(z.randNode(rand.Intn), true)

		time.Sleep(d)

		d = time.Duration(10+r.Intn(10)) * time.Second
		fmt.Printf("chaos %d: thawing cluster, next event in %s\n", i, d)
		z.freeze(z.randNode(rand.Intn), false)
	}
}

func (z *zeroSum) chaos() {
	switch z.chaosType {
	case "none":
		// nothing to do
	case "simple":
		go z.chaosSimple()
	case "flappy":
		go z.chaosFlappy()
	case "freeze":
		go z.chaosFreeze()
	default:
		log.Fatalf(context.Background(), "unknown chaos type: %s", z.chaosType)
	}
}

func (z *zeroSum) check(d time.Duration) {
	for {
		time.Sleep(d)

		client := z.clients[z.randNode(rand.Intn)]
		if err := client.CheckConsistency(keys.LocalMax, keys.MaxKey, false); err != nil {
			z.maybeLogError(err)
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
		db := z.db[z.randNode(rand.Intn)]
		if err := db.QueryRow(q).Scan(&accounts, &total); err != nil {
			z.maybeLogError(err)
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

func (z *zeroSum) rangeInfo() (int, []int) {
	replicas := make([]int, len(z.nodes))
	client := z.clients[z.randNode(rand.Intn)]
	rows, err := client.Scan(keys.Meta2Prefix, keys.Meta2KeyMax, 0)
	if err != nil {
		z.maybeLogError(err)
		return -1, replicas
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

	return len(rows), replicas
}

func (z *zeroSum) rangeStats(d time.Duration) {
	for {
		count, replicas := z.rangeInfo()
		z.ranges.Lock()
		z.ranges.count, z.ranges.replicas = count, replicas
		z.ranges.Unlock()

		time.Sleep(d)
	}
}

func (z *zeroSum) formatReplicas(replicas []int) string {
	var buf bytes.Buffer
	for i := range replicas {
		if i > 0 {
			_, _ = buf.WriteString(" ")
		}
		fmt.Fprintf(&buf, "%d", replicas[i])
		if !z.nodes[i].alive() {
			_, _ = buf.WriteString("*")
		}
	}
	return buf.String()
}

func (z *zeroSum) monitor(d time.Duration) {
	start := timeutil.Now()
	lastTime := start
	var lastOps uint64

	for ticks := 0; true; ticks++ {
		time.Sleep(d)

		if ticks%20 == 0 {
			fmt.Printf("_elapsed__accounts_________ops__ops/sec___errors___splits____xfers___ranges_____________replicas\n")
		}

		now := timeutil.Now()
		elapsed := now.Sub(lastTime).Seconds()
		ops := atomic.LoadUint64(&z.stats.ops)

		z.ranges.Lock()
		ranges, replicas := z.ranges.count, z.ranges.replicas
		z.ranges.Unlock()

		fmt.Printf("%8s %9d %11d %8.1f %8d %8d %8d %8d %20s\n",
			time.Duration(now.Sub(start).Seconds()+0.5)*time.Second,
			z.accountsLen(), ops, float64(ops-lastOps)/elapsed,
			atomic.LoadUint64(&z.stats.errors),
			atomic.LoadUint64(&z.stats.splits),
			atomic.LoadUint64(&z.stats.transfers),
			ranges, z.formatReplicas(replicas))
		lastTime = now
		lastOps = ops
	}
}

func main() {
	flag.Parse()

	c := newCluster(*numNodes)
	defer c.close()

	log.SetExitFunc(func(code int) {
		c.close()
		os.Exit(code)
	})

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		s := <-signalCh
		log.Infof(context.Background(), "signal received: %v", s)
		c.close()
		os.Exit(1)
	}()

	c.start("zerosum", flag.Args())
	c.waitForFullReplication()

	z := newZeroSum(c, *numAccounts, *chaosType)
	z.run(*workers, *monkeys)
}
