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
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/cmd/internal/localcluster"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var workers = flag.Int("w", 1, "number of workers; the i'th worker talks to node i%numNodes")
var numNodes = flag.Int("n", 4, "number of nodes")
var duration = flag.Duration("duration", math.MaxInt64, "how long to run the simulation for")
var blockSize = flag.Int("b", 1000, "block size")

func newRand() *rand.Rand {
	return rand.New(rand.NewSource(timeutil.Now().UnixNano()))
}

// allocSim is allows investigation of allocation/rebalancing heuristics. A
// pool of workers generates block_writer-style load where the i'th worker
// talks to node i%numNodes. Every second a monitor goroutine outputs status
// such as the per-node replica and leaseholder counts.
//
// TODO(peter): Allow configuration of per-node locality settings and
// zone-config constraints.

type allocSim struct {
	*localcluster.Cluster
	stats struct {
		ops    uint64
		errors uint64
	}
	ranges struct {
		syncutil.Mutex
		count    int
		replicas []int
		leases   []int
	}
}

func newAllocSim(c *localcluster.Cluster) *allocSim {
	return &allocSim{
		Cluster: c,
	}
}

func (a *allocSim) run(workers int) {
	a.setup()
	for i := 0; i < workers; i++ {
		go a.worker(i, workers)
	}
	go a.rangeStats(time.Second)
	a.monitor(time.Second)
}

func (a *allocSim) setup() {
	db := a.DB[0]
	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS allocsim"); err != nil {
		log.Fatal(context.Background(), err)
	}

	blocks := `
CREATE TABLE IF NOT EXISTS blocks (
  id INT NOT NULL,
  num INT NOT NULL,
  data BYTES NOT NULL,
  PRIMARY KEY (id, num)
)
`
	if _, err := db.Exec(blocks); err != nil {
		log.Fatal(context.Background(), err)
	}
}

func (a *allocSim) maybeLogError(err error) {
	if localcluster.IsUnavailableError(err) {
		return
	}
	log.Error(context.Background(), err)
	atomic.AddUint64(&a.stats.errors, 1)
}

func (a *allocSim) worker(i, workers int) {
	const insert = `INSERT INTO allocsim.blocks (id, num, data) VALUES ($1, $2, repeat('a', $3))`

	r := newRand()
	db := a.DB[i%len(a.DB)]

	for num := i; true; num += workers {
		if _, err := db.Exec(insert, r.Int63(), num, *blockSize); err != nil {
			a.maybeLogError(err)
		} else {
			atomic.AddUint64(&a.stats.ops, 1)
		}
	}
}

func (a *allocSim) rangeInfo() (total int, replicas []int, leases []int) {
	replicas = make([]int, len(a.Nodes))
	leases = make([]int, len(a.Nodes))

	// Retrieve the metrics for each node and extract the replica and leaseholder
	// counts.
	var wg sync.WaitGroup
	wg.Add(len(a.Status))
	for i := range a.Status {
		go func(i int) {
			defer wg.Done()
			resp, err := a.Status[i].Metrics(context.Background(), &serverpb.MetricsRequest{
				NodeId: fmt.Sprintf("%d", i+1),
			})
			if err != nil {
				log.Fatal(context.Background(), err)
			}
			var metrics map[string]interface{}
			if err := json.Unmarshal(resp.Data, &metrics); err != nil {
				log.Fatal(context.Background(), err)
			}
			stores := metrics["stores"].(map[string]interface{})
			for _, v := range stores {
				storeMetrics := v.(map[string]interface{})
				if v, ok := storeMetrics["replicas"]; ok {
					replicas[i] += int(v.(float64))
				}
				if v, ok := storeMetrics["replicas.leaseholders"]; ok {
					leases[i] += int(v.(float64))
				}
			}
		}(i)
	}
	wg.Wait()

	for _, v := range replicas {
		total += v
	}
	return total, replicas, leases
}

func (a *allocSim) rangeStats(d time.Duration) {
	for {
		count, replicas, leases := a.rangeInfo()
		a.ranges.Lock()
		a.ranges.count = count
		a.ranges.replicas = replicas
		a.ranges.leases = leases
		a.ranges.Unlock()

		time.Sleep(d)
	}
}

func printLine(inputs []string, underscores bool) {
	var buf bytes.Buffer
	for i, input := range inputs {
		switch i {
		case 0:
			fmt.Fprintf(&buf, "%8.8s", input)
		case 1, 2, 3:
			fmt.Fprintf(&buf, " %8.8s", input)
		default:
			// All replica and lease counts need 9 characters instead of 8.
			fmt.Fprintf(&buf, " %9.9s", input)
		}
	}
	if !underscores {
		fmt.Println(buf.String())
		return
	}
	fmt.Println(strings.Replace(buf.String(), " ", "_", -1))
}

func (a *allocSim) monitor(d time.Duration) {
	start := timeutil.Now()
	lastTime := start
	var lastOps uint64
	columnCount := *numNodes + 4
	header := make([]string, columnCount, columnCount)
	header[0] = "elapsed"
	header[1] = "ops/sec"
	header[2] = "errors"
	header[3] = "replicas"
	for i := 0; i < *numNodes; i++ {
		header[i+4] = fmt.Sprintf("%d", i)
	}

	const deadPre = "\033[0;31;49m"
	const deadPost = "\033[0m]"
	formatRow := func(
		elapsed time.Duration,
		ops float64,
		errs uint64,
		replicaTotal int,
		replicas, leases []int) []string {
		row := make([]string, columnCount, columnCount)
		row[0] = fmt.Sprintf("%s", elapsed)
		row[1] = fmt.Sprintf("%.1f", ops)
		row[2] = fmt.Sprintf("%d", errs)
		row[3] = fmt.Sprintf("%d", replicaTotal)
		for i := range replicas {
			var pre, post string
			if !a.Nodes[i].Alive() {
				pre = deadPre
				post = deadPost
			}
			row[4+i] = fmt.Sprintf("%s%d/%d%s", pre, replicas[i], leases[i], post)
		}
		return row
	}

	for ticks := 0; true; ticks++ {
		time.Sleep(d)

		now := timeutil.Now()
		elapsed := now.Sub(lastTime).Seconds()
		ops := atomic.LoadUint64(&a.stats.ops)

		a.ranges.Lock()
		ranges := a.ranges.count
		replicas := a.ranges.replicas
		leases := a.ranges.leases
		a.ranges.Unlock()

		if ticks%20 == 0 {
			printLine(header, true)
		}
		printLine(formatRow(
			time.Duration(now.Sub(start).Seconds()+0.5)*time.Second,
			float64(ops-lastOps)/elapsed,
			atomic.LoadUint64(&a.stats.errors),
			ranges,
			replicas,
			leases,
		), false)
		lastTime = now
		lastOps = ops
	}
}

func (a *allocSim) finalStatus() {
	a.ranges.Lock()
	defer a.ranges.Unlock()

	// TOTO(bram): With the addition of localities, these stats will have to be
	// updated.
	columnCount := *numNodes + 4
	header := make([]string, columnCount, columnCount)
	header[0] = "stats"
	for i := 0; i < *numNodes; i++ {
		header[i+4] = fmt.Sprintf("%d", i)
	}
	printLine(header, true)

	genStats := func(name string, counts []int) {
		var total float64
		for _, count := range counts {
			total += float64(count)
		}
		row := make([]string, columnCount, columnCount)
		row[0] = name
		row[1] = "(total%"
		row[2] = "/ diff%)"
		mean := total / float64(len(counts))
		for i, count := range counts {
			var percent, fromMean float64
			if total != 0 {
				percent = float64(count) / total * 100
				fromMean = math.Abs((float64(count) - mean) / total * 100)
			}
			row[i+4] = fmt.Sprintf("%#2.1f/%#2.1f", percent, fromMean)
		}
		printLine(row, false)
	}
	genStats("replicas", a.ranges.replicas)
	genStats("leases", a.ranges.leases)
}

func main() {
	flag.Parse()

	c := localcluster.New(*numNodes)
	defer c.Close()

	log.SetExitFunc(func(code int) {
		c.Close()
		os.Exit(code)
	})

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	a := newAllocSim(c)

	go func() {
		var exitStatus int
		select {
		case s := <-signalCh:
			log.Infof(context.Background(), "signal received: %v", s)
			exitStatus = 1
		case <-time.After(*duration):
			log.Infof(context.Background(), "finished run of: %s", *duration)
		}
		a.finalStatus()
		c.Close()
		os.Exit(exitStatus)
	}()

	c.Start("allocsim", *workers, flag.Args(), []string{})
	c.UpdateZoneConfig(1, 1<<20)
	a.run(*workers)
}
