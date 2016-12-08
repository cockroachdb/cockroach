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
	"sync"
	"sync/atomic"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/olekukonko/tablewriter"
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

func (a *allocSim) monitor(d time.Duration) {
	const padding = "__________"

	formatHeader := func(numReplicas int) string {
		var buf bytes.Buffer
		_, _ = buf.WriteString("_elapsed__ops/sec___errors_replicas")
		for i := 1; i <= numReplicas; i++ {
			node := fmt.Sprintf("%d", i)
			fmt.Fprintf(&buf, "%s%s", padding[:len(padding)-len(node)], node)
		}
		return buf.String()
	}

	formatNodes := func(replicas, leases []int) string {
		var buf bytes.Buffer
		for i := range replicas {
			alive := a.Nodes[i].Alive()
			if !alive {
				_, _ = buf.WriteString("\033[0;31;49m")
			}
			fmt.Fprintf(&buf, "%*s", len(padding), fmt.Sprintf("%d/%d", replicas[i], leases[i]))
			if !alive {
				_, _ = buf.WriteString("\033[0m")
			}
		}
		return buf.String()
	}

	start := timeutil.Now()
	lastTime := start
	var numReplicas int
	var lastOps uint64

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

		if ticks%20 == 0 || numReplicas != len(replicas) {
			numReplicas = len(replicas)
			fmt.Println(formatHeader(numReplicas))
		}

		fmt.Printf("%8s %8.1f %8d %8d%s\n",
			time.Duration(now.Sub(start).Seconds()+0.5)*time.Second,
			float64(ops-lastOps)/elapsed, atomic.LoadUint64(&a.stats.errors),
			ranges, formatNodes(replicas, leases))
		lastTime = now
		lastOps = ops
	}
}

func printFinalStats(name string, counts []int) {
	var max, total int
	var mean, maxDiff float64
	min := math.MaxInt32
	for _, count := range counts {
		if count > max {
			max = count
		}
		if count < min {
			min = count
		}
		mean += float64(count) / float64(len(counts))
		total += count
	}
	percentsRow := make([]string, len(counts)+1)
	percentsRow[0] = "% total"
	diffsRow := make([]string, len(counts)+1)
	diffsRow[0] = "% from mean"
	for i, count := range counts {
		percentsRow[i+1] = fmt.Sprintf("%.1f", float64(count)/float64(total)*100)
		diff := math.Abs((float64(count)-mean)/mean) * 100
		if diff > maxDiff {
			maxDiff = diff
		}
		diffsRow[i+1] = fmt.Sprintf("%.1f", diff)
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetAutoFormatHeaders(false)
	table.SetAlignment(tablewriter.ALIGN_RIGHT)

	header := make([]string, len(counts)+1)
	header[0] = name
	for i := 0; i < len(counts); i++ {
		header[i+1] = fmt.Sprintf("%d", 1)
	}
	table.SetHeader(header)
	table.Append(percentsRow)
	table.Append(diffsRow)
	table.Render()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.AlignRight)
	fmt.Fprintf(w, "Minimum\t %d\n", min)
	fmt.Fprintf(w, "Maximum\t %d\n", max)
	fmt.Fprintf(w, "Mean\t %.1f\n", mean)
	fmt.Fprintf(w, "Maximum Diff\t %.1f%%\n", maxDiff)
	w.Flush()
}

func (a *allocSim) finalStatus() {
	a.ranges.Lock()
	defer a.ranges.Unlock()

	// TOTO(bram): With the addition of localities, these stats will have to be
	// updated.

	fmt.Printf("\n\nTotal Ranges: %d\n", a.ranges.count)

	printFinalStats("Replicas", a.ranges.replicas)
	printFinalStats("Leases", a.ranges.leases)
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

	c.Start("allocsim", *workers, flag.Args(), []string{})
	c.UpdateZoneConfig(1, 1<<20)

	a := newAllocSim(c)

	go func() {
		select {
		case s := <-signalCh:
			log.Infof(context.Background(), "signal received: %v", s)
		case <-time.After(*duration):
			log.Infof(context.Background(), "finished run of: %s", *duration)
		}
		a.finalStatus()
		c.Close()
		os.Exit(1)
	}()

	a.run(*workers)
}
