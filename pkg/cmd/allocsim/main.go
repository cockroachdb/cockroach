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

	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/cmd/internal/localcluster"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

var workers = flag.Int("w", 1, "number of workers; the i'th worker talks to node i%numNodes")
var numNodes = flag.Int("n", 4, "number of nodes")
var duration = flag.Duration("duration", math.MaxInt64, "how long to run the simulation for")
var blockSize = flag.Int("b", 1000, "block size")
var numLocalities = flag.Int("l", 0, "number of localities")

func newRand() *rand.Rand {
	return rand.New(rand.NewSource(timeutil.Now().UnixNano()))
}

// allocSim allows investigation of allocation/rebalancing heuristics. A
// pool of workers generates block_writer-style load where the i'th worker
// talks to node i%numNodes. Every second a monitor goroutine outputs status
// such as the per-node replica and leaseholder counts.
//
// TODO(peter/a-robinson): Allow configuration of zone-config constraints.
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
	localities []roachpb.Locality
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
	const insert = `INSERT INTO allocsim.blocks (id, num, data) VALUES ($1, $2, repeat('a', $3)::bytes)`

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

const padding = "__________"

func formatHeader(header string, numberNodes int, localities []roachpb.Locality) string {
	var buf bytes.Buffer
	_, _ = buf.WriteString(header)
	for i := 1; i <= numberNodes; i++ {
		node := fmt.Sprintf("%d", i)
		if localities != nil {
			node += fmt.Sprintf(":%s", localities[i-1])
		}
		fmt.Fprintf(&buf, "%s%s", padding[:len(padding)-len(node)], node)
	}
	return buf.String()
}

func (a *allocSim) monitor(d time.Duration) {
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
			fmt.Println(formatHeader("_elapsed__ops/sec___errors_replicas", numReplicas, a.localities))
		}

		fmt.Printf("%8s %8.1f %8d %8d%s\n",
			time.Duration(now.Sub(start).Seconds()+0.5)*time.Second,
			float64(ops-lastOps)/elapsed, atomic.LoadUint64(&a.stats.errors),
			ranges, formatNodes(replicas, leases))
		lastTime = now
		lastOps = ops
	}
}

func (a *allocSim) finalStatus() {
	a.ranges.Lock()
	defer a.ranges.Unlock()

	// TODO(bram): With the addition of localities, these stats will have to be
	// updated.

	fmt.Println(formatHeader("___stats___________________________", len(a.ranges.replicas), a.localities))

	genStats := func(name string, counts []int) {
		var total float64
		for _, count := range counts {
			total += float64(count)
		}
		mean := total / float64(len(counts))
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "%8s  (total%% / diff%%)         ", name)
		for _, count := range counts {
			var percent, fromMean float64
			if total != 0 {
				percent = float64(count) / total * 100
				fromMean = math.Abs((float64(count) - mean) / total * 100)
			}
			fmt.Fprintf(&buf, " %9.9s", fmt.Sprintf("%.0f/%.0f", percent, fromMean))
		}
		fmt.Println(buf.String())
	}
	genStats("replicas", a.ranges.replicas)
	genStats("leases", a.ranges.leases)
}

func handleStart() bool {
	if len(os.Args) < 2 || os.Args[1] != "start" {
		return false
	}

	// Do our own hacky flag parsing here to get around the fact that the default
	// flag package complains about any unknown flags while still allowing the
	// pflags package used by cli.Start to manage the rest of the flags as usual.
	var latenciesStr string
	for i, arg := range os.Args {
		if strings.HasPrefix(arg, "--latencies=") || strings.HasPrefix(arg, "-latencies=") {
			latenciesStr = strings.SplitAfterN(arg, "=", 2)[1]
			os.Args = append(os.Args[:i], os.Args[i+1:]...)
			break
		}
	}
	if latenciesStr != "" {
		if err := configureArtificialLatencies(latenciesStr); err != nil {
			log.Fatal(context.Background(), err)
		}
	}

	cli.Main()
	return true
}

func configureArtificialLatencies(latenciesConfig string) error {
	if latenciesConfig == "" {
		return nil
	}
	delays := make(map[string]time.Duration)
	for _, latency := range strings.Split(latenciesConfig, ",") {
		parts := strings.Split(latency, "=")
		if len(parts) != 2 {
			return errors.Errorf("unexpected latency config format %q", latency)
		}
		addr := parts[0]
		delay, err := time.ParseDuration(parts[1])
		if err != nil {
			return errors.Wrapf(err, "unable to parse %q as a duration", parts[1])
		}
		delays[addr] = delay
		log.Infof(context.Background(), "adding delay %s to address %q", delay, addr)
	}
	rpc.ArtificialLatencies = delays
	return nil
}

func main() {
	if handleStart() {
		return
	}

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

	var perNodeArgs map[int][]string
	if *numLocalities > 0 {
		perNodeArgs = make(map[int][]string)
		a.localities = make([]roachpb.Locality, len(c.Nodes), len(c.Nodes))
		for i := range c.Nodes {
			locality := roachpb.Locality{
				Tiers: []roachpb.Tier{
					{
						Key:   "l",
						Value: fmt.Sprintf("%d", i%*numLocalities),
					},
				},
			}
			perNodeArgs[i] = []string{fmt.Sprintf("--locality=%s", locality)}
			a.localities[i] = locality
		}

		// TODO(a-robinson): Enable actual configuration of this and make it work for
		// different numbers of nodes.
		if len(c.Nodes) != 4 {
			log.Fatal(context.Background(), "localities/latencies are currently only supported for 4 nodes")
		}
		if *numLocalities != 2 {
			log.Fatal(context.Background(), "localities/latencies are currently only supported for 4 nodes and 2 localities")
		}
		perNodeArgs[0] = append(perNodeArgs[0],
			fmt.Sprintf("--latencies=%s=%s,%s=%s,%s=%s", localcluster.RPCAddr(1), 50*time.Millisecond, localcluster.RPCAddr(2), 0*time.Millisecond, localcluster.RPCAddr(3), 50*time.Millisecond))
		perNodeArgs[1] = append(perNodeArgs[1],
			fmt.Sprintf("--latencies=%s=%s,%s=%s,%s=%s", localcluster.RPCAddr(0), 50*time.Millisecond, localcluster.RPCAddr(2), 50*time.Millisecond, localcluster.RPCAddr(3), 0*time.Millisecond))
		perNodeArgs[2] = append(perNodeArgs[2],
			fmt.Sprintf("--latencies=%s=%s,%s=%s,%s=%s", localcluster.RPCAddr(0), 0*time.Millisecond, localcluster.RPCAddr(1), 50*time.Millisecond, localcluster.RPCAddr(3), 50*time.Millisecond))
		perNodeArgs[3] = append(perNodeArgs[3],
			fmt.Sprintf("--latencies=%s=%s,%s=%s,%s=%s", localcluster.RPCAddr(0), 50*time.Millisecond, localcluster.RPCAddr(1), 0*time.Millisecond, localcluster.RPCAddr(2), 50*time.Millisecond))
	}

	go func() {
		var exitStatus int
		select {
		case s := <-signalCh:
			log.Infof(context.Background(), "signal received: %v", s)
			exitStatus = 1
		case <-time.After(*duration):
			log.Infof(context.Background(), "finished run of: %s", *duration)
		}
		c.Close()
		a.finalStatus()
		os.Exit(exitStatus)
	}()

	c.Start("allocsim", *workers, os.Args[0], nil, flag.Args(), perNodeArgs)
	c.UpdateZoneConfig(1, 1<<20)
	a.run(*workers)
}
