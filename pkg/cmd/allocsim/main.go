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
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

var workers = flag.Int("w", 1, "number of workers; the i'th worker talks to node i%numNodes")
var numNodes = flag.Int("n", 4, "number of nodes")
var duration = flag.Duration("duration", math.MaxInt64, "how long to run the simulation for")
var blockSize = flag.Int("b", 1000, "block size")
var configFile = flag.String("f", "", "config file that specifies an allocsim workload (overrides -n)")

// Configuration provides a way to configure allocsim via a JSON file.
// TODO(a-robinson): Consider moving all the above options into the config file.
type Configuration struct {
	NumWorkers int        `json:"NumWorkers"`
	Localities []Locality `json:"Localities"`
}

// Locality defines the properties of a single locality as part of a Configuration.
type Locality struct {
	Name              string `json:"Name"`
	NumNodes          int    `json:"NumNodes"`
	NumWorkers        int    `json:"NumWorkers"`
	OutgoingLatencies []*struct {
		Name    string       `json:"Name"`
		Latency jsonDuration `json:"Latency"`
	} `json:"OutgoingLatencies"`
}

type jsonDuration time.Duration

func (j *jsonDuration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*j = jsonDuration(dur)
	return nil
}

func loadConfig(file string) (Configuration, error) {
	fileHandle, err := os.Open(file)
	if err != nil {
		return Configuration{}, errors.Wrapf(err, "failed to open config file %q", file)
	}
	defer fileHandle.Close()

	var config Configuration
	jsonParser := json.NewDecoder(fileHandle)
	if err := jsonParser.Decode(&config); err != nil {
		return Configuration{}, errors.Wrapf(err, "failed to decode %q as json", file)
	}

	*numNodes = 0
	*workers = config.NumWorkers
	for _, locality := range config.Localities {
		*numNodes += locality.NumNodes
		*workers += locality.NumWorkers
	}
	return config, nil
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
		ops               uint64
		totalLatencyNanos uint64
		errors            uint64
	}
	ranges struct {
		syncutil.Mutex
		count          int
		replicas       []int
		leases         []int
		leaseTransfers []int
	}
	localities []Locality
}

func newAllocSim(c *localcluster.Cluster) *allocSim {
	return &allocSim{
		Cluster: c,
	}
}

func (a *allocSim) run(workers int) {
	a.setup()
	for i := 0; i < workers; i++ {
		go a.roundRobinWorker(i, workers)
	}
	go a.rangeStats(time.Second)
	a.monitor(time.Second)
}

func (a *allocSim) runWithConfig(config Configuration) {
	a.setup()

	numWorkers := config.NumWorkers
	for _, locality := range config.Localities {
		numWorkers += locality.NumWorkers
	}

	firstNodeInLocality := 0
	for _, locality := range config.Localities {
		for i := 0; i < locality.NumWorkers; i++ {
			node := firstNodeInLocality + (i % locality.NumNodes)
			startNum := firstNodeInLocality + i
			go a.worker(node, startNum, numWorkers)
		}
		firstNodeInLocality += locality.NumNodes
	}
	for i := 0; i < config.NumWorkers; i++ {
		go a.roundRobinWorker(firstNodeInLocality+i, numWorkers)
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

const insertStmt = `INSERT INTO allocsim.blocks (id, num, data) VALUES ($1, $2, repeat('a', $3)::bytes)`

func (a *allocSim) worker(dbIdx, startNum, workers int) {
	r, _ := randutil.NewPseudoRand()
	db := a.DB[dbIdx%len(a.DB)]
	for num := startNum; true; num += workers {
		now := timeutil.Now()
		if _, err := db.Exec(insertStmt, r.Int63(), num, *blockSize); err != nil {
			a.maybeLogError(err)
		} else {
			atomic.AddUint64(&a.stats.ops, 1)
			atomic.AddUint64(&a.stats.totalLatencyNanos, uint64(timeutil.Since(now).Nanoseconds()))
		}
	}
}

func (a *allocSim) roundRobinWorker(startNum, workers int) {
	r, _ := randutil.NewPseudoRand()
	for i := 0; ; i++ {
		now := timeutil.Now()
		if _, err := a.DB[i%len(a.DB)].Exec(insertStmt, r.Int63(), startNum+i*workers, *blockSize); err != nil {
			a.maybeLogError(err)
		} else {
			atomic.AddUint64(&a.stats.ops, 1)
			atomic.AddUint64(&a.stats.totalLatencyNanos, uint64(timeutil.Since(now).Nanoseconds()))
		}
	}
}

func (a *allocSim) rangeInfo() (total int, replicas, leases, leaseTransfers []int) {
	replicas = make([]int, len(a.Nodes))
	leases = make([]int, len(a.Nodes))
	leaseTransfers = make([]int, len(a.Nodes))

	// Retrieve the metrics for each node and extract the replica and leaseholder
	// counts.
	var wg sync.WaitGroup
	wg.Add(len(a.Status))
	for i := range a.Status {
		go func(i int) {
			defer wg.Done()
			resp, err := a.Status[i].Metrics(context.Background(), &serverpb.MetricsRequest{
				NodeId: fmt.Sprintf("local"),
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
				if v, ok := storeMetrics["leasestransfers.success"]; ok {
					leaseTransfers[i] += int(v.(float64))
				}
			}
		}(i)
	}
	wg.Wait()

	for _, v := range replicas {
		total += v
	}
	return total, replicas, leases, leaseTransfers
}

func (a *allocSim) rangeStats(d time.Duration) {
	for {
		count, replicas, leases, leaseTransfers := a.rangeInfo()
		a.ranges.Lock()
		a.ranges.count = count
		a.ranges.replicas = replicas
		a.ranges.leases = leases
		a.ranges.leaseTransfers = leaseTransfers
		a.ranges.Unlock()

		time.Sleep(d)
	}
}

const padding = "__________________"

func formatHeader(header string, numberNodes int, localities []Locality) string {
	var buf bytes.Buffer
	_, _ = buf.WriteString(header)
	for i := 1; i <= numberNodes; i++ {
		node := fmt.Sprintf("%d", i)
		if localities != nil {
			node += fmt.Sprintf(":%s", localities[i-1].Name)
		}
		fmt.Fprintf(&buf, "%s%s", padding[:len(padding)-len(node)], node)
	}
	return buf.String()
}

func (a *allocSim) monitor(d time.Duration) {
	formatNodes := func(replicas, leases, leaseTransfers []int) string {
		var buf bytes.Buffer
		for i := range replicas {
			alive := a.Nodes[i].Alive()
			if !alive {
				_, _ = buf.WriteString("\033[0;31;49m")
			}
			fmt.Fprintf(&buf, "%*s", len(padding), fmt.Sprintf("%d/%d/%d", replicas[i], leases[i], leaseTransfers[i]))
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
		totalLatencyNanos := atomic.LoadUint64(&a.stats.totalLatencyNanos)

		a.ranges.Lock()
		ranges := a.ranges.count
		replicas := a.ranges.replicas
		leases := a.ranges.leases
		leaseTransfers := a.ranges.leaseTransfers
		a.ranges.Unlock()

		if ticks%20 == 0 || numReplicas != len(replicas) {
			numReplicas = len(replicas)
			fmt.Println(formatHeader("_elapsed__ops/sec__average__latency___errors_replicas", numReplicas, a.localities))
		}

		fmt.Printf("%8s %8.1f %8.1f %6.1fms %8d %8d%s\n",
			time.Duration(now.Sub(start).Seconds()+0.5)*time.Second,
			float64(ops-lastOps)/elapsed, float64(ops)/now.Sub(start).Seconds(),
			float64(totalLatencyNanos/ops)/float64(time.Millisecond),
			atomic.LoadUint64(&a.stats.errors), ranges, formatNodes(replicas, leases, leaseTransfers))
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
				fromMean = (float64(count) - mean) / total * 100
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
	if err := configureArtificialLatencies(latenciesStr); err != nil {
		log.Fatal(context.Background(), err)
	}

	// Speed up lease transfer decisions by not requiring quite as much data
	// before beginning to make them. Without this, the rapid splitting of ranges
	// in the few minutes after allocsim starts up causes it to take a long time
	// for leases to settle onto other nodes even when requests are skewed heavily
	// onto them.
	storage.MinLeaseTransferStatsDuration = 10 * time.Second

	cli.Main()
	return true
}

func configureArtificialLatencies(latenciesConfig string) error {
	if latenciesConfig == "" {
		return nil
	}
	for _, latency := range strings.Split(latenciesConfig, ",") {
		parts := strings.Split(latency, "=")
		if len(parts) != 2 {
			return errors.Errorf("unexpected latency config format %q", latency)
		}
		addr := parts[0]
		dur := parts[1]
		delay, err := time.ParseDuration(dur)
		if err != nil {
			return errors.Wrapf(err, "unable to parse %q as a duration", parts[1])
		}
		rpc.ArtificialLatencies[addr] = delay
		log.Infof(context.Background(), "adding delay %s to address %q", delay, addr)
	}
	return nil
}

func main() {
	if handleStart() {
		return
	}

	flag.Parse()

	var config Configuration
	if *configFile != "" {
		var err error
		config, err = loadConfig(*configFile)
		if err != nil {
			log.Fatal(context.Background(), err)
		}
	}

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
	if len(config.Localities) != 0 {
		perNodeArgs = make(map[int][]string)
		a.localities = make([]Locality, len(c.Nodes))
		nodesPerLocality := make(map[string][]int)
		var nodeIdx int
		for _, locality := range config.Localities {
			for i := 0; i < locality.NumNodes; i++ {
				perNodeArgs[nodeIdx] = []string{fmt.Sprintf("--locality=l=%s", locality.Name)}
				a.localities[nodeIdx] = locality
				nodesPerLocality[locality.Name] = append(nodesPerLocality[locality.Name], nodeIdx)
				nodeIdx++
			}
		}
		for i, locality := range a.localities {
			var latencies []string
			for _, outgoing := range locality.OutgoingLatencies {
				if outgoing.Latency > 0 {
					for _, nodeIdx := range nodesPerLocality[outgoing.Name] {
						latencies = append(latencies,
							fmt.Sprintf("%s=%s", localcluster.RPCAddr(nodeIdx), time.Duration(outgoing.Latency)))
					}
				}
			}
			if len(latencies) != 0 {
				perNodeArgs[i] = append(perNodeArgs[i], "--latencies="+strings.Join(latencies, ","))
			}
		}
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
	if len(config.Localities) != 0 {
		a.runWithConfig(config)
	} else {
		a.run(*workers)
	}
}
