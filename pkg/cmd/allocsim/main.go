// Copyright 2016 The Cockroach Authors.
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
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cockroachdb/cockroach/pkg/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/pkg/acceptance/localcluster/tc"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
	LocalityStr       string `json:"LocalityStr"`
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
		stats allocStats
	}
	localities []Locality
}

type allocStats struct {
	count          int
	replicas       []int
	leases         []int
	replicaAdds    []int
	leaseTransfers []int
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
	db := a.Nodes[0].DB()
	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS allocsim"); err != nil {
		log.Fatalf(context.Background(), "%v", err)
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
		log.Fatalf(context.Background(), "%v", err)
	}
}

func (a *allocSim) maybeLogError(err error) {
	if localcluster.IsUnavailableError(err) {
		return
	}
	log.Errorf(context.Background(), "%v", err)
	atomic.AddUint64(&a.stats.errors, 1)
}

const insertStmt = `INSERT INTO allocsim.blocks (id, num, data) VALUES ($1, $2, repeat('a', $3)::bytes)`

func (a *allocSim) worker(dbIdx, startNum, workers int) {
	r, _ := randutil.NewPseudoRand()
	db := a.Nodes[dbIdx%len(a.Nodes)].DB()
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
		db := a.Nodes[i%len(a.Nodes)].DB()
		if db == nil {
			continue // nodes are shutting down
		}
		if _, err := db.Exec(insertStmt, r.Int63(), startNum+i*workers, *blockSize); err != nil {
			a.maybeLogError(err)
		} else {
			atomic.AddUint64(&a.stats.ops, 1)
			atomic.AddUint64(&a.stats.totalLatencyNanos, uint64(timeutil.Since(now).Nanoseconds()))
		}
	}
}

func (a *allocSim) rangeInfo() allocStats {
	stats := allocStats{
		replicas:       make([]int, len(a.Nodes)),
		replicaAdds:    make([]int, len(a.Nodes)),
		leases:         make([]int, len(a.Nodes)),
		leaseTransfers: make([]int, len(a.Nodes)),
	}

	// Retrieve the metrics for each node and extract the replica and leaseholder
	// counts.
	var wg sync.WaitGroup
	wg.Add(len(a.Nodes))
	for i := 0; i < len(a.Nodes); i++ {
		go func(i int) {
			defer wg.Done()
			status := a.Nodes[i].StatusClient()
			if status == nil {
				// Cluster is shutting down.
				return
			}
			resp, err := status.Metrics(context.Background(), &serverpb.MetricsRequest{
				NodeId: "local",
			})
			if err != nil {
				log.Fatalf(context.Background(), "%v", err)
			}
			var metrics map[string]interface{}
			if err := json.Unmarshal(resp.Data, &metrics); err != nil {
				log.Fatalf(context.Background(), "%v", err)
			}
			stores := metrics["stores"].(map[string]interface{})
			for _, v := range stores {
				storeMetrics := v.(map[string]interface{})
				if v, ok := storeMetrics["replicas"]; ok {
					stats.replicas[i] += int(v.(float64))
				}
				if v, ok := storeMetrics["replicas.leaseholders"]; ok {
					stats.leases[i] += int(v.(float64))
				}
				if v, ok := storeMetrics["range.adds"]; ok {
					stats.replicaAdds[i] += int(v.(float64))
				}
				if v, ok := storeMetrics["leases.transfers.success"]; ok {
					stats.leaseTransfers[i] += int(v.(float64))
				}
			}
		}(i)
	}
	wg.Wait()

	for _, v := range stats.replicas {
		stats.count += v
	}
	return stats
}

func (a *allocSim) rangeStats(d time.Duration) {
	for {
		stats := a.rangeInfo()
		a.ranges.Lock()
		a.ranges.stats = stats
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
	formatNodes := func(stats allocStats) string {
		var buf bytes.Buffer
		for i := range stats.replicas {
			alive := a.Nodes[i].Alive()
			if !alive {
				_, _ = buf.WriteString("\033[0;31;49m")
			}
			fmt.Fprintf(&buf, "%*s", len(padding), fmt.Sprintf("%d/%d/%d/%d",
				stats.replicas[i], stats.leases[i], stats.replicaAdds[i], stats.leaseTransfers[i]))
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
		rangeStats := a.ranges.stats
		a.ranges.Unlock()

		if ticks%20 == 0 || numReplicas != len(rangeStats.replicas) {
			numReplicas = len(rangeStats.replicas)
			fmt.Println(formatHeader("_elapsed__ops/sec__average__latency___errors_replicas", numReplicas, a.localities))
		}

		var avgLatency float64
		if ops > 0 {
			avgLatency = float64(totalLatencyNanos/ops) / float64(time.Millisecond)
		}
		fmt.Printf("%8s %8.1f %8.1f %6.1fms %8d %8d%s\n",
			time.Duration(now.Sub(start).Seconds()+0.5)*time.Second,
			float64(ops-lastOps)/elapsed, float64(ops)/now.Sub(start).Seconds(), avgLatency,
			atomic.LoadUint64(&a.stats.errors), rangeStats.count, formatNodes(rangeStats))
		lastTime = now
		lastOps = ops
	}
}

func (a *allocSim) finalStatus() {
	a.ranges.Lock()
	defer a.ranges.Unlock()

	// TODO(bram): With the addition of localities, these stats will have to be
	// updated.

	fmt.Println(formatHeader("___stats___________________________", len(a.ranges.stats.replicas), a.localities))

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
	genStats("replicas", a.ranges.stats.replicas)
	genStats("leases", a.ranges.stats.leases)
}

func handleStart() bool {
	if len(os.Args) < 2 || os.Args[1] != "start" {
		return false
	}

	// Speed up lease transfer decisions by not requiring quite as much data
	// before beginning to make them. Without this, the rapid splitting of ranges
	// in the few minutes after allocsim starts up causes it to take a long time
	// for leases to settle onto other nodes even when requests are skewed heavily
	// onto them.
	kvserver.MinLeaseTransferStatsDuration = 10 * time.Second

	cli.Main()
	return true
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
			log.Fatalf(context.Background(), "%v", err)
		}
	}

	perNodeCfg := localcluster.MakePerNodeFixedPortsCfg(*numNodes)

	// TODO(a-robinson): Automatically run github.com/tylertreat/comcast for
	// simpler configs that just have a single latency between all nodes.
	var separateAddrs bool
	for _, locality := range config.Localities {
		if len(locality.OutgoingLatencies) != 0 {
			separateAddrs = true
			if runtime.GOOS != "linux" {
				log.Fatal(context.Background(),
					"configs that set per-locality outgoing latencies are only supported on linux")
			}
			break
		}
	}

	if separateAddrs {
		for i := range perNodeCfg {
			s := perNodeCfg[i]
			s.Addr = fmt.Sprintf("127.0.0.%d", i)
			perNodeCfg[i] = s
		}
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	localities := make([]Locality, *numNodes)
	if len(config.Localities) != 0 {
		nodesPerLocality := make(map[string][]int)
		var nodeIdx int
		for _, locality := range config.Localities {
			for i := 0; i < locality.NumNodes; i++ {
				s := perNodeCfg[nodeIdx] // avoid map assignment problems
				if locality.LocalityStr != "" {
					s.ExtraArgs = []string{fmt.Sprintf("--locality=%s", locality.LocalityStr)}
				} else {
					s.ExtraArgs = []string{fmt.Sprintf("--locality=l=%s", locality.Name)}
				}
				if separateAddrs {
					s.ExtraEnv = []string{fmt.Sprintf("COCKROACH_SOURCE_IP_ADDRESS=%s", s.Addr)}
				}
				localities[nodeIdx] = locality
				nodesPerLocality[locality.Name] = append(nodesPerLocality[locality.Name], nodeIdx)

				perNodeCfg[nodeIdx] = s
				nodeIdx++
			}
		}
		var tcController *tc.Controller
		if separateAddrs {
			// Since localcluster only uses loopback IPs for the nodes, we only need to
			// set up tc rules on the loopback device.
			tcController = tc.NewController("lo")
			if err := tcController.Init(); err != nil {
				log.Fatalf(context.Background(), "%v", err)
			}
			defer func() {
				if err := tcController.CleanUp(); err != nil {
					log.Errorf(context.Background(), "%v", err)
				}
			}()
		}
		for _, locality := range localities {
			for _, outgoing := range locality.OutgoingLatencies {
				if outgoing.Latency > 0 {
					for _, srcNodeIdx := range nodesPerLocality[locality.Name] {
						for _, dstNodeIdx := range nodesPerLocality[outgoing.Name] {
							if err := tcController.AddLatency(
								perNodeCfg[srcNodeIdx].Addr, perNodeCfg[dstNodeIdx].Addr, time.Duration(outgoing.Latency/2),
							); err != nil {
								log.Fatalf(context.Background(), "%v", err)
							}
						}
					}
				}
			}
		}
	}

	cfg := localcluster.ClusterConfig{
		AllNodeArgs: append(flag.Args(), "--vmodule=allocator=3,allocator_scorer=3,replicate_queue=3"),
		Binary:      os.Args[0],
		NumNodes:    *numNodes,
		DB:          "allocsim",
		NumWorkers:  *workers,
		PerNodeCfg:  perNodeCfg,
		DataDir:     "cockroach-data-allocsim",
	}

	c := localcluster.New(cfg)
	a := newAllocSim(c)
	a.localities = localities

	log.SetExitFunc(false /* hideStack */, func(code exit.Code) {
		c.Close()
		exit.WithCode(code)
	})

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

	c.Start(context.Background())
	defer c.Close()
	c.UpdateZoneConfig(1, 1<<20)
	_, err := c.Nodes[0].DB().Exec("SET CLUSTER SETTING kv.raft_log.disable_synchronization_unsafe = true")
	if err != nil {
		log.Fatalf(context.Background(), "%v", err)
	}
	if len(config.Localities) != 0 {
		a.runWithConfig(config)
	} else {
		a.run(*workers)
	}
}
