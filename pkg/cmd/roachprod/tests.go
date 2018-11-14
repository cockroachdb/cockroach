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
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/ssh"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var duration time.Duration
var concurrency string

var tests = map[string]func(clusterName, dir string){
	"kv_0":    kv0,
	"kv_95":   kv95,
	"ycsb_a":  ycsbA,
	"ycsb_b":  ycsbB,
	"ycsb_c":  ycsbC,
	"nightly": nightly,
	"splits":  splits,
}

var dirRE = regexp.MustCompile(`([^.]+)\.`)

type testMetadata struct {
	Bin     string
	Cluster string
	Nodes   []int
	Env     string
	Args    []string
	Test    string
	Date    string
}

type testRun struct {
	Concurrency int
	Elapsed     float64
	Errors      int64
	Ops         int64
	OpsSec      float64
	AvgLat      float64
	P50Lat      float64
	P95Lat      float64
	P99Lat      float64
}

func loadTestRun(dir, name string) (*testRun, error) {
	n, err := strconv.Atoi(name)
	if err != nil {
		return nil, nil
	}
	r := &testRun{Concurrency: n}

	b, err := ioutil.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return nil, err
	}

	const header = `_elapsed___errors_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)`
	i := bytes.Index(b, []byte(header))
	if i == -1 {
		return nil, nil
	}
	b = b[i+len(header)+1:]

	_, err = fmt.Fscanf(bytes.NewReader(b), " %fs %d %d %f %f %f %f %f",
		&r.Elapsed, &r.Errors, &r.Ops, &r.OpsSec, &r.AvgLat, &r.P50Lat, &r.P95Lat, &r.P99Lat)
	if err != nil {
		return nil, err
	}
	return r, nil
}

type testData struct {
	Metadata testMetadata
	Runs     []*testRun
}

func loadTestData(dir string) (*testData, error) {
	d := &testData{}
	if err := loadJSON(filepath.Join(dir, "metadata"), &d.Metadata); err != nil {
		return nil, err
	}

	ents, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, e := range ents {
		r, err := loadTestRun(dir, e.Name())
		if err != nil {
			return nil, err
		}
		if r != nil {
			d.Runs = append(d.Runs, r)
		}
	}

	sort.Slice(d.Runs, func(i, j int) bool {
		return d.Runs[i].Concurrency < d.Runs[j].Concurrency
	})
	return d, nil
}

func (d *testData) exists(concurrency int) bool {
	i := sort.Search(len(d.Runs), func(j int) bool {
		return d.Runs[j].Concurrency >= concurrency
	})
	return i < len(d.Runs) && d.Runs[i].Concurrency == concurrency
}

func (d *testData) get(concurrency int) *testRun {
	i := sort.Search(len(d.Runs), func(j int) bool {
		return d.Runs[j].Concurrency >= concurrency
	})
	if i+1 >= len(d.Runs) {
		return d.Runs[len(d.Runs)-1]
	}
	if i < 0 {
		return d.Runs[0]
	}
	a := d.Runs[i]
	b := d.Runs[i+1]
	t := float64(concurrency-a.Concurrency) / float64(b.Concurrency-a.Concurrency)
	return &testRun{
		Concurrency: concurrency,
		Elapsed:     a.Elapsed + (b.Elapsed-a.Elapsed)*t,
		Ops:         a.Ops + int64(float64(b.Ops-a.Ops)*t),
		OpsSec:      a.OpsSec + (b.OpsSec-a.OpsSec)*t,
		AvgLat:      a.AvgLat + (b.AvgLat-a.AvgLat)*t,
		P50Lat:      a.P50Lat + (b.P50Lat-a.P50Lat)*t,
		P95Lat:      a.P95Lat + (b.P95Lat-a.P95Lat)*t,
		P99Lat:      a.P99Lat + (b.P99Lat-a.P99Lat)*t,
	}
}

func alignTestData(d1, d2 *testData) (*testData, *testData) {
	if len(d1.Runs) == 0 || len(d2.Runs) == 0 {
		return &testData{Metadata: d1.Metadata}, &testData{Metadata: d2.Metadata}
	}

	minConcurrency := d1.Runs[0].Concurrency
	if c := d2.Runs[0].Concurrency; minConcurrency < c {
		minConcurrency = c
	}
	maxConcurrency := d1.Runs[len(d1.Runs)-1].Concurrency
	if c := d2.Runs[len(d2.Runs)-1].Concurrency; maxConcurrency > c {
		maxConcurrency = c
	}

	var r1 []*testRun
	var r2 []*testRun
	for i := minConcurrency; i <= maxConcurrency; i++ {
		if !d1.exists(i) || !d2.exists(i) {
			continue
		}
		r1 = append(r1, d1.get(i))
		r2 = append(r2, d2.get(i))
	}

	d1 = &testData{
		Metadata: d1.Metadata,
		Runs:     r1,
	}
	d2 = &testData{
		Metadata: d2.Metadata,
		Runs:     r2,
	}
	return d1, d2
}

func findTest(name string) (_ func(clusterName, dir string), dir string) {
	fn := tests[name]
	if fn != nil {
		return fn, ""
	}
	m := dirRE.FindStringSubmatch(filepath.Base(name))
	if len(m) != 2 {
		return nil, ""
	}
	return tests[m[1]], name
}

func runTest(name, clusterName string) error {
	fn, dir := findTest(name)
	if fn == nil {
		return fmt.Errorf("unknown test: %s", name)
	}
	fn(clusterName, dir)
	return nil
}

func allTests() []string {
	var r []string
	for k := range tests {
		r = append(r, k)
	}
	sort.Strings(r)
	return r
}

func testCluster(name string) *install.SyncedCluster {
	c, err := newCluster(name, true /* reserveLoadGen */)
	if err != nil {
		log.Fatal(err)
	}
	if c.LoadGen == 0 {
		log.Fatalf("%s: no load generator node specified", c.Name)
	}
	return c
}

func clusterVersion(c *install.SyncedCluster) string {
	switch clusterType {
	case "cockroach":
		versions := c.CockroachVersions()
		if len(versions) == 0 {
			// TODO(peter): If we're running on existing test, rather than dying let
			// the test upload the correct cockroach binary.
			log.Fatalf("unable to determine cockroach version")
		} else if len(versions) > 1 {
			// TODO(peter): Rather than dying, allow the test to upload the version to
			// run on each node.
			log.Fatalf("mismatched cockroach versions: %v", versions)
		}
		for v := range versions {
			return "cockroach-" + v
		}

	case "cassandra":
		return "cassandra"

	default:
		log.Fatalf("unsupported cluster type: %s", clusterType)
	}

	panic("not reached")
}

func testDir(name, vers string) string {
	dir := fmt.Sprintf("%s.%s", name, vers)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatal(err)
	}
	return dir
}

func parseConcurrency(s string, numNodes int) (lo int, hi int, step int) {
	// <lo>[-<hi>[/<step>]]
	//
	// If <step> is not specified, assuming numNodes. The <lo> and <hi> values
	// are always multiplied by the number of nodes.

	parts := strings.Split(s, "/")
	switch len(parts) {
	case 1:
		step = numNodes
	case 2:
		var err error
		step, err = strconv.Atoi(parts[1])
		if err != nil {
			log.Fatal(err)
		}
		s = parts[0]
	}

	parts = strings.Split(s, "-")
	switch len(parts) {
	case 1:
		lo, err := strconv.Atoi(parts[0])
		if err != nil {
			log.Fatal(err)
		}
		return lo * numNodes, lo * numNodes, step
	case 2:
		lo, err := strconv.Atoi(parts[0])
		if err != nil {
			log.Fatal(err)
		}
		hi, err := strconv.Atoi(parts[1])
		if err != nil {
			log.Fatal(err)
		}
		return lo * numNodes, hi * numNodes, step
	default:
		log.Fatalf("unable to parse concurrency setting: %s", s)
	}
	return 0, 0, 0
}

func getBin(c *install.SyncedCluster, dir string) {
	if clusterType == "cockroach" {
		bin := filepath.Join(dir, "cockroach")
		if _, err := os.Stat(bin); err == nil {
			return
		}
		t := *c
		t.Nodes = t.Nodes[:1]
		t.Get("./cockroach", bin)
	}
}

func putBin(c *install.SyncedCluster, dir string) error {
	if clusterType == "cockroach" {
		bin := filepath.Join(dir, "cockroach")
		if _, err := os.Stat(bin); err != nil {
			return err
		}
		c.Put(bin, "./cockroach")
	}
	return nil
}

func kvTest(clusterName, testName, dir, cmd string) {
	var existing *testMetadata
	if dir != "" {
		existing = &testMetadata{}
		if err := loadJSON(filepath.Join(dir, "metadata"), existing); err != nil {
			log.Fatal(err)
		}
		clusterName = existing.Cluster
		nodeArgs = existing.Args
	}

	c := testCluster(clusterName)
	m := testMetadata{
		Bin:     clusterVersion(c),
		Cluster: c.Name,
		Nodes:   c.Nodes,
		Env:     c.Env,
		Args:    c.Args,
		Test:    fmt.Sprintf("%s --duration=%s --concurrency=%%d", cmd, duration),
		Date:    timeutil.Now().Format("2006-01-02T15_04_05"),
	}
	if existing == nil {
		dir = testDir(testName, m.Bin)
		saveJSON(filepath.Join(dir, "metadata"), m)
	} else {
		if m.Bin != existing.Bin {
			if err := putBin(c, dir); err != nil {
				log.Fatalf("binary changed: %s != %s\n%s", m.Bin, existing.Bin, err)
			}
		}
		m.Nodes = existing.Nodes
		m.Env = existing.Env
	}
	fmt.Printf("%s: %s\n", c.Name, dir)
	getBin(c, dir)

	lo, hi, step := parseConcurrency(concurrency, len(c.ServerNodes()))
	for concurrency := lo; concurrency <= hi; concurrency += step {
		runName := fmt.Sprint(concurrency)
		if run, err := loadTestRun(dir, runName); err == nil && run != nil {
			continue
		}

		err := func() error {
			f, err := os.Create(filepath.Join(dir, runName))
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()
			c.Wipe()
			c.Start()
			cmd := fmt.Sprintf(m.Test, concurrency)
			stdout := io.MultiWriter(f, os.Stdout)
			stderr := io.MultiWriter(f, os.Stderr)
			return c.RunLoad(cmd, stdout, stderr)
		}()
		if err != nil {
			if !ssh.IsSigKill(err) {
				fmt.Printf("%s\n", err)
			}
			break
		}
	}
	c.Stop(9, true /* wait */)
}

func kv0(clusterName, dir string) {
	kvTest(clusterName, "kv_0", dir, "./kv --read-percent=0 --splits=1000")
}

func kv95(clusterName, dir string) {
	kvTest(clusterName, "kv_95", dir, "./kv --read-percent=95 --splits=1000")
}

func ycsbA(clusterName, dir string) {
	kvTest(clusterName, "ycsb_a", dir, "./ycsb --workload=A --splits=1000 --cassandra-replication=3")
}

func ycsbB(clusterName, dir string) {
	kvTest(clusterName, "ycsb_b", dir, "./ycsb --workload=B --splits=1000 --cassandra-replication=3")
}

func ycsbC(clusterName, dir string) {
	kvTest(clusterName, "ycsb_c", dir, "./ycsb --workload=C --splits=1000 --cassandra-replication=3")
}

func nightly(clusterName, dir string) {
	var existing *testMetadata
	if dir != "" {
		existing = &testMetadata{}
		if err := loadJSON(filepath.Join(dir, "metadata"), existing); err != nil {
			log.Fatal(err)
		}
		clusterName = existing.Cluster
		nodeArgs = existing.Args
	}

	cmds := []struct {
		name string
		cmd  string
	}{
		{"kv_0", "./kv --read-percent=0 --splits=1000 --concurrency=384 --duration=10m"},
		{"kv_95", "./kv --read-percent=95 --splits=1000 --concurrency=384 --duration=10m"},
		// TODO(tamird/petermattis): this configuration has been observed to hang
		// indefinitely. Re-enable when it is more reliable.
		//
		// {"splits", "./kv --read-percent=0 --splits=100000 --concurrency=384 --max-ops=1"},
	}

	c := testCluster(clusterName)
	m := testMetadata{
		Bin:     clusterVersion(c),
		Cluster: c.Name,
		Nodes:   c.Nodes,
		Env:     c.Env,
		Args:    c.Args,
		Test:    "nightly",
		Date:    timeutil.Now().Format("2006-01-02T15_04_05"),
	}
	if existing == nil {
		dir = testDir("nightly", m.Bin)
		saveJSON(filepath.Join(dir, "metadata"), m)
	} else {
		if m.Bin != existing.Bin {
			if err := putBin(c, dir); err != nil {
				log.Fatalf("cockroach binary changed: %s != %s\n%s", m.Bin, existing.Bin, err)
			}
		}
		m.Nodes = existing.Nodes
		m.Env = existing.Env
	}
	fmt.Printf("%s: %s\n", c.Name, dir)
	getBin(c, dir)

	for _, cmd := range cmds {
		runName := fmt.Sprint(cmd.name)
		if run, err := loadTestRun(dir, runName); err == nil && run != nil {
			continue
		}

		err := func() error {
			f, err := os.Create(filepath.Join(dir, runName))
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()
			c.Wipe()
			c.Start()
			stdout := io.MultiWriter(f, os.Stdout)
			stderr := io.MultiWriter(f, os.Stderr)
			return c.RunLoad(cmd.cmd, stdout, stderr)
		}()
		if err != nil {
			if !ssh.IsSigKill(err) {
				fmt.Printf("%s\n", err)
			}
			break
		}
	}
	c.Stop(9, true /* wait */)
}

func splits(clusterName, dir string) {
	var existing *testMetadata
	if dir != "" {
		existing = &testMetadata{}
		if err := loadJSON(filepath.Join(dir, "metadata"), existing); err != nil {
			log.Fatal(err)
		}
		clusterName = existing.Cluster
		nodeArgs = existing.Args
	}

	const cmd = "./kv --splits=500000 --concurrency=384 --max-ops=1"
	c := testCluster(clusterName)
	m := testMetadata{
		Bin:     clusterVersion(c),
		Cluster: c.Name,
		Nodes:   c.Nodes,
		Env:     c.Env,
		Args:    c.Args,
		Test:    "splits",
		Date:    timeutil.Now().Format("2006-01-02T15_04_05"),
	}
	if existing == nil {
		dir = testDir("splits", m.Bin)
		saveJSON(filepath.Join(dir, "metadata"), m)
	} else {
		if m.Bin != existing.Bin {
			if err := putBin(c, dir); err != nil {
				log.Fatalf("cockroach binary changed: %s != %s\n%s", m.Bin, existing.Bin, err)
			}
		}
		m.Nodes = existing.Nodes
		m.Env = existing.Env
	}
	fmt.Printf("%s: %s\n", c.Name, dir)
	getBin(c, dir)

	for i := 1; i <= 100; i++ {
		runName := fmt.Sprint(i)
		if run, err := loadTestRun(dir, runName); err == nil && run != nil {
			continue
		}

		err := func() error {
			f, err := os.Create(filepath.Join(dir, runName))
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()
			c.Wipe()
			c.Start()
			stdout := io.MultiWriter(f, os.Stdout)
			stderr := io.MultiWriter(f, os.Stderr)
			if err := c.RunLoad(cmd, stdout, stderr); err != nil {
				return err
			}
			c.Stop(9, true /* wait */)
			return nil
		}()
		if err != nil {
			if !ssh.IsSigKill(err) {
				fmt.Printf("%s\n", err)
			}
			break
		}
	}
	c.Stop(9, true /* wait */)
}
