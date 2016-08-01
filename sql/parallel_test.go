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
// Author: Radu Berinde (radu@cockroachlabs.com)
//
// The parallel_test adds an orchestration layer on top of the logic_test code
// with the capability of running multiple test data files in parallel.
//
// Each test lives in a separate subdir under testdata/paralleltest. Each test
// dir contains a "test.yaml" file along with a set of files in logic test
// format. The test.yaml file corresponds to the parTestSpec structure below.

package sql_test

import (
	gosql "database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"gopkg.in/yaml.v1"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/protoutil"
	"github.com/cockroachdb/cockroach/util/stop"
)

var (
	paralleltestdata = flag.String("partestdata", "partestdata/[^.]*", "test data glob")
)

type parallelTest struct {
	*testing.T
	ctx     context.Context
	cluster serverutils.TestClusterInterface
	clients [][]*gosql.DB
}

func (t *parallelTest) close() {
	t.clients = nil
	if t.cluster != nil {
		t.cluster.Stopper().Stop()
	}
}

func (t *parallelTest) processTestFile(path string, nodeIdx int, db *gosql.DB, ch chan bool) {
	if ch != nil {
		defer func() { ch <- true }()
	}

	// Set up a dummy logicTest structure to use that code.
	l := &logicTest{
		T:       t.T,
		srv:     t.cluster.Server(nodeIdx),
		db:      db,
		user:    security.RootUser,
		verbose: testing.Verbose() || log.V(1),
	}
	if err := l.processTestFile(path); err != nil {
		t.Error(err)
	}
}

func (t *parallelTest) getClient(nodeIdx, clientIdx int) *gosql.DB {
	for len(t.clients[nodeIdx]) <= clientIdx {
		// Add a client.
		pgURL, cleanupFunc := sqlutils.PGUrl(t.T,
			t.cluster.Server(nodeIdx).ServingAddr(),
			security.RootUser,
			"TestParallel")
		db, err := gosql.Open("postgres", pgURL.String())
		if err != nil {
			t.Fatal(err)
		}
		sqlutils.MakeSQLRunner(t, db).Exec("SET DATABASE = test")
		t.cluster.Stopper().AddCloser(
			stop.CloserFn(func() {
				_ = db.Close()
				cleanupFunc()
			}))
		t.clients[nodeIdx] = append(t.clients[nodeIdx], db)
	}
	return t.clients[nodeIdx][clientIdx]
}

type parTestRunEntry struct {
	Node int    `yaml:"node"`
	File string `yaml:"file"`
}

type parTestSpec struct {
	SkipReason string `yaml:"skip_reason"`

	// ClusterSize is the number of nodes in the cluster. If 0, single node.
	ClusterSize int `yaml:"cluster_size"`

	RangeSplitSize int `yaml:"range_split_size"`

	// Run contains a set of "run lists". The files in a runlist are run in
	// parallel and they complete before the next run list start.
	Run [][]parTestRunEntry `yaml:"run"`
}

func (t *parallelTest) run(dir string) {
	// Process the spec file.
	mainFile := filepath.Join(dir, "test.yaml")
	yamlData, err := ioutil.ReadFile(mainFile)
	if err != nil {
		t.Fatalf("%s: %s", mainFile, err)
	}
	var spec parTestSpec
	err = yaml.Unmarshal(yamlData, &spec)
	if err != nil {
		t.Fatalf("%s: %s", mainFile, err)
	}

	if spec.SkipReason != "" {
		log.Warningf(t.ctx, "Skipping test %s: %s", dir, spec.SkipReason)
		return
	}

	log.Infof(t.ctx, "Running test %s", dir)
	if testing.Verbose() || log.V(1) {
		log.Infof(t.ctx, "spec: %+v", spec)
	}

	t.setup(&spec)
	defer t.close()

	for runListIdx, runList := range spec.Run {
		if testing.Verbose() || log.V(1) {
			var descr []string
			for _, re := range runList {
				descr = append(descr, fmt.Sprintf("%d:%s", re.Node, re.File))
			}
			log.Infof(t.ctx, "%s: run list %d: %s", mainFile, runListIdx,
				strings.Join(descr, ", "))
		}
		// Store the number of clients used so far (per node).
		numClients := make([]int, spec.ClusterSize)
		ch := make(chan bool)
		for _, re := range runList {
			client := t.getClient(re.Node, numClients[re.Node])
			numClients[re.Node]++
			go t.processTestFile(filepath.Join(dir, re.File), re.Node, client, ch)
		}
		// Wait for all clients to complete.
		for range runList {
			<-ch
		}
	}
}

func (t *parallelTest) setup(spec *parTestSpec) {
	if spec.ClusterSize == 0 {
		spec.ClusterSize = 1
	}

	if testing.Verbose() || log.V(1) {
		log.Infof(t.ctx, "Cluster Size: %d", spec.ClusterSize)
	}

	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			MaxOffset: logicMaxOffset,
			Knobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					WaitForGossipUpdate:   true,
					CheckStmtStringChange: true,
				},
			},
		},
	}
	t.cluster = serverutils.StartTestCluster(t, spec.ClusterSize, args)
	t.clients = make([][]*gosql.DB, spec.ClusterSize)
	for i := range t.clients {
		t.clients[i] = append(t.clients[i], t.cluster.ServerConn(i))
	}
	r0 := sqlutils.MakeSQLRunner(t, t.clients[0][0])

	if spec.RangeSplitSize != 0 {
		if testing.Verbose() || log.V(1) {
			log.Infof(t.ctx, "Setting range split size: %d", spec.RangeSplitSize)
		}
		zoneCfg := config.DefaultZoneConfig()
		zoneCfg.RangeMaxBytes = int64(spec.RangeSplitSize)
		zoneCfg.RangeMinBytes = zoneCfg.RangeMaxBytes / 2
		buf, err := protoutil.Marshal(&zoneCfg)
		if err != nil {
			t.Fatal(err)
		}
		objID := keys.RootNamespaceID
		r0.Exec(`UPDATE system.zones SET config = $2 WHERE id = $1`, objID, buf)
	}

	if testing.Verbose() || log.V(1) {
		log.Infof(t.ctx, "Creating database")
	}

	r0.Exec("CREATE DATABASE test")
	for i := range t.clients {
		sqlutils.MakeSQLRunner(t, t.clients[i][0]).Exec("SET DATABASE = test")
	}

	if spec.ClusterSize >= 3 {
		if testing.Verbose() || log.V(1) {
			log.Infof(t.ctx, "Waiting for full replication")
		}
		if err := t.cluster.WaitForFullReplication(); err != nil {
			t.Fatal(err)
		}
	}
	if testing.Verbose() || log.V(1) {
		log.Infof(t.ctx, "Test setup done")
	}
}

func TestParallel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	glob := string(*paralleltestdata)
	paths, err := filepath.Glob(glob)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("No testfiles found (glob: %s)", glob)
	}
	total := 0
	for _, p := range paths {
		pt := parallelTest{T: t, ctx: context.Background()}
		pt.run(p)
		total++
	}
	log.Infof(context.Background(), "%d parallel tests passed", total)
}
