// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// The parallel_test adds an orchestration layer on top of the logic_test code
// with the capability of running multiple test data files in parallel.
//
// Each test lives in a separate subdir under testdata/paralleltest. Each test
// dir contains a "test.yaml" file along with a set of files in logic test
// format. The test.yaml file corresponds to the parTestSpec structure below.

package logictest

import (
	"context"
	gosql "database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/gogo/protobuf/proto"
	yaml "gopkg.in/yaml.v2"
)

var (
	paralleltestdata = flag.String("partestdata", "testdata/parallel_test/[^.]*", "test data glob")
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
		t.cluster.Stopper().Stop(context.Background())
	}
}

func (t *parallelTest) processTestFile(path string, nodeIdx int, db *gosql.DB, ch chan bool) {
	if ch != nil {
		defer func() { ch <- true }()
	}

	// Set up a dummy logicTest structure to use that code.
	rng, _ := randutil.NewPseudoRand()
	l := &logicTest{
		rootT:   t.T,
		cluster: t.cluster,
		nodeIdx: nodeIdx,
		db:      db,
		user:    security.RootUser,
		verbose: testing.Verbose() || log.V(1),
		rng:     rng,
	}
	if err := l.processTestFile(path, testClusterConfig{}); err != nil {
		log.Errorf(context.Background(), "error processing %s: %s", path, err)
		t.Error(err)
	}
}

func (t *parallelTest) getClient(nodeIdx, clientIdx int) *gosql.DB {
	for len(t.clients[nodeIdx]) <= clientIdx {
		// Add a client.
		pgURL, cleanupFunc := sqlutils.PGUrl(t.T,
			t.cluster.Server(nodeIdx).ServingSQLAddr(),
			"TestParallel",
			url.User(security.RootUser))
		db, err := gosql.Open("postgres", pgURL.String())
		if err != nil {
			t.Fatal(err)
		}
		sqlutils.MakeSQLRunner(db).Exec(t, "SET DATABASE = test")
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
	if err := yaml.UnmarshalStrict(yamlData, &spec); err != nil {
		t.Fatalf("%s: %s", mainFile, err)
	}

	if spec.SkipReason != "" {
		skip.IgnoreLint(t, spec.SkipReason)
	}

	log.Infof(t.ctx, "Running test %s", dir)
	if testing.Verbose() || log.V(1) {
		log.Infof(t.ctx, "spec: %+v", spec)
	}

	t.setup(context.Background(), &spec)
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

func (t *parallelTest) setup(ctx context.Context, spec *parTestSpec) {
	if spec.ClusterSize == 0 {
		spec.ClusterSize = 1
	}

	if testing.Verbose() || log.V(1) {
		log.Infof(t.ctx, "Cluster Size: %d", spec.ClusterSize)
	}

	t.cluster = serverutils.StartNewTestCluster(t, spec.ClusterSize, base.TestClusterArgs{})

	for i := 0; i < t.cluster.NumServers(); i++ {
		server := t.cluster.Server(i)
		mode := sessiondata.DistSQLOff
		st := server.ClusterSettings()
		st.Manual.Store(true)
		sql.DistSQLClusterExecMode.Override(ctx, &st.SV, int64(mode))
		// Disable automatic stats - they can interfere with the test shutdown.
		stats.AutomaticStatisticsClusterMode.Override(ctx, &st.SV, false)
	}

	t.clients = make([][]*gosql.DB, spec.ClusterSize)
	for i := range t.clients {
		t.clients[i] = append(t.clients[i], t.cluster.ServerConn(i))
	}
	r0 := sqlutils.MakeSQLRunner(t.clients[0][0])

	if spec.RangeSplitSize != 0 {
		if testing.Verbose() || log.V(1) {
			log.Infof(t.ctx, "Setting range split size: %d", spec.RangeSplitSize)
		}
		zoneCfg := zonepb.DefaultZoneConfig()
		zoneCfg.RangeMaxBytes = proto.Int64(int64(spec.RangeSplitSize))
		zoneCfg.RangeMinBytes = proto.Int64(*zoneCfg.RangeMaxBytes / 2)
		buf, err := protoutil.Marshal(&zoneCfg)
		if err != nil {
			t.Fatal(err)
		}
		objID := keys.RootNamespaceID
		r0.Exec(t, `UPDATE system.zones SET config = $2 WHERE id = $1`, objID, buf)
	}

	if testing.Verbose() || log.V(1) {
		log.Infof(t.ctx, "Creating database")
	}

	r0.Exec(t, "CREATE DATABASE test")
	for i := range t.clients {
		sqlutils.MakeSQLRunner(t.clients[i][0]).Exec(t, "SET DATABASE = test")
	}

	if testing.Verbose() || log.V(1) {
		log.Infof(t.ctx, "Test setup done")
	}
}

func TestParallel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	skip.UnderRace(t, "takes >1 min under race")
	// Note: there is special code in teamcity-trigger/main.go to run this package
	// with less concurrency in the nightly stress runs. If you see problems
	// please make adjustments there.
	// As of 6/4/2019, the logic tests never complete under race.
	skip.UnderStressRace(t, "logic tests and race detector don't mix: #37993")

	glob := *paralleltestdata
	paths, err := filepath.Glob(glob)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("No testfiles found (glob: %s)", glob)
	}
	total := 0
	failed := 0
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			pt := parallelTest{T: t, ctx: context.Background()}
			pt.run(path)
			total++
			if t.Failed() {
				failed++
			}
		})
	}
	if failed == 0 {
		log.Infof(context.Background(), "%d parallel tests passed", total)
	} else {
		log.Infof(context.Background(), "%d out of %d parallel tests failed", failed, total)
	}
}
