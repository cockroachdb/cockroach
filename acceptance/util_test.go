// Copyright 2015 The Cockroach Authors.
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

package acceptance

import (
	"bytes"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/docker/engine-api/types"
	"github.com/docker/engine-api/types/container"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/acceptance/terrafarm"
	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/log"
	_ "github.com/cockroachdb/pq"
)

var flagDuration = flag.Duration("d", cluster.DefaultDuration, "duration to run the test")
var flagNodes = flag.Int("nodes", 3, "number of nodes")
var flagStores = flag.Int("stores", 1, "number of stores to use for each node")
var flagRemote = flag.Bool("remote", false, "run the test using terrafarm instead of docker")
var flagCwd = flag.String("cwd", "../cloud/aws", "directory to run terraform from")
var flagKeyName = flag.String("key-name", "", "name of key for remote cluster")
var flagLogDir = flag.String("l", "", "the directory to store log files, relative to the test source")
var flagTestConfigs = flag.Bool("test-configs", false, "instead of using the passed in configuration, use the default "+
	"cluster configurations for each test. This overrides the nodes, stores and duration flags and will run the test "+
	"against a collection of pre-specified cluster configurations.")
var flagConfig = flag.String("config", "", "a json TestConfig proto, see testconfig.proto")

var testFuncRE = regexp.MustCompile("^(Test|Benchmark)")

var stopper = make(chan struct{})

func farmer(t *testing.T) *terrafarm.Farmer {
	if !*flagRemote {
		t.Skip("running in docker mode")
	}
	if *flagKeyName == "" {
		t.Fatal("-key-name is required") // saves a lot of trouble
	}
	logDir := *flagLogDir
	if logDir == "" {
		var err error
		logDir, err = ioutil.TempDir("", "clustertest_")
		if err != nil {
			t.Fatal(err)
		}
	}
	if !filepath.IsAbs(logDir) {
		logDir = filepath.Join(filepath.Clean(os.ExpandEnv("${PWD}")), logDir)
	}
	stores := "ssd=data0"
	for j := 1; j < *flagStores; j++ {
		stores += ",ssd=data" + strconv.Itoa(j)
	}
	f := &terrafarm.Farmer{
		Output:  os.Stderr,
		Cwd:     *flagCwd,
		LogDir:  logDir,
		KeyName: *flagKeyName,
		Stores:  stores,
	}
	log.Infof("logging to %s", logDir)
	return f
}

// readConfigFromFlags will convert the flags to a TestConfig for the purposes
// of starting up a cluster.
func readConfigFromFlags() cluster.TestConfig {
	return cluster.TestConfig{
		Name:     fmt.Sprintf("AdHoc %dx%d", *flagNodes, *flagStores),
		Duration: *flagDuration,
		Nodes: []cluster.NodeConfig{
			{
				Count:  int32(*flagNodes),
				Stores: []cluster.StoreConfig{{Count: int32(*flagStores)}},
			},
		},
	}
}

// getConfigs returns a list of test configs based on the passed in flags.
func getConfigs(t *testing.T) []cluster.TestConfig {
	// If a config not supplied, just read the flags.
	if (flagConfig == nil || len(*flagConfig) == 0) &&
		(flagTestConfigs == nil || !*flagTestConfigs) {
		return []cluster.TestConfig{readConfigFromFlags()}
	}

	var configs []cluster.TestConfig
	if flagTestConfigs != nil && *flagTestConfigs {
		configs = append(configs, cluster.DefaultConfigs()...)
	}

	if flagConfig != nil && len(*flagConfig) > 0 {
		// Read the passed in config from the command line.
		var config cluster.TestConfig
		if err := json.Unmarshal([]byte(*flagConfig), &config); err != nil {
			t.Error(err)
		}
		configs = append(configs, config)
	}

	// Override duration in all configs if the flags are set.
	for i := 0; i < len(configs); i++ {
		// Override values.
		if flagDuration != nil && *flagDuration != cluster.DefaultDuration {
			configs[i].Duration = *flagDuration
		}
		// Set missing defaults.
		if configs[i].Duration == 0 {
			configs[i].Duration = cluster.DefaultDuration
		}
	}
	return configs
}

// runTestOnConfigs retrieves the full list of test configurations and runs the
// passed in test against each on serially.
func runTestOnConfigs(t *testing.T, testFunc func(*testing.T, cluster.Cluster, cluster.TestConfig)) {
	cfgs := getConfigs(t)
	if len(cfgs) == 0 {
		t.Fatal("no config defined so most tests won't run")
	}
	for _, cfg := range cfgs {
		func() {
			cluster := StartCluster(t, cfg)
			defer cluster.AssertAndStop(t)
			testFunc(t, cluster, cfg)
		}()
	}
}

// StartCluster starts a cluster from the relevant flags. All test clusters
// should be created through this command since it sets up the logging in a
// unified way.
func StartCluster(t *testing.T, cfg cluster.TestConfig) (c cluster.Cluster) {
	var completed bool
	defer func() {
		if !completed && c != nil {
			c.AssertAndStop(t)
		}
	}()
	if !*flagRemote {
		logDir := *flagLogDir
		if logDir != "" {
			_, _, fun := caller.Lookup(3)
			if !testFuncRE.MatchString(fun) {
				t.Fatalf("invalid caller %s; want TestX -> runTestOnConfigs -> func()", fun)
			}
			logDir = filepath.Join(logDir, fun)
		}
		l := cluster.CreateLocal(cfg, logDir, stopper)
		l.Start()
		c = l
		checkRangeReplication(t, l, 20*time.Second)
		completed = true
		return l
	}
	f := farmer(t)
	c = f
	if err := f.Resize(*flagNodes, 0); err != nil {
		t.Fatal(err)
	}
	if err := f.WaitReady(5 * time.Minute); err != nil {
		_ = f.Destroy()
		t.Fatalf("cluster not ready in time: %v", err)
	}
	checkRangeReplication(t, f, 20*time.Second)
	completed = true
	return f
}

// SkipUnlessLocal calls t.Skip if not running against a local cluster.
func SkipUnlessLocal(t *testing.T) {
	if *flagRemote {
		t.Skip("skipping since not run against local cluster")
	}
}

func makePGClient(t *testing.T, dest string) *sql.DB {
	db, err := sql.Open("postgres", dest)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// HTTPClient is an http.Client configured for querying a cluster. We need to
// run with "InsecureSkipVerify" (at least on Docker) due to the fact that we
// cannot use a fixed hostname to reach the cluster. This in turn means that we
// do not have a verified server name in the certs.
var HTTPClient = http.Client{
	Timeout: base.NetworkTimeout,
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}}

// getJSON retrieves the URL specified by the parameters and
// and unmarshals the result into the supplied interface.
func getJSON(url, rel string, v interface{}) error {
	resp, err := HTTPClient.Get(url + rel)
	if err != nil {
		if log.V(1) {
			log.Info(err)
		}
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		if log.V(1) {
			log.Info(err)
		}
		return err
	}
	return json.Unmarshal(b, v)
}

// postJSON POSTs to the URL specified by the parameters and unmarshals the
// result into the supplied interface.
func postJSON(url, rel string, reqBody interface{}, v interface{}) error {
	reqBodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	resp, err := HTTPClient.Post(url+rel, util.JSONContentType, bytes.NewReader(reqBodyBytes))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// testDockerFail ensures the specified docker cmd fails.
func testDockerFail(t *testing.T, name string, cmd []string) {
	if err := testDockerSingleNode(t, name, cmd); err == nil {
		t.Error("expected failure")
	}
}

// testDockerSuccess ensures the specified docker cmd succeeds.
func testDockerSuccess(t *testing.T, name string, cmd []string) {
	if err := testDockerSingleNode(t, name, cmd); err != nil {
		t.Errorf("expected success: %s", err)
	}
}

const (
	postgresTestImage = "cockroachdb/postgres-test"
	postgresTestTag   = "20160414-1710"
)

func testDockerSingleNode(t *testing.T, name string, cmd []string) error {
	SkipUnlessLocal(t)
	cfg := cluster.TestConfig{
		Name:     name,
		Duration: *flagDuration,
		Nodes:    []cluster.NodeConfig{{Count: 1, Stores: []cluster.StoreConfig{{Count: 1}}}},
	}
	l := StartCluster(t, cfg).(*cluster.LocalCluster)
	defer l.AssertAndStop(t)

	containerConfig := container.Config{
		Image: fmt.Sprintf(postgresTestImage + ":" + postgresTestTag),
		Env: []string{
			"PGHOST=roach0",
			fmt.Sprintf("PGPORT=%s", base.DefaultPort),
			"PGSSLCERT=/certs/node.crt",
			"PGSSLKEY=/certs/node.key",
		},
		Cmd: cmd,
	}
	hostConfig := container.HostConfig{NetworkMode: "host"}
	ipo := types.ImagePullOptions{ImageID: postgresTestImage, Tag: postgresTestTag}
	return l.OneShot(ipo, containerConfig, hostConfig, "docker-"+name)
}
