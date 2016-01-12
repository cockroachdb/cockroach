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

// +build acceptance

package acceptance

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/samalba/dockerclient"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/acceptance/terrafarm"
	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/stop"
)

var duration = flag.Duration("d", 5*time.Second, "duration to run the test")
var numLocal = flag.Int("num-local", 0, "start a local cluster of the given size")
var numRemote = flag.Int("num-remote", 0, "start a remote cluster of the given size")
var numStores = flag.Int("num-stores", 1, "number of stores to use for each node")
var cwd = flag.String("cwd", "../cloud/aws", "directory to run terraform from")
var stall = flag.Duration("stall", 2*time.Minute, "duration after which if no forward progress is made, consider the test stalled")
var keyName = flag.String("key-name", "", "name of key for remote cluster")
var logDir = flag.String("l", "", "the directory to store log files, relative to the test source")
var stopper = make(chan struct{})

func farmer(t *testing.T) *terrafarm.Farmer {
	if *numRemote <= 0 {
		t.Skip("running in docker mode")
	}
	if *keyName == "" {
		t.Fatal("-key-name is required") // saves a lot of trouble
	}
	logDir := *logDir
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
	for j := 1; j < *numStores; j++ {
		stores += ",ssd=data" + strconv.Itoa(j)
	}
	f := &terrafarm.Farmer{
		Output:  os.Stderr,
		Cwd:     *cwd,
		LogDir:  logDir,
		KeyName: *keyName,
		Stores:  stores,
	}
	log.Infof("logging to %s", logDir)
	return f
}

// StartCluster starts a cluster from the relevant flags. All test clusters
// should be created through this command since it sets up the logging in a
// unified way.
func StartCluster(t *testing.T) cluster.Cluster {
	if *numLocal > 0 {
		if *numRemote > 0 {
			t.Fatal("cannot both specify -num-local and -num-remote")
		}
		logDir := *logDir
		if logDir != "" {
			if _, _, fun := caller.Lookup(1); fun != "" {
				logDir = filepath.Join(logDir, fun)
			}
		}
		l := cluster.CreateLocal(*numLocal, *numStores, logDir, stopper)
		l.Start()
		checkRangeReplication(t, l, 20*time.Second)
		return l
	}
	if *numRemote == 0 {
		t.Fatal("need to either specify -num-local or -num-remote")
	}
	f := farmer(t)
	if err := f.Resize(*numRemote, 0); err != nil {
		t.Fatal(err)
	}
	if err := f.WaitReady(5 * time.Minute); err != nil {
		_ = f.Destroy()
		t.Fatalf("cluster not ready in time: %v", err)
	}
	checkRangeReplication(t, f, 20*time.Second)
	return f
}

func TestMain(m *testing.M) {
	randutil.SeedForTests()
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig
		select {
		case <-stopper:
		default:
			// There is a very tiny race here: the cluster might be closing
			// the stopper simultaneously.
			close(stopper)
		}
	}()
	os.Exit(m.Run())
}

// SkipUnlessLocal calls t.Skip if not running against a local cluster.
func SkipUnlessLocal(t *testing.T) {
	if *numLocal == 0 {
		t.Skip("skipping since not run against local cluster")
	}
}

func makeClient(t util.Tester, str string) (*client.DB, *stop.Stopper) {
	stopper := stop.NewStopper()

	db, err := client.Open(stopper, str)

	if err != nil {
		t.Fatal(err)
	}

	return db, stopper
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

// testDockerFail ensures the specified docker cmd fails.
func testDockerFail(t *testing.T, tag string, cmd []string) {
	wr, _ := testDocker(t, tag, cmd)
	if wr.Error == nil && wr.ExitCode == 0 {
		t.Errorf("expected failure")
	}
}

// testDockerSuccess ensures the specified docker cmd succeeds.
func testDockerSuccess(t *testing.T, tag string, cmd []string) {
	wr, logs := testDocker(t, tag, cmd)
	if wr.Error != nil {
		t.Log(logs)
		t.Errorf("unexpected error: %s", wr.Error)
	} else if wr.ExitCode != 0 {
		t.Log(logs)
		t.Errorf("unexpected exit code: %v", wr.ExitCode)
	}
}

func testDocker(t *testing.T, tag string, cmd []string) (result dockerclient.WaitResult, logs string) {
	SkipUnlessLocal(t)
	l := StartCluster(t).(*cluster.LocalCluster)

	defer l.AssertAndStop(t)

	addr := l.Nodes[0].PGAddr()

	client := cluster.NewDockerClient()
	containerConfig := dockerclient.ContainerConfig{
		Image: "cockroachdb/postgres-test:" + tag,
		Env: []string{
			fmt.Sprintf("PGHOST=%s", addr.IP),
			fmt.Sprintf("PGPORT=%d", addr.Port),
			"PGSSLCERT=/certs/node.client.crt",
			"PGSSLKEY=/certs/node.client.key",
		},
		Cmd: cmd,
	}
	if err := client.PullImage(containerConfig.Image, nil); err != nil {
		t.Fatal(err)
	}
	id, err := client.CreateContainer(&containerConfig, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.RemoveContainer(id, false, false)

	hostConfig := dockerclient.HostConfig{
		Binds:       []string{fmt.Sprintf("%s:%s", l.CertsDir, "/certs")},
		NetworkMode: "host",
	}
	if err := client.StartContainer(id, &hostConfig); err != nil {
		t.Fatal(err)
	}
	wr := <-client.Wait(id)
	if wr.ExitCode != 0 {
		rc, err := client.ContainerLogs(id, &dockerclient.LogOptions{
			Stdout: true,
			Stderr: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		b, err := ioutil.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Error(err)
		}
		logs = string(b)
	}
	return wr, logs
}
