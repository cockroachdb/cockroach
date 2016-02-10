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
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/docker/docker/pkg/jsonmessage"
	dockerclient "github.com/docker/engine-api/client"
	"github.com/docker/engine-api/types"
	"github.com/docker/engine-api/types/container"
	"github.com/docker/engine-api/types/strslice"
	_ "github.com/lib/pq"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/acceptance/terrafarm"
	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/stop"
)

var flagDuration = flag.Duration("d", 5*time.Second, "duration to run the test")
var flagNodes = flag.Int("nodes", 3, "number of nodes")
var flagStores = flag.Int("stores", 1, "number of stores to use for each node")
var flagRemote = flag.Bool("remote", false, "run the test using terrafarm instead of docker")
var flagCwd = flag.String("cwd", "../cloud/aws", "directory to run terraform from")
var flagStall = flag.Duration("stall", 2*time.Minute, "duration after which if no forward progress is made, consider the test stalled")
var flagKeyName = flag.String("key-name", "", "name of key for remote cluster")
var flagLogDir = flag.String("l", "", "the directory to store log files, relative to the test source")
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

// StartCluster starts a cluster from the relevant flags. All test clusters
// should be created through this command since it sets up the logging in a
// unified way.
func StartCluster(t *testing.T) cluster.Cluster {
	if !*flagRemote {
		logDir := *flagLogDir
		if logDir != "" {
			if _, _, fun := caller.Lookup(1); fun != "" {
				logDir = filepath.Join(logDir, fun)
			}
		}
		l := cluster.CreateLocal(*flagNodes, *flagStores, logDir, stopper)
		l.Start()
		checkRangeReplication(t, l, 20*time.Second)
		return l
	}
	f := farmer(t)
	if err := f.Resize(*flagNodes, 0); err != nil {
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
	if *flagRemote {
		t.Skip("skipping since not run against local cluster")
	}
}

func makeClient(t *testing.T, str string) (*client.DB, *stop.Stopper) {
	stopper := stop.NewStopper()
	db, err := client.Open(stopper, str)
	if err != nil {
		t.Fatal(err)
	}
	return db, stopper
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

// testDockerFail ensures the specified docker cmd fails.
func testDockerFail(t *testing.T, tag string, cmd []string) {
	if exitCode, logs := testDocker(t, tag, cmd); exitCode == 0 {
		t.Log(logs)
		t.Errorf("unexpected failure:\n%s", logs)
	}
}

// testDockerSuccess ensures the specified docker cmd succeeds.
func testDockerSuccess(t *testing.T, tag string, cmd []string) {
	if exitCode, logs := testDocker(t, tag, cmd); exitCode != 0 {
		t.Errorf("unexpected success:\n%s", logs)
	}
}

func testDocker(t *testing.T, tag string, cmd []string) (exitCode int, logs string) {
	SkipUnlessLocal(t)
	l := StartCluster(t).(*cluster.LocalCluster)

	defer l.AssertAndStop(t)

	cli, err := dockerclient.NewEnvClient()
	if err != nil {
		t.Fatal(err)
	}

	addr := l.Nodes[0].PGAddr()
	containerConfig := container.Config{
		Image: "cockroachdb/postgres-test:" + tag,
		Env: []string{
			fmt.Sprintf("PGHOST=%s", addr.IP),
			fmt.Sprintf("PGPORT=%d", addr.Port),
			"PGSSLCERT=/certs/node.client.crt",
			"PGSSLKEY=/certs/node.client.key",
		},
		Cmd: strslice.New(cmd...),
	}
	hostConfig := container.HostConfig{
		Binds:       []string{fmt.Sprintf("%s:%s", l.CertsDir, "/certs")},
		NetworkMode: "host",
	}

	rc, err := cli.ImagePull(context.Background(), types.ImagePullOptions{
		ImageID: containerConfig.Image,
		Tag:     tag,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	dec := json.NewDecoder(rc)
	for {
		var message jsonmessage.JSONMessage
		if err := dec.Decode(&message); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		log.Infof("ImagePull response: %s", message)
	}

	resp, err := cli.ContainerCreate(&containerConfig, &hostConfig, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, warning := range resp.Warnings {
		log.Warning(warning)
	}
	defer func() {
		if err := cli.ContainerRemove(types.ContainerRemoveOptions{
			ContainerID:   resp.ID,
			RemoveVolumes: true,
		}); err != nil {
			t.Error(err)
		}
	}()
	if err := cli.ContainerStart(resp.ID); err != nil {
		t.Fatal(err)
	}
	exitCode, err = cli.ContainerWait(context.Background(), resp.ID)
	if err != nil {
		t.Fatal(err)
	}
	if exitCode != 0 {
		rc, err := cli.ContainerLogs(context.Background(), types.ContainerLogsOptions{
			ContainerID: resp.ID,
			ShowStdout:  true,
			ShowStderr:  true,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer rc.Close()
		b, err := ioutil.ReadAll(rc)
		if err != nil {
			t.Fatal(err)
		}
		logs = string(b)
	}
	return
}
