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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// +build acceptance

package acceptance

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-prod/tools/terrafarm"
	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/stop"
)

var duration = flag.Duration("d", 5*time.Second, "duration to run the test")
var numLocal = flag.Int("num-local", 0, "start a local cluster of the given size")
var numRemote = flag.Int("num-remote", 0, "start a remote cluster of the given size")
var cwd = flag.String("cwd", "../../cockroach-prod/terraform/aws", "directory to run terraform from")
var stall = flag.Duration("stall", time.Minute, "duration after which if no forward progress is made, consider the test stalled")
var keyName = flag.String("key-name", "cockroach", "name of key for remote cluster")
var stopper = make(chan struct{})

// StartCluster starts a cluster from the relevant flags.
func StartCluster(t *testing.T) cluster.Cluster {
	if *numLocal > 0 {
		if *numRemote > 0 {
			t.Fatal("cannot both specify -num-local and -num-remote")
		}
		l := cluster.CreateLocal(*numLocal, stopper)
		l.Start()
		checkRangeReplication(t, l, 20*time.Second)
		return l
	}
	if *numRemote == 0 {
		t.Fatal("need to either specify -num-local or -num-remote")
	}
	f := &terrafarm.Farmer{
		Debug:   true,
		Cwd:     *cwd,
		KeyName: *keyName,
	}
	if err := f.Destroy(); err != nil {
		t.Fatal(err)
	}
	if err := f.Add(*numRemote, 0); err != nil {
		t.Fatal(err)
	}
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
	if err := json.Unmarshal(b, v); err != nil {
		if log.V(1) {
			log.Info(err)
		}
	}
	return nil
}
