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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package terrafarm_test // intentionally; prevents access to internals

import (
	"flag"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/terrafarm"
)

var cwd = flag.String("cwd", func() string {
	const relPath = "../../terraform/aws"
	absPath, err := filepath.Abs(relPath)
	if err != nil {
		return relPath
	}
	return absPath
}(), "directory to run terraform from")
var keyName = flag.String("key-name", "cockroach", "name of key for cluster")
var logDir = flag.String("l", "", "log dir (empty for temporary)")
var duration = flag.Duration("d", 5*time.Minute, "duration for each test")

func farmer(t *testing.T) *terrafarm.Farmer {
	logDir := *logDir
	if logDir == "" {
		var err error
		logDir, err = ioutil.TempDir(os.TempDir(), "clustertest_")
		if err != nil {
			t.Fatal(err)
		}
	}
	f := &terrafarm.Farmer{
		Debug:   true,
		Cwd:     *cwd,
		LogDir:  logDir,
		KeyName: *keyName,
	}
	return f
}

// TestBuildCluster resizes the cluster to one node, one writer.
// It does not tear down the cluster after it's done and is mostly
// useful for testing code changes in the `terrafarm` package.
func TestBuildCluster(t *testing.T) {
	f := farmer(t)
	defer f.CollectLogs()
	if err := f.Resize(1, 1); err != nil {
		t.Fatal(err)
	}
	f.Assert(t)
}

// TestFiveNodesAndWriters runs a five node cluster and five writers.
// The test runs until SIGINT is received (or the test times out).
func TestFiveNodesAndWriters(t *testing.T) {
	deadline := time.After(*duration)
	f := farmer(t)
	defer f.MustDestroy()
	if err := f.Resize(5, 5); err != nil {
		t.Fatal(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	f.Assert(t)
	for {
		select {
		case <-deadline:
			return
		case <-c:
			return
		case <-time.After(time.Minute):
			f.Assert(t)
		}
	}
}
