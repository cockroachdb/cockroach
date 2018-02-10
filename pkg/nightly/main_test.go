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

package nightly

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

var slow = flag.Bool("slow", false, "Run slow tests")
var local = flag.Bool("local", false, "Run tests locally")
var artifacts = flag.String("artifacts", "", "Path to artifacts directory")
var cockroach = flag.String("cockroach", "", "Path to cockroach binary to use")
var workload = flag.String("workload", "", "Path to workload binary to use")

func checkTestTimeout(t testing.TB) {
	f := flag.Lookup("test.timeout")
	d := f.Value.(flag.Getter).Get().(time.Duration)
	if d == 0 {
		// The default timeout is 10m, despite the flag setting due to special code
		// in the go tool.
		d = 10 * time.Minute
	}
	if d < time.Hour {
		t.Fatalf("-timeout is too short: %s", d)
	}
}

func findBinary(binary, defValue string) (string, error) {
	if binary == "" {
		binary = defValue
	}

	if _, err := os.Stat(binary); err == nil {
		return filepath.Abs(binary)
	}

	// For "local" clusters we have to find the binary to run and translate it to
	// an absolute path. First, look for the binary in PATH.
	path, err := exec.LookPath(binary)
	if err != nil {
		if strings.HasPrefix(binary, "/") {
			return "", err
		}
		// We're unable to find the binary in PATH and "binary" is a relative path:
		// look in the cockroach repo.
		gopath := os.Getenv("GOPATH")
		if gopath == "" {
			return "", err
		}
		path = filepath.Join(gopath, "/src/github.com/cockroachdb/cockroach/", binary)
		var err2 error
		path, err2 = exec.LookPath(path)
		if err2 != nil {
			return "", err
		}
	}
	return filepath.Abs(path)
}

func initBinaries(t *testing.T) {
	var err error
	*cockroach, err = findBinary(*cockroach, "cockroach")
	if err != nil {
		t.Fatal(err)
	}
	*workload, err = findBinary(*workload, "workload")
	if err != nil {
		t.Fatal(err)
	}
}

var initOnce sync.Once

func maybeSkip(t *testing.T) {
	if !*slow {
		t.Skipf("-slow not specified")
	}

	initOnce.Do(func() {
		checkTestTimeout(t)
		initBinaries(t)
	})

	// If we're not running locally, we can run tests in parallel.
	if !*local {
		t.Parallel()
	}
}

var clusters = map[string]struct{}{}
var clustersMu syncutil.Mutex

func registerCluster(clusterName string) {
	clustersMu.Lock()
	clusters[clusterName] = struct{}{}
	clustersMu.Unlock()
}

func unregisterCluster(clusterName string) {
	clustersMu.Lock()
	delete(clusters, clusterName)
	clustersMu.Unlock()
}

func TestMain(m *testing.M) {
	go func() {
		// Shut down test clusters when interrupted (for example CTRL+C).
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig

		// Fire off a goroutine to destroy all of the clusters.
		done := make(chan struct{})
		go func() {
			defer close(done)

			var wg sync.WaitGroup
			clustersMu.Lock()
			wg.Add(len(clusters))
			for name := range clusters {
				go func(name string) {
					cmd := exec.Command("roachprod", "destroy", name)
					if err := cmd.Run(); err != nil {
						fmt.Fprintln(os.Stderr, err)
					}
				}(name)
			}
			clustersMu.Unlock()

			wg.Wait()
		}()

		// Wait up to 5 min for clusters to be destroyed. This can take a while and
		// we don't want to rush it.
		select {
		case <-done:
		case <-time.After(5 * time.Minute):
		}
	}()

	os.Exit(m.Run())
}
