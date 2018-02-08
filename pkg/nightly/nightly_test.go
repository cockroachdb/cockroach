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
	"os/user"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

var slow = flag.Bool("slow", false, "Run slow tests")
var artifacts = flag.String("artifacts", "", "Path to artifacts directory")
var cockroach = flag.String("cockroach", "", "Path to cockroach binary to use")
var workload = flag.String("workload", "", "Path to workload binary to use")
var clusterID = flag.String("clusterid", "", "An identifier to use in the test cluster's name")

func checkTestTimeout(t testing.TB) {
	f := flag.Lookup("test.timeout")
	d := f.Value.(flag.Getter).Get().(time.Duration)
	if d > 0 && d < time.Hour {
		t.Fatalf("-timeout is too short: %s", d)
	}
}

func maybeSkip(t testing.TB) {
	if !*slow {
		if testing.Verbose() {
			fmt.Fprintf(os.Stderr, "skipping %s because -slow flag was not provided\n", t.Name())
		}
		t.Skip()
	}

	checkTestTimeout(t)
}

func runCmd(l *logger, args []string) error {
	l.printf("> %s\n", strings.Join(args, " "))
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdout = &l.stdout
	cmd.Stderr = &l.stderr
	if err := cmd.Run(); err != nil {
		return errors.Wrap(err, "runCmd")
	}
	return nil
}

func runCmds(l *logger, cmds [][]string) error {
	for _, cmd := range cmds {
		if err := runCmd(l, cmd); err != nil {
			return err
		}
	}
	return nil
}

func destroyCluster(t testing.TB, l *logger, clusterName string) {
	_ = runCmd(l, []string{"roachprod", "get", clusterName, "logs",
		filepath.Join(*artifacts, fileutil.EscapeFilename(t.Name()))})
	unregisterCluster(clusterName)
	if err := runCmd(l, []string{"roachprod", "destroy", clusterName}); err != nil {
		l.errorf("%s", err)
	}
}

func makeClusterName(t testing.TB) string {
	username := os.Getenv("ROACHPROD_USER")
	if username == "" {
		usr, err := user.Current()
		if err != nil {
			panic(fmt.Sprintf("user.Current: %s", err))
		}
		username = usr.Username
	}
	id := *clusterID
	if id == "" {
		id = fmt.Sprintf("%d", timeutil.Now().Unix())
	}
	name := fmt.Sprintf("%s-%s-%s", username, id, t.Name())
	name = strings.ToLower(name)
	name = regexp.MustCompile(`[^-a-z0-9]+`).ReplaceAllString(name, "-")
	name = regexp.MustCompile(`-+`).ReplaceAllString(name, "-")
	registerCluster(name)
	return name
}

func TestSingleDC(t *testing.T) {
	maybeSkip(t)
	t.Parallel()

	// TODO(dan): It's not clear to me yet how all the various configurations of
	// clusters (# of machines in each locality) are going to generalize. For now,
	// just hardcode what we had before.
	for testName, testCmd := range map[string]string{
		"kv0":    "./workload run kv --init --read-percent=0 --splits=1000 --concurrency=384 --duration=10m",
		"kv95":   "./workload run kv --init --read-percent=95 --splits=1000 --concurrency=384 --duration=10m",
		"splits": "./workload run kv --init --read-percent=0 --splits=100000 --concurrency=384 --max-ops=1 --duration=10m",
	} {
		testName, testCmd := testName, testCmd
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			l, err := rootLogger(t.Name())
			if err != nil {
				t.Fatal(err)
			}

			clusterName := makeClusterName(t)
			defer destroyCluster(t, l, clusterName)

			err = runCmds(l, [][]string{
				{"roachprod", "create", clusterName},
				{"roachprod", "put", clusterName, *cockroach, "./cockroach"},
				{"roachprod", "put", clusterName, *workload, "./workload"},
				{"roachprod", "start", clusterName},
				{"roachprod", "ssh", clusterName + ":1", "--", testCmd},
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRoachmart(t *testing.T) {
	maybeSkip(t)
	t.Parallel()

	testutils.RunTrueAndFalse(t, "partition", func(t *testing.T, partition bool) {
		t.Parallel()

		l, err := rootLogger(t.Name())
		if err != nil {
			t.Fatal(err)
		}

		clusterName := makeClusterName(t)
		defer destroyCluster(t, l, clusterName)

		err = runCmds(l, [][]string{
			{"roachprod", "create", clusterName, "--geo", "--nodes", "9"},
			{"roachprod", "put", clusterName, *cockroach, "./cockroach"},
			{"roachprod", "put", clusterName, *workload, "./workload"},
			{"roachprod", "start", clusterName},
		})
		if err != nil {
			t.Fatal(err)
		}

		// TODO(benesch): avoid hardcoding this list.
		nodes := []struct {
			i    int
			zone string
		}{
			{1, "us-east1-b"},
			{4, "us-west1-b"},
			{7, "europe-west2-b"},
		}

		commonArgs := []string{
			"--local-percent=90", "--users=10", "--orders=100", fmt.Sprintf("--partition=%v", partition),
		}

		err = runCmd(l, append([]string{
			"roachprod", "ssh", fmt.Sprintf("%s:%d", clusterName, nodes[0].i), "--",
			"./workload", "init", "roachmart", "--local-zone", nodes[0].zone}, commonArgs...))
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		for _, node := range nodes {
			node := node
			wg.Add(1)
			go func() {
				defer wg.Done()

				cl, err := l.childLogger(node.zone)
				if err != nil {
					t.Error(err)
				}

				err = runCmd(cl, append([]string{
					"roachprod", "ssh", fmt.Sprintf("%s:%d", clusterName, node.i), "--",
					"./workload", "run", "roachmart", "--local-zone", node.zone, "--duration", "10m"},
					commonArgs...))
				if err != nil {
					t.Errorf("%d (%s): %s", node.i, node.zone, err)
				}
			}()
		}
		wg.Wait()
	})
}
