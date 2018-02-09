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
	"context"
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
	"golang.org/x/sync/errgroup"
)

var slow = flag.Bool("slow", false, "Run slow tests")
var local = flag.Bool("local", false, "Run tests locally")
var artifacts = flag.String("artifacts", "", "Path to artifacts directory")
var cockroach = flag.String("cockroach", "", "Path to cockroach binary to use")
var workload = flag.String("workload", "", "Path to workload binary to use")
var clusterID = flag.String("clusterid", "", "An identifier to use in the test cluster's name")
var initOnce sync.Once

func checkTestTimeout(t testing.TB) {
	f := flag.Lookup("test.timeout")
	d := f.Value.(flag.Getter).Get().(time.Duration)
	if d > 0 && d < time.Hour {
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

func runCmd(ctx context.Context, l *logger, args ...string) error {
	l.printf("> %s\n", strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Stdout = l.stdout
	cmd.Stderr = l.stderr
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, `runCmd: %s`, strings.Join(args, ` `))
	}
	return nil
}

func runCmds(ctx context.Context, l *logger, cmds [][]string) error {
	for _, cmd := range cmds {
		if err := runCmd(ctx, l, cmd...); err != nil {
			return err
		}
	}
	return nil
}

func destroyCluster(t testing.TB, l *logger, clusterName string) {
	// New context so cluster shutdown is unaffected by any other contexts being
	// canceled.
	ctx := context.Background()
	logPath := filepath.Join(*artifacts, fileutil.EscapeFilename(t.Name()))
	_ = runCmd(ctx, l, "roachprod", "get", clusterName, "logs", logPath)
	if err := runCmd(ctx, l, "roachprod", "destroy", clusterName); err != nil {
		l.errorf("%s", err)
	}
	unregisterCluster(clusterName)
}

func makeClusterName(t *testing.T) string {
	if *local {
		return "local"
	}

	t.Parallel()
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

func TestLocal(t *testing.T) {
	if !*local {
		t.Skipf("-local not specified")
	}
	maybeSkip(t)

	ctx := context.Background()
	c := newCluster(ctx, t, "-n", "1")
	defer c.Destroy(ctx, t)

	c.Put(ctx, t, *cockroach, "<cockroach>")
	c.Put(ctx, t, *workload, "<workload>")
	c.Start(ctx, t, 1)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		c.Run(ctx, t, 1, "<workload> run kv --init --read-percent=95 --splits=100 --duration=10s")
		return nil
	})
	c.Monitor(ctx, t, g)
}

func TestSingleDC(t *testing.T) {
	maybeSkip(t)

	concurrency := " --concurrency=384"
	duration := " --duration=10m"
	splits := " --splits=100000"
	if *local {
		concurrency = ""
		duration = " --duration=10s"
		splits = " --splits=2000"
	}

	run := func(name, cmd string) {
		t.Run(name, func(t *testing.T) {
			// TODO(dan): It's not clear to me yet how all the various configurations
			// of clusters (# of machines in each locality) are going to
			// generalize. For now, just hardcode what we had before.
			ctx := context.Background()
			c := newCluster(ctx, t, "-n", "4")
			defer c.Destroy(ctx, t)

			c.Put(ctx, t, *cockroach, "<cockroach>")
			c.Put(ctx, t, *workload, "<workload>")
			c.Start(ctx, t, 1, 3)

			g, ctx := errgroup.WithContext(ctx)
			g.Go(func() error {
				c.Run(ctx, t, 4, cmd)
				return nil
			})
			c.Monitor(ctx, t, g, 1, 3)
		})
	}

	run("kv0", "<workload> run kv --init --read-percent=0 --splits=1000"+concurrency+duration)
	run("kv95", "<workload> run kv --init --read-percent=95 --splits=1000"+concurrency+duration)
	run("splits", "<workload> run kv --init --read-percent=0 --max-ops=1"+concurrency+splits)
	run("tpcc_w1", "<workload> run tpcc --init --warehouses=1 --wait=false"+concurrency+duration)
	run("tpmc_w1", "<workload> run tpcc --init --warehouses=1 --concurrency=10"+duration)
}

func TestRoachmart(t *testing.T) {
	maybeSkip(t)

	testutils.RunTrueAndFalse(t, "partition", func(t *testing.T, partition bool) {
		ctx := context.Background()
		c := newCluster(ctx, t, "--geo", "--nodes", "9")
		defer c.Destroy(ctx, t)

		c.Put(ctx, t, *cockroach, "<cockroach>")
		c.Put(ctx, t, *workload, "<workload>")
		c.Start(ctx, t, 1, 9)

		// TODO(benesch): avoid hardcoding this list.
		nodes := []struct {
			i    int
			zone string
		}{
			{1, "us-east1-b"},
			{4, "us-west1-b"},
			{7, "europe-west2-b"},
		}

		roachmartRun := func(ctx context.Context, t *testing.T, i int, args ...string) {
			t.Helper()
			args = append(args,
				"--local-zone="+nodes[i].zone,
				"--local-percent=90",
				"--users=10",
				"--orders=100",
				fmt.Sprintf("--partition=%v", partition))

			l, err := c.l.childLogger(fmt.Sprint(nodes[i].i))
			if err != nil {
				t.Fatal(err)
			}
			c.RunL(ctx, t, l, nodes[i].i, args...)
		}
		roachmartRun(ctx, t, 0, "<workload>", "init", "roachmart")

		duration := "10m"
		if *local {
			duration = "10s"
		}

		g, ctx := errgroup.WithContext(ctx)
		for i := range nodes {
			i := i
			g.Go(func() error {
				roachmartRun(ctx, t, i, "<workload>", "run", "roachmart", "--duration", duration)
				return nil
			})
		}

		c.Monitor(ctx, t, g)
	})
}
