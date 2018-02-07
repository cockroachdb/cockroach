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
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

var slow = flag.Bool("slow", false, "Run slow tests")
var artifacts = flag.String("artifacts", "", "Path to artifacts directory")
var cockroach = flag.String("cockroach", "", "Path to cockroach binary to use")
var workload = flag.String("workload", "", "Path to workload binary to use")
var clusterID = flag.String("clusterid", "", "An identifier to use in the test cluster's name")

func maybeSkip(t testing.TB) {
	if !*slow {
		if testing.Verbose() {
			fmt.Fprintf(os.Stderr, "skipping %s because -slow flag was not provided\n", t.Name())
		}
		t.Skip()
	}
	if _, err := os.Stat(*cockroach); err != nil {
		t.Fatal(errors.Wrap(err, `missing cockroach binary`))
	}
	if _, err := os.Stat(*workload); err != nil {
		t.Fatal(errors.Wrap(err, `missing workload binary`))
	}
}

func runCmd(ctx context.Context, l *logger, args ...string) error {
	l.printf("> %s\n", strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Stdout = &l.stdout
	cmd.Stderr = &l.stderr
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
	return name
}

func TestSingleDC(t *testing.T) {
	// TODO(benesch,peter): Warn if the test timeout is too low.

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
			ctx := context.Background()

			l, err := rootLogger(t.Name())
			if err != nil {
				t.Fatal(err)
			}

			clusterName := makeClusterName(t)
			defer destroyCluster(t, l, clusterName)

			if err := runCmds(ctx, l, [][]string{
				{"roachprod", "create", clusterName},
				{"roachprod", "put", clusterName, *cockroach, "./cockroach"},
				{"roachprod", "put", clusterName, *workload, "./workload"},
				{"roachprod", "start", clusterName},
			}); err != nil {
				t.Fatal(err)
			}

			m := monitorWithContext(ctx)
			m.Worker(func(ctx context.Context) error {
				// TODO(dan): runCmd internally uses exec.CommandContext, which
				// is supposed to kill the exec'd process if the context is
				// canceled. This appears to work generally, but not for
				// `roachprod ssh`.
				return runCmd(ctx, l, "roachprod", "ssh", clusterName+":1", "--", testCmd)
			})
			if err := m.Monitor(l, "roachprod", "monitor", clusterName); err != nil {
				t.Fatalf(`%+v`, err)
			}
		})
	}
}

func TestRoachmart(t *testing.T) {
	maybeSkip(t)
	t.Parallel()

	testutils.RunTrueAndFalse(t, "partition", func(t *testing.T, partition bool) {
		t.Parallel()
		ctx := context.Background()

		l, err := rootLogger(t.Name())
		if err != nil {
			t.Fatal(err)
		}

		clusterName := makeClusterName(t)
		defer destroyCluster(t, l, clusterName)

		err = runCmds(ctx, l, [][]string{
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

		err = runCmd(ctx, l, append([]string{
			"roachprod", "ssh", fmt.Sprintf("%s:%d", clusterName, nodes[0].i), "--",
			"./workload", "init", "roachmart", "--local-zone", nodes[0].zone}, commonArgs...)...)
		if err != nil {
			t.Fatal(err)
		}

		m := monitorWithContext(ctx)
		for _, node := range nodes {
			node := node
			m.Worker(func(ctx context.Context) error {
				cl, err := l.childLogger(node.zone)
				if err != nil {
					return err
				}

				// TODO(dan): runCmd internally uses exec.CommandContext, which
				// is supposed to kill the exec'd process if the context is
				// canceled. This appears to work generally, but not for
				// `roachprod ssh`.
				workloadCmd := append([]string{
					"roachprod", "ssh", fmt.Sprintf("%s:%d", clusterName, node.i), "--",
					"./workload", "run", "roachmart", "--local-zone", node.zone, "--duration", "10m"},
					commonArgs...)
				err = runCmd(ctx, cl, workloadCmd...)
				return errors.Wrapf(err, "%d (%s): %s", node.i, node.zone, err)
			})
		}
		if err := m.Monitor(l, "roachprod", "monitor", clusterName); err != nil {
			t.Fatalf(`%+v`, err)
		}
	})
}

type monitor struct {
	ctx    context.Context
	cancel func()
	g      *errgroup.Group
}

func monitorWithContext(ctx context.Context) monitor {
	var m monitor
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.g, m.ctx = errgroup.WithContext(m.ctx)
	return m
}

func (m monitor) Worker(fn func(context.Context) error) {
	m.g.Go(func() error {
		return fn(m.ctx)
	})
}

func (m monitor) Monitor(l *logger, args ...string) error {
	monG, _ := errgroup.WithContext(m.ctx)
	monG.Go(func() error {
		defer m.cancel()
		err := m.g.Wait()
		if errors.Cause(err) == context.Canceled {
			return nil
		}
		return errors.Wrap(err, `worker`)
	})

	pipeR, pipeW := io.Pipe()
	monG.Go(func() error {
		defer func() { _ = pipeW.Close() }()

		monL, err := l.childLogger(`MONITOR`)
		if err != nil {
			return err
		}

		cmd := exec.CommandContext(m.ctx, args[0], args[1:]...)
		cmd.Stdout = io.MultiWriter(&monL.stdout, pipeW)
		cmd.Stderr = &monL.stderr
		if err := cmd.Start(); err != nil {
			if c := errors.Cause(err); c == context.Canceled {
				return nil
			}
			return errors.Wrap(err, strings.Join(args, ` `))
		}

		// If the workers finish first, the context will be canceled and so
		// exec.CommandContext will kill the monitor and we should ignore the
		// error.
		if err := cmd.Wait(); err != nil && !strings.Contains(err.Error(), `killed`) {
			return errors.Wrap(err, strings.Join(args, ` `))
		}
		return nil
	})
	monG.Go(func() error {
		defer m.cancel()
		scanner := bufio.NewScanner(pipeR)
		for scanner.Scan() {
			if strings.Contains(scanner.Text(), `dead`) {
				return errors.Errorf(`MONITOR: %s`, scanner.Text())
			}
		}
		return nil
	})

	return monG.Wait()
}

func TestMonitor(t *testing.T) {
	defer leaktest.AfterTest(t)

	l, err := rootLogger(t.Name())
	if err != nil {
		t.Fatalf(`%+v`, err)
	}

	t.Run(`success`, func(t *testing.T) {
		m := monitorWithContext(context.Background())
		m.Worker(func(ctx context.Context) error {
			return nil
		})
		if err := m.Monitor(l, `sleep`, `100`); err != nil {
			t.Errorf(`expected success got: %+v`, err)
		}
	})

	t.Run(`dead`, func(t *testing.T) {
		m := monitorWithContext(context.Background())
		m.Worker(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		expectedErr := `dead`
		if err := m.Monitor(l, `echo`, "1: 100\n1: dead"); !testutils.IsError(err, expectedErr) {
			t.Errorf(`expected %s err got: %+v`, expectedErr, err)
		}
	})

	t.Run(`worker-fail`, func(t *testing.T) {
		m := monitorWithContext(context.Background())
		m.Worker(func(_ context.Context) error {
			return errors.New(`worker-fail`)
		})
		m.Worker(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		expectedErr := `worker-fail`
		if err := m.Monitor(l, `sleep`, `60`); !testutils.IsError(err, expectedErr) {
			t.Errorf(`expected %s err got: %+v`, expectedErr, err)
		}
	})
}
