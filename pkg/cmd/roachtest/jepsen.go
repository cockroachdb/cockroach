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

package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var jepsenTests = []string{
	"bank",
	"bank-multitable",
	// The comments test is expected to fail because it requires linearizability.
	// "comments",
	"g2",
	"monotonic",
	"register",
	"sequential",
	"sets",
}

var jepsenNemeses = []string{
	"--nemesis majority-ring",
	"--nemesis split",
	"--nemesis start-kill-2",
	"--nemesis start-stop-2",
	"--nemesis strobe-skews",
	"--nemesis subcritical-skews",
	"--nemesis majority-ring --nemesis2 subcritical-skews",
	"--nemesis subcritical-skews --nemesis2 start-kill-2",
	"--nemesis majority-ring --nemesis2 start-kill-2",
	"--nemesis parts --nemesis2 start-kill-2",
}

func runJepsen(ctx context.Context, t *test, c *cluster) {
	if c.name == "local" {
		t.Fatal("local execution not supported")
	}
	controller := c.Node(c.nodes)
	workers := c.Range(1, c.nodes-1)

	// Wrap roachtest's primitive logging in something more like util/log
	logf := func(f string, args ...interface{}) {
		// This log prefix matches the one (sometimes) used in roachprod
		c.l.printf(timeutil.Now().Format("2006/01/02 15:04:05 "))
		c.l.printf(f, args...)
		c.l.printf("\n")
	}

	// Run a command with output redirected to the logs instead of to
	// os.Stdout (which doesn't go anywhere I've been able to find)
	// Don't use this if you're going to call cmd.CombinedOutput.
	loggedCommand := func(ctx context.Context, arg0 string, args ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, arg0, args...)
		cmd.Stdout = c.l.stdout
		cmd.Stderr = c.l.stderr
		return cmd
	}

	// TODO(bdarnell): Does this blanket apt update matter? I just
	// copied it from the old jepsen scripts. It's slow, so we should
	// probably either remove it or use a new base image with more of
	// these preinstalled.
	//
	// In spite of -qqy, these produce huge amounts of output, so we
	// send it all to /dev/null. I apologize to whoever debugs this in
	// the future if these start to fail.
	c.Run(ctx, c.All(), "sh", "-c", `"sudo apt-get -qqy update > /dev/null 2>&1"`)
	c.Run(ctx, c.All(), "sh", "-c", `"sudo apt-get -qqy upgrade -o Dpkg::Options::='--force-confold' > /dev/null 2>&1"`)

	// Install the binary on all nodes and package it as jepsen expects.
	// TODO(bdarnell): copying the raw binary and compressing it on the
	// other side is silly, but this lets us avoid platform-specific
	// quirks in tar. The --transform option is only available on gnu
	// tar. To be able to run from a macOS host with BSD tar we'd need
	// use the similar -s option on that platform.
	c.Put(ctx, cockroach, "./cockroach", c.All())
	// Jepsen expects a tarball that expands to cockroach/cockroach
	// (which is not how our official builds are laid out).
	c.Run(ctx, c.All(), "tar --transform s,^,cockroach/, -c -z -f cockroach.tgz cockroach")

	// Install Jepsen and its prereqs on the controller.
	c.Run(ctx, controller, "sh", "-c", `"sudo apt-get -qqy install openjdk-8-jre openjdk-8-jre-headless libjna-java gnuplot > /dev/null 2>&1"`)
	c.Run(ctx, controller, "test -x lein || (curl -o lein https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein && chmod +x lein)")
	c.GitClone(ctx, "https://github.com/cockroachdb/jepsen", "./jepsen", "tc-nightly", controller)

	// Get the IP addresses for all our workers.
	cmd := exec.CommandContext(ctx, "roachprod", "run", c.makeNodes(workers), "--", "hostname", "-I")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	var workerIPs []string
	var nodeFlags []string
	lines := strings.Split(string(output), "\n")
	// TODO(bdarnell): add an option to `roachprod run` for
	// machine-friendly output (or merge roachprod into roachtest so we
	// can access it here).
	lineRE := regexp.MustCompile(`\s*[0-9]+:\s*([0-9.]+)`)
	for i := range lines {
		fields := lineRE.FindStringSubmatch(lines[i])
		if len(fields) == 0 {
			continue
		}
		workerIPs = append(workerIPs, fields[1])
		nodeFlags = append(nodeFlags, "-n "+fields[1])
	}
	nodesStr := strings.Join(nodeFlags, " ")

	// SSH setup: create a key on the controller.
	tempDir, err := ioutil.TempDir("", "jepsen")
	if err != nil {
		t.Fatal(err)
	}
	c.Run(ctx, controller, "sh", "-c", `"test -f .ssh/id_rsa || ssh-keygen -f .ssh/id_rsa -t rsa -N ''"`)
	pubSSHKey := filepath.Join(tempDir, "id_rsa.pub")
	cmd = loggedCommand(ctx, "roachprod", "get", c.makeNodes(controller), ".ssh/id_rsa.pub", pubSSHKey)
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	// TODO(bdarnell): make this idempotent instead of filling up .ssh configs.
	c.Put(ctx, pubSSHKey, "controller_id_rsa.pub", workers)
	c.Run(ctx, workers, "sh", "-c", `"cat controller_id_rsa.pub >> .ssh/authorized_keys"`)
	// Prime the known hosts file, and use the unhashed format to
	// work around JSCH auth error: https://github.com/jepsen-io/jepsen/blob/master/README.md
	for _, ip := range workerIPs {
		c.Run(ctx, controller, "sh", "-c", fmt.Sprintf(`"ssh-keyscan -t rsa %s >> .ssh/known_hosts"`, ip))
	}

	var failures []string
	testIdx := 0
	numTests := len(jepsenTests) * len(jepsenNemeses)
	for _, testName := range jepsenTests {
		for _, nemesis := range jepsenNemeses {
			testIdx++
			testCfg := fmt.Sprintf("%s %s", testName, nemesis)
			t.Status(fmt.Sprintf("%d/%d: %s (%d failures)", testIdx, numTests, testCfg, len(failures)))
			logf("%s: running", testCfg)

			// Reset the "latest" alias for the next run.
			c.Run(ctx, controller, "rm -f jepsen/cockroachdb/store/latest")

			errCh := make(chan error, 1)
			go func() {
				errCh <- c.RunE(ctx, controller, "bash", "-e", "-c", fmt.Sprintf(`"\
cd jepsen/cockroachdb && set -eo pipefail && \
 ~/lein run test \
   --tarball file://${PWD}/cockroach.tgz \
   --username ${USER} \
   --ssh-private-key ~/.ssh/id_rsa \
   --os ubuntu \
   --time-limit 300 \
   --concurrency 30 \
   --recovery-time 25 \
   --test-count 1 \
   %s \
   --test %s %s \
> invoke.log 2>&1 \
"`, nodesStr, testName, nemesis))
			}()

			outputDir := filepath.Join(artifacts, c.t.Name(), testCfg)
			if err := os.MkdirAll(outputDir, 0777); err != nil {
				t.Fatal(err)
			}
			var failed bool
			select {
			case testErr := <-errCh:
				if testErr == nil {
					logf("%s: passed, grabbing minimal logs", testCfg)
				} else {
					logf("%s: failed: %s", testCfg, testErr)
					failed = true
				}

			case <-time.After(20 * time.Minute):
				// Although we run tests of 6 minutes each, we use a timeout
				// much larger than that. This is because Jepsen for some
				// tests (e.g. register) runs a potentially long analysis
				// after the test itself has completed, before determining
				// whether the test has succeeded or not.
				//
				// Try to get any running jvm to log its stack traces for
				// extra debugging help.
				c.Run(ctx, controller, "pkill -QUIT java")
				time.Sleep(10 * time.Second)
				c.Run(ctx, controller, "pkill java")
				logf("%s: timed out", testCfg)
				failed = true
			}

			if failed {
				failures = append(failures, testCfg)
				// collect the systemd log on failure to diagnose #20492.
				// TODO(bdarnell): remove the next two lines when that's resolved.
				logf("%s: systemd log:", testCfg)
				c.Run(ctx, controller, "journalctl -x --no-pager")
				logf("%s: grabbing artifacts from controller. Tail of controller log:", testCfg)
				c.Run(ctx, controller, "tail -n 100 jepsen/cockroachdb/invoke.log")
				cmd = loggedCommand(ctx, "roachprod", "run", c.makeNodes(controller),
					// -h causes tar to follow symlinks; needed by the "latest" symlink.
					// -f- sends the output to stdout, we read it and save it to a local file.
					"tar -chj --ignore-failed-read -f- jepsen/cockroachdb/store/latest jepsen/cockroachdb/invoke.log /var/log/")
				output, err := cmd.Output()
				if err != nil {
					t.Fatal(err)
				}
				if err := ioutil.WriteFile(filepath.Join(outputDir, "failure-logs.tbz"), output, 0666); err != nil {
					t.Fatal(err)
				}
			} else {
				collectFiles := []string{
					"test.fressian", "results.edn", "latency-quantiles.png", "latency-raw.png", "rate.png",
				}
				anyFailed := false
				for _, file := range collectFiles {
					cmd = loggedCommand(ctx, "roachprod", "get", c.makeNodes(controller),
						"jepsen/cockroachdb/store/latest/"+file,
						filepath.Join(outputDir, file))
					cmd.Stdout = c.l.stdout
					cmd.Stderr = c.l.stderr
					if err := cmd.Run(); err != nil {
						logf("failed to retrieve %s: %s", file, err)
					}
				}
				if anyFailed {
					// Try to figure out why this is so common.
					cmd = loggedCommand(ctx, "roachprod", "get", c.makeNodes(controller),
						"jepsen/cockroachdb/invoke.log",
						filepath.Join(outputDir, "invoke.log"))
					cmd.Stdout = c.l.stdout
					cmd.Stderr = c.l.stderr
					if err := cmd.Run(); err != nil {
						logf("failed to retrieve invoke.log: %s", err)
					}
				}
			}
		}
	}
	if len(failures) > 0 {
		logf("jepsen tests failed: %v", failures)
		t.Fatalf("jepsen tests failed: %v", failures)
	}
}

func registerJepsen(r *registry) {
	r.Add(testSpec{
		Name:  "jepsen",
		Nodes: nodes(6),
		Run:   runJepsen,
	})
}
