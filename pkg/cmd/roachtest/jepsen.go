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
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

func runJepsen(ctx context.Context, t *test, c *cluster) {
	if c.name == "local" {
		t.Fatal("local execution not supported")
	}
	controller := c.Node(c.nodes)
	workers := c.Range(1, c.nodes-1)

	// TODO(bdarnell): Does this blanket apt update matter? I just
	// copied it from the old jepsen scripts. It's slow, so we should
	// probably either remove it or use a new base image with more of
	// these preinstalled.
	c.Run(ctx, c.All(), "sudo", "apt-get", "-qqy", "update")
	c.Run(ctx, c.All(), "sudo", "apt-get", "-qqy", "upgrade", "-o", "Dpkg::Options::='--force-confold'")

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
	c.Run(ctx, controller, "sudo", "apt-get", "-qqy", "install",
		"openjdk-8-jre", "openjdk-8-jre-headless", "libjna-java", "gnuplot")
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
	cmd = exec.CommandContext(ctx, "roachprod", "get", c.makeNodes(controller), ".ssh/id_rsa.pub", pubSSHKey)
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

	// TODO(bdarnell): add the rest of the tests and nemeses.
	for _, testName := range []string{"bank"} {
		for _, nemesis := range []string{"start-kill-2"} {
			// TODO(bdarnell): the old jepsen scripts would abort the test
			// if it went 60s with no output. Either reintroduce that or
			// don't tee everything to stderr.
			c.Run(ctx, controller, "bash", "-e", "-c", fmt.Sprintf(`"\
cd jepsen/cockroachdb && set -eo pipefail && \
 stdbuf -oL -eL \
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
2>&1 | stdbuf -oL tee invoke.log \
"`, nodesStr, testName, nemesis))
			// TODO(bdarnell): Collect logs and publish to artifact
			// directory. Allow each test+nemesis to fail independently.
		}
	}
}

func init() {
	tests.Add(testSpec{
		Name:  "jepsen",
		Nodes: nodes(6),
		Run:   runJepsen,
	})
}
