// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

var jepsenNemeses = []struct {
	name, config string
}{
	{"majority-ring", "--nemesis majority-ring"},
	{"split", "--nemesis split"},
	{"start-kill-2", "--nemesis start-kill-2"},
	{"start-stop-2", "--nemesis start-stop-2"},
	{"strobe-skews", "--nemesis strobe-skews"},
	// TODO(bdarnell): subcritical-skews nemesis is currently flaky due to ntp rate limiting.
	// https://github.com/cockroachdb/cockroach/issues/35599
	//{"subcritical-skews", "--nemesis subcritical-skews"},
	//{"majority-ring-subcritical-skews", "--nemesis majority-ring --nemesis2 subcritical-skews"},
	//{"subcritical-skews-start-kill-2", "--nemesis subcritical-skews --nemesis2 start-kill-2"},
	{"majority-ring-start-kill-2", "--nemesis majority-ring --nemesis2 start-kill-2"},
	{"parts-start-kill-2", "--nemesis parts --nemesis2 start-kill-2"},
}

func initJepsen(ctx context.Context, t test.Test, c cluster.Cluster) {
	// NB: comment this out to see the commands jepsen would run locally.
	if c.IsLocal() {
		t.Fatal("local execution not supported")
	}

	if c.IsLocal() {
		// We can't perform any of the remaining setup locally and while we can't
		// run jepsen locally we let the test run to indicate which commands it
		// would have run remotely.
		return
	}

	controller := c.Node(c.Spec().NodeCount)
	workers := c.Range(1, c.Spec().NodeCount-1)

	// Install jepsen. This part is fast if the repo is already there,
	// so do it before the initialization check for ease of iteration.
	if err := c.GitClone(
		ctx, t.L(),
		"https://github.com/cockroachdb/jepsen", "/mnt/data1/jepsen", "tc-nightly", controller,
	); err != nil {
		t.Fatal(err)
	}

	// Check to see if the cluster has already been initialized.
	if err := c.RunE(ctx, c.Node(1), "test -e jepsen_initialized"); err == nil {
		t.L().Printf("cluster already initialized\n")
		return
	}
	t.L().Printf("initializing cluster\n")
	t.Status("initializing cluster")

	// Roachprod collects this directory by default. If we fail early,
	// this is the only log collection that is done. Otherwise, we
	// perform a second log collection in this test that varies
	// depending on whether the test passed or not.
	c.Run(ctx, c.All(), "mkdir", "-p", "logs")

	// `apt-get update` is slow but necessary: the base image has
	// outdated information and refers to package versions that are no
	// longer retrievable.
	//
	// TODO(bdarnell): Create a new base image with the packages we need
	// instead of installing them on every run.
	c.Run(ctx, c.All(), "sh", "-c", `"sudo apt-get -y update > logs/apt-upgrade.log 2>&1"`)
	c.Run(ctx, c.All(), "sh", "-c", `"sudo apt-get -y upgrade -o Dpkg::Options::='--force-confold' > logs/apt-upgrade.log 2>&1"`)

	// Install the binary on all nodes and package it as jepsen expects.
	// TODO(bdarnell): copying the raw binary and compressing it on the
	// other side is silly, but this lets us avoid platform-specific
	// quirks in tar. The --transform option is only available on gnu
	// tar. To be able to run from a macOS host with BSD tar we'd need
	// use the similar -s option on that platform.
	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	// Jepsen expects a tarball that expands to cockroach/cockroach
	// (which is not how our official builds are laid out).
	c.Run(ctx, c.All(), "tar --transform s,^,cockroach/, -c -z -f cockroach.tgz cockroach")

	// Install Jepsen's prereqs on the controller.
	if out, err := c.RunWithBuffer(
		ctx, t.L(), controller, "sh", "-c",
		`"sudo apt-get -qqy install openjdk-8-jre openjdk-8-jre-headless libjna-java gnuplot > /dev/null 2>&1"`,
	); err != nil {
		if strings.Contains(string(out), "exit status 100") {
			t.Skip("apt-get failure (#31944)", string(out))
		}
		t.Fatal(err)
	}

	c.Run(ctx, controller, "test -x lein || (curl -o lein https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein && chmod +x lein)")

	// SSH setup: create a key on the controller.
	tempDir, err := ioutil.TempDir("", "jepsen")
	if err != nil {
		t.Fatal(err)
	}
	c.Run(ctx, controller, "sh", "-c", `"test -f .ssh/id_rsa || ssh-keygen -f .ssh/id_rsa -t rsa -N ''"`)
	pubSSHKey := filepath.Join(tempDir, "id_rsa.pub")
	if err := c.Get(ctx, t.L(), ".ssh/id_rsa.pub", pubSSHKey, controller); err != nil {
		t.Fatal(err)
	}
	// TODO(bdarnell): make this idempotent instead of filling up .ssh configs.
	c.Put(ctx, pubSSHKey, "controller_id_rsa.pub", workers)
	c.Run(ctx, workers, "sh", "-c", `"cat controller_id_rsa.pub >> .ssh/authorized_keys"`)
	// Prime the known hosts file, and use the unhashed format to
	// work around JSCH auth error: https://github.com/jepsen-io/jepsen/blob/master/README.md
	ips, err := c.InternalIP(ctx, workers)
	if err != nil {
		t.Fatal(err)
	}
	for _, ip := range ips {
		c.Run(ctx, controller, "sh", "-c", fmt.Sprintf(`"ssh-keyscan -t rsa %s >> .ssh/known_hosts"`, ip))
	}

	t.L().Printf("cluster initialization complete\n")
	c.Run(ctx, c.Node(1), "touch jepsen_initialized")
}

func runJepsen(ctx context.Context, t test.Test, c cluster.Cluster, testName, nemesis string) {
	initJepsen(ctx, t, c)

	controller := c.Node(c.Spec().NodeCount)

	// Get the IP addresses for all our workers.
	var nodeFlags []string
	ips, err := c.InternalIP(ctx, c.Range(1, c.Spec().NodeCount-1))
	if err != nil {
		t.Fatal(err)
	}
	for _, ip := range ips {
		nodeFlags = append(nodeFlags, "-n "+ip)
	}
	nodesStr := strings.Join(nodeFlags, " ")

	runE := func(c cluster.Cluster, ctx context.Context, node option.NodeListOption, args ...string) error {
		if c.IsLocal() {
			// For local development.
			t.L().Printf("> %s\n", strings.Join(args, " "))
			return nil
		}
		return c.RunE(ctx, node, args...)
	}

	run := func(c cluster.Cluster, ctx context.Context, node option.NodeListOption, args ...string) {
		err := runE(c, ctx, node, args...)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Reset the "latest" alias for the next run.
	t.Status("running")
	run(c, ctx, controller, "rm -f /mnt/data1/jepsen/cockroachdb/store/latest")

	// Install the jepsen package (into ~/.m2) before running tests in
	// the cockroach package. Clojure doesn't really understand
	// monorepos so steps like this are necessary for one package to
	// depend on an unreleased package in the same repo.
	// Also remove the invoke.log from a previous test, if any.
	{
		err := runE(c, ctx, controller, "bash", "-e", "-c",
			`"cd /mnt/data1/jepsen/jepsen && ~/lein install && rm -f /mnt/data1/jepsen/cockroachdb/invoke.log"`)
		if err != nil {
			// Ignore an error like the following.
			// Could not transfer artifact org.clojure:clojure:jar:1.9.0 from/to central (https://repo1.maven.org/maven2/): GET request of: org/clojure/clojure/1.9.0/clojure-1.9.0.jar from central failed
			r := regexp.MustCompile("Could not transfer artifact|Failed to read artifact descriptor for")
			match := r.FindStringSubmatch(fmt.Sprintf("%+v", err))
			if match != nil {
				t.L().PrintfCtx(ctx, "failure installing deps (\"%s\")\nfull err: %+v",
					match, err)
				t.Skipf("failure installing deps (\"%s\"); in the past it's been transient", match)
			}
			t.Fatalf("error installing Jepsen deps: %+v", err)
		}
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- runE(c, ctx, controller, "bash", "-e", "-c", fmt.Sprintf(`"\
cd /mnt/data1/jepsen/cockroachdb && set -eo pipefail && \
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

	outputDir := t.ArtifactsDir()
	if err := os.MkdirAll(outputDir, 0777); err != nil {
		t.Fatal(err)
	}
	var testErr error
	select {
	case testErr = <-errCh:
		if testErr == nil {
			t.L().Printf("passed, grabbing minimal logs")
		} else {
			t.L().Printf("failed: %s", testErr)
		}

	case <-time.After(40 * time.Minute):
		// Although we run tests of 6 minutes each, we use a timeout
		// much larger than that. This is because Jepsen for some
		// tests (e.g. register) runs a potentially long analysis
		// after the test itself has completed, before determining
		// whether the test has succeeded or not.
		//
		// Try to get any running jvm to log its stack traces for
		// extra debugging help.
		run(c, ctx, controller, "pkill -QUIT java")
		time.Sleep(10 * time.Second)
		run(c, ctx, controller, "pkill java")
		t.L().Printf("timed out")
		testErr = fmt.Errorf("timed out")
	}

	if testErr != nil {
		t.L().Printf("grabbing artifacts from controller. Tail of controller log:")
		run(c, ctx, controller, "tail -n 100 /mnt/data1/jepsen/cockroachdb/invoke.log")
		// We recognize some errors and ignore them.
		// We're looking for the "Oh jeez" message that Jepsen prints as the test's
		// outcome, followed by some known exceptions on the next line. If we don't find
		// either one, we consider the error unrecognized.
		// TODO(andrei): The known errors are tracked in #30527 (BrokenBarrier and
		// Interrupted) and #26082 (JSch). Remove errors from this unfortunate list
		// once the respective issues are fixed.
		ignoreErr := false
		if err := runE(c, ctx, controller,
			`grep -E "(Oh jeez, I'm sorry, Jepsen broke. Here's why|Caused by)" /mnt/data1/jepsen/cockroachdb/invoke.log -A1 | grep `+
				`-e BrokenBarrierException `+
				`-e InterruptedException `+
				`-e com.jcraft.jsch.JSchException `+
				`-e ArrayIndexOutOfBoundsException `+
				`-e NullPointerException `+
				// And one more ssh failure we've seen, apparently encountered when
				// downloading logs.
				`-e "clojure.lang.ExceptionInfo: clj-ssh scp failure" `+
				// And sometimes the analysis succeeds and yet we still get an error code for some reason.
				`-e "Everything looks good" `+
				`-e "RuntimeException: Connection to"`, // timeout
		); err == nil {
			t.L().Printf("Recognized BrokenBarrier or other known exceptions (see grep output above). " +
				"Ignoring it and considering the test successful. " +
				"See #30527 or #26082 for some of the ignored exceptions.")
			ignoreErr = true
		}

		if output, err := c.RunWithBuffer(
			ctx, t.L(), controller,
			// -h causes tar to follow symlinks; needed by the "latest" symlink.
			// -f- sends the output to stdout, we read it and save it to a local file.
			"tar -chj --ignore-failed-read -C /mnt/data1/jepsen/cockroachdb -f- store/latest invoke.log",
		); err != nil {
			t.L().Printf("failed to retrieve jepsen artifacts and invoke.log: %s", err)
		} else if err := ioutil.WriteFile(filepath.Join(outputDir, "failure-logs.tbz"), output, 0666); err != nil {
			t.Fatal(err)
		} else {
			t.L().Printf("downloaded jepsen logs in failure-logs.tbz")
		}
		if ignoreErr {
			t.Skip("recognized known error", testErr.Error())
		}
		t.Fatal(testErr)
	} else {
		collectFiles := []string{
			"test.fressian", "results.edn", "latency-quantiles.png", "latency-raw.png", "rate.png",
		}
		anyFailed := false
		for _, file := range collectFiles {
			if err := c.Get(
				ctx, t.L(),
				"/mnt/data1/jepsen/cockroachdb/store/latest/"+file,
				filepath.Join(outputDir, file),
				controller,
			); err != nil {
				anyFailed = true
				t.L().Printf("failed to retrieve %s: %s", file, err)
			}
		}
		if anyFailed {
			// Try to figure out why this is so common.
			if err := c.Get(ctx, t.L(),
				"/mnt/data1/jepsen/cockroachdb/invoke.log",
				filepath.Join(outputDir, "invoke.log"),
				controller,
			); err != nil {
				t.L().Printf("failed to retrieve invoke.log: %s", err)
			}
		}
	}
}

// RegisterJepsen registers the Jepsen test suite, which primarily checks for
// transaction anomalies.
func RegisterJepsen(r registry.Registry) {
	// NB: the "comments" test is not included because it requires
	// linearizability.
	tests := []string{
		"bank",
		"bank-multitable",
		"g2",
		"monotonic",
		"register",
		"sequential",
		"sets",
		"multi-register",
	}
	for _, testName := range tests {
		testName := testName
		for _, nemesis := range jepsenNemeses {
			nemesis := nemesis // copy for closure
			s := registry.TestSpec{
				Name: fmt.Sprintf("jepsen/%s/%s", testName, nemesis.name),
				// We don't run jepsen on older releases due to the high rate of flakes.
				Owner: registry.OwnerKV,
				// The Jepsen tests do funky things to machines, like muck with the
				// system clock; therefore, their clusters cannot be reused other tests
				// except the Jepsen ones themselves which reset all this state when
				// they start. It is important, however, that the Jepsen tests reuses
				// clusters because they have a lengthy setup step, but avoid doing it
				// if they detect that the machines have already been properly
				// initialized.
				Cluster: r.MakeClusterSpec(6, spec.ReuseTagged("jepsen")),
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runJepsen(ctx, t, c, testName, nemesis.config)
				},
			}
			r.Add(s)
		}
	}
}
