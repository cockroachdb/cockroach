// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package acceptance

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const testGlob = "../cli/interactive_tests/test*.tcl"
const containerPath = "/go/src/github.com/cockroachdb/cockroach/cli/interactive_tests"

var cmdBase = []string{
	"/usr/bin/env",
	"COCKROACH_SKIP_UPDATE_CHECK=1",
	"COCKROACH_CRASH_REPORTS=",
	"/bin/bash",
	"-c",
}

func TestDockerCLI(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	containerConfig := defaultContainerConfig()
	containerConfig.Cmd = []string{"stat", cluster.CockroachBinaryInContainer}
	containerConfig.Env = []string{fmt.Sprintf("PGUSER=%s", security.RootUser)}
	ctx := context.Background()
	if err := testDockerOneShot(ctx, t, "cli_test", containerConfig); err != nil {
		t.Skipf(`TODO(dt): No binary in one-shot container, see #6086: %s`, err)
	}

	paths, err := filepath.Glob(testGlob)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("no testfiles found (%v)", testGlob)
	}

	for _, p := range paths {
		testFile := filepath.Base(p)
		testPath := filepath.Join(containerPath, testFile)
		if strings.Contains(testPath, "disabled") {
			t.Logf("Skipping explicitly disabled test %s", testFile)
			continue
		}
		t.Run(testFile, func(t *testing.T) {
			log.Infof(ctx, "-- starting tests from: %s", testFile)

			// Symlink the logs directory to /logs, which is visible outside of the
			// container and preserved if the test fails. (They don't write to /logs
			// directly because they are often run manually outside of Docker, where
			// /logs is unlikely to exist.)
			cmd := "ln -s /logs logs"

			// We run the expect command using `bash -c "(expect ...)"`.
			//
			// We cannot run `expect` directly, nor `bash -c "expect ..."`,
			// because both cause Expect to become the PID 1 process inside
			// the container. On Unix, orphan processes need to be wait()ed
			// upon by the PID 1 process when they terminate, lest they
			// remain forever in the zombie state. Unfortunately, Expect
			// does not contain code to do this. Bash does.
			cmd += "; (expect"
			if log.V(2) {
				cmd = cmd + " -d"
			}
			cmd = cmd + " -f " + testPath + " " + cluster.CockroachBinaryInContainer + ")"
			containerConfig.Cmd = append(cmdBase, cmd)

			if err := testDockerOneShot(ctx, t, "cli_test", containerConfig); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestDockerStartFlags(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	containerConfig := defaultContainerConfig()
	containerConfig.Cmd = []string{"stat", cluster.CockroachBinaryInContainer}
	ctx := context.Background()
	if err := testDockerOneShot(ctx, t, "start_flags_test", containerConfig); err != nil {
		t.Skipf(`TODO(dt): No binary in one-shot container, see #6086: %s`, err)
	}

	script := `
set -eux
bin=/cockroach/cockroach

touch out
function finish() {
	cat out
}
trap finish EXIT

HOST=$(hostname -f)
$bin start --logtostderr=INFO --background --insecure --listen-addr="${HOST}":12345 &> out
$bin sql --insecure --host="${HOST}":12345 -e "show databases"
$bin quit --insecure --host="${HOST}":12345
`
	containerConfig.Cmd = []string{"/bin/bash", "-c", script}
	if err := testDockerOneShot(ctx, t, "start_flags_test", containerConfig); err != nil {
		t.Error(err)
	}

}
