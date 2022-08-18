// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/alessio/shellescape"
	"github.com/cockroachdb/errors"
)

func runBuildozer(args []string) {
	const buildozer = "_bazel/bin/external/com_github_bazelbuild_buildtools/buildozer/buildozer_/buildozer"
	cmd := exec.Command(buildozer, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		var cmderr *exec.ExitError
		// NB: buildozer returns an exit status of 3 if the command was successful
		// but no files were changed.
		if !errors.As(err, &cmderr) || cmderr.ProcessState.ExitCode() != 3 {
			fmt.Printf("failed to run buildozer, got output: %s", string(output))
			panic(err)
		}
	}
}

func getTestTargets(testTargetSize string) ([]string, error) {
	match, _ := regexp.MatchString("\\b(?:small|medium|large|enormous)\\b", testTargetSize)
	if !match {
		return nil, errors.Newf("testTargetSize should be one of {small,medium,large,enormous}. Got '%s'.", testTargetSize)
	}
	cmd := exec.Command(
		"bazel",
		"query",
		fmt.Sprintf(`attr(size, %s, kind("go_test", tests(//pkg/...)))`, testTargetSize),
		"--output=label",
	)
	buf, err := cmd.Output()
	if err != nil {
		log.Printf("Could not query Bazel tests: got error %v", err)
		var cmderr *exec.ExitError
		if errors.As(err, &cmderr) {
			log.Printf("Got error output: %s", string(cmderr.Stderr))
		} else {
			log.Printf("Run `%s` to reproduce the failure", shellescape.QuoteCommand(cmd.Args))
		}
		os.Exit(1)
	}
	return strings.Split(strings.TrimSpace(string(buf[:])), "\n"), nil
}

func main() {
	testSizeToDefaultTimeout := map[string]int{
		"small":    60,
		"medium":   300,
		"large":    900,
		"enormous": 3600,
	}
	for _, size := range []string{"small", "medium", "large", "enormous"} {
		targets, err := getTestTargets(size)
		if err != nil {
			log.Fatal(err)
		}
		// Let the `go test` process timeout 5 seconds before bazel attempts to kill it.
		// Note that if this causes issues such as not having enough time to run normally
		// (because of the 5 seconds taken) then the troubled test target size must be bumped
		// to the next size because it shouldn't be passing at the edge of its deadline
		// anyways to avoid flakiness.
		runBuildozer(append([]string{
			fmt.Sprintf(`set args "-test.timeout=%ds"`, testSizeToDefaultTimeout[size]-5)},
			targets...,
		))
	}
}
