// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"

	"github.com/alessio/shellescape"
)

var (
	testSizeToDefaultTimeout = map[string]int{
		"small":    60,
		"medium":   300,
		"large":    900,
		"enormous": 3600,
	}
	testSizeToCiTimeout = map[string]int{
		"small":    60,
		"medium":   300,
		"large":    900,
		"enormous": 900,
	}
)

func runBuildozer(args []string) {
	var buildozer = os.Args[0] + ".runfiles/../../../../../external/com_github_bazelbuild_buildtools/buildozer/buildozer_/buildozer"
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

// parseQueryXML is used because Go doesn't support parsing XML "1.1".
// It returns a map where the key is a test size {small,medium,large,enormous} and
// the value is a list of test targets having that size.
func parseQueryXML(data []byte) (map[string][]string, error) {
	targetNameRegex, err := regexp.Compile(`<rule.*class="go_test".*name="(.*)".*>`)
	if err != nil {
		return nil, err
	}

	targetSizeRegex, err := regexp.Compile(`<string.*name="size".*value="(.*)".*/>`)
	if err != nil {
		return nil, err
	}

	targetToSize := make(map[string]string)
	var currentTargetName string
	for _, line := range strings.Split(string(data), "\n") {
		// Check if the line contains a target name.
		line = strings.TrimSpace(line)
		if submatch := targetNameRegex.FindStringSubmatch(line); submatch != nil {
			currentTargetName = submatch[1]
			// Default size is medium so if not found then it will be medium.
			targetToSize[currentTargetName] = "medium"
			continue
		}
		// Check if the line contains a target size.
		if submatch := targetSizeRegex.FindStringSubmatch(line); submatch != nil {
			targetToSize[currentTargetName] = submatch[1]
		}
	}
	sizeToTargets := make(map[string][]string)
	for target, size := range targetToSize {
		sizeToTargets[size] = append(sizeToTargets[size], target)
	}
	return sizeToTargets, nil
}

func getTestTargets() (map[string][]string, error) {
	cmd := exec.Command(
		"bazel",
		"query",
		fmt.Sprintf(`kind("go_test", %s)`, getPackagesToQuery()),
		"--output=xml",
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
	return parseQueryXML(buf)
}

func getPackagesToQuery() string {
	// First list all test and binary targets.
	infos, err := os.ReadDir("pkg")
	if err != nil {
		panic(err)
	}
	var packagesToQuery []string
	for _, info := range infos {
		// We don't want to query into pkg/ui because it only contains a
		// single Go test target at its root which will be included below.
		// Querying into its subdirectories is unneeded and causes a pull from `npm`.
		if !info.IsDir() || info.Name() == "ui" {
			continue
		}
		packagesToQuery = append(packagesToQuery, fmt.Sprintf("//pkg/%s/...", info.Name()))
	}
	packagesToQuery = append(packagesToQuery, "//pkg/ui:*")
	return strings.Join(packagesToQuery, "+")
}

func generateTestSuites() {
	cmd := exec.Command(
		"bazel", "query",
		fmt.Sprintf(`kind("((go|sh)_(binary|library|test|transition_binary|transition_test))", %s)`, getPackagesToQuery()),
		"--output=label_kind",
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
	var goLabels, testLabels []string
	for _, line := range strings.Split(string(buf[:]), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		kind := fields[0]
		label := fields[2]
		if kind == "go_library" && !strings.Contains(label, "TxnStateTransition") {
			goLabels = append(goLabels, label)
		} else if kind == "go_test" {
			testLabels = append(testLabels, label)
			goLabels = append(goLabels, label)
		} else if kind == "go_transition_test" {
			goLabels = append(goLabels, label)
		} else if kind == "sh_test" {
			testLabels = append(testLabels, label)
		} else if (kind == "go_binary" || kind == "go_transition_binary") && !strings.HasSuffix(label, "_gomock_prog_bin") && !strings.Contains(label, "TxnStateTransitions") {
			goLabels = append(goLabels, label)
		}
	}
	sort.Strings(goLabels)
	sort.Strings(testLabels)

	f, err := os.Create("pkg/BUILD.bazel")
	if err != nil {
		log.Fatalf("Failed to open file `pkg/BUILD.bazel` - %v", err)
	}
	w := bufio.NewWriter(f)

	fmt.Fprintln(w, `# Code generated by generate-bazel-extra, DO NOT EDIT.
# gazelle:proto_strip_import_prefix /pkg

ALL_TESTS = [`)
	for _, label := range testLabels {
		fmt.Fprintf(w, "    %q,\n", label)
	}
	fmt.Fprintln(w, `]

GO_TARGETS = [`)
	for _, label := range goLabels {
		fmt.Fprintf(w, "    %q,\n", label)
	}
	fmt.Fprintln(w, `]

# These suites run only the tests with the appropriate "size" (excepting those
# tagged "flaky" or "integration") [1]. Note that tests have a default timeout
# depending on the size [2].

# [1] https://docs.bazel.build/versions/master/be/general.html#test_suite
# [2] https://docs.bazel.build/versions/master/be/common-definitions.html#common-attributes-tests`)

	fmt.Fprintln(w, `
test_suite(
    name = "all_tests",
    tags = [
        "-integration",
    ],
    tests = ALL_TESTS,
)`)

	fmt.Fprintln(w, `
test_suite(
    name = "ccl_tests",
    tags = [
        "-integration",
        "ccl_test",
    ],
    tests = ALL_TESTS,
)`)

	for _, size := range []string{"small", "medium", "large", "enormous"} {
		fmt.Fprintf(w, `
test_suite(
    name = "%[1]s_non_ccl_tests",
    tags = [
        "-ccl_test",
        "-flaky",
        "-integration",
        "%[1]s",
    ],
    tests = ALL_TESTS,
)
`, size)
	}

	if err := w.Flush(); err != nil {
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
}

// excludeReallyEnormousTargets removes the really enormous test targets
// from the given list of targets and returns the updated list.
func excludeReallyEnormousTargets(targets []string) []string {
	for i := 0; i < len(targets); i++ {
		var excluded bool
		// Answer the following questions before adding a test target to this list:
		//  1. Does this target run in Bazel Essential CI? If it does and you need
		//     timeout to be > 1 hour then you need to talk to dev-inf. This is not
		//     expected.
		//  2. Are you increasing the timeout for stress-testing purposes in CI? Make
		//     your change in `pkg/cmd/teamcity-trigger` by updating `customTimeouts`.
		//  3. You should only add a test target here if it's for stand-alone testing.
		//     For example: `/pkg/sql/sqlitelogictest` is only tested in a nightly in
		//     `build/teamcity/cockroach/nightlies/sqlite_logic_test_impl.sh`. If this is
		//     the case, you should tag your test as `integration`.
		//  4. If you are not sure, please ask the dev-inf team for help.
		for _, toExclude := range []string{
			"//pkg/ccl/sqlitelogictestccl",
			"//pkg/sql/sqlitelogictest",
			// acceptance is excluded because it's an integration test.
			"//pkg/acceptance",
		} {
			if strings.HasPrefix(targets[i], toExclude) {
				excluded = true
				break
			}
		}
		if !excluded {
			continue
		}
		copy(targets[i:], targets[i+1:])
		targets = targets[:len(targets)-1]
		i--
	}
	return targets
}

func generateTestsTimeouts() {
	targets, err := getTestTargets()
	if err != nil {
		log.Fatal(err)
	}
	for size, defaultTimeout := range testSizeToDefaultTimeout {
		if size == "enormous" {
			runBuildozer(append([]string{`dict_set exec_properties Pool:large`}, targets[size]...))
			// Exclude really enormous targets since they have a custom timeout that
			// exceeds the default 1h.
			targets[size] = excludeReallyEnormousTargets(targets[size])
		}
		// Let the `go test` process timeout 5 seconds before bazel attempts to kill it.
		// Note that if this causes issues such as not having enough time to run normally
		// (because of the 5 seconds taken) then the troubled test target size must be bumped
		// to the next size because it shouldn't be passing at the edge of its deadline
		// anyways to avoid flakiness.
		if size == "enormous" {
			runBuildozer(append([]string{
				fmt.Sprintf(`set_select args //build/toolchains:use_ci_timeouts "-test.timeout=%ds" //conditions:default "-test.timeout=%ds"`, testSizeToCiTimeout[size]-5, defaultTimeout-5)},
				targets[size]...,
			))
		} else {
			runBuildozer(append([]string{
				fmt.Sprintf(`set args "-test.timeout=%ds"`, defaultTimeout-5)},
				targets[size]...,
			))
		}
	}
	var ccl_targets []string
	for _, targetsForSize := range targets {
		for _, target := range targetsForSize {
			if strings.HasPrefix(target, "//pkg/ccl") {
				ccl_targets = append(ccl_targets, target)
			}
		}
	}
	runBuildozer(append([]string{`add tags "ccl_test"`}, ccl_targets...))
}

func main() {
	doTestSuites := flag.Bool("gen_test_suites", false, "generate test suites")
	doTestsTimeouts := flag.Bool("gen_tests_timeouts", false, "generate tests timeouts")
	flag.Parse()
	if *doTestSuites {
		generateTestSuites()
	}
	if *doTestsTimeouts {
		generateTestsTimeouts()
	}
}
