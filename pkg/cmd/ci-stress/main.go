// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
)

// It is a Test if there is a character after Test that is not a lower-case letter.
const goTestStr = `func (Test[^a-z]\w*)\(.*\*testing\.TB?\) {$`

var currentGoTestRE = regexp.MustCompile(`.*` + goTestStr)

// getDiff returns the output of `git diff` from the given baseRef to the
// current `HEAD`.
func getDiff(ctx context.Context, baseRef string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "diff", "--no-ext-diff", fmt.Sprintf("%s..HEAD", baseRef))
	outputBytes, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("unable to get diff: git diff %s..HEAD [...]: %w", baseRef, err)
	}
	return strings.TrimSpace(string(outputBytes)), nil
}

// getPkgToTests parses a git-style diff and returns a mapping from directories
// to affected tests in those directories in the given diff.
func getPkgToTests(diff string) map[string][]string {
	const newFilePrefix = "+++ b/"

	ret := make(map[string][]string)

	var curPkgName string

	for _, line := range strings.Split(diff, "\n") {
		if strings.HasPrefix(line, newFilePrefix) {
			if strings.HasSuffix(line, ".go") {
				curPkgName = filepath.Dir(strings.TrimPrefix(line, newFilePrefix))
			} else {
				curPkgName = ""
			}
		} else if currentGoTestRE.MatchString(line) && curPkgName != "" {
			curTestName := ""
			if !strings.HasPrefix(line, "-") {
				curTestName = currentGoTestRE.ReplaceAllString(line, "$1")
			}
			if curPkgName != "" && curTestName != "" {
				ret[curPkgName] = append(ret[curPkgName], curTestName)
			}
		}
	}

	// Sanity-check: Make sure there is a `BUILD.bazel` file in each pkg,
	// or else it's not a real Go test. (Could be testdata for a different
	// package, etc.) Don't do this in test builds however, as we would
	// never find those files.
	//
	// We also take the opportunity to limit stressing to a constant
	// number of packages in case this PR changed a ton of packages
	// (find-and-replace, bulk changes, etc.)
	if !buildutil.CrdbTestBuild {
		taken := 0
		for pkg := range ret {
			_, err := os.Stat(filepath.Join(pkg, "BUILD.bazel"))
			if taken >= 10 {
				delete(ret, pkg)
				continue
			}
			if err != nil && errors.Is(err, os.ErrNotExist) {
				fmt.Printf("skipping testing package %s as we could not find a BUILD.bazel file in that directory\n", pkg)
				delete(ret, pkg)
				continue
			} else if err != nil {
				panic(err)
			}
			taken += 1
		}
	}

	for _, tests := range ret {
		slices.Sort(tests)
	}
	// De-duplicate.
	for pkg := range ret {
		ret[pkg] = slices.Compact(ret[pkg])
	}

	// We arbitrarily limit the number of tests per package. We randomize
	// the tests selected.
	for pkg := range ret {
		const maxTests = 10
		tests := ret[pkg]
		if len(tests) > maxTests {
			rand.Shuffle(len(tests), func(i, j int) {
				tests[i], tests[j] = tests[j], tests[i]
			})
			tests = tests[:maxTests]
			slices.Sort(tests)
			ret[pkg] = tests
		}
	}

	return ret
}

func runTests(ctx context.Context, pkgToTests map[string][]string, extraBazelArgs []string) error {
	var testPackages []string
	for pkg := range pkgToTests {
		testPackages = append(testPackages, fmt.Sprintf("//%s:%s_test", pkg, filepath.Base(pkg)))
	}
	allTests := make(map[string]struct{})
	for _, tests := range pkgToTests {
		for _, test := range tests {
			allTests[test] = struct{}{}
		}
	}
	allTestsSlice := make([]string, 0, len(allTests))
	for test := range allTests {
		allTestsSlice = append(allTestsSlice, test)
	}
	slices.Sort(allTestsSlice)
	testFilter := strings.Join(allTestsSlice, "|")
	testFilter = "^(" + testFilter + ")$"
	// Run each test multiple times.
	bazelArgs := []string{"test", "--test_filter", testFilter, "--runs_per_test", "10"}
	bazelArgs = append(bazelArgs, testPackages...)
	bazelArgs = append(bazelArgs, extraBazelArgs...)
	fmt.Printf("running `bazel` with args %+v\n", bazelArgs)
	cmd := exec.CommandContext(ctx, "bazel", bazelArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func mainImpl(baseRef string, bazelArgs []string) error {
	ctx := context.Background()
	diff, err := getDiff(ctx, baseRef)
	if err != nil {
		return err
	}
	pkgToTests := getPkgToTests(diff)
	if len(pkgToTests) == 0 {
		fmt.Println("could not find any eligible tests to stress, exiting")
		return nil
	}
	return runTests(ctx, pkgToTests, bazelArgs)
}

func main() {
	if len(os.Args) == 1 {
		panic("expected at least one argument (the ref of the base brach)")
	}
	baseRef := os.Args[1]
	bazelArgs := os.Args[2:]
	if err := mainImpl(baseRef, bazelArgs); err != nil {
		panic(err)
	}
}
