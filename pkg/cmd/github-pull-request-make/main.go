// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This utility detects new tests added in a given pull request, and runs them
// under stress in our CI infrastructure.
//
// Note that this program will directly invoke the build system, so there is no
// need to process its output. See build/teamcity-support.sh for usage examples.
//
// Note that our CI infrastructure has no notion of "pull requests", forcing
// the approach taken here be quite brute-force with respect to its use of the
// GitHub API.
package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/errors"
)

const (
	githubAPITokenEnv    = "GITHUB_API_TOKEN"
	teamcityVCSNumberEnv = "BUILD_VCS_NUMBER"
	targetEnv            = "TARGET"
	// The following environment variables are for testing and are
	// prefixed with GHM_ to help prevent accidentally triggering
	// test code inside the CI pipeline.
	packageEnv    = "GHM_PACKAGES"
	forceBazelEnv = "GHM_FORCE_BAZEL"
)

// https://github.com/golang/go/blob/go1.7.3/src/cmd/go/test.go#L1260:L1262
//
// It is a Test (say) if there is a character after Test that is not a lower-case letter.
// We don't want TesticularCancer.
const goTestStr = `func (Test[^a-z]\w*)\(.*\*testing\.TB?\) {$`

const bazelStressTarget = "@com_github_cockroachdb_stress//:stress"

var currentGoTestRE = regexp.MustCompile(`.*` + goTestStr)
var dontStressTests = []*regexp.Regexp{
	regexp.MustCompile("^TestDataDriven$"),
	regexp.MustCompile("^TestLogic_"),
}

type pkg struct {
	tests map[string]struct{}
}

func makePkg(tests []string) pkg {
	newPkg := pkg{
		tests: map[string]struct{}{},
	}
	for _, test := range tests {
		newPkg.maybeAddTest(test)
	}
	return newPkg
}

func (p *pkg) maybeAddTest(testName string) {
	if len(p.tests) == 0 {
		p.tests = map[string]struct{}{}
	}
	if _, ok := p.tests[testName]; !ok {
		p.tests[testName] = struct{}{}
	}
}

func (p *pkg) export() []string {
	tests := make([]string, 0, len(p.tests))
	for test := range p.tests {
		tests = append(tests, test)
	}
	return tests
}

func pkgsForSHA(ctx context.Context, sha string) (map[string]pkg, error) {
	diff, err := getDiff(ctx, sha)
	if err != nil {
		return nil, err
	}

	return pkgsFromDiff(strings.NewReader(diff))
}

// pkgsFromDiff parses a git-style diff and returns a mapping from directories
// to tests added in those directories in the given diff.
func pkgsFromDiff(r io.Reader) (map[string]pkg, error) {
	const newFilePrefix = "+++ b/"
	const goFileSuffix = ".go"
	const replacement = "$1"

	pkgs := make(map[string]pkg)

	var curPkgName string
	var inPrefix bool

	// We only stress tests in go test files.
	var isGoPackage bool

	for reader := bufio.NewReader(r); ; {
		line, isPrefix, err := reader.ReadLine()
		switch {
		case err == nil:
		case err == io.EOF:
			return chooseFiveTestsPerPackage(pkgs), nil
		default:
			return nil, err
		}
		// Ignore generated files a la embedded.go.
		if isPrefix {
			inPrefix = true
			continue
		} else if inPrefix {
			inPrefix = false
			continue
		}

		switch {
		case bytes.HasPrefix(line, []byte(newFilePrefix)):
			curPkgName = filepath.Dir(string(bytes.TrimPrefix(line, []byte(newFilePrefix))))
			isGoPackage = bytes.HasSuffix(line, []byte(goFileSuffix))
		case currentGoTestRE.Match(line) && isGoPackage:
			curTestName := ""
			if !bytes.HasPrefix(line, []byte{'-'}) {
				curTestName = string(currentGoTestRE.ReplaceAll(line, []byte(replacement)))
			}
			if curPkgName != "" && len(curTestName) > 0 && okToStress(curTestName) {
				curPkg := pkgs[curPkgName]
				curPkg.maybeAddTest(curTestName)
				pkgs[curPkgName] = curPkg
			}
		}
	}
}

func chooseFiveTestsPerPackage(pkgs map[string]pkg) map[string]pkg {

	scrambleTestOrder := func(pkgTestNames pkg) []string {
		testNames := make([]string, 0, len(pkgTestNames.tests))

		for testName := range pkgTestNames.tests {
			testNames = append(testNames, testName)
		}

		for i := range testNames {
			j := rand.Intn(i + 1)
			testNames[i], testNames[j] = testNames[j], testNames[i]
		}
		return testNames
	}
	croppedPkgs := make(map[string]pkg)
	for pkgName, tests := range pkgs {
		randomOrderTests := scrambleTestOrder(tests)
		cropIdx := 4
		if len(randomOrderTests) < cropIdx {
			cropIdx = len(randomOrderTests)
		}
		croppedPkgs[pkgName] = makePkg(randomOrderTests[:cropIdx])
	}
	return croppedPkgs
}

func okToStress(testName string) bool {
	for _, dontStress := range dontStressTests {
		if dontStress.MatchString(testName) {
			return false
		}
	}
	return true
}

func getDiff(ctx context.Context, sha string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "merge-base", "origin/master", sha)
	baseShaBytes, err := cmd.Output()
	if err != nil {
		return "", err
	}
	baseSha := strings.TrimSpace(string(baseShaBytes))
	cmd = exec.CommandContext(ctx, "git", "diff", "--no-ext-diff", baseSha, sha, "--", ":!pkg/acceptance/compose/**")
	outputBytes, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(outputBytes)), nil
}

func parsePackagesFromEnvironment(input string) (map[string]pkg, error) {
	const expectedFormat = "PACKAGE_NAME=TEST_NAME[,TEST_NAME...][;PACKAGE_NAME=...]"
	pkgTestStrs := strings.Split(input, ";")
	pkgs := make(map[string]pkg, len(pkgTestStrs))
	for _, pts := range pkgTestStrs {
		ptsParts := strings.Split(pts, "=")
		if len(ptsParts) < 2 {
			return nil, fmt.Errorf("invalid format for package environment variable: %q (expected format: %s)",
				input, expectedFormat)
		}
		pkgName := ptsParts[0]
		tests := ptsParts[1]
		pkgs[pkgName] = makePkg(strings.Split(tests, ","))
	}
	return pkgs, nil
}

func main() {
	sha, ok := os.LookupEnv(teamcityVCSNumberEnv)
	if !ok {
		log.Fatalf("VCS number environment variable %s is not set", teamcityVCSNumberEnv)
	}

	target, ok := os.LookupEnv(targetEnv)
	if !ok {
		log.Fatalf("target variable %s is not set", targetEnv)
	}
	if target != "stress" && target != "stressrace" {
		log.Fatalf("environment variable %s is %s; expected 'stress' or 'stressrace'", targetEnv, target)
	}

	forceBazel := false
	if forceBazelStr, ok := os.LookupEnv(forceBazelEnv); ok {
		forceBazel, _ = strconv.ParseBool(forceBazelStr)
	}
	var bazciPath string
	if bazel.BuiltWithBazel() || forceBazel {
		// NB: bazci is expected to be put in `PATH` by the caller.
		var err error
		bazciPath, err = exec.LookPath("bazci")
		if err != nil {
			log.Fatal(err)
		}
	}

	crdb, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	var pkgs map[string]pkg
	if pkgStr, ok := os.LookupEnv(packageEnv); ok {
		log.Printf("Using packages from environment variable %s", packageEnv)
		pkgs, err = parsePackagesFromEnvironment(pkgStr)
		if err != nil {
			log.Fatal(err)
		}

	} else {
		ctx := context.Background()
		pkgs, err = pkgsForSHA(ctx, sha)
		if err != nil {
			log.Fatal(err)
		}
	}

	if len(pkgs) > 0 {
		for name, pkg := range pkgs {
			// 20 minutes total seems OK, but at least 2 minutes per test.
			// This should be reduced. See #46941.
			target, ok := os.LookupEnv(targetEnv)
			var duration time.Duration
			if ok && target == "stressrace" {
				duration = (30 * time.Minute) / time.Duration(len(pkgs))
			} else {
				duration = (20 * time.Minute) / time.Duration(len(pkgs))
			}
			minDuration := (2 * time.Minute) * time.Duration(len(pkg.tests))
			if duration < minDuration {
				duration = minDuration
			}
			// Use a timeout shorter than the duration so that hanging tests don't
			// get a free pass.
			timeout := (9 * duration) / 10

			// The stress -p flag defaults to the number of CPUs, which is too
			// aggressive on big machines and can cause tests to fail. Under nightly
			// stress, we usually use 4 or 2, so run with 8 here to make sure the
			// test becomes an obvious candidate for skipping under race before it
			// has to deal with the nightlies.
			parallelism := 16
			if target == "stressrace" {
				parallelism = 8
			}

			var args []string
			if bazel.BuiltWithBazel() || forceBazel {
				args = append(args, "test")

				// NB: We use a pretty dumb technique to list the bazel test
				// targets: we ask bazel query to enumerate all the tests in this
				// package. bazel queries can take a second or so to run, so it's
				// conceivable that the delay introduced by this could be
				// noticeable. For packages that have two or more test targets, the
				// test filters should mean that we don't execute more tests than
				// we need to. This should be refactored to improve performance and
				// to strip out the unnecessary calls to `bazel`, but that might
				// better be saved for when we no longer need `make` support and
				// don't have to worry about accidentally breaking it.
				out, err := exec.Command(
					"bazel",
					"query",
					fmt.Sprintf("kind(go_test, //%s:all) except attr(tags, \"integration\", //%s:all)", name, name),
					"--output=label").Output()
				if err != nil {
					var stderr []byte
					if exitErr := (*exec.ExitError)(nil); errors.As(err, &exitErr) {
						stderr = exitErr.Stderr
					}
					fmt.Printf("bazel query over pkg %s failed; got stdout %s, stderr %s\n", name, string(out), string(stderr))
					log.Fatal(err)
				}

				numTargets := 0
				for _, target := range strings.Split(string(out), "\n") {
					target = strings.TrimSpace(target)
					if target != "" {
						args = append(args, target)
						numTargets++
					}
				}
				if numTargets == 0 {
					// In this case there's nothing to test, so we can bail out early.
					log.Printf("found no targets to test under package %s\n", name)
					continue
				}
				args = append(args, "--")
				if target == "stressrace" {
					args = append(args, "--config=race")
				}
				args = append(args, "--test_sharding_strategy=disabled")
				var filters []string
				for test := range pkg.tests {
					filters = append(filters, "^"+test+"$")
				}
				args = append(args, fmt.Sprintf("--test_filter=%s", strings.Join(filters, "|")))
				args = append(args, "--test_env=COCKROACH_NIGHTLY_STRESS=true")
				// Give the entire test 1 more minute than the duration to wrap up.
				args = append(args, fmt.Sprintf("--test_timeout=%d", int((duration+1*time.Minute).Seconds())))
				args = append(args, "--test_output", "streamed")

				args = append(args, "--run_under", fmt.Sprintf("%s -bazel -shardable-artifacts 'XML_OUTPUT_FILE=%s merge-test-xmls' -stderr -maxfails 1 -maxtime %s -p %d", bazelStressTarget, bazciPath, duration, parallelism))
				args = append(args, "--test_arg", "-test.timeout", "--test_arg", timeout.String())
				cmd := exec.Command("bazci", args...)
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				log.Println(cmd.Args)
				if err := cmd.Run(); err != nil {
					log.Fatal(err)
				}
			} else {
				tests := "-"
				if len(pkg.tests) > 0 {
					tests = "(" + strings.Join(pkg.export(), "$$|") + "$$)"
				}

				args = append(
					args,
					target,
					fmt.Sprintf("PKG=./%s", name),
					fmt.Sprintf("TESTS=%s", tests),
					fmt.Sprintf("TESTTIMEOUT=%s", timeout),
					"GOTESTFLAGS=-json", // allow TeamCity to parse failures
					fmt.Sprintf("STRESSFLAGS=-stderr -maxfails 1 -maxtime %s -p %d", duration, parallelism),
				)
				cmd := exec.Command("make", args...)
				cmd.Env = append(os.Environ(), "COCKROACH_NIGHTLY_STRESS=true")
				cmd.Dir = crdb
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				log.Println(cmd.Args)
				if err := cmd.Run(); err != nil {
					log.Fatal(err)
				}
			}
		}
	}
}
