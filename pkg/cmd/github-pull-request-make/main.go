// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This utility detects new tests added in a given pull request, and runs them
// under stress in our CI infrastructure.
//
// Note that this program will directly exec `make`, so there is no need to
// process its output. See build/teamcity-test{,race}.sh for usage examples.
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
	"go/build"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	_ "github.com/cockroachdb/cockroach/pkg/testutils/buildutil"
	"github.com/google/go-github/github"
	"golang.org/x/oauth2"
)

const githubAPITokenEnv = "GITHUB_API_TOKEN"
const teamcityVCSNumberEnv = "BUILD_VCS_NUMBER"
const makeTargetEnv = "TARGET"

// https://github.com/golang/go/blob/go1.7.3/src/cmd/go/test.go#L1260:L1262
//
// It is a Test (say) if there is a character after Test that is not a lower-case letter.
// We don't want TesticularCancer.
const goTestStr = `func (Test[^a-z]\w*)\(.*\*testing\.TB?\) {$`
const goBenchmarkStr = `func (Benchmark[^a-z]\w*)\(.*\*testing\.T?B\) {$`

var currentGoTestRE = regexp.MustCompile(`.*` + goTestStr)
var currentGoBenchmarkRE = regexp.MustCompile(`.*` + goBenchmarkStr)
var newGoTestRE = regexp.MustCompile(`^\+\s*` + goTestStr)
var newGoBenchmarkRE = regexp.MustCompile(`^\+\s*` + goBenchmarkStr)

type pkg struct {
	tests, benchmarks []string
}

// pkgsFromDiff parses a git-style diff and returns a mapping from directories
// to tests and benchmarks added in those directories in the given diff.
func pkgsFromDiff(r io.Reader) (map[string]pkg, error) {
	const newFilePrefix = "+++ b/"
	const replacement = "$1"

	pkgs := make(map[string]pkg)

	var curPkgName string
	var curTestName string
	var curBenchmarkName string
	var inPrefix bool
	for reader := bufio.NewReader(r); ; {
		line, isPrefix, err := reader.ReadLine()
		switch {
		case err == nil:
		case err == io.EOF:
			return pkgs, nil
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
		case newGoTestRE.Match(line):
			curPkg := pkgs[curPkgName]
			curPkg.tests = append(curPkg.tests, string(newGoTestRE.ReplaceAll(line, []byte(replacement))))
			pkgs[curPkgName] = curPkg
		case newGoBenchmarkRE.Match(line):
			curPkg := pkgs[curPkgName]
			curPkg.benchmarks = append(curPkg.benchmarks, string(newGoBenchmarkRE.ReplaceAll(line, []byte(replacement))))
			pkgs[curPkgName] = curPkg
		case currentGoTestRE.Match(line):
			curTestName = ""
			curBenchmarkName = ""
			if !bytes.HasPrefix(line, []byte{'-'}) {
				curTestName = string(currentGoTestRE.ReplaceAll(line, []byte(replacement)))
			}
		case currentGoBenchmarkRE.Match(line):
			curTestName = ""
			curBenchmarkName = ""
			if !bytes.HasPrefix(line, []byte{'-'}) {
				curBenchmarkName = string(currentGoBenchmarkRE.ReplaceAll(line, []byte(replacement)))
			}
		case bytes.HasPrefix(line, []byte{'-'}) && bytes.Contains(line, []byte(".Skip")):
			if curPkgName != "" {
				switch {
				case len(curTestName) > 0:
					if !(curPkgName == "build" && curTestName == "TestStyle") {
						curPkg := pkgs[curPkgName]
						curPkg.tests = append(curPkg.tests, curTestName)
						pkgs[curPkgName] = curPkg
					}
				case len(curBenchmarkName) > 0:
					curPkg := pkgs[curPkgName]
					curPkg.benchmarks = append(curPkg.benchmarks, curBenchmarkName)
					pkgs[curPkgName] = curPkg
				}
			}
		}
	}
}

func findPullRequest(
	ctx context.Context, client *github.Client, org, repo, sha string,
) *github.PullRequest {
	opts := &github.PullRequestListOptions{
		ListOptions: github.ListOptions{PerPage: 100},
	}
	for {
		pulls, resp, err := client.PullRequests.List(ctx, org, repo, opts)
		if err != nil {
			log.Fatal(err)
		}

		for _, pull := range pulls {
			if *pull.Head.SHA == sha {
				return pull
			}
		}

		if resp.NextPage == 0 {
			return nil
		}
		opts.Page = resp.NextPage
	}
}

func ghClient(ctx context.Context) *github.Client {
	var httpClient *http.Client
	if token, ok := os.LookupEnv(githubAPITokenEnv); ok {
		httpClient = oauth2.NewClient(ctx, oauth2.StaticTokenSource(
			&oauth2.Token{AccessToken: token},
		))
	} else {
		log.Printf("GitHub API token environment variable %s is not set", githubAPITokenEnv)
	}
	return github.NewClient(httpClient)
}

func getDiff(
	ctx context.Context, client *github.Client, org, repo string, prNum int,
) (string, error) {
	diff, _, err := client.PullRequests.GetRaw(
		ctx,
		org,
		repo,
		prNum,
		github.RawOptions{Type: github.Diff},
	)
	return diff, err
}

func main() {
	sha, ok := os.LookupEnv(teamcityVCSNumberEnv)
	if !ok {
		log.Fatalf("VCS number environment variable %s is not set", teamcityVCSNumberEnv)
	}

	target, ok := os.LookupEnv(makeTargetEnv)
	if !ok {
		log.Fatalf("make target variable %s is not set", makeTargetEnv)
	}

	const org = "cockroachdb"
	const repo = "cockroach"

	crdb, err := build.Import(fmt.Sprintf("github.com/%s/%s", org, repo), "", build.FindOnly)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	client := ghClient(ctx)

	currentPull := findPullRequest(ctx, client, org, repo, sha)
	if currentPull == nil {
		log.Printf("SHA %s not found in open pull requests, skipping stress", sha)
		return
	}

	diff, err := getDiff(ctx, client, org, repo, *currentPull.Number)
	if err != nil {
		log.Fatal(err)
	}

	if target == "checkdeps" {
		var vendorChanged bool
		for _, path := range []string{"Gopkg.lock", "vendor"} {
			if strings.Contains(diff, fmt.Sprintf("\n--- a/%[1]s\n+++ b/%[1]s\n", path)) {
				vendorChanged = true
				break
			}
		}
		if vendorChanged {
			cmd := exec.Command("dep", "ensure", "-v")
			cmd.Dir = crdb.Dir
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			log.Println(cmd.Args)
			if err := cmd.Run(); err != nil {
				log.Fatal(err)
			}

			// Check for diffs.
			var foundDiff bool
			for _, dir := range []string{filepath.Join(crdb.Dir, "vendor"), crdb.Dir} {
				cmd := exec.Command("git", "diff")
				cmd.Dir = dir
				log.Println(cmd.Dir, cmd.Args)
				if output, err := cmd.CombinedOutput(); err != nil {
					log.Fatalf("%s: %s", err, string(output))
				} else if len(output) > 0 {
					foundDiff = true
					log.Printf("unexpected diff:\n%s", output)
				}
			}
			if foundDiff {
				os.Exit(1)
			}
		}
	} else {
		pkgs, err := pkgsFromDiff(strings.NewReader(diff))
		if err != nil {
			log.Fatal(err)
		}
		if len(pkgs) > 0 {
			for name, pkg := range pkgs {
				// 20 minutes total seems OK, but at least 2 minutes per test.
				// This should be reduced. See #46941.
				duration := (20 * time.Minute) / time.Duration(len(pkgs))
				minDuration := (2 * time.Minute) * time.Duration(len(pkg.tests))
				if duration < minDuration {
					duration = minDuration
				}
				// Use a timeout shorter than the duration so that hanging tests don't
				// get a free pass.
				timeout := (3 * duration) / 4

				tests := "-"
				if len(pkg.tests) > 0 {
					tests = "(" + strings.Join(pkg.tests, "$$|") + "$$)"
				}

				// The stress -p flag defaults to the number of CPUs, which is too
				// aggressive on big machines and can cause tests to fail. Under nightly
				// stress, we usually use 4 or 2, so run with 8 here to make sure the
				// test becomes an obvious candidate for skipping under race before it
				// has to deal with the nightlies.
				parallelism := 16
				if target == "stressrace" {
					parallelism = 8
				}

				cmd := exec.Command(
					"make",
					target,
					fmt.Sprintf("PKG=./%s", name),
					fmt.Sprintf("TESTS=%s", tests),
					fmt.Sprintf("TESTTIMEOUT=%s", timeout),
					"GOTESTFLAGS=-json", // allow TeamCity to parse failures
					fmt.Sprintf("STRESSFLAGS=-stderr -maxfails 1 -maxtime %s -p %d", duration, parallelism),
				)
				cmd.Env = append(os.Environ(), "COCKROACH_NIGHTLY_STRESS=true")
				cmd.Dir = crdb.Dir
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
