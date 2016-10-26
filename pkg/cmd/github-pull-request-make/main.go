// Copyright 2016 The Cockroach Authors.
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
// permissions and limitations under the License.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package main

import (
	"bufio"
	"bytes"
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

	"golang.org/x/oauth2"

	"github.com/google/go-github/github"
)

const githubAPITokenEnv = "GITHUB_API_TOKEN"
const teamcityVCSNumberEnv = "BUILD_VCS_NUMBER"
const makeTargetEnv = "TARGET"

// https://github.com/golang/go/blob/go1.7.3/src/cmd/go/test.go#L1260:L1262
//
// It is a Test (say) if there is a character after Test that is not a lower-case letter.
// We don't want TesticularCancer.
var newGoTestRE = regexp.MustCompile(`^\+\s*func (Test[^a-z]\w*)\(.*\*testing\.TB?\) {$`)
var newGoBenchmarkRE = regexp.MustCompile(`^\+\s*func (Benchmark[^a-z]\w*)\(.*\*testing\.T?B\) {$`)

type pkg struct {
	tests, benchmarks []string
}

func parseDiff(r io.Reader) (map[string]pkg, error) {
	const newFilePrefix = "+++ b/"
	const replacement = "$1"

	pkgs := make(map[string]pkg)

	var curPkg string
	var inPrefix bool
	for reader := bufio.NewReader(r); ; {
		line, isPrefix, err := reader.ReadLine()
		switch err {
		case nil:
		case io.EOF:
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
			curPkg = filepath.Dir(string(bytes.TrimPrefix(line, []byte(newFilePrefix))))
		case newGoTestRE.Match(line):
			pkg := pkgs[curPkg]
			pkg.tests = append(pkg.tests, string(newGoTestRE.ReplaceAll(line, []byte(replacement))))
			pkgs[curPkg] = pkg
		case newGoBenchmarkRE.Match(line):
			pkg := pkgs[curPkg]
			pkg.benchmarks = append(pkg.benchmarks, string(newGoBenchmarkRE.ReplaceAll(line, []byte(replacement))))
			pkgs[curPkg] = pkg
		}
	}
}

func main() {
	token, ok := os.LookupEnv(githubAPITokenEnv)
	if !ok {
		log.Fatalf("GitHub API token environment variable %s is not set", githubAPITokenEnv)
	}

	sha, ok := os.LookupEnv(teamcityVCSNumberEnv)
	if !ok {
		log.Fatalf("VCS number environment variable %s is not set", teamcityVCSNumberEnv)
	}

	target, ok := os.LookupEnv(makeTargetEnv)
	if !ok {
		log.Fatalf("make target variable %s is not set", makeTargetEnv)
	}

	client := github.NewClient(oauth2.NewClient(oauth2.NoContext, oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token},
	)))

	crdb, err := build.Import("github.com/cockroachdb/cockroach", "", build.FindOnly)
	if err != nil {
		log.Fatal(err)
	}

	pulls, _, err := client.PullRequests.List("cockroachdb", "cockroach", nil)
	if err != nil {
		log.Fatal(err)
	}
	var currentPull *github.PullRequest
	for _, pull := range pulls {
		if *pull.Head.SHA == sha {
			currentPull = pull
			break
		}
	}

	resp, err := http.Get(*currentPull.DiffURL)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("http.Get(%s).Status = %s", *currentPull.DiffURL, resp.Status)
	}

	pkgs, err := parseDiff(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	for name, pkg := range pkgs {
		tests := "-"
		if len(pkg.tests) > 0 {
			tests = "(" + strings.Join(pkg.tests, "|") + ")"
		}
		benchmarks := "-"
		if len(pkg.benchmarks) > 0 {
			benchmarks = "(" + strings.Join(pkg.benchmarks, "|") + ")"
		}

		cmd := exec.Command(
			"make",
			target,
			fmt.Sprintf("PKG=./%s", name),
			fmt.Sprintf("TESTS=%s", tests),
			fmt.Sprintf("TESTFLAGS=-test.bench %s", benchmarks),
			"STRESSFLAGS=-stderr -maxfails 1 -maxtime 5m",
		)
		cmd.Dir = crdb.Dir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		log.Println(cmd.Args)
		if err := cmd.Run(); err != nil {
			log.Fatal(err)
		}
	}
}
