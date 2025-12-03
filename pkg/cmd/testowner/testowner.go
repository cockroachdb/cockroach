package main

// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// testowner is a helper binary to get the owner of a given test.
// Before invoking this binary, you should first generate Go code: `dev gen go`
// This is because tests can be found in generated files that need to exist in
// the workspace. Further, this should be run from the root of the workspace.
//
// The binary can be invoked in one of two ways:
// * single test lookup: `testowner PKG TEST`. The owner(s) for the test will be
//   printed to stdout, separated by a space.
// * multi-test lookup: `testowner` (with no arguments). In this case, stdin
//   will be a list of test lookups, one on each line, of the form `PKG TEST`.
//   the output will be one owner per line.

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/codeowners"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
)

func main() {
	co, err := codeowners.DefaultLoadCodeOwners()
	if err != nil {
		panic(err)
	}
	stdout := bufio.NewWriter(os.Stdout)
	stderr := bufio.NewWriter(os.Stderr)
	if len(os.Args) == 1 {
		stdin := bufio.NewScanner(os.Stdin)
		for stdin.Scan() {
			line := stdin.Text()
			pkg, test := parseQuery(line)
			teams, logs := co.GetTestOwner(pkg, test)
			writeOwners(stdout, teams)
			for _, log := range logs {
				fmt.Fprintln(stderr, log)
			}
		}
		if err := stdin.Err(); err != nil {
			panic(err)
		}
	} else if len(os.Args) == 2 {
		pkg, test := parseQuery(os.Args[1])
		teams, logs := co.GetTestOwner(pkg, test)
		writeOwners(stdout, teams)
		for _, log := range logs {
			fmt.Fprintln(stderr, log)
		}
	} else {
		fmt.Fprintf(stderr, "usage: `testowner PKG.TEST` or `testowner` (with queries in stdin); got %d arguments, expected 1-2", len(os.Args))
	}
	if err := stdout.Flush(); err != nil {
		panic(err)
	}
	if err := stderr.Flush(); err != nil {
		panic(err)
	}
}

func parseQuery(pkgAndTest string) (pkg string, test string) {
	const githubPrefix = "github.com/cockroachdb/cockroach/"
	var dotIdx int
	if strings.HasPrefix(pkgAndTest, githubPrefix) {
		dotIdx = strings.IndexByte(pkgAndTest[len(githubPrefix):], '.')
		if dotIdx <= 0 {
			panic(fmt.Sprintf("could not parse query (expected PKG.TEST): %s", pkgAndTest))
		}
		dotIdx += len(githubPrefix)
	} else {
		dotIdx = strings.IndexByte(pkgAndTest, '.')
		if dotIdx <= 0 {
			panic(fmt.Sprintf("could not parse query (expected PKG.TEST): %s", pkgAndTest))
		}
	}
	pkg, test = pkgAndTest[:dotIdx], pkgAndTest[dotIdx+1:]
	return
}

func writeOwners(w io.Writer, teams []team.Team) {
	var prev bool
	if len(teams) == 0 {
		panic("empty team slice")
	}
	for _, team := range teams {
		if prev {
			_, err := w.Write([]byte{' '})
			if err != nil {
				panic(err)
			}
		}
		fmt.Fprintf(w, "%s", team.TeamName)
		prev = true
	}
	_, err := w.Write([]byte{'\n'})
	if err != nil {
		panic(err)
	}
}
