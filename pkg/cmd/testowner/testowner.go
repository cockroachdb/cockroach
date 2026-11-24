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
// * single test lookup: `testowner PKG.TEST`. The owner(s) for the test will be
//   printed to stdout, separated by a space.
// * multi-test lookup: `testowner` (with no arguments). In this case, stdin
//   will be a list of test lookups, one on each line, of the form `PKG.TEST`.
//   the output will be one owner per line.
// You can additionally pass in a command-line flag -roachtest. If true, the
// test lookup will performed as if the test were a roachtest rather than a Go
// test. In this case, the input should be passed in as the test name alone
// instead of PKG.TEST.
//
// Examples:
//   testowner github.com/cockroachdb/cockroach/pkg/cmd/dev.TestDataDriven
//   testowner pkg/cmd/dev.TestDataDriven
//   testowner -roachtest acceptance/build-analyze

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/tests"
	"github.com/cockroachdb/cockroach/pkg/internal/codeowners"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type registryImpl struct {
	nameToOwner map[string]registry.Owner
}

func (r *registryImpl) MakeClusterSpec(nodeCount int, opts ...spec.Option) spec.ClusterSpec {
	return spec.ClusterSpec{}
}

func (r *registryImpl) Add(spec registry.TestSpec) {
	name, owner := spec.Name, spec.Owner
	_, ok := r.nameToOwner[name]
	if ok {
		panic(fmt.Sprintf("duplicate test of name %s", name))
	}
	r.nameToOwner[name] = owner
}

func (r *registryImpl) AddOperation(registry.OperationSpec) {}

func (r *registryImpl) PromFactory() promauto.Factory {
	return promauto.With(nil)
}

var (
	roachtest                   = flag.Bool("roachtest", false, "whether to look up the test as a roachtest")
	_         registry.Registry = (*registryImpl)(nil)
)

func mainImpl() int {
	flag.Parse()
	stdout := bufio.NewWriter(os.Stdout)
	stderr := bufio.NewWriter(os.Stderr)
	defer func() {
		_ = stdout.Flush()
	}()
	defer func() {
		_ = stderr.Flush()
	}()

	if *roachtest {
		reg := registryImpl{
			nameToOwner: make(map[string]registry.Owner),
		}
		tests.RegisterTests(&reg)
		if len(flag.Args()) == 0 {
			stdin := bufio.NewScanner(os.Stdin)
			for stdin.Scan() {
				line := stdin.Text()
				owner, ok := reg.nameToOwner[line]
				if !ok {
					panic(fmt.Sprintf("unknown roachtest of name %s", line))
				}
				_, err := stdout.WriteString(string(owner.ToTeamAlias()))
				if err != nil {
					panic(err)
				}
				err = stdout.WriteByte('\n')
				if err != nil {
					panic(err)
				}
			}
			if err := stdin.Err(); err != nil {
				panic(err)
			}

		} else if len(flag.Args()) == 1 {
			owner, ok := reg.nameToOwner[flag.Args()[0]]
			if !ok {
				panic(fmt.Sprintf("unknown roachtest of name %s", flag.Args()[0]))
			}
			_, err := stdout.WriteString(string(owner.ToTeamAlias()))
			if err != nil {
				panic(err)
			}
			err = stdout.WriteByte('\n')
			if err != nil {
				panic(err)
			}
		} else {
			fmt.Fprintf(stderr, "usage: `testowner TEST` or `testowner` (with queries in stdin); got %d arguments, expected 0-1\n", len(flag.Args()))
			return 1
		}
	} else {
		co, err := codeowners.DefaultLoadCodeOwners()
		if err != nil {
			panic(err)
		}

		if len(flag.Args()) == 0 {
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
		} else if len(flag.Args()) == 1 {
			pkg, test := parseQuery(flag.Args()[0])
			teams, logs := co.GetTestOwner(pkg, test)
			writeOwners(stdout, teams)
			for _, log := range logs {
				fmt.Fprintln(stderr, log)
			}
		} else {
			fmt.Fprintf(stderr, "usage: `testowner PKG.TEST` or `testowner` (with queries in stdin); got %d arguments, expected 0-1\n", len(flag.Args()))
			return 1
		}
	}
	return 0
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

func main() {
	os.Exit(mainImpl())
}
