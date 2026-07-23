// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Command genbzl is used to generate bazel files which then get imported by
// the gen package's BUILD.bazel to facilitate hoisting these generated files
// back into the source tree.
//
// The flow is that we invoke this binary inside the
// bazelutil/bazel-generate.sh script which writes out some bzl files
// defining lists of targets for generation and hoisting.
//
// The program assumes it will be run from the root of the cockroach workspace.
// If it's not, errors will occur.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var (
	outDir = flag.String(
		"out-dir", "",
		"directory in which to place the generated files",
	)
)

func main() {
	flag.Parse()
	if err := generate(*outDir); err != nil {
		fmt.Fprintf(os.Stderr, "failed to generate files: %v\n", err)
		os.Exit(1)
	}
}

func generate(outDir string) error {
	qd, err := getQueryData()
	if err != nil {
		return err
	}
	for _, q := range targets {
		if q.doNotGenerate {
			continue
		}
		var out []string
		if len(q.hardCodedQueryResults) == 0 {
			var err error
			out, err = q.target.execQuery(qd)
			if err != nil {
				return err
			}
		} else {
			out = q.hardCodedQueryResults
		}
		if err := q.target.write(outDir, out); err != nil {
			return err
		}
	}
	return nil
}

// getQueryData gets a bazel query expression that attempt to capture all of
// the files and targets we're interested in. Importantly, it excludes certain
// directories such as this here gen directory and pkg/ui. It excludes the
// current directory, because the currently generated files may have errors.
// It excludes the pkg/ui directory because including it will lead to an
// invocation of npm, which can be slow and painful on platforms without good
// npm support.
//
// The targets should be thought of as the following expression, constructed
// additively in code.
//
//	build/...:* + //docs/...:* + //pkg/...:* - //pkg//ui/...:* - //pkg/gen/...:*
func getQueryData() (*queryData, error) {
	dirs := []string{"build", "docs"}
	ents, err := os.ReadDir("pkg")
	if err != nil {
		return nil, err
	}
	toSkip := map[string]struct{}{"ui": {}, "gen": {}}
	for _, e := range ents {
		if _, shouldSkip := toSkip[e.Name()]; shouldSkip || !e.IsDir() {
			continue
		}
		dirs = append(dirs, filepath.Join("pkg", e.Name()))
	}
	exprs := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		exprs = append(exprs, "//"+dir+"/...:*")
	}
	return &queryData{
		All: "(" + strings.Join(exprs, " + ") + ")",
	}, nil
}
