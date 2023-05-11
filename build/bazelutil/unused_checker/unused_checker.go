// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// unused_checker is a utility binary that aggregates data from the
// `unused` lint to ensure that nothing is actually unused. The "unused"
// information from any given binary will necessarily be incomplete because any
// unused object may be used by another binary (e.g.: a test-only interface that
// is consumed by the test binary). unused_checker coordinates building all of
// the binaries and figuring out which stuff is actually globally unused.

//go:build bazel
// +build bazel

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
)

type Usage map[string]map[string]struct{}

// UnusedResult is the encoded data structure we get from unused.out in the .x
// archives.
type UnusedResult struct {
	Unused map[string]*[]string
	Used   map[string]*[]string
}

func impl() error {
	tmpdir, err := bazel.NewTmpDir("unused")
	if err != nil {
		return err
	}
	defer func() {
		err := os.RemoveAll(tmpdir)
		if err != nil {
			panic(err)
		}
	}()

	gobin, err := bazel.Runfile("bin/go")
	if err != nil {
		return err
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	unused := make(Usage)
	used := make(Usage)

	for _, nogoX := range os.Args[1:] {
		nogoX = filepath.Join(cwd, nogoX)
		cmd := exec.Command(gobin, "tool", "pack", "x", nogoX, "unused.out")
		cmd.Dir = tmpdir
		if err := cmd.Run(); err != nil {
			// The unused.out file might be missing -- this is fine.
			continue
		}
		encoded, err := os.ReadFile(filepath.Join(tmpdir, "unused.out"))
		if err != nil {
			return err
		}
		var unusedResult UnusedResult
		if err := json.Unmarshal(encoded, &unusedResult); err != nil {
			return err
		}
		logUsage(unusedResult.Unused, &unused)
		logUsage(unusedResult.Used, &used)
		if err := os.Remove(filepath.Join(tmpdir, "unused.out")); err != nil {
			return err
		}
	}

	var failures []string
	for pkgPath, objs := range unused {
		usedPkgMap := used[pkgPath]
		for obj := range objs {
			_, ok := usedPkgMap[obj]
			if !ok {
				split := strings.Split(obj, ":")
				failure := fmt.Sprintf("%s.%s (%s:%s)", pkgPath, split[0], split[1], split[2])
				if !ignoreUnused(failure) {
					failures = append(failures, failure)
				}
			}
		}
	}

	if len(failures) > 0 {
		msg := fmt.Sprintf("the following objects are unused: %s", strings.Join(failures, ", "))
		fmt.Printf("##teamcity[message text='%s' errorDetails='stack trace' status='ERROR']\n", msg)
		return errors.New(msg)
	}

	return nil
}

func logUsage(in map[string]*[]string, out *Usage) {
	for pkgPath, objs := range in {
		pkgMap, ok := (*out)[pkgPath]
		if !ok {
			(*out)[pkgPath] = make(map[string]struct{})
			pkgMap = (*out)[pkgPath]
		}
		for _, obj := range *objs {
			pkgMap[obj] = struct{}{}
		}
	}
}

func ignoreUnused(obj string) bool {
	prefixes := []string{
		"github.com/cockroachdb/cockroach/pkg/ccl/gssapiccl.",
		"github.com/cockroachdb/cockroach/pkg/geo/geographiclib.",
		"github.com/cockroachdb/cockroach/pkg/util/goschedstats.",
		"github.com/cockroachdb/cockroach/pkg/sql/parser.",
		"github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser.",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(obj, prefix) {
			return true
		}
	}
	return false
}

func main() {
	if err := impl(); err != nil {
		panic(err)
	}
}
