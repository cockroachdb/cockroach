// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// fuzz builds and executes fuzz tests.
//
// Fuzz tests can be added to CockroachDB by adding a function of the form:
//   func FuzzXXX(data []byte) int
// To help the fuzzer increase coverage, this function should return 1 on
// interesting input (for example, a parse succeeded) and 0 otherwise. Panics
// will be detected and reported.
//
// To exclude this file except during fuzzing, tag it with:
//   // +build gofuzz
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"golang.org/x/tools/go/packages"
)

var (
	flags   = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	tests   = flags.String("tests", "", "regex filter for tests to run")
	timeout = flags.Duration("timeout", 1*time.Minute, "time to run each Fuzz func")
	verbose = flags.Bool("v", false, "verbose output")
)

func usage() {
	fmt.Fprintf(flags.Output(), "Usage of %s:\n", os.Args[0])
	flags.PrintDefaults()
	os.Exit(1)
}

func main() {
	// go-fuzz-build doesn't seem to support the vendor directory. It
	// appears to require the go-fuzz-dep be in the canonical
	// location. Hence we can't vendor go-fuzz and go-fuzz-build, we
	// require the user install them to their global GOPATH.
	for _, file := range []string{"go-fuzz", "go-fuzz-build"} {
		if _, err := exec.LookPath(file); err != nil {
			fmt.Println(file, "must be in your PATH")
			fmt.Println("Run `go get -u github.com/dvyukov/go-fuzz/...` to install.")
			os.Exit(1)
		}
	}
	if err := flags.Parse(os.Args[1:]); err != nil {
		usage()
	}
	patterns := flags.Args()
	if len(patterns) == 0 {
		fmt.Print("missing packages\n\n")
		usage()
	}
	crashers, err := fuzz(patterns, *tests, *timeout)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if crashers > 0 {
		fmt.Println(crashers, "crashers")
		os.Exit(2)
	}
}

func fatal(arg interface{}) {
	panic(arg)
}

func log(format string, args ...interface{}) {
	if !*verbose {
		return
	}
	fmt.Fprintf(os.Stderr, format, args...)
}

func fuzz(patterns []string, tests string, timeout time.Duration) (int, error) {
	ctx := context.Background()
	pkgs, err := packages.Load(&packages.Config{
		Mode:       packages.NeedFiles,
		BuildFlags: []string{"-tags", "gofuzz"},
	}, patterns...)
	if err != nil {
		return 0, err
	}
	var testsRE *regexp.Regexp
	if tests != "" {
		testsRE, err = regexp.Compile(tests)
		if err != nil {
			return 0, err
		}
	}
	crashers := 0
	for _, pkg := range pkgs {
		if len(pkg.Errors) > 0 {
			return 0, pkg.Errors[0]
		}
		log("%s: searching for Fuzz funcs\n", pkg)
		fns, err := findFuncs(pkg)
		if err != nil {
			return 0, err
		}
		if len(fns) == 0 {
			continue
		}
		dir := filepath.Dir(pkg.GoFiles[0])
		{
			log("%s: executing go-fuzz-build...", pkg)
			cmd := exec.Command("go-fuzz-build",
				// These packages break go-fuzz for some reason, so skip them.
				"-preserve", "github.com/cockroachdb/cockroach/pkg/sql/stats,github.com/cockroachdb/cockroach/pkg/server/serverpb",
			)
			cmd.Dir = dir
			out, err := cmd.CombinedOutput()
			log(" done\n")
			if err != nil {
				log("%s\n", out)
				return 0, err
			}
		}
		for _, fn := range fns {
			if testsRE != nil && !testsRE.MatchString(fn) {
				continue
			}
			crashers += execGoFuzz(ctx, pkg, dir, fn, timeout)
		}
	}
	return crashers, nil
}

var goFuzzRE = regexp.MustCompile(`crashers: (\d+)`)

// execGoFuzz executes go-fuzz and returns the number of crashers found.
func execGoFuzz(
	ctx context.Context, pkg *packages.Package, dir, fn string, timeout time.Duration,
) int {
	log("\n%s: fuzzing %s for %v\n", pkg, fn, timeout)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	workdir := fmt.Sprintf("work-%s", fn)
	cmd := exec.CommandContext(ctx, "go-fuzz", "-func", fn, "-workdir", workdir)
	cmd.Dir = dir
	stderr, err := cmd.StderrPipe()
	if err != nil {
		fatal(err)
	}
	if err := cmd.Start(); err != nil {
		fatal(err)
	}
	crashers := 0
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		line := scanner.Text()
		log("%s\n", line)
		matches := goFuzzRE.FindStringSubmatch(line)
		if len(matches) == 0 {
			continue
		}
		i, err := strconv.Atoi(matches[1])
		if err != nil {
			fatal(err)
		}
		if i > crashers {
			if crashers == 0 {
				fmt.Printf("workdir: %s\n", filepath.Join(dir, workdir))
			}
			crashers = i
			fmt.Printf("crashers: %d\n", crashers)
		}
	}
	if err := scanner.Err(); err != nil {
		fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		fatal(err)
	}
	return crashers
}

var fuzzFuncRE = regexp.MustCompile(`(?m)^func (Fuzz\w*)\(\w+ \[\]byte\) int {$`)

// findFuncs returns a list of fuzzable function names in the given package.
func findFuncs(pkg *packages.Package) ([]string, error) {
	var ret []string
	for _, file := range pkg.GoFiles {
		content, err := ioutil.ReadFile(file)
		if err != nil {
			return nil, err
		}
		matches := fuzzFuncRE.FindAllSubmatch(content, -1)
		for _, match := range matches {
			ret = append(ret, string(match[1]))
		}
	}
	return ret, nil
}
