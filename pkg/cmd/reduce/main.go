// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// reduce reduces SQL passed over stdin using cockroach demo. The input is
// simplified such that the contains argument is present as an error during SQL
// execution.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/reduce"
	"github.com/cockroachdb/cockroach/pkg/testutils/reduce/reducesql"
	"github.com/cockroachdb/errors"
)

var (
	// Some quick benchmarks show that somewhere around 1/3 of NumCPUs
	// performs best. This can probably be tweaked with benchmarks from
	// other machines, but is probably a good place to start.
	goroutines = func() int {
		// Round up by adding 2.
		// Num CPUs -> n:
		// 1-3: 1
		// 4-6: 2
		// 7-9: 3
		// etc.
		n := (runtime.NumCPU() + 2) / 3
		if n < 1 {
			n = 1
		}
		return n
	}()
	flags    = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	path     = flags.String("path", "./cockroach", "path to cockroach binary")
	verbose  = flags.Bool("v", false, "log progress")
	contains = flags.String("contains", "", "error regex to search for")
	unknown  = flags.Bool("unknown", false, "print unknown types during walk")
	workers  = flags.Int("goroutines", goroutines, "number of worker goroutines (defaults to NumCPU/3")
)

func usage() {
	fmt.Fprintf(flags.Output(), "Usage of %s:\n", os.Args[0])
	flags.PrintDefaults()
	os.Exit(1)
}

func main() {
	if err := flags.Parse(os.Args[1:]); err != nil {
		usage()
	}
	if *contains == "" {
		fmt.Print("missing contains\n\n")
		usage()
	}
	reducesql.LogUnknown = *unknown
	out, err := reduceSQL(*path, *contains, *workers, *verbose)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(out)
}

func reduceSQL(path, contains string, workers int, verbose bool) (string, error) {
	containsRE, err := regexp.Compile(contains)
	if err != nil {
		return "", err
	}
	var input []byte
	{
		done := make(chan struct{}, 1)
		go func() {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				log.Fatal("timeout waiting for input on stdin")
			}
		}()
		input, err = ioutil.ReadAll(os.Stdin)
		done <- struct{}{}
		if err != nil {
			return "", err
		}
	}

	// Pretty print the input so the file size comparison is useful.
	inputSQL, err := reducesql.Pretty(input)
	if err != nil {
		return "", err
	}

	var logger io.Writer
	if verbose {
		logger = os.Stderr
		fmt.Fprintf(logger, "input SQL pretty printed, %d bytes -> %d bytes\n", len(input), len(inputSQL))
	}

	interesting := func(ctx context.Context, f reduce.File) bool {
		// Disable telemetry and license generation.
		cmd := exec.CommandContext(ctx, path,
			"demo",
			"--empty",
			"--disable-demo-license",
		)
		cmd.Env = []string{"COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING", "true"}
		sql := string(f)
		if !strings.HasSuffix(sql, ";") {
			sql += ";"
		}
		cmd.Stdin = strings.NewReader(sql)
		out, err := cmd.CombinedOutput()
		switch {
		case errors.HasType(err, (*exec.Error)(nil)):
			if errors.Is(err, exec.ErrNotFound) {
				log.Fatal(err)
			}
		case errors.HasType(err, (*os.PathError)(nil)):
			log.Fatal(err)
		}
		return containsRE.Match(out)
	}

	out, err := reduce.Reduce(logger, reduce.File(inputSQL), interesting, workers, reduce.ModeInteresting, reducesql.SQLPasses...)
	return string(out), err
}
