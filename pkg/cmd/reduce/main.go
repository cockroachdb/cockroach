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
// execution. Run `make bin/reduce` to compile the reduce program.
package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/reduce/reduce"
	"github.com/cockroachdb/cockroach/pkg/cmd/reduce/reduce/reducesql"
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
		n := (runtime.GOMAXPROCS(0) + 2) / 3
		if n < 1 {
			n = 1
		}
		return n
	}()
	flags           = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	binary          = flags.String("binary", "./cockroach", "path to cockroach binary")
	file            = flags.String("file", "", "the path to a file containing a SQL query to reduce")
	verbose         = flags.Bool("v", false, "print progress to standard output and the original test case output if it is not interesting")
	contains        = flags.String("contains", "", "error regex to search for")
	unknown         = flags.Bool("unknown", false, "print unknown types during walk")
	workers         = flags.Int("goroutines", goroutines, "number of worker goroutines (defaults to NumCPU/3")
	chunkReductions = flags.Int("chunk", 0, "number of consecutive chunk reduction failures allowed before halting chunk reduction (default 0)")
	tlp             = flags.Bool("tlp", false, "last two non-empty lines in file are equivalent queries returning different results, -file is required")
)

const description = `
The reduce utility attempts to simplify SQL that produces an error in
CockroachDB. The problematic SQL, passed via standard input, is
repeatedly reduced as long as it produces an error in the CockroachDB
demo that matches the provided -contains regex.

The following options are available:

`

func usage() {
	fmt.Fprintf(flags.Output(), "Usage of %s:\n", os.Args[0])
	fmt.Fprint(flags.Output(), description)
	flags.PrintDefaults()
	os.Exit(1)
}

func main() {
	flags.Usage = usage
	if err := flags.Parse(os.Args[1:]); err != nil {
		usage()
	}
	if (*tlp && (*file == "" || *contains != "")) || (!*tlp && *contains == "") {
		fmt.Printf("%s: either -contains or -tlp and -file must be provided\n\n", os.Args[0])
		usage()
	}
	reducesql.LogUnknown = *unknown
	out, err := reduceSQL(*binary, *contains, file, *workers, *verbose, *chunkReductions, *tlp)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(out)
}

func reduceSQL(
	binary, contains string, file *string, workers int, verbose bool, chunkReductions int, tlp bool,
) (string, error) {
	const tlpFailureError = "TLP_FAILURE"
	if tlp {
		contains = tlpFailureError
	}
	containsRE, err := regexp.Compile(contains)
	if err != nil {
		return "", err
	}
	var input []byte
	{
		if file == nil {
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
		} else {
			input, err = ioutil.ReadFile(*file)
			if err != nil {
				return "", err
			}
		}
	}

	inputString := string(input)
	var tlpCheck string

	// If TLP check is requested, then we remove the last two queries from the
	// input (each query is expected to be a single line before pretty-printing)
	// which we use then to construct a special TLP check query that results in
	// an error if two removed queries return different results.
	//
	// We do not just include the TLP check query into the input string because
	// the reducer would then reduce the check query itself, making the
	// reduction meaningless.
	if tlp {
		removeSemicolonIfLast := func(s string) string {
			if len(s) == 0 {
				return s
			}
			if s[len(s)-1] != ';' {
				return s
			}
			return s[:len(s)-1]
		}
		var unpartitioned, partitioned string
		lines := strings.Split(string(input), "\n")
		lineIdx := len(lines) - 1
		for partitioned == "" {
			if lines[lineIdx] != "" {
				partitioned = removeSemicolonIfLast(lines[lineIdx])
			}
			lineIdx--
		}
		for unpartitioned == "" {
			if lines[lineIdx] != "" {
				unpartitioned = removeSemicolonIfLast(lines[lineIdx])
			}
			lineIdx--
		}
		inputString = strings.Join(lines[:lineIdx], "\n")
		// tlpCheck is a query that will result in an error with tlpFailureError
		// error message when unpartitioned and partitioned queries return
		// different results (which is the case when there are rows in one
		// result set that are not present in the other).
		tlpCheck = fmt.Sprintf(`
SELECT CASE
  WHEN
    (SELECT count(*) FROM ((%[1]s) EXCEPT ALL (%[2]s))) != 0
  OR
    (SELECT count(*) FROM ((%[2]s) EXCEPT ALL (%[1]s))) != 0
  THEN
    crdb_internal.force_error('', '%[3]s')
  END;`, unpartitioned, partitioned, tlpFailureError)
	}

	// Pretty print the input so the file size comparison is useful.
	inputSQL, err := reducesql.Pretty(inputString)
	if err != nil {
		return "", err
	}

	var logger *log.Logger
	if verbose {
		logger = log.New(os.Stderr, "", 0)
		logger.Printf("input SQL pretty printed, %d bytes -> %d bytes\n", len(input), len(inputSQL))
		if tlp {
			prettyTLPCheck, err := reducesql.Pretty(tlpCheck)
			if err != nil {
				return "", err
			}
			logger.Printf("\nTLP check query:\n%s\n\n", prettyTLPCheck)
		}
	}

	var chunkReducer reduce.ChunkReducer
	if chunkReductions > 0 {
		chunkReducer = reducesql.NewSQLChunkReducer(chunkReductions)
	}

	isInteresting := func(ctx context.Context, sql string) (interesting bool, logOriginalHint func()) {
		// Disable telemetry and license generation. Do not exit on errors so
		// the entirety of the input SQL is processed.
		cmd := exec.CommandContext(ctx, binary,
			"demo",
			"--empty",
			"--disable-demo-license",
			"--set=errexit=false",
		)
		cmd.Env = []string{"COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING", "true"}
		if !strings.HasSuffix(sql, ";") {
			sql += ";"
		}
		// If -tlp was not specified, this is a noop, if it was specified, then
		// we append the special TLP check query.
		sql += tlpCheck
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
		if verbose {
			logOriginalHint = func() {
				logger.Printf("output did not match regex %s:\n\n%s", contains, string(out))
			}
		}
		return containsRE.Match(out), logOriginalHint
	}

	out, err := reduce.Reduce(
		logger,
		inputSQL,
		isInteresting,
		workers,
		reduce.ModeInteresting,
		chunkReducer,
		reducesql.SQLPasses...,
	)
	return out, err
}
