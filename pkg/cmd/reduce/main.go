// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// reduce reduces SQL passed from the input file using cockroach demo. The input
// file is simplified such that -contains argument is present as an error during
// SQL execution. Run `make bin/reduce` to compile the reduce program.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/reduce/reduce"
	"github.com/cockroachdb/cockroach/pkg/cmd/reduce/reduce/reducesql"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/errors"
	"github.com/google/go-cmp/cmp"
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
	flags             = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	binary            = flags.String("binary", "./cockroach", "path to cockroach binary")
	httpPort          = flags.Int("http-port", 8080, "first port number for HTTP servers in demo")
	sqlPort           = flags.Int("sql-port", 26257, "first port number for SQL servers in demo")
	file              = flags.String("file", "", "the path to a file containing SQL queries to reduce; required")
	outFlag           = flags.String("out", "", "if set, the path to a new file where reduced result will be written to")
	verbose           = flags.Bool("v", false, "print progress to standard output and the original test case output if it is not interesting")
	contains          = flags.String("contains", "", "error regex to search for")
	unknown           = flags.Bool("unknown", false, "print unknown types during walk")
	workers           = flags.Int("goroutines", goroutines, "number of worker goroutines (defaults to NumCPU/3")
	chunkReductions   = flags.Int("chunk", 0, "number of consecutive chunk reduction failures allowed before halting chunk reduction (default 0)")
	multiRegion       = flags.Bool("multi-region", false, "test with a 9-node multi-region demo cluster")
	tlp               = flags.Bool("tlp", false, "last two statements in file are equivalent queries returning different results")
	costfuzz          = flags.Bool("costfuzz", false, "last four statements in file are two identical queries separated by settings changes")
	unoptimizedOracle = flags.Bool("unoptimized-query-oracle", false, "last several statements in file are two identical queries separated by settings changes")
)

const description = `
The reduce utility attempts to simplify SQL that produces specific
output in CockroachDB. The problematic SQL, specified via -file flag, is
repeatedly reduced as long as it produces results or errors in cockroach
demo that match the provided -contains regex.

An alternative mode of operation is enabled by specifying -tlp option:
in such case the last two queries in the file must be equivalent and
produce different results. (Note that statements in the file must be
separated by blank lines for -tlp to function correctly.)

Another alternative mode of operation is enabled by the -costfuzz
option: in which case the last four statements in the file must be two
identical queries separated by two settings changes, which produce different
results. (Note that statements in the file must be separated by blank
lines for -costfuzz to function correctly.)

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
	if *file == "" {
		fmt.Printf("%s: -file must be provided\n\n", os.Args[0])
		usage()
	}
	var modes int
	if *contains != "" {
		modes++
	}
	if *tlp {
		modes++
	}
	if *costfuzz {
		modes++
	}
	if *unoptimizedOracle {
		modes++
	}

	if modes != 1 {
		fmt.Printf("%s: exactly one of -contains, -tlp, or -costfuzz must be specified\n\n", os.Args[0])
		usage()
	}
	reducesql.LogUnknown = *unknown
	out, err := reduceSQL(
		*binary, *httpPort, *sqlPort, *contains, file, *workers, *verbose,
		*chunkReductions, *multiRegion, *tlp, *costfuzz, *unoptimizedOracle,
	)
	if err != nil {
		log.Fatal(err)
	}
	if *outFlag != "" {
		file, err := os.Create(*outFlag)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		fmt.Fprint(file, out)
	} else {
		fmt.Println(out)
	}
}

func reduceSQL(
	binary string,
	httpPort, sqlPort int,
	contains string,
	file *string,
	workers int,
	verbose bool,
	chunkReductions int,
	multiRegion bool,
	tlp, costfuzz, unoptimizedOracle bool,
) (string, error) {
	var settings string
	if devLicense, ok := envutil.EnvString("COCKROACH_DEV_LICENSE", 0); ok {
		settings += "SET CLUSTER SETTING cluster.organization = 'Cockroach Labs - Production Testing';\n"
		settings += fmt.Sprintf("SET CLUSTER SETTING enterprise.license = '%s';\n", devLicense)
	}

	const (
		tlpFailureError      = "TLP_FAILURE"
		costfuzzSep          = "COSTFUZZ_SEP"
		unoptimizedOracleSep = "UNOPTIMIZED_ORACLE_SEP"
	)
	if tlp {
		contains = tlpFailureError
	}
	containsRE, err := regexp.Compile(contains)
	if err != nil {
		return "", err
	}
	input, err := os.ReadFile(*file)
	if err != nil {
		return "", err
	}

	inputString := string(input)
	var queryComparisonCheck string

	// If TLP check is requested, then we remove the last two queries from the
	// input (each query is expected to be delimited by empty lines) which we
	// use then to construct a special TLP check query that results in an error
	// if two removed queries return different results.
	//
	// We do not just include the TLP check query into the input string because
	// the reducer would then reduce the check query itself, making the
	// reduction meaningless.
	if tlp {
		lines := strings.Split(string(input), "\n")
		lineIdx := len(lines) - 1
		partitioned, lineIdx := findPreviousQuery(lines, lineIdx)
		unpartitioned, lineIdx := findPreviousQuery(lines, lineIdx)
		inputString = strings.Join(lines[:lineIdx], "\n")
		// We make queryComparisonCheck a query that will result in an error with
		// tlpFailureError error message when unpartitioned and partitioned queries
		// return different results (which is the case when there are rows in one
		// result set that are not present in the other).
		queryComparisonCheck = fmt.Sprintf(`
SELECT CASE
  WHEN
    (SELECT count(*) FROM ((%[1]s) EXCEPT ALL (%[2]s))) != 0
  OR
    (SELECT count(*) FROM ((%[2]s) EXCEPT ALL (%[1]s))) != 0
  THEN
    crdb_internal.force_error('', '%[3]s')
  END;`, unpartitioned, partitioned, tlpFailureError)
	}

	// If costfuzz mode is requested, then we remove the last four statements
	// from the input (statements are expected to be delimited by empty lines)
	// which we then save for the costfuzz check.
	if costfuzz {
		lines := strings.Split(string(input), "\n")
		lineIdx := len(lines) - 1
		perturb, lineIdx := findPreviousQuery(lines, lineIdx)
		setting2, lineIdx := findPreviousQuery(lines, lineIdx)
		setting1, lineIdx := findPreviousQuery(lines, lineIdx)
		control, lineIdx := findPreviousQuery(lines, lineIdx)
		inputString = strings.Join(lines[:lineIdx], "\n")
		// We make queryComparisonCheck the original control / settings / perturbed
		// statements, surrounded by sentinel statements.
		queryComparisonCheck = fmt.Sprintf(`
SELECT '%[1]s';

%[2]s;

SELECT '%[1]s';

%[3]s;

SELECT '%[1]s';

%[4]s;

SELECT '%[1]s';

%[5]s;

SELECT '%[1]s';
`, costfuzzSep, control, setting1, setting2, perturb)
	}

	// If unoptimizedOracle mode is requested, then we remove the last several statements
	// from the input (statements are expected to be delimited by empty lines)
	// which we then save for the unoptimizedOracle check.
	if unoptimizedOracle {
		lines := strings.Split(string(input), "\n")
		lineIdx := len(lines) - 1
		optimized, lineIdx := findPreviousQuery(lines, lineIdx)
		settings2, lineIdx := findPreviousSetStatements(lines, lineIdx)
		unoptimized, lineIdx := findPreviousQuery(lines, lineIdx)
		settings1, lineIdx := findPreviousSetStatements(lines, lineIdx)
		inputString = strings.Join(lines[:lineIdx], "\n")
		// We make queryComparisonCheck the original unoptimized / settings /
		// optimized statements, surrounded by sentinel statements.
		queryComparisonCheck = fmt.Sprintf(`
SELECT '%[1]s';

%[2]s

SELECT '%[1]s';

%[3]s;

SELECT '%[1]s';

%[4]s

SELECT '%[1]s';

%[5]s;

SELECT '%[1]s';
`, unoptimizedOracleSep, settings1, unoptimized, settings2, optimized)
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
		if tlp || costfuzz || unoptimizedOracle {
			prettyTLPCheck, err := reducesql.Pretty(queryComparisonCheck)
			if err != nil {
				return "", err
			}
			logger.Printf("\nCheck query:\n%s\n\n", prettyTLPCheck)
		}
	}

	var chunkReducer reduce.ChunkReducer
	if chunkReductions > 0 {
		chunkReducer = reducesql.NewSQLChunkReducer(chunkReductions)
	}

	isInteresting := func(ctx context.Context, sql string) (interesting bool, logOriginalHint func()) {
		args := []string{
			"demo",
			"--insecure",
			"--empty",
			// Do not exit on errors so the entirety of the input SQL is
			// processed.
			"--set=errexit=false",
			"--format=tsv",
			fmt.Sprintf("--http-port=%d", httpPort),
			fmt.Sprintf("--sql-port=%d", sqlPort),
		}
		if multiRegion {
			args = append(args, "--nodes=9")
			args = append(args, "--multitenant=false")
		}
		cmd := exec.CommandContext(ctx, binary, args...)
		// Disable telemetry.
		cmd.Env = []string{"COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING", "true"}
		sql = settings + sql
		if !strings.HasSuffix(sql, ";") {
			sql += ";"
		}
		// If neither -tlp nor -costfuzz nor -unoptimized-query-oracle were specified, this is a noop.
		sql += queryComparisonCheck
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
		if costfuzz {
			parts := bytes.Split(out, []byte(costfuzzSep))
			if len(parts) != 6 {
				if verbose {
					logOriginalHint = func() {
						logger.Printf("could not divide output into 6 parts:\n%s", string(out))
					}
				}
				return false, logOriginalHint
			}
			if verbose {
				logOriginalHint = func() {
					logger.Printf("control and perturbed query results were the same: \n%v\n\n%v\n", string(parts[1]), string(parts[4]))
				}
			}
			return !unsortedLinesEqual(string(parts[1]), string(parts[4])), logOriginalHint
		}
		if unoptimizedOracle {
			parts := bytes.Split(out, []byte(unoptimizedOracleSep))
			if len(parts) != 6 {
				if verbose {
					logOriginalHint = func() {
						logger.Printf("could not divide output into 6 parts:\n%s", string(out))
					}
				}
				return false, logOriginalHint
			}
			if verbose {
				logOriginalHint = func() {
					logger.Printf("unoptimized and optimized query results were the same: \n%v\n\n%v\n", string(parts[2]), string(parts[4]))
				}
			}
			return !unsortedLinesEqual(string(parts[2]), string(parts[4])), logOriginalHint
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

// findPreviousQuery return the query preceding lineIdx without a semicolon.
// Queries are expected to be delimited with empty lines.
func findPreviousQuery(lines []string, lineIdx int) (string, int) {
	// Skip empty lines.
	for lines[lineIdx] == "" {
		lineIdx--
	}
	lastQueryLineIdx := lineIdx
	// Now skip over all lines comprising the query.
	for lines[lineIdx] != "" {
		lineIdx--
	}
	// lineIdx right now points at an empty line before the query.
	query := strings.Join(lines[lineIdx+1:lastQueryLineIdx+1], "\n")
	// Remove the semicolon.
	return strings.TrimSuffix(query, ";"), lineIdx
}

// findPreviousSetStatements returns any SET or RESET statements preceding
// lineIdx. Queries are expected to be delimited with empty lines.
func findPreviousSetStatements(lines []string, lineIdx int) (string, int) {
	// Skip empty lines.
	for lines[lineIdx] == "" {
		lineIdx--
	}
	lastQueryLineIdx := lineIdx
	firstQueryLineIdx := lineIdx
	for {
		// Now skip over all lines comprising the query.
		for lines[lineIdx] != "" {
			lineIdx--
		}
		if !strings.HasPrefix(lines[lineIdx+1], "SET") && !strings.HasPrefix(lines[lineIdx+1], "RESET") {
			break
		}
		firstQueryLineIdx = lineIdx
		// Skip empty lines.
		for lines[lineIdx] == "" {
			lineIdx--
		}
	}
	// firstQueryLineIdx right now points at an empty line before the statement.
	query := strings.Join(lines[firstQueryLineIdx+1:lastQueryLineIdx+1], "\n")
	return query, firstQueryLineIdx
}

func unsortedLinesEqual(part1, part2 string) bool {
	lines1 := strings.Split(part1, "\n")
	lines2 := strings.Split(part2, "\n")
	sort.Strings(lines1)
	sort.Strings(lines2)
	return cmp.Equal(lines1, lines2)
}
