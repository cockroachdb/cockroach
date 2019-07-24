// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logictest

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	gosql "database/sql"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime/debug"
	"runtime/trace"
	"sort"
	"strconv"
	"strings"
	"testing"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// This file is home to TestLogic, a general-purpose engine for
// running SQL logic tests.
//
// TestLogic implements the infrastructure that runs end-to-end tests
// against CockroachDB's SQL layer. It is typically used to run
// CockroachDB's own tests (stored in the `testdata` directory) during
// development and CI, and a subset of SQLite's "Sqllogictest" during
// nightly CI runs. However, any test input can be specified via
// command-line flags (see below).
//
// In a nutshell, TestLogic reads one or more test input files
// containing sequences of SQL statements and queries. Each input file
// is meant to test a feature group. The reason why tests can/should
// be split across multiple files is that each test input file gets
// its own fresh, empty database.
//
// Input files for unit testing are stored alongside the source code
// in the `testdata` subdirectory. The input files for the larger
// TestSqlLiteLogic tests are stored in a separate repository.
//
// The test input is expressed using a domain-specific language, called
// Test-Script, defined by SQLite's "Sqllogictest".  The official home
// of Sqllogictest and Test-Script is
//      https://www.sqlite.org/sqllogictest/
//
// (the TestSqlLiteLogic test uses a fork of the Sqllogictest test files;
// its input files are hosted at https://github.com/cockroachdb/sqllogictest)
//
// Test-Script is line-oriented. It supports both statements which
// generate no result rows, and queries that produce result rows. The
// result of queries can be checked either using an explicit reference
// output in the test file, or using the expected row count and a hash
// of the expected output. A test can also check for expected column
// names for query results, or expected errors.
//
// Logic tests can start with a directive as follows:
//
//   # LogicTest: local fakedist
//
// This directive lists configurations; the test is run once in each
// configuration (in separate subtests). The configurations are defined by
// logicTestConfigs. If the directive is missing, the test is run in the
// default configuration.
//
// The Test-Script language is extended here for use with CockroachDB. The
// supported directives are:
//
//  - statement ok
//    Runs the statement that follows and expects success. For
//    example:
//      statement ok
//      CREATE TABLE kv (k INT PRIMARY KEY, v INT)
//
//
//  - statement count N
//    Like "statement ok" but expect a final RowsAffected count of N.
//    example:
//      statement count 2
//      INSERT INTO kv VALUES (1,2), (2,3)
//
//  - statement error <regexp>
//    Runs the statement that follows and expects an
//    error that matches the given regexp.
//
//  - query <typestring> <options> <label>
//    Runs the query that follows and verifies the results (specified after the
//    query and a ---- separator). Example:
//      query I
//      SELECT 1, 2
//      ----
//      1 2
//
//    The type string specifies the number of columns and their types:
//      - T for text; also used for various types which get converted
//        to string (arrays, timestamps, etc.).
//      - I for integer
//      - R for floating point or decimal
//      - B for boolean
//      - O for oid
//
//    Options are comma separated strings from the following:
//      - nosort (default)
//      - rowsort: sorts both the returned and the expected rows assuming one
//            white-space separated word per column.
//      - valuesort: sorts all values on all rows as one big set of
//            strings (for both the returned and the expected rows).
//      - partialsort(x,y,..): performs a partial sort on both the
//            returned and the expected results, preserving the relative
//            order of rows that differ on the specified columns
//            (1-indexed); for results that are expected to be already
//            ordered according to these columns. See partialSort() for
//            more information.
//      - colnames: column names are verified (the expected column names
//            are the first line in the expected results).
//      - retry: if the expected results do not match the actual results, the
//            test will be retried with exponential backoff up to some maximum
//            duration. If the test succeeds at any time during that period, it
//            is considered successful. Otherwise, it is a failure. See
//            testutils.SucceedsSoon for more information. If run with the
//            -rewrite flag, inserts a 500ms sleep before executing the query
//            once.
//
//    The label is optional. If specified, the test runner stores a hash
//    of the results of the query under the given label. If the label is
//    reused, the test runner verifies that the results are the
//    same. This can be used to verify that two or more queries in the
//    same test script that are logically equivalent always generate the
//    same output. If a label is provided, expected results don't need to
//    be provided (in which case there should be no ---- separator).
//
//  - query error <regexp>
//    Runs the query that follows and expects an error
//    that matches the given regexp.
//
//  - repeat <number>
//    It causes the following `statement` or `query` to be repeated the given
//    number of times. For example:
//      repeat 50
//      statement ok
//      INSERT INTO T VALUES ((SELECT MAX(k+1) FROM T))
//
//  - let $varname
//    Executes the query that follows (expecting a single result) and remembers
//    the result (as a string) for later use. Any `$varname` occurrences in
//    subsequent statements or queries are replaced with the result. The
//    variable name must start with a letter, and subsequent characters must be
//    letters, digits, or underscores. Example:
//      let $foo
//      SELECT MAX(v) FROM kv
//
//      statement ok
//      SELECT * FROM kv WHERE v = $foo
//
//  - sleep <duration>
//    Introduces a sleep period. Example: sleep 2s
//
//  - user <username>
//    Changes the user for subsequent statements or queries.
//
//  - skipif <mysql/mssql/postgresql/cockroachdb>
//    Skips the following `statement` or `query` if the argument is postgresql
//    or cockroachdb.
//
//  - onlyif <mysql/mssql/postgresql/cockroachdb>
//    Skips the following `statement` or query if the argument is not postgresql
//    or cockroachdb.
//
//  - traceon <file>
//    Enables tracing to the given file.
//
//  - traceoff
//    Stops tracing.
//
//  - kv-batch-size <num>
//    Limits the kvfetcher batch size; it can be used to trigger certain error
//    conditions or corner cases around limited batches.
//
//  - subtest <testname>
//    Defines the start of a subtest. The subtest is any number of statements
//    that occur after this command until the end of file or the next subtest
//    command.
//
// The overall architecture of TestLogic is as follows:
//
// - TestLogic() selects the input files and instantiates
//   a `logicTest` object for each input file.
//
// - logicTest.run() sets up a new database.
// - logicTest.processTestFile() runs all tests from that input file.
//
// - each individual test in an input file is instantiated either as a
//   logicStatement or logicQuery object. These are then processed by
//   either logicTest.execStatement() or logicTest.execQuery().
//
// TestLogic has three main parameter groups:
//
// - Which input files are processed.
// - How and when to stop when tests fail.
// - Which results are reported.
//
// The parameters are typically set using the TESTFLAGS `make`
// parameter, as in:
//
//   make test PKG=./pkg/sql TESTS=TestLogic TESTFLAGS='....'
//
// Input file selection:
//
// -d <glob>  selects all files matching <glob>. This can mix and
//            match wildcards (*/?) or groups like {a,b,c}.
//
// -bigtest   enable the long-running SqlLiteLogic test, which uses files from
//            CockroachDB's fork of Sqllogictest.
//
// Configuration:
//
// -config name[,name2,...]   customizes the test cluster configuration for test
//                files that lack LogicTest directives; must be one
//                of `logicTestConfigs`.
//                Example:
//                  -config local-opt,fakedist-opt
//
// Error mode:
//
// -max-errors N  stop testing after N errors have been
//                encountered. Default 1. Set to 0 for no limit.
//
// -allow-prepare-fail
//                tolerate / ignore errors that occur during query
//                preparation. With -allow-prepare-fail you can
//                indicate that it is OK as long as the database
//                reports early to the client that it does not support
//                a given query.  Errors are still reported if queries
//                fail during execution only or if a statement fails.
//
// -flex-types    tolerate when a result column is produced with a
//                different numeric type than the one expected by the
//                test. This enables reusing tests designed for
//                databases with slightly different typing semantics.
//
// Test output:
//
// -v             (or -test.v if the test is compiled as a standalone
//                binary). Go `testing`'s `verbose` flag.
//
// The output generated by the following flags is suppressed unless
// either -v is given or a test fails.
//
// -show-sql      show SQL statements/queries immediately before they
//                are tested. This can be useful for example when
//                troubleshooting errors that cause the database/test
//                to stop before the test completes.  When -show-sql
//                is set, individual test results are annoted with
//                either "OK" (test passed as expected), "XFAIL"
//                (expected failure, test failed as expected), or
//                "FAIL" to indicate an unexpected/undesired test
//                failure.
//
// -error-summary produces a report organized by error message
//                of all queries that have caused that error.  Useful
//                with -allow-prepare-fail and/or -flex-types.
//
// -full-messages by default -error-summary shortens error messages
//                and queries so they fit in a moderately large
//                terminal screen. With this parameter, the
//                full text of errors and queries is printed.
//
// Suggested use:
//
// - For validation testing: just -d or -bigtest.
// - For compatibility testing: add -allow-prepare-fail -flex-types.
// - For troubleshooting / analysis: add -v -show-sql -error-summary.

var (
	resultsRE = regexp.MustCompile(`^(\d+)\s+values?\s+hashing\s+to\s+([0-9A-Fa-f]+)$`)
	errorRE   = regexp.MustCompile(`^(?:statement|query)\s+error\s+(?:pgcode\s+([[:alnum:]]+)\s+)?(.*)$`)
	varRE     = regexp.MustCompile(`\$[a-zA-Z][a-zA-Z_0-9]*`)

	// Input selection
	logictestdata  = flag.String("d", "", "glob that selects subset of files to run")
	bigtest        = flag.Bool("bigtest", false, "enable the long-running SqlLiteLogic test")
	overrideConfig = flag.String(
		"config", "",
		"sets the test cluster configuration; comma-separated values",
	)

	// Testing mode
	maxErrs = flag.Int(
		"max-errors", 1,
		"stop processing input files after this number of errors (set to 0 for no limit)",
	)
	allowPrepareFail = flag.Bool(
		"allow-prepare-fail", false, "tolerate unexpected errors when preparing a query",
	)
	flexTypes = flag.Bool(
		"flex-types", false,
		"do not fail when a test expects a column of a numeric type but the query provides another type",
	)

	// Output parameters
	showSQL = flag.Bool("show-sql", false,
		"print the individual SQL statement/queries before processing",
	)
	printErrorSummary = flag.Bool("error-summary", false,
		"print a per-error summary of failing queries at the end of testing, "+
			"when -allow-prepare-fail is set",
	)
	fullMessages = flag.Bool("full-messages", false,
		"do not shorten the error or SQL strings when printing the summary for -allow-prepare-fail "+
			"or -flex-types.",
	)
	rewriteResultsInTestfiles = flag.Bool(
		"rewrite", false,
		"ignore the expected results and rewrite the test files with the actual results from this "+
			"run. Used to update tests when a change affects many cases; please verify the testfile "+
			"diffs carefully!",
	)
	rewriteSQL = flag.Bool(
		"rewrite-sql", false,
		"pretty-reformat the SQL queries. Only use this incidentally when importing new tests. "+
			"beware! some tests INTEND to use non-formatted SQL queries (e.g. case sensitivity). "+
			"do not bluntly apply!",
	)
	sqlfmtLen = flag.Int("line-length", tree.DefaultPrettyCfg().LineWidth,
		"target line length when using -rewrite-sql")
	disableOptRuleProbability = flag.Float64(
		"disable-opt-rule-probability", 0,
		"disable transformation rules in the cost-based optimizer with the given probability.")
	optimizerCostPerturbation = flag.Float64(
		"optimizer-cost-perturbation", 0,
		"randomly perturb the estimated cost of each expression in the query tree by at most the "+
			"given fraction for the purpose of creating alternate query plans in the optimizer.")
)

type testClusterConfig struct {
	// name is the name of the config (used for subtest names).
	name                string
	numNodes            int
	useFakeSpanResolver bool
	// if non-empty, overrides the default optimizer mode.
	overrideOptimizerMode string
	// if non-empty, overrides the default distsql mode.
	overrideDistSQLMode string
	// if non-empty, overrides the default experimental_vectorize mode.
	overrideExpVectorize string
	// if non-empty, overrides the default automatic statistics mode.
	overrideAutoStats string
	// if set, queries using distSQL processors that can fall back to disk do
	// so immediately, using only their disk-based implementation.
	distSQLUseDisk bool
	// if set, enables DistSQL metadata propagation tests.
	distSQLMetadataTestEnabled bool
	// if set and the -test.short flag is passed, skip this config.
	skipShort bool
	// If not empty, bootstrapVersion controls what version the cluster will be
	// bootstrapped at.
	bootstrapVersion cluster.ClusterVersion
	// If not empty, serverVersion is used to set what the Server will consider to
	// be "the server version".
	// TODO(andrei): clarify this comment and many others around the "server
	// version".
	serverVersion  roachpb.Version
	disableUpgrade bool
}

// logicTestConfigs contains all possible cluster configs. A test file can
// specify a list of configs they run on in a file-level comment like:
//   # LogicTest: default distsql
// The test is run once on each configuration (in different subtests).
// If no configs are indicated, the default one is used (unless overridden
// via -config).
var logicTestConfigs = []testClusterConfig{
	{name: "local", numNodes: 1, overrideDistSQLMode: "off", overrideOptimizerMode: "off"},
	{name: "local-v1.1@v1.0-noupgrade", numNodes: 1,
		overrideDistSQLMode: "off", overrideOptimizerMode: "off",
		bootstrapVersion: cluster.ClusterVersion{
			Version: roachpb.Version{Major: 1},
		},
		serverVersion:  roachpb.Version{Major: 1, Minor: 1},
		disableUpgrade: true,
	},
	{name: "local-opt", numNodes: 1, overrideDistSQLMode: "off", overrideOptimizerMode: "on", overrideAutoStats: "false"},
	{name: "local-vec", numNodes: 1, overrideOptimizerMode: "on", overrideExpVectorize: "on"},
	{name: "fakedist", numNodes: 3, useFakeSpanResolver: true, overrideDistSQLMode: "on", overrideOptimizerMode: "off"},
	{name: "fakedist-opt", numNodes: 3, useFakeSpanResolver: true, overrideDistSQLMode: "on", overrideOptimizerMode: "on", overrideAutoStats: "false"},
	{name: "fakedist-metadata", numNodes: 3, useFakeSpanResolver: true, overrideDistSQLMode: "on", overrideOptimizerMode: "off",
		distSQLMetadataTestEnabled: true, skipShort: true},
	{name: "fakedist-disk", numNodes: 3, useFakeSpanResolver: true, overrideDistSQLMode: "on", overrideOptimizerMode: "off",
		distSQLUseDisk: true, skipShort: true},
	{name: "5node-local", numNodes: 5, overrideDistSQLMode: "off", overrideOptimizerMode: "off"},
	{name: "5node-dist", numNodes: 5, overrideDistSQLMode: "on", overrideOptimizerMode: "off"},
	{name: "5node-dist-vec", numNodes: 5, overrideDistSQLMode: "on", overrideOptimizerMode: "on", overrideExpVectorize: "on", overrideAutoStats: "false"},
	{name: "5node-dist-opt", numNodes: 5, overrideDistSQLMode: "on", overrideOptimizerMode: "on", overrideAutoStats: "false"},
	{name: "5node-dist-metadata", numNodes: 5, overrideDistSQLMode: "on", distSQLMetadataTestEnabled: true,
		skipShort: true, overrideOptimizerMode: "off"},
	{name: "5node-dist-disk", numNodes: 5, overrideDistSQLMode: "on", distSQLUseDisk: true, skipShort: true,
		overrideOptimizerMode: "off"},
}

func parseTestConfig(names []string) []logicTestConfigIdx {
	ret := make([]logicTestConfigIdx, len(names))
	for i, name := range names {
		idx, ok := findLogicTestConfig(name)
		if !ok {
			panic(fmt.Errorf("unknown config %s", name))
		}
		ret[i] = idx
	}
	return ret
}

var (
	defaultConfigNames = []string{
		"local",
		"local-opt",
		"fakedist",
		"fakedist-opt",
		"fakedist-metadata",
		"fakedist-disk",
	}
	defaultConfig = parseTestConfig(defaultConfigNames)
)

// An index in the above slice.
type logicTestConfigIdx int

func findLogicTestConfig(name string) (logicTestConfigIdx, bool) {
	for i, cfg := range logicTestConfigs {
		if cfg.name == name {
			return logicTestConfigIdx(i), true
		}
	}
	return -1, false
}

// lineScanner handles reading from input test files.
type lineScanner struct {
	*bufio.Scanner
	line int
	skip bool
}

func newLineScanner(r io.Reader) *lineScanner {
	return &lineScanner{
		Scanner: bufio.NewScanner(r),
		line:    0,
	}
}

func (l *lineScanner) Scan() bool {
	ok := l.Scanner.Scan()
	if ok {
		l.line++
	}
	return ok
}

func (l *lineScanner) Text() string {
	return l.Scanner.Text()
}

// logicStatement represents a single statement test in Test-Script.
type logicStatement struct {
	// file and line number of the test.
	pos string
	// SQL string to be sent to the database.
	sql string
	// expected error, if any. "" indicates the statement should
	// succeed.
	expectErr string
	// expected pgcode for the error, if any. "" indicates the
	// test does not check the pgwire error code.
	expectErrCode string
	// expected rows affected count. -1 to avoid testing this.
	expectCount int64
}

// readSQL reads the lines of a SQL statement or query until the first blank
// line or (optionally) a "----" separator, and sets stmt.sql.
//
// If a separator is found, returns separator=true. If a separator is found when
// it is not expected, returns an error.
func (ls *logicStatement) readSQL(
	t *logicTest, s *lineScanner, allowSeparator bool,
) (separator bool, _ error) {
	var buf bytes.Buffer
	hasVars := false
	for s.Scan() {
		line := s.Text()
		if !*rewriteSQL {
			t.emit(line)
		}
		substLine := t.substituteVars(line)
		if line != substLine {
			hasVars = true
			line = substLine
		}
		if line == "" {
			break
		}
		if line == "----" {
			separator = true
			if ls.expectErr != "" {
				return false, errors.Errorf(
					"%s: invalid ---- separator after a statement or query expecting an error: %s",
					ls.pos, ls.expectErr,
				)
			}
			if !allowSeparator {
				return false, errors.Errorf("%s: unexpected ---- separator", ls.pos)
			}
			break
		}
		fmt.Fprintln(&buf, line)
	}
	ls.sql = strings.TrimSpace(buf.String())
	if *rewriteSQL {
		if !hasVars {
			newSyntax, err := func(inSql string) (string, error) {
				// Can't rewrite the SQL otherwise because the vars make it invalid.
				stmtList, err := parser.Parse(inSql)
				if err != nil {
					if ls.expectErr != "" {
						// Maybe a parse error was expected. Simply do not rewrite.
						return inSql, nil
					}
					return "", errors.Wrapf(err, "%s: error while parsing SQL for reformat:\n%s", ls.pos, ls.sql)
				}
				var newSyntax bytes.Buffer
				pcfg := tree.DefaultPrettyCfg()
				pcfg.LineWidth = *sqlfmtLen
				pcfg.Simplify = false
				pcfg.UseTabs = false
				for i := range stmtList {
					if i > 0 {
						fmt.Fprintln(&newSyntax, ";")
					}
					fmt.Fprint(&newSyntax, pcfg.Pretty(stmtList[i].AST))
				}
				return newSyntax.String(), nil
			}(ls.sql)
			if err != nil {
				return false, err
			}
			ls.sql = newSyntax
		}

		t.emit(ls.sql)
		if separator {
			t.emit("----")
		} else {
			t.emit("")
		}
	}
	return separator, nil
}

// logicSorter sorts result rows (or not) depending on Test-Script's
// sorting option for a "query" test. See the implementation of the
// "query" directive below for details.
type logicSorter func(numCols int, values []string)

type rowSorter struct {
	numCols int
	numRows int
	values  []string
}

func (r rowSorter) row(i int) []string {
	return r.values[i*r.numCols : (i+1)*r.numCols]
}

func (r rowSorter) Len() int {
	return r.numRows
}

func (r rowSorter) Less(i, j int) bool {
	a := r.row(i)
	b := r.row(j)
	for k := range a {
		if a[k] < b[k] {
			return true
		}
		if a[k] > b[k] {
			return false
		}
	}
	return false
}

func (r rowSorter) Swap(i, j int) {
	a := r.row(i)
	b := r.row(j)
	for i := range a {
		a[i], b[i] = b[i], a[i]
	}
}

func rowSort(numCols int, values []string) {
	sort.Sort(rowSorter{
		numCols: numCols,
		numRows: len(values) / numCols,
		values:  values,
	})
}

func valueSort(numCols int, values []string) {
	sort.Strings(values)
}

// partialSort rearranges consecutive rows that have the same values on a
// certain set of columns (orderedCols).
//
// More specifically: rows are partitioned into groups of consecutive rows that
// have the same values for columns orderedCols. Inside each group, the rows are
// sorted. The relative order of any two rows that differ on orderedCols is
// preserved.
//
// This is useful when comparing results for a statement that guarantees a
// partial, but not a total order. Consider:
//
//   SELECT a, b FROM ab ORDER BY a
//
// Some possible outputs for the same data:
//   1 2        1 5        1 2
//   1 5        1 4        1 4
//   1 4   or   1 2   or   1 5
//   2 3        2 2        2 3
//   2 2        2 3        2 2
//
// After a partialSort with orderedCols = {0} all become:
//   1 2
//   1 4
//   1 5
//   2 2
//   2 3
//
// An incorrect output like:
//   1 5                          1 2
//   1 2                          1 5
//   2 3          becomes:        2 2
//   2 2                          2 3
//   1 4                          1 4
// and it is detected as different.
func partialSort(numCols int, orderedCols []int, values []string) {
	// We use rowSorter here only as a container.
	c := rowSorter{
		numCols: numCols,
		numRows: len(values) / numCols,
		values:  values,
	}

	// Sort the group of rows [rowStart, rowEnd).
	sortGroup := func(rowStart, rowEnd int) {
		sort.Sort(rowSorter{
			numCols: numCols,
			numRows: rowEnd - rowStart,
			values:  values[rowStart*numCols : rowEnd*numCols],
		})
	}

	groupStart := 0
	for rIdx := 1; rIdx < c.numRows; rIdx++ {
		// See if this row belongs in the group with the previous row.
		row := c.row(rIdx)
		start := c.row(groupStart)
		differs := false
		for _, i := range orderedCols {
			if start[i] != row[i] {
				differs = true
				break
			}
		}
		if differs {
			// Sort the group and start a new group with just this row in it.
			sortGroup(groupStart, rIdx)
			groupStart = rIdx
		}
	}
	sortGroup(groupStart, c.numRows)
}

// logicQuery represents a single query test in Test-Script.
type logicQuery struct {
	logicStatement

	// colTypes indicates the expected result column types.
	colTypes string
	// colNames controls the inclusion of column names in the query result.
	colNames bool
	// retry indicates if the query should be retried in case of failure with
	// exponential backoff up to some maximum duration.
	retry bool
	// some tests require the output to match modulo sorting.
	sorter logicSorter
	// expectedErr and expectedErrCode are as in logicStatement.

	// if set, the results are cross-checked against previous queries with the
	// same label.
	label string

	checkResults bool
	// valsPerLine is the number of values included in each line of the expected
	// results. This can either be 1, or else it must match the number of expected
	// columns (i.e. len(colTypes)).
	valsPerLine int
	// expectedResults indicates the expected sequence of text words
	// when flattening a query's results.
	expectedResults []string
	// expectedResultsRaw is the same as expectedResults, but
	// retaining the original formatting (whitespace, indentation) as
	// the test input file. This is used for pretty-printing unexpected
	// results.
	expectedResultsRaw []string
	// expectedHash indicates the expected hash of all result rows
	// combined. "" indicates hash checking is disabled.
	expectedHash string

	// expectedValues indicates the number of rows expected when
	// expectedHash is set.
	expectedValues int

	// rawOpts are the query options, before parsing. Used to display in error
	// messages.
	rawOpts string
}

// logicTest executes the test cases specified in a file. The file format is
// taken from the sqllogictest tool
// (http://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) with various
// extensions to allow specifying errors and additional options. See
// https://github.com/gregrahn/sqllogictest/ for a github mirror of the
// sqllogictest source.
type logicTest struct {
	t        *testing.T
	subtestT *testing.T
	cfg      testClusterConfig
	// the number of nodes in the cluster.
	cluster serverutils.TestClusterInterface
	// the index of the node (within the cluster) against which we run the test
	// statements.
	nodeIdx int
	// map of built clients. Needs to be persisted so that we can
	// re-use them and close them all on exit.
	clients map[string]*gosql.DB
	// client currently in use. This can change during processing
	// of a test input file when encountering the "user" directive.
	// see setUser() for details.
	user         string
	db           *gosql.DB
	cleanupFuncs []func()
	// progress holds the number of tests executed so far.
	progress int
	// failures holds the number of tests failed so far, when
	// -try-harder is set.
	failures int
	// unsupported holds the number of queries ignored due
	// to prepare errors, when -allow-prepare-fail is set.
	unsupported int
	// lastProgress is used for the progress indicator message.
	lastProgress time.Time
	// traceFile holds the current trace file between "traceon"
	// and "traceoff" directives.
	traceFile *os.File
	// verbose indicate whether -v was passed.
	verbose bool
	// perErrorSummary retains the per-error list of failing queries
	// when -error-summary is set.
	perErrorSummary map[string][]string
	// labelMap retains the expected result hashes that have
	// been marked using a result label in the input. See the
	// explanation for labels in processInputFiles().
	labelMap map[string]string

	// varMap remembers the variables set with "let".
	varMap map[string]string

	rewriteResTestBuf bytes.Buffer

	curPath   string
	curLineNo int
}

func (t *logicTest) traceStart(filename string) {
	if t.traceFile != nil {
		t.Fatalf("tracing already active")
	}
	var err error
	t.traceFile, err = os.Create(filename)
	if err != nil {
		t.Fatalf("unable to open trace output file: %s", err)
	}
	if err := trace.Start(t.traceFile); err != nil {
		t.Fatalf("unable to start tracing: %s", err)
	}
}

func (t *logicTest) traceStop() {
	if t.traceFile != nil {
		trace.Stop()
		t.traceFile.Close()
		t.traceFile = nil
	}
}

// substituteVars replaces all occurrences of "$abc", where "abc" is a variable
// previously defined by a let, with the value of that variable.
func (t *logicTest) substituteVars(line string) string {
	if len(t.varMap) == 0 {
		return line
	}

	// See if there are any $vars to replace.
	return varRE.ReplaceAllStringFunc(line, func(varName string) string {
		if replace, ok := t.varMap[varName]; ok {
			return replace
		}
		return line
	})
}

// emit is used for the --generate-testfiles mode; it emits a line of testfile.
func (t *logicTest) emit(line string) {
	if *rewriteResultsInTestfiles || *rewriteSQL {
		t.rewriteResTestBuf.WriteString(line)
		t.rewriteResTestBuf.WriteString("\n")
	}
}

func (t *logicTest) close() {
	t.traceStop()

	for _, cleanup := range t.cleanupFuncs {
		cleanup()
	}
	t.cleanupFuncs = nil

	if t.cluster != nil {
		t.cluster.Stopper().Stop(context.TODO())
		t.cluster = nil
	}
	if t.clients != nil {
		for _, c := range t.clients {
			c.Close()
		}
		t.clients = nil
	}
	t.db = nil
}

// out emits a message both on stdout and the log files if
// verbose is set.
func (t *logicTest) outf(format string, args ...interface{}) {
	if t.verbose {
		fmt.Printf(format, args...)
		fmt.Println()
		log.Infof(context.Background(), format, args...)
	}
}

// setUser sets the DB client to the specified user.
// It returns a cleanup function to be run when the credentials
// are no longer needed.
func (t *logicTest) setUser(user string) func() {
	if t.clients == nil {
		t.clients = map[string]*gosql.DB{}
	}
	if db, ok := t.clients[user]; ok {
		t.db = db
		t.user = user

		// No cleanup necessary, but return a no-op func to avoid nil pointer dereference.
		return func() {}
	}

	addr := t.cluster.Server(t.nodeIdx).ServingAddr()
	pgURL, cleanupFunc := sqlutils.PGUrl(t.t, addr, "TestLogic", url.User(user))
	pgURL.Path = "test"
	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	// Enable the cost-based optimizer rather than the heuristic planner.
	if optMode := t.cfg.overrideOptimizerMode; optMode != "" {
		if _, err := db.Exec(fmt.Sprintf("SET OPTIMIZER = %s;", optMode)); err != nil {
			t.Fatal(err)
		}
	}
	// The default value for extra_float_digits assumed by tests is
	// 0. However, lib/pq by default configures this to 2 during
	// connection initialization, so we need to set it back to 0 before
	// we run anything.
	if _, err := db.Exec("SET extra_float_digits = 0"); err != nil {
		t.Fatal(err)
	}
	t.clients[user] = db
	t.db = db
	t.user = user

	return cleanupFunc
}

func (t *logicTest) setup(cfg testClusterConfig) {
	t.cfg = cfg
	// TODO(pmattis): Add a flag to make it easy to run the tests against a local
	// MySQL or Postgres instance.
	// TODO(andrei): if createTestServerParams() is used here, the command filter
	// it installs detects a transaction that doesn't have
	// modifiedSystemConfigSpan set even though it should, for
	// "testdata/rename_table". Figure out what's up with that.
	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// Specify a fixed memory limit (some test cases verify OOM conditions; we
			// don't want those to take long on large machines).
			SQLMemoryPoolSize: 192 * 1024 * 1024,
			Knobs: base.TestingKnobs{
				Store: &storage.StoreTestingKnobs{
					// The consistency queue makes a lot of noisy logs during logic tests.
					DisableConsistencyQueue: true,
				},
				SQLEvalContext: &tree.EvalContextTestingKnobs{
					AssertBinaryExprReturnTypes:     true,
					AssertUnaryExprReturnTypes:      true,
					AssertFuncExprReturnTypes:       true,
					DisableOptimizerRuleProbability: *disableOptRuleProbability,
					OptimizerCostPerturbation:       *optimizerCostPerturbation,
				},
			},
			UseDatabase: "test",
			// Set Locality so we can use it in zone config tests.
			Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "test"}}},
		},
		// For distributed SQL tests, we use the fake span resolver; it doesn't
		// matter where the data really is.
		ReplicationMode: base.ReplicationManual,
	}

	distSQLKnobs := &distsqlrun.TestingKnobs{
		MetadataTestLevel: distsqlrun.Off, DeterministicStats: true,
	}
	if cfg.distSQLUseDisk {
		distSQLKnobs.MemoryLimitBytes = 1
	}
	if cfg.distSQLMetadataTestEnabled {
		distSQLKnobs.MetadataTestLevel = distsqlrun.On
	}
	params.ServerArgs.Knobs.DistSQL = distSQLKnobs
	if cfg.bootstrapVersion != (cluster.ClusterVersion{}) {
		params.ServerArgs.Knobs.Store.(*storage.StoreTestingKnobs).BootstrapVersion = &cfg.bootstrapVersion
	}
	if cfg.disableUpgrade {
		if params.ServerArgs.Knobs.Server == nil {
			params.ServerArgs.Knobs.Server = &server.TestingKnobs{}
		}
		params.ServerArgs.Knobs.Server.(*server.TestingKnobs).DisableAutomaticVersionUpgrade = 1
	}

	if cfg.serverVersion != (roachpb.Version{}) {
		// If we want to run a specific server version, we assume that it
		// supports at least the bootstrap version.
		paramsPerNode := map[int]base.TestServerArgs{}
		minVersion := cfg.serverVersion
		if cfg.bootstrapVersion != (cluster.ClusterVersion{}) {
			minVersion = cfg.bootstrapVersion.Version
		}
		for i := 0; i < cfg.numNodes; i++ {
			nodeParams := params.ServerArgs
			nodeParams.Settings = cluster.MakeClusterSettings(minVersion, cfg.serverVersion)
			paramsPerNode[i] = nodeParams
		}
		params.ServerArgsPerNode = paramsPerNode
	}

	// Update the defaults for automatic statistics to avoid delays in testing.
	// Avoid making the DefaultAsOfTime too small to avoid interacting with
	// schema changes and causing transaction retries.
	// TODO(radu): replace these with testing knobs.
	stats.DefaultAsOfTime = 10 * time.Millisecond
	stats.DefaultRefreshInterval = time.Millisecond

	t.cluster = serverutils.StartTestCluster(t.t, cfg.numNodes, params)
	if cfg.useFakeSpanResolver {
		fakeResolver := distsqlutils.FakeResolverForTestCluster(t.cluster)
		t.cluster.Server(t.nodeIdx).SetDistSQLSpanResolver(fakeResolver)
	}

	if _, err := t.cluster.ServerConn(0).Exec(
		"SET CLUSTER SETTING sql.stats.automatic_collection.min_stale_rows = $1::int", 5,
	); err != nil {
		t.Fatal(err)
	}

	if cfg.overrideDistSQLMode != "" {
		if _, err := t.cluster.ServerConn(0).Exec(
			"SET CLUSTER SETTING sql.defaults.distsql = $1::string", cfg.overrideDistSQLMode,
		); err != nil {
			t.Fatal(err)
		}
		_, ok := sessiondata.DistSQLExecModeFromString(cfg.overrideDistSQLMode)
		if !ok {
			t.Fatalf("invalid distsql mode override: %s", cfg.overrideDistSQLMode)
		}
		// Wait until all servers are aware of the setting.
		testutils.SucceedsSoon(t.t, func() error {
			for i := 0; i < t.cluster.NumServers(); i++ {
				var m string
				err := t.cluster.ServerConn(i % t.cluster.NumServers()).QueryRow(
					"SHOW CLUSTER SETTING sql.defaults.distsql",
				).Scan(&m)
				if err != nil {
					t.Fatal(errors.Wrapf(err, "%d", i))
				}
				if m != cfg.overrideDistSQLMode {
					return errors.Errorf("node %d is still waiting for update of DistSQLMode to %s (have %s)",
						i, cfg.overrideDistSQLMode, m,
					)
				}
			}
			return nil
		})
	}
	if cfg.overrideExpVectorize != "" {
		if _, err := t.cluster.ServerConn(0).Exec(
			"SET CLUSTER SETTING sql.defaults.experimental_vectorize = $1::string", cfg.overrideExpVectorize,
		); err != nil {
			t.Fatal(err)
		}
	}
	if cfg.overrideAutoStats != "" {
		if _, err := t.cluster.ServerConn(0).Exec(
			"SET CLUSTER SETTING sql.stats.automatic_collection.enabled = $1::bool", cfg.overrideAutoStats,
		); err != nil {
			t.Fatal(err)
		}
	} else {
		// Background stats collection is enabled by default, but we've seen tests
		// flake with it on. When the issue manifests, it seems to be around a
		// schema change transaction getting pushed, which causes it to increment a
		// table ID twice instead of once, causing non-determinism.
		//
		// In the short term, we disable auto stats by default to avoid the flakes.
		//
		// In the long run, these tests should be running with default settings as
		// much as possible, so we likely want to address this. Two options are
		// either making schema changes more resilient to being pushed or possibly
		// making auto stats avoid pushing schema change transactions. There might
		// be other better alternatives than these.
		//
		// See #37751 for details.
		if _, err := t.cluster.ServerConn(0).Exec(
			"SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false",
		); err != nil {
			t.Fatal(err)
		}
	}

	// db may change over the lifetime of this function, with intermediate
	// values cached in t.clients and finally closed in t.close().
	t.cleanupFuncs = append(t.cleanupFuncs, t.setUser(security.RootUser))

	if _, err := t.db.Exec(`
CREATE DATABASE test;
`); err != nil {
		t.Fatal(err)
	}

	if _, err := t.db.Exec(fmt.Sprintf("CREATE USER %s;", server.TestUser)); err != nil {
		t.Fatal(err)
	}

	t.labelMap = make(map[string]string)
	t.varMap = make(map[string]string)

	t.progress = 0
	t.failures = 0
	t.unsupported = 0
}

// readTestFileConfigs reads any LogicTest directive at the beginning of a
// test file. A line that starts with "# LogicTest:" specifies a list of
// configuration names. The test file is run against each of those
// configurations.
//
// Example:
//   # LogicTest: default distsql
//
// If the file doesn't contain a directive, the default config is returned.
func readTestFileConfigs(t *testing.T, path string) []logicTestConfigIdx {
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	s := newLineScanner(file)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) == 0 {
			continue
		}
		cmd := fields[0]
		if !strings.HasPrefix(cmd, "#") {
			// Stop at the first line that's not a comment (or empty).
			break
		}
		// Directive lines are of the form:
		// # LogicTest: opt1=val1 opt2=val3 boolopt1
		if len(fields) > 1 && cmd == "#" && fields[1] == "LogicTest:" {
			if len(fields) == 2 {
				t.Fatalf("%s: empty LogicTest directive", path)
			}
			var configs []logicTestConfigIdx
			for _, configName := range fields[2:] {
				idx, ok := findLogicTestConfig(configName)
				if !ok {
					t.Fatalf("%s: unknown config name %s", path, configName)
				}
				configs = append(configs, idx)
			}
			return configs
		}
	}
	// No directive found, return the default config.
	return defaultConfig
}

type subtestDetails struct {
	name                  string        // the subtest's name, empty if not a subtest
	buffer                *bytes.Buffer // a chunk of the test file representing the subtest
	lineLineIndexIntoFile int           // the line number of the test file where the subtest started
}

func (t *logicTest) processTestFile(path string, config testClusterConfig) error {
	subtests, err := fetchSubtests(path)
	if err != nil {
		return err
	}

	if *showSQL {
		t.outf("--- queries start here (file: %s)", path)
	}
	defer t.printCompletion(path, config)

	for _, subtest := range subtests {
		if *maxErrs > 0 && t.failures >= *maxErrs {
			break
		}
		// If subtest has no name, then it is not a subtest, so just run the lines
		// in the overall test. Note that this can only happen in the first subtest.
		if len(subtest.name) == 0 {
			if err := t.processSubtest(subtest, path, config); err != nil {
				return err
			}
		} else {
			t.emit(fmt.Sprintf("subtest %s", subtest.name))
			t.t.Run(subtest.name, func(subtestT *testing.T) {
				t.subtestT = subtestT
				defer func() {
					t.subtestT = nil
				}()
				if err := t.processSubtest(subtest, path, config); err != nil {
					t.Error(err)
				}
			})
		}
	}

	if (*rewriteResultsInTestfiles || *rewriteSQL) && !t.t.Failed() {
		// Rewrite the test file.
		file, err := os.Create(path)
		if err != nil {
			return err
		}
		defer file.Close()
		// Remove any trailing blank line.
		data := t.rewriteResTestBuf.String()
		if l := len(data); l > 2 && data[l-1] == '\n' && data[l-2] == '\n' {
			data = data[:l-1]
		}
		fmt.Fprint(file, data)
	}

	return nil
}

// fetchSubtests reads through the test file and splices it into subtest chunks.
// If there is no subtest, the output will only contain a single entry.
func fetchSubtests(path string) ([]subtestDetails, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	s := newLineScanner(file)
	var subtests []subtestDetails
	var curName string
	var curLineIndexIntoFile int
	buffer := &bytes.Buffer{}
	for s.Scan() {
		line := s.Text()
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == "subtest" {
			if len(fields) != 2 {
				return nil, errors.Errorf(
					"%s:%d expected only one field following the subtest command\n"+
						"Note that this check does not respect the other commands so if a query result has a "+
						"line that starts with \"subtest\" it will either fail or be split into a subtest.",
					path, s.line,
				)
			}
			subtests = append(subtests, subtestDetails{
				name:                  curName,
				buffer:                buffer,
				lineLineIndexIntoFile: curLineIndexIntoFile,
			})
			buffer = &bytes.Buffer{}
			curName = fields[1]
			curLineIndexIntoFile = s.line + 1
		} else {
			buffer.WriteString(line)
			buffer.WriteRune('\n')
		}
	}
	subtests = append(subtests, subtestDetails{
		name:                  curName,
		buffer:                buffer,
		lineLineIndexIntoFile: curLineIndexIntoFile,
	})

	return subtests, nil
}

func (t *logicTest) processSubtest(
	subtest subtestDetails, path string, config testClusterConfig,
) error {
	defer t.traceStop()

	s := newLineScanner(subtest.buffer)
	t.lastProgress = timeutil.Now()

	repeat := 1
	for s.Scan() {
		t.curPath, t.curLineNo = path, s.line+subtest.lineLineIndexIntoFile
		if *maxErrs > 0 && t.failures >= *maxErrs {
			return errors.Errorf("%s:%d: too many errors encountered, skipping the rest of the input",
				path, s.line+subtest.lineLineIndexIntoFile,
			)
		}
		line := s.Text()
		t.emit(line)
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		cmd := fields[0]
		if strings.HasPrefix(cmd, "#") {
			// Skip comment lines.
			continue
		}
		if len(fields) == 2 && fields[1] == "error" {
			return errors.Errorf("%s:%d: no expected error provided",
				path, s.line+subtest.lineLineIndexIntoFile,
			)
		}
		switch cmd {
		case "repeat":
			// A line "repeat X" makes the test repeat the following statement or query X times.
			var err error
			count := 0
			if len(fields) != 2 {
				err = errors.New("invalid line format")
			} else if count, err = strconv.Atoi(fields[1]); err == nil && count < 2 {
				err = errors.New("invalid count")
			}
			if err != nil {
				return errors.Errorf("%s:%d invalid repeat line: %s",
					path, s.line+subtest.lineLineIndexIntoFile, err,
				)
			}
			repeat = count

		case "sleep":
			var err error
			var duration time.Duration
			// A line "sleep Xs" makes the test sleep for X seconds.
			if len(fields) != 2 {
				err = errors.New("invalid line format")
			} else if duration, err = time.ParseDuration(fields[1]); err != nil {
				err = errors.New("invalid duration")
			}
			if err != nil {
				return errors.Errorf("%s:%d invalid sleep line: %s",
					path, s.line+subtest.lineLineIndexIntoFile, err,
				)
			}
			time.Sleep(duration)

		case "statement":
			stmt := logicStatement{
				pos:         fmt.Sprintf("\n%s:%d", path, s.line+subtest.lineLineIndexIntoFile),
				expectCount: -1,
			}
			// Parse "statement error <regexp>"
			if m := errorRE.FindStringSubmatch(s.Text()); m != nil {
				stmt.expectErrCode = m[1]
				stmt.expectErr = m[2]
			}
			if len(fields) >= 3 && fields[1] == "count" {
				n, err := strconv.ParseInt(fields[2], 10, 64)
				if err != nil {
					return err
				}
				stmt.expectCount = n
			}
			if _, err := stmt.readSQL(t, s, false /* allowSeparator */); err != nil {
				return err
			}
			if !s.skip {
				for i := 0; i < repeat; i++ {
					if cont, err := t.execStatement(stmt); err != nil {
						if !cont {
							return err
						}
						t.Error(err)
					}
				}
			} else {
				s.skip = false
			}
			repeat = 1
			t.success(path)

		case "query":
			var query logicQuery
			query.pos = fmt.Sprintf("\n%s:%d", path, s.line+subtest.lineLineIndexIntoFile)
			// Parse "query error <regexp>"
			if m := errorRE.FindStringSubmatch(s.Text()); m != nil {
				query.expectErrCode = m[1]
				query.expectErr = m[2]
			} else if len(fields) < 2 {
				return errors.Errorf("%s: invalid test statement: %s", query.pos, s.Text())
			} else {
				// Parse "query <type-string> <options> <label>"
				query.colTypes = fields[1]
				if *bigtest {
					// bigtests put each expected value on its own line.
					query.valsPerLine = 1
				} else {
					// Otherwise, expect number of values to match expected type count.
					query.valsPerLine = len(query.colTypes)
				}

				if len(fields) >= 3 {
					query.rawOpts = fields[2]

					tokens := strings.Split(query.rawOpts, ",")

					// One of the options can be partialSort(1,2,3); we want this to be
					// a single token.
					for i := 0; i < len(tokens)-1; i++ {
						if strings.HasPrefix(tokens[i], "partialsort(") && !strings.HasSuffix(tokens[i], ")") {
							// Merge this token with the next.
							tokens[i] = tokens[i] + "," + tokens[i+1]
							// Delete tokens[i+1].
							copy(tokens[i+1:], tokens[i+2:])
							tokens = tokens[:len(tokens)-1]
							// Look at the new token again.
							i--
						}
					}

					for _, opt := range tokens {
						if strings.HasPrefix(opt, "partialsort(") && strings.HasSuffix(opt, ")") {
							s := opt
							s = strings.TrimPrefix(s, "partialsort(")
							s = strings.TrimSuffix(s, ")")

							var orderedCols []int
							for _, c := range strings.Split(s, ",") {
								colIdx, err := strconv.Atoi(c)
								if err != nil || colIdx < 1 {
									return errors.Errorf("%s: invalid sort mode: %s", query.pos, opt)
								}
								orderedCols = append(orderedCols, colIdx-1)
							}
							if len(orderedCols) == 0 {
								return errors.Errorf("%s: invalid sort mode: %s", query.pos, opt)
							}
							query.sorter = func(numCols int, values []string) {
								partialSort(numCols, orderedCols, values)
							}
							continue
						}

						switch opt {
						case "nosort":
							query.sorter = nil

						case "rowsort":
							query.sorter = rowSort

						case "valuesort":
							query.sorter = valueSort

						case "colnames":
							query.colNames = true

						case "retry":
							query.retry = true

						default:
							return errors.Errorf("%s: unknown sort mode: %s", query.pos, opt)
						}
					}
				}
				if len(fields) >= 4 {
					query.label = fields[3]
				}
			}

			separator, err := query.readSQL(t, s, true /* allowSeparator */)
			if err != nil {
				return err
			}

			query.checkResults = true
			if separator {
				// Query results are either a space separated list of values up to a
				// blank line or a line of the form "xx values hashing to yyy". The
				// latter format is used by sqllogictest when a large number of results
				// match the query.
				if s.Scan() {
					if m := resultsRE.FindStringSubmatch(s.Text()); m != nil {
						var err error
						query.expectedValues, err = strconv.Atoi(m[1])
						if err != nil {
							return err
						}
						query.expectedHash = m[2]
						query.checkResults = false
					} else {
						for {
							// Normalize each expected row by discarding leading/trailing
							// whitespace and by replacing each run of contiguous whitespace
							// with a single space.
							query.expectedResultsRaw = append(query.expectedResultsRaw, s.Text())
							results := strings.Fields(s.Text())
							if len(results) == 0 {
								break
							}

							if query.sorter == nil {
								// When rows don't need to be sorted, then always compare by
								// tokens, regardless of where row/column boundaries are.
								query.expectedResults = append(query.expectedResults, results...)
							} else {
								// It's important to know where row/column boundaries are in
								// order to correctly perform sorting. Assume that boundaries
								// are delimited by whitespace. This means that values cannot
								// contain whitespace when there are multiple columns, since
								// that would be interpreted as extra values:
								//
								//   foo bar baz
								//
								// If there are two expected columns, then it's not possible
								// to know whether the expected results are ("foo bar", "baz")
								// or ("foo", "bar baz"), so just error in that case.
								if query.valsPerLine == 1 {
									// Only one expected value per line, so never ambiguous,
									// even if there is whitespace in the value.
									query.expectedResults = append(query.expectedResults, strings.Join(results, " "))
								} else {
									// Don't error if --rewrite is specified, since the expected
									// results are ignored in that case.
									if !*rewriteResultsInTestfiles && len(results) != len(query.colTypes) {
										return errors.Errorf("expected results are invalid: unexpected column count")
									}
									query.expectedResults = append(query.expectedResults, results...)
								}
							}

							if !s.Scan() {
								break
							}
						}
					}
				}
			} else if query.label != "" {
				// Label and no separator; we won't be directly checking results; we
				// cross-check results between all queries with the same label.
				query.checkResults = false
			}

			if !s.skip {
				for i := 0; i < repeat; i++ {
					if query.retry && !*rewriteResultsInTestfiles {
						testutils.SucceedsSoon(t.t, func() error {
							return t.execQuery(query)
						})
					} else {
						if query.retry && *rewriteResultsInTestfiles {
							// The presence of the retry flag indicates that we expect this
							// query may need some time to succeed. If we are rewriting, wait
							// 500ms before executing the query.
							// TODO(rytaft): We may want to make this sleep time configurable.
							time.Sleep(time.Millisecond * 500)
						}
						if err := t.execQuery(query); err != nil {
							t.Error(err)
						}
					}
				}
			} else {
				s.skip = false
			}
			repeat = 1
			t.success(path)

		case "let":
			// let $<name>
			// <query>
			if len(fields) != 2 {
				return errors.Errorf("let command requires one argument, found: %v", fields)
			}
			varName := fields[1]
			if !varRE.MatchString(varName) {
				return errors.Errorf("invalid target name for let: %s", varName)
			}

			stmt := logicStatement{
				pos: fmt.Sprintf("\n%s:%d", path, s.line+subtest.lineLineIndexIntoFile),
			}
			if _, err := stmt.readSQL(t, s, false /* allowSeparator */); err != nil {
				return err
			}
			rows, err := t.db.Query(stmt.sql)
			if err != nil {
				return errors.Wrapf(err, "%s: error running query %s", stmt.pos, stmt.sql)
			}
			if !rows.Next() {
				return errors.Errorf("%s: no rows returned by query %s", stmt.pos, stmt.sql)
			}
			var val string
			if err := rows.Scan(&val); err != nil {
				return errors.Wrapf(err, "%s: error getting result from query %s", stmt.pos, stmt.sql)
			}
			if rows.Next() {
				return errors.Errorf("%s: more than one row returned by query  %s", stmt.pos, stmt.sql)
			}
			t.varMap[varName] = val

		case "halt", "hash-threshold":

		case "user":
			if len(fields) < 2 {
				return errors.Errorf("user command requires one argument, found: %v", fields)
			}
			if len(fields[1]) == 0 {
				return errors.Errorf("user command requires a non-blank argument")
			}
			cleanupUserFunc := t.setUser(fields[1])
			defer cleanupUserFunc()

		case "skipif":
			if len(fields) < 2 {
				return errors.Errorf("skipif command requires one argument, found: %v", fields)
			}
			switch fields[1] {
			case "":
				return errors.Errorf("skipif command requires a non-blank argument")
			case "mysql", "mssql":
			case "postgresql", "cockroachdb":
				s.skip = true
				continue
			default:
				return errors.Errorf("unimplemented test statement: %s", s.Text())
			}

		case "onlyif":
			if len(fields) < 2 {
				return errors.Errorf("onlyif command requires one argument, found: %v", fields)
			}
			switch fields[1] {
			case "":
				return errors.New("onlyif command requires a non-blank argument")
			case "cockroachdb":
			case "mysql":
				s.skip = true
				continue
			case "mssql":
				s.skip = true
				continue
			default:
				return errors.Errorf("unimplemented test statement: %s", s.Text())
			}

		case "traceon":
			if len(fields) != 2 {
				return errors.Errorf("traceon requires a filename argument, found: %v", fields)
			}
			t.traceStart(fields[1])

		case "traceoff":
			if t.traceFile == nil {
				return errors.Errorf("no trace active")
			}
			t.traceStop()

		case "kv-batch-size":
			// kv-batch-size limits the kvfetcher batch size. It can be used to
			// trigger certain error conditions around limited batches.
			if len(fields) != 2 {
				return errors.Errorf(
					"kv-batch-size needs an integer argument, found: %v",
					fields[1:],
				)
			}
			batchSize, err := strconv.Atoi(fields[1])
			if err != nil {
				return errors.Errorf("kv-batch-size needs an integer argument; %s", err)
			}
			t.outf("Setting kv batch size %d", batchSize)
			defer row.SetKVBatchSize(int64(batchSize))()

		default:
			return errors.Errorf("%s:%d: unknown command: %s",
				path, s.line+subtest.lineLineIndexIntoFile, cmd,
			)
		}
	}
	return s.Err()
}

// verifyError checks that either no error was found where none was
// expected, or that an error was found when one was expected.
// Returns a nil error to indicate the behavior was as expected.  If
// non-nil, returns also true in the boolean flag whether it is safe
// to continue (i.e. an error was expected, an error was obtained, and
// the errors didn't match).
func (t *logicTest) verifyError(
	sql, pos, expectErr, expectErrCode string, err error,
) (bool, error) {
	if expectErr == "" && expectErrCode == "" && err != nil {
		cont := t.unexpectedError(sql, pos, err)
		if cont {
			// unexpectedError() already reported via t.Errorf. no need for more.
			err = nil
		}
		return cont, err
	}
	if !testutils.IsError(err, expectErr) {
		if err == nil {
			newErr := errors.Errorf("%s: %s\nexpected %q, but no error occurred", pos, sql, expectErr)
			return false, newErr
		}

		errString := pgerror.FullError(err)
		newErr := errors.Errorf("%s: %s\nexpected:\n%s\n\ngot:\n%s", pos, sql, expectErr, errString)
		if err != nil && strings.Contains(errString, expectErr) {
			if t.subtestT != nil {
				t.subtestT.Logf("The output string contained the input regexp. Perhaps you meant to write:\n"+
					"query error %s", regexp.QuoteMeta(errString))
			} else {
				t.t.Logf("The output string contained the input regexp. Perhaps you meant to write:\n"+
					"query error %s", regexp.QuoteMeta(errString))
			}
		}
		return (err == nil) == (expectErr == ""), newErr
	}
	if err != nil {
		pqErr, ok := err.(*pq.Error)
		if ok &&
			strings.HasPrefix(string(pqErr.Code), "XX" /* internal error, corruption, etc */) &&
			string(pqErr.Code) != pgcode.Uncategorized /* this is also XX but innocuous */ {
			if expectErrCode != string(pqErr.Code) {
				return false, errors.Errorf(
					"%s: %s: serious error with code %q occurred; if expected, must use 'error pgcode %s ...' in test:\n%s",
					pos, sql, pqErr.Code, pqErr.Code, pgerror.FullError(err))
			}
		}
	}
	if expectErrCode != "" {
		if err != nil {
			pqErr, ok := err.(*pq.Error)
			if !ok {
				newErr := errors.Errorf("%s %s\n: expected error code %q, but the error we found is not "+
					"a libpq error: %s", pos, sql, expectErrCode, err)
				return true, newErr
			}
			if pqErr.Code != pq.ErrorCode(expectErrCode) {
				newErr := errors.Errorf("%s: %s\nexpected error code %q, but found code %q (%s)",
					pos, sql, expectErrCode, pqErr.Code, pqErr.Code.Name())
				return true, newErr
			}
		} else {
			newErr := errors.Errorf("%s: %s\nexpected error code %q, but found success",
				pos, sql, expectErrCode)
			return (err != nil), newErr
		}
	}
	return true, nil
}

// formatErr attempts to provide more details if present.
func formatErr(err error) string {
	if pqErr, ok := err.(*pq.Error); ok {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "(%s) %s", pqErr.Code, pqErr.Message)
		if pqErr.File != "" || pqErr.Line != "" || pqErr.Routine != "" {
			fmt.Fprintf(&buf, "\n%s:%s: in %s()", pqErr.File, pqErr.Line, pqErr.Routine)
		}
		if pqErr.Detail != "" {
			fmt.Fprintf(&buf, "\nDETAIL: %s", pqErr.Detail)
		}
		if pqErr.Code == pgcode.Internal {
			fmt.Fprintln(&buf, "\nNOTE: internal errors may have more details in logs. Use -show-logs.")
		}
		return buf.String()
	}
	return err.Error()
}

// unexpectedError handles ignoring queries that fail during prepare
// when -allow-prepare-fail is specified. The argument "sql" is "" to indicate the
// work is done on behalf of a statement, which always fail upon an
// unexpected error.
func (t *logicTest) unexpectedError(sql string, pos string, err error) bool {
	if *allowPrepareFail && sql != "" {
		// This is a query and -allow-prepare-fail is set.  Try to prepare
		// the query. If prepare fails, this means we (probably) do not
		// support the input syntax, and -allow-prepare-fail instructs us
		// to ignore the unexpected error.
		stmt, err := t.db.Prepare(sql)
		if err != nil {
			if *showSQL {
				t.outf("\t-- fails prepare: %s", formatErr(err))
			}
			t.signalIgnoredError(err, pos, sql)
			return true
		}
		if err := stmt.Close(); err != nil {
			t.Errorf("%s: %s\nerror when closing prepared statement: %s", sql, pos, formatErr(err))
		}
	}
	t.Errorf("%s: %s\nexpected success, but found\n%s", pos, sql, formatErr(err))
	return false
}

func (t *logicTest) execStatement(stmt logicStatement) (bool, error) {
	if *showSQL {
		t.outf("%s;", stmt.sql)
	}
	res, err := t.db.Exec(stmt.sql)
	if err == nil {
		sqlutils.VerifyStatementPrettyRoundtrip(t.t, stmt.sql)
	}
	if err == nil && stmt.expectCount >= 0 {
		var count int64
		count, err = res.RowsAffected()

		// If err becomes non-nil here, we'll catch it below.

		if err == nil && count != stmt.expectCount {
			t.Errorf("%s: %s\nexpected %d rows affected, got %d", stmt.pos, stmt.sql, stmt.expectCount, count)
		}
	}

	// General policy for failing vs. continuing:
	// - we want to do as much work as possible;
	// - however, a statement that fails when it should succeed or
	//   a statement that succeeds when it should fail may have left
	//   the database in an improper state, so we stop there;
	// - error on expected error is worth going further, even
	//   if the obtained error does not match the expected error.
	cont, err := t.verifyError("", stmt.pos, stmt.expectErr, stmt.expectErrCode, err)
	if err != nil {
		t.finishOne("OK")
	}
	return cont, err
}

func (t *logicTest) hashResults(results []string) (string, error) {
	// Hash the values using MD5. This hashing precisely matches the hashing in
	// sqllogictest.c.
	h := md5.New()
	for _, r := range results {
		if _, err := h.Write(append([]byte(r), byte('\n'))); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func (t *logicTest) execQuery(query logicQuery) error {
	if *showSQL {
		t.outf("%s;", query.sql)
	}
	rows, err := t.db.Query(query.sql)
	if err == nil {
		sqlutils.VerifyStatementPrettyRoundtrip(t.t, query.sql)
	}
	if _, err := t.verifyError(query.sql, query.pos, query.expectErr, query.expectErrCode, err); err != nil {
		return err
	}
	if err != nil {
		// An error occurred, but it was expected.
		t.finishOne("XFAIL")
		return nil
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	if len(query.colTypes) != len(cols) {
		return fmt.Errorf("%s: expected %d columns, but found %d",
			query.pos, len(query.colTypes), len(cols))
	}
	vals := make([]interface{}, len(cols))
	for i := range vals {
		vals[i] = new(interface{})
	}

	var actualResultsRaw []string
	if query.colNames {
		actualResultsRaw = append(actualResultsRaw, cols...)
	}
	for rows.Next() {
		if err := rows.Scan(vals...); err != nil {
			return err
		}
		for i, v := range vals {
			if val := *v.(*interface{}); val != nil {
				valT := reflect.TypeOf(val).Kind()
				colT := query.colTypes[i]
				switch colT {
				case 'T':
					if valT != reflect.String && valT != reflect.Slice && valT != reflect.Struct {
						return fmt.Errorf("%s: expected text value for column %d, but found %T: %#v",
							query.pos, i, val, val,
						)
					}
				case 'I':
					if valT != reflect.Int64 {
						if *flexTypes && (valT == reflect.Float64 || valT == reflect.Slice) {
							t.signalIgnoredError(
								fmt.Errorf("result type mismatch: expected I, got %T", val), query.pos, query.sql,
							)
							return nil
						}
						return fmt.Errorf("%s: expected int value for column %d, but found %T: %#v",
							query.pos, i, val, val,
						)
					}
				case 'R':
					if valT != reflect.Float64 && valT != reflect.Slice {
						if *flexTypes && (valT == reflect.Int64) {
							t.signalIgnoredError(
								fmt.Errorf("result type mismatch: expected R, got %T", val), query.pos, query.sql,
							)
							return nil
						}
						return fmt.Errorf("%s: expected float/decimal value for column %d, but found %T: %#v",
							query.pos, i, val, val,
						)
					}
				case 'B':
					if valT != reflect.Bool {
						return fmt.Errorf("%s: expected boolean value for column %d, but found %T: %#v",
							query.pos, i, val, val,
						)
					}
				case 'O':
					if valT != reflect.Slice {
						return fmt.Errorf("%s: expected oid value for column %d, but found %T: %#v",
							query.pos, i, val, val,
						)
					}
				default:
					return fmt.Errorf("%s: unknown type in type string: %c in %s",
						query.pos, colT, query.colTypes,
					)
				}

				if byteArray, ok := val.([]byte); ok {
					// The postgres wire protocol does not distinguish between
					// strings and byte arrays, but our tests do. In order to do
					// The Right Thing™, we replace byte arrays which are valid
					// UTF-8 with strings. This allows byte arrays which are not
					// valid UTF-8 to print as a list of bytes (e.g. `[124 107]`)
					// while printing valid strings naturally.
					if str := string(byteArray); utf8.ValidString(str) {
						val = str
					}
				}
				// Empty strings are rendered as "·" (middle dot)
				if val == "" {
					val = "·"
				}
				actualResultsRaw = append(actualResultsRaw, fmt.Sprint(val))
			} else {
				actualResultsRaw = append(actualResultsRaw, "NULL")
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Normalize each row in the result by mapping each run of contiguous
	// whitespace to a single space.
	var actualResults []string
	if actualResultsRaw != nil {
		actualResults = make([]string, 0, len(actualResultsRaw))
		for _, result := range actualResultsRaw {
			if query.sorter == nil || query.valsPerLine != 1 {
				actualResults = append(actualResults, strings.Fields(result)...)
			} else {
				actualResults = append(actualResults, strings.Join(strings.Fields(result), " "))
			}
		}
	}

	if query.sorter != nil {
		query.sorter(len(cols), actualResults)
		query.sorter(len(cols), query.expectedResults)
	}

	hash, err := t.hashResults(actualResults)
	if err != nil {
		return err
	}

	if query.expectedHash != "" {
		n := len(actualResults)
		if query.expectedValues != n {
			return fmt.Errorf("%s: expected %d results, but found %d", query.pos, query.expectedValues, n)
		}
		if query.expectedHash != hash {
			return fmt.Errorf("%s: expected %s, but found %s", query.pos, query.expectedHash, hash)
		}
	}

	if *rewriteResultsInTestfiles || *rewriteSQL {
		if query.expectedHash != "" {
			if query.expectedValues == 1 {
				t.emit(fmt.Sprintf("1 value hashing to %s", query.expectedHash))
			} else {
				t.emit(fmt.Sprintf("%d values hashing to %s", query.expectedValues, query.expectedHash))
			}
		}

		if query.checkResults {
			// If the results match or we're not rewriting, emit them the way they were originally
			// formatted/ordered in the testfile. Otherwise, emit the actual results.
			if !*rewriteResultsInTestfiles || reflect.DeepEqual(query.expectedResults, actualResults) {
				for _, l := range query.expectedResultsRaw {
					t.emit(l)
				}
			} else {
				// Emit the actual results.
				for _, line := range t.formatValues(actualResultsRaw, query.valsPerLine) {
					t.emit(line)
				}
			}
		}
		return nil
	}

	if query.checkResults && !reflect.DeepEqual(query.expectedResults, actualResults) {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "%s: %s\nexpected:\n", query.pos, query.sql)
		for _, line := range query.expectedResultsRaw {
			fmt.Fprintf(&buf, "    %s\n", line)
		}
		sortMsg := ""
		if query.sorter != nil {
			// We performed an order-insensitive comparison of "actual" vs "expected"
			// rows by sorting both, but we'll display the error with the expected
			// rows in the order in which they were put in the file, and the actual
			// rows in the order in which the query returned them.
			sortMsg = " -> ignore the following ordering of rows"
		}
		fmt.Fprintf(&buf, "but found (query options: %q%s) :\n", query.rawOpts, sortMsg)
		for _, line := range t.formatValues(actualResultsRaw, query.valsPerLine) {
			fmt.Fprintf(&buf, "    %s\n", line)
		}
		return errors.New(buf.String())
	}

	if query.label != "" {
		if prevHash, ok := t.labelMap[query.label]; ok && prevHash != hash {
			t.Errorf(
				"%s: error in input: previous values for label %s (hash %s) do not match (hash %s)",
				query.pos, query.label, prevHash, hash,
			)
		}
		t.labelMap[query.label] = hash
	}

	t.finishOne("OK")
	return nil
}

func (t *logicTest) formatValues(vals []string, valsPerLine int) []string {
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)

	for line := 0; line < len(vals)/valsPerLine; line++ {
		for i := 0; i < valsPerLine; i++ {
			fmt.Fprintf(tw, "%s\t", vals[line*valsPerLine+i])
		}
		fmt.Fprint(tw, "\n")
	}
	_ = tw.Flush()

	// Split into lines and trim any trailing whitespace.
	// Note that the last line will be empty (which is what we want).
	results := make([]string, 0, len(vals)/valsPerLine)
	for _, s := range strings.Split(buf.String(), "\n") {
		results = append(results, strings.TrimRight(s, " "))
	}
	return results
}

func (t *logicTest) success(file string) {
	t.progress++
	now := timeutil.Now()
	if now.Sub(t.lastProgress) >= 2*time.Second {
		t.lastProgress = now
		t.outf("--- progress: %s: %d statements/queries", file, t.progress)
	}
}

func (t *logicTest) runFile(path string, config testClusterConfig) {
	defer t.close()

	defer func() {
		if r := recover(); r != nil {
			// Translate panics during the test to test errors.
			t.Fatalf("panic: %v\n%s", r, string(debug.Stack()))
		}
	}()

	if err := t.processTestFile(path, config); err != nil {
		t.Fatal(err)
	}
}

var skipLogicTests = envutil.EnvOrDefaultBool("COCKROACH_LOGIC_TESTS_SKIP", false)
var logicTestsConfigExclude = envutil.EnvOrDefaultString("COCKROACH_LOGIC_TESTS_SKIP_CONFIG", "")
var logicTestsConfigFilter = envutil.EnvOrDefaultString("COCKROACH_LOGIC_TESTS_CONFIG", "")

// RunLogicTest is the main entry point for the logic test. The globs parameter
// specifies the default sets of files to run.
func RunLogicTest(t *testing.T, globs ...string) {
	// Note: there is special code in teamcity-trigger/main.go to run this package
	// with less concurrency in the nightly stress runs. If you see problems
	// please make adjustments there.
	// As of 6/4/2019, the logic tests never complete under race.
	if testutils.NightlyStress() && util.RaceEnabled {
		t.Skip("logic tests and race detector don't mix: #37993")
	}

	if skipLogicTests {
		t.Skip("COCKROACH_LOGIC_TESTS_SKIP")
	}

	// Override default glob sets if -d flag was specified.
	if *logictestdata != "" {
		globs = []string{*logictestdata}
	}

	// Set time.Local to time.UTC to circumvent pq's timetz parsing flaw.
	time.Local = time.UTC

	// A new cluster is set up for each separate file in the test.
	var paths []string
	for _, g := range globs {
		match, err := filepath.Glob(g)
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, match...)
	}

	if len(paths) == 0 {
		t.Fatalf("No testfiles found (globs: %v)", globs)
	}

	// mu protects the following vars, which all get updated from within the
	// possibly parallel subtests.
	var progress = struct {
		syncutil.Mutex
		total, totalFail, totalUnsupported int
		lastProgress                       time.Time
	}{
		lastProgress: timeutil.Now(),
	}

	// Read the configuration directives from all the files and accumulate a list
	// of paths per config.
	configPaths := make([][]string, len(logicTestConfigs))
	var configs []logicTestConfigIdx
	if *overrideConfig != "" {
		configs = parseTestConfig(strings.Split(*overrideConfig, ","))
	}

	for _, path := range paths {
		if *overrideConfig == "" {
			configs = readTestFileConfigs(t, path)
		}
		for _, idx := range configs {
			configPaths[idx] = append(configPaths[idx], path)
		}
	}

	// The tests below are likely to run concurrently; `log` is shared
	// between all the goroutines and thus all tests, so it doesn't make
	// sense to try to use separate `log.Scope` instances for each test.
	logScope := log.Scope(t)
	defer logScope.Close(t)

	verbose := testing.Verbose() || log.V(1)
	for idx, cfg := range logicTestConfigs {
		paths := configPaths[idx]
		if len(paths) == 0 {
			continue
		}
		// Top-level test: one per test configuration.
		t.Run(cfg.name, func(t *testing.T) {
			if testing.Short() && cfg.skipShort {
				t.Skip("config skipped by -test.short")
			}
			if logicTestsConfigExclude != "" && cfg.name == logicTestsConfigExclude {
				t.Skip("config excluded via env var")
			}
			if logicTestsConfigFilter != "" && cfg.name != logicTestsConfigFilter {
				t.Skip("config does not match env var")
			}
			for _, path := range paths {
				path := path // Rebind range variable.
				// Inner test: one per file path.
				t.Run(filepath.Base(path), func(t *testing.T) {
					// Run the test in parallel, unless:
					//  - we're printing out all of the SQL interactions, or
					//  - we're generating testfiles, or
					//  - we are in race mode (where we can hit a limit on alive
					//    goroutines).
					if !*showSQL && !*rewriteResultsInTestfiles && !*rewriteSQL && !util.RaceEnabled {
						// Skip parallelizing tests that use the kv-batch-size directive since
						// the batch size is a global variable.
						// TODO(jordan, radu): make sqlbase.kvBatchSize non-global to fix this.
						if filepath.Base(path) != "select_index_span_ranges" {
							t.Parallel() // SAFE FOR TESTING (this comments satisfies the linter)
						}
					}
					lt := logicTest{
						t:               t,
						verbose:         verbose,
						perErrorSummary: make(map[string][]string),
					}
					if *printErrorSummary {
						defer lt.printErrorSummary()
					}
					lt.setup(cfg)
					lt.runFile(path, cfg)

					progress.Lock()
					defer progress.Unlock()
					progress.total += lt.progress
					progress.totalFail += lt.failures
					progress.totalUnsupported += lt.unsupported
					now := timeutil.Now()
					if now.Sub(progress.lastProgress) >= 2*time.Second {
						progress.lastProgress = now
						lt.outf("--- total progress: %d statements/queries", progress.total)
					}
				})
			}
		})
	}

	unsupportedMsg := ""
	if progress.totalUnsupported > 0 {
		unsupportedMsg = fmt.Sprintf(", ignored %d unsupported queries", progress.totalUnsupported)
	}

	if verbose {
		fmt.Printf("--- total: %d tests, %d failures%s\n",
			progress.total, progress.totalFail, unsupportedMsg,
		)
	}
}

type errorSummaryEntry struct {
	errmsg string
	sql    []string
}
type errorSummary []errorSummaryEntry

func (e errorSummary) Len() int { return len(e) }
func (e errorSummary) Less(i, j int) bool {
	if len(e[i].sql) == len(e[j].sql) {
		return e[i].errmsg < e[j].errmsg
	}
	return len(e[i].sql) < len(e[j].sql)
}
func (e errorSummary) Swap(i, j int) {
	t := e[i]
	e[i] = e[j]
	e[j] = t
}

// printErrorSummary shows the final per-error list of failing queries when
// -allow-prepare-fail or -flex-types are specified.
func (t *logicTest) printErrorSummary() {
	if t.unsupported == 0 {
		return
	}

	t.outf("--- summary of ignored errors:")
	summary := make(errorSummary, len(t.perErrorSummary))
	i := 0
	for errmsg, sql := range t.perErrorSummary {
		summary[i] = errorSummaryEntry{errmsg: errmsg, sql: sql}
	}
	sort.Sort(summary)
	for _, s := range summary {
		t.outf("%s (%d entries)", s.errmsg, len(s.sql))
		var buf bytes.Buffer
		for _, q := range s.sql {
			buf.WriteByte('\t')
			buf.WriteString(strings.Replace(q, "\n", "\n\t", -1))
		}
		t.outf("%s", buf.String())
	}
}

// shortenString cuts its argument on the right so that it more likely
// fits onto the developer's screen.  The behavior can be disabled by
// the command-line flag "-full-messages".
func shortenString(msg string) string {
	if *fullMessages {
		return msg
	}

	shortened := false

	nlPos := strings.IndexRune(msg, '\n')
	if nlPos >= 0 {
		shortened = true
		msg = msg[:nlPos]
	}

	if len(msg) > 80 {
		shortened = true
		msg = msg[:80]
	}

	if shortened {
		msg = msg + "..."
	}

	return msg
}

// simplifyError condenses long error strings to the shortest form
// that still explains the origin of the error.
func simplifyError(msg string) (string, string) {
	prefix := strings.Split(msg, ": ")

	// Split:  "a: b: c"-> "a: b", "c"
	expected := ""
	if len(prefix) > 1 {
		expected = prefix[len(prefix)-1]
		prefix = prefix[:len(prefix)-1]
	}

	// Simplify: "a: b: c: d" -> "a: d"
	if !*fullMessages && len(prefix) > 2 {
		prefix[1] = prefix[len(prefix)-1]
		prefix = prefix[:2]
	}

	// Mark the error message as shortened if necessary.
	if expected != "" {
		prefix = append(prefix, "...")
	}

	return strings.Join(prefix, ": "), expected
}

// signalIgnoredError registers a failing but ignored query.
func (t *logicTest) signalIgnoredError(err error, pos string, sql string) {
	t.unsupported++

	if !*printErrorSummary {
		return
	}

	// Save the error for later reporting.
	errmsg, expected := simplifyError(fmt.Sprintf("%s", err))
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "-- %s (%s)\n%s", pos, shortenString(expected), shortenString(sql+";"))
	errmsg = shortenString(errmsg)
	t.perErrorSummary[errmsg] = append(t.perErrorSummary[errmsg], buf.String())
}

// Error is a wrapper around testing.T.Error that handles printing the per-query
// "FAIL" marker when -show-sql is set. It also registers the error to the
// failure counter.
func (t *logicTest) Error(args ...interface{}) {
	if *showSQL {
		t.outf("\t-- FAIL")
	}
	log.Error(context.Background(), "\n", fmt.Sprint(args...))
	if t.subtestT != nil {
		t.subtestT.Error("\n", fmt.Sprint(args...))
	} else {
		t.t.Error("\n", fmt.Sprint(args...))
	}
	t.failures++
}

// Errorf is a wrapper around testing.T.Errorf that handles printing the
// per-query "FAIL" marker when -show-sql is set. It also registers the error to
// the failure counter.
func (t *logicTest) Errorf(format string, args ...interface{}) {
	if *showSQL {
		t.outf("\t-- FAIL")
	}
	log.Errorf(context.Background(), format, args...)
	if t.subtestT != nil {
		t.subtestT.Errorf("\n"+format, args...)
	} else {
		t.t.Errorf("\n"+format, args...)
	}
	t.failures++
}

// Fatal is a wrapper around testing.T.Fatal that ensures the fatal error message
// is printed on its own line when -show-sql is set.
func (t *logicTest) Fatal(args ...interface{}) {
	if *showSQL {
		fmt.Println()
	}
	log.Error(context.Background(), args...)
	if t.subtestT != nil {
		t.subtestT.Logf("\n%s:%d: error while processing", t.curPath, t.curLineNo)
		t.subtestT.Fatal(args...)
	} else {
		t.t.Logf("\n%s:%d: error while processing", t.curPath, t.curLineNo)
		t.t.Fatal(args...)
	}
}

// Fatalf is a wrapper around testing.T.Fatalf that ensures the fatal error
// message is printed on its own line when -show-sql is set.
func (t *logicTest) Fatalf(format string, args ...interface{}) {
	if *showSQL {
		fmt.Println()
	}
	log.Errorf(context.Background(), format, args...)
	if t.subtestT != nil {
		t.subtestT.Fatalf(format, args...)
	} else {
		t.t.Fatalf(format, args...)
	}
}

// finishOne marks completion of a single test. It handles
// printing the success marker then -show-sql is set.
func (t *logicTest) finishOne(msg string) {
	if *showSQL {
		t.outf("\t-- %s;", msg)
	}
}

// printCompletion reports on the completion of all tests in a given
// input file.
func (t *logicTest) printCompletion(path string, config testClusterConfig) {
	unsupportedMsg := ""
	if t.unsupported > 0 {
		unsupportedMsg = fmt.Sprintf(", ignored %d unsupported queries", t.unsupported)
	}
	t.outf("--- done: %s with config %s: %d tests, %d failures%s", path, config.name,
		t.progress, t.failures, unsupportedMsg)
}
