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
	"math/rand"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"runtime/debug"
	"runtime/trace"
	"sort"
	"strconv"
	"strings"
	"testing"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach-go/v2/testserver"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/externalconn/providers" // imported to register ExternalConnection providers
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/logictest/logictestbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/corpus"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/floatcmp"
	"github.com/cockroachdb/cockroach/pkg/testutils/physicalplanutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/binfetcher"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/lib/pq"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/require"
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
//
// ###########################################
//           TEST CONFIGURATION DIRECTIVES
// ###########################################
//
// Logic tests can start with a directive as follows:
//
//   # LogicTest: local fakedist
//
// This directive lists configurations; the test is run once in each
// configuration (in separate subtests). The configurations are defined by
// LogicTestConfigs. If the directive is missing, the test is run in the
// default configuration.
//
// The directive also supports blocklists, i.e. running all specified
// configurations apart from a blocklisted configuration:
//
//   # LogicTest: default-configs !3node-tenant
//
// If a blocklist is specified without an accompanying configuration, the
// default config is assumed. i.e., the following directive is equivalent to the
// one above:
//
//   # LogicTest: !3node-tenant
//
// An issue can optionally be specified as the reason for blocking a given
// configuration:
//
//   # LogicTest: !3node-tenant(<issue number>)
//
// A link to the issue will be printed out if the -print-blocklist-issues flag
// is specified.
//
// There is a special directive '!metamorphic-batch-sizes' that adjusts the
// server to force the usage of production values related for some constants,
// mostly related to batch sizes, that might change via metamorphic testing.
//
//
// ###########################################################
//           TENANT CLUSTER SETTING OVERRIDE OPTION DIRECTIVES
// ###########################################################
//
// Test files can also contain tenant cluster setting override directives around
// the beginning of the file. These directives can be used to configure tenant
// cluster setting overrides during setup. This can be useful for altering
// tenant read-only settings for configurations that run their tests as
// secondary tenants (eg. 3node-tenant). While these directives apply to all
// configurations under which the test will be run, it's only really meaningful
// when the test runs as a secondary tenant; the configuration has no effect if
// the test is run as the system tenant.
//
// The directives line looks like:
// # tenant-cluster-setting-override-opt: setting_name1=setting_value1 setting_name2=setting_value2
//
//
// ###########################################################
//           TENANT CAPABILITY OVERRIDE OPTION DIRECTIVES
// ###########################################################
//
// Test files can also contain tenant capability override directives around
// the beginning of the file. These directives can be used to configure tenant
// capability overrides during setup. This can be useful for altering
// tenant capabilities for configurations that run their tests as
// secondary tenants (eg. 3node-tenant). While these directives apply to all
// configurations under which the test will be run, it's only really meaningful
// when the test runs as a secondary tenant; the configuration has no effect if
// the test is run as the system tenant.
//
// The directives line looks like:
// # tenant-capability-override-opt: capability_id1=capability_value1 capability_id2=capability_value2
//
//
// ###########################################
//           CLUSTER OPTION DIRECTIVES
// ###########################################
//
// Besides test configuration directives, test files can also contain cluster
// option directives around the beginning of the file (these directive can
// before or appear after the configuration ones). These directives affect the
// settings of the cluster created for the test, across all the configurations
// that the test will run under.
//
// The directives line looks like:
// # cluster-opt: opt1 opt2
//
// The options are:
// - disable-span-config: If specified, the span configs infrastructure will be
//   disabled.
// - tracing-off: If specified, tracing defaults to being turned off. This is
//   used to override the environment, which may ask for tracing to be on by
//   default.
//
//
// ###########################################
//           LogicTest language
// ###########################################
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
//  - statement notice <regexp>
//    Like "statement ok" but expects a notice that matches the given regexp.
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
//  - statement async <name> <options>
//    Runs a statement asynchronously, marking it as a pending
//    statement with a unique name, to be completed and validated later with
//    "awaitstatement". This is intended for use with statements that may block,
//    such as those contending on locks. Other statement options described
//    above are supported, though the statement may not be in a "repeat".
//    Incomplete pending statements will result in an error on test completion.
//    Note that as the statement will be run asynchronously, subsequent queries
//    that depend on the state of the statement should be run with the "retry"
//    option to ensure deterministic test results.
//
//  - awaitstatement <name>
//    Completes a pending statement with the provided name, validating its
//    results as expected per the given options to "statement async <name>...".
//
//  - copy,copy-error
//    Runs a COPY FROM STDIN statement, because of the separate data chunk it requires
//    special logictest support. Format is:
//      copy
//      COPY <table> FROM STDIN;
//      <blankline>
//      COPY DATA
//      ----
//      <NUMROWS>
//
//    copy-error is just like copy but an error is expected and results should be error
//    string.
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
//      - F for floating point (matches 15 significant decimal digits,
//        https://www.postgresql.org/docs/9.0/datatype-numeric.html)
//      - R for decimal
//      - B for boolean
//      - O for oid
//      - _ to include the column header, but ignore the column results.
//        This is useful to verify that a column exists when the results are
//        non-deterministic and to avoid projecting all other columns (for
//        example `SHOW RANGES FROM TABLE`). A "_" placeholder is written in
//        place of actual results.
//
//    Options are comma separated strings from the following:
//      - nosort: sorts neither the returned or expected rows. Skips the
//            flakiness check that forces either rowsort, valuesort,
//            partialsort, or an ORDER BY clause to be present.
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
//            -rewrite flag, the query will be run only once after a 2s sleep.
//      - async: runs the query asynchronously, marking it as a pending
//            query using the label parameter as a unique name, to be completed
//            and validated later with "awaitquery". This is intended for use
//            with queries that may block, such as those contending on locks.
//            It is supported with other options, with the exception of "retry",
//            and may not be in a "repeat". Note that as the query will be run
//            asynchronously, subsequent queries that depend on the state of
//            the query should be run with the "retry" option to ensure
//            deterministic test results.
//      - kvtrace: runs the query and compares against the results of the
//            kv operations trace of the query. kvtrace optionally accepts
//            arguments of the form kvtrace(op,op,...). Op is one of
//            the accepted k/v arguments such as 'CPut', 'Scan' etc. It
//            also accepts arguments of the form 'prefix=...'. For example,
//            if kvtrace(CPut,Del,prefix=/Table/54,prefix=/Table/55), the
//            results will be filtered to contain messages starting with
//            CPut /Table/54, CPut /Table/55, Del /Table/54, Del /Table/55.
//            Cannot be combined with noticetrace.
//      - noticetrace: runs the query and compares only the notices that
//						appear. Cannot be combined with kvtrace.
//      - nodeidx=N: runs the query on node N of the cluster.
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
//  - awaitquery <name>
//    Completes a pending query with the provided name, validating its
//    results as expected per the given options to "query ... async ... <label>".
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
//  - user <username> [nodeidx=N]
//    Changes the user for subsequent statements or queries.
//    If nodeidx is specified, this user will connect to the node
//    in the cluster with index N (note this is 0-indexed, while
//    node IDs themselves are 1-indexed). Otherwise, it will connect
//    to the node with index 0 (node ID 1).
//    A "host-cluster-" prefix can be prepended to the user, which will force
//    the user session to be against the host cluster (useful for multi-tenant
//    configurations).
//
//  - upgrade N
//    When using a cockroach-go/testserver logictest, upgrades the node at
//    index N to the version specified by the logictest config.
//
//  - skipif <mysql/mssql/postgresql/cockroachdb/config CONFIG [ISSUE]>
//    Skips the following `statement` or `query` if the argument is
//    postgresql, cockroachdb, or a config matching the currently
//    running configuration.
//
//  - onlyif <mysql/mssql/postgresql/cockroachdb/config CONFIG [ISSUE]>
//    Skips the following `statement` or `query` if the argument is not
//    postgresql, cockroachdb, or a config matching the currently
//    running configuration.
//
//  - traceon <file>
//    Enables tracing to the given file.
//
//  - traceoff
//    Stops tracing.
//
//  - subtest <testname>
//    Defines the start of a subtest. The subtest is any number of statements
//    that occur after this command until the end of file or the next subtest
//    command.
//
//  - retry
//    Specifies that the next occurrence of a statement or query directive
//    (including those which expect errors) will be retried for a fixed
//    duration until the test passes, or the alloted time has elapsed.
//    This is similar to the retry option of the query directive.
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
//                of `LogicTestConfigs`.
//                Example:
//                  -config local,fakedist
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
	resultsRE   = regexp.MustCompile(`^(\d+)\s+values?\s+hashing\s+to\s+([0-9A-Fa-f]+)$`)
	noticeRE    = regexp.MustCompile(`^statement\s+notice\s+(.*)$`)
	errorRE     = regexp.MustCompile(`^(?:statement|query)\s+error\s+(?:pgcode\s+([[:alnum:]]+)\s+)?(.*)$`)
	varRE       = regexp.MustCompile(`\$[a-zA-Z][a-zA-Z_0-9]*`)
	orderRE     = regexp.MustCompile(`(?i)ORDER\s+BY`)
	explainRE   = regexp.MustCompile(`(?i)EXPLAIN\W+`)
	showTraceRE = regexp.MustCompile(`(?i)SHOW\s+(KV\s+)?TRACE`)

	// Bigtest is a flag which should be set if the long-running sqlite logic tests should be run.
	Bigtest = flag.Bool("bigtest", false, "enable the long-running SqlLiteLogic test")

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

	showDiff          = flag.Bool("show-diff", false, "generate a diff for expectation mismatches when possible")
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
	saveDeclarativeCorpus = flag.String(
		"declarative-corpus", "",
		"enables generation and storage of a declarative schema changer	corpus",
	)
	defaultWorkmem = flag.Bool("default-workmem", false,
		"disable randomization of sql.distsql.temp_storage.workmem",
	)
	// globalMVCCRangeTombstone will write a global MVCC range tombstone across
	// the entire user keyspace during cluster bootstrapping. This should not
	// semantically affect the test data written above it, but will activate MVCC
	// range tombstone code paths in the storage layer for testing.
	globalMVCCRangeTombstone = util.ConstantWithMetamorphicTestBool(
		"logictest-global-mvcc-range-tombstone", false)

	// useMVCCRangeTombstonesForPointDeletes will use point-sized MVCC range
	// tombstones for point deletions, on a best-effort basis. These should be
	// indistinguishable to a KV client, but activate MVCC range tombstone
	// code paths in the storage/KV layer, for testing. This may result in
	// incorrect MVCC stats for RangeKey* fields in rare cases, due to point
	// writes not holding appropriate latches for range key stats update.
	useMVCCRangeTombstonesForPointDeletes = util.ConstantWithMetamorphicTestBool(
		"logictest-use-mvcc-range-tombstones-for-point-deletes", false)

	// BackupRestoreProbability is the environment variable for `3node-backup` config.
	backupRestoreProbability = envutil.EnvOrDefaultFloat64("COCKROACH_LOGIC_TEST_BACKUP_RESTORE_PROBABILITY", 0.0)
)

const queryRewritePlaceholderPrefix = "__async_query_rewrite_placeholder"

// logicStatement represents a single statement test in Test-Script.
type logicStatement struct {
	// file and line number of the test.
	pos string
	// SQL string to be sent to the database.
	sql string
	// expected notice, if any.
	expectNotice string
	// expected error, if any. "" indicates the statement should
	// succeed.
	expectErr string
	// expected pgcode for the error, if any. "" indicates the
	// test does not check the pgwire error code.
	expectErrCode string
	// expected rows affected count. -1 to avoid testing this.
	expectCount int64
	// if this statement is to run asynchronously, and become a pendingStatement.
	expectAsync bool
	// the name key to use for the pendingStatement.
	statementName string
}

// pendingExecResult represents the asynchronous result of a logicStatement
// run against the DB, as well as the final SQL used in execution.
type pendingExecResult struct {
	execSQL string
	res     gosql.Result
	err     error
}

// pendingStatement encapsulates a logicStatement that is expected to block and
// as such is run in a separate goroutine, as well as the channel on which to
// receive the results of the statement execution.
type pendingStatement struct {
	logicStatement

	// The channel on which to receive the execution results, when completed.
	resultChan chan pendingExecResult
}

// pendingQueryResult represents the asynchronous result of a logicQuery
// run against the DB, including any returned rows.
type pendingQueryResult struct {
	rows *gosql.Rows
	err  error
}

// pendingQuery encapsulates a logicQuery that is expected to block and
// as such is run in a separate goroutine, as well as the channel on which to
// receive the results of the query execution.
type pendingQuery struct {
	logicQuery

	// The channel on which to receive the query results, when completed.
	resultChan chan pendingQueryResult
}

// readSQL reads the lines of a SQL statement or query until the first blank
// line or (optionally) a "----" separator, and sets stmt.sql.
//
// If a separator is found, returns separator=true. If a separator is found when
// it is not expected, returns an error.
func (ls *logicStatement) readSQL(
	t *logicTest, s *logictestbase.LineScanner, allowSeparator bool,
) (separator bool, _ error) {
	if err := t.maybeBackupRestore(t.rng, t.cfg); err != nil {
		return false, err
	}

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
			if ls.expectNotice != "" {
				return false, errors.Errorf(
					"%s: invalid ---- separator after a statement expecting a notice: %s",
					ls.pos, ls.expectNotice,
				)
			}
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
//	SELECT a, b FROM ab ORDER BY a
//
// Some possible outputs for the same data:
//
//	1 2        1 5        1 2
//	1 5        1 4        1 4
//	1 4   or   1 2   or   1 5
//	2 3        2 2        2 3
//	2 2        2 3        2 2
//
// After a partialSort with orderedCols = {0} all become:
//
//	1 2
//	1 4
//	1 5
//	2 2
//	2 3
//
// An incorrect output like:
//
//	1 5                          1 2
//	1 2                          1 5
//	2 3          becomes:        2 2
//	2 2                          2 3
//	1 4                          1 4
//
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
	// some tests require the output to match modulo sorting.
	sorter logicSorter
	// noSort is true if the nosort option was explicitly provided in the test.
	noSort bool
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

	// kvtrace indicates that we're comparing the output of a kv trace.
	kvtrace bool
	// kvOpTypes can be used only when kvtrace is true. It contains
	// the particular operation types to filter on, such as CPut or Del.
	kvOpTypes        []string
	keyPrefixFilters []string

	// nodeIdx determines which node on the cluster to execute a query on for the given query.
	nodeIdx int

	// noticetrace indicates we're comparing the output of a notice trace.
	noticetrace bool

	// rawOpts are the query options, before parsing. Used to display in error
	// messages.
	rawOpts string

	// roundFloatsInStringsSigFigs specifies the number of significant figures
	// to round floats embedded in strings to where zero means do not round.
	roundFloatsInStringsSigFigs int
}

var allowedKVOpTypes = []string{
	"CPut",
	"Put",
	"InitPut",
	"Del",
	"DelRange",
	"ClearRange",
	"Get",
	"Scan",
}

func isAllowedKVOp(op string) bool {
	for _, s := range allowedKVOpTypes {
		if op == s {
			return true
		}
	}
	return false
}

// logicTest executes the test cases specified in a file. The file format is
// taken from the sqllogictest tool
// (http://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) with various
// extensions to allow specifying errors and additional options. See
// https://github.com/gregrahn/sqllogictest/ for a github mirror of the
// sqllogictest source.
type logicTest struct {
	rootT    *testing.T
	subtestT *testing.T
	rng      *rand.Rand
	cfg      logictestbase.TestClusterConfig
	// serverArgs are the parameters used to create a cluster for this test.
	// They are persisted since a cluster can be recreated throughout the
	// lifetime of the test and we should create all clusters with the same
	// arguments.
	serverArgs *TestServerArgs
	// clusterOpts are the options used to create a cluster for this test.
	// They are persisted since a cluster can be recreated throughout the
	// lifetime of the test and we should create all clusters with the same
	// arguments.
	clusterOpts []clusterOpt
	// knobOpts are the options used to create testing knobs.
	knobOpts []knobOpt
	// toa contains tenant overrides that are persisted here because a cluster
	// can be recreated throughout the lifetime of a test, and we should use
	// tenant overrides each time this happens.
	toa tenantOverrideArgs
	// cluster is the test cluster against which we are testing. This cluster
	// may be reset during the lifetime of the test.
	cluster serverutils.TestClusterInterface
	// testserverCluster is the testserver cluster. This uses real binaries.
	testserverCluster testserver.TestServer
	// sharedIODir is the ExternalIO directory that is shared between all clusters
	// created in the same logicTest. It is populated during setup() of the logic
	// test.
	sharedIODir string
	// the index of the node (within the cluster) against which we run the test
	// statements.
	nodeIdx int
	// If this test uses a SQL tenant server, this is its address. In this case,
	// all clients are created against this tenant.
	tenantAddrs []string
	tenantApps  []serverutils.ApplicationLayerInterface
	// map of built clients, keyed first on username and then node idx.
	// They are persisted so that they can be reused. They are not closed
	// until the end of a test.
	clients map[string]map[int]*gosql.DB
	// client currently in use. This can change during processing
	// of a test input file when encountering the "user" directive.
	// see setSessionUser() for details.
	user string
	db   *gosql.DB
	// clusterCleanupFuncs contains the cleanup methods that are specific to a
	// cluster. These will be called during cluster tear-down. Note that 1 logic
	// test may reset its cluster throughout a test. Cleanup methods that should
	// be stored here include PGUrl connections for the users for a cluster.
	clusterCleanupFuncs []func()
	// testCleanupFuncs are cleanup methods that are only called when closing a
	// test (rather than a specific cluster). One test may reset a cluster with a
	// new one, but keep some shared resources across the entire test. An example
	// would be an IO directory used throughout the test.
	testCleanupFuncs []func()
	// progress holds the number of statements executed so far.
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

	// pendingStatements tracks any async statements by name key.
	pendingStatements map[string]pendingStatement

	// pendingQueries tracks any async queries by name key.
	pendingQueries map[string]pendingQuery

	// noticeBuffer retains the notices from the past query.
	noticeBuffer []string

	rewriteResTestBuf bytes.Buffer

	curPath   string
	curLineNo int

	// skipOnRetry is explicitly set to true by the skip_on_retry directive.
	// If true, serializability violations in the test when unexpected cause the
	// entire test to be skipped and the below skippedOnRetry to be set to true.
	skipOnRetry    bool
	skippedOnRetry bool

	// declarativeCorpusCollector used to save declarative schema changer state
	// to disk.
	declarativeCorpusCollector *corpus.Collector

	// forceBackupAndRestore is set to true if the user wants to run a cluster
	// backup and restore before running the next SQL statement. This can be set
	// to true using the `force-backup-restore` directive.
	forceBackupAndRestore bool

	// retry indicates if the statement or query should be retried in case of
	// failure with exponential backoff up to some maximum duration. It is reset
	// to false after every successful statement or query test point, including
	// those which are supposed to error out.
	retry bool
}

func (t *logicTest) t() *testing.T {
	if t.subtestT != nil {
		return t.subtestT
	}
	return t.rootT
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
		return varName
	})
}

// rewriteUpToRegex rewrites the rewriteResTestBuf up to the line which matches
// the provided regex, returning a scanner containing the remainder of the lines.
// This is used to aid in rewriting the results of asynchronously run queries.
func (t *logicTest) rewriteUpToRegex(matchRE *regexp.Regexp) *bufio.Scanner {
	remainder := bytes.NewReader(t.rewriteResTestBuf.Bytes())
	t.rewriteResTestBuf = bytes.Buffer{}
	scanner := bufio.NewScanner(remainder)
	for scanner.Scan() {
		if matchRE.Match(scanner.Bytes()) {
			break
		}
		t.rewriteResTestBuf.Write(scanner.Bytes())
		t.rewriteResTestBuf.WriteString("\n")
	}
	return scanner
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

	t.shutdownCluster()

	for _, cleanup := range t.testCleanupFuncs {
		cleanup()
	}
	t.testCleanupFuncs = nil
}

// out emits a message both on stdout and the log files if
// verbose is set.
func (t *logicTest) outf(format string, args ...interface{}) {
	if !t.verbose {
		return
	}
	log.Infof(context.Background(), format, args...)
	msg := fmt.Sprintf(format, args...)
	now := timeutil.Now().Format("15:04:05")
	fmt.Printf("[%s] %s\n", now, msg)
}

// setSessionUser sets the DB client to the specified user and connects
// to the node in the cluster at index nodeIdx.
func (t *logicTest) setSessionUser(user string, nodeIdx int) {
	db := t.getOrOpenClient(user, nodeIdx)
	t.db = db
	t.user = user
	t.nodeIdx = nodeIdx
}

// getOrOpenClient returns the existing client for the given user and nodeIdx,
// if one exists. Otherwise, it opens and returns a new client.
func (t *logicTest) getOrOpenClient(user string, nodeIdx int) *gosql.DB {
	if db, ok := t.clients[user][nodeIdx]; ok {
		return db
	}

	var pgURL url.URL
	pgUser := strings.TrimPrefix(user, "host-cluster-")
	if t.cfg.UseCockroachGoTestserver {
		pgURL = *t.testserverCluster.PGURLForNode(nodeIdx)
		_, port, err := net.SplitHostPort(pgURL.Host)
		if err != nil {
			t.Fatal(err)
		}
		// The host needs to use 127.0.0.1 instead of localhost, since if the node
		// is listening on port 0, then macs only listen on ipv4 and not ipv6.
		pgURL.Host = net.JoinHostPort("127.0.0.1", port)
		pgURL.User = url.User(pgUser)
	} else {
		addr := t.cluster.Server(nodeIdx).AdvSQLAddr()
		if len(t.tenantAddrs) > 0 && !strings.HasPrefix(user, "host-cluster-") {
			addr = t.tenantAddrs[nodeIdx]
		}
		var cleanupFunc func()
		pgURL, cleanupFunc = sqlutils.PGUrl(t.rootT, addr, "TestLogic", url.User(pgUser))
		t.clusterCleanupFuncs = append(t.clusterCleanupFuncs, cleanupFunc)
	}
	pgURL.Path = "test"

	db := t.openDB(pgURL)

	// The default value for extra_float_digits assumed by tests is
	// 1. However, lib/pq by default configures this to 2 during
	// connection initialization, so we need to set it back to 1 before
	// we run anything.
	if _, err := db.Exec("SET extra_float_digits = 1"); err != nil {
		t.Fatal(err)
	}
	// The default setting for index_recommendations_enabled is true. We do not
	// want to display index recommendations in logic tests, so we disable them
	// here.
	if _, err := db.Exec("SET index_recommendations_enabled = false"); err != nil {
		t.Fatal(err)
	}
	if t.clients == nil {
		t.clients = make(map[string]map[int]*gosql.DB)
	}
	if t.clients[user] == nil {
		t.clients[user] = make(map[int]*gosql.DB)
	}
	t.clients[user][nodeIdx] = db

	return db
}

func (t *logicTest) openDB(pgURL url.URL) *gosql.DB {
	base, err := pq.NewConnector(pgURL.String())
	if err != nil {
		t.Fatal(err)
	}

	connector := pq.ConnectorWithNoticeHandler(base, func(notice *pq.Error) {
		t.noticeBuffer = append(t.noticeBuffer, notice.Severity+": "+notice.Message)
		if notice.Detail != "" {
			t.noticeBuffer = append(t.noticeBuffer, "DETAIL: "+notice.Detail)
		}
		if notice.Hint != "" {
			t.noticeBuffer = append(t.noticeBuffer, "HINT: "+notice.Hint)
		}
		if notice.Code != "" && notice.Code != "00000" {
			t.noticeBuffer = append(t.noticeBuffer, "SQLSTATE: "+string(notice.Code))
		}
	})

	return gosql.OpenDB(connector)
}

// Prevent a lint failure "this value is never used" in
// `(*logicTest).setup` when bazel.BuiltWithBazel returns false.
var _ = ((*logicTest)(nil)).newTestServerCluster

// newTestServerCluster creates a 3-node cluster using the cockroach-go library.
// bootstrapBinaryPath is given by the config's CockroachGoBootstrapVersion.
// upgradeBinaryPath is given by the config's CockroachGoUpgradeVersion, or
// is the locally built version if CockroachGoUpgradeVersion was not specified.
func (t *logicTest) newTestServerCluster(bootstrapBinaryPath, upgradeBinaryPath string) {
	logsDir, err := os.MkdirTemp("", "cockroach-logs*")
	if err != nil {
		t.Fatal(err)
	}
	cleanupLogsDir := func() {
		if t.rootT.Failed() {
			fmt.Fprintf(os.Stderr, "cockroach logs captured in: %s\n", logsDir)
		} else {
			_ = os.RemoveAll(logsDir)
		}
	}

	// During config initialization, NumNodes is required to be 3.
	opts := []testserver.TestServerOpt{
		testserver.ThreeNodeOpt(),
		testserver.StoreOnDiskOpt(),
		testserver.CockroachBinaryPathOpt(bootstrapBinaryPath),
		testserver.UpgradeCockroachBinaryPathOpt(upgradeBinaryPath),
		testserver.PollListenURLTimeoutOpt(120),
		testserver.CockroachLogsDirOpt(logsDir),
	}
	if strings.Contains(upgradeBinaryPath, "cockroach-short") {
		// If we're using a cockroach-short binary, that means it was
		// locally built, so we need to opt-out of version offsetting to
		// better simulate a real upgrade path.
		opts = append(opts, testserver.EnvVarOpt([]string{
			"COCKROACH_TESTING_FORCE_RELEASE_BRANCH=true",
		}))
	}

	ts, err := testserver.NewTestServer(opts...)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < t.cfg.NumNodes; i++ {
		// Wait for each node to be reachable.
		if err := ts.WaitForInitFinishForNode(i); err != nil {
			t.Fatal(err)
		}
	}

	t.testserverCluster = ts
	t.clusterCleanupFuncs = append(t.clusterCleanupFuncs, ts.Stop, cleanupLogsDir)

	t.setSessionUser(username.RootUser, 0 /* nodeIdx */)

	// These tests involve stopping and starting nodes, so to reduce flakiness,
	// we increase the lease Transfer timeout.
	if _, err := t.db.Exec("SET CLUSTER SETTING server.shutdown.lease_transfer_wait = '40s'"); err != nil {
		t.Fatal(err)
	}
}

// newCluster creates a new cluster. It should be called after the logic tests's
// server args are configured. That is, either during setup() when creating the
// initial cluster to be used in a test, or when creating additional test
// clusters, after logicTest.setup() has been called.
func (t *logicTest) newCluster(
	serverArgs TestServerArgs, clusterOpts []clusterOpt, knobOpts []knobOpt, toa tenantOverrideArgs,
) {
	makeClusterSettings := func(forSystemTenant bool) *cluster.Settings {
		var st *cluster.Settings
		if forSystemTenant {
			// System tenants use the constructor that doesn't initialize the
			// cluster version (see makeTestConfigFromParams). This is needed
			// for local-mixed-22.2-23.1 config.
			st = cluster.MakeClusterSettings()
		} else {
			// Regular tenants use the constructor that initializes the cluster
			// version (see TestServer.StartTenant).
			st = cluster.MakeTestingClusterSettings()
		}
		// Disable stats collection on system tables before the cluster is
		// started, otherwise there is a race condition where stats may be
		// collected before we can disable them with `SET CLUSTER SETTING`. We
		// disable stats collection on system tables in order to have
		// deterministic tests.
		stats.AutomaticStatisticsOnSystemTables.Override(context.Background(), &st.SV, false)
		if t.cfg.UseFakeSpanResolver {
			// We will need to update the DistSQL span resolver with the fake
			// resolver, but this can only be done while DistSQL is disabled.
			// Note that this is needed since the internal queries could use
			// DistSQL if it's not disabled, and we have to disable it before
			// the cluster started (so that we don't have any internal queries
			// using DistSQL concurrently with updating the span resolver).
			sql.DistSQLClusterExecMode.Override(context.Background(), &st.SV, int64(sessiondatapb.DistSQLOff))
		}
		return st
	}
	setSQLTestingKnobs := func(knobs *base.TestingKnobs) {
		knobs.SQLEvalContext = &eval.TestingKnobs{
			AssertBinaryExprReturnTypes:     true,
			AssertUnaryExprReturnTypes:      true,
			AssertFuncExprReturnTypes:       true,
			DisableOptimizerRuleProbability: *disableOptRuleProbability,
			OptimizerCostPerturbation:       *optimizerCostPerturbation,
			ForceProductionValues:           serverArgs.ForceProductionValues,
		}
		knobs.SQLExecutor = &sql.ExecutorTestingKnobs{
			DeterministicExplain:            true,
			UseTransactionalDescIDGenerator: true,
		}
		knobs.SQLStatsKnobs = sqlstats.CreateTestingKnobs()
		if serverArgs.DeclarativeCorpusCollection && t.declarativeCorpusCollector != nil {
			knobs.SQLDeclarativeSchemaChanger = &scexec.TestingKnobs{
				BeforeStage: t.declarativeCorpusCollector.GetBeforeStage(t.rootT.Name(), t.t()),
			}
		}
		knobs.DistSQL = &execinfra.TestingKnobs{
			ForceDiskSpill: t.cfg.SQLExecUseDisk,
		}
	}
	// TODO(andrei): if createTestServerParams() is used here, the command filter
	// it installs detects a transaction that doesn't have
	// modifiedSystemConfigSpan set even though it should, for
	// "testdata/rename_table". Figure out what's up with that.

	// We have some queries that bump into 100MB default temp storage limit
	// when run with fakedist-disk config, so we'll use a larger limit here.
	// There isn't really a downside to doing so.
	tempStorageDiskLimit := int64(512 << 20) /* 512 MiB */
	// MVCC range tombstones are only available in 22.2 or newer.
	shouldUseMVCCRangeTombstonesForPointDeletes := useMVCCRangeTombstonesForPointDeletes && !serverArgs.DisableUseMVCCRangeTombstonesForPointDeletes
	ignoreMVCCRangeTombstoneErrors := globalMVCCRangeTombstone || shouldUseMVCCRangeTombstonesForPointDeletes

	var defaultTestTenant base.DefaultTestTenantOptions
	switch t.cfg.UseSecondaryTenant {
	case logictestbase.Always, logictestbase.Never:
		// If the test tenant is explicitly enabled or disabled then
		// `logic test` will handle the creation of a configured test
		// tenant, thus for this case we disable the implicit creation of
		// the default test tenant.
		defaultTestTenant = base.TestControlsTenantsExplicitly
	case logictestbase.Random:
		// Delegate to the test framework what to do.
		defaultTestTenant = base.TestTenantProbabilisticOnly
	}

	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			SQLMemoryPoolSize: serverArgs.MaxSQLMemoryLimit,
			DefaultTestTenant: defaultTestTenant,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					// The consistency queue makes a lot of noisy logs during logic tests.
					DisableConsistencyQueue:  true,
					GlobalMVCCRangeTombstone: globalMVCCRangeTombstone,
					EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
						DisableInitPutFailOnTombstones:    ignoreMVCCRangeTombstoneErrors,
						UseRangeTombstonesForPointDeletes: shouldUseMVCCRangeTombstonesForPointDeletes,
					},
				},
				RangeFeed: &rangefeed.TestingKnobs{
					IgnoreOnDeleteRangeError: ignoreMVCCRangeTombstoneErrors,
				},
			},
			ClusterName:   "testclustername",
			ExternalIODir: t.sharedIODir,
		},
		// For distributed SQL tests, we use the fake span resolver; it doesn't
		// matter where the data really is.
		ReplicationMode: base.ReplicationManual,
	}
	setSQLTestingKnobs(&params.ServerArgs.Knobs)

	cfg := t.cfg
	if cfg.UseSecondaryTenant == logictestbase.Always {
		// In the tenant case we need to enable replication in order to split and
		// relocate ranges correctly.
		//
		// TODO(#76378): This condition is faulty. In the case where the
		// profile is configured with "Random", we want to set the
		// replication mode as well when a test tenant is effectively
		// created. This currently is not happening.
		params.ReplicationMode = base.ReplicationAuto
	}
	if cfg.BootstrapVersion != clusterversion.Key(0) {
		if params.ServerArgs.Knobs.Server == nil {
			params.ServerArgs.Knobs.Server = &server.TestingKnobs{}
		}
		params.ServerArgs.Knobs.Server.(*server.TestingKnobs).BootstrapVersionKeyOverride = cfg.BootstrapVersion
		params.ServerArgs.Knobs.Server.(*server.TestingKnobs).BinaryVersionOverride = clusterversion.ByKey(cfg.BootstrapVersion)
	}
	if cfg.DisableUpgrade {
		if params.ServerArgs.Knobs.Server == nil {
			params.ServerArgs.Knobs.Server = &server.TestingKnobs{}
		}
		params.ServerArgs.Knobs.Server.(*server.TestingKnobs).DisableAutomaticVersionUpgrade = make(chan struct{})
	}
	for _, opt := range clusterOpts {
		t.rootT.Logf("apply cluster opt %T", opt)
		opt.apply(&params.ServerArgs)
	}
	for _, opt := range knobOpts {
		t.rootT.Logf("apply knob opt %T", opt)
		opt.apply(&params.ServerArgs.Knobs)
	}

	paramsPerNode := map[int]base.TestServerArgs{}
	require.Truef(
		t.rootT,
		len(cfg.Localities) == 0 || len(cfg.Localities) == cfg.NumNodes,
		"localities must be set for each node -- got %#v for %d nodes",
		cfg.Localities,
		cfg.NumNodes,
	)
	for i := 0; i < cfg.NumNodes; i++ {
		nodeParams := params.ServerArgs
		if locality, ok := cfg.Localities[i+1]; ok {
			nodeParams.Locality = locality
		} else {
			require.Lenf(t.rootT, cfg.Localities, 0, "node %d does not have a locality set", i+1)
		}
		nodeParams.Settings = makeClusterSettings(true /* forSystemTenant */)
		nodeParams.TempStorageConfig = base.DefaultTestTempStorageConfigWithSize(
			nodeParams.Settings, tempStorageDiskLimit,
		)
		paramsPerNode[i] = nodeParams
	}
	params.ServerArgsPerNode = paramsPerNode

	// Update the defaults for automatic statistics to avoid delays in testing.
	// Avoid making the DefaultAsOfTime too small to avoid interacting with
	// schema changes and causing transaction retries.
	// TODO(radu): replace these with testing knobs.
	stats.DefaultAsOfTime = 10 * time.Millisecond
	stats.DefaultRefreshInterval = time.Millisecond

	t.cluster = serverutils.StartNewTestCluster(t.rootT, cfg.NumNodes, params)
	if cfg.UseFakeSpanResolver {
		// We need to update the DistSQL span resolver with the fake resolver.
		// Note that DistSQL was disabled in makeClusterSetting above, so we
		// will reset the setting after updating the span resolver.
		for nodeIdx := 0; nodeIdx < cfg.NumNodes; nodeIdx++ {
			fakeResolver := physicalplanutils.FakeResolverForTestCluster(t.cluster)
			t.cluster.Server(nodeIdx).SetDistSQLSpanResolver(fakeResolver)
		}
		serverutils.SetClusterSetting(t.rootT, t.cluster, "sql.defaults.distsql", "auto")
	}

	connsForClusterSettingChanges := []*gosql.DB{t.cluster.ServerConn(0)}
	if cfg.UseSecondaryTenant == logictestbase.Always {
		// The config profile requires the test to run with a secondary
		// tenant. Set the tenant servers up now.
		//
		// TODO(cli): maybe share this code with the code in
		// cli/democluster which does a very similar thing.
		t.tenantAddrs = make([]string, cfg.NumNodes)
		t.tenantApps = make([]serverutils.ApplicationLayerInterface, cfg.NumNodes)
		for i := 0; i < cfg.NumNodes; i++ {
			settings := makeClusterSettings(false /* forSystemTenant */)
			tempStorageConfig := base.DefaultTestTempStorageConfigWithSize(settings, tempStorageDiskLimit)
			tenantArgs := base.TestTenantArgs{
				TenantID: serverutils.TestTenantID(),
				Settings: settings,
				TestingKnobs: base.TestingKnobs{
					RangeFeed: paramsPerNode[i].Knobs.RangeFeed,
				},
				MemoryPoolSize:    params.ServerArgs.SQLMemoryPoolSize,
				TempStorageConfig: &tempStorageConfig,
				Locality:          paramsPerNode[i].Locality,
				TracingDefault:    params.ServerArgs.TracingDefault,
				// Give every tenant its own ExternalIO directory.
				ExternalIODir: path.Join(t.sharedIODir, strconv.Itoa(i)),
			}
			setSQLTestingKnobs(&tenantArgs.TestingKnobs)

			for _, opt := range knobOpts {
				t.rootT.Logf("apply knob opt %T to tenant", opt)
				opt.apply(&tenantArgs.TestingKnobs)
			}

			tenant, err := t.cluster.Server(i).StartTenant(context.Background(), tenantArgs)
			if err != nil {
				t.rootT.Fatalf("%+v", err)
			}
			t.tenantApps[i] = tenant
			t.tenantAddrs[i] = tenant.SQLAddr()
		}

		// Open a connection to a tenant to set any cluster settings specified
		// by the test config.
		db := t.tenantApps[0].SQLConn(t.rootT, "")
		connsForClusterSettingChanges = append(connsForClusterSettingChanges, db)

		// Increase tenant rate limits for faster tests.
		conn := t.cluster.ServerConn(0)
		if _, err := conn.Exec("SET CLUSTER SETTING kv.tenant_rate_limiter.rate_limit = 100000"); err != nil {
			t.Fatal(err)
		}

		// Reduce the schema GC job's MVCC polling interval for faster tests.
		if _, err := conn.Exec(
			"SET CLUSTER SETTING sql.gc_job.wait_for_gc.interval = '3s'",
		); err != nil {
			t.Fatal(err)
		}
	}

	// If we've created a tenant (either explicitly, or probabilistically and
	// implicitly) set any necessary cluster settings to override blocked
	// behavior.
	if cfg.UseSecondaryTenant == logictestbase.Always || t.cluster.StartedDefaultTestTenant() {

		conn := t.cluster.SystemLayer(0).SQLConn(t.rootT, "")
		clusterSettings := toa.clusterSettings
		_, ok := clusterSettings[sql.SecondaryTenantZoneConfigsEnabled.Key()]
		if ok {
			// We reduce the closed timestamp duration on the host tenant so that the
			// setting override can propagate to the tenant faster.
			if _, err := conn.Exec(
				"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'",
			); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(
				"SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '50ms'",
			); err != nil {
				t.Fatal(err)
			}
		}

		tenantID := serverutils.TestTenantID()
		for name, value := range clusterSettings {
			query := fmt.Sprintf("ALTER TENANT [$1] SET CLUSTER SETTING %s = $2", name)
			if _, err := conn.Exec(query, tenantID.ToUint64(), value); err != nil {
				t.Fatal(err)
			}
		}

		capabilities := toa.capabilities
		for name, value := range capabilities {
			query := fmt.Sprintf("ALTER TENANT [$1] GRANT CAPABILITY %s = $2", name)
			if _, err := conn.Exec(query, tenantID.ToUint64(), value); err != nil {
				t.Fatal(err)
			}
		}
		numCapabilities := len(capabilities)
		if numCapabilities > 0 {
			capabilityMap := make(map[tenantcapabilities.ID]string, numCapabilities)
			for k, v := range capabilities {
				capability, ok := tenantcapabilities.FromName(k)
				if !ok {
					t.Fatalf("cannot get capability from name %q", k)
				}
				capabilityMap[capability.ID()] = v
			}
			t.cluster.WaitForTenantCapabilities(t.t(), tenantID, capabilityMap)
		}
	}

	var randomWorkmem int
	if t.rng.Float64() < 0.5 && !serverArgs.DisableWorkmemRandomization {
		// Randomize sql.distsql.temp_storage.workmem cluster setting in
		// [10KiB, 100KiB) range for normal tests and even bigger for sqlite
		// tests.
		if *Bigtest {
			randomWorkmem = 100<<10 + t.rng.Intn(90<<10)
		} else {
			randomWorkmem = 10<<10 + t.rng.Intn(90<<10)
		}
	}

	// Set cluster settings.
	for _, conn := range connsForClusterSettingChanges {
		if _, err := conn.Exec(
			"SET CLUSTER SETTING sql.stats.automatic_collection.min_stale_rows = $1::int", 5,
		); err != nil {
			t.Fatal(err)
		}

		if cfg.OverrideDistSQLMode != "" {
			if _, err := conn.Exec(
				"SET CLUSTER SETTING sql.defaults.distsql = $1::string", cfg.OverrideDistSQLMode,
			); err != nil {
				t.Fatal(err)
			}
		}

		if cfg.OverrideVectorize != "" {
			if _, err := conn.Exec(
				"SET CLUSTER SETTING sql.defaults.vectorize = $1::string", cfg.OverrideVectorize,
			); err != nil {
				t.Fatal(err)
			}
		}

		// We support disabling the declarative schema changer, so that no regressions
		// occur in the legacy schema changer.
		if cfg.DisableDeclarativeSchemaChanger {
			if _, err := conn.Exec(
				"SET CLUSTER SETTING sql.defaults.use_declarative_schema_changer='off'"); err != nil {
				t.Fatal(err)
			}
		}

		if cfg.DisableLocalityOptimizedSearch {
			if _, err := conn.Exec(
				"SET CLUSTER SETTING sql.defaults.locality_optimized_partitioned_index_scan.enabled = false",
			); err != nil {
				t.Fatal(err)
			}
		}

		// We disable the automatic stats collection in order to have
		// deterministic tests.
		//
		// We've also seen tests flake with it on. When the issue manifests, it
		// seems to be around a schema change transaction getting pushed, which
		// causes it to increment a table ID twice instead of once, causing
		// non-determinism.
		//
		// In the short term, we disable auto stats by default to avoid the
		// flakes.
		//
		// In the long run, these tests should be running with default settings
		// as much as possible, so we likely want to address this. Two options
		// are either making schema changes more resilient to being pushed or
		// possibly making auto stats avoid pushing schema change transactions.
		// There might be other better alternatives than these.
		//
		// See #37751 for details.
		if _, err := conn.Exec(
			"SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false",
		); err != nil {
			t.Fatal(err)
		}

		// We also disable stats forecasts to have deterministic tests. See #97003
		// for details.
		if _, err := conn.Exec(
			"SET CLUSTER SETTING sql.stats.forecasts.enabled = false",
		); err != nil {
			t.Fatal(err)
		}

		// Update the default AS OF time for querying the system.table_statistics
		// table to create the crdb_internal.table_row_statistics table.
		if _, err := conn.Exec(
			"SET CLUSTER SETTING sql.crdb_internal.table_row_statistics.as_of_time = '-1µs'",
		); err != nil {
			t.Fatal(err)
		}

		if randomWorkmem != 0 {
			query := fmt.Sprintf("SET CLUSTER SETTING sql.distsql.temp_storage.workmem = '%dB'", randomWorkmem)
			if _, err := conn.Exec(query); err != nil {
				t.Fatal(err)
			}
			t.outf("setting distsql_workmem='%dB';", randomWorkmem)
		}

		if serverArgs.DisableDirectColumnarScans {
			if _, err := conn.Exec(
				"SET CLUSTER SETTING sql.distsql.direct_columnar_scans.enabled = false",
			); err != nil {
				t.Fatal(err)
			}
		}
	}

	if cfg.OverrideDistSQLMode != "" {
		_, ok := sessiondatapb.DistSQLExecModeFromString(cfg.OverrideDistSQLMode)
		if !ok {
			t.Fatalf("invalid distsql mode override: %s", cfg.OverrideDistSQLMode)
		}
		// Wait until all servers are aware of the setting.
		testutils.SucceedsSoon(t.rootT, func() error {
			for i := 0; i < t.cluster.NumServers(); i++ {
				var m string
				err := t.cluster.ServerConn(i % t.cluster.NumServers()).QueryRow(
					"SHOW CLUSTER SETTING sql.defaults.distsql",
				).Scan(&m)
				if err != nil {
					t.Fatal(errors.Wrapf(err, "%d", i))
				}
				if m != cfg.OverrideDistSQLMode {
					return errors.Errorf("node %d is still waiting for update of DistSQLMode to %s (have %s)",
						i, cfg.OverrideDistSQLMode, m,
					)
				}
			}
			return nil
		})
	}

	for name, value := range toa.clusterSettings {
		t.waitForTenantReadOnlyClusterSettingToTakeEffectOrFatal(
			name, value, params.ServerArgs.Insecure,
		)
	}

	t.setSessionUser(username.RootUser, 0 /* nodeIdx */)
}

// waitForTenantReadOnlyClusterSettingToTakeEffectOrFatal waits until all tenant
// servers are aware about the supplied setting's expected value. Fatal's if
// this doesn't happen within the SucceedsSoonDuration.
func (t *logicTest) waitForTenantReadOnlyClusterSettingToTakeEffectOrFatal(
	settingName string, expValue string, insecure bool,
) {
	// Wait until all tenant servers are aware of the setting override.
	dbs := make([]*gosql.DB, len(t.tenantApps))
	for i := range dbs {
		dbs[i] = t.tenantApps[i].SQLConn(t.rootT, "")
	}
	testutils.SucceedsSoon(t.rootT, func() error {
		for i := 0; i < len(t.tenantApps); i++ {
			var val string
			err := dbs[i].QueryRow(
				fmt.Sprintf("SHOW CLUSTER SETTING %s", settingName),
			).Scan(&val)
			if err != nil {
				t.Fatal(errors.Wrapf(err, "%d", i))
			}
			if val != expValue {
				return errors.Errorf("tenant server %d is still waiting zone config cluster setting update",
					i,
				)
			}
		}
		return nil
	})
}

// shutdownCluster performs the necessary cleanup to shutdown the current test
// cluster.
func (t *logicTest) shutdownCluster() {
	for _, cleanup := range t.clusterCleanupFuncs {
		cleanup()
	}
	t.clusterCleanupFuncs = nil

	if t.cluster != nil {
		t.cluster.Stopper().Stop(context.TODO())
		t.cluster = nil
	}
	if t.clients != nil {
		for _, userClients := range t.clients {
			for _, c := range userClients {
				c.Close()
			}
		}
		t.clients = nil
	}
	t.db = nil
}

// resetCluster cleans up the current cluster, and creates a fresh one.
func (t *logicTest) resetCluster() {
	t.traceStop()
	t.shutdownCluster()
	if t.serverArgs == nil {
		// We expect the server args to be persisted to the test during test
		// setup.
		t.Fatal("resetting the cluster before server args were set")
	}
	serverArgs := *t.serverArgs
	t.newCluster(serverArgs, t.clusterOpts, t.knobOpts, t.toa)
}

// setup creates the initial cluster for the logic test and populates the
// relevant fields on logicTest. It is expected to be called only once (per test
// file), and before processing any test files - unless a mock logicTest is
// created (see parallelTest.processTestFile).
func (t *logicTest) setup(
	cfg logictestbase.TestClusterConfig,
	serverArgs TestServerArgs,
	clusterOpts []clusterOpt,
	knobOpts []knobOpt,
	toa tenantOverrideArgs,
) {
	t.cfg = cfg
	t.serverArgs = &serverArgs
	t.serverArgs.DeclarativeCorpusCollection = cfg.DeclarativeCorpusCollection
	t.clusterOpts = clusterOpts[:]
	t.knobOpts = knobOpts[:]
	t.toa = toa
	// TODO(pmattis): Add a flag to make it easy to run the tests against a local
	// MySQL or Postgres instance.
	tempExternalIODir, tempExternalIODirCleanup := testutils.TempDir(t.rootT)
	t.sharedIODir = tempExternalIODir
	t.testCleanupFuncs = append(t.testCleanupFuncs, tempExternalIODirCleanup)

	if cfg.UseCockroachGoTestserver {
		skip.UnderRace(t.t(), "test uses a different binary, so the race detector doesn't work")
		skip.UnderStress(t.t(), "test takes a long time and downloads release artifacts")
		if !bazel.BuiltWithBazel() {
			skip.IgnoreLint(t.t(), "cockroach-go/testserver can only be uzed in bazel builds")
		}
		if cfg.NumNodes != 3 {
			t.Fatal("cockroach-go testserver tests must use 3 nodes")
		}

		upgradeVersion, err := version.Parse(build.BinaryVersion())
		if err != nil {
			t.Fatal(err)
		}
		bootstrapVersion, err := release.LatestPredecessor(upgradeVersion)
		if err != nil {
			t.Fatal(err)
		}
		bootstrapBinaryPath, err := binfetcher.Download(context.Background(), binfetcher.Options{
			Binary:  "cockroach",
			Dir:     tempExternalIODir,
			Version: "v" + bootstrapVersion,
			GOOS:    runtime.GOOS,
			GOARCH:  runtime.GOARCH,
		})
		if err != nil {
			t.Fatal(err)
		}

		// Prevent a lint failure "this value is never used" when
		// bazel.BuiltWithBazel returns false above.
		_ = bootstrapBinaryPath
		upgradeBinaryPath, found := bazel.FindBinary("pkg/cmd/cockroach-short/cockroach-short_/", "cockroach-short")
		if !found {
			t.Fatal(errors.New("cockroach binary not found"))
		}
		t.newTestServerCluster(bootstrapBinaryPath, upgradeBinaryPath)
	} else {
		t.newCluster(serverArgs, t.clusterOpts, t.knobOpts, t.toa)
	}

	// Only create the test database on the initial cluster, since cluster restore
	// expects an empty cluster.
	if _, err := t.db.Exec(`
CREATE DATABASE test; USE test;
`); err != nil {
		t.Fatal(err)
	}

	if _, err := t.db.Exec(fmt.Sprintf("CREATE USER %s;", username.TestUser)); err != nil {
		t.Fatal(err)
	}

	t.labelMap = make(map[string]string)
	t.varMap = make(map[string]string)
	t.pendingStatements = make(map[string]pendingStatement)
	t.pendingQueries = make(map[string]pendingQuery)

	t.progress = 0
	t.failures = 0
	t.unsupported = 0
}

// tenantOverrideArgs are the arguments used by the host cluster to configure
// tenant overrides (eg. cluster settings, capabilities) during setup.
type tenantOverrideArgs struct {
	clusterSettings map[string]string
	capabilities    map[string]string
}

// clusterOpt is implemented by options for configuring the test cluster under
// which a test will run.
type clusterOpt interface {
	apply(args *base.TestServerArgs)
}

// clusterOptTracingOff corresponds to the tracing-off directive.
type clusterOptTracingOff struct{}

var _ clusterOpt = clusterOptTracingOff{}

// apply implements the clusterOpt interface.
func (c clusterOptTracingOff) apply(args *base.TestServerArgs) {
	args.TracingDefault = tracing.TracingModeOnDemand
}

// knobOpt is implemented by options for configuring the testing knobs
// for the cluster under which a test will run.
type knobOpt interface {
	apply(args *base.TestingKnobs)
}

// knobOptSynchronousEventLog corresponds to the sync write
// event log testing knob.
type knobOptSynchronousEventLog struct{}

var _ knobOpt = knobOptSynchronousEventLog{}

// apply implements the clusterOpt interface.
func (c knobOptSynchronousEventLog) apply(args *base.TestingKnobs) {
	_, ok := args.EventLog.(*sql.EventLogTestingKnobs)
	if !ok {
		args.EventLog = &sql.EventLogTestingKnobs{}
	}
	args.EventLog.(*sql.EventLogTestingKnobs).SyncWrites = true
}

// clusterOptIgnoreStrictGCForTenants corresponds to the
// ignore-tenant-strict-gc-enforcement directive.
type clusterOptIgnoreStrictGCForTenants struct{}

var _ clusterOpt = clusterOptIgnoreStrictGCForTenants{}

// apply implements the clusterOpt interface.
func (c clusterOptIgnoreStrictGCForTenants) apply(args *base.TestServerArgs) {
	_, ok := args.Knobs.Store.(*kvserver.StoreTestingKnobs)
	if !ok {
		args.Knobs.Store = &kvserver.StoreTestingKnobs{}
	}
	args.Knobs.Store.(*kvserver.StoreTestingKnobs).IgnoreStrictGCEnforcement = true
}

// knobOptDisableCorpusGeneration disables corpus generation for declarative
// schema changer.
type knobOptDisableCorpusGeneration struct{}

var _ knobOpt = knobOptDisableCorpusGeneration{}

func (c knobOptDisableCorpusGeneration) apply(args *base.TestingKnobs) {
	args.SQLDeclarativeSchemaChanger = nil
}

// parseDirectiveOptions looks around the beginning of the file for a line
// looking like:
// # <directiveName>: opt1 opt2 ...
// and parses the options associated with the directive. The given callback is
// invoked with each option.
func parseDirectiveOptions(t *testing.T, path string, directiveName string, f func(opt string)) {
	switch directiveName {
	case knobDirective,
		clusterDirective,
		tenantClusterSettingOverrideDirective,
		tenantCapabilityOverrideDirective:
	default:
		t.Fatalf("cannot parse unknown directive %s", directiveName)
	}
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	beginningOfFile := true
	directiveFound := false
	s := logictestbase.NewLineScanner(file)

	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) == 0 {
			continue
		}
		cmd := fields[0]
		if !strings.HasPrefix(cmd, "#") {
			// Further directives are not allowed.
			beginningOfFile = false
		}
		// Cluster config directive lines are of the form:
		// # cluster-opt: opt1 opt2 ...
		if len(fields) > 1 && cmd == "#" && fields[1] == fmt.Sprintf("%s:", directiveName) {
			require.True(
				t,
				beginningOfFile,
				"%s directive needs to be at the beginning of file",
				directiveName,
			)
			require.False(
				t,
				directiveFound,
				"only one %s directive allowed per file; second one found: %s",
				directiveName,
				s.Text(),
			)
			directiveFound = true
			if len(fields) == 2 {
				t.Fatalf("%s: empty LogicTest directive", path)
			}
			for _, opt := range fields[2:] {
				f(opt)
			}
		}
	}
}

const (
	tenantClusterSettingOverrideDirective = "tenant-cluster-setting-override-opt"
	tenantCapabilityOverrideDirective     = "tenant-capability-override-opt"
)

// readTenantOverrideArgs looks around the beginning of the file
// for a line looking like:
// # tenant-cluster-setting-override-opt: opt1 opt2 ...
// # tenant-capability-override-opt: opt1 opt2
// and parses that line into a set of tenantOverrideArgs that need
// to be overriden by the host cluster before the test begins.
func readTenantOverrideArgs(t *testing.T, path string) tenantOverrideArgs {
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	getConfigMap := func(directiveName string, configType string) map[string]string {
		configMap := make(map[string]string)
		parseDirectiveOptions(t, path, directiveName, func(opt string) {
			parts := strings.Split(opt, "=")
			if len(parts) != 2 {
				t.Fatalf("%s %q must be in format name=value", configType, opt)
			}
			name := parts[0]
			value := parts[1]
			_, ok := configMap[name]
			if ok {
				t.Fatalf("cannot set %s %q more than once", configType, name)
			}
			configMap[name] = value
		})
		return configMap
	}

	return tenantOverrideArgs{
		clusterSettings: getConfigMap(tenantClusterSettingOverrideDirective, "cluster setting"),
		capabilities:    getConfigMap(tenantCapabilityOverrideDirective, "capability"),
	}
}

const knobDirective = "knob-opt"

// readKnobOptions looks around the beginning of the file for a line looking like:
// # knob-opt: opt1 opt2 ...
// and parses that line into a set of knobOpts that need to be applied to the
// TestServerArgs.Knobs before the cluster is started for the respective test file.
func readKnobOptions(t *testing.T, path string) []knobOpt {
	var res []knobOpt
	parseDirectiveOptions(t, path, knobDirective, func(opt string) {
		switch opt {
		case "disable-corpus-generation":
			res = append(res, knobOptDisableCorpusGeneration{})
		case "sync-event-log":
			res = append(res, knobOptSynchronousEventLog{})
		default:
			t.Fatalf("unrecognized knob option: %s", opt)
		}
	})
	return res
}

const clusterDirective = "cluster-opt"

// readClusterOptions looks around the beginning of the file for a line looking like:
// # cluster-opt: opt1 opt2 ...
// and parses that line into a set of clusterOpts that need to be applied to the
// TestServerArgs before the cluster is started for the respective test file.
func readClusterOptions(t *testing.T, path string) []clusterOpt {
	var res []clusterOpt
	parseDirectiveOptions(t, path, clusterDirective, func(opt string) {
		switch opt {
		case "tracing-off":
			res = append(res, clusterOptTracingOff{})
		case "ignore-tenant-strict-gc-enforcement":
			res = append(res, clusterOptIgnoreStrictGCForTenants{})
		default:
			t.Fatalf("unrecognized cluster option: %s", opt)
		}
	})
	return res
}

type subtestDetails struct {
	name                  string        // the subtest's name, empty if not a subtest
	buffer                *bytes.Buffer // a chunk of the test file representing the subtest
	lineLineIndexIntoFile int           // the line number of the test file where the subtest started
}

func (t *logicTest) processTestFile(path string, config logictestbase.TestClusterConfig) error {
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
			t.rootT.Run(subtest.name, func(subtestT *testing.T) {
				t.subtestT = subtestT
				defer func() {
					t.subtestT = nil
				}()
				if err := t.processSubtest(subtest, path, config); err != nil {
					t.Error(err)
				}
			})
			t.maybeSkipOnRetry(nil)
		}
	}

	if (*rewriteResultsInTestfiles || *rewriteSQL) && !t.rootT.Failed() {
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

func (t *logicTest) hasOpenTxns(ctx context.Context) bool {
	for _, userClients := range t.clients {
		for _, user := range userClients {
			existingTxnPriority := "NORMAL"
			err := user.QueryRow("SHOW TRANSACTION PRIORITY").Scan(&existingTxnPriority)
			if err != nil {
				// If we are unable to see transaction priority assume we're in the middle
				// of an explicit txn.
				log.Warningf(ctx, "failed to check txn priority with %v", err)
				return true
			}
			if _, err := user.Exec("SET TRANSACTION PRIORITY NORMAL;"); !testutils.IsError(err, "there is no transaction in progress") {
				// Reset the txn priority to what it was before we checked for open txns.
				_, err := user.Exec(fmt.Sprintf(`SET TRANSACTION PRIORITY %s`, existingTxnPriority))
				if err != nil {
					log.Warningf(ctx, "failed to reset txn priority with %v", err)
				}
				return true
			}
		}
	}
	return false
}

// maybeBackupRestore will randomly issue a cluster backup, create a new
// cluster, and restore that backup to the cluster before continuing the test.
// The probability of executing a backup and restore is specified in the
// logictest.TestClusterConfig.
func (t *logicTest) maybeBackupRestore(
	rng *rand.Rand, config logictestbase.TestClusterConfig,
) error {
	defer func() {
		t.forceBackupAndRestore = false
	}()

	// We decide if we want to take a backup here based on a probability
	// specified in the logic test config.
	if rng.Float64() > config.BackupRestoreProbability && !t.forceBackupAndRestore {
		return nil
	}

	// Check if any users have open transactions in the logictest. If they do, we
	// do not want to teardown the cluster and create a new one as it might
	// interfere with what the logictest is trying to test.
	//
	// We could perhaps make this smarter and perform the backup after the
	// transaction is close.
	if t.hasOpenTxns(context.Background()) {
		return nil
	}

	oldUser := t.user
	oldNodeIdx := t.nodeIdx
	defer func() {
		t.setSessionUser(oldUser, oldNodeIdx)
	}()

	log.Info(context.Background(), "Running cluster backup and restore")

	// To restore the same state in for the logic test, we need to restore the
	// data and the session state. The session state includes things like session
	// variables that are set for every session that is open.
	//
	// TODO(adityamaru): A better approach might be to wipe the cluster once we
	// have a command that enables this. That way all of the session data will not
	// be lost in the process of creating a new cluster.
	users := make(map[string][]int, len(t.clients))
	userToHexSession := make(map[string]map[int]string, len(t.clients))
	userToSessionVars := make(map[string]map[int]map[string]string, len(t.clients))
	for user, userClients := range t.clients {
		userToHexSession[user] = make(map[int]string)
		userToSessionVars[user] = make(map[int]map[string]string)
		for nodeIdx := range userClients {
			users[user] = append(users[user], nodeIdx)
			t.setSessionUser(user, nodeIdx)

			// Serialize session variables.
			var userSession string
			var err error
			if err = t.db.QueryRow(`SELECT encode(crdb_internal.serialize_session(), 'hex')`).Scan(&userSession); err == nil {
				userToHexSession[user][nodeIdx] = userSession
				continue
			}
			log.Warningf(context.Background(), "failed to serialize session: %+v", err)

			// If we failed to serialize the session variables, lets save the output of
			// `SHOW ALL`. This usually happens if the session contains prepared
			// statements or portals that cause the `serialize_session()` to fail.
			//
			// Saving the session variables in this manner does not guarantee the test
			// will succeed since there are no ordering semantics when we go to apply
			// them. There are some session variables that need to be applied before
			// others for them to be valid. Thus, it is strictly better to use
			// `serialize/deserialize_session()`, this "hack" just gives the test one
			// more chance to succeed.
			userSessionVars := make(map[string]string)
			existingSessionVars, err := t.db.Query("SHOW ALL")
			if err != nil {
				return err
			}
			for existingSessionVars.Next() {
				var key, value string
				if err := existingSessionVars.Scan(&key, &value); err != nil {
					return errors.Wrap(err, "scanning session variables")
				}
				userSessionVars[key] = value
			}
			userToSessionVars[user][nodeIdx] = userSessionVars
		}
	}

	bucket := testutils.BackupTestingBucket()
	backupLocation := fmt.Sprintf("gs://%s/logic-test-backup-restore-nightly/%s?AUTH=implicit",
		bucket, strconv.FormatInt(timeutil.Now().UnixNano(), 10))

	// Perform the backup and restore as root.
	t.setSessionUser(username.RootUser, 0 /* nodeIdx */)

	if _, err := t.db.Exec(fmt.Sprintf("BACKUP INTO '%s'", backupLocation)); err != nil {
		return errors.Wrap(err, "backing up cluster")
	}

	// Create a new cluster. Perhaps this can be updated to just wipe the exiting
	// cluster once we have the ability to easily wipe a cluster through SQL.
	t.resetCluster()

	// Run the restore as root.
	t.setSessionUser(username.RootUser, 0 /* nodeIdx */)
	if _, err := t.db.Exec(fmt.Sprintf("RESTORE FROM LATEST IN '%s'", backupLocation)); err != nil {
		return errors.Wrap(err, "restoring cluster")
	}

	// Restore the session state that was in the old cluster.

	// Create new connections for the existing users, and restore the session
	// variables that we collected.
	for user, userNodeIdxs := range users {
		for _, nodeIdx := range userNodeIdxs {
			// Call setUser for every user to create the connection for that user.
			t.setSessionUser(user, nodeIdx)

			if userSession, ok := userToHexSession[user][nodeIdx]; ok {
				if _, err := t.db.Exec(fmt.Sprintf(`SELECT crdb_internal.deserialize_session(decode('%s', 'hex'))`, userSession)); err != nil {
					return errors.Wrapf(err, "deserializing session")
				}
			} else if vars, ok := userToSessionVars[user][nodeIdx]; ok {
				// We now attempt to restore the session variables that were set on the
				// backing up cluster. These are not included in the backup restore and so
				// have to be restored manually.
				for key, value := range vars {
					// First try setting the cluster setting as a string.
					if _, err := t.db.Exec(fmt.Sprintf("SET %s='%s'", key, value)); err != nil {
						// If it fails, try setting the value as an int.
						log.Infof(context.Background(), "setting session variable as string failed (err: %v), trying as int", pretty.Formatter(err))
						if _, err := t.db.Exec(fmt.Sprintf("SET %s=%s", key, value)); err != nil {
							// Some cluster settings can't be set at all, so ignore these errors.
							// If a setting that we needed could not be restored, we expect the
							// logic test to fail and let us know.
							log.Infof(context.Background(), "setting session variable as int failed: %v (continuing anyway)", pretty.Formatter(err))
							continue
						}
					}
				}
			}
		}
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

	s := logictestbase.NewLineScanner(file)
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
					path, s.Line,
				)
			}
			subtests = append(subtests, subtestDetails{
				name:                  curName,
				buffer:                buffer,
				lineLineIndexIntoFile: curLineIndexIntoFile,
			})
			buffer = &bytes.Buffer{}
			curName = fields[1]
			curLineIndexIntoFile = s.Line + 1
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

func (t *logicTest) purgeZoneConfig() {
	if t.cluster == nil {
		// We can only purge zone configs for in-memory test clusters.
		return
	}
	for i := 0; i < t.cluster.NumServers(); i++ {
		sysconfigProvider := t.cluster.Server(i).SystemConfigProvider()
		sysconfig := sysconfigProvider.GetSystemConfig()
		if sysconfig != nil {
			sysconfig.PurgeZoneConfigCache()
		}
	}
}

func (t *logicTest) processSubtest(
	subtest subtestDetails, path string, config logictestbase.TestClusterConfig,
) error {
	defer t.traceStop()

	s := logictestbase.NewLineScanner(subtest.buffer)
	t.lastProgress = timeutil.Now()

	repeat := 1
	t.retry = false

	for s.Scan() {
		t.curPath, t.curLineNo = path, s.Line+subtest.lineLineIndexIntoFile
		if *maxErrs > 0 && t.failures >= *maxErrs {
			return errors.Errorf("%s:%d: too many errors encountered, skipping the rest of the input",
				path, s.Line+subtest.lineLineIndexIntoFile,
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
				path, s.Line+subtest.lineLineIndexIntoFile,
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
				return errors.Wrapf(err, "%s:%d invalid repeat line",
					path, s.Line+subtest.lineLineIndexIntoFile,
				)
			}
			repeat = count
		case "skip_on_retry":
			t.skipOnRetry = true

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
				return errors.Wrapf(err, "%s:%d invalid sleep line",
					path, s.Line+subtest.lineLineIndexIntoFile,
				)
			}
			time.Sleep(duration)

		case "awaitstatement":
			if len(fields) != 2 {
				return errors.New("invalid line format")
			}

			name := fields[1]

			var pending pendingStatement
			var ok bool
			if pending, ok = t.pendingStatements[name]; !ok {
				return errors.Newf("pending statement with name %q unknown", name)
			}

			execRes := <-pending.resultChan
			cont, err := t.finishExecStatement(pending.logicStatement, execRes.execSQL, execRes.res, execRes.err)

			if err != nil {
				if !cont {
					return err
				}
				t.Error(err)
			}

			delete(t.pendingStatements, name)

			t.success(path)

		case "retry":
			// retry is a standalone command that may precede a "statement" or "query"
			// command. It has the same retry effect as the retry option of the query
			// command.
			t.retry = true
		case "statement":
			stmt := logicStatement{
				pos:         fmt.Sprintf("\n%s:%d", path, s.Line+subtest.lineLineIndexIntoFile),
				expectCount: -1,
			}
			// Parse "statement (notice|error) <regexp>"
			if m := noticeRE.FindStringSubmatch(s.Text()); m != nil {
				stmt.expectNotice = m[1]
			} else if m := errorRE.FindStringSubmatch(s.Text()); m != nil {
				stmt.expectErrCode = m[1]
				stmt.expectErr = m[2]
			}
			if len(fields) >= 3 && fields[1] == "async" {
				stmt.expectAsync = true
				stmt.statementName = fields[2]
				copy(fields[1:], fields[3:])
				fields = fields[:len(fields)-2]
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
			if !s.Skip {
				for i := 0; i < repeat; i++ {
					var cont bool
					var err error
					if t.retry {
						err = testutils.SucceedsSoonError(func() error {
							t.purgeZoneConfig()
							var tempErr error
							cont, tempErr = t.execStatement(stmt)
							return tempErr
						})
					} else {
						cont, err = t.execStatement(stmt)
					}
					if err != nil {
						if !cont {
							return err
						}
						t.Error(err)
					}
				}
			} else {
				s.LogAndResetSkip(t.t())
			}
			repeat = 1
			t.success(path)

		case "awaitquery":
			if len(fields) != 2 {
				return errors.New("invalid line format")
			}

			name := fields[1]

			var pending pendingQuery
			var ok bool
			if pending, ok = t.pendingQueries[name]; !ok {
				return errors.Newf("pending query with name %q unknown", name)
			}

			execRes := <-pending.resultChan
			err := t.finishExecQuery(pending.logicQuery, execRes.rows, execRes.err)
			if err != nil {
				t.Error(err)
			}

			delete(t.pendingQueries, name)
			t.success(path)

		case "query":
			var query logicQuery
			query.pos = fmt.Sprintf("\n%s:%d", path, s.Line+subtest.lineLineIndexIntoFile)
			query.nodeIdx = t.nodeIdx
			// Parse "query error <regexp>"
			if m := errorRE.FindStringSubmatch(s.Text()); m != nil {
				query.expectErrCode = m[1]
				query.expectErr = m[2]
			} else if len(fields) < 2 {
				return errors.Errorf("%s: invalid test statement: %s", query.pos, s.Text())
			} else {
				// Parse "query <type-string> <options> <label>"
				query.colTypes = fields[1]
				if *Bigtest {
					// bigtests put each expected value on its own line.
					query.valsPerLine = 1
				} else {
					// Otherwise, expect number of values to match expected type count.
					query.valsPerLine = len(query.colTypes)
				}

				if len(fields) >= 3 {
					query.rawOpts = fields[2]

					tokens := strings.Split(query.rawOpts, ",")

					// For tokens of the form tok(arg1, arg2, arg3), we want to collapse
					// these split tokens into one.
					buildArgumentTokens := func(argToken string) {
						for i := 0; i < len(tokens)-1; i++ {
							if strings.HasPrefix(tokens[i], argToken+"(") && !strings.HasSuffix(tokens[i], ")") {
								// Merge this token with the next.
								tokens[i] = tokens[i] + "," + tokens[i+1]
								// Delete tokens[i+1].
								copy(tokens[i+1:], tokens[i+2:])
								tokens = tokens[:len(tokens)-1]
								// Look at the new token again.
								i--
							}
						}
					}

					buildArgumentTokens("partialsort")
					buildArgumentTokens("kvtrace")

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

						if strings.HasPrefix(opt, "kvtrace(") && strings.HasSuffix(opt, ")") {
							s := opt
							s = strings.TrimPrefix(s, "kvtrace(")
							s = strings.TrimSuffix(s, ")")

							query.kvtrace = true
							query.kvOpTypes = nil
							query.keyPrefixFilters = nil
							for _, c := range strings.Split(s, ",") {
								if strings.HasPrefix(c, "prefix=") {
									matched := strings.TrimPrefix(c, "prefix=")
									query.keyPrefixFilters = append(query.keyPrefixFilters, matched)
								} else if isAllowedKVOp(c) {
									query.kvOpTypes = append(query.kvOpTypes, c)
								} else {
									return errors.Errorf(
										"invalid filter '%s' provided. Expected one of %v or a prefix of the form 'prefix=x'",
										c,
										allowedKVOpTypes,
									)
								}
							}
							continue
						}

						switch opt {
						case "nosort":
							query.sorter = nil
							query.noSort = true

						case "rowsort":
							query.sorter = rowSort

						case "valuesort":
							query.sorter = valueSort

						case "colnames":
							query.colNames = true

						case "retry":
							t.retry = true

						case "kvtrace":
							// kvtrace without any arguments doesn't perform any additional
							// filtering of results. So it displays kv's from all tables
							// and all operation types.
							query.kvtrace = true
							query.kvOpTypes = nil
							query.keyPrefixFilters = nil

						case "noticetrace":
							query.noticetrace = true

						case "async":
							query.expectAsync = true

						default:
							if strings.HasPrefix(opt, "round-in-strings") {
								significantFigures, err := floatcmp.ParseRoundInStringsDirective(opt)
								if err != nil {
									return err
								}
								query.roundFloatsInStringsSigFigs = significantFigures
								break
							}

							if strings.HasPrefix(opt, "nodeidx=") {
								idx, err := strconv.ParseInt(strings.SplitN(opt, "=", 2)[1], 10, 64)
								if err != nil {
									return errors.Wrapf(err, "error parsing nodeidx")
								}
								query.nodeIdx = int(idx)
								break
							}

							return errors.Errorf("%s: unknown sort mode: %s", query.pos, opt)
						}
					}
				}
				if len(fields) >= 4 {
					query.label = fields[3]
					if query.expectAsync {
						query.statementName = fields[3]
					}
				}
			}

			if query.noticetrace && query.kvtrace {
				return errors.Errorf(
					"%s: cannot have both noticetrace and kvtrace on at the same time",
					query.pos,
				)
			}

			if query.expectAsync && query.statementName == "" {
				return errors.Errorf(
					"%s: cannot have async enabled without a label",
					query.pos,
				)
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
										return errors.Errorf("expected results are invalid: unexpected column count %d != %d (%s)",
											len(results), len(query.colTypes), results)
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

			if !s.Skip {
				if query.kvtrace {
					_, err := t.db.Exec("SET TRACING=on,kv")
					if err != nil {
						return err
					}
					_, err = t.db.Exec(query.sql)
					if err != nil {
						t.Error(err)
					}
					_, err = t.db.Exec("SET TRACING=off")
					if err != nil {
						return err
					}

					queryPrefix := `SELECT message FROM [SHOW KV TRACE FOR SESSION] `
					buildQuery := func(ops []string, keyFilters []string) string {
						var sb strings.Builder
						sb.WriteString(queryPrefix)
						if len(keyFilters) == 0 {
							keyFilters = []string{""}
						}
						for i, c := range ops {
							for j, f := range keyFilters {
								if i+j == 0 {
									sb.WriteString("WHERE ")
								} else {
									sb.WriteString("OR ")
								}
								sb.WriteString(fmt.Sprintf("message like '%s %s%%'", c, f))
							}
						}
						return sb.String()
					}

					query.colTypes = "T"
					if len(query.kvOpTypes) == 0 {
						query.sql = buildQuery(allowedKVOpTypes, query.keyPrefixFilters)
					} else {
						query.sql = buildQuery(query.kvOpTypes, query.keyPrefixFilters)
					}
				}

				if query.noticetrace {
					query.colTypes = "T"
				}

				for i := 0; i < repeat; i++ {
					if t.retry && !*rewriteResultsInTestfiles {
						if err := testutils.SucceedsSoonError(func() error {
							t.purgeZoneConfig()
							return t.execQuery(query)
						}); err != nil {
							t.Error(err)
						}
					} else {
						if t.retry && *rewriteResultsInTestfiles {
							t.purgeZoneConfig()
							// The presence of the retry flag indicates that we expect this
							// query may need some time to succeed. If we are rewriting, wait
							// 2s before executing the query.
							// TODO(rytaft): We may want to make this sleep time configurable.
							time.Sleep(time.Second * 2)
						}
						if err := t.execQuery(query); err != nil {
							t.Error(err)
						}
					}
				}
			} else {
				if *rewriteResultsInTestfiles {
					for _, l := range query.expectedResultsRaw {
						t.emit(l)
					}
				}
				s.LogAndResetSkip(t.t())
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
				pos: fmt.Sprintf("\n%s:%d", path, s.Line+subtest.lineLineIndexIntoFile),
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
			t.t().Logf("let %s = %s\n", varName, val)
			t.varMap[varName] = val

		case "halt", "hash-threshold":

		case "user":
			var nodeIdx int
			if len(fields) < 2 {
				return errors.Errorf("user command requires one argument, found: %v", fields)
			}
			if len(fields[1]) == 0 {
				return errors.Errorf("user command requires a non-blank argument")
			}
			if len(fields) >= 3 {
				if strings.HasPrefix(fields[2], "nodeidx=") {
					idx, err := strconv.ParseInt(strings.SplitN(fields[2], "=", 2)[1], 10, 64)
					if err != nil {
						return errors.Wrapf(err, "error parsing nodeidx")
					}
					nodeIdx = int(idx)
				}
			}
			t.setSessionUser(fields[1], nodeIdx)
			// In multi-tenant tests, we may need to also create database test when
			// we switch to a different tenant.
			//
			// TODO(#76378): It seems the conditional should include `||
			// t.cluster.StartedDefaultTestTenant()` here, to cover the case
			// where the config specified "Random" and a test tenant was
			// effectively created.
			if t.cfg.UseSecondaryTenant == logictestbase.Always && strings.HasPrefix(fields[1], "host-cluster-") {
				if _, err := t.db.Exec("CREATE DATABASE IF NOT EXISTS test; USE test;"); err != nil {
					return errors.Wrapf(err, "error creating database on admin tenant")
				}
			}

		case "skip":
			reason := "skipped"
			if len(fields) > 1 {
				reason = fields[1]
			}
			skip.IgnoreLint(t.t(), reason)

		case "force-backup-restore":
			t.forceBackupAndRestore = true
			continue

		case "skipif":
			if len(fields) < 2 {
				return errors.Errorf("skipif command requires one argument, found: %v", fields)
			}
			switch fields[1] {
			case "":
				return errors.Errorf("skipif command requires a non-blank argument")
			case "mysql", "mssql":
			case "postgresql", "cockroachdb":
				s.SetSkip("")
				continue
			case "config":
				if len(fields) < 3 {
					return errors.New("skipif config CONFIG [ISSUE] command requires configuration parameter")
				}
				configName := fields[2]
				if t.cfg.Name == configName || logictestbase.ConfigIsInDefaultList(t.cfg.Name, configName) {
					issue := "no issue given"
					if len(fields) > 3 {
						issue = fields[3]
					}
					s.SetSkip(fmt.Sprintf("unsupported configuration %s (%s)", configName, issue))
				}
			case "backup-restore":
				if config.BackupRestoreProbability > 0.0 {
					s.SetSkip("backup-restore interferes with this check")
				}
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
			case "mysql", "mssql":
				s.SetSkip("")
				continue
			case "config":
				if len(fields) < 3 {
					return errors.New("onlyif config CONFIG [ISSUE] command requires configuration parameter")
				}
				configName := fields[2]
				if t.cfg.Name != configName && !logictestbase.ConfigIsInDefaultList(t.cfg.Name, configName) {
					issue := "no issue given"
					if len(fields) > 3 {
						issue = fields[3]
					}
					s.SetSkip(fmt.Sprintf("unsupported configuration %s, statement/query only supports %s (%s)", t.cfg.Name, configName, issue))
				}
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

		case "upgrade":
			if len(fields) != 2 {
				return errors.Errorf("upgrade requires a node num argument, found: %v", fields)
			}
			if t.testserverCluster == nil {
				return errors.Errorf(`could not perform "upgrade", not a cockroach-go/testserver cluster`)
			}
			nodeIdx, err := strconv.Atoi(fields[1])
			if err != nil {
				t.Fatal(err)
			}
			if err := t.testserverCluster.UpgradeNode(nodeIdx); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < t.cfg.NumNodes; i++ {
				// Wait for each node to be reachable, since UpgradeNode uses `kill`
				// to terminate nodes, and may introduce temporary unavailability in
				// the system range.
				if err := t.testserverCluster.WaitForInitFinishForNode(i); err != nil {
					t.Fatal(err)
				}
			}
			// The port may have changed, so we must remove all the cached connections
			// to this node.
			for _, m := range t.clients {
				if c, ok := m[nodeIdx]; ok {
					_ = c.Close()
				}
				delete(m, nodeIdx)
			}
			// If we upgraded the node we are currently on, we need to open a new
			// connection since the previous one might now be invalid.
			if t.nodeIdx == nodeIdx {
				t.setSessionUser(t.user, nodeIdx)
			}
		default:
			return errors.Errorf("%s:%d: unknown command: %s",
				path, s.Line+subtest.lineLineIndexIntoFile, cmd,
			)
		}
	}
	return s.Err()
}

// Some tests encounter serializability failures sometimes and indicate
// that they want to just get skipped when that happens.
func (t *logicTest) maybeSkipOnRetry(err error) {
	if pqErr := (*pq.Error)(nil); t.skippedOnRetry ||
		(t.skipOnRetry && errors.As(err, &pqErr) &&
			pgcode.MakeCode(string(pqErr.Code)) == pgcode.SerializationFailure) {
		t.skippedOnRetry = true
		skip.WithIssue(t.t(), 53724)
	}
}

// verifyError checks that either:
// - no error was found when none was expected, or
// - in case no error was found, a notice was found when one was expected, or
// - an error was found when one was expected.
// Returns a nil error to indicate the behavior was as expected.  If
// non-nil, returns also true in the boolean flag whether it is safe
// to continue (i.e. an error was expected, an error was obtained, and
// the errors didn't match).
func (t *logicTest) verifyError(
	sql, pos, expectNotice, expectErr, expectErrCode string, err error,
) (bool, error) {
	t.maybeSkipOnRetry(err)
	if expectErr == "" && expectErrCode == "" && err != nil {
		return t.unexpectedError(sql, pos, err)
	}
	if expectNotice != "" {
		foundNotice := strings.Join(t.noticeBuffer, "\n")
		match, _ := regexp.MatchString(expectNotice, foundNotice)
		if !match {
			return false, errors.Errorf("%s: %s\nexpected notice pattern:\n%s\n\ngot:\n%s", pos, sql, expectNotice, foundNotice)
		}
		return true, nil
	}
	if !testutils.IsError(err, expectErr) {
		if err == nil {
			newErr := errors.Errorf("%s: %s\nexpected %q, but no error occurred", pos, sql, expectErr)
			return false, newErr
		}

		errString := pgerror.FullError(err)
		newErr := errors.Errorf("%s: %s\nexpected:\n%s\n\ngot:\n%s", pos, sql, expectErr, errString)
		if strings.Contains(errString, expectErr) {
			t.t().Logf("The output string contained the input regexp. Perhaps you meant to write:\n"+
				"query error %s", regexp.QuoteMeta(errString))
		}
		// We can't rewrite the error, but we can at least print the regexp.
		if *rewriteResultsInTestfiles {
			r := regexp.QuoteMeta(errString)
			r = strings.Trim(r, "\n ")
			r = strings.ReplaceAll(r, "\n", "\\n")
			t.t().Logf("Error regexp: %s\n", r)
		}
		return expectErr != "", newErr
	}
	if err != nil {
		if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) &&
			strings.HasPrefix(string(pqErr.Code), "XX" /* internal error, corruption, etc */) &&
			pgcode.MakeCode(string(pqErr.Code)) != pgcode.Uncategorized /* this is also XX but innocuous */ {
			if expectErrCode != string(pqErr.Code) {
				return false, errors.Errorf(
					"%s: %s: serious error with code %q occurred; if expected, must use 'error pgcode %s ...' in test:\n%s",
					pos, sql, pqErr.Code, pqErr.Code, pgerror.FullError(err))
			}
		}
	}
	if expectErrCode != "" {
		if err != nil {
			var pqErr *pq.Error
			if !errors.As(err, &pqErr) {
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
			return err != nil, newErr
		}
	}
	return true, nil
}

// formatErr attempts to provide more details if present.
func formatErr(err error) string {
	if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "(%s) %s", pqErr.Code, pqErr.Message)
		if pqErr.File != "" || pqErr.Line != "" || pqErr.Routine != "" {
			fmt.Fprintf(&buf, "\n%s:%s: in %s()", pqErr.File, pqErr.Line, pqErr.Routine)
		}
		if pqErr.Detail != "" {
			fmt.Fprintf(&buf, "\nDETAIL: %s", pqErr.Detail)
		}
		if pgcode.MakeCode(string(pqErr.Code)) == pgcode.Internal {
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
func (t *logicTest) unexpectedError(sql string, pos string, err error) (bool, error) {
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
			return true, nil
		}
		if err := stmt.Close(); err != nil {
			t.Errorf("%s: %s\nerror when closing prepared statement: %s", sql, pos, formatErr(err))
		}
	}
	// N.B. We return an error instead of calling t.Errorf because this query
	// could be asking for a retry. We still use t.Errorf above because
	// stmt.Close error is probably a sign of bigger issues and not
	// something retryable.
	return false, fmt.Errorf("%s: %s\nexpected success, but found\n%s", pos, sql, formatErr(err))
}

func (t *logicTest) execStatement(stmt logicStatement) (bool, error) {
	db := t.db
	t.noticeBuffer = nil
	if *showSQL {
		t.outf("%s;", stmt.sql)
	}
	execSQL, changed := randgen.ApplyString(t.rng, stmt.sql, randgen.ColumnFamilyMutator)
	if changed {
		log.Infof(context.Background(), "Rewrote test statement:\n%s", execSQL)
		if *showSQL {
			t.outf("rewrote:\n%s\n", execSQL)
		}
	}

	if stmt.expectAsync {
		if _, ok := t.pendingStatements[stmt.statementName]; ok {
			return false, errors.Newf("pending statement with name %q already exists", stmt.statementName)
		}

		pending := pendingStatement{
			logicStatement: stmt,
			resultChan:     make(chan pendingExecResult),
		}
		t.pendingStatements[stmt.statementName] = pending

		startedChan := make(chan struct{})
		go func() {
			startedChan <- struct{}{}
			res, err := db.Exec(execSQL)
			pending.resultChan <- pendingExecResult{execSQL, res, err}
		}()

		<-startedChan
		return true, nil
	}

	res, err := db.Exec(execSQL)
	return t.finishExecStatement(stmt, execSQL, res, err)
}

func (t *logicTest) finishExecStatement(
	stmt logicStatement, execSQL string, res gosql.Result, err error,
) (bool, error) {
	if err == nil {
		sqlutils.VerifyStatementPrettyRoundtrip(t.t(), stmt.sql)
	}
	if err == nil && stmt.expectCount >= 0 {
		var count int64
		count, err = res.RowsAffected()

		// If err becomes non-nil here, we'll catch it below.

		if err == nil && count != stmt.expectCount {
			t.Errorf("%s: %s\nexpected %d rows affected, got %d", stmt.pos, execSQL, stmt.expectCount, count)
		}
	}

	// General policy for failing vs. continuing:
	// - we want to do as much work as possible;
	// - however, a statement that fails when it should succeed or
	//   a statement that succeeds when it should fail may have left
	//   the database in an improper state, so we stop there;
	// - error on expected error is worth going further, even
	//   if the obtained error does not match the expected error.
	cont, err := t.verifyError("", stmt.pos, stmt.expectNotice, stmt.expectErr, stmt.expectErrCode, err)
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

	t.noticeBuffer = nil

	db := t.db
	if query.nodeIdx != t.nodeIdx {
		db = t.getOrOpenClient(t.user, query.nodeIdx)
	}

	if query.expectAsync {
		if _, ok := t.pendingQueries[query.statementName]; ok {
			return errors.Newf("pending query with name %q already exists", query.statementName)
		}

		pending := pendingQuery{
			logicQuery: query,
			resultChan: make(chan pendingQueryResult),
		}
		t.pendingQueries[query.statementName] = pending

		if *rewriteResultsInTestfiles || *rewriteSQL {
			t.emit(fmt.Sprintf("%s_%s", queryRewritePlaceholderPrefix, query.statementName))
		}

		startedChan := make(chan struct{})
		go func() {
			startedChan <- struct{}{}
			rows, err := db.Query(query.sql)
			pending.resultChan <- pendingQueryResult{rows, err}
		}()

		<-startedChan
		return nil
	}

	rows, err := db.Query(query.sql)
	return t.finishExecQuery(query, rows, err)
}

func (t *logicTest) finishExecQuery(query logicQuery, rows *gosql.Rows, err error) error {
	if err == nil {
		sqlutils.VerifyStatementPrettyRoundtrip(t.t(), query.sql)

		// If expecting an error, then read all result rows, since some errors are
		// only triggered after initial rows are returned.
		if query.expectErr != "" {
			// Break early if error is detected, and be sure to test for error in case
			// where Next returns false.
			for rows.Next() {
				if rows.Err() != nil {
					break
				}
			}
			err = rows.Err()
		}
	}
	if _, err := t.verifyError(query.sql, query.pos, "", query.expectErr, query.expectErrCode, err); err != nil {
		return err
	}
	if err != nil {
		// An error occurred, but it was expected.
		t.finishOne("XFAIL")
		//nolint:returnerrcheck
		return nil
	}
	defer rows.Close()

	var actualResultsRaw []string
	rowCount := 0
	if query.noticetrace {
		// We have to force close the results for the notice handler from lib/pq
		// returns results.
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()
		actualResultsRaw = t.noticeBuffer
	} else {
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

		if query.colNames {
			actualResultsRaw = append(actualResultsRaw, cols...)
		}
		for nextResultSet := true; nextResultSet; nextResultSet = rows.NextResultSet() {
			for rows.Next() {
				if err := rows.Scan(vals...); err != nil {
					return err
				}
				rowCount++
				for i, v := range vals {
					colT := query.colTypes[i]
					// Ignore column - useful for non-deterministic output.
					if colT == '_' {
						actualResultsRaw = append(actualResultsRaw, "_")
						continue
					}
					val := *v.(*interface{})
					if val == nil {
						actualResultsRaw = append(actualResultsRaw, "NULL")
						continue
					}
					valT := reflect.TypeOf(val).Kind()
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
					case 'F', 'R':
						if valT != reflect.Float64 && valT != reflect.Slice {
							if *flexTypes && (valT == reflect.Int64) {
								t.signalIgnoredError(
									fmt.Errorf("result type mismatch: expected F or R, got %T", val), query.pos, query.sql,
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
					// Empty strings are rendered as "·" (middle dot).
					if val == "" {
						val = "·"
					}
					s := fmt.Sprint(val)
					if query.roundFloatsInStringsSigFigs > 0 {
						s = floatcmp.RoundFloatsInString(s, query.roundFloatsInStringsSigFigs)
					}
					actualResultsRaw = append(actualResultsRaw, s)
				}
			}
			if err := rows.Err(); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
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

	allDuplicateRows := true
	numCols := len(query.colTypes)
	resultsWithoutColNames := actualResults
	if query.colNames {
		resultsWithoutColNames = resultsWithoutColNames[numCols:]
	}
	for i := numCols; i < len(resultsWithoutColNames); i++ {
		// There are numCols*numRows elements in actualResults, each a string
		// representation of a single column in a row. The element at i%numCols
		// is the value in the first row in the same column as i.
		if resultsWithoutColNames[i%numCols] != resultsWithoutColNames[i] {
			allDuplicateRows = false
			break
		}
	}

	if rowCount > 1 && !allDuplicateRows && query.sorter == nil && !query.noSort &&
		!query.kvtrace && !orderRE.MatchString(query.sql) && !explainRE.MatchString(query.sql) &&
		!showTraceRE.MatchString(query.sql) {
		return fmt.Errorf("to prevent flakes in queries that return multiple rows, " +
			"add the rowsort option, the valuesort option, the partialsort option, " +
			"or an ORDER BY clause. If you are certain that your test will not flake " +
			"due to a non-deterministic ordering of rows, you can add the nosort option " +
			"to ignore this error")
	}

	if query.sorter != nil {
		query.sorter(len(query.colTypes), actualResults)
		query.sorter(len(query.colTypes), query.expectedResults)
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
			var suffix string
			for _, colT := range query.colTypes {
				if colT == 'F' {
					suffix = "\tthis might be due to floating numbers precision deviation"
					break
				}
			}
			return fmt.Errorf("%s: expected %s, but found %s%s", query.pos, query.expectedHash, hash, suffix)
		}
	}

	resultsMatch := func() error {
		makeError := func() error {
			var expFormatted strings.Builder
			var actFormatted strings.Builder
			for _, line := range query.expectedResultsRaw {
				fmt.Fprintf(&expFormatted, "    %s\n", line)
			}
			for _, line := range t.formatValues(actualResultsRaw, query.valsPerLine) {
				fmt.Fprintf(&actFormatted, "    %s\n", line)
			}

			sortMsg := ""
			if query.sorter != nil {
				// We performed an order-insensitive comparison of "actual" vs "expected"
				// rows by sorting both, but we'll display the error with the expected
				// rows in the order in which they were put in the file, and the actual
				// rows in the order in which the query returned them.
				sortMsg = " -> ignore the following ordering of rows"
			}
			var buf bytes.Buffer
			fmt.Fprintf(&buf, "%s: %s\nexpected:\n%s", query.pos, query.sql, expFormatted.String())
			fmt.Fprintf(&buf, "but found (query options: %q%s) :\n%s", query.rawOpts, sortMsg, actFormatted.String())
			if *showDiff {
				if diff, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A:        difflib.SplitLines(expFormatted.String()),
					B:        difflib.SplitLines(actFormatted.String()),
					FromFile: "Expected",
					FromDate: "",
					ToFile:   "Actual",
					ToDate:   "",
					Context:  1,
				}); err == nil {
					fmt.Fprintf(&buf, "\nDiff:\n%s", diff)
				}
			}
			return errors.Newf("%s\n", buf.String())
		}
		if len(query.expectedResults) != len(actualResults) {
			return makeError()
		}
		for i := range query.expectedResults {
			expected, actual := query.expectedResults[i], actualResults[i]
			var resultMatches bool
			if query.noticetrace {
				resultMatches, err = regexp.MatchString(expected, actual)
				if err != nil {
					return errors.CombineErrors(makeError(), err)
				}
			} else {
				resultMatches = expected == actual
			}

			// Results are flattened into columns for each row.
			// To find the coltype for the given result, mod the result number
			// by the number of coltypes.
			colT := query.colTypes[i%len(query.colTypes)]
			if !resultMatches {
				var err error
				// On s390x, check that values for both float ('F') and decimal
				// ('R') coltypes are approximately equal to take into account
				// platform differences in floating point calculations.
				if runtime.GOARCH == "s390x" && (colT == 'F' || colT == 'R') {
					resultMatches, err = floatcmp.FloatsMatchApprox(expected, actual)
				} else if colT == 'F' {
					resultMatches, err = floatcmp.FloatsMatch(expected, actual)
				}
				if err != nil {
					return errors.CombineErrors(makeError(), err)
				}
			}
			if !resultMatches {
				return makeError()
			}
		}
		return nil
	}

	if *rewriteResultsInTestfiles || *rewriteSQL {
		var remainder *bufio.Scanner
		if query.expectAsync {
			remainder = t.rewriteUpToRegex(regexp.MustCompile(fmt.Sprintf("^%s_%s$", queryRewritePlaceholderPrefix, query.statementName)))
		}
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
			if !*rewriteResultsInTestfiles || resultsMatch() == nil {
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
		if remainder != nil {
			for remainder.Scan() {
				t.emit(remainder.Text())
			}
		}
		return nil
	}

	if query.checkResults {
		if err := resultsMatch(); err != nil {
			return err
		}
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

	numLines := len(vals) / valsPerLine
	for line := 0; line < numLines; line++ {
		maxSubLines := 0
		lineOffset := line * valsPerLine

		// Split multi-line values into sublines to output correctly formatted rows.
		lineSubLines := make([][]string, valsPerLine)
		for i := 0; i < valsPerLine; i++ {
			cellSubLines := strings.Split(vals[lineOffset+i], "\n")
			lineSubLines[i] = cellSubLines
			numSubLines := len(cellSubLines)
			if numSubLines > maxSubLines {
				maxSubLines = numSubLines
			}
		}

		for j := 0; j < maxSubLines; j++ {
			for i := 0; i < len(lineSubLines); i++ {
				cellSubLines := lineSubLines[i]
				// If a value's #sublines < #maxSubLines, an empty cell (just a "\t") is written to preserve columns.
				if j < len(cellSubLines) {
					cellSubLine := cellSubLines[j]
					// Replace tabs with spaces to prevent them from being interpreted by tabwriter.
					cellSubLine = strings.ReplaceAll(cellSubLine, "\t", "  ")
					fmt.Fprint(tw, cellSubLine)
				}
				fmt.Fprint(tw, "\t")
			}
			fmt.Fprint(tw, "\n")
		}
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
	t.retry = false
	t.progress++
	now := timeutil.Now()
	if now.Sub(t.lastProgress) >= 2*time.Second {
		t.lastProgress = now
		t.outf("--- progress: %s: %d statements", file, t.progress)
	}
}

func (t *logicTest) validateAfterTestCompletion() error {
	// Error on any unfinished async statements or queries
	if len(t.pendingStatements) > 0 || len(t.pendingQueries) > 0 {
		t.Fatalf("%d remaining async statements, %d remaining async queries", len(t.pendingStatements), len(t.pendingQueries))
	}

	// Close all clients other than "root"
	for user, userClients := range t.clients {
		if user == username.RootUser {
			continue
		}
		for i, c := range userClients {
			if err := c.Close(); err != nil {
				t.Fatalf("failed to close connection to node %d for user %s: %v", i, user, err)
			}
		}
		delete(t.clients, user)
	}
	t.setSessionUser(username.RootUser, 0 /* nodeIdx */)

	// Some cleanup to make sure the following validation queries can run
	// successfully. First we rollback in case the logic test had an uncommitted
	// txn and second we reset vectorize mode in case it was switched to
	// `experimental_always`.
	_, _ = t.db.Exec("ROLLBACK")
	if _, err := t.db.Exec("RESET vectorize"); err != nil {
		t.Fatal(errors.Wrap(err, "could not reset vectorize mode"))
	}
	if _, err := t.db.Exec("RESET ROLE"); err != nil {
		t.Fatal(errors.Wrap(err, "could not reset role"))
	}

	validate := func() (string, error) {
		rows, err := t.db.Query(`SELECT * FROM "".crdb_internal.invalid_objects ORDER BY id`)
		if err != nil {
			return "", err
		}
		defer rows.Close()

		var id int64
		var db, schema, objName, errStr string
		invalidObjects := make([]string, 0)
		for rows.Next() {
			if err := rows.Scan(&id, &db, &schema, &objName, &errStr); err != nil {
				return "", err
			}
			invalidObjects = append(
				invalidObjects,
				fmt.Sprintf("id %d, db %s, schema %s, name %s: %s", id, db, schema, objName, errStr),
			)
		}
		if err := rows.Err(); err != nil {
			return "", err
		}
		return strings.Join(invalidObjects, "\n"), nil
	}

	invalidObjects, err := validate()
	if err != nil {
		return errors.Wrap(err, "running object validation failed")
	}
	if invalidObjects != "" {
		return errors.Errorf("descriptor validation failed:\n%s", invalidObjects)
	}

	// Ensure that all of the created descriptors can round-trip through json.
	{
		// If `useCockroachGoTestserver` is true and we do an upgrade,
		// this may fail if we're in between migrations that
		// upgrade the descriptors.
		//
		// We also want to skip this check for mixed-version configurations (which
		// have DisableUpgrade=true and an old BootstrapVersion) in case new
		// fields are added to the descriptor in the newer version. In mixed-version
		// test configs, nodes are bootstraped with the older version of the system
		// tables which don't include the new fields in the protobuf, so they will
		// fail to round-trip.
		if !t.cfg.UseCockroachGoTestserver &&
			!(t.cfg.DisableUpgrade && t.cfg.BootstrapVersion != clusterversion.Key(0)) {
			rows, err := t.db.Query(
				`
SELECT encode(descriptor, 'hex') AS descriptor
  FROM system.descriptor
 WHERE descriptor
       != crdb_internal.json_to_pb(
            'cockroach.sql.sqlbase.Descriptor',
            crdb_internal.pb_to_json(
                'cockroach.sql.sqlbase.Descriptor',
                descriptor,
                false -- emit_defaults
            )
        );
`,
			)
			if err != nil {
				return errors.Wrap(err, "failed to test for descriptor JSON round-trip")
			}
			rowsMat, err := sqlutils.RowsToStrMatrix(rows)
			if err != nil {
				return errors.Wrap(err, "failed read rows from descriptor JSON round-trip")
			}
			if len(rowsMat) > 0 {
				return errors.Errorf("some descriptors did not round-trip:\n%s",
					sqlutils.MatrixToStr(rowsMat))
			}
		}
	}

	if err := t.maybeDropDatabases(); err != nil {
		return err
	}

	// Ensure after dropping all databases state is still valid.
	invalidObjects, err = validate()
	if err != nil {
		return errors.Wrap(err, "running object validation after database drops failed")
	}
	if invalidObjects != "" {
		return errors.Errorf(
			"descriptor validation failed after dropping databases:\n%s", invalidObjects,
		)
	}

	return nil
}

func (t *logicTest) maybeDropDatabases() error {
	var dbNames pq.StringArray
	if err := t.db.QueryRow(
		`SELECT array_agg(database_name) FROM [SHOW DATABASES] WHERE database_name NOT IN ('system', 'postgres')`,
	).Scan(&dbNames); err != nil {
		return errors.Wrap(err, "error getting database names")
	}

	for _, dbName := range dbNames {
		if err := func() (retErr error) {
			ctx := context.Background()
			// Open a new connection, since we want to preserve the original DB
			// that we were originally on in t.db.
			conn, err := t.db.Conn(ctx)
			if err != nil {
				return errors.Wrap(err, "error grabbing new connection")
			}
			defer func() {
				retErr = errors.CombineErrors(retErr, conn.Close())
			}()
			if _, err := conn.ExecContext(ctx, "SET database = $1", dbName); err != nil {
				return errors.Wrapf(err, "error validating zone config for database %s", dbName)
			}
			// Ensure each database's zone configs are valid.
			if _, err := conn.ExecContext(ctx, "SELECT crdb_internal.validate_multi_region_zone_configs()"); err != nil {
				return errors.Wrapf(err, "error validating zone config for database %s", dbName)
			}
			// Drop the database.
			dbTreeName := tree.Name(dbName)
			dropDatabaseStmt := fmt.Sprintf(
				"DROP DATABASE %s CASCADE",
				dbTreeName.String(),
			)
			if _, err := t.db.Exec(dropDatabaseStmt); err != nil {
				return errors.Wrapf(err, "dropping database %s failed", dbName)
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}

func (t *logicTest) runFile(path string, config logictestbase.TestClusterConfig) {
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

	if err := t.validateAfterTestCompletion(); err != nil {
		t.Fatal(errors.Wrap(err, "test was successful but validation upon completion failed"))
	}
}

var skipLogicTests = envutil.EnvOrDefaultBool("COCKROACH_LOGIC_TESTS_SKIP", false)
var logicTestsConfigExclude = envutil.EnvOrDefaultString("COCKROACH_LOGIC_TESTS_SKIP_CONFIG", "")
var logicTestsConfigFilter = envutil.EnvOrDefaultString("COCKROACH_LOGIC_TESTS_CONFIG", "")

// TestServerArgs contains the parameters that callers of RunLogicTest might
// want to specify for the test clusters to be created with.
type TestServerArgs struct {
	// MaxSQLMemoryLimit determines the value of --max-sql-memory startup
	// argument for the server. If unset, then the default limit of 256MiB will
	// be used.
	MaxSQLMemoryLimit int64
	// If set, mutations.MaxBatchSize, row.getKVBatchSize, and other values
	// randomized via the metamorphic testing will be overridden to use the
	// production value.
	ForceProductionValues bool
	// If set, then sql.distsql.temp_storage.workmem is not randomized.
	DisableWorkmemRandomization bool
	// DeclarativeCorpusCollection corpus will be collected for the declarative
	// schema changer.
	DeclarativeCorpusCollection bool
	// If set, then we will disable the metamorphic randomization of
	// useMVCCRangeTombstonesForPointDeletes variable.
	DisableUseMVCCRangeTombstonesForPointDeletes bool
	// If positive, it provides a lower bound for the default-batch-bytes-limit
	// metamorphic constant.
	BatchBytesLimitLowerBound int64
	// If set, sql.distsql.direct_columnar_scans.enabled is set to false.
	DisableDirectColumnarScans bool
}

// RunLogicTests runs logic tests for all files matching the given glob.
func RunLogicTests(
	t *testing.T, serverArgs TestServerArgs, configIdx logictestbase.ConfigIdx, glob string,
) {
	paths, err := filepath.Glob(glob)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		RunLogicTest(t, serverArgs, configIdx, p)
	}
}

// RunLogicTest is the main entry point for the logic test.
func RunLogicTest(
	t *testing.T, serverArgs TestServerArgs, configIdx logictestbase.ConfigIdx, path string,
) {
	// Note: there is special code in teamcity-trigger/main.go to run this package
	// with less concurrency in the nightly stress runs. If you see problems
	// please make adjustments there.
	// As of 6/4/2019, the logic tests never complete under race.
	skip.UnderStressRace(t, "logic tests and race detector don't mix: #37993")

	if skipLogicTests {
		skip.IgnoreLint(t, "COCKROACH_LOGIC_TESTS_SKIP")
	}

	var progress = struct {
		total, totalFail, totalUnsupported int
		lastProgress                       time.Time
	}{
		lastProgress: timeutil.Now(),
	}

	// Check whether the test can only be run in non-metamorphic mode.
	_, nonMetamorphicBatchSizes :=
		logictestbase.ReadTestFileConfigs(t, path, logictestbase.ConfigSet{configIdx})
	config := logictestbase.LogicTestConfigs[configIdx]

	// The tests below are likely to run concurrently; `log` is shared
	// between all the goroutines and thus all tests, so it doesn't make
	// sense to try to use separate `log.Scope` instances for each test.
	logScope := log.Scope(t)
	defer logScope.Close(t)

	verbose := testing.Verbose() || log.V(1)

	// Only used in rewrite mode, where we don't need to run the same file through
	// multiple configs.

	if testing.Short() && config.SkipShort {
		skip.IgnoreLint(t, "config skipped by -test.short")
	}
	if logicTestsConfigExclude != "" && config.Name == logicTestsConfigExclude {
		skip.IgnoreLint(t, "config excluded via env var")
	}
	if logicTestsConfigFilter != "" && config.Name != logicTestsConfigFilter {
		skip.IgnoreLint(t, "config does not match env var")
	}

	var cc *corpus.Collector
	if *saveDeclarativeCorpus != "" {
		var err error
		cc, err = corpus.NewCorpusCollector(*saveDeclarativeCorpus)
		if err != nil {
			t.Fatalf("failed to create collector %v", err)
		}
		defer func() {
			err := cc.UpdateCorpus()
			if err != nil {
				t.Fatalf("failed writing decalarative schema changer corpus: %v", err)
			}
		}()
	}

	// Testing sql.distsql.temp_storage.workmem metamorphically isn't needed
	// when rewriting logic test files, so we disable workmem randomization if
	// the --rewrite flag is present.
	if *defaultWorkmem || *rewriteResultsInTestfiles {
		serverArgs.DisableWorkmemRandomization = true
	}

	rng, _ := randutil.NewTestRand()
	lt := logicTest{
		rootT:                      t,
		verbose:                    verbose,
		perErrorSummary:            make(map[string][]string),
		rng:                        rng,
		declarativeCorpusCollector: cc,
	}
	if *printErrorSummary {
		defer lt.printErrorSummary()
	}
	// Each test needs a copy because of Parallel
	serverArgsCopy := serverArgs
	serverArgsCopy.ForceProductionValues = serverArgs.ForceProductionValues || nonMetamorphicBatchSizes
	if serverArgsCopy.ForceProductionValues {
		if err := coldata.SetBatchSizeForTests(coldata.DefaultColdataBatchSize); err != nil {
			panic(errors.Wrapf(err, "could not set batch size for test"))
		}
	} else if serverArgsCopy.BatchBytesLimitLowerBound > 0 {
		// If we're not forcing the production values, but we're asked to have a
		// lower bound on the batch bytes limit, then check whether the lower
		// bound is already satisfied and update the value if not.
		min := rowinfra.BytesLimit(serverArgsCopy.BatchBytesLimitLowerBound)
		if rowinfra.GetDefaultBatchBytesLimit(false /* forceProductionValue */) < min {
			value := min + rowinfra.BytesLimit(rng.Intn(100<<10))
			rowinfra.SetDefaultBatchBytesLimitForTests(value)
		}
	}
	hasOverride, overriddenBackupRestoreProbability := logictestbase.ReadBackupRestoreProbabilityOverride(t, path)
	config.BackupRestoreProbability = backupRestoreProbability
	if hasOverride {
		config.BackupRestoreProbability = overriddenBackupRestoreProbability
	}

	lt.setup(
		config, serverArgsCopy, readClusterOptions(t, path), readKnobOptions(t, path), readTenantOverrideArgs(t, path),
	)

	lt.runFile(path, config)

	progress.total += lt.progress
	progress.totalFail += lt.failures
	progress.totalUnsupported += lt.unsupported
	now := timeutil.Now()
	if now.Sub(progress.lastProgress) >= 2*time.Second {
		progress.lastProgress = now
		lt.outf("--- total progress: %d statements", progress.total)
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
	t.t().Helper()
	if *showSQL {
		t.outf("\t-- FAIL")
	}
	log.Errorf(context.Background(), "\n%s", fmt.Sprint(args...))
	t.t().Error("\n", fmt.Sprint(args...))
	t.failures++
}

// Errorf is a wrapper around testing.T.Errorf that handles printing the
// per-query "FAIL" marker when -show-sql is set. It also registers the error to
// the failure counter.
func (t *logicTest) Errorf(format string, args ...interface{}) {
	t.t().Helper()
	if *showSQL {
		t.outf("\t-- FAIL")
	}
	log.Errorf(context.Background(), format, args...)
	t.t().Errorf("\n"+format, args...)
	t.failures++
}

// Fatal is a wrapper around testing.T.Fatal that ensures the fatal error message
// is printed on its own line when -show-sql is set.
func (t *logicTest) Fatal(args ...interface{}) {
	t.t().Helper()
	if *showSQL {
		fmt.Println()
	}
	log.Errorf(context.Background(), "%s", fmt.Sprint(args...))
	t.t().Logf("\n%s:%d: error while processing", t.curPath, t.curLineNo)
	t.t().Fatal(args...)
}

// Fatalf is a wrapper around testing.T.Fatalf that ensures the fatal error
// message is printed on its own line when -show-sql is set.
func (t *logicTest) Fatalf(format string, args ...interface{}) {
	if *showSQL {
		fmt.Println()
	}
	log.Errorf(context.Background(), format, args...)
	t.t().Logf("\n%s:%d: error while processing", t.curPath, t.curLineNo)
	t.t().Fatalf(format, args...)
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
func (t *logicTest) printCompletion(path string, config logictestbase.TestClusterConfig) {
	unsupportedMsg := ""
	if t.unsupported > 0 {
		unsupportedMsg = fmt.Sprintf(", ignored %d unsupported queries", t.unsupported)
	}
	t.outf("--- done: %s with config %s: %d tests, %d failures%s", path, config.Name,
		t.progress, t.failures, unsupportedMsg)
}
