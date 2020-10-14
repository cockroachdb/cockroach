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
	gobuild "go/build"
	"io"
	"math"
	"math/rand"
	"net/url"
	"os"
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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/floatcmp"
	"github.com/cockroachdb/cockroach/pkg/testutils/physicalplanutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
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
// logicTestConfigs. If the directive is missing, the test is run in the
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
// There is a special blocklist directive '!metamorphic' that skips the whole
// test when TAGS=metamorphic is specified for the logic test invocation.
// NOTE: metamorphic directive takes precedence over all other directives.
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
//  - user <username> [nodeidx=N]
//    Changes the user for subsequent statements or queries.
//    If nodeidx is specified, this user will connect to the node
//    in the cluster with index N (note this is 0-indexed, while
//    node IDs themselves are 1-indexed).
//
//    A "host-cluster-" prefix can be prepended to the user, which will force
//    the user session to be against the host cluster (useful for multi-tenant
//    configurations).
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
	resultsRE = regexp.MustCompile(`^(\d+)\s+values?\s+hashing\s+to\s+([0-9A-Fa-f]+)$`)
	noticeRE  = regexp.MustCompile(`^statement\s+notice\s+(.*)$`)
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
	printBlocklistIssues = flag.Bool(
		"print-blocklist-issues", false,
		"for any test files that contain a blocklist directive, print a link to the associated issue",
	)
)

type testClusterConfig struct {
	// name is the name of the config (used for subtest names).
	name     string
	numNodes int
	// TODO(asubiotto): The fake span resolver does not currently play well with
	// contention events and tracing (see #61438).
	useFakeSpanResolver bool
	// if non-empty, overrides the default distsql mode.
	overrideDistSQLMode string
	// if non-empty, overrides the default vectorize mode.
	overrideVectorize string
	// if non-empty, overrides the default automatic statistics mode.
	overrideAutoStats string
	// if non-empty, overrides the default experimental DistSQL planning mode.
	overrideExperimentalDistSQLPlanning string
	// if set, queries using distSQL processors or vectorized operators that can
	// fall back to disk do so immediately, using only their disk-based
	// implementation.
	sqlExecUseDisk bool
	// if set, enables DistSQL metadata propagation tests.
	distSQLMetadataTestEnabled bool
	// if set and the -test.short flag is passed, skip this config.
	skipShort bool
	// If not empty, bootstrapVersion controls what version the cluster will be
	// bootstrapped at.
	bootstrapVersion roachpb.Version
	// If not empty, binaryVersion is used to set what the Server will consider
	// to be the binary version.
	binaryVersion  roachpb.Version
	disableUpgrade bool
	// If true, a sql tenant server will be started and pointed at a node in the
	// cluster. Connections on behalf of the logic test will go to that tenant.
	useTenant bool
	// isCCLConfig should be true for any config that can only be run with a CCL
	// binary.
	isCCLConfig bool
	// localities is set if nodes should be set to a particular locality.
	// Nodes are 1-indexed.
	localities map[int]roachpb.Locality
	// backupRestoreProbability will periodically backup the cluster and restore
	// it's state to a new cluster at random points during a logic test.
	backupRestoreProbability float64
}

const threeNodeTenantConfigName = "3node-tenant"

var multiregion9node3region3azsLocalities = map[int]roachpb.Locality{
	1: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "ap-southeast-2"},
			{Key: "availability-zone", Value: "ap-az1"},
		},
	},
	2: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "ap-southeast-2"},
			{Key: "availability-zone", Value: "ap-az2"},
		},
	},
	3: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "ap-southeast-2"},
			{Key: "availability-zone", Value: "ap-az3"},
		},
	},
	4: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "ca-central-1"},
			{Key: "availability-zone", Value: "ca-az1"},
		},
	},
	5: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "ca-central-1"},
			{Key: "availability-zone", Value: "ca-az2"},
		},
	},
	6: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "ca-central-1"},
			{Key: "availability-zone", Value: "ca-az3"},
		},
	},
	7: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east-1"},
			{Key: "availability-zone", Value: "us-az1"},
		},
	},
	8: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east-1"},
			{Key: "availability-zone", Value: "us-az2"},
		},
	},
	9: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east-1"},
			{Key: "availability-zone", Value: "us-az3"},
		},
	},
}

// logicTestConfigs contains all possible cluster configs. A test file can
// specify a list of configs they run on in a file-level comment like:
//   # LogicTest: default distsql
// The test is run once on each configuration (in different subtests).
// If no configs are indicated, the default one is used (unless overridden
// via -config).
var logicTestConfigs = []testClusterConfig{
	{
		name:                "local",
		numNodes:            1,
		overrideDistSQLMode: "off",
		overrideAutoStats:   "false",
	},
	{
		name:                "local-vec-off",
		numNodes:            1,
		overrideDistSQLMode: "off",
		overrideAutoStats:   "false",
		overrideVectorize:   "off",
	},
	{
		name:                "local-v1.1@v1.0-noupgrade",
		numNodes:            1,
		overrideDistSQLMode: "off",
		overrideAutoStats:   "false",
		bootstrapVersion:    roachpb.Version{Major: 1},
		binaryVersion:       roachpb.Version{Major: 1, Minor: 1},
		disableUpgrade:      true,
	},
	{
		name:                                "local-spec-planning",
		numNodes:                            1,
		overrideDistSQLMode:                 "off",
		overrideAutoStats:                   "false",
		overrideExperimentalDistSQLPlanning: "on",
	},
	{
		name:                "fakedist",
		numNodes:            3,
		useFakeSpanResolver: true,
		overrideDistSQLMode: "on",
		overrideAutoStats:   "false",
	},
	{
		name:                "fakedist-vec-off",
		numNodes:            3,
		useFakeSpanResolver: true,
		overrideDistSQLMode: "on",
		overrideAutoStats:   "false",
		overrideVectorize:   "off",
	},
	{
		name:                       "fakedist-metadata",
		numNodes:                   3,
		useFakeSpanResolver:        true,
		overrideDistSQLMode:        "on",
		overrideAutoStats:          "false",
		distSQLMetadataTestEnabled: true,
		skipShort:                  true,
	},
	{
		name:                "fakedist-disk",
		numNodes:            3,
		useFakeSpanResolver: true,
		overrideDistSQLMode: "on",
		overrideAutoStats:   "false",
		sqlExecUseDisk:      true,
		skipShort:           true,
	},
	{
		name:                                "fakedist-spec-planning",
		numNodes:                            3,
		useFakeSpanResolver:                 true,
		overrideDistSQLMode:                 "on",
		overrideAutoStats:                   "false",
		overrideExperimentalDistSQLPlanning: "on",
	},
	{
		name:                "5node",
		numNodes:            5,
		overrideDistSQLMode: "on",
		overrideAutoStats:   "false",
	},
	{
		name:                       "5node-metadata",
		numNodes:                   5,
		overrideDistSQLMode:        "on",
		overrideAutoStats:          "false",
		distSQLMetadataTestEnabled: true,
		skipShort:                  true,
	},
	{
		name:                "5node-disk",
		numNodes:            5,
		overrideDistSQLMode: "on",
		overrideAutoStats:   "false",
		sqlExecUseDisk:      true,
		skipShort:           true,
	},
	{
		name:                                "5node-spec-planning",
		numNodes:                            5,
		overrideDistSQLMode:                 "on",
		overrideAutoStats:                   "false",
		overrideExperimentalDistSQLPlanning: "on",
	},
	{
		// 3node-tenant is a config that runs the test as a SQL tenant. This config
		// can only be run with a CCL binary, so is a noop if run through the normal
		// logictest command.
		// To run a logic test with this config as a directive, run:
		// make test PKG=./pkg/ccl/logictestccl TESTS=TestTenantLogic//<test_name>
		name:     threeNodeTenantConfigName,
		numNodes: 3,
		// overrideAutoStats will disable automatic stats on the cluster this tenant
		// is connected to.
		overrideAutoStats: "false",
		useTenant:         true,
		isCCLConfig:       true,
	},
	// Regions and zones below are named deliberately, and contain "-"'s to be reflective
	// of the naming convention in public clouds.  "-"'s are handled differently in SQL
	// (they're double double quoted) so we explicitly test them here to ensure that
	// the multi-region code handles them correctly.

	{
		name:              "multiregion-invalid-locality",
		numNodes:          3,
		overrideAutoStats: "false",
		localities: map[int]roachpb.Locality{
			1: {
				Tiers: []roachpb.Tier{
					{Key: "invalid-region-setup", Value: "test1"},
					{Key: "availability-zone", Value: "test1-az1"},
				},
			},
			2: {
				Tiers: []roachpb.Tier{},
			},
			3: {
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "test1"},
					{Key: "availability-zone", Value: "test1-az3"},
				},
			},
		},
	},
	{
		name:              "multiregion-3node-3superlongregions",
		numNodes:          3,
		overrideAutoStats: "false",
		localities: map[int]roachpb.Locality{
			1: {
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "veryveryveryveryveryveryverylongregion1"},
				},
			},
			2: {
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "veryveryveryveryveryveryverylongregion2"},
				},
			},
			3: {
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "veryveryveryveryveryveryverylongregion3"},
				},
			},
		},
	},
	{
		name:              "multiregion-9node-3region-3azs",
		numNodes:          9,
		overrideAutoStats: "false",
		localities:        multiregion9node3region3azsLocalities,
	},
	{
		name:              "multiregion-9node-3region-3azs-tenant",
		numNodes:          9,
		overrideAutoStats: "false",
		localities:        multiregion9node3region3azsLocalities,
		useTenant:         true,
	},
	{
		name:              "multiregion-9node-3region-3azs-vec-off",
		numNodes:          9,
		overrideAutoStats: "false",
		localities:        multiregion9node3region3azsLocalities,
		overrideVectorize: "off",
	},
	{
		name:                "local-mixed-21.2-22.1",
		numNodes:            1,
		overrideDistSQLMode: "off",
		overrideAutoStats:   "false",
		bootstrapVersion:    roachpb.Version{Major: 21, Minor: 2},
		binaryVersion:       roachpb.Version{Major: 22, Minor: 1},
		disableUpgrade:      true,
	},
	{
		// 3node-backup is a config that periodically performs a cluster backup,
		// and restores that backup into a new cluster before continuing the test.
		// This config can only be run with a CCL binary, so is a noop if run
		// through the normal logictest command.
		// To run a logic test with this config as a directive, run:
		//  make test PKG=./pkg/ccl/logictestccl TESTS=TestBackupRestoreLogic//<test_name>
		name:                     "3node-backup",
		numNodes:                 3,
		backupRestoreProbability: envutil.EnvOrDefaultFloat64("COCKROACH_LOGIC_TEST_BACKUP_RESTORE_PROBABILITY", 0.0),
		isCCLConfig:              true,
	},
}

// An index in the above slice.
type logicTestConfigIdx int

// A collection of configurations.
type configSet []logicTestConfigIdx

var logicTestConfigIdxToName = make(map[logicTestConfigIdx]string)

func init() {
	for i, cfg := range logicTestConfigs {
		logicTestConfigIdxToName[logicTestConfigIdx(i)] = cfg.name
	}
}

func parseTestConfig(names []string) configSet {
	ret := make(configSet, len(names))
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
	// defaultConfigName is a special alias for the default configs.
	defaultConfigName  = "default-configs"
	defaultConfigNames = []string{
		"local",
		"local-vec-off",
		"local-spec-planning",
		"fakedist",
		"fakedist-vec-off",
		"fakedist-metadata",
		"fakedist-disk",
		"fakedist-spec-planning",
	}
	// fiveNodeDefaultConfigName is a special alias for all 5 node configs.
	fiveNodeDefaultConfigName  = "5node-default-configs"
	fiveNodeDefaultConfigNames = []string{
		"5node",
		"5node-metadata",
		"5node-disk",
		"5node-spec-planning",
	}
	defaultConfig         = parseTestConfig(defaultConfigNames)
	fiveNodeDefaultConfig = parseTestConfig(fiveNodeDefaultConfigNames)
)

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
	line       int
	skip       bool
	skipReason string
}

func (l *lineScanner) SetSkip(reason string) {
	l.skip = true
	l.skipReason = reason
}

func (l *lineScanner) LogAndResetSkip(t *logicTest) {
	if l.skipReason != "" {
		t.t().Logf("statement/query skipped with reason: %s", l.skipReason)
	}
	l.skipReason = ""
	l.skip = false
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

	// roundFloatsInStrings can be set to use a regular expression to find floats
	// that may be embedded in strings and replace them with rounded versions.
	roundFloatsInStrings bool
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
	cfg      testClusterConfig
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
	// cluster is the test cluster against which we are testing. This cluster
	// may be reset during the lifetime of the test.
	cluster serverutils.TestClusterInterface
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
	// map of built clients. Needs to be persisted so that we can
	// re-use them and close them all on exit.
	clients map[string]*gosql.DB
	// client currently in use. This can change during processing
	// of a test input file when encountering the "user" directive.
	// see setUser() for details.
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

// setUser sets the DB client to the specified user.
// It returns a cleanup function to be run when the credentials
// are no longer needed.
func (t *logicTest) setUser(user string, nodeIdxOverride int) func() {
	if t.clients == nil {
		t.clients = map[string]*gosql.DB{}
	}
	if db, ok := t.clients[user]; ok {
		t.db = db
		t.user = user

		// No cleanup necessary, but return a no-op func to avoid nil pointer dereference.
		return func() {}
	}

	nodeIdx := t.nodeIdx
	if nodeIdxOverride > 0 {
		nodeIdx = nodeIdxOverride
	}

	addr := t.cluster.Server(nodeIdx).ServingSQLAddr()
	if len(t.tenantAddrs) > 0 && !strings.HasPrefix(user, "host-cluster-") {
		addr = t.tenantAddrs[nodeIdx]
	}
	pgUser := strings.TrimPrefix(user, "host-cluster-")
	pgURL, cleanupFunc := sqlutils.PGUrl(t.rootT, addr, "TestLogic", url.User(pgUser))
	pgURL.Path = "test"
	db := t.openDB(pgURL)

	// The default value for extra_float_digits assumed by tests is
	// 0. However, lib/pq by default configures this to 2 during
	// connection initialization, so we need to set it back to 0 before
	// we run anything.
	if _, err := db.Exec("SET extra_float_digits = 0"); err != nil {
		t.Fatal(err)
	}
	// The default setting for index_recommendations_enabled is true. We do not
	// want to display index recommendations in logic tests, so we disable them
	// here.
	if _, err := db.Exec("SET index_recommendations_enabled = false"); err != nil {
		t.Fatal(err)
	}
	t.clients[user] = db
	t.db = db
	t.user = pgUser

	return cleanupFunc
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
	})

	return gosql.OpenDB(connector)
}

// newCluster creates a new cluster. It should be called after the logic tests's
// server args are configured. That is, either during setup() when creating the
// initial cluster to be used in a test, or when creating additional test
// clusters, after logicTest.setup() has been called.
func (t *logicTest) newCluster(serverArgs TestServerArgs, opts []clusterOpt) {
	// TODO(andrei): if createTestServerParams() is used here, the command filter
	// it installs detects a transaction that doesn't have
	// modifiedSystemConfigSpan set even though it should, for
	// "testdata/rename_table". Figure out what's up with that.
	if serverArgs.maxSQLMemoryLimit == 0 {
		// Specify a fixed memory limit (some test cases verify OOM conditions;
		// we don't want those to take long on large machines).
		serverArgs.maxSQLMemoryLimit = 192 * 1024 * 1024
	}
	var tempStorageConfig base.TempStorageConfig
	if serverArgs.tempStorageDiskLimit == 0 {
		tempStorageConfig = base.DefaultTestTempStorageConfig(cluster.MakeTestingClusterSettings())
	} else {
		tempStorageConfig = base.DefaultTestTempStorageConfigWithSize(cluster.MakeTestingClusterSettings(), serverArgs.tempStorageDiskLimit)
	}

	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			SQLMemoryPoolSize: serverArgs.maxSQLMemoryLimit,
			TempStorageConfig: tempStorageConfig,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					// The consistency queue makes a lot of noisy logs during logic tests.
					DisableConsistencyQueue: true,
				},
				SQLEvalContext: &tree.EvalContextTestingKnobs{
					AssertBinaryExprReturnTypes:     true,
					AssertUnaryExprReturnTypes:      true,
					AssertFuncExprReturnTypes:       true,
					DisableOptimizerRuleProbability: *disableOptRuleProbability,
					OptimizerCostPerturbation:       *optimizerCostPerturbation,
					ForceProductionBatchSizes:       serverArgs.forceProductionBatchSizes,
				},
				SQLExecutor: &sql.ExecutorTestingKnobs{
					DeterministicExplain: true,
				},
				SQLStatsKnobs: &sqlstats.TestingKnobs{
					AOSTClause: "AS OF SYSTEM TIME '-1us'",
				},
			},
			ClusterName:   "testclustername",
			ExternalIODir: t.sharedIODir,
		},
		// For distributed SQL tests, we use the fake span resolver; it doesn't
		// matter where the data really is.
		ReplicationMode: base.ReplicationManual,
	}

	cfg := t.cfg
	distSQLKnobs := &execinfra.TestingKnobs{
		MetadataTestLevel: execinfra.Off,
	}
	if cfg.sqlExecUseDisk {
		distSQLKnobs.ForceDiskSpill = true
	}
	if cfg.distSQLMetadataTestEnabled {
		distSQLKnobs.MetadataTestLevel = execinfra.On
	}
	params.ServerArgs.Knobs.DistSQL = distSQLKnobs
	if cfg.bootstrapVersion != (roachpb.Version{}) {
		if params.ServerArgs.Knobs.Server == nil {
			params.ServerArgs.Knobs.Server = &server.TestingKnobs{}
		}
		params.ServerArgs.Knobs.Server.(*server.TestingKnobs).BinaryVersionOverride = cfg.bootstrapVersion
	}
	if cfg.disableUpgrade {
		if params.ServerArgs.Knobs.Server == nil {
			params.ServerArgs.Knobs.Server = &server.TestingKnobs{}
		}
		params.ServerArgs.Knobs.Server.(*server.TestingKnobs).DisableAutomaticVersionUpgrade = make(chan struct{})
	}
	for _, opt := range opts {
		opt.apply(&params.ServerArgs)
	}

	paramsPerNode := map[int]base.TestServerArgs{}
	require.Truef(
		t.rootT,
		len(cfg.localities) == 0 || len(cfg.localities) == cfg.numNodes,
		"localities must be set for each node -- got %#v for %d nodes",
		cfg.localities,
		cfg.numNodes,
	)
	for i := 0; i < cfg.numNodes; i++ {
		nodeParams := params.ServerArgs
		if locality, ok := cfg.localities[i+1]; ok {
			nodeParams.Locality = locality
		} else {
			require.Lenf(t.rootT, cfg.localities, 0, "node %d does not have a locality set", i+1)
		}

		if cfg.binaryVersion != (roachpb.Version{}) {
			binaryMinSupportedVersion := cfg.binaryVersion
			if cfg.bootstrapVersion != (roachpb.Version{}) {
				// If we want to run a specific server version, we assume that it
				// supports at least the bootstrap version.
				binaryMinSupportedVersion = cfg.bootstrapVersion
			}
			nodeParams.Settings = cluster.MakeTestingClusterSettingsWithVersions(
				cfg.binaryVersion,
				binaryMinSupportedVersion,
				false, /* initializeVersion */
			)

			// If we're injecting fake versions, hook up logic to simulate the end
			// version existing.
			from := clusterversion.ClusterVersion{Version: cfg.bootstrapVersion}
			to := clusterversion.ClusterVersion{Version: cfg.binaryVersion}
			if len(clusterversion.ListBetween(from, to)) == 0 {
				mm, ok := nodeParams.Knobs.MigrationManager.(*migration.TestingKnobs)
				if !ok {
					mm = &migration.TestingKnobs{}
					nodeParams.Knobs.MigrationManager = mm
				}
				mm.ListBetweenOverride = func(
					from, to clusterversion.ClusterVersion,
				) []clusterversion.ClusterVersion {
					return []clusterversion.ClusterVersion{to}
				}
			}
		}
		paramsPerNode[i] = nodeParams
	}
	params.ServerArgsPerNode = paramsPerNode

	// Update the defaults for automatic statistics to avoid delays in testing.
	// Avoid making the DefaultAsOfTime too small to avoid interacting with
	// schema changes and causing transaction retries.
	// TODO(radu): replace these with testing knobs.
	stats.DefaultAsOfTime = 10 * time.Millisecond
	stats.DefaultRefreshInterval = time.Millisecond

	t.cluster = serverutils.StartNewTestCluster(t.rootT, cfg.numNodes, params)
	if cfg.useFakeSpanResolver {
		fakeResolver := physicalplanutils.FakeResolverForTestCluster(t.cluster)
		t.cluster.Server(t.nodeIdx).SetDistSQLSpanResolver(fakeResolver)
	}

	connsForClusterSettingChanges := []*gosql.DB{t.cluster.ServerConn(0)}
	if cfg.useTenant {
		t.tenantAddrs = make([]string, cfg.numNodes)
		for i := 0; i < cfg.numNodes; i++ {
			tenantArgs := base.TestTenantArgs{
				TenantID:                    serverutils.TestTenantID(),
				AllowSettingClusterSettings: true,
				TestingKnobs: base.TestingKnobs{
					SQLExecutor: &sql.ExecutorTestingKnobs{
						DeterministicExplain: true,
					},
					SQLStatsKnobs: &sqlstats.TestingKnobs{
						AOSTClause: "AS OF SYSTEM TIME '-1us'",
					},
				},
				MemoryPoolSize:    params.ServerArgs.SQLMemoryPoolSize,
				TempStorageConfig: &params.ServerArgs.TempStorageConfig,
				Locality:          paramsPerNode[i].Locality,
				Existing:          i > 0,
				TracingDefault:    params.ServerArgs.TracingDefault,
			}

			tenant, err := t.cluster.Server(i).StartTenant(context.Background(), tenantArgs)
			if err != nil {
				t.rootT.Fatalf("%+v", err)
			}
			t.tenantAddrs[i] = tenant.SQLAddr()
		}

		// Open a connection to a tenant to set any cluster settings specified
		// by the test config.
		pgURL, cleanup := sqlutils.PGUrl(t.rootT, t.tenantAddrs[0], "Tenant", url.User(security.RootUser))
		defer cleanup()
		if params.ServerArgs.Insecure {
			pgURL.RawQuery = "sslmode=disable"
		}
		db, err := gosql.Open("postgres", pgURL.String())
		if err != nil {
			t.rootT.Fatal(err)
		}
		defer db.Close()
		connsForClusterSettingChanges = append(connsForClusterSettingChanges, db)

		// Increase tenant rate limits for faster tests.
		conn := t.cluster.ServerConn(0)
		if _, err := conn.Exec("SET CLUSTER SETTING kv.tenant_rate_limiter.rate_limit = 100000"); err != nil {
			t.Fatal(err)
		}
	}

	// Set cluster settings.
	for _, conn := range connsForClusterSettingChanges {
		if _, err := conn.Exec(
			"SET CLUSTER SETTING sql.stats.automatic_collection.min_stale_rows = $1::int", 5,
		); err != nil {
			t.Fatal(err)
		}

		if cfg.overrideDistSQLMode != "" {
			if _, err := conn.Exec(
				"SET CLUSTER SETTING sql.defaults.distsql = $1::string", cfg.overrideDistSQLMode,
			); err != nil {
				t.Fatal(err)
			}
		}

		if cfg.overrideVectorize != "" {
			if _, err := conn.Exec(
				"SET CLUSTER SETTING sql.defaults.vectorize = $1::string", cfg.overrideVectorize,
			); err != nil {
				t.Fatal(err)
			}
		}

		if cfg.overrideAutoStats != "" {
			if _, err := conn.Exec(
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
			if _, err := conn.Exec(
				"SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false",
			); err != nil {
				t.Fatal(err)
			}
		}

		if cfg.overrideExperimentalDistSQLPlanning != "" {
			if _, err := conn.Exec(
				"SET CLUSTER SETTING sql.defaults.experimental_distsql_planning = $1::string", cfg.overrideExperimentalDistSQLPlanning,
			); err != nil {
				t.Fatal(err)
			}
		}

		// Update the default AS OF time for querying the system.table_statistics
		// table to create the crdb_internal.table_row_statistics table.
		if _, err := conn.Exec(
			"SET CLUSTER SETTING sql.crdb_internal.table_row_statistics.as_of_time = '-1µs'",
		); err != nil {
			t.Fatal(err)
		}
	}

	if cfg.overrideDistSQLMode != "" {
		_, ok := sessiondatapb.DistSQLExecModeFromString(cfg.overrideDistSQLMode)
		if !ok {
			t.Fatalf("invalid distsql mode override: %s", cfg.overrideDistSQLMode)
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
				if m != cfg.overrideDistSQLMode {
					return errors.Errorf("node %d is still waiting for update of DistSQLMode to %s (have %s)",
						i, cfg.overrideDistSQLMode, m,
					)
				}
			}
			return nil
		})
	}

	// db may change over the lifetime of this function, with intermediate
	// values cached in t.clients and finally closed in t.close().
	t.clusterCleanupFuncs = append(t.clusterCleanupFuncs, t.setUser(security.RootUser, 0 /* nodeIdxOverride */))
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
		for _, c := range t.clients {
			c.Close()
		}
		t.clients = nil
	}
	t.db = nil
}

// resetCluster cleans up the current cluster, and creates a fresh one.
func (t *logicTest) resetCluster() {
	t.shutdownCluster()
	if t.serverArgs == nil {
		// We expect the server args to be persisted to the test during test
		// setup.
		t.Fatal("resetting the cluster before server args were set")
	}
	serverArgs := *t.serverArgs
	t.newCluster(serverArgs, t.clusterOpts)
}

// setup creates the initial cluster for the logic test and populates the
// relevant fields on logicTest. It is expected to be called only once (per test
// file), and before processing any test files - unless a mock logicTest is
// created (see parallelTest.processTestFile).
func (t *logicTest) setup(cfg testClusterConfig, serverArgs TestServerArgs, opts []clusterOpt) {
	t.cfg = cfg
	t.serverArgs = &serverArgs
	t.clusterOpts = opts[:]
	// TODO(pmattis): Add a flag to make it easy to run the tests against a local
	// MySQL or Postgres instance.
	tempExternalIODir, tempExternalIODirCleanup := testutils.TempDir(t.rootT)
	t.sharedIODir = tempExternalIODir
	t.testCleanupFuncs = append(t.testCleanupFuncs, tempExternalIODirCleanup)

	t.newCluster(serverArgs, t.clusterOpts)

	// Only create the test database on the initial cluster, since cluster restore
	// expects an empty cluster.
	if _, err := t.db.Exec(`
CREATE DATABASE test; USE test;
`); err != nil {
		t.Fatal(err)
	}

	if _, err := t.db.Exec(fmt.Sprintf("CREATE USER %s;", security.TestUser)); err != nil {
		t.Fatal(err)
	}

	t.labelMap = make(map[string]string)
	t.varMap = make(map[string]string)

	t.progress = 0
	t.failures = 0
	t.unsupported = 0
}

// applyBlocklistToConfigs applies the given blocklist to configs, returning the
// result.
func applyBlocklistToConfigs(configs configSet, blocklist map[string]int) configSet {
	if len(blocklist) == 0 {
		return configs
	}
	var newConfigs configSet
	for _, idx := range configs {
		if _, ok := blocklist[logicTestConfigIdxToName[idx]]; ok {
			continue
		}
		newConfigs = append(newConfigs, idx)
	}
	return newConfigs
}

// getBlocklistIssueNo takes a blocklist directive with an optional issue number
// and returns the stripped blocklist name with the corresponding issue number
// as an integer.
// e.g. an input of "3node-tenant(123456)" would return "3node-tenant", 123456
func getBlocklistIssueNo(blocklistDirective string) (string, int) {
	parts := strings.Split(blocklistDirective, "(")
	if len(parts) != 2 {
		return blocklistDirective, 0
	}

	issueNo, err := strconv.Atoi(strings.TrimRight(parts[1], ")"))
	if err != nil {
		panic(fmt.Sprintf("possibly malformed blocklist directive: %s: %v", blocklistDirective, err))
	}
	return parts[0], issueNo
}

// processConfigs, given a list of configNames, returns the list of
// corresponding logicTestConfigIdxs as well as a boolean indicating whether
// the test works only in non-metamorphic setting.
func processConfigs(
	t *testing.T, path string, defaults configSet, configNames []string,
) (_ configSet, onlyNonMetamorphic bool) {
	const blocklistChar = '!'
	// blocklist is a map from a blocked config to a corresponding issue number.
	// If 0, there is no associated issue.
	blocklist := make(map[string]int)
	allConfigNamesAreBlocklistDirectives := true
	for _, configName := range configNames {
		if configName[0] != blocklistChar {
			allConfigNamesAreBlocklistDirectives = false
			continue
		}

		blockedConfig, issueNo := getBlocklistIssueNo(configName[1:])
		if *printBlocklistIssues && issueNo != 0 {
			t.Logf("will skip %s config in test %s due to issue: %s", blockedConfig, path, build.MakeIssueURL(issueNo))
		}
		blocklist[blockedConfig] = issueNo
	}

	if _, ok := blocklist["metamorphic"]; ok && util.IsMetamorphicBuild() {
		onlyNonMetamorphic = true
	}
	if len(blocklist) != 0 && allConfigNamesAreBlocklistDirectives {
		// No configs specified, this blocklist applies to the default configs.
		return applyBlocklistToConfigs(defaults, blocklist), onlyNonMetamorphic
	}

	var configs configSet
	for _, configName := range configNames {
		if configName[0] == blocklistChar {
			continue
		}
		if _, ok := blocklist[configName]; ok {
			continue
		}

		idx, ok := findLogicTestConfig(configName)
		if !ok {
			switch configName {
			case defaultConfigName:
				configs = append(configs, applyBlocklistToConfigs(defaults, blocklist)...)
			case fiveNodeDefaultConfigName:
				configs = append(configs, applyBlocklistToConfigs(fiveNodeDefaultConfig, blocklist)...)
			default:
				t.Fatalf("%s: unknown config name %s", path, configName)
			}
		} else {
			configs = append(configs, idx)
		}
	}

	return configs, onlyNonMetamorphic
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
func readTestFileConfigs(
	t *testing.T, path string, defaults configSet,
) (_ configSet, onlyNonMetamorphic bool) {
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
			return processConfigs(t, path, defaults, fields[2:])
		}
	}
	// No directive found, return the default config.
	return defaults, false
}

// clusterOpt is implemented by options for configuring the test cluster under
// which a test will run.
type clusterOpt interface {
	apply(args *base.TestServerArgs)
}

// clusterOptDisableSpanConfigs corresponds to the disable-span-configs
// directive.
type clusterOptDisableSpanConfigs struct{}

var _ clusterOpt = clusterOptDisableSpanConfigs{}

// apply implements the clusterOpt interface.
func (c clusterOptDisableSpanConfigs) apply(args *base.TestServerArgs) {
	args.DisableSpanConfigs = true
}

// clusterOptTracingOff corresponds to the tracing-off directive.
type clusterOptTracingOff struct{}

var _ clusterOpt = clusterOptTracingOff{}

// apply implements the clusterOpt interface.
func (c clusterOptTracingOff) apply(args *base.TestServerArgs) {
	args.TracingDefault = tracing.TracingModeOnDemand
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

// readClusterOptions looks around the beginning of the file for a line looking like:
// # cluster-opt: opt1 opt2 ...
// and parses that line into a set of clusterOpts that need to be applied to the
// TestServerArgs before the cluster is started for the respective test file.
func readClusterOptions(t *testing.T, path string) []clusterOpt {
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	var res []clusterOpt

	beginningOfFile := true
	directiveFound := false

	s := newLineScanner(file)
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
		if len(fields) > 1 && cmd == "#" && fields[1] == "cluster-opt:" {
			require.True(t, beginningOfFile, "cluster-opt directive needs to be at the beginning of file")
			require.False(t, directiveFound, "only one cluster-opt directive allowed per file; second one found: %s", s.Text())
			directiveFound = true
			if len(fields) == 2 {
				t.Fatalf("%s: empty LogicTest directive", path)
			}
			for _, opt := range fields[2:] {
				switch opt {
				case "disable-span-configs":
					res = append(res, clusterOptDisableSpanConfigs{})
				case "tracing-off":
					res = append(res, clusterOptTracingOff{})
				case "ignore-tenant-strict-gc-enforcement":
					res = append(res, clusterOptIgnoreStrictGCForTenants{})
				default:
					t.Fatalf("unrecognized cluster option: %s", opt)
				}
			}
		}
	}
	return res
}

type subtestDetails struct {
	name                  string        // the subtest's name, empty if not a subtest
	buffer                *bytes.Buffer // a chunk of the test file representing the subtest
	lineLineIndexIntoFile int           // the line number of the test file where the subtest started
}

func (t *logicTest) processTestFile(path string, config testClusterConfig) error {
	rng, seed := randutil.NewPseudoRand()
	t.outf("rng seed: %d\n", seed)

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
			if err := t.processSubtest(subtest, path, config, rng); err != nil {
				return err
			}
		} else {
			t.emit(fmt.Sprintf("subtest %s", subtest.name))
			t.rootT.Run(subtest.name, func(subtestT *testing.T) {
				t.subtestT = subtestT
				defer func() {
					t.subtestT = nil
				}()
				if err := t.processSubtest(subtest, path, config, rng); err != nil {
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
	for _, user := range t.clients {
		existingTxnPriority := "NORMAL"
		err := user.QueryRow("SHOW TRANSACTION PRIORITY").Scan(&existingTxnPriority)
		if err != nil {
			// Ignore an error if we are unable to see transaction priority.
			log.Warningf(ctx, "failed to check txn priority with %v", err)
			continue
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
	return false
}

// maybeBackupRestore will randomly issue a cluster backup, create a new
// cluster, and restore that backup to the cluster before continuing the test.
// The probability of executing a backup and restore is specified in the
// testClusterConfig.
func (t *logicTest) maybeBackupRestore(rng *rand.Rand, config testClusterConfig) error {
	if config.backupRestoreProbability != 0 && !config.isCCLConfig {
		return errors.Newf("logic test config %s specifies a backup restore probability but is not CCL",
			config.name)
	}

	// We decide if we want to take a backup here based on a probability
	// specified in the logic test config.
	if rng.Float64() > config.backupRestoreProbability {
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
	defer func() {
		t.setUser(oldUser, 0 /* nodeIdxOverride */)
	}()

	// To restore the same state in for the logic test, we need to restore the
	// data and the session state. The session state includes things like session
	// variables that are set for every session that is open.
	//
	// TODO(adityamaru): A better approach might be to wipe the cluster once we
	// have a command that enables this. That way all of the session data will not
	// be lost in the process of creating a new cluster.
	users := make([]string, 0, len(t.clients))
	userToHexSession := make(map[string]string, len(t.clients))
	userToSessionVars := make(map[string]map[string]string, len(t.clients))
	for user := range t.clients {
		t.setUser(user, 0 /* nodeIdxOverride */)
		users = append(users, user)

		// Serialize session variables.
		var userSession string
		var err error
		if err = t.db.QueryRow(`SELECT encode(crdb_internal.serialize_session(), 'hex')`).Scan(&userSession); err == nil {
			userToHexSession[user] = userSession
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
		userToSessionVars[user] = userSessionVars
	}

	backupLocation := fmt.Sprintf("nodelocal://1/logic-test-backup-%s",
		strconv.FormatInt(timeutil.Now().UnixNano(), 10))

	// Perform the backup and restore as root.
	t.setUser(security.RootUser, 0 /* nodeIdxOverride */)

	if _, err := t.db.Exec(fmt.Sprintf("BACKUP TO '%s'", backupLocation)); err != nil {
		return errors.Wrap(err, "backing up cluster")
	}

	// Create a new cluster. Perhaps this can be updated to just wipe the exiting
	// cluster once we have the ability to easily wipe a cluster through SQL.
	t.resetCluster()

	// Run the restore as root.
	t.setUser(security.RootUser, 0 /* nodeIdxOverride */)
	if _, err := t.db.Exec(fmt.Sprintf("RESTORE FROM '%s'", backupLocation)); err != nil {
		return errors.Wrap(err, "restoring cluster")
	}

	// Restore the session state that was in the old cluster.

	// Create new connections for the existing users, and restore the session
	// variables that we collected.
	for _, user := range users {
		// Call setUser for every user to create the connection for that user.
		t.setUser(user, 0 /* nodeIdxOverride */)

		if userSession, ok := userToHexSession[user]; ok {
			if _, err := t.db.Exec(fmt.Sprintf(`SELECT crdb_internal.deserialize_session(decode('%s', 'hex'))`, userSession)); err != nil {
				return errors.Wrapf(err, "deserializing session")
			}
		} else if vars, ok := userToSessionVars[user]; ok {
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
	subtest subtestDetails, path string, config testClusterConfig, rng *rand.Rand,
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
		if err := t.maybeBackupRestore(rng, config); err != nil {
			return err
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
					path, s.line+subtest.lineLineIndexIntoFile,
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
					path, s.line+subtest.lineLineIndexIntoFile,
				)
			}
			time.Sleep(duration)

		case "statement":
			stmt := logicStatement{
				pos:         fmt.Sprintf("\n%s:%d", path, s.line+subtest.lineLineIndexIntoFile),
				expectCount: -1,
			}
			// Parse "statement (notice|error) <regexp>"
			if m := noticeRE.FindStringSubmatch(s.Text()); m != nil {
				stmt.expectNotice = m[1]
			} else if m := errorRE.FindStringSubmatch(s.Text()); m != nil {
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
				s.LogAndResetSkip(t)
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

						case "rowsort":
							query.sorter = rowSort

						case "valuesort":
							query.sorter = valueSort

						case "colnames":
							query.colNames = true

						case "retry":
							query.retry = true

						case "kvtrace":
							// kvtrace without any arguments doesn't perform any additional
							// filtering of results. So it displays kv's from all tables
							// and all operation types.
							query.kvtrace = true
							query.kvOpTypes = nil
							query.keyPrefixFilters = nil

						case "noticetrace":
							query.noticetrace = true

						case "round-in-strings":
							query.roundFloatsInStrings = true

						default:
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
				}
			}

			if query.noticetrace && query.kvtrace {
				return errors.Errorf(
					"%s: cannot have both noticetrace and kvtrace on at the same time",
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
					if query.retry && !*rewriteResultsInTestfiles {
						testutils.SucceedsSoon(t.rootT, func() error {
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
				if *rewriteResultsInTestfiles {
					for _, l := range query.expectedResultsRaw {
						t.emit(l)
					}
				}
				s.LogAndResetSkip(t)
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
			cleanupUserFunc := t.setUser(fields[1], nodeIdx)
			defer cleanupUserFunc()

		case "skip":
			reason := "skipped"
			if len(fields) > 1 {
				reason = fields[1]
			}
			skip.IgnoreLint(t.t(), reason)

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
				if t.cfg.name == configName {
					issue := "no issue given"
					if len(fields) > 3 {
						issue = fields[3]
					}
					s.SetSkip(fmt.Sprintf("unsupported configuration %s (%s)", configName, issue))
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
				if t.cfg.name != configName {
					issue := "no issue given"
					if len(fields) > 3 {
						issue = fields[3]
					}
					s.SetSkip(fmt.Sprintf("unsupported configuration %s, statement/query only supports %s (%s)", t.cfg.name, configName, issue))
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

		default:
			return errors.Errorf("%s:%d: unknown command: %s",
				path, s.line+subtest.lineLineIndexIntoFile, cmd,
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
	if expectErr == "" && expectErrCode == "" && err != nil {
		t.maybeSkipOnRetry(err)
		cont := t.unexpectedError(sql, pos, err)
		if cont {
			// unexpectedError() already reported via t.Errorf. no need for more.
			err = nil
		}
		return cont, err
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
			return (err != nil), newErr
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
	res, err := t.db.Exec(execSQL)
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
	if query.nodeIdx != 0 {
		addr := t.cluster.Server(query.nodeIdx).ServingSQLAddr()
		if len(t.tenantAddrs) > 0 {
			addr = t.tenantAddrs[query.nodeIdx]
		}
		pgURL, cleanupFunc := sqlutils.PGUrl(t.rootT, addr, "TestLogic", url.User(t.user))
		defer cleanupFunc()
		pgURL.Path = "test"

		db = t.openDB(pgURL)
		defer func() {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}()
	}
	rows, err := db.Query(query.sql)
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
						// Empty strings are rendered as "·" (middle dot)
						if val == "" {
							val = "·"
						}
						s := fmt.Sprint(val)
						if query.roundFloatsInStrings {
							s = roundFloatsInString(s)
						}
						actualResultsRaw = append(actualResultsRaw, s)
					} else {
						actualResultsRaw = append(actualResultsRaw, "NULL")
					}
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
			resultMatches := expected == actual
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
					resultMatches, err = floatsMatchApprox(expected, actual)
				} else if colT == 'F' {
					resultMatches, err = floatsMatch(expected, actual)
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

// parseExpectedAndActualFloats converts the strings expectedString and
// actualString to float64 values.
func parseExpectedAndActualFloats(expectedString, actualString string) (float64, float64, error) {
	expected, err := strconv.ParseFloat(expectedString, 64 /* bitSize */)
	if err != nil {
		return 0, 0, errors.Wrap(err, "when parsing expected")
	}
	actual, err := strconv.ParseFloat(actualString, 64 /* bitSize */)
	if err != nil {
		return 0, 0, errors.Wrap(err, "when parsing actual")
	}
	return expected, actual, nil
}

// floatsMatchApprox returns whether two floating point represented as
// strings are equal within a tolerance.
func floatsMatchApprox(expectedString, actualString string) (bool, error) {
	expected, actual, err := parseExpectedAndActualFloats(expectedString, actualString)
	if err != nil {
		return false, err
	}
	return floatcmp.EqualApprox(expected, actual, floatcmp.CloseFraction, floatcmp.CloseMargin), nil
}

// floatsMatch returns whether two floating point numbers represented as
// strings have matching 15 significant decimal digits (this is the precision
// that Postgres supports for 'double precision' type).
func floatsMatch(expectedString, actualString string) (bool, error) {
	expected, actual, err := parseExpectedAndActualFloats(expectedString, actualString)
	if err != nil {
		return false, err
	}
	// Check special values - NaN, +Inf, -Inf, 0.
	if math.IsNaN(expected) || math.IsNaN(actual) {
		return math.IsNaN(expected) == math.IsNaN(actual), nil
	}
	if math.IsInf(expected, 0 /* sign */) || math.IsInf(actual, 0 /* sign */) {
		bothNegativeInf := math.IsInf(expected, -1 /* sign */) == math.IsInf(actual, -1 /* sign */)
		bothPositiveInf := math.IsInf(expected, 1 /* sign */) == math.IsInf(actual, 1 /* sign */)
		return bothNegativeInf || bothPositiveInf, nil
	}
	if expected == 0 || actual == 0 {
		return expected == actual, nil
	}
	// Check that the numbers have the same sign.
	if expected*actual < 0 {
		return false, nil
	}
	expected = math.Abs(expected)
	actual = math.Abs(actual)
	// Check that 15 significant digits match. We do so by normalizing the
	// numbers and then checking one digit at a time.
	//
	// normalize converts f to base * 10**power representation where base is in
	// [1.0, 10.0) range.
	normalize := func(f float64) (base float64, power int) {
		for f >= 10 {
			f = f / 10
			power++
		}
		for f < 1 {
			f *= 10
			power--
		}
		return f, power
	}
	var expPower, actPower int
	expected, expPower = normalize(expected)
	actual, actPower = normalize(actual)
	if expPower != actPower {
		return false, nil
	}
	// TODO(yuzefovich): investigate why we can't always guarantee deterministic
	// 15 significant digits and switch back from 14 to 15 digits comparison
	// here. See #56446 for more details.
	for i := 0; i < 14; i++ {
		expDigit := int(expected)
		actDigit := int(actual)
		if expDigit != actDigit {
			return false, nil
		}
		expected -= (expected - float64(expDigit)) * 10
		actual -= (actual - float64(actDigit)) * 10
	}
	return true, nil
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
		t.outf("--- progress: %s: %d statements", file, t.progress)
	}
}

func (t *logicTest) validateAfterTestCompletion() error {
	// Close all clients other than "root"
	for username, c := range t.clients {
		if username == "root" {
			continue
		}
		delete(t.clients, username)
		if err := c.Close(); err != nil {
			t.Fatalf("failed to close connection for user %s: %v", username, err)
		}
	}
	t.setUser("root", 0 /* nodeIdxOverride */)

	// Some cleanup to make sure the following validation queries can run
	// successfully. First we rollback in case the logic test had an uncommitted
	// txn and second we reset vectorize mode in case it was switched to
	// `experimental_always`.
	_, _ = t.db.Exec("ROLLBACK")
	_, err := t.db.Exec("RESET vectorize")
	if err != nil {
		t.Fatal(errors.Wrap(err, "could not reset vectorize mode"))
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
	// maxSQLMemoryLimit determines the value of --max-sql-memory startup
	// argument for the server. If unset, then the default limit of 192MiB will
	// be used.
	maxSQLMemoryLimit int64
	// tempStorageDiskLimit determines the limit for the temp storage (that is
	// actually in-memory). If it is unset, then the default limit of 100MB
	// will be used.
	tempStorageDiskLimit int64
	// If set, mutations.MaxBatchSize and row.getKVBatchSize will be overridden
	// to use the non-test value.
	forceProductionBatchSizes bool
}

// RunLogicTest is the main entry point for the logic test. The globs parameter
// specifies the default sets of files to run.
func RunLogicTest(t *testing.T, serverArgs TestServerArgs, globs ...string) {
	RunLogicTestWithDefaultConfig(t, serverArgs, *overrideConfig, false /* runCCLConfigs */, globs...)
}

// RunLogicTestWithDefaultConfig is the main entry point for the logic test.
// The globs parameter specifies the default sets of files to run. The config
// override parameter, if not empty, specifies the set of configurations to run
// those files in. If empty, the default set of configurations is used.
// runCCLConfigs specifies whether the test runner should skip configs that can
// only be run with a CCL binary.
func RunLogicTestWithDefaultConfig(
	t *testing.T,
	serverArgs TestServerArgs,
	configOverride string,
	runCCLConfigs bool,
	globs ...string,
) {
	// Note: there is special code in teamcity-trigger/main.go to run this package
	// with less concurrency in the nightly stress runs. If you see problems
	// please make adjustments there.
	// As of 6/4/2019, the logic tests never complete under race.
	skip.UnderStressRace(t, "logic tests and race detector don't mix: #37993")

	if skipLogicTests {
		skip.IgnoreLint(t, "COCKROACH_LOGIC_TESTS_SKIP")
	}

	// Override default glob sets if -d flag was specified.
	if *logictestdata != "" {
		globs = []string{*logictestdata}
	}

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
	// nonMetamorphic mirrors configPaths and indicates whether a particular
	// config on a particular path can only run in non-metamorphic setting.
	nonMetamorphic := make([][]bool, len(logicTestConfigs))
	configDefaults := defaultConfig
	var configFilter map[string]struct{}
	if configOverride != "" {
		// If a config override is provided, we use it to replace the default
		// config set. This ensures that the overrides are used for files where:
		// 1. no config directive is present
		// 2. a config directive containing only a blocklist is present
		// 3. a config directive containing "default-configs" is present
		//
		// We also create a filter to restrict configs to only those in the
		// override list.
		names := strings.Split(configOverride, ",")
		configDefaults = parseTestConfig(names)
		configFilter = make(map[string]struct{})
		for _, name := range names {
			configFilter[name] = struct{}{}
		}
	}
	for _, path := range paths {
		configs, onlyNonMetamorphic := readTestFileConfigs(t, path, configDefaults)
		for _, idx := range configs {
			config := logicTestConfigs[idx]
			configName := config.name
			if _, ok := configFilter[configName]; configFilter != nil && !ok {
				// Config filter present but not containing test.
				continue
			}
			if config.isCCLConfig && !runCCLConfigs {
				// Config is a CCL config and the caller specified that CCL configs
				// should not be run.
				continue
			}
			configPaths[idx] = append(configPaths[idx], path)
			nonMetamorphic[idx] = append(nonMetamorphic[idx], onlyNonMetamorphic)
		}
	}

	// The tests below are likely to run concurrently; `log` is shared
	// between all the goroutines and thus all tests, so it doesn't make
	// sense to try to use separate `log.Scope` instances for each test.
	logScope := log.Scope(t)
	defer logScope.Close(t)

	verbose := testing.Verbose() || log.V(1)

	// Only used in rewrite mode, where we don't need to run the same file through
	// multiple configs.
	seenPaths := make(map[string]struct{})
	for idx, cfg := range logicTestConfigs {
		paths := configPaths[idx]
		nonMetamorphic := nonMetamorphic[idx]
		if len(paths) == 0 {
			continue
		}
		// Top-level test: one per test configuration.
		t.Run(cfg.name, func(t *testing.T) {
			if testing.Short() && cfg.skipShort {
				skip.IgnoreLint(t, "config skipped by -test.short")
			}
			if logicTestsConfigExclude != "" && cfg.name == logicTestsConfigExclude {
				skip.IgnoreLint(t, "config excluded via env var")
			}
			if logicTestsConfigFilter != "" && cfg.name != logicTestsConfigFilter {
				skip.IgnoreLint(t, "config does not match env var")
			}
			for i, path := range paths {
				path := path // Rebind range variable.
				onlyNonMetamorphic := nonMetamorphic[i]
				// Inner test: one per file path.
				t.Run(filepath.Base(path), func(t *testing.T) {
					if *rewriteResultsInTestfiles {
						if _, seen := seenPaths[path]; seen {
							skip.IgnoreLint(t, "test file already rewritten")
						}
						seenPaths[path] = struct{}{}
					}

					// Run the test in parallel, unless:
					//  - we're printing out all of the SQL interactions, or
					//  - we're generating testfiles, or
					//  - we are in race mode (where we can hit a limit on alive
					//    goroutines).
					//  - we have too many nodes (this can lead to general slowness)
					if !*showSQL &&
						!*rewriteResultsInTestfiles &&
						!*rewriteSQL &&
						!util.RaceEnabled &&
						!cfg.useTenant &&
						cfg.numNodes <= 5 {
						// Skip parallelizing tests that use the kv-batch-size directive since
						// the batch size is a global variable.
						//
						// We also cannot parallelise tests that use tenant servers
						// because they change shared state in the logging configuration
						// and there is an assertion against conflicting changes.
						//
						// TODO(jordan, radu): make sqlbase.kvBatchSize non-global to fix this.
						if filepath.Base(path) != "select_index_span_ranges" {
							t.Parallel() // SAFE FOR TESTING (this comments satisfies the linter)
						}
					}
					rng, _ := randutil.NewTestRand()
					lt := logicTest{
						rootT:           t,
						verbose:         verbose,
						perErrorSummary: make(map[string][]string),
						rng:             rng,
					}
					if *printErrorSummary {
						defer lt.printErrorSummary()
					}
					// Each test needs a copy because of Parallel
					serverArgsCopy := serverArgs
					serverArgsCopy.forceProductionBatchSizes = onlyNonMetamorphic
					lt.setup(cfg, serverArgsCopy, readClusterOptions(t, path))
					lt.runFile(path, cfg)

					progress.Lock()
					defer progress.Unlock()
					progress.total += lt.progress
					progress.totalFail += lt.failures
					progress.totalUnsupported += lt.unsupported
					now := timeutil.Now()
					if now.Sub(progress.lastProgress) >= 2*time.Second {
						progress.lastProgress = now
						lt.outf("--- total progress: %d statements", progress.total)
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

// RunSQLLiteLogicTest is the main entry point to run the suite of SQLLite logic
// tests. It runs logic tests from CockroachDB's fork of sqllogictest:
//
//   https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
//
// This fork contains many generated tests created by the SqlLite project that
// ensure the tested SQL database returns correct statement and query output.
// The logic tests are reasonably independent of the specific dialect of each
// database so that they can be retargeted. In fact, the expected output for
// each test can be generated by one database and then used to verify the output
// of another database.
//
// The tests are run with the default set of configurations specified in
// configOverride. If empty, the default set of configurations is used.
//
// By default, these tests are skipped, unless the `bigtest` flag is specified.
// The reason for this is that these tests are contained in another repo that
// must be present on the machine, and because they take a long time to run.
//
// See the comments in logic.go for more details.
func RunSQLLiteLogicTest(t *testing.T, configOverride string) {
	runSQLLiteLogicTest(t,
		configOverride,
		"/test/index/between/*/*.test",
		"/test/index/commute/*/*.test",
		"/test/index/delete/*/*.test",
		"/test/index/in/*/*.test",
		"/test/index/orderby/*/*.test",
		"/test/index/orderby_nosort/*/*.test",
		"/test/index/view/*/*.test",

		"/test/select1.test",
		"/test/select2.test",
		"/test/select3.test",
		"/test/select4.test",

		// TODO(andyk): No support for join ordering yet, so this takes too long.
		// "/test/select5.test",

		// TODO(pmattis): Incompatibilities in numeric types.
		// For instance, we type SUM(int) as a decimal since all of our ints are
		// int64.
		// "/test/random/expr/*.test",

		// TODO(pmattis): We don't support unary + on strings.
		// "/test/index/random/*/*.test",
		// "/test/random/aggregates/*.test",
		// "/test/random/groupby/*.test",
		// "/test/random/select/*.test",
	)
}

func runSQLLiteLogicTest(t *testing.T, configOverride string, globs ...string) {
	if !*bigtest {
		skip.IgnoreLint(t, "-bigtest flag must be specified to run this test")
	}

	var logicTestPath string
	if bazel.BuiltWithBazel() {
		runfilesPath, err := bazel.RunfilesPath()
		if err != nil {
			t.Fatal(err)
		}
		logicTestPath = filepath.Join(runfilesPath, "external", "com_github_cockroachdb_sqllogictest")
	} else {
		logicTestPath = gobuild.Default.GOPATH + "/src/github.com/cockroachdb/sqllogictest"
		if _, err := os.Stat(logicTestPath); oserror.IsNotExist(err) {
			fullPath, err := filepath.Abs(logicTestPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Fatalf("unable to find sqllogictest repo: %s\n"+
				"git clone https://github.com/cockroachdb/sqllogictest %s",
				logicTestPath, fullPath)
			return
		}
	}

	// Prefix the globs with the logicTestPath.
	prefixedGlobs := make([]string, len(globs))
	for i, glob := range globs {
		prefixedGlobs[i] = logicTestPath + glob
	}

	// SQLLite logic tests can be very memory and disk intensive, so we give
	// them larger limits than other logic tests get.
	serverArgs := TestServerArgs{
		maxSQLMemoryLimit:    512 << 20, // 512 MiB
		tempStorageDiskLimit: 512 << 20, // 512 MiB
	}
	RunLogicTestWithDefaultConfig(t, serverArgs, configOverride, true /* runCCLConfigs */, prefixedGlobs...)
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
func (t *logicTest) printCompletion(path string, config testClusterConfig) {
	unsupportedMsg := ""
	if t.unsupported > 0 {
		unsupportedMsg = fmt.Sprintf(", ignored %d unsupported queries", t.unsupported)
	}
	t.outf("--- done: %s with config %s: %d tests, %d failures%s", path, config.name,
		t.progress, t.failures, unsupportedMsg)
}

func roundFloatsInString(s string) string {
	return string(regexp.MustCompile(`(\d+\.\d+)`).ReplaceAllFunc([]byte(s), func(x []byte) []byte {
		f, err := strconv.ParseFloat(string(x), 64)
		if err != nil {
			return []byte(err.Error())
		}
		return []byte(fmt.Sprintf("%.6g", f))
	}))
}
