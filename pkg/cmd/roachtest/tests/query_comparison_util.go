// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	b64 "encoding/base64"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/internal/workloadreplay"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/floatcmp"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/google/go-cmp/cmp"
)

const (
	bucketName = "roachtest-snowflake-costfuzz"
	keyTag     = "GOOGLE_EPHEMERAL_CREDENTIALS"
)

type queryComparisonTest struct {
	name          string
	setupName     string
	isMultiRegion bool
	nodeCount     int
	run           func(queryGenerator, *rand.Rand, queryComparisonHelper) error
}

// queryGenerator provides interface for tests to generate queries via method
// Generate which will vary per test.
type queryGenerator interface {
	Generate() string
}

type workloadReplayGenerator struct {
	stmt string
}

// Generate is part of the queryGenerator interface.
func (wlrg *workloadReplayGenerator) Generate() string {
	return wlrg.stmt
}

var _ queryGenerator = (*workloadReplayGenerator)(nil)

func runQueryComparison(
	ctx context.Context, t test.Test, c cluster.Cluster, qct *queryComparisonTest,
) {
	roundTimeout := 10 * time.Minute

	// A cluster context with a slightly longer timeout than the test timeout is
	// used so cluster creation or cleanup commands should never time out and
	// result in "context deadline exceeded" Github issues being created.
	var clusterCancel context.CancelFunc
	var clusterCtx context.Context
	clusterCtx, clusterCancel = context.WithTimeout(ctx, t.Spec().(*registry.TestSpec).Timeout-2*time.Minute)
	defer clusterCancel()

	// Run 10 minute iterations of query comparison in a loop for about the entire
	// test, giving 5 minutes at the end to allow the test to shut down cleanly.
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(clusterCtx, t.Spec().(*registry.TestSpec).Timeout-5*time.Minute)
	defer cancel()
	done := ctx.Done()
	shouldExit := func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}

	for i := 0; ; i++ {
		if shouldExit() {
			return
		}
		c.Start(clusterCtx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())

		runOneRoundQueryComparison(ctx, i, roundTimeout, t, c, qct)
		// If this iteration was interrupted because the timeout of ctx has been
		// reached, we want to cleanly exit from the test, without wiping out the
		// cluster (if we tried that, we'd get an error because ctx is canceled).
		if shouldExit() {
			return
		}
		c.Stop(clusterCtx, t.L(), option.DefaultStopOpts())
		c.Wipe(clusterCtx)
	}
}

// runOneRoundQueryComparison creates a random schema, inserts random data, and
// then executes queries until the roundTimeout is reached.
func runOneRoundQueryComparison(
	ctx context.Context,
	iter int,
	roundTimeout time.Duration,
	t test.Test,
	c cluster.Cluster,
	qct *queryComparisonTest,
) {
	// Set up a statement logger for easy reproduction. We only
	// want to log successful statements and statements that
	// produced a final error or panic.

	logPath := filepath.Join(t.ArtifactsDir(), fmt.Sprintf("%s%03d.log", qct.name, iter))
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("could not create %s%03d.log: %v", qct.name, iter, err)
	}
	defer log.Close()
	logStmt := func(stmt string) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			return
		}
		fmt.Fprint(log, stmt)
		if !strings.HasSuffix(stmt, ";") {
			fmt.Fprint(log, ";")
		}
		// Blank lines are necessary for reduce -costfuzz to function correctly.
		fmt.Fprint(log, "\n\n")
	}
	printStmt := func(stmt string) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			return
		}
		t.L().Printf(stmt)
		if !strings.HasSuffix(stmt, ";") {
			t.L().Printf(";")
		}
		t.L().Printf("\n\n")
	}

	// We will create the failure log file on demand.
	var failureLog *os.File
	defer func() {
		if failureLog != nil {
			_ = failureLog.Close()
		}
	}()
	logFailure := func(stmt string, rows [][]string) {
		if failureLog == nil {
			failureLogName := fmt.Sprintf("%s%03d.failure.log", qct.name, iter)
			failureLogPath := filepath.Join(t.ArtifactsDir(), failureLogName)
			var err error
			failureLog, err = os.Create(failureLogPath)
			if err != nil {
				t.Fatalf("could not create %s: %v", failureLogName, err)
			}
		}
		fmt.Fprint(failureLog, stmt)
		fmt.Fprint(failureLog, "\n----\n")
		fmt.Fprint(failureLog, sqlutils.MatrixToStr(rows))
		fmt.Fprint(failureLog, "\n")
	}

	connSetup := func(conn *gosql.DB) {
		setStmtTimeout := fmt.Sprintf("SET statement_timeout='%s';", statementTimeout.String())
		t.Status("setting statement_timeout")
		t.L().Printf("statement timeout:\n%s", setStmtTimeout)
		if _, err := conn.Exec(setStmtTimeout); err != nil {
			t.Fatal(err)
		}
		logStmt(setStmtTimeout)

		setUnconstrainedStmt := "SET unconstrained_non_covering_index_scan_enabled = true;"
		t.Status("setting unconstrained_non_covering_index_scan_enabled")
		t.L().Printf("\n%s", setUnconstrainedStmt)
		if _, err := conn.Exec(setUnconstrainedStmt); err != nil {
			logStmt(setUnconstrainedStmt)
			t.Fatal(err)
		}
		logStmt(setUnconstrainedStmt)
	}

	node := 1
	conn := c.Conn(ctx, t.L(), node)

	rnd, seed := randutil.NewTestRand()
	t.L().Printf("seed: %d", seed)
	t.L().Printf("setupName: %s", qct.setupName)

	connSetup(conn)

	if qct.setupName == "workload-replay" {

		logTest := func(logStr string, logType string) {
			stmt := strings.TrimSpace(logStr)
			if stmt == "" {
				return
			}
			fmt.Fprint(log, logType+":"+stmt)
			fmt.Fprint(log, "\n")
		}

		var finalStmt string
		var signatures map[string][]string

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			t.L().Printf("Choosing Random Query")
			finalStmt, signatures, err = workloadreplay.ChooseRandomQuery(ctx, log)
			if err != nil {
				// An error here likely denotes an infrastructure issue; i.e., unable to connect to snowflake. Wrapping
				// it as a transient failure will route the test failure to TestEng, bypassing the (issue) owner.
				t.Fatal(rperrors.TransientFailure(err, "snowflake connectivity issue"))
			}
			if finalStmt == "" {
				continue
			}
			t.L().Printf("Generating Random Data in Snowflake")
			schemaMap, err := workloadreplay.CreateRandomDataSnowflake(ctx, signatures, log)
			if err != nil {
				t.Fatal(rperrors.TransientFailure(err, "snowflake connectivity issue"))
			}
			if schemaMap == nil {
				continue
			}
			// Create tables.

			for tableName, schemaInfo := range schemaMap {
				t.L().Printf("creating table: " + tableName)
				fmt.Println(schemaInfo[2])
				if _, err := conn.Exec(schemaInfo[2]); err != nil {
					t.L().Printf("error while creating table: %v", err)
				}
			}

			// Load tables with initial data.

			importStr := ""
			for tableName, schemaInfo := range schemaMap {
				t.L().Printf("inserting rows into table:" + tableName)
				credKey, ok := os.LookupEnv(keyTag)
				if !ok {
					t.L().Printf("%s not set\n", keyTag)
					t.Fatal(rperrors.TransientFailure(err, "GCE credentials issue"))
				}
				encodedKey := b64.StdEncoding.EncodeToString([]byte(credKey))
				importStr = "IMPORT INTO " + tableName + " (" + schemaInfo[1] + ")\n"
				csvStr := " CSV DATA ('gs://" + bucketName + "/" + schemaInfo[0] + "?AUTH=specified&CREDENTIALS=" + encodedKey + "');"
				queryStr := importStr + csvStr
				logTest(queryStr, "QUERY_SNOWFLAKE_TO_GCS:")
				if _, err := conn.Exec(queryStr); err != nil {
					t.L().Printf("error while inserting rows: %v", err)
					t.Fatal(rperrors.TransientFailure(err, "snowflake connectivity issue"))
				}
			}

			// Test if query will run with the schemas created.
			t.L().Printf("Testing if valid query: %v", finalStmt)
			_, err = conn.Query("explain " + finalStmt)
			if err != nil {
				t.L().Printf("Not a Valid query with respect to table schema: %v", err)
				logTest(finalStmt, "Query/Schema Above is Invalid")
				for tablename := range schemaMap {
					if _, droperror := conn.Exec("drop table " + tablename); droperror != nil {
						t.L().Printf("error while dropping table: %v", droperror)

					}
				}
			} else { // Valid query so can run query and perform test.
				logTest(finalStmt, "Valid Query")

				h := queryComparisonHelper{
					conn1:      conn,
					conn2:      conn,
					logStmt:    logStmt,
					logFailure: logFailure,
					printStmt:  printStmt,
					stmtNo:     0,
					rnd:        rnd,
				}

				workloadqg := workloadReplayGenerator{finalStmt}

				if err := qct.run(&workloadqg, rnd, h); err != nil {
					t.Fatal(err)
				}

			}
		}

	} else {

		setup := sqlsmith.Setups[qct.setupName](rnd)

		t.Status("executing setup")
		t.L().Printf("setup:\n%s", strings.Join(setup, "\n"))
		for _, stmt := range setup {
			if _, err := conn.Exec(stmt); err != nil {
				t.Fatal(err)
			} else {
				logStmt(stmt)
			}
		}

		conn2 := conn
		node2 := 1
		if qct.isMultiRegion {
			t.Status("setting up multi-region database")
			setupMultiRegionDatabase(t, conn, rnd, logStmt)

			// Choose a different node than node 1.
			node2 = rnd.Intn(qct.nodeCount-1) + 2
			t.Status(fmt.Sprintf("running some queries from node %d with conn1 and some queries from node %d with conn2", node, node2))
			conn2 = c.Conn(ctx, t.L(), node2)
			connSetup(conn2)
		}

		// Initialize a smither that generates only INSERT and UPDATE statements with
		// the InsUpdOnly option.
		mutatingSmither := newMutatingSmither(conn, rnd, t, true /* disableDelete */, qct.isMultiRegion)
		defer mutatingSmither.Close()

		// Initialize a smither that generates only deterministic SELECT statements.
		smither, err := sqlsmith.NewSmither(conn, rnd,
			sqlsmith.DisableMutations(), sqlsmith.DisableNondeterministicFns(),
			sqlsmith.DisableNondeterministicLimits(),
			sqlsmith.UnlikelyConstantPredicate(), sqlsmith.FavorCommonData(),
			sqlsmith.UnlikelyRandomNulls(), sqlsmith.DisableCrossJoins(),
			sqlsmith.DisableIndexHints(), sqlsmith.DisableWith(), sqlsmith.DisableDecimals(),
			sqlsmith.LowProbabilityWhereClauseWithJoinTables(),
			sqlsmith.SetComplexity(.3),
			sqlsmith.SetScalarComplexity(.1),
			sqlsmith.SimpleNames(),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer smither.Close()

		t.Status("running ", qct.name)
		until := time.After(roundTimeout)
		done := ctx.Done()

		for i := 1; ; i++ {
			select {
			case <-until:
				return
			case <-done:
				return
			default:
			}

			numInitialMutations := 1000
			if qct.isMultiRegion {
				// Inserts are slow in multi-region setups, so we can't get through as
				// many before the end of the test.
				numInitialMutations = 100
			}

			if i == numInitialMutations {
				t.Status("running ", qct.name, ": ", i, " initial mutations completed")
				// Initialize a new mutating smither that generates INSERT, UPDATE and
				// DELETE statements with the MutationsOnly option.
				mutatingSmither = newMutatingSmither(conn, rnd, t, false /* disableDelete */, qct.isMultiRegion)
				//nolint:deferloop TODO(#137605)
				defer mutatingSmither.Close()
			}

			if i%numInitialMutations == 0 {
				if i != numInitialMutations {
					t.Status("running ", qct.name, ": ", i, " statements completed")
				}
			}

			h := queryComparisonHelper{
				conn1:      conn,
				conn2:      conn2,
				node1:      node,
				node2:      node2,
				logStmt:    logStmt,
				logFailure: logFailure,
				printStmt:  printStmt,
				stmtNo:     i,
				rnd:        rnd,
			}

			// Run `numInitialMutations` mutations first so that the tables have rows.
			// Run a mutation every 25th iteration afterwards to continually change the
			// state of the database.
			if i < numInitialMutations || i%25 == 0 {
				mConn, mConnInfo := h.chooseConn()
				runMutationStatement(t, mConn, mConnInfo, mutatingSmither, logStmt)
				continue
			}

			if err := qct.run(smither, rnd, h); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func newMutatingSmither(
	conn *gosql.DB, rnd *rand.Rand, t test.Test, disableDelete bool, isMultiRegion bool,
) (mutatingSmither *sqlsmith.Smither) {
	var smitherOpts []sqlsmith.SmitherOption
	smitherOpts = append(smitherOpts,
		sqlsmith.FavorCommonData(), sqlsmith.UnlikelyRandomNulls(),
		sqlsmith.DisableInsertSelect(), sqlsmith.DisableCrossJoins(),
		sqlsmith.SetComplexity(.05),
		sqlsmith.SetScalarComplexity(.01),
		sqlsmith.SimpleNames(),
	)
	if disableDelete {
		smitherOpts = append(smitherOpts, sqlsmith.InsUpdOnly())
	} else {
		smitherOpts = append(smitherOpts, sqlsmith.MutationsOnly())
	}
	if isMultiRegion {
		smitherOpts = append(smitherOpts, sqlsmith.EnableAlters())
		smitherOpts = append(smitherOpts, sqlsmith.MultiRegionDDLs())
	}
	var err error
	mutatingSmither, err = sqlsmith.NewSmither(conn, rnd, smitherOpts...)
	if err != nil {
		t.Fatal(err)
	}
	return mutatingSmither
}

// sqlAndOutput holds a SQL statement and its output.
type sqlAndOutput struct {
	sql    string
	output [][]string
}

// queryComparisonHelper is used to execute statements in query comparison
// tests. It keeps track of each statement that is executed so they can be
// logged in case of failure.
type queryComparisonHelper struct {
	// There are two different connections so that we sometimes execute the
	// queries on different nodes.
	conn1, conn2 *gosql.DB
	node1, node2 int
	logStmt      func(string)
	logFailure   func(string, [][]string)
	printStmt    func(string)
	stmtNo       int
	rnd          *rand.Rand

	statements            []string
	statementsAndExplains []sqlAndOutput
	colTypes              []string
}

// chooseConn flips a coin to determine which connection is used. It returns the
// connection and a string to identify the connection for debugging purposes.
func (h *queryComparisonHelper) chooseConn() (conn *gosql.DB, connInfo string) {
	if h.node1 == h.node2 {
		return h.conn1, ""
	}
	// Assuming we will be using cockroach demo --insecure to replay any failed
	// logs, we add a \connect command for the SQL CLI to switch to the other node
	// using only a port number.
	defaultFirstPort, _ := strconv.Atoi(base.DefaultPort)
	if h.rnd.Intn(2) == 0 {
		conn = h.conn1
		connInfo = fmt.Sprintf("\\connect - - - %d\n", defaultFirstPort+h.node1-1)
	} else {
		conn = h.conn2
		connInfo = fmt.Sprintf("\\connect - - - %d\n", defaultFirstPort+h.node2-1)
	}
	return conn, connInfo
}

// runQuery runs the given query and returns the output. If the stmt doesn't
// result in an error, as a side effect, it also saves the query, the query
// plan, and the output of running the query so they can be logged in case of
// failure.
//
//	stmt - the query to run
//	conn - the connection to use
//	connInfo - a string to identify the connection for debugging purposes
func (h *queryComparisonHelper) runQuery(
	stmt string, conn *gosql.DB, connInfo string,
) ([][]string, error) {
	// Log this statement with a timestamp but commented out. This will help in
	// cases when the stmt will get stuck and the whole test will time out (in
	// such a scenario, since the stmt didn't execute successfully, it won't get
	// logged by the caller).
	h.logStmt(fmt.Sprintf("-- %s: %s", timeutil.Now(),
		// Escape all control characters, including newline symbols, to log this
		// stmt as a single line. This way this auxiliary logging takes up less
		// space (if the stmt executes successfully, it'll still get logged with the
		// usual formatting below).
		strconv.Quote(connInfo+stmt+";"),
	))

	runQueryImpl := func(stmt string, conn *gosql.DB) ([][]string, error) {
		rows, err := conn.Query(stmt)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		cts, err := rows.ColumnTypes()
		if err != nil {
			return nil, err
		}
		h.colTypes = make([]string, len(cts))
		for i, ct := range cts {
			h.colTypes[i] = ct.DatabaseTypeName()
		}
		return sqlutils.RowsToStrMatrix(rows)
	}

	// First use EXPLAIN (DISTSQL) to try to get the query plan. This is
	// best-effort, and only for the purpose of debugging, so ignore any errors.
	explainStmt := "EXPLAIN (DISTSQL) " + stmt
	explainRows, err := runQueryImpl(explainStmt, conn)
	if err == nil {
		h.statementsAndExplains = append(
			h.statementsAndExplains, sqlAndOutput{sql: explainStmt, output: explainRows},
		)
	}

	// Now run the query and save the output.
	rows, err := runQueryImpl(stmt, conn)
	if err != nil {
		return nil, err
	}
	// Only save the stmt on success - this makes it easier to reproduce the
	// log. The caller still can include it into the statements later if
	// necessary.
	h.addStmtForLogging(connInfo+stmt, rows)
	return rows, nil
}

// addStmtForLogging includes the provided stmt (as well as optional output
// rows) to be included into logging later.
func (h *queryComparisonHelper) addStmtForLogging(stmt string, rows [][]string) {
	h.statements = append(h.statements, stmt)
	h.statementsAndExplains = append(h.statementsAndExplains, sqlAndOutput{sql: stmt, output: rows})
}

// execStmt executes the given statement. As a side effect, it also saves the
// statement so it can be logged in case of failure.
//
//	stmt - the statement to execute
//	conn - the connection to use
//	connInfo - a string to identify the connection for debugging purposes
func (h *queryComparisonHelper) execStmt(stmt string, conn *gosql.DB, connInfo string) error {
	h.addStmtForLogging(connInfo+stmt, nil /* rows */)
	_, err := conn.Exec(stmt)
	return err
}

// logStatements logs all the queries and statements that were executed.
func (h *queryComparisonHelper) logStatements() {
	for _, stmt := range h.statements {
		h.logStmt(stmt)
		h.printStmt(stmt)
	}
}

// logVerboseOutput logs all the queries and statements that were executed,
// as well as the output rows and EXPLAIN output.
func (h *queryComparisonHelper) logVerboseOutput() {
	for _, stmtOrExplain := range h.statementsAndExplains {
		h.logFailure(stmtOrExplain.sql, stmtOrExplain.output)
	}
}

// makeError wraps the error with the given message and includes the number of
// statements run so far.
func (h *queryComparisonHelper) makeError(err error, msg string) error {
	return errors.Wrapf(err, "%s. %d statements run", msg, h.stmtNo)
}

// joinAndSortRows sorts rows using string comparison.
func joinAndSortRows(rowMatrix1, rowMatrix2 [][]string, sep string) (rows1, rows2 []string) {
	for _, row := range rowMatrix1 {
		rows1 = append(rows1, strings.Join(row[:], sep))
	}
	for _, row := range rowMatrix2 {
		rows2 = append(rows2, strings.Join(row[:], sep))
	}
	sort.Strings(rows1)
	sort.Strings(rows2)
	return rows1, rows2
}

func isFloatArray(colType string) bool {
	switch colType {
	case "[]FLOAT4", "[]FLOAT8", "_FLOAT4", "_FLOAT8":
		return true
	default:
		return false
	}
}

func isDecimalArray(colType string) bool {
	switch colType {
	case "[]DECIMAL", "_DECIMAL":
		return true
	default:
		return false
	}
}

func needApproximateMatch(colType string) bool {
	// On s390x, check that values for both float and decimal coltypes are
	// approximately equal to take into account platform differences in floating
	// point calculations. On other architectures, check float values only.
	return (runtime.GOARCH == "s390x" && (colType == "DECIMAL" || isDecimalArray(colType))) ||
		colType == "FLOAT4" || colType == "FLOAT8" || isFloatArray(colType)
}

// sortRowsWithFloatComp is similar to joinAndSortRows, but it uses float
// comparison for float columns (and decimal columns when on s390x).
func sortRowsWithFloatComp(rowMatrix1, rowMatrix2 [][]string, colTypes []string) {
	floatsLess := func(i, j int, rowMatrix [][]string) bool {
		for idx := range colTypes {
			if needApproximateMatch(colTypes[idx]) {
				cmpFn := floatcmp.FloatsCmp
				if isFloatArray(colTypes[idx]) || isDecimalArray(colTypes[idx]) {
					cmpFn = floatcmp.FloatArraysCmp
				}
				res, err := cmpFn(rowMatrix[i][idx], rowMatrix[j][idx])
				if err != nil {
					panic(errors.NewAssertionErrorWithWrappedErrf(err, "error comparing floats %s and %s",
						rowMatrix[i][idx], rowMatrix[j][idx]))
				}
				if res != 0 {
					return res == -1
				}
			} else {
				if rowMatrix[i][idx] != rowMatrix[j][idx] {
					return rowMatrix[i][idx] < rowMatrix[j][idx]
				}
			}
		}
		return false
	}

	sort.Slice(rowMatrix1, func(i, j int) bool {
		return floatsLess(i, j, rowMatrix1)
	})
	sort.Slice(rowMatrix2, func(i, j int) bool {
		return floatsLess(i, j, rowMatrix2)
	})
}

func trimDecimalTrailingZeroes(d string) string {
	if !strings.Contains(d, ".") {
		// The number is an integer - nothing to trim.
		return d
	}
	d = strings.TrimRight(d, "0")
	if d[len(d)-1] == '.' {
		// We fully trimmed the fractional part, so we need to remove the
		// dangling dot.
		d = d[:len(d)-1]
	}
	return d
}

func trimDecimalsTrailingZeroesInArray(array string) string {
	decimals := strings.Split(strings.Trim(array, "{}"), ",")
	for i := range decimals {
		decimals[i] = trimDecimalTrailingZeroes(decimals[i])
	}
	return "{" + strings.Join(decimals, ",") + "}"
}

// unsortedMatricesDiffWithFloatComp sorts and compares the rows in rowMatrix1
// to rowMatrix2 and outputs a diff or message related to the comparison. If a
// string comparison of the rows fails, and they contain floats or decimals, it
// performs an approximate comparison of the values.
// TODO(yuzefovich): if we extend this logic to handle COLLATED STRINGs in a
// special way (i.e. using DB connection), then we can remove some code
// introduced in #124677.
func unsortedMatricesDiffWithFloatComp(
	rowMatrix1, rowMatrix2 [][]string, colTypes []string,
) (string, error) {
	rows1, rows2 := joinAndSortRows(rowMatrix1, rowMatrix2, ",")
	result := cmp.Diff(rows1, rows2)
	if result == "" {
		return result, nil
	}
	if len(rows1) != len(rows2) || len(colTypes) != len(rowMatrix1[0]) || len(colTypes) != len(rowMatrix2[0]) {
		return result, nil
	}
	var needCustomMatch bool
	for _, colType := range colTypes {
		if needApproximateMatch(colType) || colType == "DECIMAL" || isDecimalArray(colType) {
			needCustomMatch = true
			break
		}
	}
	if !needCustomMatch {
		return result, nil
	}
	sortRowsWithFloatComp(rowMatrix1, rowMatrix2, colTypes)
	for i := range rowMatrix1 {
		row1 := rowMatrix1[i]
		row2 := rowMatrix2[i]

		for j, colType := range colTypes {
			if needApproximateMatch(colType) {
				isFloatOrDecimalArray := isFloatArray(colType) || isDecimalArray(colType)
				cmpFn := floatcmp.FloatsMatch
				switch {
				case runtime.GOARCH == "s390x" && isFloatOrDecimalArray:
					cmpFn = floatcmp.FloatArraysMatchApprox
				case runtime.GOARCH == "s390x" && !isFloatOrDecimalArray:
					cmpFn = floatcmp.FloatsMatchApprox
				case isFloatOrDecimalArray:
					cmpFn = floatcmp.FloatArraysMatch
				}
				match, err := cmpFn(row1[j], row2[j])
				if err != nil {
					return "", err
				}
				if !match {
					return result, nil
				}
			} else {
				switch {
				case colType == "DECIMAL":
					// For decimals, remove any trailing zeroes.
					row1[j] = trimDecimalTrailingZeroes(row1[j])
					row2[j] = trimDecimalTrailingZeroes(row2[j])
				case isDecimalArray(colType):
					// For decimal arrays, remove any trailing zeroes from each
					// decimal.
					row1[j] = trimDecimalsTrailingZeroesInArray(row1[j])
					row2[j] = trimDecimalsTrailingZeroesInArray(row2[j])
				}
				// Check that other columns are equal with a string comparison.
				if row1[j] != row2[j] {
					return result, nil
				}
			}
		}
	}
	return "", nil
}

// unsortedMatricesDiff sorts and compares rows of data.
// TODO(yuzefovich): if we extend this logic to handle COLLATED STRINGs in a
// special way (i.e. using DB connection), then we can remove some code
// introduced in #124677.
func unsortedMatricesDiff(rowMatrix1, rowMatrix2 [][]string) string {
	var rows1 []string
	for _, row := range rowMatrix1 {
		rows1 = append(rows1, strings.Join(row[:], ","))
	}
	var rows2 []string
	for _, row := range rowMatrix2 {
		rows2 = append(rows2, strings.Join(row[:], ","))
	}
	sort.Strings(rows1)
	sort.Strings(rows2)
	return cmp.Diff(rows1, rows2)
}
