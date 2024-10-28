// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package sctest contains tools to run end-to-end datadriven tests in both
// ccl and non-ccl settings.
package sctest

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// EndToEndSideEffects is a data-driven test runner that executes DDL statements in the
// declarative schema changer injected with test dependencies and compares the
// accumulated side effects logs with expected results from the data-driven
// test file.
//
// It shares a data-driven format with Rollback.
func EndToEndSideEffects(t *testing.T, relTestCaseDir string, factory TestServerFactory) {
	// These tests are expensive.
	skip.UnderStress(t)
	skip.UnderRace(t)

	ctx := context.Background()
	testCaseDir := datapathutils.RewritableDataPath(t, relTestCaseDir)
	testCaseDefinition := filepath.Join(testCaseDir, filepath.Base(testCaseDir)+".definition")
	// Create a test cluster.
	factory.Run(ctx, t, func(s serverutils.TestServerInterface, db *gosql.DB) {
		tdb := sqlutils.MakeSQLRunner(db)
		numTestStatementsObserved := 0
		var setupStmts statements.Statements
		var setupOutput string
		datadriven.RunTest(t, testCaseDefinition, func(t *testing.T, d *datadriven.TestData) string {
			parseStmts := func() (statements.Statements, func()) {
				sqlutils.VerifyStatementPrettyRoundtrip(t, d.Input)
				stmts, err := parser.Parse(d.Input)
				require.NoError(t, err)
				require.NotEmpty(t, stmts)
				return stmts, func() {
					for _, stmt := range stmts {
						tdb.Exec(t, stmt.SQL)
					}
					waitForSchemaChangesToSucceed(t, tdb)
				}
			}
			switch d.Cmd {
			case "skip":
				var issue int
				var testsCSV string
				d.ScanArgs(t, "issue-num", &issue)
				d.MaybeScanArgs(t, "tests", &testsCSV)
				for _, skippedKind := range strings.Split(testsCSV, ",") {
					if strings.HasPrefix(t.Name(), skippedKind) {
						skip.WithIssue(t, issue)
					}
				}
			case "setup":
				stmts, execStmts := parseStmts()
				a := prettyNamespaceDump(t, tdb)
				execStmts()
				b := prettyNamespaceDump(t, tdb)
				setupStmts = stmts
				setupOutput = sctestutils.Diff(a, b, sctestutils.DiffArgs{CompactLevel: 1})
			case "stage-exec", "stage-query":
				// Both of these commands are DML injections, which is not relevant
				// for end-to-end side-effect testing, so we ignore them.
				break
			case "test":
				stmts, _ := parseStmts()
				require.Lessf(t, numTestStatementsObserved, 1, "only one test per-file.")
				numTestStatementsObserved++
				stmtSqls := make([]string, 0, len(stmts))
				for _, stmt := range stmts {
					stmtSqls = append(stmtSqls, stmt.SQL)
				}

				// Create test dependencies and execute the schema changer.
				// The schema changer test dependencies do not hold any reference to the
				// test cluster, here the SQLRunner is only used to populate the mocked
				// catalog state.
				// It is declared here because it's used in its initialization below.
				var deps *sctestdeps.TestState
				// Set up a reference provider factory for the purpose of proper
				// dependency resolution.
				execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
				refFactory, refFactoryCleanup := sql.NewReferenceProviderFactoryForTest(
					ctx, "test" /* opName */, kv.NewTxn(ctx, s.DB(), s.NodeID()), username.RootUserName(), &execCfg, "defaultdb",
				)
				defer refFactoryCleanup()

				deps = sctestdeps.NewTestDependencies(
					sctestdeps.WithDescriptors(sctestdeps.ReadDescriptorsFromDB(ctx, t, tdb).Catalog),
					sctestdeps.WithSystemDatabaseDescriptor(),
					sctestdeps.WithNamespace(sctestdeps.ReadNamespaceFromDB(t, tdb).Catalog),
					sctestdeps.WithCurrentDatabase(sctestdeps.ReadCurrentDatabaseFromDB(t, tdb)),
					sctestdeps.WithSessionData(sctestdeps.ReadSessionDataFromDB(t, tdb, func(
						sd *sessiondata.SessionData,
					) {
						// For setting up a builder inside tests we will ensure that the new schema
						// changer will allow non-fully implemented operations.
						sd.NewSchemaChangerMode = sessiondatapb.UseNewSchemaChangerUnsafe
						sd.TempTablesEnabled = true
						sd.ApplicationName = ""
						sd.EnableUniqueWithoutIndexConstraints = true // this allows `ADD UNIQUE WITHOUT INDEX` in the testing suite.
						sd.AlterColumnTypeGeneralEnabled = true
					})),
					sctestdeps.WithTestingKnobs(&scexec.TestingKnobs{
						BeforeStage: func(p scplan.Plan, stageIdx int) error {
							deps.LogSideEffectf("## %s", p.Stages[stageIdx].String())
							return nil
						},
					}),
					sctestdeps.WithStatements(stmtSqls...),
					sctestdeps.WithComments(sctestdeps.ReadCommentsFromDB(t, tdb)),
					sctestdeps.WithIDGenerator(s.ApplicationLayer()),
					sctestdeps.WithReferenceProviderFactory(refFactory),
				)
				stmtStates := execStatementWithTestDeps(ctx, t, deps, stmts...)
				var fileNameSuffix string
				const inRollback = false
				for i, stmt := range stmts {
					if len(stmts) > 1 {
						fileNameSuffix = fmt.Sprintf("__statement_%d_of_%d", i+1, len(stmts))
					}
					checkExplainDiagrams(t, testCaseDir, setupStmts, stmts[:i], stmt.SQL, fileNameSuffix, stmtStates[i], inRollback, d.Rewrite)
				}
				output := replaceNonDeterministicOutput(deps.SideEffectLog())
				checkSideEffects(t, testCaseDir, setupStmts, stmts, setupOutput, output, d.Rewrite)
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
			return d.Expected
		})
	})
}

// checkSideEffects checks or rewrites the side effects log.
func checkSideEffects(
	t *testing.T,
	testCaseDir string,
	setupStmts, stmts statements.Statements,
	setupOutput, output string,
	rewrite bool,
) {
	var actual bytes.Buffer
	{
		actual.WriteString("/* setup */\n")
		for _, stmt := range setupStmts {
			actual.WriteString(stmt.SQL)
			actual.WriteString(";\n")
		}
		actual.WriteString("----\n")
		actual.WriteString(setupOutput)
		actual.WriteString("\n\n/* test */\n")
		for _, stmt := range stmts {
			actual.WriteString(stmt.SQL)
			actual.WriteString(";\n")
		}
		actual.WriteString("----\n")
		actual.WriteString(output)
	}
	testCaseName := filepath.Base(testCaseDir)
	expectedOutputFileName := filepath.Join(testCaseDir, testCaseName+".side_effects")
	if rewrite {
		f, err := os.Create(expectedOutputFileName)
		defer func() { require.NoError(t, f.Close()) }()
		require.NoError(t, err)
		_, err = f.Write(actual.Bytes())
		require.NoError(t, err)
		return
	}
	f, err := os.Open(expectedOutputFileName)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	expected, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, string(expected), actual.String(), testCaseName)
}

// checkExplainDiagrams checks the output of the compact and shape explain
// diagrams for the statements and plans from the test. If rewrite is passed,
// the plans will be rewritten.
func checkExplainDiagrams(
	t *testing.T,
	testCaseDir string,
	setupStmts, stmts statements.Statements,
	explainedStmt, fileNameSuffix string,
	state scpb.CurrentState,
	inRollback, rewrite bool,
) {
	testCaseName := filepath.Base(testCaseDir)
	explainFilePrefix := filepath.Join(testCaseDir, testCaseName+fileNameSuffix)
	makeFile := func(suffix string, openFunc func(string) (*os.File, error)) *os.File {
		explainFile, err := openFunc(explainFilePrefix + suffix)
		require.NoError(t, err)
		return explainFile
	}
	writePlan := func(file io.Writer, tag string, fn func() (string, error)) {
		var prefixBuf bytes.Buffer
		prefixBuf.WriteString("/* setup */\n")
		for _, stmt := range setupStmts {
			prefixBuf.WriteString(stmt.SQL)
			prefixBuf.WriteString(";\n")
		}
		prefixBuf.WriteString("\n/* test */\n")
		for _, stmt := range stmts {
			_, _ = fmt.Fprintf(&prefixBuf, "%s;\n", stmt.SQL)
		}
		_, err := file.Write(prefixBuf.Bytes())
		require.NoError(t, err)
		_, err = fmt.Fprintf(file, "EXPLAIN (%s) %s;\n----\n", tag, explainedStmt)
		require.NoError(t, err)
		out, err := fn()
		require.NoError(t, err)
		_, err = io.WriteString(file, out)
		require.NoError(t, err)
	}
	writePlanToFile := func(suffix, tag string, fn func() (string, error)) {
		file := makeFile(suffix, os.Create)
		defer func() { require.NoError(t, file.Close()) }()
		writePlan(file, tag, fn)
	}
	checkPlan := func(suffix, tag string, fn func() (string, error)) {
		file := makeFile(suffix, os.Open)
		defer func() { require.NoError(t, file.Close()) }()
		var buf bytes.Buffer
		writePlan(&buf, tag, fn)
		got, err := io.ReadAll(file)
		require.NoError(t, err)
		require.Equal(t, string(got), buf.String(), testCaseName+fileNameSuffix)
	}
	action := checkPlan
	if rewrite {
		action = writePlanToFile
	}
	params := scplan.Params{
		ActiveVersion:              clusterversion.TestingClusterVersion,
		ExecutionPhase:             scop.StatementPhase,
		SchemaChangerJobIDSupplier: func() jobspb.JobID { return 1 },
		MemAcc:                     mon.NewStandaloneUnlimitedAccount(),
	}
	if inRollback {
		params.InRollback = true
		params.ExecutionPhase = scop.PostCommitNonRevertiblePhase
	}
	pl, err := scplan.MakePlan(context.Background(), state, params)
	require.NoErrorf(t, err, "%s: %s", fileNameSuffix, explainedStmt)
	action(".explain", "DDL", pl.ExplainCompact)
	if !inRollback {
		action(".explain_shape", "DDL, SHAPE", pl.ExplainShape)
	}
}

// scheduleIDRegexp captures either `scheduleId: 384784` or `scheduleId: "374764"`.
var scheduleIDRegexp = regexp.MustCompile(`scheduleId: "?[0-9]+"?`)

// dropTimeRegexp captures either `dropTime: \"time\"`.
var dropTimeRegexp = regexp.MustCompile("dropTime: \"[0-9]+")

func replaceNonDeterministicOutput(text string) string {
	// scheduleIDs change based on execution time, so redact the output.
	nextString := scheduleIDRegexp.ReplaceAllString(text, "scheduleId: <redacted>")
	return dropTimeRegexp.ReplaceAllString(nextString, "dropTime: <redacted>")
}

// execStatementWithTestDeps executes the DDL statement using the declarative
// schema changer with testing dependencies injected.
func execStatementWithTestDeps(
	ctx context.Context,
	t *testing.T,
	deps *sctestdeps.TestState,
	stmts ...statements.Statement[tree.Statement],
) (stateAfterBuildingEachStatement []scpb.CurrentState) {
	var jobID jobspb.JobID
	var state scpb.CurrentState
	var logSchemaChangesFn scbuild.LogSchemaChangerEventsFn
	var err error

	deps.WithTxn(func(s *sctestdeps.TestState) {
		// Run statement phase.
		deps.IncrementPhase()
		deps.LogSideEffectf("# begin %s", deps.Phase())
		for _, stmt := range stmts {
			state, logSchemaChangesFn, err = scbuild.Build(ctx, deps, state, stmt.AST, mon.NewStandaloneUnlimitedAccount())
			require.NoError(t, err, "error in builder")
			require.NoError(t, logSchemaChangesFn(ctx), "error generating event log entries")
			stateAfterBuildingEachStatement = append(stateAfterBuildingEachStatement, state)
			state, _, err = scrun.RunStatementPhase(ctx, s.TestingKnobs(), s, state)
			require.NoError(t, err, "error in %s", s.Phase())
		}
		deps.LogSideEffectf("# end %s", deps.Phase())
		// Run pre-commit phase.
		deps.IncrementPhase()
		deps.LogSideEffectf("# begin %s", deps.Phase())
		state, jobID, err = scrun.RunPreCommitPhase(ctx, s.TestingKnobs(), s, state)
		require.NoError(t, err, "error in %s", s.Phase())
		deps.LogSideEffectf("# end %s", deps.Phase())
	})
	if job := deps.JobRecord(jobID); job != nil {
		// Run post-commit phase in mock schema change job.
		deps.IncrementPhase()
		deps.LogSideEffectf("# begin %s", deps.Phase())
		err = scrun.RunSchemaChangesInJob(
			ctx, deps.TestingKnobs(), deps, jobID, job.DescriptorIDs, nil, /* rollbackCause */
		)
		require.NoError(t, err, "error in mock schema change job execution")
		deps.LogSideEffectf("# end %s", deps.Phase())
	}
	return stateAfterBuildingEachStatement
}

// prettyNamespaceDump prints the state of the namespace table, minus
// descriptorless public schema and system database entries.
func prettyNamespaceDump(t *testing.T, tdb *sqlutils.SQLRunner) string {
	rows := tdb.QueryStr(t, fmt.Sprintf(`
		SELECT "parentID", "parentSchemaID", name, id
		FROM system.namespace
		WHERE id NOT IN (%d, %d) AND "parentID" <> %d
		ORDER BY id ASC`,
		keys.PublicSchemaID,
		keys.SystemDatabaseID,
		keys.SystemDatabaseID,
	))
	lines := make([]string, 0, len(rows))
	for _, row := range rows {
		parentID, parentSchemaID, name, id := row[0], row[1], row[2], row[3]
		nameType := "object"
		if parentSchemaID == "0" {
			if parentID == "0" {
				nameType = "database"
			} else {
				nameType = "schema"
			}
		}
		line := fmt.Sprintf("%s {%s %s %s} -> %s", nameType, parentID, parentSchemaID, name, id)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}
