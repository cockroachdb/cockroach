// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// NewClusterFunc provides functionality to construct a new cluster
// given testing knobs.
type NewClusterFunc func(
	t *testing.T, knobs *scexec.TestingKnobs,
) (_ serverutils.TestServerInterface, _ *gosql.DB, cleanup func())

// NewMixedClusterFunc provides functionality to construct a new cluster
// given testing knobs.
type NewMixedClusterFunc func(
	t *testing.T, knobs *scexec.TestingKnobs, downlevelVersion bool,
) (_ serverutils.TestServerInterface, _ *gosql.DB, cleanup func())

// SingleNodeCluster is a NewClusterFunc.
func SingleNodeCluster(
	t *testing.T, knobs *scexec.TestingKnobs,
) (serverutils.TestServerInterface, *gosql.DB, func()) {
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disabled due to a failure in TestBackupRestore. Tracked with #76378.
		DisableDefaultTestTenant: true,
		Knobs: base.TestingKnobs{
			SQLDeclarativeSchemaChanger: knobs,
			JobsTestingKnobs:            newJobsKnobs(),
			SQLExecutor: &sql.ExecutorTestingKnobs{
				UseTransactionalDescIDGenerator: true,
			},
		},
	})

	return s, db, func() {
		s.Stopper().Stop(context.Background())
	}
}

// SingleNodeMixedCluster is a NewClusterFunc.
func SingleNodeMixedCluster(
	t *testing.T, knobs *scexec.TestingKnobs, downlevelVersion bool,
) (serverutils.TestServerInterface, *gosql.DB, func()) {
	targetVersion := clusterversion.TestingBinaryVersion
	if downlevelVersion {
		targetVersion = clusterversion.ByKey(clusterversion.V22_2)
	}
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disabled due to a failure in TestBackupRestore. Tracked with #76378.
		DisableDefaultTestTenant: true,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				BinaryVersionOverride:          targetVersion,
				DisableAutomaticVersionUpgrade: make(chan struct{}),
			},
			SQLDeclarativeSchemaChanger: knobs,
			JobsTestingKnobs:            newJobsKnobs(),
			SQLExecutor: &sql.ExecutorTestingKnobs{
				UseTransactionalDescIDGenerator: true,
			},
		},
	})

	return s, db, func() {
		s.Stopper().Stop(context.Background())
	}
}

// newJobsKnobs constructs jobs.TestingKnobs for the end-to-end tests.
func newJobsKnobs() *jobs.TestingKnobs {
	jobKnobs := jobs.NewTestingKnobsWithShortIntervals()

	// We want to force the process of marking the job as successful
	// to fail sometimes. This will ensure that the schema change job
	// is idempotent.
	var injectedFailures = struct {
		syncutil.Mutex
		m map[jobspb.JobID]struct{}
	}{
		m: make(map[jobspb.JobID]struct{}),
	}
	jobKnobs.BeforeUpdate = func(orig, updated jobs.JobMetadata) error {
		sc := orig.Payload.GetNewSchemaChange()
		if sc == nil {
			return nil
		}
		if orig.Status != jobs.StatusRunning || updated.Status != jobs.StatusSucceeded {
			return nil
		}
		injectedFailures.Lock()
		defer injectedFailures.Unlock()
		if _, ok := injectedFailures.m[orig.ID]; !ok {
			injectedFailures.m[orig.ID] = struct{}{}
			log.Infof(context.Background(), "injecting failure while marking job succeeded")
			return errors.New("injected failure when marking succeeded")
		}
		return nil
	}
	return jobKnobs
}

// EndToEndSideEffects is a data-driven test runner that executes DDL statements in the
// declarative schema changer injected with test dependencies and compares the
// accumulated side effects logs with expected results from the data-driven
// test file.
//
// It shares a data-driven format with Rollback.
func EndToEndSideEffects(t *testing.T, relPath string, newCluster NewClusterFunc) {
	skip.UnderStress(t)
	skip.UnderStressRace(t)
	ctx := context.Background()
	path := datapathutils.RewritableDataPath(t, relPath)
	// Create a test cluster.
	s, db, cleanup := newCluster(t, nil /* knobs */)
	tdb := sqlutils.MakeSQLRunner(db)
	defer cleanup()
	numTestStatementsObserved := 0
	var setupStmts statements.Statements
	datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
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
		case "setup":
			stmts, execStmts := parseStmts()
			a := prettyNamespaceDump(t, tdb)
			execStmts()
			b := prettyNamespaceDump(t, tdb)
			setupStmts = stmts
			return sctestutils.Diff(a, b, sctestutils.DiffArgs{CompactLevel: 1})
		case "stage-exec":
			fallthrough
		case "stage-query":
			// Both of these commands are DML injections, which is not relevant
			// for end-to-end testing. We don't actually execute statements here.
			// So return the original output, these will get rewritten in
			// cumulativeTest.
			return d.Expected
		case "test":
			stmts, execStmts := parseStmts()
			require.Lessf(t, numTestStatementsObserved, 1, "only one test per-file.")
			numTestStatementsObserved++
			stmtSqls := make([]string, 0, len(stmts))
			for _, stmt := range stmts {
				stmtSqls = append(stmtSqls, stmt.SQL)
			}
			// Keep test cluster in sync.
			defer execStmts()

			// Wait for any jobs due to previous schema changes to finish.
			sctestdeps.WaitForNoRunningSchemaChanges(t, tdb)
			var deps *sctestdeps.TestState
			// Create test dependencies and execute the schema changer.
			// The schema changer test dependencies do not hold any reference to the
			// test cluster, here the SQLRunner is only used to populate the mocked
			// catalog state.
			// Set up a reference provider factory for the purpose of proper
			// dependency resolution.
			execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
			refFactory, refFactoryCleanup := sql.NewReferenceProviderFactoryForTest(
				"test" /* opName */, kv.NewTxn(context.Background(), s.DB(), s.NodeID()), username.RootUserName(), &execCfg, "defaultdb",
			)
			defer refFactoryCleanup()

			deps = sctestdeps.NewTestDependencies(
				sctestdeps.WithDescriptors(sctestdeps.ReadDescriptorsFromDB(ctx, t, tdb).Catalog),
				sctestdeps.WithNamespace(sctestdeps.ReadNamespaceFromDB(t, tdb).Catalog),
				sctestdeps.WithCurrentDatabase(sctestdeps.ReadCurrentDatabaseFromDB(t, tdb)),
				sctestdeps.WithSessionData(sctestdeps.ReadSessionDataFromDB(t, tdb, func(
					sd *sessiondata.SessionData,
				) {
					// For setting up a builder inside tests we will ensure that the new schema
					// changer will allow non-fully implemented operations.
					sd.NewSchemaChangerMode = sessiondatapb.UseNewSchemaChangerUnsafe
					sd.ApplicationName = ""
					sd.EnableUniqueWithoutIndexConstraints = true // this allows `ADD UNIQUE WITHOUT INDEX` in the testing suite.
				})),
				sctestdeps.WithTestingKnobs(&scexec.TestingKnobs{
					BeforeStage: func(p scplan.Plan, stageIdx int) error {
						deps.LogSideEffectf("## %s", p.Stages[stageIdx].String())
						return nil
					},
				}),
				sctestdeps.WithStatements(stmtSqls...),
				sctestdeps.WithComments(sctestdeps.ReadCommentsFromDB(t, tdb)),
				sctestdeps.WithIDGenerator(s),
				sctestdeps.WithReferenceProviderFactory(refFactory),
			)
			stmtStates := execStatementWithTestDeps(ctx, t, deps, stmts...)
			var fileNameSuffix string
			const inRollback = false
			for i, stmt := range stmts {
				if len(stmts) > 1 {
					fileNameSuffix = fmt.Sprintf(".statement_%d_of_%d", i+1, len(stmts))
				}
				checkExplainDiagrams(t, path, setupStmts, stmts[:i], stmt.SQL, fileNameSuffix, stmtStates[i], inRollback, d.Rewrite)
			}
			return replaceNonDeterministicOutput(deps.SideEffectLog())

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

// Subdirectories of the testdata directory containing test files in which to
// write explain diagrams.
const (
	explainDirName        = "explain"
	explainVerboseDirName = "explain_verbose"
)

// checkExplainDiagrams checks the output of the compact and verbose explain
// diagrams for the statements and plans from the test. If rewrite is passed,
// the plans will be rewritten.
func checkExplainDiagrams(
	t *testing.T,
	path string,
	setupStmts, stmts statements.Statements,
	explainedStmt, fileNameSuffix string,
	state scpb.CurrentState,
	inRollback, rewrite bool,
) {

	testDataDir := filepath.Dir(filepath.Dir(path))
	mkdir := func(name string) string {
		dir := filepath.Join(testDataDir, name)
		require.NoError(t, os.MkdirAll(dir, 0777))
		return dir
	}
	explainDir := mkdir(explainDirName)
	explainVerboseDir := mkdir(explainVerboseDirName)
	baseName := filepath.Base(path)
	makeFile := func(dir string, openFunc func(string) (*os.File, error)) *os.File {
		name := baseName + fileNameSuffix
		explainFile, err := openFunc(filepath.Join(dir, name))
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
	writePlanToFile := func(dir, tag string, fn func() (string, error)) {
		file := makeFile(dir, os.Create)
		defer func() { require.NoError(t, file.Close()) }()
		writePlan(file, tag, fn)
	}
	checkPlan := func(dir, tag string, fn func() (string, error)) {
		file := makeFile(dir, os.Open)
		defer func() { require.NoError(t, file.Close()) }()
		var buf bytes.Buffer
		writePlan(&buf, tag, fn)
		got, err := io.ReadAll(file)
		require.NoError(t, err)
		require.Equal(t, string(got), buf.String(), filepath.Base(dir))
	}
	action := checkPlan
	if rewrite {
		action = writePlanToFile
	}
	params := scplan.Params{
		ActiveVersion:              clusterversion.TestingClusterVersion,
		ExecutionPhase:             scop.StatementPhase,
		SchemaChangerJobIDSupplier: func() jobspb.JobID { return 1 },
	}
	if inRollback {
		params.InRollback = true
		params.ExecutionPhase = scop.PostCommitNonRevertiblePhase
	}
	pl, err := scplan.MakePlan(context.Background(), state, params)
	require.NoErrorf(t, err, "%s: %s", fileNameSuffix, explainedStmt)
	action(explainDir, "ddl", pl.ExplainCompact)
	action(explainVerboseDir, "ddl, verbose", pl.ExplainVerbose)
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
	var err error

	deps.WithTxn(func(s *sctestdeps.TestState) {
		// Run statement phase.
		deps.IncrementPhase()
		deps.LogSideEffectf("# begin %s", deps.Phase())
		for _, stmt := range stmts {
			state, err = scbuild.Build(ctx, deps, state, stmt.AST, nil /* memAcc */)
			require.NoError(t, err, "error in builder")
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

func waitForSchemaChangesToSucceed(t *testing.T, tdb *sqlutils.SQLRunner) {
	tdb.CheckQueryResultsRetry(
		t, schemaChangeWaitQuery(`('succeeded')`), [][]string{},
	)
}

func waitForSchemaChangesToFinish(t *testing.T, tdb *sqlutils.SQLRunner) {
	tdb.CheckQueryResultsRetry(
		t, schemaChangeWaitQuery(`('succeeded', 'failed')`), [][]string{},
	)
}

func schemaChangeWaitQuery(statusInString string) string {
	q := fmt.Sprintf(
		`SELECT status, job_type, description FROM [SHOW JOBS] WHERE job_type IN ('%s', '%s', '%s') AND status NOT IN %s`,
		jobspb.TypeSchemaChange,
		jobspb.TypeTypeSchemaChange,
		jobspb.TypeNewSchemaChange,
		statusInString,
	)
	return q
}
