// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sctest

import (
	"context"
	gosql "database/sql"
	"flag"
	"fmt"
	"math"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/corpus"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type stageExecType int

const (
	_                 stageExecType = iota
	stageExecuteQuery stageExecType = 1
	stageExecuteStmt  stageExecType = 2
)

// stageExecStmt a statement that will be executed during a given stage,
// including any expected errors from this statement or any schema change
// running concurrently.
type stageExecStmt struct {
	execType               stageExecType
	stmts                  []string
	expectedOutput         string
	observedOutput         string
	schemaChangeErrorRegex *regexp.Regexp
	// schemaChangeErrorRegexRollback will cause a rollback.
	schemaChangeErrorRegexRollback *regexp.Regexp
}

// HasSchemaChangeError indicates if a schema change error will be observed,
// if the current DML statement is executed.
func (e *stageExecStmt) HasSchemaChangeError() bool {
	return e.schemaChangeErrorRegex != nil
}

func (e *stageExecStmt) HasAnySchemaChangeError() *regexp.Regexp {
	if e.schemaChangeErrorRegex != nil {
		return e.schemaChangeErrorRegex
	}
	return e.schemaChangeErrorRegexRollback
}

// stageKeyOrdinalLatest targets the latest ordinal in a stage.
const stageKeyOrdinalLatest = math.MaxUint16

// stageKey represents a phase and stage range to target (in an
// inclusive manner).
type stageKey struct {
	minOrdinal int
	maxOrdinal int
	phase      scop.Phase
	rollback   bool
}

// makeStageKey constructs a stage key targeting a single ordinal.
func makeStageKey(phase scop.Phase, ordinal int, rollback bool) stageKey {
	return stageKey{
		phase:      phase,
		minOrdinal: ordinal,
		maxOrdinal: ordinal,
		rollback:   rollback,
	}
}

// AsInt converts the stage index into a unique numeric integer.
func (s *stageKey) AsInt() int {
	// Assuming we never have plans with more than 1000 stages per-phase.
	return (int(s.phase) * 1000) + s.minOrdinal
}

// String implements fmt.Stringer
func (s *stageKey) String() string {
	if s.minOrdinal == s.maxOrdinal {
		return fmt.Sprintf("(phase = %s stageOrdinal=%d)",
			s.phase, s.minOrdinal)
	}
	return fmt.Sprintf("(phase = %s stageMinOrdinal=%d stageMaxOrdinal=%d),",
		s.phase, s.minOrdinal, s.maxOrdinal)
}

// IsEmpty detects if a stage key is empty.
func (s *stageKey) IsEmpty() bool {
	return s.phase == 0
}

type stageKeyEntry struct {
	stageKey
	stmt *stageExecStmt
}

// stageExecStmtMap maps statements that should be executed based on a given
// stage. This function also tracks output that should be used for rewrites.
type stageExecStmtMap struct {
	entries    []stageKeyEntry
	usedMap    map[*stageExecStmt]struct{}
	rewriteMap map[string]*stageExecStmt
}

func makeStageExecStmtMap() *stageExecStmtMap {
	return &stageExecStmtMap{
		usedMap:    make(map[*stageExecStmt]struct{}),
		rewriteMap: make(map[string]*stageExecStmt),
	}
}

// getExecStmts gets the statements to be used for a given phase and range
// of stages.
func (m *stageExecStmtMap) getExecStmts(targetKey stageKey) []*stageExecStmt {
	var stmts []*stageExecStmt
	if targetKey.minOrdinal != targetKey.maxOrdinal {
		panic(fmt.Sprintf("only a single ordinal key can be looked up %v ",
			targetKey))
	}
	for _, key := range m.entries {
		if key.stageKey.phase == targetKey.phase &&
			key.stageKey.rollback == targetKey.rollback &&
			targetKey.minOrdinal >= key.stageKey.minOrdinal &&
			targetKey.minOrdinal <= key.stageKey.maxOrdinal {
			stmts = append(stmts, key.stmt)
		}
	}
	return stmts
}

// AssertMapIsUsed asserts that all injected DML statements injected
// at various stages.
func (m *stageExecStmtMap) AssertMapIsUsed(t *testing.T) {
	// If there is any rollback error, then not all stages will be used.
	for _, e := range m.entries {
		if e.stmt.schemaChangeErrorRegexRollback != nil {
			return
		}
	}
	if len(m.entries) != len(m.usedMap) {
		for _, entry := range m.entries {
			if _, ok := m.usedMap[entry.stmt]; !ok {
				t.Logf("Missing stage: %v of type %d", entry.stageKey, entry.stmt.execType)
			}
		}
	}
	require.Equal(t, len(m.usedMap), len(m.entries), "All declared entries was not used")
}

// GetInjectionRanges gets a set of ranges that should have DML statements injected.
// This function will return the set of ranges where we will generate spans of
// statements that can be executed without any errors, until the first
// schema change error is hit.
// For example if we have the following DML statements concurrently with
// schema changes:
// 1) Statement A from phases 1:14 that will fail.
// 2) Statement B from phases 15:16 where the schema change will fail.
// We are going to generate the following runs of statements to inject:
//  1. 1:14 with statement A
//  2. 15:15 with statement B
//  3. 16:16 with statement B
//
// For each run the original DDL (schema change will be executed) with the
// DML statements from the given ranges.
func (m *stageExecStmtMap) GetInjectionRanges(
	totalPostCommit int, totalPostCommitNonRevertible int,
) []stageKey {
	var start stageKey
	var end stageKey
	var result []stageKey

	// First split any ranges that have schema change errors to have their own
	// entries. i.e. If stages 1 to N will generate schema change errors due to
	// some statement, we need to have one entry for each one. Additionally, convert
	// any latest ordinal values, to actual values.
	var forcedSplitEntries []stageKeyEntry
	for _, key := range m.entries {
		if key.stmt.execType != stageExecuteStmt {
			continue
		}
		if key.maxOrdinal == stageKeyOrdinalLatest {
			switch key.phase {
			case scop.PostCommitPhase:
				key.maxOrdinal = totalPostCommit
			case scop.PostCommitNonRevertiblePhase:
				key.maxOrdinal = totalPostCommitNonRevertible
			default:
				panic("unknown phase type for latest")
			}
		}
		if !key.stmt.HasSchemaChangeError() && !key.rollback {
			forcedSplitEntries = append(forcedSplitEntries, key)
		} else if !key.rollback {
			for i := key.minOrdinal; i <= key.maxOrdinal; i++ {
				forcedSplitEntries = append(forcedSplitEntries,
					stageKeyEntry{
						stageKey: stageKey{
							minOrdinal: i,
							maxOrdinal: i,
							phase:      key.phase,
							rollback:   key.rollback,
						},
						stmt: key.stmt,
					},
				)
			}
		}
	}
	// Next loop over those split entries, and try and generate runs until we
	// hit a schema change error or until we run out of statements.
	for i, key := range forcedSplitEntries {
		addEntry := func() {
			keyRange := stageKey{
				minOrdinal: start.minOrdinal,
				maxOrdinal: end.maxOrdinal,
				phase:      end.phase,
			}
			result = append(result, keyRange)
		}
		if !key.stmt.HasSchemaChangeError() &&
			start.IsEmpty() {
			// If we see a schema change error, and no other statements were executed
			// earlier, then this is an entry on its own.
			start = key.stageKey
			end = key.stageKey
		} else if !start.IsEmpty() &&
			(key.stmt.HasSchemaChangeError() ||
				(key.phase != start.phase)) {
			// If we already have a start, and we either hit a schema change error
			// or separate phase, then we need to emit a new entry.
			setStart := true
			if (key.phase == start.phase &&
				!key.stmt.HasSchemaChangeError()) ||
				start.IsEmpty() {
				setStart = false
				end = key.stageKey
				if start.IsEmpty() {
					start = end
				}
			}
			addEntry()
			start, end = stageKey{}, stageKey{}
			if setStart {
				start = key.stageKey
				end = key.stageKey
				// If we are forced to emit an entry because of a phase change,
				// then the current entry may need to be added if a schemchange
				// error exists,
				if key.stmt.HasSchemaChangeError() {
					addEntry()
					start, end = stageKey{}, stageKey{}
				}
			}
		} else {
			end = key.stageKey
			// If the start is empty, then we had a schema change error on this
			// entry already, so just added it directly.
			if start.IsEmpty() {
				start = end
				addEntry()
				start, end = stageKey{}, stageKey{}
			}
		}
		// No matter what for the last entry always emit an entry
		// if a start and end were set.
		if i == len(forcedSplitEntries)-1 && !start.IsEmpty() {
			addEntry()
		}
	}
	return result
}

// ParseStageQuery parses a stage-query statement.
func (m *stageExecStmtMap) ParseStageQuery(t *testing.T, d *datadriven.TestData) {
	m.parseStageCommon(t, d, stageExecuteQuery)
}

// ParseStageExec parses a stage-exec statement.
func (m *stageExecStmtMap) ParseStageExec(t *testing.T, d *datadriven.TestData) {
	m.parseStageCommon(t, d, stageExecuteStmt)
}

// parseStageCommon common fields between stage-exec and stage-query, which
// support the following keys:
//   - phase - The phase in which this statement/query should be injected, of the
//     string scop.Phase. Note: PreCommitPhase with stage 1 can be used to
//     inject failures that will only happen for DML injection testing.
//   - stage / stageStart / stageEnd - The ordinal for the stage where this
//     statement should be injected. stageEnd accepts the special value
//     latest which will map to the highest observed stage.
//   - schemaChangeExecError a schema change execution error will be encountered
//     by injecting at this stage.
//   - schemaChangeExecErrorForRollback a schema change execution error that will
//     be encountered at a future stage, leading to a rollback.
//   - statements can refer to builtin variable names with a $
//   - $stageKey - A unique identifier for stages and phases
//   - $successfulStageCount - Number of stages of the that have been successfully
//     executed with injections
func (m *stageExecStmtMap) parseStageCommon(
	t *testing.T, d *datadriven.TestData, execType stageExecType,
) {
	var key stageKey
	var schemaChangeErrorRegex *regexp.Regexp
	var schemaChangeErrorRegexRollback *regexp.Regexp
	stmts := strings.Split(d.Input, ";")
	require.NotEmpty(t, stmts)
	// Remove any trailing empty lines.
	if stmts[len(stmts)-1] == "" {
		stmts = stmts[0 : len(stmts)-1]
	}
	for _, cmdArgs := range d.CmdArgs {
		switch cmdArgs.Key {
		case "phase":
			found := false
			for i := scop.EarliestPhase; i <= scop.LatestPhase; i++ {
				if cmdArgs.Vals[0] == i.String() {
					key.phase = i
					found = true
					break
				}
			}
			require.Truef(t, found, "invalid phase name %s", cmdArgs.Key)
			if !found {
				panic("phase not mapped")
			}
		case "stage":
			// Detect ranges, otherwise we are looking at single value.
			if strings.Contains(cmdArgs.Vals[0], ":") {
				rangeVals := strings.Split(cmdArgs.Vals[0], ":")
				key.minOrdinal = 1
				key.maxOrdinal = stageKeyOrdinalLatest
				if len(rangeVals) >= 1 && len(rangeVals[0]) > 0 {
					ordinal, err := strconv.Atoi(rangeVals[0])
					require.Greater(t, ordinal, 0, "minimum ordinal is zero")
					require.NoError(t, err)
					key.minOrdinal = ordinal
				}
				if len(rangeVals) == 2 && len(rangeVals[1]) > 0 {
					ordinal, err := strconv.Atoi(rangeVals[1])
					require.Greater(t, ordinal, 0, "minimum ordinal is zero")
					require.NoError(t, err)
					require.GreaterOrEqualf(t, key.maxOrdinal, key.minOrdinal, "ordinals range is invalid")
					key.maxOrdinal = ordinal
				}
			} else {
				ordinal, err := strconv.Atoi(cmdArgs.Vals[0])
				require.Greater(t, ordinal, 0, "minimum ordinal is zero")
				require.NoError(t, err)
				key.minOrdinal = ordinal
				key.maxOrdinal = ordinal
			}
		case "schemaChangeExecError":
			schemaChangeErrorRegex = regexp.MustCompile(strings.Join(cmdArgs.Vals, " "))
			require.Nil(t, schemaChangeErrorRegexRollback, "future and current stage errors cannot be set concurrently")
		case "schemaChangeExecErrorForRollback":
			schemaChangeErrorRegexRollback = regexp.MustCompile(strings.Join(cmdArgs.Vals, " "))
			require.Nil(t, schemaChangeErrorRegex, "rollback and current stage errors cannot be set concurrently")
		case "rollback":
			rollback, err := strconv.ParseBool(cmdArgs.Vals[0])
			require.NoError(t, err)
			key.rollback = rollback
		default:
			require.Failf(t, "unknown key encountered", "key was %s", cmdArgs.Key)
		}
	}
	entry := stageKeyEntry{
		stageKey: key,
		stmt: &stageExecStmt{
			execType:                       execType,
			stmts:                          stmts,
			observedOutput:                 "",
			expectedOutput:                 d.Expected,
			schemaChangeErrorRegex:         schemaChangeErrorRegex,
			schemaChangeErrorRegexRollback: schemaChangeErrorRegexRollback,
		},
	}
	m.entries = append(m.entries, entry)
	m.rewriteMap[d.Pos] = entry.stmt
}

// GetExpectedOutputForPos returns the expected output for a given line,
// in rewrite mode this is updated.
func (m *stageExecStmtMap) GetExpectedOutputForPos(pos string) string {
	return m.rewriteMap[pos].expectedOutput
}

type execInjectionCallback func(stage stageKey, runner *sqlutils.SQLRunner, successfulStageCount int) []*stageExecStmt

type stageExecVariables struct {
	successfulStageCount int
	stage                stageKey
}

// Exec executes the statements for given stage and validates the output.
func (e *stageExecStmt) Exec(
	t *testing.T, runner *sqlutils.SQLRunner, stageVariables *stageExecVariables, rewrite bool,
) {
	for idx, stmt := range e.stmts {
		// Skip empty statements.
		if len(stmt) == 0 {
			continue
		}
		// Bind any variables for the statement.
		boundSQL := os.Expand(stmt, func(s string) string {
			switch s {
			case "successfulStageCount":
				return strconv.Itoa(stageVariables.successfulStageCount)
			case "stageKey":
				return strconv.Itoa(stageVariables.stage.AsInt() * 1000)
			default:
				t.Fatalf("unknown variable name %s", s)
			}
			return ""
		})
		if e.execType == stageExecuteStmt {
			_, err := runner.DB.ExecContext(context.Background(), boundSQL)
			if (e.expectedOutput == "" ||
				idx != len(e.stmts)-1) && err != nil {
				if !rewrite {
					t.Fatalf("unexpected error executing query %v", err)
				}
				e.expectedOutput = err.Error()
			} else if err != nil {
				errorMatches := testutils.IsError(err, strings.TrimSuffix(e.expectedOutput, "\n"))
				if !errorMatches {
					if !rewrite {
						require.Truef(t,
							errorMatches,
							"unexpected error got: %v expected %v",
							err,
							e.expectedOutput)
					}
					e.expectedOutput = err.Error()
				}
			}
		} else {
			var expectedQueryResult [][]string
			for _, expectedRow := range strings.Split(
				strings.TrimSuffix(e.expectedOutput, "\n"),
				"\n") {
				expectRowArray := strings.Split(expectedRow, ",")
				expectedQueryResult = append(expectedQueryResult, expectRowArray)
			}
			results := runner.QueryStr(t, boundSQL)
			if !reflect.DeepEqual(results, expectedQueryResult) {
				if !rewrite {
					t.Fatalf("query '%s': expected:\n%v\ngot:\n%v\n",
						stmt, sqlutils.MatrixToStr(expectedQueryResult), sqlutils.MatrixToStr(results))
				}
				e.expectedOutput = sqlutils.MatrixToStr(results)
			}
		}
	}
}

// GetInjectionCallback gets call back that will inject statements based on a
// given stage.
func (m *stageExecStmtMap) GetInjectionCallback(
	t *testing.T, rewrite bool,
) (execInjectionCallback, error) {
	return func(stage stageKey, runner *sqlutils.SQLRunner, successfulStageCount int) []*stageExecStmt {
		execStmts := m.getExecStmts(stage)
		for _, execStmt := range execStmts {
			m.usedMap[execStmt] = struct{}{}
			execStmt.Exec(t, runner, &stageExecVariables{
				successfulStageCount: successfulStageCount,
				stage:                stage,
			}, rewrite)
		}
		return execStmts
	}, nil
}

// cumulativeTest is a foundational helper for building tests over the
// datadriven format used by this package. This style of test will call
// the passed function for each test directive in the file. The setup
// statements passed to the function will be all statements from all
// previous test and setup blocks combined.
// To support injection of statements and rewriting the cumulative function
// will go through the file twice using the data driven functions:
//
//  1. The first pass will collect all the stage-exec/stage-query statements,
//     setup commands. Then executing the test statement and setup statements
//     via the tf callback. The callback can if the rewrite is enabled update
//     the expected output inside stageExecStmtMap (see stageExecStmt.Exec).
//
//  2. The second pass will for any stage-exec/stage-query functions look up
//     the output using stageExecStmtMap.GetExpectedOutputForPos and return
//     it, only when rewrite is enabled.
func cumulativeTest(
	t *testing.T,
	relPath string,
	tf func(t *testing.T, path string, rewrite bool, setup, stmts []statements.Statement[tree.Statement], stageExecMap *stageExecStmtMap),
) {
	skip.UnderStress(t)
	skip.UnderRace(t)
	path := datapathutils.RewritableDataPath(t, relPath)
	var setup []statements.Statement[tree.Statement]
	stageExecMap := makeStageExecStmtMap()
	rewrite := false
	var testStmts statements.Statements
	var lines []string
	numTestStmts := 0

	// First pass collect stage-exec/stage-query/setup commands and execute them
	// test once the test command is encountered. Only a single test command is
	// allowed via an assertion which guarantees all others appear first.
	datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
		// Assert that only one test statement shows up and nothing can follow it
		// afterwards.
		require.Zero(t, numTestStmts, "only one test command per-test, "+
			"and it must be the last one.")
		switch d.Cmd {
		case "setup":
			stmts, err := parser.Parse(d.Input)
			setup = append(setup, stmts...)
			require.NoError(t, err)
			require.NotEmpty(t, stmts)
		// no-op
		case "stage-exec":
			// DML injected statements will only be executed on cumalative tests,
			// for end-to-end tests these are fully ignored.
			stageExecMap.ParseStageExec(t, d)
		case "stage-query":
			stageExecMap.ParseStageQuery(t, d)
		case "test":
			stmts, err := parser.Parse(d.Input)
			require.NoError(t, err)
			require.NotEmpty(t, stmts)
			testStmts = stmts
			for _, stmt := range stmts {
				lines = append(lines, stmt.SQL)
			}
			rewrite = d.Rewrite
			numTestStmts++
			tf(t, path, rewrite, setup, testStmts, stageExecMap)
		default:
			t.Fatalf("unknown command type %s", d.Cmd)
		}
		return d.Expected
	})
	// Run through and recover the observed output for statements.
	if rewrite {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			// Retrieve the generated output from the previous execution in the
			// rewrite mode of DML injection. We are going to the store the
			// observed output based on the line number and file names for the
			// stage-exec and stage-query commands.
			case "stage-exec":
				fallthrough
			case "stage-query":
				return stageExecMap.GetExpectedOutputForPos(d.Pos)
			}
			// cumlativeTest will rewrite only the stage-exec and stage-query commands,
			// all others are rewritten by the end-to-end tests.
			return d.Expected
		})
	}
}

// TODO(ajwerner): For all the non-rollback variants, we'd really actually
// like them to run over each of the rollback stages too.

// Rollback tests that the schema changer job rolls back properly.
// This data-driven test uses the same input as EndToEndSideEffects
// but ignores the expected output.
func Rollback(t *testing.T, relPath string, newCluster NewClusterFunc) {
	countRevertiblePostCommitStages := func(
		t *testing.T, setup, stmts []statements.Statement[tree.Statement],
	) (n int) {
		processPlanInPhase(
			t, newCluster, setup, stmts, scop.PostCommitPhase,
			func(p scplan.Plan) { n = len(p.StagesForCurrentPhase()) },
			func(db *gosql.DB) {},
		)
		return n
	}
	var testRollbackCase func(
		t *testing.T, path string, rewrite bool, setup, stmts []statements.Statement[tree.Statement], ord, n int,
	)
	testFunc := func(t *testing.T, path string, rewrite bool, setup, stmts []statements.Statement[tree.Statement], _ *stageExecStmtMap) {
		n := countRevertiblePostCommitStages(t, setup, stmts)
		if n == 0 {
			t.Logf("test case has no revertible post-commit stages, skipping...")
			return
		}
		t.Logf("test case has %d revertible post-commit stages", n)
		for i := 1; i <= n; i++ {
			if !t.Run(
				fmt.Sprintf("rollback stage %d of %d", i, n),
				func(t *testing.T) { testRollbackCase(t, path, rewrite, setup, stmts, i, n) },
			) {
				return
			}
		}
	}

	testRollbackCase = func(
		t *testing.T, path string, rewrite bool, setup, stmts []statements.Statement[tree.Statement], ord, n int,
	) {
		var numInjectedFailures uint32
		var numCheckedExplainInRollback uint32
		beforeStage := func(p scplan.Plan, stageIdx int) error {
			s := p.Stages[stageIdx]
			if atomic.LoadUint32(&numInjectedFailures) > 0 {
				// At this point, if a failure has already been injected, any stage
				// should be non-revertible.
				require.Equal(t, scop.PostCommitNonRevertiblePhase, s.Phase)
				// EXPLAIN the rollback plan as early as possible.
				if atomic.LoadUint32(&numCheckedExplainInRollback) > 0 {
					return nil
				}
				atomic.AddUint32(&numCheckedExplainInRollback, 1)
				fileNameSuffix := fmt.Sprintf(".rollback_%d_of_%d", ord, n)
				explainedStmt := fmt.Sprintf("rollback at post-commit stage %d of %d", ord, n)
				const inRollback = true
				checkExplainDiagrams(t, path, setup, stmts, explainedStmt, fileNameSuffix, p.CurrentState, inRollback, rewrite)
				return nil
			}
			if s.Phase == scop.PostCommitPhase && s.Ordinal == ord {
				atomic.AddUint32(&numInjectedFailures, 1)
				return errors.Errorf("boom %d", ord)
			}
			return nil
		}

		_, db, cleanup := newCluster(t, &scexec.TestingKnobs{
			BeforeStage: beforeStage,
			OnPostCommitPlanError: func(err error) error {
				panic(fmt.Sprintf("%+v", err))
			},
			OnPostCommitError: func(p scplan.Plan, stageIdx int, err error) error {
				if strings.Contains(err.Error(), "boom") {
					return err
				}
				panic(fmt.Sprintf("%+v", err))
			},
		})
		defer cleanup()

		tdb := sqlutils.MakeSQLRunner(db)
		var before [][]string
		beforeFunc := func() {
			before = tdb.QueryStr(t, fetchDescriptorStateQuery)
		}
		onError := func(err error) error {
			// If the statement execution failed, then we expect to end up in the same
			// state as when we started.
			require.Equal(t, before, tdb.QueryStr(t, fetchDescriptorStateQuery))
			return err
		}
		err := executeSchemaChangeTxn(
			context.Background(), t, setup, stmts, db, beforeFunc, nil, onError,
		)
		if atomic.LoadUint32(&numInjectedFailures) == 0 {
			require.NoError(t, err)
		} else {
			require.Regexp(t, fmt.Sprintf("boom %d", ord), err)
			require.NotZero(t, atomic.LoadUint32(&numCheckedExplainInRollback))
		}
	}
	cumulativeTest(t, relPath, testFunc)
}

// fetchDescriptorStateQuery returns the CREATE statements for all descriptors
// minus any COMMENT ON statements because these aren't consistently backed up.
const fetchDescriptorStateQuery = `
SELECT
	split_part(create_statement, ';', 1) AS create_statement
FROM
	( 
		SELECT descriptor_id, create_statement FROM crdb_internal.create_schema_statements
		UNION ALL SELECT descriptor_id, create_statement FROM crdb_internal.create_statements
		UNION ALL SELECT descriptor_id, create_statement FROM crdb_internal.create_type_statements
    UNION ALL SELECT function_id as descriptor_id, create_statement FROM crdb_internal.create_function_statements
	)
WHERE descriptor_id IN (SELECT id FROM system.namespace)
ORDER BY
	create_statement;`

// Pause tests that the schema changer can handle being paused and resumed
// correctly. This data-driven test uses the same input as EndToEndSideEffects
// but ignores the expected output.
func Pause(t *testing.T, relPath string, newCluster NewClusterFunc) {
	var postCommit, nonRevertible int
	countStages := func(
		t *testing.T, setup, stmts []statements.Statement[tree.Statement],
	) {
		processPlanInPhase(t, newCluster, setup, stmts, scop.PostCommitPhase, func(
			p scplan.Plan,
		) {
			postCommit = len(p.StagesForCurrentPhase())
			nonRevertible = len(p.Stages) - postCommit
		}, nil)
	}
	var testPauseCase func(
		t *testing.T, setup, stmts []statements.Statement[tree.Statement], ord int,
	)
	testFunc := func(t *testing.T, _ string, _ bool, setup, stmts []statements.Statement[tree.Statement], _ *stageExecStmtMap) {
		countStages(t, setup, stmts)
		n := postCommit + nonRevertible
		if n == 0 {
			t.Logf("test case has no revertible post-commit stages, skipping...")
			return
		}
		t.Logf("test case has %d revertible post-commit stages", n)
		for i := 1; i <= n; i++ {
			if !t.Run(
				fmt.Sprintf("pause stage %d of %d", i, n),
				func(t *testing.T) { testPauseCase(t, setup, stmts, i) },
			) {
				return
			}
		}

		// Need to reset "postCommit" and "nonRevertible" before testFunc being
		// called for next test. The reason is that if a test did not generate any
		// post commit phase, the "countStates()" function won't take any effect
		// since "processPlanInPhase()" only calls the input "processFunc" for the
		// specified phase. So that such test would inherit "postCommit" and
		// "nonRevertible" from a previous test which generates post commit phase
		// stages.
		postCommit = 0
		nonRevertible = 0
	}
	testPauseCase = func(t *testing.T, setup, stmts []statements.Statement[tree.Statement], ord int) {
		var numInjectedFailures uint32
		// TODO(ajwerner): It'd be nice to assert something about the number of
		// remaining stages before the pause and then after. It's not totally
		// trivial, as we don't checkpoint during non-mutation stages, so we'd
		// need to look back and find the last mutation phase.
		_, db, cleanup := newCluster(t, &scexec.TestingKnobs{
			BeforeStage: func(p scplan.Plan, stageIdx int) error {
				if atomic.LoadUint32(&numInjectedFailures) > 0 {
					return nil
				}
				s := p.Stages[stageIdx]
				if s.Phase == scop.PostCommitPhase && s.Ordinal == ord ||
					s.Phase == scop.PostCommitNonRevertiblePhase && s.Ordinal+postCommit == ord {
					atomic.AddUint32(&numInjectedFailures, 1)
					return jobs.MarkPauseRequestError(errors.Errorf("boom %d", ord))
				}
				return nil
			},
		})
		defer cleanup()
		tdb := sqlutils.MakeSQLRunner(db)
		onError := func(err error) error {
			// Check that it's a pause error, with a job.
			// Resume the job and wait for the job.
			re := regexp.MustCompile(
				`job (\d+) was paused before it completed with reason: boom (\d+)`,
			)
			match := re.FindStringSubmatch(err.Error())
			require.NotNil(t, match)
			idx, err := strconv.Atoi(match[2])
			require.NoError(t, err)
			require.Equal(t, ord, idx)
			jobID, err := strconv.Atoi(match[1])
			require.NoError(t, err)
			t.Logf("found job %d", jobID)
			tdb.Exec(t, "RESUME JOB $1", jobID)
			tdb.CheckQueryResultsRetry(t, "SELECT status, error FROM [SHOW JOB "+match[1]+"]", [][]string{
				{"succeeded", ""},
			})
			return nil
		}
		require.NoError(t, executeSchemaChangeTxn(
			context.Background(), t, setup, stmts, db, nil, nil, onError,
		))
		require.Equal(t, uint32(1), atomic.LoadUint32(&numInjectedFailures))
	}
	cumulativeTest(t, relPath, testFunc)
}

// ExecuteWithDMLInjection tests that the schema changer behaviour is sane
// once we start injecting DML statements into execution.
func ExecuteWithDMLInjection(t *testing.T, relPath string, newCluster NewClusterFunc) {
	jobErrorMutex := syncutil.Mutex{}
	var testDMLInjectionCase func(
		t *testing.T, setup, stmts []statements.Statement[tree.Statement], key stageKey,
	)
	var injectionFunc execInjectionCallback
	testFunc := func(t *testing.T, _ string, rewrite bool, setup, stmts []statements.Statement[tree.Statement], execMap *stageExecStmtMap) {
		var postCommit, nonRevertible int
		processPlanInPhase(t, newCluster, setup, stmts, scop.PostCommitPhase, func(
			p scplan.Plan,
		) {
			postCommit = len(p.StagesForCurrentPhase())
			nonRevertible = len(p.Stages) - postCommit
		}, nil)

		injectionFunc, _ = execMap.GetInjectionCallback(t, rewrite)
		injectionRanges := execMap.GetInjectionRanges(postCommit, nonRevertible)
		defer execMap.AssertMapIsUsed(t)
		for _, injection := range injectionRanges {
			if !t.Run(
				fmt.Sprintf("injection stage %v", injection),
				func(t *testing.T) { testDMLInjectionCase(t, setup, stmts, injection) },
			) {
				return
			}
		}
	}
	testDMLInjectionCase = func(t *testing.T, setup, stmts []statements.Statement[tree.Statement], injection stageKey) {
		var schemaChangeErrorRegex *regexp.Regexp
		var lastRollbackStageKey *stageKey
		usedStages := make(map[int]struct{})
		successfulStages := 0
		var tdb *sqlutils.SQLRunner
		_, db, cleanup := newCluster(t, &scexec.TestingKnobs{
			BeforeStage: func(p scplan.Plan, stageIdx int) error {
				s := p.Stages[stageIdx]
				if (injection.phase == p.Stages[stageIdx].Phase &&
					p.Stages[stageIdx].Ordinal >= injection.minOrdinal &&
					p.Stages[stageIdx].Ordinal <= injection.maxOrdinal) ||
					(p.InRollback || p.CurrentState.InRollback) || /* Rollbacks are always injected */
					(p.Stages[stageIdx].Phase == scop.PreCommitPhase) {
					jobErrorMutex.Lock()
					defer jobErrorMutex.Unlock()
					key := makeStageKey(s.Phase, s.Ordinal, p.InRollback || p.CurrentState.InRollback)
					if _, ok := usedStages[key.AsInt()]; !ok {
						// Rollbacks don't count towards the successful count
						if !p.InRollback && !p.CurrentState.InRollback &&
							p.Stages[stageIdx].Phase != scop.PreCommitPhase {
							successfulStages++
						} else {
							lastRollbackStageKey = &key
						}
						injectStmts := injectionFunc(key, tdb, successfulStages)
						regexSetOnce := false
						for _, injectStmt := range injectStmts {
							if injectStmt != nil &&
								injectStmt.HasAnySchemaChangeError() != nil {
								require.Falsef(t, regexSetOnce, "multiple statements are expecting errors in the same phase.")
								schemaChangeErrorRegex = injectStmt.HasAnySchemaChangeError()
								regexSetOnce = true
								t.Logf("Expecting schema change error: %v", schemaChangeErrorRegex)
							}
						}
						usedStages[key.AsInt()] = struct{}{}
						t.Logf("Completed stage: %v", key)
					} else {
						t.Logf("Retrying stage: %v", key)
					}

				}
				return nil
			},
		})
		defer cleanup()
		tdb = sqlutils.MakeSQLRunner(db)
		errorDetected := false
		onError := func(err error) error {
			if schemaChangeErrorRegex != nil &&
				schemaChangeErrorRegex.MatchString(err.Error()) {
				errorDetected = true
				return nil
			}
			return err
		}
		require.NoError(t, executeSchemaChangeTxn(
			context.Background(), t, setup, stmts, db, nil, nil, onError,
		))
		// Re-inject anything from the rollback once the job transaction
		// commits, this enforces any sanity checks one last time in
		// the final descriptor state.
		if lastRollbackStageKey != nil {
			injectionFunc(*lastRollbackStageKey, tdb, successfulStages)
		}
		require.Equal(t, errorDetected, schemaChangeErrorRegex != nil)
	}
	cumulativeTest(t, relPath, testFunc)
}

// Used for saving corpus information in TestGenerateCorpus
var corpusPath string

func init() {
	flag.StringVar(&corpusPath, "declarative-corpus", "", "path to the corpus file")
}

// GenerateSchemaChangeCorpus executes each post commit stage of a given set of
// statements and writes them into a corpus file. This file can be later used to
// validate mixed version / forward compatibility.
func GenerateSchemaChangeCorpus(t *testing.T, path string, newCluster NewClusterFunc) {
	if corpusPath == "" {
		skip.IgnoreLintf(t, "requires declarative-corpus path parameter")
	}
	cc, err := corpus.NewCorpusCollector(corpusPath)
	if err != nil {
		t.Fatalf("failed to create collection %v", err)
	}
	defer func() {
		err := cc.UpdateCorpus()
		if err != nil {
			panic(err)
		}
	}()
	var testCorpusCollect func(
		t *testing.T, setup, stmts []statements.Statement[tree.Statement],
	)
	testFunc := func(t *testing.T, path string, rewrite bool, setup, stmts []statements.Statement[tree.Statement], _ *stageExecStmtMap) {
		if !t.Run("starting",
			func(t *testing.T) { testCorpusCollect(t, setup, stmts) },
		) {
			return
		}
	}
	testCorpusCollect = func(t *testing.T, setup, stmts []statements.Statement[tree.Statement]) {
		// If any of the statements are not supported, then skip over this
		// file for the corpus.
		for _, stmt := range stmts {
			if !scbuild.IsFullySupportedWithFalsePositive(stmt.AST, clusterversion.TestingClusterVersion) {
				return
			}
		}
		_, db, cleanup := newCluster(t, &scexec.TestingKnobs{
			BeforeStage: cc.GetBeforeStage("EndToEndCorpus", t),
		})

		defer cleanup()
		require.NoError(t, executeSchemaChangeTxn(
			context.Background(), t, setup, stmts, db, nil, nil, nil,
		))
	}
	cumulativeTest(t, path, testFunc)
}

// runAllBackups runs all the backup tests, disabling the random skipping.
var runAllBackups = flag.Bool(
	"run-all-backups", false,
	"if true, run all backups instead of a random subset",
)

// runAllBackups runs all the mixed version tests, disabling the random skipping.
var runAllMixedTests = flag.Bool(
	"run-all-mixed", false,
	"if true, run all mixed version tests instead of a random subset",
)

// Backup tests that the schema changer can handle being backed up and
// restored correctly. This data-driven test uses the same input as
// EndToEndSideEffects but ignores the expected output. Note that the
// cluster constructor needs to provide a cluster with CCL BACKUP/RESTORE
// functionality enabled.
func Backup(t *testing.T, path string, newCluster NewClusterFunc) {
	var after [][]string // CREATE_STATEMENT for all descriptors after finishing `stmts` in each test case.
	var dbName string
	r, _ := randutil.NewTestRand()
	const runRate = .5

	maybeRandomlySkip := func(t *testing.T) {
		if !*runAllBackups && r.Float64() >= runRate {
			skip.IgnoreLint(t, "skipping due to randomness")
		}
	}

	// A function that executes `setup` first and then count the number of
	// postCommit and postCommitNonRevertible stages for executing `stmts`.
	// It also initializes `after` and `dbName` here.
	countStages := func(
		t *testing.T, setup, stmts []statements.Statement[tree.Statement],
	) (postCommit, nonRevertible int) {
		var pl scplan.Plan
		processPlanInPhase(t, newCluster, setup, stmts, scop.PostCommitPhase,
			func(p scplan.Plan) {
				pl = p
				postCommit = len(p.StagesForCurrentPhase())
				nonRevertible = len(p.Stages) - postCommit
			}, func(db *gosql.DB) {
				tdb := sqlutils.MakeSQLRunner(db)
				var ok bool
				dbName, ok = maybeGetDatabaseForIDs(t, tdb, screl.AllTargetDescIDs(pl.TargetState))
				if ok {
					tdb.Exec(t, fmt.Sprintf("USE %q", dbName))
				}
				after = tdb.QueryStr(t, fetchDescriptorStateQuery)
			})
		return postCommit, nonRevertible
	}

	// A function that takes backup at `ord`-th stage while executing `stmts` after
	// finishing `setup`. It also takes `ord` backups at each of the preceding stage
	// if it's a revertible stage.
	// It then restores the backup(s) in various "flavors" (see
	// comment below for details) and expect the restore to finish the schema change job
	// as if the backup/restore had never happened.
	testBackupRestoreCase := func(
		t *testing.T, setup, stmts []statements.Statement[tree.Statement], ord int,
	) {
		type stage struct {
			p        scplan.Plan
			stageIdx int
			resume   chan error
		}

		stageChan := make(chan stage)
		var closedStageChan bool // mark when stageChan is closed in the callback
		ctx, cancel := context.WithCancel(context.Background())
		_, db, cleanup := newCluster(t, &scexec.TestingKnobs{
			BeforeStage: func(p scplan.Plan, stageIdx int) error {

				// If the plan has no post-commit stages, we'll close the
				// stageChan eagerly.
				if p.Stages[len(p.Stages)-1].Phase < scop.PostCommitPhase {

					// The other test goroutine will set stageChan to nil later on.
					// We only want to close it once, and then we don't want to look
					// at it again. Avoid the racy access to stageChan by consulting
					// the bool.
					if !closedStageChan {
						closedStageChan = true
						close(stageChan)
					}
					return nil
				}
				if p.Stages[stageIdx].Phase < scop.PostCommitPhase {
					return nil
				}
				if stageChan != nil {
					s := stage{p: p, stageIdx: stageIdx, resume: make(chan error)}
					select {
					case stageChan <- s:
					case <-ctx.Done():
						return ctx.Err()
					}
					select {
					case err := <-s.resume:
						return err
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			},
		})

		// Start with full database backup/restore.
		defer cleanup()
		defer cancel()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		tdb := sqlutils.MakeSQLRunner(conn)
		// TODO(postamar): remove this threshold bump
		//   This requires the test cases to be properly parallelized.
		tdb.SucceedsSoonDuration = testutils.RaceSucceedsSoonDuration
		tdb.Exec(t, "create database backups")
		var g errgroup.Group
		var before [][]string
		beforeFunc := func() {
			tdb.Exec(t, fmt.Sprintf("USE %q", dbName))
			before = tdb.QueryStr(t, fetchDescriptorStateQuery)
		}
		g.Go(func() error {
			return executeSchemaChangeTxn(
				context.Background(), t, setup, stmts, db, beforeFunc, nil, nil,
			)
		})
		type backup struct {
			name       string
			isRollback bool
			url        string
			s          stage
		}
		var backups []backup
		var done bool
		var rollbackStage int
		type stageKey struct {
			stage    int
			rollback bool
		}
		completedStages := make(map[stageKey]struct{})
		for i := 0; !done; i++ {
			// We want to let the stages up to ord continue unscathed. Then, we'll
			// start taking backups at ord. If ord corresponds to a revertible
			// stage, we'll inject an error, forcing the schema change to revert.
			// At each subsequent stage, we also take a backup. At the very end,
			// we'll have one backup where things should succeed and N backups
			// where we're reverting. In each case, we want to have the end state
			// of the restored set of descriptors match what we have in the original
			// cluster.
			//
			// Lastly, we'll hit an ord corresponding to the first non-revertible
			// stage. At this point, we'll take a backup for each non-revertible
			// stage and confirm that restoring them and letting the jobs run
			// leaves the database in the right state.
			s := <-stageChan
			// If there is no post-commit stages. Just consider it as done.
			if s.p.Stages == nil {
				done = true
				stageChan = nil
				break
			}
			// Move the index backwards if we see the same stage repeat due to a txn
			// retry error for example.
			stage := stageKey{
				stage:    s.stageIdx,
				rollback: s.p.InRollback,
			}
			if _, ok := completedStages[stage]; ok {
				i--
				if stage.rollback {
					rollbackStage--
				}
			}
			completedStages[stage] = struct{}{}
			shouldFail := ord == i &&
				s.p.Stages[s.stageIdx].Phase != scop.PostCommitNonRevertiblePhase &&
				!s.p.InRollback
			done = len(s.p.Stages) == s.stageIdx+1 && !shouldFail
			t.Logf("stage %d/%d in %v (rollback=%v) %d %q %v",
				s.stageIdx+1, len(s.p.Stages), s.p.Stages[s.stageIdx].Phase, s.p.InRollback, ord, dbName, done)

			// If the database has been dropped, there is nothing for
			// us to do here.
			var exists bool
			tdb.QueryRow(t,
				`SELECT count(*) > 0 FROM system.namespace WHERE "parentID" = 0 AND name = $1`,
				dbName).Scan(&exists)
			if !exists || (i < ord && !done) {
				close(s.resume)
				continue
			}

			// This test assumes that all the descriptors being modified in the
			// transaction are in the same database.
			//
			// TODO(ajwerner): Deal with trying to restore just some of the tables.
			backupURL := fmt.Sprintf("userfile://backups.public.userfiles_$user/data%d", i)
			tdb.Exec(t, fmt.Sprintf(
				"BACKUP DATABASE %s INTO '%s'", dbName, backupURL))
			backups = append(backups, backup{
				name:       dbName,
				isRollback: rollbackStage > 0,
				url:        backupURL,
				s:          s,
			})

			if s.p.InRollback {
				rollbackStage++
			}
			if done {
				t.Logf("reached final stage, waiting for completion")
				stageChan = nil // allow the restored jobs to proceed
			}
			if shouldFail {
				s.resume <- errors.Newf("boom %d", i)
			} else {
				close(s.resume)
			}
		}
		if err := g.Wait(); rollbackStage > 0 {
			require.Regexp(t, fmt.Sprintf("boom %d", ord), err)
		} else {
			require.NoError(t, err)
		}

		t.Logf("finished")

		for i, b := range backups {
			// For each backup, we restore it in three flavors.
			// 1. RESTORE DATABASE
			// 2. RESTORE DATABASE WITH schema_only
			// 3. RESTORE TABLE tbl1, tbl2, ..., tblN
			// We then assert that the restored database should correctly finish
			// the ongoing schema change job when the backup was taken, and
			// reaches the expected state as if the back/restore had not happened at all.
			// Skip a backup randomly.
			type backupConsumptionFlavor struct {
				name         string
				restoreSetup []string
				restoreQuery string
			}
			flavors := []backupConsumptionFlavor{
				{
					name: "restore database",
					restoreSetup: []string{
						fmt.Sprintf("DROP DATABASE IF EXISTS %q CASCADE", dbName),
						"SET use_declarative_schema_changer = 'off'",
					},
					restoreQuery: fmt.Sprintf("RESTORE DATABASE %s FROM LATEST IN '%s'", dbName, b.url),
				},
				{
					name: "restore database with schema-only",
					restoreSetup: []string{
						fmt.Sprintf("DROP DATABASE IF EXISTS %q CASCADE", dbName),
						"SET use_declarative_schema_changer = 'off'",
					},
					restoreQuery: fmt.Sprintf("RESTORE DATABASE %s FROM LATEST IN '%s' with schema_only", dbName, b.url),
				},
			}

			// For the third flavor, we restore all tables in the backup.
			// Skip it if there is no tables.
			rows := tdb.QueryStr(t, `
			SELECT parent_schema_name, object_name
			FROM [SHOW BACKUP FROM LATEST IN $1]
			WHERE database_name = $2 AND object_type = 'table'`, b.url, dbName)
			var tablesToRestore []string
			for _, row := range rows {
				tablesToRestore = append(tablesToRestore, fmt.Sprintf("%s.%s.%s", dbName, row[0], row[1]))
			}

			if len(tablesToRestore) > 0 {
				flavors = append(flavors, backupConsumptionFlavor{
					name: "restore all tables in database",
					restoreSetup: []string{
						fmt.Sprintf("DROP DATABASE IF EXISTS %q CASCADE", dbName),
						fmt.Sprintf("CREATE DATABASE %q", dbName),
						"SET use_declarative_schema_changer = 'off'",
					},
					restoreQuery: fmt.Sprintf("RESTORE TABLE %s FROM LATEST IN '%s' WITH skip_missing_sequences, skip_missing_udfs",
						strings.Join(tablesToRestore, ","), b.url),
				})
			}

			// TODO (xiang): Add here the fourth flavor that restores
			// only a subset, maybe randomly chosen, of all tables with
			// `RESTORE TABLE`. Currently, it's blocked by issue #87518.
			// We will need to change what the expected output will be
			// in this case, since it will no longer be simply `before`
			// and `after`.

			containsUDF := func(expr tree.Expr) (bool, error) {
				var foundUDF bool
				_, err := tree.SimpleVisit(expr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
					if fe, ok := expr.(*tree.FuncExpr); ok {
						ref := fe.Func.FunctionReference.(*tree.UnresolvedName)
						fn, err := ref.ToFunctionName()
						require.NoError(t, err)
						fd, err := tree.GetBuiltinFuncDefinition(fn, &sessiondata.DefaultSearchPath)
						require.NoError(t, err)
						if fd == nil {
							foundUDF = true
						}
					}
					return true, expr, nil
				})
				if err != nil {
					return false, err
				}
				return foundUDF, nil
			}

			removeDefsDependOnUDFs := func(n *tree.CreateTable) {
				var newDefs tree.TableDefs
				for _, def := range n.Defs {
					var usesUDF bool
					switch d := def.(type) {
					case *tree.CheckConstraintTableDef:
						usesUDF, err = containsUDF(d.Expr)
						require.NoError(t, err)
						if usesUDF {
							continue
						}
					case *tree.ColumnTableDef:
						if d.DefaultExpr.Expr != nil {
							usesUDF, err = containsUDF(d.DefaultExpr.Expr)
							require.NoError(t, err)
							if usesUDF {
								d.DefaultExpr = struct {
									Expr           tree.Expr
									ConstraintName tree.Name
								}{}
							}
						}
					}
					newDefs = append(newDefs, def)
				}
				n.Defs = newDefs
			}

			for _, flavor := range flavors {
				t.Run(flavor.name, func(t *testing.T) {
					maybeRandomlySkip(t)
					t.Logf("testing backup %d (rollback=%v)", i, b.isRollback)
					tdb.ExecMultiple(t, flavor.restoreSetup...)
					tdb.Exec(t, flavor.restoreQuery)
					tdb.Exec(t, fmt.Sprintf("USE %q", dbName))
					waitForSchemaChangesToFinish(t, tdb)
					afterRestore := tdb.QueryStr(t, fetchDescriptorStateQuery)

					if flavor.name != "restore all tables in database" {
						if b.isRollback {
							require.Equal(t, before, afterRestore)
						} else {
							require.Equal(t, after, afterRestore)
						}
					} else {
						// If the flavor restore only tables, there can be missing UDF
						// dependencies which cause check constraints or expressions
						// dropped through RESTORE due to missing UDFs. We need to remove
						// those kind of table elements from original AST.
						var expected [][]string
						if b.isRollback {
							expected = before
						} else {
							expected = after
						}
						require.Equal(t, len(expected), len(afterRestore))
						for i := range expected {
							require.Equal(t, 1, len(expected[i]))
							require.Equal(t, 1, len(afterRestore[i]))
							afterNode, _ := parser.ParseOne(expected[i][0])
							afterRestoreNode, _ := parser.ParseOne(afterRestore[i][0])
							if _, ok := afterNode.AST.(*tree.CreateTable); !ok {
								require.Equal(t, expected[i][0], afterRestore[i][0])
							} else {
								createTbl := afterNode.AST.(*tree.CreateTable)
								removeDefsDependOnUDFs(createTbl)
								createTblRestored := afterRestoreNode.AST.(*tree.CreateTable)
								require.Equal(t, tree.AsString(createTbl), tree.AsString(createTblRestored))
							}
						}
					}
					// Hack to deal with corrupt userfiles tables due to #76764.
					const validateQuery = `
SELECT * FROM crdb_internal.invalid_objects WHERE database_name != 'backups'
`
					tdb.CheckQueryResults(t, validateQuery, [][]string{})
					tdb.Exec(t, fmt.Sprintf("DROP DATABASE %q CASCADE", dbName))
					tdb.Exec(t, "USE backups")
					tdb.CheckQueryResults(t, validateQuery, [][]string{})
				})
			}
		}
	}

	testFunc := func(t *testing.T, _ string, _ bool, setup, stmts []statements.Statement[tree.Statement], _ *stageExecStmtMap) {
		postCommit, nonRevertible := countStages(t, setup, stmts)
		n := postCommit + nonRevertible
		t.Logf(
			"test case has %d revertible post-commit stages and %d non-revertible"+
				" post-commit stages", postCommit, nonRevertible,
		)
		for i := 0; i <= n; i++ {
			if !t.Run(
				fmt.Sprintf("backup/restore stage %d of %d", i, n),
				func(t *testing.T) {
					maybeRandomlySkip(t)
					testBackupRestoreCase(t, setup, stmts, i)
				},
			) {
				return
			}
		}
	}

	cumulativeTest(t, path, testFunc)
}

func maybeGetDatabaseForIDs(
	t *testing.T, tdb *sqlutils.SQLRunner, ids catalog.DescriptorIDSet,
) (dbName string, exists bool) {
	err := tdb.DB.QueryRowContext(context.Background(), `
SELECT name
  FROM system.namespace
 WHERE id
       IN (
            SELECT DISTINCT
                   COALESCE(
                    d->'database'->>'id',
                    d->'schema'->>'parentId',
                    d->'type'->>'parentId',
                    d->'table'->>'parentId'
                   )::INT8
              FROM (
                    SELECT crdb_internal.pb_to_json('desc', descriptor) AS d
                      FROM system.descriptor
                     WHERE id IN (SELECT * FROM ROWS FROM (unnest($1::INT8[])))
                   )
        )
`, pq.Array(ids.Ordered())).
		Scan(&dbName)
	if errors.Is(err, gosql.ErrNoRows) {
		return "", false
	}

	require.NoError(t, err)
	return dbName, true
}

// processPlanInPhase will call processFunc with the plan as of the first
// stage in the requested phase. The function will be called at most once.
func processPlanInPhase(
	t *testing.T,
	newCluster NewClusterFunc,
	setup, stmts []statements.Statement[tree.Statement],
	phaseToProcess scop.Phase,
	processFunc func(p scplan.Plan),
	after func(db *gosql.DB),
) {
	var processOnce sync.Once
	_, db, cleanup := newCluster(t, &scexec.TestingKnobs{
		BeforeStage: func(p scplan.Plan, _ int) error {
			if p.Params.ExecutionPhase == phaseToProcess {
				processOnce.Do(func() { processFunc(p) })
			}
			return nil
		},
	})
	defer cleanup()
	require.NoError(t, executeSchemaChangeTxn(
		context.Background(), t, setup, stmts, db, nil, nil, nil,
	))
	if after != nil {
		after(db)
	}
}

// executeSchemaChangeTxn spins up a test cluster, executes the setup
// statements with the legacy schema changer, then executes the test statements
// with the declarative schema changer.
func executeSchemaChangeTxn(
	ctx context.Context,
	t *testing.T,
	setup []statements.Statement[tree.Statement],
	stmts []statements.Statement[tree.Statement],
	db *gosql.DB,
	before func(),
	txnStartCallback func(),
	onError func(err error) error,
) (err error) {

	tdb := sqlutils.MakeSQLRunner(db)

	// Execute the setup statements with the legacy schema changer so that the
	// declarative schema changer testing knobs don't get used.
	tdb.Exec(t, "SET use_declarative_schema_changer = 'off'")
	for _, stmt := range setup {
		_, err := db.Exec(stmt.SQL)
		if err != nil {
			// nolint:errcmp
			switch errT := err.(type) {
			case *pq.Error:
				if string(errT.Code) == pgcode.FeatureNotSupported.String() {
					skip.IgnoreLint(t, "skipping due to unimplemented feature in old cluster version.")
				}
			}
			return err
		}
	}
	waitForSchemaChangesToFinish(t, tdb)
	if before != nil {
		before()
	}

	// Execute the tested statements with the declarative schema changer and fail
	// the test if it all takes too long. This prevents the test suite from
	// hanging when a regression is introduced.
	{
		c := make(chan error, 1)
		go func() {
			conn, err := db.Conn(ctx)
			if err != nil {
				c <- err
				return
			}
			defer func() { _ = conn.Close() }()
			c <- crdb.Execute(func() (err error) {
				_, err = conn.ExecContext(
					ctx, "SET use_declarative_schema_changer = 'unsafe_always'",
				)
				if err != nil {
					return err
				}
				var tx *gosql.Tx
				tx, err = conn.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				defer func() {
					if err != nil {
						err = errors.WithSecondaryError(err, tx.Rollback())
					} else {
						err = tx.Commit()
					}
				}()
				if txnStartCallback != nil {
					txnStartCallback()
				}
				for _, stmt := range stmts {
					if _, err := tx.Exec(stmt.SQL); err != nil {
						return err
					}
				}
				return nil
			})
		}()
		testutils.SucceedsSoon(t, func() error {
			select {
			case e := <-c:
				err = e
				return nil
			default:
				return errors.New("waiting for statements to execute")
			}
		})
	}

	if err != nil && onError != nil {
		err = onError(err)
	}
	if err != nil {
		return err
	}

	// Ensure we're really done here.
	waitForSchemaChangesToFinish(t, tdb)
	return nil
}

// ValidateMixedVersionElements executes each phase within a mixed version
// state for the cluster.
func ValidateMixedVersionElements(t *testing.T, path string, newCluster NewMixedClusterFunc) {
	var testValidateMixedVersionElements func(
		t *testing.T, setup, stmts []statements.Statement[tree.Statement],
	)
	downlevelClusterFunc := func(t *testing.T, knobs *scexec.TestingKnobs,
	) (_ serverutils.TestServerInterface, _ *gosql.DB, cleanup func()) {
		return newCluster(t, knobs, true)
	}
	countPostCommitStages := func(
		t *testing.T, setup, stmts []statements.Statement[tree.Statement],
	) (postCommitCount int, postCommitNonRevertibleCount int) {
		processPlanInPhase(
			t, downlevelClusterFunc, setup, stmts, scop.PostCommitPhase,
			func(p scplan.Plan) {
				for _, s := range p.Stages {
					if s.Phase == scop.PostCommitPhase {
						postCommitCount += 1
					} else if s.Phase == scop.PostCommitNonRevertiblePhase {
						postCommitNonRevertibleCount += 1
					}
				}
			},
			func(db *gosql.DB) {},
		)
		return postCommitCount, postCommitNonRevertibleCount
	}
	r, _ := randutil.NewTestRand()
	const runRate = .5

	maybeRandomlySkip := func(t *testing.T) {
		if !*runAllMixedTests && r.Float64() >= runRate {
			skip.IgnoreLint(t, "skipping due to randomness")
		}
	}
	testFunc := func(t *testing.T, path string, rewrite bool, setup, stmts []statements.Statement[tree.Statement], _ *stageExecStmtMap) {
		if !t.Run("Starting",
			func(t *testing.T) { testValidateMixedVersionElements(t, setup, stmts) },
		) {
			return
		}
	}
	testValidateMixedVersionElements = func(t *testing.T, setup, stmts []statements.Statement[tree.Statement]) {
		// If any of the statements are not supported, then skip over this
		// file for the corpus.
		for _, stmt := range stmts {
			if !scbuild.IsFullySupportedWithFalsePositive(stmt.AST, clusterversion.ClusterVersion{Version: clusterversion.ByKey(clusterversion.V22_2)}) {
				return
			}
		}
		postCommitCount, postCommitNonRevertibleCount := countPostCommitStages(t, setup, stmts)
		stageCounts := []int{postCommitCount, postCommitNonRevertibleCount}
		stageTypes := []scop.Phase{scop.PostCommitPhase, scop.PostCommitNonRevertiblePhase}

		jobPauseResumeChannel := make(chan jobspb.JobID)
		waitForPause := make(chan struct{})
		for stageTypIdx := range stageCounts {
			stageType := stageTypes[stageTypIdx]
			for stageOrdinal := 1; stageOrdinal < stageCounts[stageTypIdx]+1; stageOrdinal++ {
				t.Run(fmt.Sprintf("%s_%d_of_%d", stageType, stageType, stageCounts[stageTypIdx]+1), func(t *testing.T) {
					maybeRandomlySkip(t)
					pauseComplete := false
					var db *gosql.DB
					var cleanup func()
					_, db, cleanup = newCluster(t, &scexec.TestingKnobs{
						BeforeStage: func(p scplan.Plan, stageIdx int) error {
							if stageOrdinal == p.Stages[stageIdx].Ordinal &&
								p.Stages[stageIdx].Phase == stageType && !pauseComplete {
								jobPauseResumeChannel <- p.JobID
								<-waitForPause
								pauseComplete = true
								return kvpb.NewTransactionRetryError(kvpb.RETRY_REASON_UNKNOWN, "test")
							}
							return nil
						},
					}, true /*down level*/)

					go func() {
						jobID := <-jobPauseResumeChannel
						_, err := db.Exec("PAUSE JOB $1", jobID)
						require.NoError(t, err)
						waitForPause <- struct{}{}
						_, err = db.Exec("SET CLUSTER SETTING VERSION=$1", clusterversion.TestingBinaryVersion.String())
						require.NoError(t, err)
						testutils.SucceedsSoon(t, func() error {
							_, err = db.Exec("RESUME JOB $1", jobID)
							return err
						})
					}()

					defer cleanup()
					require.NoError(t, executeSchemaChangeTxn(
						context.Background(), t, setup, stmts, db, nil, nil, func(err error) error {
							return nil // FIXME: Check for pause
						},
					))

					// All schema change jobs should succeed after migration.
					detectJobsComplete := fmt.Sprintf(`
SELECT
    count(*)
FROM
    [SHOW JOBS]
WHERE
    job_type = 'NEW SCHEMA CHANGE'
    AND status NOT IN ('%s')
`,
						string(jobs.StatusSucceeded))
					testutils.SucceedsSoon(t, func() error {
						var count int64
						row := db.QueryRow(detectJobsComplete)
						if err := row.Scan(&count); err != nil {
							return err
						}
						if count > 0 {
							return errors.AssertionFailedf("unexpected count of %d", count)
						}
						return nil
					})
				})
			}
		}
	}
	cumulativeTest(t, path, testFunc)
}

func BackupMixedVersionElements(t *testing.T, path string, newCluster NewMixedClusterFunc) {
	testVersion := clusterversion.ClusterVersion{
		Version: clusterversion.ByKey(clusterversion.V23_1_SchemaChangerDeprecatedIndexPredicates - 1),
	}
	var after [][]string // CREATE_STATEMENT for all descriptors after finishing `stmts` in each test case.
	var dbName string
	r, _ := randutil.NewTestRand()
	const runRate = .5

	maybeRandomlySkip := func(t *testing.T) {
		if !*runAllBackups && r.Float64() >= runRate {
			skip.IgnoreLint(t, "skipping due to randomness")
		}
	}
	downlevelClusterFunc := func(t *testing.T, knobs *scexec.TestingKnobs,
	) (_ serverutils.TestServerInterface, _ *gosql.DB, cleanup func()) {
		return newCluster(t, knobs, true)
	}
	// A function that executes `setup` first and then count the number of
	// postCommit and postCommitNonRevertible stages for executing `stmts`.
	// It also initializes `after` and `dbName` here.
	countStages := func(
		t *testing.T, setup, stmts []statements.Statement[tree.Statement],
	) (postCommit, nonRevertible int) {
		var pl scplan.Plan
		processPlanInPhase(t, downlevelClusterFunc, setup, stmts, scop.PostCommitPhase,
			func(p scplan.Plan) {
				pl = p
				postCommit = len(p.StagesForCurrentPhase())
				nonRevertible = len(p.Stages) - postCommit
			}, func(db *gosql.DB) {
				tdb := sqlutils.MakeSQLRunner(db)
				var ok bool
				dbName, ok = maybeGetDatabaseForIDs(t, tdb, screl.AllTargetDescIDs(pl.TargetState))
				if ok {
					tdb.Exec(t, fmt.Sprintf("USE %q", dbName))
				}
				after = tdb.QueryStr(t, fetchDescriptorStateQuery)
			})
		return postCommit, nonRevertible
	}

	// A function that takes backup at `ord`-th stage while executing `stmts` after
	// finishing `setup`. It also takes `ord` backups at each of the preceding stage
	// if it's a revertible stage.
	// It then restores the backup(s) in various "flavors" (see
	// comment below for details) and expect the restore to finish the schema change job
	// as if the backup/restore had never happened.
	testBackupRestoreCase := func(
		t *testing.T, setup, stmts []statements.Statement[tree.Statement], ord int,
	) {
		type stage struct {
			p        scplan.Plan
			stageIdx int
			resume   chan error
		}

		stageChan := make(chan stage)
		ctx, cancel := context.WithCancel(context.Background())
		_, db, cleanup := newCluster(t, &scexec.TestingKnobs{
			BeforeStage: func(p scplan.Plan, stageIdx int) error {
				if p.Stages[len(p.Stages)-1].Phase < scop.PostCommitPhase {
					if stageChan != nil {
						close(stageChan)
					}
					return nil
				}
				if p.Stages[stageIdx].Phase < scop.PostCommitPhase {
					return nil
				}
				if stageChan != nil {
					s := stage{p: p, stageIdx: stageIdx, resume: make(chan error)}
					// Let the test program to proceed by sending to `stageChan`
					select {
					case stageChan <- s:
					case <-ctx.Done():
						return ctx.Err()
					}
					// Wait on `s.resume` until the test program has taken a backup on
					// the database. This is also where the test program injects error
					// into the schema change to force reverting.
					select {
					case err := <-s.resume:
						return err
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			},
		}, true)

		// Start with full database backup/restore.
		defer cleanup()
		defer cancel()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		tdb := sqlutils.MakeSQLRunner(conn)
		// TODO(postamar): remove this threshold bump
		//   This requires the test cases to be properly parallelized.
		tdb.SucceedsSoonDuration = testutils.RaceSucceedsSoonDuration
		tdb.Exec(t, "create database backups")
		var g errgroup.Group
		var before [][]string
		beforeFunc := func() {
			tdb.Exec(t, fmt.Sprintf("USE %q", dbName))
			before = tdb.QueryStr(t, fetchDescriptorStateQuery)
		}
		g.Go(func() error {
			return executeSchemaChangeTxn(
				context.Background(), t, setup, stmts, db, beforeFunc, nil, nil,
			)
		})
		type backup struct {
			name       string
			isRollback bool
			url        string
			s          stage
		}
		var backups []backup
		var done bool
		var rollbackStage int
		type stageKey struct {
			stage    int
			rollback bool
		}
		completedStages := make(map[stageKey]struct{})
		for i := 0; !done; i++ {
			// We want to let the stages up to ord continue unscathed. Then, we'll
			// start taking backups at ord. If ord corresponds to a revertible
			// stage, we'll inject an error, forcing the schema change to revert.
			// At each subsequent stage, we also take a backup. At the very end,
			// we'll have one backup where things should succeed and N backups
			// where we're reverting. In each case, we want to have the end state
			// of the restored set of descriptors match what we have in the original
			// cluster.
			//
			// Lastly, we'll hit an ord corresponding to the first non-revertible
			// stage. At this point, we'll take a backup for each non-revertible
			// stage and confirm that restoring them and letting the jobs run
			// leaves the database in the right state.

			// `stageChan` and `s.resume` allow this for-loop (on `i`) to be in sync
			// with the schema change job stages. E.g. if `i=0`, then we know that
			// the schema change is blocked right before executing the 0-th stage.
			s := <-stageChan
			// If there is no post-commit stages. Just consider it as done.
			if s.p.Stages == nil {
				done = true
				stageChan = nil
				break
			}
			// Move the index backwards if we see the same stage repeat due to a txn
			// retry error for example.
			stage := stageKey{
				stage:    s.stageIdx,
				rollback: s.p.InRollback,
			}
			if _, ok := completedStages[stage]; ok {
				i--
				if stage.rollback {
					rollbackStage--
				}
			}
			completedStages[stage] = struct{}{}
			shouldFail := ord == i &&
				s.p.Stages[s.stageIdx].Phase != scop.PostCommitNonRevertiblePhase &&
				!s.p.InRollback
			done = len(s.p.Stages) == s.stageIdx+1 && !shouldFail
			t.Logf("stage %d/%d in %v (rollback=%v) %d %q %v",
				s.stageIdx+1, len(s.p.Stages), s.p.Stages[s.stageIdx].Phase, s.p.InRollback, ord, dbName, done)

			// If the database has been dropped, there is nothing for
			// us to do here.
			var exists bool
			tdb.QueryRow(t,
				`SELECT count(*) > 0 FROM system.namespace WHERE "parentID" = 0 AND name = $1`,
				dbName).Scan(&exists)
			if !exists || (i < ord && !done) {
				close(s.resume)
				continue
			}

			// This test assumes that all the descriptors being modified in the
			// transaction are in the same database.
			//
			// TODO(ajwerner): Deal with trying to restore just some of the tables.
			backupURL := fmt.Sprintf("userfile://backups.public.userfiles_$user/data%d", i)
			tdb.Exec(t, fmt.Sprintf(
				"BACKUP DATABASE %s INTO '%s'", dbName, backupURL))
			backups = append(backups, backup{
				name:       dbName,
				isRollback: rollbackStage > 0,
				url:        backupURL,
				s:          s,
			})

			if s.p.InRollback {
				rollbackStage++
			}
			if done {
				t.Logf("reached final stage, waiting for completion")
				stageChan = nil // allow the restored jobs to proceed
			}
			if shouldFail {
				s.resume <- errors.Newf("boom %d", i)
			} else {
				close(s.resume)
			}
		}
		if err := g.Wait(); rollbackStage > 0 {
			require.Regexp(t, fmt.Sprintf("boom %d", ord), err)
		} else {
			require.NoError(t, err)
		}

		t.Logf("finished")

		_, err = db.Exec("SET CLUSTER SETTING VERSION=$1", clusterversion.TestingBinaryVersion.String())
		require.NoError(t, err)

		for i, b := range backups {
			// For each backup, we restore it in three flavors.
			// 1. RESTORE DATABASE
			// 2. RESTORE DATABASE WITH schema_only
			// 3. RESTORE TABLE tbl1, tbl2, ..., tblN
			// We then assert that the restored database should correctly finish
			// the ongoing schema change job when the backup was taken, and
			// reaches the expected state as if the back/restore had not happened at all.
			// Skip a backup randomly.
			type backupConsumptionFlavor struct {
				name         string
				restoreSetup []string
				restoreQuery string
			}
			flavors := []backupConsumptionFlavor{
				{
					name: "restore database",
					restoreSetup: []string{
						fmt.Sprintf("DROP DATABASE IF EXISTS %q CASCADE", dbName),
						"SET use_declarative_schema_changer = 'off'",
					},
					restoreQuery: fmt.Sprintf("RESTORE DATABASE %s FROM LATEST IN '%s'", dbName, b.url),
				},
				{
					name: "restore database with schema-only",
					restoreSetup: []string{
						fmt.Sprintf("DROP DATABASE IF EXISTS %q CASCADE", dbName),
						"SET use_declarative_schema_changer = 'off'",
					},
					restoreQuery: fmt.Sprintf("RESTORE DATABASE %s FROM LATEST IN '%s' with schema_only", dbName, b.url),
				},
			}

			// For the third flavor, we restore all tables in the backup.
			// Skip it if there is no tables.
			rows := tdb.QueryStr(t, `
			SELECT parent_schema_name, object_name
			FROM [SHOW BACKUP FROM LATEST IN $1]
			WHERE database_name = $2 AND object_type = 'table'`, b.url, dbName)
			var tablesToRestore []string
			for _, row := range rows {
				tablesToRestore = append(tablesToRestore, fmt.Sprintf("%s.%s.%s", dbName, row[0], row[1]))
			}

			if len(tablesToRestore) > 0 {
				flavors = append(flavors, backupConsumptionFlavor{
					name: "restore all tables in database",
					restoreSetup: []string{
						fmt.Sprintf("DROP DATABASE IF EXISTS %q CASCADE", dbName),
						fmt.Sprintf("CREATE DATABASE %q", dbName),
						"SET use_declarative_schema_changer = 'off'",
					},
					restoreQuery: fmt.Sprintf("RESTORE TABLE %s FROM LATEST IN '%s' WITH skip_missing_sequences",
						strings.Join(tablesToRestore, ","), b.url),
				})
			}

			// TODO (xiang): Add here the fourth flavor that restores
			// only a subset, maybe randomly chosen, of all tables with
			// `RESTORE TABLE`. Currently, it's blocked by issue #87518.
			// We will need to change what the expected output will be
			// in this case, since it will no longer be simply `before`
			// and `after`.

			for _, flavor := range flavors {
				t.Run(flavor.name, func(t *testing.T) {
					maybeRandomlySkip(t)
					t.Logf("testing backup %d (rollback=%v)", i, b.isRollback)
					tdb.ExecMultiple(t, flavor.restoreSetup...)
					tdb.Exec(t, flavor.restoreQuery)
					tdb.Exec(t, fmt.Sprintf("USE %q", dbName))
					waitForSchemaChangesToFinish(t, tdb)
					afterRestore := tdb.QueryStr(t, fetchDescriptorStateQuery)
					if b.isRollback {
						require.Equal(t, before, afterRestore)
					} else {
						require.Equal(t, after, afterRestore)
					}
					// Hack to deal with corrupt userfiles tables due to #76764.
					const validateQuery = `
SELECT * FROM crdb_internal.invalid_objects WHERE database_name != 'backups'
`
					tdb.CheckQueryResults(t, validateQuery, [][]string{})
					tdb.Exec(t, fmt.Sprintf("DROP DATABASE %q CASCADE", dbName))
					tdb.Exec(t, "USE backups")
					tdb.CheckQueryResults(t, validateQuery, [][]string{})
				})
			}
		}
	}

	testFunc := func(t *testing.T, _ string, _ bool, setup, stmts []statements.Statement[tree.Statement], _ *stageExecStmtMap) {
		for _, stmt := range stmts {
			supported := scbuild.IsFullySupportedWithFalsePositive(stmt.AST, testVersion)
			if !supported {
				skip.IgnoreLint(t, "statement not supported in current release")
			}
		}
		postCommit, nonRevertible := countStages(t, setup, stmts)
		n := postCommit + nonRevertible
		t.Logf(
			"test case has %d revertible post-commit stages and %d non-revertible"+
				" post-commit stages", postCommit, nonRevertible,
		)
		for i := 0; i <= n; i++ {
			if !t.Run(
				fmt.Sprintf("backup/restore stage %d of %d", i, n),
				func(t *testing.T) {
					maybeRandomlySkip(t)
					testBackupRestoreCase(t, setup, stmts, i)
				},
			) {
				return
			}
		}
	}

	cumulativeTest(t, path, testFunc)
}
