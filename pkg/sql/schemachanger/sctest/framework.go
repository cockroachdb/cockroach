// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sctest

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

//go:generate stringer -type=stageExecType
type stageExecType int

const (
	_ stageExecType = iota
	stageExecuteQuery
	stageExecuteStmt
)

// stageExecStmt represents statements that will be executed during a given
// stage, including any expected errors from this statement or any schema change
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
		return fmt.Sprintf("(phase = %s stageOrdinal=%d rollback=%t)",
			s.phase, s.minOrdinal, s.rollback)
	}
	return fmt.Sprintf("(phase = %s stageMinOrdinal=%d stageMaxOrdinal=%d rollback=%t),",
		s.phase, s.minOrdinal, s.maxOrdinal, s.rollback)
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

// getExecStmts gets the statements to be used for a particular phase and a
// particular stage.
func (m *stageExecStmtMap) getExecStmts(targetKey stageKey) []*stageExecStmt {
	var stmts []*stageExecStmt
	if targetKey.minOrdinal != targetKey.maxOrdinal {
		panic(fmt.Sprintf("only a single ordinal key can be looked up %s ", &targetKey))
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

// AssertMapIsUsed asserts that all DML statements are injected at various
// stages.
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
				t.Logf("Missing stage of type %q: %+v", entry.stmt.execType, entry.stageKey)
			}
		}
	}
	require.Equal(t, len(m.usedMap), len(m.entries), "All declared entries was not used")
}

// GetInjectionRuns returns a set of stage keys, where each key corresponds to a
// separate run of the schema change statement with the DML statements injected
// "appropriately". That is, for each stageKey (phase:startStage:endStage) in
// the return, we run the schema change statement and during each stage `s` within
// [startStage, endStage], we run DML statements that are requested to be injected
// in phase:`s`.
//
// The rule to generate the return is to aggregate all stages where injected DML
// statements does not cause the schema change to error, until the first schema change
// error is hit.
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
func (m *stageExecStmtMap) GetInjectionRuns(
	totalPostCommit int, totalPostCommitNonRevertible int,
) []stageKey {
	var start stageKey
	var end stageKey
	var result []stageKey
	// First split any ranges that have schema change errors to have their own
	// entries. i.e. If stages 1 to N will generate schema change errors due to
	// some statement, we need to have one entry for each one. Additionally,
	// convert any latest ordinal values, to actual values.
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
		// Skip over anything in the pre-commit phase.
		if key.phase == scop.PreCommitPhase {
			continue
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
		if !key.stmt.HasSchemaChangeError() && start.IsEmpty() {
			// If we see a schema change error, and no other statements were executed
			// earlier, then this is an entry on its own.
			start = key.stageKey
			end = key.stageKey
		} else if !start.IsEmpty() && (key.stmt.HasSchemaChangeError() || key.phase != start.phase) {
			// If we already have a start, and we either hit a schema change error
			// or separate phase, then we need to emit a new entry.
			setStart := true
			if (key.phase == start.phase && !key.stmt.HasSchemaChangeError()) || start.IsEmpty() {
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

// parseStageCommon processes common arguments for "stage-exec"-directive and
// "stage-query" directive, which include:
//   - "phase": The phase in which this statement/query should be injected, of the
//     string scop.Phase.
//   - "stage": A range of stage ordinals, of the form "stage=x:y", in the
//     specified phase where this statement should be injected.
//     Note: There is a few shorthands for the notation. If we want to inject
//     only at one particular stage, we can use "stage=x" If we want to inject
//     the statement in all stages, we can use "stage=:". If we want to inject
//     the statement in all stages starting from the x-th stage, we can use
//     "stage=x:".
//     Note: PreCommitPhase with stage 1 can be used to inject failures that will
//     only happen for DML injection testing.
//   - "schemaChangeExecError": assert that the DML injection will cause the
//     schema change to fail with a particular error at the same stage.
//   - "schemaChangeExecErrorForRollback": assert that the DML injection will cause
//     the schema change to fail with a particular error in a future stage.
//   - "rollback": mark that the injection happens during rollback.
//
// Note: statements can refer to builtin variable names with a dollar sign ($):
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
	for _, cmdArg := range d.CmdArgs {
		switch cmdArg.Key {
		case "phase":
			found := false
			for i := scop.EarliestPhase; i <= scop.LatestPhase; i++ {
				if cmdArg.Vals[0] == i.String() {
					key.phase = i
					found = true
					break
				}
			}
			require.Truef(t, found, "invalid phase name %q", cmdArg.Key)
			if !found {
				panic("phase not mapped")
			}
		case "stage":
			// Detect ranges, otherwise we are looking at single value.
			if strings.Contains(cmdArg.Vals[0], ":") {
				rangeVals := strings.Split(cmdArg.Vals[0], ":")
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
				ordinal, err := strconv.Atoi(cmdArg.Vals[0])
				require.Greater(t, ordinal, 0, "minimum ordinal is zero")
				require.NoError(t, err)
				key.minOrdinal = ordinal
				key.maxOrdinal = ordinal
			}
		case "schemaChangeExecError":
			schemaChangeErrorRegex = regexp.MustCompile(strings.Join(cmdArg.Vals, " "))
			require.Nil(t, schemaChangeErrorRegexRollback, "future and current stage errors cannot be set concurrently")
		case "schemaChangeExecErrorForRollback":
			schemaChangeErrorRegexRollback = regexp.MustCompile(strings.Join(cmdArg.Vals, " "))
			require.Nil(t, schemaChangeErrorRegex, "rollback and current stage errors cannot be set concurrently")
		case "rollback":
			rollback, err := strconv.ParseBool(cmdArg.Vals[0])
			require.NoError(t, err)
			key.rollback = rollback
		default:
			require.Failf(t, "unknown key encountered", "key was %q", cmdArg.Key)
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
		t.Logf("Starting execution of statment: %+v", stmt)
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
		switch e.execType {
		case stageExecuteStmt:
			_, err := runner.DB.ExecContext(context.Background(), boundSQL)
			if err != nil {
				if idx != len(e.stmts)-1 {
					// We require that only the last statement in a stage-exec block can cause an error.
					t.Fatalf("statement[%d] %q execution encountered unexpected error (only the last statement may expect an error): %v", idx, boundSQL, err)
				} else {
					// Fail the test unless the error is expected (from e.expectedOutput), or
					// "rewrite" is set, in which case we record the error and proceed.
					errorMatches := testutils.IsError(err, strings.TrimSuffix(e.expectedOutput, "\n"))
					if !errorMatches {
						if !rewrite {
							t.Fatalf("unexpected error: got: %v, expected: %v", err, e.expectedOutput)
						}
						e.expectedOutput = err.Error()
					}
				}
			}
		case stageExecuteQuery:
			results := runner.QueryStr(t, boundSQL)
			actualOutput := sqlutils.MatrixToStr(results)
			if e.expectedOutput != actualOutput {
				if !rewrite {
					t.Fatalf(
						"query '%s' ($stageKey=%s,$successfulStageCount=%d): expected:\n%v\ngot:\n%v\n",
						stmt,
						stageVariables.stage.String(),
						stageVariables.successfulStageCount,
						e.expectedOutput,
						actualOutput,
					)
				}
				e.expectedOutput = actualOutput
			}
		default:
			t.Fatal("unknown execType")
		}
	}
}

// GetInjectionCallback gets call back that will inject statements based on a
// given stage.
func (m *stageExecStmtMap) GetInjectionCallback(t *testing.T, rewrite bool) execInjectionCallback {
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
	}
}

type CumulativeTestSpec struct {
	Path         string
	Rewrite      bool
	Setup, Stmts []statements.Statement[tree.Statement]
	stageExecMap *stageExecStmtMap
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
	t *testing.T, relTestCaseDir string, tf func(t *testing.T, spec CumulativeTestSpec),
) {

	testCaseDir := datapathutils.RewritableDataPath(t, relTestCaseDir)
	testCaseDefinition := filepath.Join(testCaseDir, filepath.Base(testCaseDir)+".definition")
	var setup []statements.Statement[tree.Statement]
	stageExecMap := makeStageExecStmtMap()
	var rewrite bool
	var hasSeenTestCmd bool

	// First pass collect stage-exec/stage-query/setup commands and execute them
	// once the test command is encountered. Only a single test command is allowed
	// via an assertion which guarantees all others appear first.
	// This pass does no "checking" in the sense that "it does not compare any
	// actual to any expected" (look: it always returns d.Expected!). Its only
	// purpose is to run the "test"-ed statement, inject DMLs as specified, and
	// collect output of those DML injections, so they can be used to rewrite the
	// expected output of those DML injected in the second pass.
	datadriven.RunTest(t, testCaseDefinition, func(t *testing.T, d *datadriven.TestData) string {
		// Assert that only one "test"-directive statement shows up and nothing can
		// follow it afterwards.
		require.False(t, hasSeenTestCmd, "only one test command per-test, "+
			"and it must be the last one.")
		rewrite = d.Rewrite
		switch d.Cmd {
		case "skip":
			var issue int
			var csv string
			d.ScanArgs(t, "issue-num", &issue)
			d.MaybeScanArgs(t, "tests", &csv)
			for _, skippedKind := range strings.Split(csv, ",") {
				if strings.HasPrefix(t.Name(), skippedKind) {
					skip.WithIssue(t, issue)
				}
			}
		case "setup":
			// Store setup stmts into `setup` slice (without executing them).
			stmts, err := parser.Parse(d.Input)
			setup = append(setup, stmts...)
			require.NoError(t, err)
			require.NotEmpty(t, stmts)
		case "stage-exec":
			stageExecMap.ParseStageExec(t, d)
		case "stage-query":
			stageExecMap.ParseStageQuery(t, d)
		case "test":
			stmts, err := parser.Parse(d.Input)
			require.NoError(t, err)
			require.NotEmpty(t, stmts)
			tf(t, CumulativeTestSpec{
				Path:         testCaseDir,
				Rewrite:      rewrite,
				Setup:        setup,
				Stmts:        stmts,
				stageExecMap: stageExecMap,
			})
			hasSeenTestCmd = true

		default:
			t.Fatalf("unknown command type %s", d.Cmd)
		}
		return d.Expected
	})

	// Second pass is reserved to rewrite expected output of DML injections. For
	// all other directives, this pass effectively ignores them by returning
	// d.Expected.
	if rewrite {
		datadriven.RunTest(t, testCaseDefinition, func(t *testing.T, d *datadriven.TestData) string {
			if d.Cmd == "stage-exec" || d.Cmd == "stage-query" {
				// Retrieve the actual output of each DML injection block (from first
				// pass), indexed by file:line.
				return stageExecMap.GetExpectedOutputForPos(d.Pos)
			}
			return d.Expected
		})
	}
}

type CumulativeTestCaseSpec struct {
	CumulativeTestSpec
	Phase                     scop.Phase
	StageOrdinal, StagesCount int

	// After contains the CREATE_STATEMENT for all descriptors after finishing
	// `Stmts` in each test case.
	After [][]string

	// DatabaseName contains the name of the database on which the schema change
	// is applied.
	DatabaseName string
}

func (cs CumulativeTestCaseSpec) run(t *testing.T, fn func(t *testing.T)) bool {
	var prefix string
	switch cs.Phase {
	case scop.PostCommitPhase:
		prefix = "post_commit"
	case scop.PostCommitNonRevertiblePhase:
		prefix = "post_commit_non_revertible"
	default:
		return true
	}
	return t.Run(fmt.Sprintf("%s_stage_%d_of_%d", prefix, cs.StageOrdinal, cs.StagesCount), fn)
}

// cumulativeTestForEachPostCommitStage invokes `tf` once for each stage in the
// PostCommitPhase.
func cumulativeTestForEachPostCommitStage(
	t *testing.T,
	relTestCaseDir string,
	factory TestServerFactory,
	tf func(t *testing.T, spec CumulativeTestCaseSpec),
) {
	testFunc := func(t *testing.T, spec CumulativeTestSpec) {
		// Skip this test if any of the stmts is not fully supported.
		if err := areStmtsFullySupportedAtClusterVersion(t, spec, factory); err != nil {
			skip.IgnoreLint(t, "test is skipped because", err.Error())
		}
		var postCommitCount, postCommitNonRevertibleCount int
		var after [][]string
		var dbName string
		prepfn := func(db *gosql.DB, p scplan.Plan) {
			for _, s := range p.Stages {
				switch s.Phase {
				case scop.PostCommitPhase:
					postCommitCount++
				case scop.PostCommitNonRevertiblePhase:
					postCommitNonRevertibleCount++
				}
			}
			tdb := sqlutils.MakeSQLRunner(db)
			var ok bool
			dbName, ok = maybeGetDatabaseForIDs(t, tdb, screl.AllTargetStateDescIDs(p.TargetState))
			if ok {
				tdb.Exec(t, fmt.Sprintf("USE %q", dbName))
			}
			after = tdb.QueryStr(t, fetchDescriptorStateQuery)
		}
		withPostCommitPlanAfterSchemaChange(t, spec, factory, prepfn)
		if postCommitCount+postCommitNonRevertibleCount == 0 {
			skip.IgnoreLint(t, "test case has no post-commit stages")
			return
		}
		if dbName == "" {
			skip.IgnoreLint(t, "test case has no usable database")
			return
		}
		var testCases []CumulativeTestCaseSpec
		for stageOrdinal := 1; stageOrdinal <= postCommitCount; stageOrdinal++ {
			testCases = append(testCases, CumulativeTestCaseSpec{
				CumulativeTestSpec: spec,
				Phase:              scop.PostCommitPhase,
				StageOrdinal:       stageOrdinal,
				StagesCount:        postCommitCount,
				After:              after,
				DatabaseName:       dbName,
			})
		}
		for stageOrdinal := 1; stageOrdinal <= postCommitNonRevertibleCount; stageOrdinal++ {
			testCases = append(testCases, CumulativeTestCaseSpec{
				CumulativeTestSpec: spec,
				Phase:              scop.PostCommitNonRevertiblePhase,
				StageOrdinal:       stageOrdinal,
				StagesCount:        postCommitNonRevertibleCount,
				After:              after,
				DatabaseName:       dbName,
			})
		}
		var hasFailed bool
		for _, tc := range testCases {
			fn := func(t *testing.T) {
				tf(t, tc)
			}
			if hasFailed {
				fn = func(t *testing.T) {
					skip.IgnoreLint(t, "skipping test cases subsequent to earlier failure")
				}
			}
			if !tc.run(t, fn) {
				hasFailed = true
			}
		}
	}
	cumulativeTest(t, relTestCaseDir, testFunc)
}

// fetchDescriptorStateQuery returns the CREATE statements for all descriptors
// minus any COMMENT ON statements because these aren't consistently backed up.
const fetchDescriptorStateQuery = `
SELECT
	CASE
		WHEN needs_split THEN split_part(create_statement, ';', 1)
		ELSE create_statement
	END AS create_statement
FROM
	( 
		SELECT descriptor_id, create_statement, false AS needs_split FROM crdb_internal.create_schema_statements
		UNION ALL SELECT descriptor_id, create_statement, true AS needs_split FROM crdb_internal.create_statements
		UNION ALL SELECT descriptor_id, create_statement, false AS needs_split FROM crdb_internal.create_type_statements
    UNION ALL SELECT function_id as descriptor_id, create_statement, false AS needs_split FROM crdb_internal.create_function_statements
	)
WHERE descriptor_id IN (
	SELECT id FROM system.namespace
	UNION
	SELECT (json_array_elements((json_each).@2->'signatures')->'id')::INT8 AS id
	FROM (
		SELECT
			json_each(crdb_internal.pb_to_json('desc', descriptor)->'schema'->'functions')
		FROM system.descriptor
		JOIN system.namespace ns ON ns.id = descriptor.id
		WHERE crdb_internal.pb_to_json('desc', descriptor) ? 'schema'
	)
)
ORDER BY
	create_statement;`

func maybeGetDatabaseForIDs(
	t *testing.T, tdb *sqlutils.SQLRunner, ids catalog.DescriptorIDSet,
) (dbName string, exists bool) {
	const q = `
	SELECT
		name
	FROM
		system.namespace
	WHERE
		id
		IN (
				SELECT
					DISTINCT
					COALESCE(
						d->'database'->>'id',
						d->'schema'->>'parentId',
						d->'type'->>'parentId',
						d->'function'->>'parentId',
						d->'table'->>'parentId'
					)::INT8
				FROM
					(
						SELECT
							crdb_internal.pb_to_json('desc', descriptor) AS d
						FROM
							system.descriptor
						WHERE
							id IN (SELECT * FROM ROWS FROM (unnest($1::INT8[])))
					)
			)
	`
	results := tdb.QueryStr(t, q, pq.Array(ids.Ordered()))
	if len(results) > 1 {
		skip.IgnoreLintf(t, "requires all schema changes to happen within one database;"+
			" get %v: %v", len(results), results)
	}
	if len(results) == 0 {
		return "", false
	}
	return results[0][0], true
}

// withPostCommitPlanAfterSchemaChange
//   - spins up a test cluster,
//   - applies the schema change,
//   - and calls fn with the plan as it was computed for the post-commit phase.
func withPostCommitPlanAfterSchemaChange(
	t *testing.T,
	spec CumulativeTestSpec,
	factory TestServerFactory,
	fn func(db *gosql.DB, p scplan.Plan),
) {
	ctx := context.Background()
	var processOnce sync.Once
	var postCommitPlan scplan.Plan
	factory.WithSchemaChangerKnobs(&scexec.TestingKnobs{
		BeforeStage: func(p scplan.Plan, _ int) error {
			if p.Params.ExecutionPhase >= scop.PostCommitPhase {
				processOnce.Do(func() { postCommitPlan = p })
			}
			return nil
		},
	}).Run(context.Background(), t, func(_ serverutils.TestServerInterface, db *gosql.DB) {
		require.NoError(t, setupSchemaChange(ctx, t, spec, db))
		require.NoError(t, executeSchemaChangeTxn(ctx, t, spec, db))
		waitForSchemaChangesToFinish(t, sqlutils.MakeSQLRunner(db))
		fn(db, postCommitPlan)
	})
}

// setupSchemaChange executes the setup statements with the legacy schema
// changer.
func setupSchemaChange(
	ctx context.Context, t *testing.T, spec CumulativeTestSpec, db *gosql.DB,
) error {

	tdb := sqlutils.MakeSQLRunner(db)

	// Execute the setup statements with the legacy schema changer so that the
	// declarative schema changer testing knobs don't get used.
	tdb.Exec(t, "SET use_declarative_schema_changer = 'off'")
	for i, stmt := range spec.Setup {
		if _, err := tdb.DB.ExecContext(ctx, stmt.SQL); err != nil {
			// nolint:errcmp
			switch errT := err.(type) {
			case *pq.Error:
				if string(errT.Code) == pgcode.FeatureNotSupported.String() {
					skip.IgnoreLint(t, "skipping due to unimplemented feature in old cluster version.")
				}
			}
			return errors.Wrapf(err, "statement[%d] %q", i, stmt.SQL)
		}
	}
	waitForSchemaChangesToFinish(t, tdb)
	return nil
}

// executeSchemaChangeTxn executes the test statements with the declarative
// schema changer and fails the test if it all takes too long.
// This prevents the test suite from hanging when a regression is introduced.
func executeSchemaChangeTxn(
	ctx context.Context, t *testing.T, spec CumulativeTestSpec, db *gosql.DB,
) (err error) {
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
			_, err = conn.ExecContext(
				ctx, "SET experimental_enable_temp_tables=true",
			)
			_, err = conn.ExecContext(
				ctx, "SET enable_row_level_security=true",
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
			for i, stmt := range spec.Stmts {
				if _, err := tx.Exec(stmt.SQL); err != nil {
					return errors.Wrapf(err, "error processing statement[%d]: %q", i, stmt.SQL)
				}
			}
			return nil
		})
	}()
	testutils.SucceedsWithin(t, func() error {
		select {
		case e := <-c:
			err = e
			return nil
		default:
			return errors.Newf("waiting for statements to execute: %v", spec.Stmts)
		}
	},
		time.Minute*2)
	return err
}

// areStmtsFullySupportedAtClusterVersion determines if `stmts` are fully supported
// by running `stmts` in a transaction with declarative schema changer.
// It returns any error it encounters.
func areStmtsFullySupportedAtClusterVersion(
	t *testing.T, spec CumulativeTestSpec, factory TestServerFactory,
) (err error) {
	ctx := context.Background()
	factory.Run(ctx, t, func(s serverutils.TestServerInterface, db *gosql.DB) {
		cv := s.ClusterSettings().Version
		// Sieve 1: check whether the statements are even implemented and the schema
		// changer mode.
		for _, stmt := range spec.Stmts {
			if !scbuild.IsFullySupportedWithFalsePositive(stmt.AST, cv.ActiveVersion(ctx)) {
				err = scerrors.NotImplementedError(stmt.AST)
				return
			}
		}
		// Sieve 2: false positives might fall through sieve 1 and we need to actually run
		// it to account for version gates in the builder.
		require.NoError(t, setupSchemaChange(ctx, t, spec, db))
		err = executeSchemaChangeTxn(ctx, t, spec, db)
		if err == nil {
			waitForSchemaChangesToFinish(t, sqlutils.MakeSQLRunner(db))
		}
	})
	return err
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

func hasLatestSchemaChangeSucceeded(t *testing.T, tdb *sqlutils.SQLRunner) bool {
	result := tdb.QueryStr(t, fmt.Sprintf(
		`SELECT status FROM [SHOW JOBS] WHERE job_type IN ('%s') ORDER BY finished DESC, job_id DESC LIMIT 1`,
		jobspb.TypeNewSchemaChange,
	))
	return result[0][0] == "succeeded"
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

// PlanExplainer makes it easy to store a declarative schema changer
// plan EXPLAIN (DDL) diagram in a BeforeStage testing knob and load
// it later on to decorate failing test assertions.
//
// This object only works when all schema changes run sequentially, but
// in these kinds of test that's usually desirable anyway.
type PlanExplainer struct {
	atomicString *syncutil.AtomicString
}

// MakePlanExplainer is a PlanExplainer constructor.
func MakePlanExplainer() PlanExplainer {
	return PlanExplainer{atomicString: &syncutil.AtomicString{}}
}

// RequestPlan changes the PlanExplainer state so that the next
// call to MaybeUpdateWithPlan will not be a no-op.
func (pe PlanExplainer) RequestPlan() {
	pe.atomicString.Set("requested")
}

// MaybeUpdateWithPlan is a no-op unless RequestPlan has been called
// beforehand. In that case, it updates its internal state with the
// EXPLAIN (DDL) diagram for the given plan. Subsequent calls to
// MaybeUpdateWithPlan are then also no-ops until RequestPlan is called again.
// This diagram can then be retrieved by calling GetExplain.
func (pe PlanExplainer) MaybeUpdateWithPlan(p scplan.Plan) error {
	if pe.atomicString.Get() == "requested" {
		if explain, err := p.ExplainCompact(); err != nil {
			return err
		} else {
			pe.atomicString.Set(explain)
		}
	}
	return nil
}

// GetExplain returns the EXPLAIN (DDL) diagram that has been requested.
func (pe PlanExplainer) GetExplain() string {
	explain := pe.atomicString.Get()
	switch explain {
	case "":
		return "EXPLAIN (DDL) diagram not available: never requested"
	case "requested":
		return "EXPLAIN (DDL) diagram not available: requested but never provided"
	}
	return explain
}
