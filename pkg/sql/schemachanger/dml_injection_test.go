// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemachanger_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// phaseOrdinal uniquely identifies a stage. The stageIdx cannot be used
// because the number of stages in the plan changes between stages.
type phaseOrdinal struct {
	phase   scop.Phase
	ordinal int
}

func toPhaseOrdinal(stage scplan.Stage) phaseOrdinal {
	return phaseOrdinal{
		phase:   stage.Phase,
		ordinal: stage.Ordinal,
	}
}

func (po phaseOrdinal) String() string {
	return fmt.Sprintf("%s:%d", po.phase, po.ordinal)
}

func toAnySlice(strings []string) []any {
	result := make([]any, 0, len(strings))
	for _, s := range strings {
		result = append(result, s)
	}
	return result
}

const (
	valInitial = "1"
	valUpdated = "2"
	opDelete   = "delete"
	opUpdate   = "update"
	na         = "n/a"
	// insert_phase_ordinal is the phaseOrdinal the record was inserted by
	// operation_phase_ordinal is the phaseOrdinal that will modify the record
	// operation is the operation the operation_phase_ordinal will do to the record
	// val gets updated for 'update' operations
	createTable = `
CREATE TABLE tbl (
	insert_phase_ordinal TEXT NOT NULL,
	operation_phase_ordinal TEXT NOT NULL,
	operation TEXT NOT NULL,
	val INT,
	PRIMARY KEY (insert_phase_ordinal, operation_phase_ordinal, operation)
)`
	createTableNoPK = `
CREATE TABLE tbl (
	insert_phase_ordinal TEXT NOT NULL,
	operation_phase_ordinal TEXT NOT NULL,
	operation TEXT NOT NULL,
	val INT
)`
)

func (po phaseOrdinal) noopRow() []string {
	return []string{
		po.String(),
		na,
		na,
		valInitial,
	}
}

func (po phaseOrdinal) deleteRow(operationPO phaseOrdinal) []string {
	return []string{
		po.String(),
		operationPO.String(),
		opDelete,
		valInitial,
	}
}

func (po phaseOrdinal) updateRow(operationPO phaseOrdinal, isUpdated bool) []string {
	val := valInitial
	if isUpdated {
		val = valUpdated
	}
	return []string{
		po.String(),
		operationPO.String(),
		opUpdate,
		val,
	}
}

type testCase struct {
	desc         string
	setup        []string
	createTable  string
	schemaChange string
	expectedErr  string
	skipIssue    int
	// Optional: If you want a query to run at each stage, you can include it here.
	// We don't evaluate the results; we simply assert that the query executes without errors.
	query string
}

// Captures testCase before t.Parallel is called.
func (tc testCase) capture(f func(*testing.T, testCase)) func(*testing.T) {
	return func(t *testing.T) {
		f(t, tc)
	}
}

// TestAlterTableDMLInjection creates a table, runs optional setup DDL, then
// runs a schema change. Before the schema change a single record is inserted
// so that the table is not empty. During each schema change stage, the
// expected state of the table is verified and records are
// inserted/updated/deleted as follows:
//   - insert an immutable operation="n/a" row that later stages will never
//     modify
//   - insert an operation="delete" row for each later stage - each later stage
//     will delete these rows that previous stages inserted for it
//   - insert an operation="update" row for each later stage - each later stage
//     will update these rows that previous stages inserted for it
func TestAlterTableDMLInjection(t *testing.T) {
	t.Cleanup(leaktest.AfterTest(t))
	scope := log.Scope(t)
	t.Cleanup(func() {
		scope.Close(t)
	})

	skip.UnderStressWithIssue(t, 111663)

	testCases := []testCase{
		{
			desc:         "add column",
			schemaChange: "ALTER TABLE tbl ADD COLUMN new_col INT",
		},
		{
			desc:         "drop column",
			setup:        []string{"ALTER TABLE tbl ADD COLUMN new_col INT"},
			schemaChange: "ALTER TABLE tbl DROP COLUMN new_col",
		},
		{
			desc:         "add column default value",
			schemaChange: "ALTER TABLE tbl ADD COLUMN new_col INT NOT NULL DEFAULT 1",
		},
		{
			desc:         "add column default sequence",
			setup:        []string{"CREATE SEQUENCE seq"},
			schemaChange: "ALTER TABLE tbl ADD COLUMN new_col INT NOT NULL DEFAULT nextval('seq')",
			expectedErr:  "cannot evaluate scalar expressions containing sequence operations in this context",
		},
		{
			desc:         "alter column type trivial",
			setup:        []string{"ALTER TABLE tbl ADD COLUMN new_col SMALLINT NOT NULL DEFAULT 100"},
			schemaChange: "ALTER TABLE tbl ALTER COLUMN new_col SET DATA TYPE BIGINT",
		},
		{
			desc:         "alter column type validate",
			setup:        []string{"ALTER TABLE tbl ADD COLUMN new_col BIGINT NOT NULL DEFAULT 100"},
			schemaChange: "ALTER TABLE tbl ALTER COLUMN new_col SET DATA TYPE SMALLINT",
		},
		{
			desc: "alter column type general",
			setup: []string{
				"SET enable_experimental_alter_column_type_general=TRUE",
				"ALTER TABLE tbl ADD COLUMN new_col BIGINT NOT NULL DEFAULT 100",
			},
			schemaChange: "ALTER TABLE tbl ALTER COLUMN new_col SET DATA TYPE TEXT",
			query:        "SELECT new_col FROM tbl LIMIT 1",
		},
		{
			desc: "alter column type general compute",
			setup: []string{
				"SET enable_experimental_alter_column_type_general=TRUE",
				"ALTER TABLE tbl ADD COLUMN new_col DATE NOT NULL DEFAULT '2013-05-06', " +
					"ADD COLUMN new_comp DATE AS (new_col) STORED",
			},
			schemaChange: "ALTER TABLE tbl ALTER COLUMN new_comp SET DATA TYPE DATE USING '2021-05-06'",
			query:        "SELECT new_comp FROM tbl LIMIT 1",
		},
		{
			desc:         "add column default udf",
			setup:        []string{"CREATE FUNCTION f() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$"},
			schemaChange: "ALTER TABLE tbl ADD COLUMN new_col INT NOT NULL DEFAULT f()",
			skipIssue:    87699,
		},
		{
			desc: "drop column default udf",
			setup: []string{
				"CREATE FUNCTION f() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$",
				"ALTER TABLE tbl ADD COLUMN new_col INT NOT NULL DEFAULT f()",
			},
			schemaChange: "ALTER TABLE tbl DROP COLUMN new_col",
			skipIssue:    87699,
		},
		{
			desc:         "add column unique not null",
			schemaChange: "ALTER TABLE tbl ADD COLUMN new_col TEXT NOT NULL UNIQUE",
			expectedErr:  "null value in column \"new_col\" violates not-null constraint",
		},
		{
			desc:         "add column default unique",
			schemaChange: "ALTER TABLE tbl ADD COLUMN new_col TEXT NOT NULL UNIQUE DEFAULT insert_phase_ordinal || operation_phase_ordinal || operation",
			expectedErr:  "variable sub-expressions are not allowed in DEFAULT",
		},
		{
			desc:         "add column stored",
			schemaChange: "ALTER TABLE tbl ADD COLUMN new_col TEXT NOT NULL AS (insert_phase_ordinal) STORED",
		},
		{
			desc:         "drop column stored",
			setup:        []string{"ALTER TABLE tbl ADD COLUMN new_col TEXT NOT NULL AS (insert_phase_ordinal) STORED"},
			schemaChange: "ALTER TABLE tbl DROP COLUMN new_col",
		},
		{
			desc:         "add column stored family",
			schemaChange: "ALTER TABLE tbl ADD COLUMN new_col TEXT NOT NULL AS (insert_phase_ordinal) STORED CREATE FAMILY fam",
		},
		{
			desc:         "drop column stored family",
			setup:        []string{"ALTER TABLE tbl ADD COLUMN new_col TEXT NOT NULL AS (insert_phase_ordinal) STORED CREATE FAMILY fam"},
			schemaChange: "ALTER TABLE tbl DROP COLUMN new_col",
		},
		{
			desc:         "add column virtual NOT NULL",
			schemaChange: "ALTER TABLE tbl ADD COLUMN new_col TEXT NOT NULL AS (NULL::TEXT) VIRTUAL",
			expectedErr:  "validation of column \"new_col\" NOT NULL failed on row: insert_phase_ordinal='pre-schema-change', operation_phase_ordinal='n/a', operation='n/a', val=1, new_col=NULL",
		},
		{
			desc:         "add column virtual",
			schemaChange: "ALTER TABLE tbl ADD COLUMN new_col TEXT NOT NULL AS (insert_phase_ordinal) VIRTUAL",
		},
		{
			desc:         "drop column virtual",
			setup:        []string{"ALTER TABLE tbl ADD COLUMN new_col TEXT NOT NULL AS (insert_phase_ordinal) VIRTUAL"},
			schemaChange: "ALTER TABLE tbl DROP COLUMN new_col",
		},
		{
			desc:         "add constraint check",
			schemaChange: "ALTER TABLE tbl ADD CONSTRAINT c CHECK (val > -1)",
		},
		{
			desc:         "drop constraint check",
			setup:        []string{"ALTER TABLE tbl ADD CONSTRAINT c CHECK (val > -1)"},
			schemaChange: "ALTER TABLE tbl DROP CONSTRAINT c",
		},
		{
			desc:         "add constraint check udf",
			setup:        []string{"CREATE FUNCTION f(i INT) RETURNS INT LANGUAGE SQL AS $$ SELECT i $$"},
			schemaChange: "ALTER TABLE tbl ADD CONSTRAINT c CHECK (f(val) > -1)",
		},
		{
			desc:         "add constraint check not valid",
			schemaChange: "ALTER TABLE tbl ADD CONSTRAINT c CHECK (val > -1) NOT VALID",
		},
		{
			desc:         "validate constraint",
			setup:        []string{"ALTER TABLE tbl ADD CONSTRAINT c CHECK (val > -1) NOT VALID"},
			schemaChange: "ALTER TABLE tbl VALIDATE CONSTRAINT c",
		},
		{
			desc:         "add constraint check udt",
			setup:        []string{"CREATE TYPE udt AS ENUM ('1', '2')"},
			schemaChange: "ALTER TABLE tbl ADD CONSTRAINT c CHECK (val::text::udt IN ('1', '2'))",
		},
		{
			desc: "add constraint foreign key",
			setup: []string{
				"CREATE TABLE tbl2 (val INT PRIMARY KEY)",
				"INSERT INTO tbl2 VALUES (1), (2)",
			},
			schemaChange: "ALTER TABLE tbl ADD CONSTRAINT fk FOREIGN KEY (val) REFERENCES tbl2 (val)",
		},
		{
			desc: "drop constraint foreign key",
			setup: []string{
				"CREATE TABLE tbl2 (val INT PRIMARY KEY)",
				"INSERT INTO tbl2 VALUES (1), (2)",
				"ALTER TABLE tbl ADD CONSTRAINT fk FOREIGN KEY (val) REFERENCES tbl2 (val)",
			},
			schemaChange: "ALTER TABLE tbl DROP CONSTRAINT fk",
		},
		{
			desc:         "add primary key",
			createTable:  createTableNoPK,
			schemaChange: "ALTER TABLE tbl ADD PRIMARY KEY (insert_phase_ordinal, operation_phase_ordinal, operation)",
		},
		{
			desc:         "add constraint unique without index",
			setup:        []string{"SET experimental_enable_unique_without_index_constraints = true"},
			schemaChange: "ALTER TABLE tbl ADD CONSTRAINT c UNIQUE WITHOUT INDEX (insert_phase_ordinal, operation_phase_ordinal, operation)",
		},
		{
			desc: "drop constraint unique without index",
			setup: []string{
				"SET experimental_enable_unique_without_index_constraints = true",
				"ALTER TABLE tbl ADD CONSTRAINT c UNIQUE WITHOUT INDEX (insert_phase_ordinal, operation_phase_ordinal, operation)",
			},
			schemaChange: "ALTER TABLE tbl DROP CONSTRAINT c",
		},
		{
			desc:        "alter primary key using columns",
			createTable: createTableNoPK,
			setup: []string{
				"ALTER TABLE tbl ADD COLUMN id SERIAL NOT NULL",
				"ALTER TABLE tbl ADD PRIMARY KEY (id)",
			},
			schemaChange: "ALTER TABLE tbl ALTER PRIMARY KEY USING COLUMNS (insert_phase_ordinal, operation_phase_ordinal, operation)",
		},
		{
			desc:        "alter primary key and replace rowid in PK",
			createTable: createTableNoPK,
			setup: []string{
				"CREATE INDEX i1 ON tbl (val)",
			},
			// Run a query against the secondary index at each stage.
			query:        "SELECT operation FROM tbl@i1",
			schemaChange: "ALTER TABLE tbl ALTER PRIMARY KEY USING COLUMNS (insert_phase_ordinal, operation_phase_ordinal, operation)",
			skipIssue:    133129,
		},
		{
			desc:        "alter primary key using columns using hash",
			createTable: createTableNoPK,
			setup: []string{
				"ALTER TABLE tbl ADD COLUMN id SERIAL NOT NULL",
				"ALTER TABLE tbl ADD PRIMARY KEY (id)",
			},
			schemaChange: "ALTER TABLE tbl ALTER PRIMARY KEY USING COLUMNS (insert_phase_ordinal, operation_phase_ordinal, operation) USING HASH",
		},
		{
			desc:         "create index",
			schemaChange: "CREATE INDEX idx ON tbl (val)",
		},
		{
			desc:         "drop index",
			setup:        []string{"CREATE INDEX idx ON tbl (val)"},
			schemaChange: "DROP INDEX idx",
		},
		{
			desc: "drop column with index",
			setup: []string{
				"ALTER TABLE tbl ADD COLUMN i INT NOT NULL DEFAULT 1",
				"CREATE INDEX idx ON tbl (i)",
			},
			schemaChange: "ALTER TABLE tbl DROP COLUMN i",
		},
		{
			desc:         "add column and check constraint",
			schemaChange: "ALTER TABLE tbl ADD COLUMN new_col INT NOT NULL DEFAULT 1, ADD CHECK (new_col > 0)",
		},
		{
			desc:         "create index using hash",
			schemaChange: "CREATE INDEX idx ON tbl (val) USING HASH",
		},
		{
			desc:         "drop index using hash",
			setup:        []string{"CREATE INDEX idx ON tbl (val) USING HASH"},
			schemaChange: "DROP INDEX idx",
		},
		{
			desc: "drop column with index using hash cascade",
			setup: []string{
				"ALTER TABLE tbl ADD COLUMN i INT NOT NULL DEFAULT 1",
				"CREATE INDEX idx ON tbl (i) USING HASH",
			},
			schemaChange: "ALTER TABLE tbl DROP COLUMN i CASCADE",
		},
		{
			desc: "drop column with composite index + fk",
			setup: []string{
				"ALTER TABLE tbl ADD COLUMN i INT NOT NULL DEFAULT unique_rowid()",
				"CREATE UNIQUE INDEX idx ON tbl (val, i)",
				"CREATE TABLE tbl_ref (val int primary key, i int, CONSTRAINT \"j_k_fk\" FOREIGN KEY (val, i) REFERENCES tbl(val, i))",
			},
			schemaChange: "DROP INDEX tbl@idx CASCADE",
		},
		{
			desc:         "create unique index",
			schemaChange: "CREATE UNIQUE INDEX idx ON tbl (insert_phase_ordinal, operation_phase_ordinal, operation)",
		},
		{
			desc:         "drop unique index",
			setup:        []string{"CREATE UNIQUE INDEX idx ON tbl (insert_phase_ordinal, operation_phase_ordinal, operation)"},
			schemaChange: "DROP INDEX idx",
		},
		{
			desc: "drop column with unique index",
			setup: []string{
				"ALTER TABLE tbl ADD COLUMN i INT NOT NULL DEFAULT 1",
				"CREATE UNIQUE INDEX idx ON tbl (insert_phase_ordinal, operation_phase_ordinal, operation, i)",
			},
			schemaChange: "ALTER TABLE tbl DROP COLUMN i",
		},
		{
			desc:         "create partial index",
			schemaChange: "CREATE INDEX idx ON tbl (val) WHERE val > 1",
		},
		{
			desc:         "drop partial index",
			setup:        []string{"CREATE INDEX idx ON tbl (val) WHERE val > 1"},
			schemaChange: "DROP INDEX idx",
		},
		{
			desc: "drop column with partial index",
			setup: []string{
				"ALTER TABLE tbl ADD COLUMN i INT NOT NULL DEFAULT 1",
				"CREATE INDEX idx ON tbl (val) WHERE i > 1",
			},
			schemaChange: "ALTER TABLE tbl DROP COLUMN i",
			skipIssue:    97813,
		},
		{
			desc:         "create expression index",
			schemaChange: "CREATE INDEX idx ON tbl ((val + 1))",
		},
		{
			desc:         "drop expression index",
			setup:        []string{"CREATE INDEX idx ON tbl ((val + 1))"},
			schemaChange: "DROP INDEX idx",
		},
		{
			desc: "drop column with expression index cascade",
			setup: []string{
				"ALTER TABLE tbl ADD COLUMN i INT NOT NULL DEFAULT 1",
				"CREATE INDEX idx ON tbl ((i + 1))",
			},
			schemaChange: "ALTER TABLE tbl DROP COLUMN i CASCADE",
		},
		{
			desc:         "create materialized view from index",
			schemaChange: "CREATE MATERIALIZED VIEW mv AS SELECT * FROM tbl@tbl_pkey",
		},
		{
			desc: "drop index cascade to materialized view",
			setup: []string{
				"CREATE INDEX idx ON tbl (val)",
				"CREATE MATERIALIZED VIEW mv AS SELECT * FROM tbl@idx",
			},
			schemaChange: "DROP INDEX idx CASCADE",
		},
	}

	ctx := context.Background()
	for _, tc := range testCases {
		t.Run(tc.desc, tc.capture(func(t *testing.T, tc testCase) {
			t.Parallel() // SAFE FOR TESTING
			if issue := tc.skipIssue; issue != 0 {
				skip.WithIssue(t, issue)
			}
			const insert = `INSERT INTO tbl VALUES ($1, $2, $3, $4)`
			preSchemaChangeRow := []string{
				"pre-schema-change",
				na,
				na,
				valInitial,
			}
			var sqlDB *sqlutils.SQLRunner
			var clusterCreated atomic.Bool
			poMap := make(map[phaseOrdinal]int)
			poCompleted := make(map[phaseOrdinal]struct{})
			var poSlice []phaseOrdinal
			testCluster := serverutils.StartCluster(t, 1, base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						SQLEvalContext: &eval.TestingKnobs{
							// We disable the randomization of some batch sizes because with
							// some low values the test takes much longer.
							ForceProductionValues: true,
						},
						SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
							BeforeStage: func(p scplan.Plan, stageIdx int) error {
								if !clusterCreated.Load() {
									// Do nothing if cluster creation isn't finished. Certain schema
									// changes are run during cluster creation (e.g. `CREATE DATABASE
									// defaultdb`) and we don't want those to hijack this knob.
									return nil
								}

								// Cannot verify DML injection for unsupported schema changes.
								if tc.expectedErr != "" {
									return nil
								}

								if t.Failed() {
									// Just bail out of this hook if the test has failed,
									// returning errors will break rollbacks.
									return nil
								}

								currentStage := p.Stages[stageIdx]
								currentPO := toPhaseOrdinal(currentStage)
								errorMessage := fmt.Sprintf("phaseOrdinal=%s", currentPO)

								// Capture all stages in the StatementPhase before they disappear,
								// only if they haven't been collected (we could encounter retries).
								if currentStage.Phase == scop.StatementPhase &&
									len(poSlice) == 0 {
									for i, s := range p.Stages {
										po := toPhaseOrdinal(s)
										poMap[po] = i
										poSlice = append(poSlice, po)
									}
								}

								numStages := len(poMap)
								poIdx := poMap[currentPO]

								require.NotNil(t, sqlDB, errorMessage)

								// Add preSchemaChangeRow.
								expectedResults := [][]string{preSchemaChangeRow}
								for i := 0; i < poIdx; i++ {
									// Add no-op rows from previous stages.
									expectedResults = append(expectedResults, poSlice[i].noopRow())

									// Add update rows from previous stages for all stages.
									for j := i + 1; j < numStages; j++ {
										isJPrev := false
										if j < poIdx {
											isJPrev = true
										}
										expectedResults = append(expectedResults, poSlice[i].updateRow(poSlice[j], isJPrev))
									}

									// Add delete rows from previous stages for this and later stages.
									for j := poIdx; j < numStages; j++ {
										expectedResults = append(expectedResults, poSlice[i].deleteRow(poSlice[j]))
									}
								}
								// Sort expectedResults to match order returned by SELECT.
								slices.SortFunc(expectedResults, func(a, b []string) int {
									require.Equal(t, len(a), len(b), errorMessage)
									for i := 0; i < len(a); i++ {
										if c := strings.Compare(a[i], b[i]); c != 0 {
											return c
										}
									}
									panic(fmt.Sprintf("slice contains duplicate elements a=%s b=%s %s", a, b, errorMessage))
								})
								actualResults := sqlDB.QueryStr(t, `SELECT 	insert_phase_ordinal, operation_phase_ordinal, operation, val FROM tbl`)
								// Transaction retry errors can occur, so don't repeat the same
								// DML if hit such a case to avoid flaky tests.
								if _, exists := poCompleted[currentPO]; exists {
									return nil
								}
								// Use subset instead of equals for better error output.
								require.Subset(t, expectedResults, actualResults, errorMessage)
								require.Subset(t, actualResults, expectedResults, errorMessage)

								// If a query is provided, run it without checking the results—just
								// ensure it doesn't fail.
								if tc.query != "" {
									sqlDB.Exec(t, tc.query)
								}

								for i := 0; i < poIdx; i++ {
									insertPO := poSlice[i]
									// Verify 1 row is correctly deleted.
									sqlDB.ExecRowsAffectedWithMessage(
										t,
										1,
										errorMessage,
										`DELETE FROM tbl WHERE insert_phase_ordinal = $1 AND operation_phase_ordinal = $2 AND operation = $3`,
										insertPO.String(), currentPO.String(), opDelete,
									)
									// Verify 1 row is correctly updated.
									sqlDB.ExecRowsAffectedWithMessage(
										t,
										1,
										errorMessage,
										`UPDATE tbl SET val = 2 WHERE insert_phase_ordinal = $1 AND operation_phase_ordinal = $2 AND operation = $3`,
										insertPO.String(), currentPO.String(), opUpdate,
									)
								}

								// This record should not be modified by any stage.
								sqlDB.ExecWithMessage(t, errorMessage, insert, toAnySlice(currentPO.noopRow())...)
								for i := poIdx + 1; i < numStages; i++ {
									// These records should be modified by later stages.
									//  'delete' should be deleted at operation_stage.
									//  'update' should have its val updated at operation_stage.
									sqlDB.ExecWithMessage(t, errorMessage, insert, toAnySlice(currentPO.deleteRow(poSlice[i]))...)
									sqlDB.ExecWithMessage(t, errorMessage, insert, toAnySlice(currentPO.updateRow(poSlice[i], false))...)
								}
								poCompleted[currentPO] = struct{}{}
								return nil
							},
						},
					},
				},
			})
			defer testCluster.Stopper().Stop(ctx)
			sqlDB = sqlutils.MakeSQLRunner(testCluster.ServerConn(0))
			create := tc.createTable
			if create == "" {
				create = createTable
			}
			sqlDB.Exec(t, create)
			sqlDB.Exec(t, insert, toAnySlice(preSchemaChangeRow)...)
			for i, setup := range tc.setup {
				sqlDB.ExecWithMessage(t, fmt.Sprintf("i=%d", i), setup)
			}
			clusterCreated.Store(true)
			_, err := sqlDB.DB.ExecContext(ctx, tc.schemaChange)
			if expectedErr := tc.expectedErr; expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, expectedErr)
			}
		}))
	}
}
