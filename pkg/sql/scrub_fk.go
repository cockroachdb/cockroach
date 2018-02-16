// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
)

// sqlForeignKeyCheckOperation is a check on an indexes physical data.
type sqlForeignKeyCheckOperation struct {
	tableName  *tree.TableName
	tableDesc  *sqlbase.TableDescriptor
	constraint *sqlbase.ConstraintDetail
	asOf       hlc.Timestamp

	colIDToRowIdx map[sqlbase.ColumnID]int

	run sqlForeignKeyConstraintCheckRun
}

// sqlForeignKeyConstraintCheckRun contains the run-time state for
// sqlForeignKeyConstraintCheckOperation during local execution.
type sqlForeignKeyConstraintCheckRun struct {
	started  bool
	rows     *sqlbase.RowContainer
	rowIndex int
}

func newSQLForeignKeyCheckOperation(
	tableName *tree.TableName,
	tableDesc *sqlbase.TableDescriptor,
	constraint sqlbase.ConstraintDetail,
	asOf hlc.Timestamp,
) *sqlForeignKeyCheckOperation {
	return &sqlForeignKeyCheckOperation{
		tableName:  tableName,
		tableDesc:  tableDesc,
		constraint: &constraint,
		asOf:       asOf,
	}
}

// Start implements the checkOperation interface.
// It creates a query string and generates a plan from it, which then
// runs in the distSQL execution engine.
func (o *sqlForeignKeyCheckOperation) Start(params runParams) error {
	ctx := params.ctx
	checkQuery, err := createFKCheckQuery(o.tableName.Catalog(), o.tableDesc, o.constraint, o.asOf)
	if err != nil {
		return err
	}
	plan, err := params.p.delegateQuery(ctx, "SCRUB TABLE ... WITH OPTIONS CONSTRAINT", checkQuery, nil, nil)
	if err != nil {
		return err
	}

	// All columns projected in the plan generated from the query are
	// needed. The columns are the index columns and extra columns in the
	// index, twice -- for the primary and then secondary index.
	needed := make([]bool, len(planColumns(plan)))
	for i := range needed {
		needed[i] = true
	}

	// Optimize the plan. This is required in order to populate scanNode
	// spans.
	plan, err = params.p.optimizePlan(ctx, plan, needed)
	if err != nil {
		plan.Close(ctx)
		return err
	}
	defer plan.Close(ctx)

	// Collect the expected types for the query results. This is all
	// columns and extra columns in the secondary index used for foreign
	// key referencing. This also implicitly includes all primary index
	// columns.
	columnsByID := make(map[sqlbase.ColumnID]*sqlbase.ColumnDescriptor, len(o.tableDesc.Columns))
	for i := range o.tableDesc.Columns {
		columnsByID[o.tableDesc.Columns[i].ID] = &o.tableDesc.Columns[i]
	}

	colIDs, _ := o.constraint.Index.FullColumnIDs()
	columnTypes := make([]sqlbase.ColumnType, len(colIDs))
	o.colIDToRowIdx = make(map[sqlbase.ColumnID]int, len(colIDs))
	for i, id := range colIDs {
		columnTypes[i] = columnsByID[id].Type
		o.colIDToRowIdx[id] = i
	}

	planCtx := params.extendedEvalCtx.DistSQLPlanner.newPlanningCtx(ctx, params.extendedEvalCtx, params.p.txn)
	physPlan, err := scrubPlanDistSQL(ctx, &planCtx, plan)
	if err != nil {
		return err
	}

	// Set NullEquality to true on all MergeJoinerSpecs. This changes the
	// behavior of the query's equality semantics in the ON predicate. The
	// equalities will now evaluate NULL = NULL to true, which is what we
	// desire when testing the equivilance of two index entries. There
	// might be multiple merge joiners (with hash-routing).
	var foundMergeJoiner bool
	for i := range physPlan.Processors {
		if physPlan.Processors[i].Spec.Core.MergeJoiner != nil {
			physPlan.Processors[i].Spec.Core.MergeJoiner.NullEquality = true
			foundMergeJoiner = true
		}
	}
	if !foundMergeJoiner {
		return errors.Errorf("could not find MergeJoinerSpec in plan")
	}

	rows, err := scrubRunDistSQL(ctx, &planCtx, params.p, physPlan, columnTypes)
	if err != nil {
		rows.Close(ctx)
		return err
	}

	o.run.started = true
	o.run.rows = rows
	return nil
}

// Next implements the checkOperation interface.
func (o *sqlForeignKeyCheckOperation) Next(params runParams) (tree.Datums, error) {
	row := o.run.rows.At(o.run.rowIndex)
	o.run.rowIndex++

	details := make(map[string]interface{})
	rowDetails := make(map[string]interface{})
	details["row_data"] = rowDetails
	details["constraint_name"] = o.constraint.FK.Name

	// Collect the primary index values for generating the primary key
	// pretty string.
	primaryKeyDatums := make(tree.Datums, 0, len(o.tableDesc.PrimaryIndex.ColumnIDs))
	for _, id := range o.tableDesc.PrimaryIndex.ColumnIDs {
		idx := o.colIDToRowIdx[id]
		primaryKeyDatums = append(primaryKeyDatums, row[idx])
	}

	// Collect all of the values fetched from the index to generate a
	// pretty JSON dictionary for row_data.
	for _, id := range o.constraint.Index.ColumnIDs {
		idx := o.colIDToRowIdx[id]
		name := o.constraint.Index.ColumnNames[idx]
		rowDetails[name] = row[idx].String()
	}
	for _, id := range o.constraint.Index.ExtraColumnIDs {
		idx := o.colIDToRowIdx[id]
		col, err := o.tableDesc.FindActiveColumnByID(id)
		if err != nil {
			return nil, err
		}
		rowDetails[col.Name] = row[idx].String()
	}

	detailsJSON, err := tree.MakeDJSON(details)
	if err != nil {
		return nil, err
	}

	return tree.Datums{
		// TODO(joey): Add the job UUID once the SCRUB command uses jobs.
		tree.DNull, /* job_uuid */
		tree.NewDString(scrub.ForeignKeyConstraintViolation),
		tree.NewDString(o.tableName.Catalog()),
		tree.NewDString(o.tableName.Table()),
		tree.NewDString(primaryKeyDatums.String()),
		tree.MakeDTimestamp(params.extendedEvalCtx.GetStmtTimestamp(), time.Nanosecond),
		tree.DBoolFalse,
		detailsJSON,
	}, nil
}

// Started implements the checkOperation interface.
func (o *sqlForeignKeyCheckOperation) Started() bool {
	return o.run.started
}

// Done implements the checkOperation interface.
func (o *sqlForeignKeyCheckOperation) Done(ctx context.Context) bool {
	return o.run.rows == nil || o.run.rowIndex >= o.run.rows.Len()
}

// Close implements the checkOperation interface.
func (o *sqlForeignKeyCheckOperation) Close(ctx context.Context) {
	if o.run.rows != nil {
		o.run.rows.Close(ctx)
	}
}

// createFKCheckQuery will make the foreign key check query for a given
// table, the referenced tables and the mapping from table columns to
// referenced table columns.
//
// For example, given the following table schemas:
//
//   CREATE TABLE parent (
//     id INT, dept_id INT,
//     PRIMARY KEY (id),
//     UNIQUE INDEX dept_idx (dept_id)
//   )
//
//   CREATE TABLE child (
//     id INT, dept_id INT,
//     PRIMARY KEY (id),
//     CONSTRAINT "parent_fk" FOREIGN KEY (dept_id) REFERENCES parent (dept_id)
//   )
//
// The generated query to check the `parent_fk` will be:
//
//   SELECT p.id, p.dept_id
//   FROM
//     (SELECT id, dept_id FROM child@{FORCE_INDEX=[..],NO_INDEX_JOIN} ORDER BY dept_id) AS p
//   LEFT OUTER JOIN
//     (SELECT dept_id FROM parent@{FORCE_INDEX=dept_idx,NO_INDEX_JOIN} ORDER BY dept_id) AS c
//   ON
//      p.dept_id = c.dept_id
//   WHERE p.dept_id IS NOT NULL AND c.dept_id IS NULL
//
// In short, this query is:
// 1) Scanning the foreign key index of the child table and the
//    referenced index of the foreign table.
// 2) Explicitly ordering both of them by their index columns. This
//    should be a NOOP as the indexes should already have a matching
//    order. This is done to force a distSQL merge join.
// 3) Joining all of the referencing columns from both tables columns
//    that are equivalent. NB: The distSQL plan generated from this
//    query is toggled to make NULL = NULL evaluate to true using the
//    MergeJoinerSpec.NullEquality flag. Otherwise, we would need an ON
//    predicate which uses hash join and is extremely slow.
// 4) Filtering to achieve an anti-join. This filters out any child rows
//    which have all NULL foreign key values and filters out any
//    matches.
//
func createFKCheckQuery(
	database string,
	tableDesc *sqlbase.TableDescriptor,
	constraint *sqlbase.ConstraintDetail,
	asOf hlc.Timestamp,
) (string, error) {
	var asOfClauseStr string
	// If SCRUB is called with AS OF SYSTEM TIME <expr> the
	// checkIndexQuery will also include the as of clause.
	if asOf != hlc.MaxTimestamp {
		asOfClauseStr = fmt.Sprintf("AS OF SYSTEM TIME %d", asOf.WallTime)
	}

	const checkIndexQuery = `
				SELECT %[1]s
				FROM
					(SELECT %[9]s FROM %[2]s.%[3]s@{NO_INDEX_JOIN} %[12]s ORDER BY %[10]s) AS p
				FULL OUTER JOIN
					(SELECT %[11]s FROM %[2]s.%[4]s@{FORCE_INDEX=[%[5]d],NO_INDEX_JOIN} %[12]s ORDER BY %[11]s) AS c
					ON %[6]s
        WHERE (%[7]s) AND %[8]s`

	columnNames := append([]string(nil), constraint.Index.ColumnNames...)
	// ExtraColumns will include primary index key values not already part of the index.
	for _, id := range constraint.Index.ExtraColumnIDs {
		column, err := tableDesc.FindActiveColumnByID(id)
		if err != nil {
			return "", err
		}
		columnNames = append(columnNames, column.Name)
	}

	return fmt.Sprintf(checkIndexQuery,
		tableColumnsProjection("p", columnNames), // 1
		database,                                                                                           // 2
		tableDesc.Name,                                                                                     // 3
		constraint.ReferencedTable.Name,                                                                    // 4
		constraint.ReferencedIndex.ID,                                                                      // 5
		tableColumnsEQ("p", "c", constraint.Columns, constraint.ReferencedIndex.ColumnNames),               // 6
		tableColumnsIsNullPredicate("p", constraint.Columns, "OR", false /* isNull */),                     // 7
		tableColumnsIsNullPredicate("c", constraint.ReferencedIndex.ColumnNames, "AND", true /* isNull */), // 8
		strings.Join(columnNames, ","),                                                                     // 9
		strings.Join(constraint.Columns, ","),                                                              // 10
		strings.Join(constraint.ReferencedIndex.ColumnNames, ","),                                          // 11
		asOfClauseStr, //12
	), nil
}
