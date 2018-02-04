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

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// indexCheckOperation implements the checkOperation interface. It is a
// scrub check for a secondary index's integrity. This operation will
// detect:
// 1) Missing index entries. When there is a secondary index entry
//    expected, but is not found.
// 2) Dangling index references. When there is a secondary index entry
//    that refers to a primary index key that cannot be found.
type indexCheckOperation struct {
	tableName *tree.TableName
	tableDesc *sqlbase.TableDescriptor
	indexDesc *sqlbase.IndexDescriptor
	asOf      hlc.Timestamp

	// columns is a list of the columns returned by one side of the
	// queries join. The actual resulting rows from the RowContainer is
	// twice this.
	columns []*sqlbase.ColumnDescriptor
	// primaryColIdxs maps PrimaryIndex.Columns to the row
	// indexes in the query result tree.Datums.
	primaryColIdxs []int

	run indexCheckRun
}

// indexCheckRun contains the run-time state for indexCheckOperation
// during local execution.
type indexCheckRun struct {
	started bool
	// Intermediate values.
	rows     *sqlbase.RowContainer
	rowIndex int
}

func newIndexCheckOperation(
	tableName *tree.TableName,
	tableDesc *sqlbase.TableDescriptor,
	indexDesc *sqlbase.IndexDescriptor,
	asOf hlc.Timestamp,
) *indexCheckOperation {
	return &indexCheckOperation{
		tableName: tableName,
		tableDesc: tableDesc,
		indexDesc: indexDesc,
		asOf:      asOf,
	}
}

// Start will plan and run an index check using the distSQL execution
// engine.
func (o *indexCheckOperation) Start(params runParams) error {
	ctx := params.ctx

	columns, columnNames, columnTypes := getColumns(o.tableDesc, o.indexDesc)

	// Because the row results include both primary key data and secondary
	// key data, the row results will contain two copies of the column
	// data.
	columnTypes = append(columnTypes, columnTypes...)

	// Find the row indexes for all of the primary index columns.
	primaryColIdxs, err := getPrimaryColIdxs(o.tableDesc, columns)
	if err != nil {
		return err
	}

	checkQuery := createIndexCheckQuery(columnNames, o.tableDesc, o.tableName, o.indexDesc, o.asOf)
	plan, err := params.p.delegateQuery(ctx, "SCRUB TABLE ... WITH OPTIONS INDEX", checkQuery, nil, nil)
	if err != nil {
		log.Errorf(ctx, "failed to create query plan for query: %s", checkQuery)
		return errors.Wrapf(err, "could not create query plan")
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
	o.primaryColIdxs = primaryColIdxs
	o.columns = columns
	return nil
}

// Next implements the checkOperation interface.
func (o *indexCheckOperation) Next(params runParams) (tree.Datums, error) {
	row := o.run.rows.At(o.run.rowIndex)
	o.run.rowIndex++

	// Check if this row has results from the left. See the comment above
	// createIndexCheckQuery indicating why this is true.
	var isMissingIndexReferenceError bool
	if row[o.primaryColIdxs[0]] != tree.DNull {
		isMissingIndexReferenceError = true
	}

	colLen := len(o.columns)
	var errorType tree.Datum
	var primaryKeyDatums tree.Datums
	if isMissingIndexReferenceError {
		errorType = tree.NewDString(scrub.MissingIndexEntryError)
		// Fetch the primary index values from the primary index row data.
		for _, rowIdx := range o.primaryColIdxs {
			primaryKeyDatums = append(primaryKeyDatums, row[rowIdx])
		}
	} else {
		errorType = tree.NewDString(scrub.DanglingIndexReferenceError)
		// Fetch the primary index values from the secondary index row
		// data, because no primary index was found. The secondary index columns
		// are offset by the length of the distinct columns, as the first
		// set of columns is for the primary index.
		for _, rowIdx := range o.primaryColIdxs {
			primaryKeyDatums = append(primaryKeyDatums, row[rowIdx+colLen])
		}
	}
	primaryKey := tree.NewDString(primaryKeyDatums.String())
	timestamp := tree.MakeDTimestamp(
		params.extendedEvalCtx.GetStmtTimestamp(), time.Nanosecond)

	details := make(map[string]interface{})
	rowDetails := make(map[string]interface{})
	details["row_data"] = rowDetails
	details["index_name"] = o.indexDesc.Name
	if isMissingIndexReferenceError {
		// Fetch the primary index values from the primary index row data.
		for rowIdx, col := range o.columns {
			// TODO(joey): We should maybe try to get the underlying type.
			rowDetails[col.Name] = row[rowIdx].String()
		}
	} else {
		// Fetch the primary index values from the secondary index row data,
		// because no primary index was found. The secondary index columns
		// are offset by the length of the distinct columns, as the first
		// set of columns is for the primary index.
		for rowIdx, col := range o.columns {
			// TODO(joey): We should maybe try to get the underlying type.
			rowDetails[col.Name] = row[rowIdx+colLen].String()
		}
	}

	detailsJSON, err := tree.MakeDJSON(details)
	if err != nil {
		return nil, err
	}

	return tree.Datums{
		// TODO(joey): Add the job UUID once the SCRUB command uses jobs.
		tree.DNull, /* job_uuid */
		errorType,
		tree.NewDString(o.tableName.Catalog()),
		tree.NewDString(o.tableName.Table()),
		primaryKey,
		timestamp,
		tree.DBoolFalse,
		detailsJSON,
	}, nil
}

// Started implements the checkOperation interface.
func (o *indexCheckOperation) Started() bool {
	return o.run.started
}

// Done implements the checkOperation interface.
func (o *indexCheckOperation) Done(ctx context.Context) bool {
	return o.run.rows == nil || o.run.rowIndex >= o.run.rows.Len()
}

// Close4 implements the checkOperation interface.
func (o *indexCheckOperation) Close(ctx context.Context) {
	if o.run.rows != nil {
		o.run.rows.Close(ctx)
	}
}

// createIndexCheckQuery will make the index check query for a given
// table and secondary index. It will also take into account an AS OF
// SYSTEM TIME clause. For example, given the following table schema:
//
//   CREATE TABLE test (
//     k INT, s INT, v INT, misc INT,
//     PRIMARY KEY (k, s),
//     INDEX v_idx (v),
//   )
//
// The generated query to check the `v_idx` will be:
//
//   SELECT left.k, left.s, left.v, right.k, right.s, right.v
//   FROM
//     (SELECT * FROM test@{NO_INDEX_JOIN} AS left ORDER BY k, s, v)
//   FULL OUTER JOIN
//     (SELECT * FROM test@{FORCE_INDEX=v_idx,NO_INDEX_JOIN} AS right ORDER BY k, s, v)
//   ON
//      left.k = right.k AND
//      left.s = right.s AND
//      left.v = right.v AND
//   WHERE (left.k  IS NULL AND left.s  IS NULL) OR
//         (right.k IS NULL AND right.s IS NULL)
//
// In short, this query is:
// 1) Scanning the primary index and the secondary index.
// 2) Ordering both of them by the primary then secondary index columns.
//    This is done to force a distSQL merge join.
// 3) Joining both sides on all of the columns contained by the secondary
//    index key-value pairs. NB: The distSQL plan generated from this
//    query is toggled to make NULL = NULL evaluate to true using the
//    MergeJoinerSpec.NullEquality flag. Otherwise, we would need an ON
//    predicate which uses hash join and is extremely slow.
// 4) Filtering to achieve an anti-join. It looks for when the primary
//    index values are null, as they are non-nullable columns. The first
//    line of the predicate takes rows on the right for the anti-join.
//    The second line of the predicate takes rows on the left for the
//    anti-join.
//
// Because this is an anti-join, the results are as follows:
// - If any primary index column on the left is NULL, that means the
//   right columns are present. This is because of the invariant that
//   primary index columns are never null.
// - Otherwise, the left columns is present.
//
func createIndexCheckQuery(
	columnNames []string,
	tableDesc *sqlbase.TableDescriptor,
	tableName *tree.TableName,
	indexDesc *sqlbase.IndexDescriptor,
	asOf hlc.Timestamp,
) string {
	var asOfClauseStr string
	// If SCRUB is called with AS OF SYSTEM TIME <expr> the
	// checkIndexQuery will also include the as of clause.
	if asOf != hlc.MaxTimestamp {
		asOfClauseStr = fmt.Sprintf("AS OF SYSTEM TIME %d", asOf.WallTime)
	}

	// We need to make sure we can handle the non-public column `rowid`
	// that is created for implicit primary keys. In order to do so, the
	// rendered columns need to explicit in the inner selects.
	const checkIndexQuery = `
				SELECT %[1]s, %[2]s
				FROM
					(SELECT %[9]s FROM %[3]s@{FORCE_INDEX=[1],NO_INDEX_JOIN} %[10]s ORDER BY %[5]s) AS leftside
				FULL OUTER JOIN
					(SELECT %[9]s FROM %[3]s@{FORCE_INDEX=[%[4]d],NO_INDEX_JOIN} %[10]s ORDER BY %[5]s) AS rightside
					ON %[6]s
				WHERE (%[7]s) OR
							(%[8]s)`
	return fmt.Sprintf(checkIndexQuery,
		tableColumnsProjection("leftside", columnNames),                                                        // 1
		tableColumnsProjection("rightside", columnNames),                                                       // 2
		tableName.String(),                                                                                     // 3
		indexDesc.ID,                                                                                           // 4
		strings.Join(columnNames, ","),                                                                         // 5
		tableColumnsEQ("leftside", "rightside", columnNames, columnNames),                                      // 6
		tableColumnsIsNullPredicate("leftside", tableDesc.PrimaryIndex.ColumnNames, "AND", true /* isNull */),  // 7
		tableColumnsIsNullPredicate("rightside", tableDesc.PrimaryIndex.ColumnNames, "AND", true /* isNull */), // 8
		strings.Join(columnNames, ","),                                                                         // 9
		asOfClauseStr,                                                                                          // 10
	)
}
