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
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type scrubNode struct {
	optColumnsSlot

	n *tree.Scrub

	run scrubRun
}

// checkOperation is an interface for scrub check execution. The
// different types of checks implement the interface. The checks are
// then bundled together and iterated through to pull results.
//
// NB: Other changes that need to be made to implement a new check are:
//  1) Add the option parsing in startScrubTable
//  2) Queue the checkOperation structs into scrubNode.checkQueue.
//
// TODO(joey): Eventually we will add the ability to repair check
// failures. In that case, we can add a AttemptRepair function that is
// called after each call to Next.
type checkOperation interface {
	// Started indicates if a checkOperation has already been initialized
	// by Start during the lifetime of the operation.
	Started() bool

	// Start initializes the check. In many cases, this does the bulk of
	// the work behind a check.
	Start(context.Context, *planner) error

	// Next will return the next check result. The datums returned have
	// the column types specified by scrubTypes, which are the valeus
	// returned to the user.
	//
	// Next is not called if Done() is false.
	Next(context.Context, *planner) (tree.Datums, error)

	// Done indicates when there are no more results to iterate through.
	Done(context.Context) bool

	// Close will clean up any in progress resources.
	Close(context.Context)
}

// Scrub checks the database.
// Privileges: superuser.
func (p *planner) Scrub(ctx context.Context, n *tree.Scrub) (planNode, error) {
	if err := p.RequireSuperUser("SCRUB"); err != nil {
		return nil, err
	}
	return &scrubNode{n: n}, nil
}

var scrubColumns = sqlbase.ResultColumns{
	{Name: "job_uuid", Typ: types.UUID},
	{Name: "error_type", Typ: types.String},
	{Name: "database", Typ: types.String},
	{Name: "table", Typ: types.String},
	{Name: "primary_key", Typ: types.String},
	{Name: "timestamp", Typ: types.Timestamp},
	{Name: "repaired", Typ: types.Bool},
	{Name: "details", Typ: types.JSON},
}

// scrubRun contains the run-time state of scrubNode during local execution.
type scrubRun struct {
	checkQueue []checkOperation
	row        tree.Datums
}

func (n *scrubNode) Start(params runParams) error {
	switch n.n.Typ {
	case tree.ScrubTable:
		tableName, err := n.n.Table.NormalizeWithDatabaseName(params.p.session.Database)
		if err != nil {
			return err
		}
		if err := tableName.QualifyWithDatabase(params.p.session.Database); err != nil {
			return err
		}
		// If the tableName provided refers to a view and error will be
		// returned here.
		tableDesc, err := MustGetTableDesc(params.ctx, params.p.txn, params.p.getVirtualTabler(),
			tableName, false /* allowAdding */)
		if err != nil {
			return err
		}
		if err := n.startScrubTable(params.ctx, params.p, tableDesc, tableName); err != nil {
			return err
		}
	case tree.ScrubDatabase:
		if err := n.startScrubDatabase(params.ctx, params.p, &n.n.Database); err != nil {
			return err
		}
	default:
		return pgerror.NewErrorf(pgerror.CodeInternalError,
			"unexpected SCRUB type received, got: %v", n.n.Typ)
	}
	return nil
}

func (n *scrubNode) Next(params runParams) (bool, error) {
	for len(n.run.checkQueue) > 0 {
		nextCheck := n.run.checkQueue[0]
		if !nextCheck.Started() {
			if err := nextCheck.Start(params.ctx, params.p); err != nil {
				return false, err
			}
		}

		// Check if the iterator is finished before calling Next. This
		// happens if there are no more results to report.
		if !nextCheck.Done(params.ctx) {
			var err error
			n.run.row, err = nextCheck.Next(params.ctx, params.p)
			if err != nil {
				return false, err
			}
			return true, nil
		}

		nextCheck.Close(params.ctx)
		// Prepare the next iterator. If we happen to finish this iterator,
		// we want to begin the next one so we still return a result.
		n.run.checkQueue = n.run.checkQueue[1:]
	}
	return false, nil
}

func (n *scrubNode) Values() tree.Datums {
	return n.run.row
}

func (n *scrubNode) Close(ctx context.Context) {
	// Close any iterators which have not been completed.
	for len(n.run.checkQueue) > 0 {
		n.run.checkQueue[0].Close(ctx)
		n.run.checkQueue = n.run.checkQueue[1:]
	}
}

const (
	// ScrubErrorMissingIndexEntry occurs when a primary k/v is missing a
	// corresponding secondary index k/v.
	ScrubErrorMissingIndexEntry = "missing_index_entry"
	// ScrubErrorDanglingIndexReference occurs when a secondary index k/v
	// points to a non-existing primary k/v.
	ScrubErrorDanglingIndexReference = "dangling_index_reference"
)

// startScrubDatabase prepares a scrub check for each of the tables in
// the database. Views are skipped without errors.
func (n *scrubNode) startScrubDatabase(ctx context.Context, p *planner, name *tree.Name) error {
	// Check that the database exists.
	database := string(*name)
	dbDesc, err := MustGetDatabaseDesc(ctx, p.txn, p.getVirtualTabler(), database)
	if err != nil {
		return err
	}
	tbNames, err := getTableNames(ctx, p.txn, p.getVirtualTabler(), dbDesc, false)
	if err != nil {
		return err
	}

	for i := range tbNames {
		tableName := &tbNames[i]
		if err := tableName.QualifyWithDatabase(database); err != nil {
			return err
		}
		tableDesc, err := MustGetTableOrViewDesc(ctx, p.txn, p.getVirtualTabler(), tableName,
			false /* allowAdding */)
		if err != nil {
			return err
		}
		// Skip views and don't throw an error if we encounter one.
		if tableDesc.IsView() {
			continue
		}
		if err := n.startScrubTable(ctx, p, tableDesc, tableName); err != nil {
			return err
		}
	}
	return nil
}

func (n *scrubNode) startScrubTable(
	ctx context.Context, p *planner, tableDesc *sqlbase.TableDescriptor, tableName *tree.TableName,
) error {
	// Process SCRUB options. These are only present during a SCRUB TABLE
	// statement.
	var indexesSet bool
	var physicalCheckSet bool
	var constraintsSet bool
	for _, option := range n.n.Options {
		switch v := option.(type) {
		case *tree.ScrubOptionIndex:
			if indexesSet {
				return pgerror.NewErrorf(pgerror.CodeSyntaxError,
					"cannot specify INDEX option more than once")
			}
			indexesSet = true
			indexesToCheck, err := createIndexCheckOperations(v.IndexNames, tableDesc, tableName)
			if err != nil {
				return err
			}
			n.run.checkQueue = append(n.run.checkQueue, indexesToCheck...)
		case *tree.ScrubOptionPhysical:
			if physicalCheckSet {
				return pgerror.NewErrorf(pgerror.CodeSyntaxError,
					"cannot specify PHYSICAL option more than once")
			}
			physicalCheckSet = true
			// TODO(joey): Initialize physical index to check.
		case *tree.ScrubOptionConstraint:
			if constraintsSet {
				return pgerror.NewErrorf(pgerror.CodeSyntaxError,
					"cannot specify CONSTRAINT option more than once")
			}
			constraintsSet = true
		default:
			panic(fmt.Sprintf("Unhandled SCRUB option received: %+v", v))
		}
	}

	// When no options are provided the default behavior is to run
	// exhaustive checks.
	if len(n.n.Options) == 0 {
		indexesToCheck, err := createIndexCheckOperations(nil /* indexNames */, tableDesc, tableName)
		if err != nil {
			return err
		}
		n.run.checkQueue = append(n.run.checkQueue, indexesToCheck...)
		// TODO(joey): Initialize physical index to check.
	}

	return nil
}

// getColumns returns the columns that are stored in an index k/v. The
// column names and types are also returned.
func getColumns(
	tableDesc *sqlbase.TableDescriptor, indexDesc *sqlbase.IndexDescriptor,
) (columns []*sqlbase.ColumnDescriptor, columnNames []string, columnTypes []sqlbase.ColumnType) {
	colToIdx := make(map[sqlbase.ColumnID]int)
	for i, col := range tableDesc.Columns {
		colToIdx[col.ID] = i
	}

	// Collect all of the columns we are fetching from the index. This
	// includes the columns involved in the index: columns, extra columns,
	// and store columns.
	for _, colID := range indexDesc.ColumnIDs {
		columns = append(columns, &tableDesc.Columns[colToIdx[colID]])
	}
	for _, colID := range indexDesc.ExtraColumnIDs {
		columns = append(columns, &tableDesc.Columns[colToIdx[colID]])
	}
	for _, colID := range indexDesc.StoreColumnIDs {
		columns = append(columns, &tableDesc.Columns[colToIdx[colID]])
	}

	// Collect the column names and types.
	for _, col := range columns {
		columnNames = append(columnNames, col.Name)
		columnTypes = append(columnTypes, col.Type)
	}
	return columns, columnNames, columnTypes
}

// getPrimaryColIdxs returns a list of the primary index columns and
// their corresponding index in the columns list.
func getPrimaryColIdxs(
	tableDesc *sqlbase.TableDescriptor, columns []*sqlbase.ColumnDescriptor,
) (primaryColIdxs []int, err error) {
	for i, colID := range tableDesc.PrimaryIndex.ColumnIDs {
		rowIdx := -1
		for idx, col := range columns {
			if col.ID == colID {
				rowIdx = idx
				break
			}
		}
		if rowIdx == -1 {
			return nil, errors.Errorf(
				"could not find primary index column in projection: columnID=%d columnName=%s",
				colID,
				tableDesc.PrimaryIndex.ColumnNames[i])
		}
		primaryColIdxs = append(primaryColIdxs, rowIdx)
	}
	return primaryColIdxs, nil
}

// indexCheckOperation is a check on a secondary index's integrity.
type indexCheckOperation struct {
	tableName *tree.TableName
	tableDesc *sqlbase.TableDescriptor
	indexDesc *sqlbase.IndexDescriptor

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
	tableName *tree.TableName, tableDesc *sqlbase.TableDescriptor, indexDesc *sqlbase.IndexDescriptor,
) *indexCheckOperation {
	return &indexCheckOperation{
		tableName: tableName,
		tableDesc: tableDesc,
		indexDesc: indexDesc,
	}
}

// Start will plan and run an index check using the distSQL execution
// engine.
func (o *indexCheckOperation) Start(ctx context.Context, p *planner) error {
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

	checkQuery := createIndexCheckQuery(columnNames, o.tableDesc, o.tableName, o.indexDesc)
	plan, err := p.delegateQuery(ctx, "SCRUB TABLE ... WITH OPTIONS INDEX", checkQuery, nil, nil)
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
	plan, err = p.optimizePlan(ctx, plan, needed)
	if err != nil {
		plan.Close(ctx)
		return err
	}
	defer plan.Close(ctx)

	planCtx := p.session.distSQLPlanner.newPlanningCtx(ctx, &p.evalCtx, p.txn)
	physPlan, err := scrubPlanDistSQL(ctx, &planCtx, p, plan)
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

	rows, err := scrubRunDistSQL(ctx, &planCtx, p, physPlan, columnTypes)
	if err != nil {
		rows.Close(ctx)
		return err
	} else if rows.Len() == 0 {
		rows.Close(ctx)
		rows = nil
	}

	o.run.started = true
	o.run.rows = rows
	o.primaryColIdxs = primaryColIdxs
	o.columns = columns
	return nil
}

// Next implements the checkOperation interface.
func (o *indexCheckOperation) Next(ctx context.Context, p *planner) (tree.Datums, error) {
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
		errorType = tree.NewDString(ScrubErrorMissingIndexEntry)
		// Fetch the primary index values from the primary index row data.
		for _, rowIdx := range o.primaryColIdxs {
			primaryKeyDatums = append(primaryKeyDatums, row[rowIdx])
		}
	} else {
		errorType = tree.NewDString(ScrubErrorDanglingIndexReference)
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
		p.evalCtx.GetStmtTimestamp(), time.Nanosecond)

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
		tree.NewDString(o.tableName.Database()),
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

// tableColumnsIsNullPredicate creates a predicate that checks if all of
// the specified columns for a table are NULL (or not NULL, based on the
// isNull flag). For example, given table is t1 and the columns id,
// name, data, then the returned string is:
//
//   t1.id IS NULL AND t1.name IS NULL AND t1.data IS NULL
//
func tableColumnsIsNullPredicate(tableName string, columns []string, isNull bool) string {
	var buf bytes.Buffer
	nullCheck := "NOT NULL"
	if isNull {
		nullCheck = "NULL"
	}
	for i, col := range columns {
		if i > 0 {
			buf.WriteString(" AND ")
		}
		fmt.Fprintf(&buf, "%[1]s.%[2]s IS %[3]s", tableName, col, nullCheck)
	}
	return buf.String()
}

// tableColumnsEQ creates a predicate that checks if all of the
// specified columns for two tables are equal. For example, given tables
// t1, t2 and the columns id, name, then the returned string is:
//
//   t1.id = t2.id AND t1.name = t2.name
//
func tableColumnsEQ(tableName string, otherTableName string, columns []string) string {
	var buf bytes.Buffer
	for i, col := range columns {
		if i > 0 {
			buf.WriteString(" AND ")
		}
		fmt.Fprintf(&buf, `%[1]s.%[3]s = %[2]s.%[3]s`, tableName, otherTableName, col)
	}
	return buf.String()
}

// tableColumnsProjection creates the select projection statement (a
// comma delimetered column list), for the specified table and
// columns. For example, if the table is t1 and the columns are id,
// name, data, then the returned string is:
//
//   t1.id, t1.name, t1.data
func tableColumnsProjection(tableName string, columns []string) string {
	var buf bytes.Buffer
	for i, col := range columns {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%[1]s.%[2]s", tableName, col)
	}
	return buf.String()
}

// createIndexCheckQuery will make the index check query for a given
// table and secondary index.
// For example, given the following table schema:
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
// TODO(joey): Once we support ANTI JOIN in distSQL this can be
// simplified, as the the WHERE clause can be completely dropped.
func createIndexCheckQuery(
	columnNames []string,
	tableDesc *sqlbase.TableDescriptor,
	tableName *tree.TableName,
	indexDesc *sqlbase.IndexDescriptor,
) string {
	// We need to make sure we can handle the non-public column `rowid`
	// that is created for implicit primary keys. In order to do so, the
	// rendered columns need to explicit in the inner selects.
	const checkIndexQuery = `
				SELECT %[1]s, %[2]s
				FROM
					(SELECT %[9]s FROM %[3]s@{FORCE_INDEX=[1],NO_INDEX_JOIN} ORDER BY %[5]s) AS leftside
				FULL OUTER JOIN
					(SELECT %[9]s FROM %[3]s@{FORCE_INDEX=[%[4]d],NO_INDEX_JOIN} ORDER BY %[5]s) AS rightside
					ON %[6]s
				WHERE (%[7]s) OR
							(%[8]s)`
	return fmt.Sprintf(checkIndexQuery,
		tableColumnsProjection("leftside", columnNames),                                                 // 1
		tableColumnsProjection("rightside", columnNames),                                                // 2
		tableName.String(),                                                                              // 3
		indexDesc.ID,                                                                                    // 4
		strings.Join(columnNames, ","),                                                                  // 5
		tableColumnsEQ("leftside", "rightside", columnNames),                                            // 6
		tableColumnsIsNullPredicate("leftside", tableDesc.PrimaryIndex.ColumnNames, true /* isNull */),  // 7
		tableColumnsIsNullPredicate("rightside", tableDesc.PrimaryIndex.ColumnNames, true /* isNull */), // 8
		strings.Join(columnNames, ","),                                                                  // 9
	)
}

// createIndexCheckOperations will return the checkOperations for the
// provided indexes. If indexNames is nil, then all indexes are
// returned. TODO(joey): This can be simplified with
// TableDescriptor.FindIndexByName(), but this will only report the
// first invalid index.
func createIndexCheckOperations(
	indexNames tree.NameList, tableDesc *sqlbase.TableDescriptor, tableName *tree.TableName,
) (results []checkOperation, err error) {
	if indexNames == nil {
		// Populate results with all secondary indexes of the
		// table.
		for i := range tableDesc.Indexes {
			results = append(results, newIndexCheckOperation(
				tableName,
				tableDesc,
				&tableDesc.Indexes[i],
			))
		}
		return results, nil
	}

	// Find the indexes corresponding to the user input index names.
	names := make(map[string]struct{})
	for _, idxName := range indexNames {
		names[idxName.String()] = struct{}{}
	}
	for i := range tableDesc.Indexes {
		if _, ok := names[tableDesc.Indexes[i].Name]; ok {
			results = append(results, newIndexCheckOperation(
				tableName,
				tableDesc,
				&tableDesc.Indexes[i],
			))
			delete(names, tableDesc.Indexes[i].Name)
		}
	}
	if len(names) > 0 {
		// Get a list of all the indexes that could not be found.
		missingIndexNames := []string(nil)
		for _, idxName := range indexNames {
			if _, ok := names[idxName.String()]; ok {
				missingIndexNames = append(missingIndexNames, idxName.String())
			}
		}
		return nil, pgerror.NewErrorf(pgerror.CodeUndefinedObjectError,
			"specified indexes to check that do not exist on table %q: %v",
			tableDesc.Name, strings.Join(missingIndexNames, ", "))
	}
	return results, nil
}

// scrubPlanDistSQL will prepare and run the plan in distSQL.
func scrubPlanDistSQL(
	ctx context.Context, planCtx *planningCtx, p *planner, plan planNode,
) (*physicalPlan, error) {
	log.VEvent(ctx, 1, "creating DistSQL plan")
	physPlan, err := p.session.distSQLPlanner.createPlanForNode(planCtx, plan)
	if err != nil {
		return nil, err
	}
	p.session.distSQLPlanner.FinalizePlan(planCtx, &physPlan)

	return &physPlan, err
}

// scrubRunDistSQL run a distSQLPhysicalPlan plan in distSQL. If a
// RowContainer is returned the caller must close it.
func scrubRunDistSQL(
	ctx context.Context,
	planCtx *planningCtx,
	p *planner,
	plan *physicalPlan,
	columnTypes []sqlbase.ColumnType,
) (*sqlbase.RowContainer, error) {
	ci := sqlbase.ColTypeInfoFromColTypes(columnTypes)
	rows := sqlbase.NewRowContainer(*p.evalCtx.ActiveMemAcc, ci, 0)
	rowResultWriter := NewRowResultWriter(tree.Rows, rows)
	recv, err := makeDistSQLReceiver(
		ctx,
		rowResultWriter,
		p.ExecCfg().RangeDescriptorCache,
		p.ExecCfg().LeaseHolderCache,
		p.txn,
		func(ts hlc.Timestamp) {
			_ = p.ExecCfg().Clock.Update(ts)
		},
	)
	if err != nil {
		return rows, err
	}

	if err := p.session.distSQLPlanner.Run(planCtx, p.txn, plan, &recv, p.evalCtx); err != nil {
		return rows, err
	} else if recv.err != nil {
		return rows, recv.err
	}

	return rows, nil
}
