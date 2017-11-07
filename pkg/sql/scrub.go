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

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

const (
	// ScrubErrorMissingIndexEntry occurs when a primary k/v is missing a
	// corresponding secondary index k/v.
	ScrubErrorMissingIndexEntry = "missing_index_entry"
	// ScrubErrorDanglingIndexReference occurs when a secondary index k/v
	// points to a non-existing primary k/v.
	ScrubErrorDanglingIndexReference = "dangling_index_reference"
)

type scrubNode struct {
	optColumnsSlot

	n *parser.Scrub
	// indexes is a queue of index checks planned to be executed.
	indexes []indexCheckDetails

	// currentIndex holds the intermediate buffered results for an index
	// check.
	currentIndex *checkedIndex

	row parser.Datums
}

// indexCheckDetails holds the information for an index check that
// will be executed.
type indexCheckDetails struct {
	indexDesc *sqlbase.IndexDescriptor
	tableDesc *sqlbase.TableDescriptor
	tableName *parser.TableName
}

// checkedIndex holds the intermediate results for an index that has
// buffered results.
type checkedIndex struct {
	details indexCheckDetails

	// Intermediate values.
	rows     *sqlbase.RowContainer
	rowIndex int

	// columns is a list of the columns returned by one side of the
	// queries join. The actual resulting rows from the RowContainer is
	// twice this.
	columns []*sqlbase.ColumnDescriptor
	// primaryIndexColumnIndexes maps PrimaryIndex.Columns to the row
	// indexes in the query result parser.Datums.
	primaryIndexColumnIndexes []int
}

// Scrub checks the database.
// Privileges: security.RootUser user.
func (p *planner) Scrub(ctx context.Context, n *parser.Scrub) (planNode, error) {
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

func (n *scrubNode) Start(params runParams) error {
	switch n.n.Typ {
	case parser.ScrubTable:
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
	case parser.ScrubDatabase:
		if err := n.startScrubDatabase(params.ctx, params.p, &n.n.Database); err != nil {
			return err
		}
	default:
		return pgerror.NewErrorf(pgerror.CodeInternalError,
			"unexpected SCRUB type received, got: %v", n.n.Typ)
	}
	return nil
}

// startScrubDatabase prepares a scrub check for each of the tables in
// the database. Views are skipped without errors.
func (n *scrubNode) startScrubDatabase(ctx context.Context, p *planner, name *parser.Name) error {
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
		// Skip views, but don't throw an error if we encounter one.
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
	ctx context.Context, p *planner, tableDesc *sqlbase.TableDescriptor, tableName *parser.TableName,
) error {
	// Process SCRUB options. These are only present during a SCRUB TABLE
	// statement.
	var indexesSet bool
	var physical bool
	for _, option := range n.n.Options {
		switch v := option.(type) {
		case *parser.ScrubOptionIndex:
			if indexesSet {
				return pgerror.NewErrorf(pgerror.CodeSyntaxError,
					"cannot specify INDEX option more than once")
			}
			indexesToCheck, err := indexesToCheck(v.IndexNames, tableDesc, tableName)
			if err != nil {
				return err
			}
			n.indexes = append(n.indexes, indexesToCheck...)
			indexesSet = true
		case *parser.ScrubOptionPhysical:
			if physical {
				return pgerror.NewErrorf(pgerror.CodeSyntaxError,
					"cannot specify PHYSICAL option more than once")
			}
			physical = true
		default:
			panic(fmt.Sprintf("Unhandled SCRUB option received: %+v", v))
		}
	}

	// When no options are provided the default is to run exhaustive checks.
	if len(n.n.Options) == 0 {
		indexesToCheck, err := indexesToCheck(nil /* indexNames */, tableDesc, tableName)
		if err != nil {
			return err
		}
		n.indexes = append(n.indexes, indexesToCheck...)
		physical = true
	}

	return nil
}

// startIndexCheck will plan and run an index check using the distSQL
// execution engine. The returned value contains the row iterator and
// the information about the index that was checked.
func (n *scrubNode) startIndexCheck(
	ctx context.Context, p *planner, indexToCheck indexCheckDetails,
) (*checkedIndex, error) {
	tableDesc := indexToCheck.tableDesc
	indexDesc := indexToCheck.indexDesc
	tableName := indexToCheck.tableName

	colToIdx := make(map[sqlbase.ColumnID]int)
	for i, col := range tableDesc.Columns {
		colToIdx[col.ID] = i
	}

	// Collect all of the columns we are fetching from the secondary
	// index. This includes the columns involved in the index: columns,
	// extra columns, and store columns.
	var columns []*sqlbase.ColumnDescriptor
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
	var columnNames []string
	var columnTypes []sqlbase.ColumnType
	for _, col := range columns {
		columnNames = append(columnNames, col.Name)
		columnTypes = append(columnTypes, col.Type)
	}

	// Because the row results include both primary key data and secondary
	// key data, the row results will contain two copies of the column
	// data.
	columnTypes = append(columnTypes, columnTypes...)

	// Find the row indexes for all of the primary index columns.
	var primaryColumnRowIndexes []int
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
		primaryColumnRowIndexes = append(primaryColumnRowIndexes, rowIdx)
	}

	checkQuery := createIndexCheckQuery(columnNames,
		tableDesc.PrimaryIndex.ColumnNames, tableName, indexDesc.ID)
	plan, err := p.delegateQuery(ctx, "SCRUB TABLE ... WITH OPTIONS INDEX", checkQuery, nil, nil)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	defer plan.Close(ctx)

	rows, err := scrubIndexRunDistSQL(ctx, p, plan, columnTypes)
	if err != nil {
		rows.Close(ctx)
		return nil, err
	} else if rows == nil {
		return nil, nil
	}

	return &checkedIndex{
		details: indexToCheck,
		rows:    rows,
		primaryIndexColumnIndexes: primaryColumnRowIndexes,
		columns:                   columns,
	}, nil
}

// scrubIndexRunDistSQL will prepare and run the plan in distSQL. If a
// RowContainer is returned the caller must close it.
func scrubIndexRunDistSQL(
	ctx context.Context, p *planner, plan planNode, columnTypes []sqlbase.ColumnType,
) (*sqlbase.RowContainer, error) {
	ci := sqlbase.ColTypeInfoFromColTypes(columnTypes)
	rows := sqlbase.NewRowContainer(*p.evalCtx.ActiveMemAcc, ci, 0)
	rowResultWriter := NewRowResultWriter(parser.Rows, rows)
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

	err = p.session.distSQLPlanner.PlanAndRun(ctx, p.txn, plan, &recv, p.evalCtx)
	if err != nil {
		return rows, err
	} else if recv.err != nil {
		return rows, recv.err
	}

	if rows.Len() == 0 {
		rows.Close(ctx)
		return nil, nil
	}

	return rows, nil
}

// tableColumnsIsNullPredicate creates a predicate that checks if all of
// the specified columns for a table are NULL (or not null, based on the
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
// specified columns for two tables are equal. This predicate alos needs
// NULL-checks to work around the equivilancy of NULL = NULL. For
// example, given tables t1, t2 and the columns id, name, then the
// returned string is:
//
//   ((t1.id IS NOT NULL AND t2.id IS NOT NULL AND t1.id = t2.id) OR
//    (t1.id IS NULL AND t2.id IS NULL)) AND
//   ((t1.name IS NOT NULL AND t2.name IS NOT NULL AND t1.name = t2.name) OR
//    (t1.name IS NULL AND t2.name IS NULL))
//
func tableColumnsEQ(tableName string, otherTableName string, columns []string) string {
	var buf bytes.Buffer
	for i, col := range columns {
		if i > 0 {
			buf.WriteString(" AND ")
		}
		fmt.Fprintf(&buf, `
			((%[1]s.%[3]s IS NOT NULL AND %[2]s.%[3]s IS NOT NULL AND %[1]s.%[3]s = %[2]s.%[3]s) OR
			 (%[1]s.%[3]s IS NULL AND %[2]s.%[3]s IS NULL))`,
			tableName, otherTableName, col)
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
//   SELECT left.k, left.s, left.v, right.k, right.s. right.v
//   FROM
//     test@{NO_INDEX_JOIN} as left
//   FULL OUTER JOIN
//     test@{FORCE_INDEX=v_idx,NO_INDEX_JOIN} as right
//   ON
//      ((left.k IS NOT NULL AND right.k IS NOT NULL AND left.k = right.k) OR
//       (left.k IS NULL AND right.k IS NULL)) AND
//      ((left.s IS NOT NULL AND right.s IS NOT NULL AND left.s = right.s) OR
//       (left.s IS NULL AND right.s IS NULL)) AND
//      ((left.v IS NOT NULL AND right.v IS NOT NULL AND left.v = right.v) OR
//       (left.v IS NULL AND right.v IS NULL))
//   WHERE (left.k  IS NULL AND left.s  IS NULL) OR
//         (right.k IS NULL AND right.s IS NULL)
//
// In short, this query is:
// 1) Scanning the primary index and the secondary index.
// 2) Joining them on all of the secondary index columns and extra
//    columns that are equivalent. This is a verbose check due to
//    equivilancy involving NULLs.
// 3) Filtering to achieve an anti-join. The first line of the predicate
//    takes rows on the right for the anti-join. The second line of the
//    predicate takes rows on the left for the anti-join.
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
	primaryKeyColumnNames []string,
	tableName *parser.TableName,
	indexID sqlbase.IndexID,
) string {
	const checkIndexQuery = `
				SELECT %[1]s, %[2]s
				FROM
					%[3]s@{NO_INDEX_JOIN} as leftside
				FULL OUTER JOIN
					%[3]s@{FORCE_INDEX=[%[4]d],NO_INDEX_JOIN} as rightside
					ON %[5]s
        WHERE (%[6]s) OR
              (%[7]s)`

	return fmt.Sprintf(checkIndexQuery,
		tableColumnsProjection("leftside", columnNames),
		tableColumnsProjection("rightside", columnNames),
		tableName.String(),
		indexID,
		tableColumnsEQ("leftside", "rightside", columnNames),
		tableColumnsIsNullPredicate("leftside", primaryKeyColumnNames, true /* isNull */),
		tableColumnsIsNullPredicate("rightside", primaryKeyColumnNames, true /* isNull */),
	)
}

// indexesToCheck will return all of the indexes that are being checked.
// If indexNames is nil, then all indexes are returned.
// TODO(joey): This can be simplified with
// TableDescriptor.FindIndexByName(), but this will only report the
// first invalid index.
func indexesToCheck(
	indexNames parser.NameList, tableDesc *sqlbase.TableDescriptor, tableName *parser.TableName,
) (results []indexCheckDetails, err error) {
	if indexNames == nil {
		// Populate results with all secondary indexes of the
		// table.
		for _, indexDesc := range tableDesc.Indexes {
			results = append(results, indexCheckDetails{
				indexDesc: &indexDesc,
				tableName: tableName,
				tableDesc: tableDesc,
			})
		}
		return results, nil
	}

	// Find the indexes corresponding to the user input index names.
	names := make(map[string]struct{})
	for _, idxName := range indexNames {
		names[idxName.String()] = struct{}{}
	}
	for _, indexDesc := range tableDesc.Indexes {
		if _, ok := names[indexDesc.Name]; ok {
			results = append(results, indexCheckDetails{
				indexDesc: &indexDesc,
				tableName: tableName,
				tableDesc: tableDesc,
			})
			delete(names, indexDesc.Name)
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

func (n *scrubNode) Next(params runParams) (bool, error) {
	var err error
	// Begin the next index check query. It is planned then executed in
	// the distSQL execution engine. While n.currentIndex is nil, the last
	// index checked had no errors.
	for len(n.indexes) > 0 && n.currentIndex == nil {
		n.currentIndex, err = n.startIndexCheck(params.ctx, params.p, n.indexes[0])
		if err != nil {
			return false, err
		}
		n.indexes = n.indexes[1:]
	}

	// If an index check query is in progress then we will pull the rows
	// from its buffer.
	if n.currentIndex != nil {
		resultRow := n.currentIndex.rows.At(n.currentIndex.rowIndex)
		n.row, err = getNextIndexError(params.p, n.currentIndex, resultRow)
		if err != nil {
			return false, err
		}
		n.currentIndex.rowIndex++
		if n.currentIndex.rowIndex >= n.currentIndex.rows.Len() {
			n.currentIndex.rows.Close(params.ctx)
			n.currentIndex = nil
		}
		return true, nil
	}

	return false, nil
}

// getNextIndexError will translate an row returned from an index error
// and generate the corresponding SCRUB result row.
func getNextIndexError(
	p *planner, currentIndex *checkedIndex, row parser.Datums,
) (parser.Datums, error) {
	// Check if this row has results from the left. See the comment above
	// createIndexCheckQuery indicating why this is true.
	var isMissingIndexReferenceError bool
	if row[currentIndex.primaryIndexColumnIndexes[0]] != parser.DNull {
		isMissingIndexReferenceError = true
	}

	colLen := len(currentIndex.columns)
	var primaryKeyDatums parser.Datums
	if isMissingIndexReferenceError {
		// Fetch the primary index values from the primary index row data.
		for _, rowIdx := range currentIndex.primaryIndexColumnIndexes {
			primaryKeyDatums = append(primaryKeyDatums, row[rowIdx])
		}
	} else {
		// Fetch the primary index values from the secondary index row
		// data, because no primary index was found. The secondary index columns
		// are offset by the length of the distinct columns, as the first
		// set of columns is for the primary index.
		for _, rowIdx := range currentIndex.primaryIndexColumnIndexes {
			primaryKeyDatums = append(primaryKeyDatums, row[rowIdx+colLen])
		}
	}
	primaryKey := parser.NewDString(primaryKeyDatums.String())
	timestamp := parser.MakeDTimestamp(
		p.evalCtx.GetStmtTimestamp(), time.Nanosecond)

	var errorType parser.Datum
	if isMissingIndexReferenceError {
		errorType = parser.NewDString(ScrubErrorMissingIndexEntry)
	} else {
		errorType = parser.NewDString(ScrubErrorDanglingIndexReference)
	}

	details := make(map[string]interface{})
	rowDetails := make(map[string]interface{})
	details["row_data"] = rowDetails
	details["index_name"] = currentIndex.details.indexDesc.Name
	if isMissingIndexReferenceError {
		// Fetch the primary index values from the primary index row data.
		for rowIdx, col := range currentIndex.columns {
			// TODO(joey): We should maybe try to get the underlying type.
			rowDetails[col.Name] = row[rowIdx].String()
		}
	} else {
		// Fetch the primary index values from the secondary index row data,
		// because no primary index was found. The secondary index columns
		// are offset by the length of the distinct columns, as the first
		// set of columns is for the primary index.
		for rowIdx, col := range currentIndex.columns {
			// TODO(joey): We should maybe try to get the underlying type.
			rowDetails[col.Name] = row[rowIdx+colLen].String()
		}
	}

	detailsJSON, err := parser.MakeDJSON(details)
	if err != nil {
		return nil, err
	}

	return parser.Datums{
		// TODO(joey): Add the job UUID once the SCRUB command uses jobs.
		parser.DNull, /* job_uuid */
		errorType,
		parser.NewDString(currentIndex.details.tableName.Database()),
		parser.NewDString(currentIndex.details.tableName.Table()),
		primaryKey,
		timestamp,
		parser.DBoolFalse,
		detailsJSON,
	}, nil
}

func (n *scrubNode) Close(ctx context.Context) {
}

func (n *scrubNode) Values() parser.Datums {
	return n.row
}
