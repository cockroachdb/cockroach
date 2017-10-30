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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

// ScrubError are the possble error strings for the error_type column of
// SCRUB.
type ScrubError string

const (
	// ScrubErrorMissingIndexEntry occurs when a primary k/v is missing a
	// corresponding secondary index k/v.
	ScrubErrorMissingIndexEntry ScrubError = "missing_index_entry"
	// ScrubErrorDanglingIndexReference occurs when a secondary index k/v
	// points to a non-existing primary k/v.
	ScrubErrorDanglingIndexReference = "dangling_index_reference"
)

type scrubNode struct {
	optColumnsSlot

	n *parser.Scrub

	checkedIndexes []*checkedIndex

	row parser.Datums
}

// checkedIndex holds the intermediate results for one index that is
// checked.
type checkedIndex struct {
	// Intermediate values.
	rows     *sqlbase.RowContainer
	rowIndex int

	// Context of the index check.
	databaseName string
	tableName    string
	indexName    string
	// columns is a list of the columns returned in the query result
	// parser.Datums.
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
	{Name: "job_uuid", Typ: parser.TypeUUID},
	{Name: "error_type", Typ: parser.TypeString},
	{Name: "database", Typ: parser.TypeString},
	{Name: "table", Typ: parser.TypeString},
	{Name: "primary_key", Typ: parser.TypeString},
	{Name: "timestamp", Typ: parser.TypeTimestamp},
	{Name: "repaired", Typ: parser.TypeBool},
	// TODO(joey): We can use the DJSON once parser.MakeDJSON is merged.
	{Name: "details", Typ: parser.TypeString},
}

func (n *scrubNode) Start(params runParams) error {
	tn, err := n.n.Table.NormalizeWithDatabaseName(params.p.session.Database)
	if err != nil {
		return err
	}

	tableDesc, err := params.p.getTableDesc(params.ctx, tn)
	if err != nil {
		return err
	}

	if tableDesc.IsView() {
		return pgerror.NewErrorf(pgerror.CodeWrongObjectTypeError, "cannot run SCRUB on views")
	}

	// Process SCRUB options
	var indexes []*sqlbase.IndexDescriptor
	for _, option := range n.n.Options {
		switch v := option.(type) {
		case *parser.ScrubOptionIndex:
			if indexes != nil {
				return pgerror.NewErrorf(pgerror.CodeSyntaxError,
					"cannot specify INDEX option more than once")
			}
			indexes, err = indexesToCheck(v.IndexNames, tableDesc)
			if err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("Unhandled SCRUB option received: %+v", v))
		}
	}

	// No options were provided. By default exhaustive checks are run.
	if len(n.n.Options) == 0 {
		indexes, err = indexesToCheck(nil /* indexNames */, tableDesc)
		if err != nil {
			return err
		}
	}

	// Run the index check on each index.
	if indexes != nil {
		n.checkedIndexes = []*checkedIndex(nil)
		for _, index := range indexes {
			if err := n.startIndexCheck(params.ctx, params.p, tableDesc, tn, index); err != nil {
				return err
			}
		}
	}
	return nil
}

// startIndexCheck will plan and run the index check using the distSQL
// execution engine.
func (n *scrubNode) startIndexCheck(
	ctx context.Context,
	p *planner,
	tableDesc *sqlbase.TableDescriptor,
	tableName *parser.TableName,
	indexDesc *sqlbase.IndexDescriptor,
) error {
	colToIdx := make(map[sqlbase.ColumnID]int)
	for i, col := range tableDesc.Columns {
		colToIdx[col.ID] = i
	}

	// Collect all of the columns we are fetching from the secondary
	// index. This includes the columns involved in the index, and extra
	// columns.
	columns := []*sqlbase.ColumnDescriptor(nil)
	for _, colID := range indexDesc.ColumnIDs {
		columns = append(columns, &tableDesc.Columns[colToIdx[colID]])
	}
	for _, colID := range indexDesc.ExtraColumnIDs {
		columns = append(columns, &tableDesc.Columns[colToIdx[colID]])
	}

	// Collect the column types.
	columnTypes := []sqlbase.ColumnType(nil)
	for _, col := range columns {
		columnTypes = append(columnTypes, col.Type)
	}

	// Because the row results include both primary key data and secondary
	// key data, the row results will contain two copies of the column
	// data.
	columnTypes = append(columnTypes, columnTypes...)

	// Find the row indexes for all of the primary index columns.
	primaryColumnRowIndexes := []int(nil)
	for i, colID := range tableDesc.PrimaryIndex.ColumnIDs {
		rowIdx := -1
		for idx, col := range columns {
			if col.ID == colID {
				rowIdx = idx
				break
			}
		}
		// FIXME(joey): This is a bit of defensive programming, in the event
		// the IndexDescriptor doesn't contain all primary index columns. Is
		// this really necessary?
		if rowIdx == -1 {
			return errors.Errorf(
				"could not find primary index column in projection: columnID=%d columnName=%s",
				colID,
				tableDesc.PrimaryIndex.ColumnNames[i])
		}
		primaryColumnRowIndexes = append(primaryColumnRowIndexes, rowIdx)
	}

	checkQuery := createIndexCheckQuery(columns, tableName, indexDesc.ID)
	plan, err := p.delegateQuery(ctx, "SCRUB TABLE ... INDEX", checkQuery, nil, nil)
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

	ci := sqlbase.ColTypeInfoFromColTypes(columnTypes)
	// FIXME(joey): Usage of rows.Close() may be incorrect. Currently it is:
	// - Closed after reading the rows
	// - Closed if an error is received, and we don't expect rows, or use the container.
	rows := sqlbase.NewRowContainer(*p.evalCtx.ActiveMemAcc, ci, 0)
	rowResultWriter := NewRowResultWriter(parser.Rows, rows)
	recv, err := makeDistSQLReceiver(
		ctx,
		rowResultWriter,
		nil, /* rangeCache */
		nil, /* leaseCache */
		p.txn,
		nil, /* updateClock */
	)
	if err != nil {
		rows.Close(ctx)
		return err
	}

	err = p.session.distSQLPlanner.PlanAndRun(ctx, p.txn, plan, &recv, p.evalCtx)
	if err != nil {
		rows.Close(ctx)
		return err
	} else if recv.err != nil {
		rows.Close(ctx)
		return recv.err
	}

	if rows.Len() > 0 {
		n.checkedIndexes = append(n.checkedIndexes, &checkedIndex{
			rows:                      rows,
			tableName:                 tableName.Table(),
			databaseName:              tableName.Database(),
			indexName:                 indexDesc.Name,
			primaryIndexColumnIndexes: primaryColumnRowIndexes,
			columns:                   columns,
		})
	} else {
		rows.Close(ctx)
	}

	return nil
}

// createIndexCheckQuery will make the index check query for a table and
// secondary index.
func createIndexCheckQuery(
	columns []*sqlbase.ColumnDescriptor, tableName *parser.TableName, indexID sqlbase.IndexID,
) string {
	const checkIndexQuery = `
				SELECT %[1]s, %[2]s
				FROM
					%[3]s@{NO_INDEX_JOIN} as t1
				FULL OUTER JOIN
					%[3]s@{FORCE_INDEX=[%[4]d],NO_INDEX_JOIN} as t2
					ON %[5]s
				WHERE (%[6]s) OR (%[7]s AND %[8]s)`

	// tableColumnsNullPredicate creates a predicate that checks if all of
	// the specified columns for a table are NULL or NOT NULL, as
	// indicated by isNull.
	tableColumnsNullPredicate := func(
		tableName string, columns []*sqlbase.ColumnDescriptor, isNull bool,
	) string {
		var buffer bytes.Buffer
		for i, col := range columns {
			if i > 0 {
				buffer.WriteString(" AND ")
			}
			if isNull {
				buffer.WriteString(fmt.Sprintf("%[1]s.%[2]s IS NULL", tableName, col.Name))
			} else {
				buffer.WriteString(fmt.Sprintf("%[1]s.%[2]s IS NOT NULL", tableName, col.Name))
			}
		}
		return buffer.String()
	}

	// tableColumnsEQ creates a predicate that checks if all of the
	// specified columns for two tables are equal.
	tableColumnsEQ := func(
		tableName string, otherTableName string, columns []*sqlbase.ColumnDescriptor,
	) string {
		var buffer bytes.Buffer
		for i, col := range columns {
			if i > 0 {
				buffer.WriteString(" AND ")
			}
			buffer.WriteString(fmt.Sprintf("%[1]s.%[3]s = %[2]s.%[3]s",
				tableName, otherTableName, col.Name))
		}
		return buffer.String()
	}

	// tableColumnsProjection creates the select projection statement (a
	// comma delimetered column list), for the specified table and
	// columns.
	tableColumnsProjection := func(tableName string, columns []*sqlbase.ColumnDescriptor) string {
		var buffer bytes.Buffer
		for i, col := range columns {
			if i > 0 {
				buffer.WriteString(", ")
			}
			buffer.WriteString(fmt.Sprintf("%[1]s.%[2]s", tableName, col.Name))
		}
		return buffer.String()
	}

	return fmt.Sprintf(checkIndexQuery,
		tableColumnsProjection("t1", columns),
		tableColumnsProjection("t2", columns),
		tableName,
		indexID,
		tableColumnsEQ("t1", "t2", columns),
		tableColumnsNullPredicate("t1", columns, true /* isNull */),
		tableColumnsNullPredicate("t1", columns, false /* isNull */),
		tableColumnsNullPredicate("t2", columns, true /* isNull */),
	)
}

// indexesToCheck will return all of the indexes that are being checked.
// If indexNames is nil, then all indexes are returned.
func indexesToCheck(
	indexNames parser.NameList, tableDesc *sqlbase.TableDescriptor,
) (results []*sqlbase.IndexDescriptor, err error) {
	if indexNames == nil {
		// Populate results with all secondary indexes of the
		// table.
		for _, idx := range tableDesc.Indexes {
			results = append(results, &idx)
		}
		return results, nil
	}

	// Find the indexes corresponding to the user input index names.
	names := make(map[string]struct{})
	for _, idxName := range indexNames {
		names[idxName.String()] = struct{}{}
	}
	for _, idx := range tableDesc.Indexes {
		if _, ok := names[idx.Name]; ok {
			results = append(results, &idx)
			delete(names, idx.Name)
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
	if n.checkedIndexes != nil && len(n.checkedIndexes) > 0 {
		checkedIndex := n.checkedIndexes[0]
		row := checkedIndex.rows.At(checkedIndex.rowIndex)

		// rowDataSize is the size for one index's row data. The total data
		// in row is twice that, as it includes row data for both the
		// primary index and the secondary index.
		rowDataSize := len(row) / 2

		// If any of the primary key values is present in the primary index
		// row data, then this row has no corresponding secondary index
		// entry.
		var isMissingIndexReferenceError bool
		if row[checkedIndex.primaryIndexColumnIndexes[0]] != parser.DNull {
			isMissingIndexReferenceError = true
		}

		primaryKeyDatums := parser.Datums{}
		if isMissingIndexReferenceError {
			// Fetch the primary index values from the primary index row data.
			for _, rowIdx := range checkedIndex.primaryIndexColumnIndexes {
				primaryKeyDatums = append(primaryKeyDatums, row[rowIdx])
			}
		} else {
			// Fetch the primary index values from the secondary index row
			// data, because no primary index was found.
			for _, rowIdx := range checkedIndex.primaryIndexColumnIndexes {
				primaryKeyDatums = append(primaryKeyDatums, row[rowIdx+rowDataSize])
			}
		}
		primaryKey := parser.NewDString(primaryKeyDatums.String())

		timestamp := parser.MakeDTimestamp(params.p.evalCtx.GetStmtTimestamp(), time.Nanosecond)

		var errorType parser.Datum
		if isMissingIndexReferenceError {
			errorType = parser.NewDString(string(ScrubErrorMissingIndexEntry))
		} else {
			errorType = parser.NewDString(string(ScrubErrorDanglingIndexReference))
		}

		details := make(map[string]interface{})
		rowDetails := make(map[string]interface{})
		details["row_data"] = rowDetails
		details["index_name"] = checkedIndex.indexName
		if isMissingIndexReferenceError {
			// Fetch the primary index values from the primary index row data.
			for rowIdx, col := range checkedIndex.columns {
				// FIXME(joey): Should we maybe try to get the underlying type?
				rowDetails[col.Name] = row[rowIdx].String()
			}
		} else {
			// Fetch the primary index values from the secondary index row
			// data, because no primary index was found.
			for rowIdx, col := range checkedIndex.columns {
				// FIXME(joey): Should we maybe try to get the underlying type?
				rowDetails[col.Name] = row[rowIdx+rowDataSize].String()
			}
		}

		// TODO(joey): We can use the new parser.MakeDJSON interface once
		// it's merged in, so the result type can be JSON as opposed to
		// STRING.
		detailsBytes, err := json.Marshal(details)
		if err != nil {
			return false, err
		}

		n.row = parser.Datums{
			// TODO(joey): Add the job UUID once the SCRUB command uses jobs.
			parser.DNull, /* job_uuid */
			errorType,    /* error_type */
			parser.NewDString(checkedIndex.databaseName),
			parser.NewDString(checkedIndex.tableName),
			primaryKey,
			timestamp,
			parser.DBoolFalse,
			parser.NewDString(string(detailsBytes)),
		}

		checkedIndex.rowIndex++
		if checkedIndex.rowIndex >= checkedIndex.rows.Len() {
			checkedIndex.rows.Close(params.ctx)
			n.checkedIndexes = n.checkedIndexes[1:]
		}
		return true, nil
	}

	return false, nil
}

func (n *scrubNode) Close(ctx context.Context) {
}

func (n *scrubNode) Values() parser.Datums {
	return n.row
}
