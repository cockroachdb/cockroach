// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	tableDesc catalog.TableDescriptor
	index     catalog.Index
	asOf      hlc.Timestamp

	// columns is a list of the columns returned by one side of the
	// queries join. The actual resulting rows from the RowContainer is
	// twice this.
	columns []catalog.Column
	// primaryColIdxs maps PrimaryIndex.Columns to the row
	// indexes in the query result tree.Datums.
	primaryColIdxs []int

	run indexCheckRun
}

// indexCheckRun contains the run-time state for indexCheckOperation
// during local execution.
type indexCheckRun struct {
	started  bool
	rows     []tree.Datums
	rowIndex int
}

func newIndexCheckOperation(
	tableName *tree.TableName,
	tableDesc catalog.TableDescriptor,
	index catalog.Index,
	asOf hlc.Timestamp,
) *indexCheckOperation {
	return &indexCheckOperation{
		tableName: tableName,
		tableDesc: tableDesc,
		index:     index,
		asOf:      asOf,
	}
}

// Start will plan and run an index check using the distSQL execution
// engine.
func (o *indexCheckOperation) Start(params runParams) error {
	ctx := params.ctx

	var colToIdx catalog.TableColMap
	for _, c := range o.tableDesc.PublicColumns() {
		colToIdx.Set(c.GetID(), c.Ordinal())
	}

	var pkColumns, otherColumns []catalog.Column

	for i := 0; i < o.tableDesc.GetPrimaryIndex().NumKeyColumns(); i++ {
		colID := o.tableDesc.GetPrimaryIndex().GetKeyColumnID(i)
		col := o.tableDesc.PublicColumns()[colToIdx.GetDefault(colID)]
		pkColumns = append(pkColumns, col)
		colToIdx.Set(colID, -1)
	}

	// Collect all of the columns we are fetching from the index. This
	// includes the columns involved in the index: columns, extra columns,
	// and store columns.
	colIDs := catalog.TableColSet{}
	colIDs.UnionWith(o.index.CollectKeyColumnIDs())
	colIDs.UnionWith(o.index.CollectSecondaryStoredColumnIDs())
	colIDs.UnionWith(o.index.CollectKeySuffixColumnIDs())
	colIDs.ForEach(func(colID descpb.ColumnID) {
		pos := colToIdx.GetDefault(colID)
		if pos == -1 {
			return
		}
		col := o.tableDesc.PublicColumns()[pos]
		otherColumns = append(otherColumns, col)
	})

	colNames := func(cols []catalog.Column) []string {
		res := make([]string, len(cols))
		for i := range cols {
			res[i] = cols[i].GetName()
		}
		return res
	}

	checkQuery := createIndexCheckQuery(
		colNames(pkColumns), colNames(otherColumns), o.tableDesc.GetID(), o.index.GetID(),
	)

	rows, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.QueryBuffered(
		ctx, "scrub-index", params.p.txn, checkQuery,
	)
	if err != nil {
		return err
	}

	o.run.started = true
	o.run.rows = rows
	o.primaryColIdxs = make([]int, len(pkColumns))
	for i := range o.primaryColIdxs {
		o.primaryColIdxs[i] = i
	}
	o.columns = append(pkColumns, otherColumns...)
	return nil
}

// Next implements the checkOperation interface.
func (o *indexCheckOperation) Next(params runParams) (tree.Datums, error) {
	row := o.run.rows[o.run.rowIndex]
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
	timestamp, err := tree.MakeDTimestamp(
		params.extendedEvalCtx.GetStmtTimestamp(), time.Nanosecond)
	if err != nil {
		return nil, err
	}

	details := make(map[string]interface{})
	rowDetails := make(map[string]interface{})
	details["row_data"] = rowDetails
	details["index_name"] = o.index.GetName()
	if isMissingIndexReferenceError {
		// Fetch the primary index values from the primary index row data.
		for rowIdx, col := range o.columns {
			// TODO(joey): We should maybe try to get the underlying type.
			rowDetails[col.GetName()] = row[rowIdx].String()
		}
	} else {
		// Fetch the primary index values from the secondary index row data,
		// because no primary index was found. The secondary index columns
		// are offset by the length of the distinct columns, as the first
		// set of columns is for the primary index.
		for rowIdx, col := range o.columns {
			// TODO(joey): We should maybe try to get the underlying type.
			rowDetails[col.GetName()] = row[rowIdx+colLen].String()
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
	return o.run.rows == nil || o.run.rowIndex >= len(o.run.rows)
}

// Close4 implements the checkOperation interface.
func (o *indexCheckOperation) Close(ctx context.Context) {
	o.run.rows = nil
}

// createIndexCheckQuery will make the index check query for a given
// table and secondary index.
//
// The primary column names and the rest of the index
// columnsIt will also take into account an AS OF
// SYSTEM TIME clause.
//
// For example, given the following table schema:
//
//   CREATE TABLE table (
//     k INT, l INT, a INT, b INT, c INT,
//     PRIMARY KEY (k, l),
//     INDEX idx (a,b),
//   )
//
// The generated query to check the `v_idx` will be:
//
//   SELECT pri.k  pri.l, pri.a, pri.b,
//          sec.k, sec.l, sec.a, sec.b
//   FROM
//     (SELECT k, l, a, b FROM [tbl_id AS table_pri]@{FORCE_INDEX=[1]}) AS pri
//   FULL OUTER JOIN
//     (SELECT k, l, a, b FROM [tbl_id AS table_sec]@{FORCE_INDEX=[idx_id]} AS sec
//   ON
//     pri.k = sec.k AND
//     pri.l = sec.l AND
//     pri.a IS NOT DISTINCT FROM sec.a AND
//     pri.b IS NOT DISTINCT FROM sec.b
//   WHERE
//     pri.k IS NULL OR sec.k IS NULL
//
// Explanation:
//   1) We scan both the primary index and the secondary index.
//
//   2) We join them on equality on the PK columns and "IS NOT DISTINCT FROM" on
//      the other index columns. "IS NOT DISTINCT FROM" is like equality except
//      that NULL equals NULL; it is not needed for the PK columns because those
//      can't be NULL.
//
//      Note: currently, only the PK columns will be used as join equality
//      columns, but that is sufficient.
//
//   3) We select the "outer" rows (those that had no match), effectively
//      achieving a "double" anti-join. We use the PK columns which cannot be
//      NULL except on these rows.
//
//   4) The results are as follows:
//       - if a PK column on the left is NULL, that means that the right-hand
//         side row from the secondary index had no match in the primary index.
//       - if a PK column on the right is NULL, that means that the left-hand
//         side row from the primary key had no match in the secondary index.
//
func createIndexCheckQuery(
	pkColumns []string, otherColumns []string, tableID descpb.ID, indexID descpb.IndexID,
) string {
	allColumns := append(pkColumns, otherColumns...)
	// We need to make sure we can handle the non-public column `rowid`
	// that is created for implicit primary keys. In order to do so, the
	// rendered columns need to explicit in the inner selects.
	const checkIndexQuery = `
    SELECT %[1]s, %[2]s
    FROM
      (SELECT %[8]s FROM [%[3]d AS table_pri]@{FORCE_INDEX=[1]}) AS pri
    FULL OUTER JOIN
      (SELECT %[8]s FROM [%[3]d AS table_sec]@{FORCE_INDEX=[%[4]d]}) AS sec
    ON %[5]s
    WHERE %[6]s IS NULL OR %[7]s IS NULL`
	return fmt.Sprintf(
		checkIndexQuery,

		// 1: pri.k, pri.l, pri.a, pri.b
		strings.Join(colRefs("pri", allColumns), ", "),

		// 2: sec.k, sec.l, sec.a, sec.b
		strings.Join(colRefs("sec", allColumns), ", "),

		// 3
		tableID,

		// 4
		indexID,

		// 5: pri.k = sec.k AND pri.l = sec.l AND
		//    pri.a IS NOT DISTINCT FROM sec.a AND pri.b IS NOT DISTINCT FROM sec.b
		// Note: otherColumns can be empty.
		strings.Join(
			append(
				pairwiseOp(colRefs("pri", pkColumns), colRefs("sec", pkColumns), "="),
				pairwiseOp(colRefs("pri", otherColumns), colRefs("sec", otherColumns), "IS NOT DISTINCT FROM")...,
			),
			" AND ",
		),

		// 6: pri.k
		colRef("pri", pkColumns[0]),

		// 7: sec.k
		colRef("sec", pkColumns[0]),

		// 8: k, l, a, b
		strings.Join(colRefs("", append(pkColumns, otherColumns...)), ", "),
	)
}
