// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// sqlUniqueConstraintCheckOperation is a check which validates a
// UNIQUE constraint on a table.
type sqlUniqueConstraintCheckOperation struct {
	tableName  *tree.TableName
	tableDesc  catalog.TableDescriptor
	constraint catalog.UniqueConstraint
	cols       []catid.ColumnID
	name       string
	asOf       hlc.Timestamp
	predicate  string

	// columns is a list of the columns returned in the query result
	// tree.Datums.
	columns []catalog.Column
	// primaryColIdxs maps PrimaryIndex.Columns to the row
	// indexes in the query result tree.Datums.
	primaryColIdxs []int

	run sqlCheckConstraintCheckRun
}

func newSQLUniqueWithIndexConstraintCheckOperation(
	tableName *tree.TableName,
	tableDesc catalog.TableDescriptor,
	constraint catalog.UniqueWithIndexConstraint,
	asOf hlc.Timestamp,
) *sqlUniqueConstraintCheckOperation {
	op := sqlUniqueConstraintCheckOperation{
		tableName:  tableName,
		tableDesc:  tableDesc,
		constraint: constraint,
		asOf:       asOf,
		cols:       constraint.IndexDesc().KeyColumnIDs,
		name:       constraint.GetName(),
		predicate:  constraint.GetPredicate(),
	}
	// Partitioning columns are prepended to the index but are not part of the
	// unique constraint, so we ignore them.
	if n := constraint.GetPartitioning().NumImplicitColumns(); n > 0 {
		op.cols = op.cols[n:]
	}
	return &op
}

func newSQLUniqueWithoutIndexConstraintCheckOperation(
	tableName *tree.TableName,
	tableDesc catalog.TableDescriptor,
	constraint catalog.UniqueWithoutIndexConstraint,
	asOf hlc.Timestamp,
) *sqlUniqueConstraintCheckOperation {
	op := sqlUniqueConstraintCheckOperation{
		tableName:  tableName,
		tableDesc:  tableDesc,
		constraint: constraint,
		asOf:       asOf,
		cols:       constraint.UniqueWithoutIndexDesc().ColumnIDs,
		name:       constraint.GetName(),
		predicate:  constraint.GetPredicate(),
	}
	return &op
}

// Start implements the checkOperation interface.
// It creates a SELECT expression and generates a plan from it, which
// then runs in the distSQL execution engine.
func (o *sqlUniqueConstraintCheckOperation) Start(params runParams) error {
	ctx := params.ctx
	// Create a query of the form:
	// SELECT a,b,c FROM db.t AS tbl1 JOIN
	//   (SELECT b, c FROM db.t GROUP BY b, c
	//     WHERE b IS NOT NULL AND c IS NOT NULL [AND partial index predicate]
	//     HAVING COUNT(*) > 1) as tbl2
	//   ON tbl1.b = tbl2.b AND tbl1.c = tbl2.c;
	// Where a, b, and c are all the public columns in table db.t and b and c are
	// the unique columns. We select all public columns to provide detailed
	// information if there are constraint violations.

	// Collect all the columns.
	o.columns = o.tableDesc.PublicColumns()

	// Make a list of all the unique column names.
	keyCols := make([]string, len(o.cols))
	matchers := make([]string, len(o.cols))
	for i := 0; i < len(o.cols); i++ {
		col, err := catalog.MustFindColumnByID(o.tableDesc, o.cols[i])
		if err != nil {
			return err
		}
		keyCols[i] = tree.NameString(col.GetName())
		matchers[i] = fmt.Sprintf("tbl1.%[1]s=tbl2.%[1]s", keyCols[i])
	}
	// Make a list of all the public column names.
	pCols := make([]string, len(o.columns))
	for i := 0; i < len(o.columns); i++ {
		pCols[i] = fmt.Sprintf("tbl1.%[1]s", tree.NameString(o.columns[i].GetName()))
	}
	asOf := ""
	if o.asOf != hlc.MaxTimestamp {
		asOf = fmt.Sprintf("AS OF SYSTEM TIME '%s'", o.asOf.AsOfSystemTime())
	}
	tableName := fmt.Sprintf("%s.%s", o.tableName.Catalog(), o.tableName.Table())
	dup, _, err := duplicateRowQuery(o.tableDesc, o.cols, o.predicate,
		0 /* indexIDForValidation */, false /* limitResults */)
	if err != nil {
		return err
	}

	sel := fmt.Sprintf(`SELECT %[1]s 
FROM %[2]s AS tbl1 JOIN 
(%[3]s) AS tbl2 
ON %[4]s
%[5]s `,
		strings.Join(pCols, ","),        // 1
		tableName,                       // 2
		dup,                             // 3
		strings.Join(matchers, " AND "), // 4
		asOf,                            // 5
	)

	rows, err := params.p.InternalSQLTxn().QueryBuffered(
		ctx, "scrub-unique", params.p.txn, sel,
	)
	if err != nil {
		return err
	}

	o.run.started = true
	o.run.rows = rows
	// Find the row indexes for all of the primary index columns.
	o.primaryColIdxs, err = getPrimaryColIdxs(o.tableDesc, o.columns)
	return err
}

// Next implements the checkOperation interface.
func (o *sqlUniqueConstraintCheckOperation) Next(params runParams) (tree.Datums, error) {
	row := o.run.rows[o.run.rowIndex]
	o.run.rowIndex++
	timestamp, err := tree.MakeDTimestamp(
		params.extendedEvalCtx.GetStmtTimestamp(),
		time.Nanosecond,
	)
	if err != nil {
		return nil, err
	}

	var primaryKeyDatums tree.Datums
	for _, rowIdx := range o.primaryColIdxs {
		primaryKeyDatums = append(primaryKeyDatums, row[rowIdx])
	}

	details := make(map[string]interface{})
	rowDetails := make(map[string]interface{})
	details["row_data"] = rowDetails
	details["constraint_name"] = o.name
	for rowIdx, col := range o.columns {
		rowDetails[col.GetName()] = row[rowIdx].String()
	}
	detailsJSON, err := tree.MakeDJSON(details)
	if err != nil {
		return nil, err
	}

	return tree.Datums{
		tree.DNull, /* job_uuid */
		tree.NewDString(scrub.UniqueConstraintViolation),
		tree.NewDString(o.tableName.Catalog()),
		tree.NewDString(o.tableName.Table()),
		tree.NewDString(primaryKeyDatums.String()),
		timestamp,
		tree.DBoolFalse,
		detailsJSON,
	}, nil
}

// Started implements the checkOperation interface.
func (o *sqlUniqueConstraintCheckOperation) Started() bool {
	return o.run.started
}

// Done implements the checkOperation interface.
func (o *sqlUniqueConstraintCheckOperation) Done(ctx context.Context) bool {
	return o.run.rows == nil || o.run.rowIndex >= len(o.run.rows)
}

// Close implements the checkOperation interface.
func (o *sqlUniqueConstraintCheckOperation) Close(ctx context.Context) {
	o.run.rows = nil
}
