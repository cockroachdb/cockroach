// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logical

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type applierDecision string

const (
	noDecision applierDecision = ""
	// ignoreProposed indicates that the mutation should not be applied.
	ignoreProposed applierDecision = "ignore_proposed"
	// acceptProposed indicates that the mutation should be applied. An insert
	// or update will be upserted. A delete will be deleted.
	acceptProposed applierDecision = "accept_proposed"
	// upsertProposed indicatest that an insert or update mutation should be applied.
	upsertProposed  applierDecision = "upsert_proposed"
	deleteProposed  applierDecision = "delete_proposed"
	upsertSpecified applierDecision = "upsert_specified"
)

type mutationType string

const (
	insertMutation mutationType = "insert"
	updateMutation mutationType = "update"
	deleteMutation mutationType = "delete"
)

const (
	insertQuery = 0
	updateQuery = 1
	deleteQuery = 2
)

const (
	applierTypes = `
-- crdb_replication_applier_decision is the return type for any user-provided
-- function. The row field is the row to be insert when the decision is upsert_specified.
-- Note that users need to take special care that the columns returned in row are in canonical
-- order. It isn't particularly clear how the user is going to do this easily.
CREATE TYPE crdb_replication_applier_decision AS (decision STRING, row RECORD)`

	applierQueryBase = `
WITH data (%s)
AS (VALUES (%s))
SELECT (res).decision, (res).row FROM
(
	SELECT %s('%s', data, existing, (%s), existing.crdb_internal_mvcc_timestamp) AS res
	FROM data LEFT JOIN [%d as existing]
	%s
)`
	applierUpsertQueryBase = `UPSERT INTO [%d as t] (%s) VALUES (%s)`
	applierDeleteQueryBase = `DELETE FROM [%d as t] WHERE %s`
)

// applierQuerier is a SQL querier that applies rows only after
// defering to a user-provided UDF.
//
// The UDF receives the proposed action, proposed row, and existing row
// and then returns an indication of how to handle the proposed action.
//
// It would be nice if the response to the user's decision could be handled in
// the same query. But currently we issue two queries after doing some dispatch
// based on the user's decision.
//
// Current caveats:
//
//   - We don't support tables with hidden columns. This includes
//     columns such as rowid or crdb_region.
//
//   - The user can only issue 1 action in response to 1 mutation because
//     plpgsql doesn't support SETOF returns.
//
//   - Compute columns will always be NULL in the proposed value
//     passed to the UDF.
type applierQuerier struct {
	udfName     string
	settings    *cluster.Settings
	queryBuffer queryBuffer
	sd          sessiondata.InternalExecutorOverride
}

func makeUDFApplierProcessor(
	ctx context.Context,
	settings *cluster.Settings,
	tableDescs map[int32]descpb.TableDescriptor,
	ie isql.Executor,
) (*sqlRowProcessor, error) {
	qb := queryBuffer{
		deleteQueries:  make(map[catid.DescID]queryBuilder, len(tableDescs)),
		insertQueries:  make(map[catid.DescID]map[catid.FamilyID]queryBuilder, len(tableDescs)),
		applierQueries: make(map[catid.DescID]map[catid.FamilyID]queryBuilder, len(tableDescs)),
	}

	getDatabaseNameQuery := `SELECT name FROM system.namespace WHERE id IN (SELECT "parentID" FROM system.namespace where id = $1)`
	var tableID int32
	for t := range tableDescs {
		tableID = t
		break
	}

	datums, err := ie.QueryRow(ctx, "replication-get-database", nil, getDatabaseNameQuery, tableID)
	if err != nil {
		return nil, err
	}
	databaseName, ok := datums[0].(*tree.DString)
	if !ok {
		return nil, errors.AssertionFailedf("unexpected type for database name: %T", datums[0])
	}

	sd := ieOverrides
	sd.Database = string(*databaseName)

	return makeSQLProcessorFromQuerier(ctx, settings, tableDescs, ie, &applierQuerier{
		queryBuffer: qb,
		settings:    settings,
		sd:          sd,
		// TODO(ssd): Thread this name through from the SQL statement.
		udfName: "repl_apply",
	})
}

func (aq *applierQuerier) AddTable(targetDescID int32, td catalog.TableDescriptor) error {
	var err error
	aq.queryBuffer.applierQueries[td.GetID()], err = makeApplierApplyQueries(targetDescID, td, aq.udfName)
	if err != nil {
		return err
	}
	aq.queryBuffer.insertQueries[td.GetID()], err = makeApplierInsertQueries(targetDescID, td)
	if err != nil {
		return err
	}
	aq.queryBuffer.deleteQueries[td.GetID()], err = makeApplierDeleteQuery(targetDescID, td)
	return err
}

func (aq *applierQuerier) RequiresParsedBeforeRow() bool { return true }

func (aq *applierQuerier) InsertRow(
	ctx context.Context,
	txn isql.Txn,
	ie isql.Executor,
	row cdcevent.Row,
	prevRow *cdcevent.Row,
	likelyInsert bool,
) (batchStats, error) {
	mutType := updateMutation
	if likelyInsert {
		mutType = insertMutation
	}
	return aq.processRow(ctx, txn, ie, row, prevRow, mutType)
}

func (aq *applierQuerier) DeleteRow(
	ctx context.Context, txn isql.Txn, ie isql.Executor, row cdcevent.Row, prevRow *cdcevent.Row,
) (batchStats, error) {
	return aq.processRow(ctx, txn, ie, row, prevRow, deleteMutation)
}

func (aq *applierQuerier) processRow(
	ctx context.Context,
	txn isql.Txn,
	ie isql.Executor,
	row cdcevent.Row,
	prevRow *cdcevent.Row,
	mutType mutationType,
) (batchStats, error) {
	var kvTxn *kv.Txn
	if txn != nil {
		kvTxn = txn.KV()
	}
	decision, decisionRow, err := aq.applyUDF(ctx, kvTxn, ie, mutType, row, prevRow)
	if err != nil {
		return batchStats{}, err
	}
	return aq.applyDecicion(ctx, kvTxn, ie, row, decision, decisionRow)
}

func (aq *applierQuerier) applyUDF(
	ctx context.Context,
	txn *kv.Txn,
	ie isql.Executor,
	mutType mutationType,
	row cdcevent.Row,
	prevRow *cdcevent.Row,
) (applierDecision, tree.Datums, error) {
	applyQueryBuilder, err := aq.queryBuffer.ApplierQueryForRow(row)
	if err != nil {
		return noDecision, nil, err
	}
	if err := applyQueryBuilder.AddRowDefaultNull(&row); err != nil {
		return noDecision, nil, err
	}
	if err := applyQueryBuilder.AddRowDefaultNull(prevRow); err != nil {
		return noDecision, nil, err
	}

	var query int
	switch mutType {
	case insertMutation:
		query = insertQuery
	case updateMutation:
		query = updateQuery
	case deleteMutation:
		query = deleteQuery
	}

	stmt, datums, err := applyQueryBuilder.Query(query)
	if err != nil {
		return noDecision, nil, err
	}

	decisionRow, err := aq.queryRowExParsed(ctx, "replicated-apply-udf", txn, ie, stmt, datums...)
	if err != nil {
		return noDecision, nil, err
	}
	if len(decisionRow) < 2 {
		return noDecision, nil, errors.Errorf("unexpected number of return values from custom UDF: %d", len(decisionRow))
	}
	decisionStr, ok := decisionRow[0].(*tree.DString)
	if !ok {
		return noDecision, nil, errors.Errorf("unexpected return type for first return value from custom UDF: %v", decisionRow[0])
	}

	var mutatedDatums tree.Datums
	mutatedDatumsTuple, ok := decisionRow[1].(*tree.DTuple)
	if ok {
		mutatedDatums = mutatedDatumsTuple.D
	}

	decision := applierDecision(*decisionStr)
	if decision == acceptProposed {
		switch mutType {
		case deleteMutation:
			decision = deleteProposed
		default:
			decision = upsertProposed
		}
	}

	return decision, mutatedDatums, nil
}

func (aq *applierQuerier) applyDecicion(
	ctx context.Context,
	txn *kv.Txn,
	ie isql.Executor,
	row cdcevent.Row,
	decision applierDecision,
	decisionRow tree.Datums,
) (batchStats, error) {
	switch decision {
	case ignoreProposed:
		return batchStats{}, nil
	case deleteProposed:
		dq, err := aq.queryBuffer.DeleteQueryForRow(row)
		if err != nil {
			return batchStats{}, err
		}
		if err := dq.AddRow(row); err != nil {
			return batchStats{}, err
		}
		stmt, datums, err := dq.Query(defaultQuery)
		if err != nil {
			return batchStats{}, err
		}
		if err := aq.execParsed(ctx, "replicated-delete", txn, ie, stmt, datums...); err != nil {
			return batchStats{}, err
		}
		return batchStats{}, nil
	case upsertProposed:
		q, err := aq.queryBuffer.InsertQueryForRow(row)
		if err != nil {
			return batchStats{}, err
		}
		if err := q.AddRow(row); err != nil {
			return batchStats{}, err
		}
		stmt, datums, err := q.Query(defaultQuery)
		if err != nil {
			return batchStats{}, err
		}
		if err := aq.execParsed(ctx, "replicated-insert", txn, ie, stmt, datums...); err != nil {
			return batchStats{}, err
		}
		return batchStats{}, nil
	case upsertSpecified:
		q, err := aq.queryBuffer.InsertQueryForRow(row)
		if err != nil {
			return batchStats{}, err
		}
		stmt, _, err := q.Query(defaultQuery)
		if err != nil {
			return batchStats{}, err
		}
		datums := make([]interface{}, len(decisionRow)+1)
		for i := range decisionRow {
			datums[i] = decisionRow[i]
		}
		// TODO(ssd): Should we even record an
		// crdb_replication_origin_timestamp here. Perhaps the UDF mode
		// should leave this completely to the user and not use this
		// extra column at all since in the case of upsertSpecfied, the
		// origin is really the UDF itself, not the remote cluster.
		datums[len(datums)-1] = &tree.DDecimal{Decimal: eval.TimestampToDecimal(row.MvccTimestamp)}
		if err := aq.execParsed(ctx, "replicated-insert", txn, ie, stmt, datums...); err != nil {
			return batchStats{}, err
		}
		return batchStats{}, nil
	default:
		return batchStats{}, errors.Errorf("unimplemented or unknown decision: %s", decision)
	}
}

func (aq *applierQuerier) execParsed(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	ie isql.Executor,
	stmt statements.Statement[tree.Statement],
	datums ...interface{},
) error {
	if _, err := ie.ExecParsed(ctx, opName, txn, aq.sd, stmt, datums...); err != nil {
		log.Warningf(ctx, "%s failed (query: %s): %s", opName, stmt.SQL, err.Error())
		return err
	}
	return nil
}

func (aq *applierQuerier) queryRowExParsed(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	ie isql.Executor,
	stmt statements.Statement[tree.Statement],
	datums ...interface{},
) (tree.Datums, error) {
	if row, err := ie.QueryRowExParsed(ctx, opName, txn, aq.sd, stmt, datums...); err != nil {
		log.Warningf(ctx, "%s failed (query: %s): %s", opName, stmt.SQL, err.Error())
		return nil, err
	} else {
		return row, nil
	}
}

// makeApplierJoinClause creates a join clause for all primary keys.  It would
// be _nice_ if we could instead use the 'USING(...)' syntax here but it appears
// that NOT VISIBLE columns can't be referenced in a USING clause.
//
// Ref: https://github.com/cockroachdb/cockroach/issues/126767
func makeApplierJoinClause(pkColumns []string) string {
	var joinClause strings.Builder
	joinClause.WriteString("ON ")
	addJoinColumn := func(name string) {
		name = lexbase.EscapeSQLIdent(name)
		fmt.Fprintf(&joinClause, "data.%s = existing.%s", name, name)
	}

	addJoinColumn(pkColumns[0])
	if len(pkColumns) > 1 {
		for _, name := range pkColumns[1:] {
			joinClause.WriteString(" AND ")
			addJoinColumn(name)
		}
	}
	return joinClause.String()
}

func escapedColumnNameList(names []string) string {
	if len(names) < 1 {
		return ""
	}
	var nameList strings.Builder
	nameList.WriteString(lexbase.EscapeSQLIdent(names[0]))
	if len(names) > 1 {
		for _, name := range names[1:] {
			nameList.WriteString(",")
			nameList.WriteString(lexbase.EscapeSQLIdent(name))
		}
	}
	return nameList.String()
}

func makeApplierApplyQueries(
	dstTableDescID int32, td catalog.TableDescriptor, udfName string,
) (map[catid.FamilyID]queryBuilder, error) {
	if td.NumFamilies() > 1 {
		return nil, errors.Errorf("multiple-column familes not supported by the custom-UDF applier")
	}

	// We pass all Visible columns into the applier. We can't pass non-visible
	// columns currently as we are using the implicit record type for the table
	// as the argument type for the UDF. Implicit record types only contain
	// visible columns. As a result, we can't currently support tables that have
	// a hidden rowid column using this applier.
	visColumns := td.VisibleColumns()
	inputColumnNames := make([]string, len(visColumns))
	for i := range visColumns {
		inputColumnNames[i] = visColumns[i].GetName()
	}
	colCount := len(inputColumnNames)
	colNames := escapedColumnNameList(inputColumnNames)
	valStr := valueStringForNumItems(colCount, 1)
	prevValStr := valueStringForNumItems(colCount, colCount+1)
	joinClause := makeApplierJoinClause(td.TableDesc().PrimaryIndex.KeyColumnNames)

	statements := make([]statements.Statement[tree.Statement], 3)
	for statementIdx, mutType := range map[int]mutationType{
		insertQuery: insertMutation,
		updateQuery: updateMutation,
		deleteQuery: deleteMutation,
	} {
		q := fmt.Sprintf(applierQueryBase,
			colNames,
			valStr,
			udfName,
			mutType,
			prevValStr,
			dstTableDescID,
			joinClause,
		)
		var err error
		statements[statementIdx], err = parser.ParseOne(q)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing %s", q)
		}
	}
	return map[catid.FamilyID]queryBuilder{
		0: {
			stmts:         statements,
			inputColumns:  inputColumnNames,
			scratchDatums: make([]interface{}, len(inputColumnNames)+1),
		}}, nil
}

func makeApplierInsertQueries(
	dstTableDescID int32, td catalog.TableDescriptor,
) (map[catid.FamilyID]queryBuilder, error) {
	queryBuilders := make(map[catid.FamilyID]queryBuilder, td.NumFamilies())
	if err := td.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
		inputColumnNames, err := insertColumnNamesForFamily(td, family, false)
		if err != nil {
			return err
		}
		colNames := escapedColumnNameList(append(inputColumnNames, originTimestampColumnName))
		valStr := valueStringForNumItems(len(inputColumnNames)+1, 1)
		upsertQuery, err := parser.ParseOne(fmt.Sprintf(applierUpsertQueryBase,
			dstTableDescID,
			colNames,
			valStr,
		))
		if err != nil {
			return err
		}

		queryBuilders[family.ID] = queryBuilder{
			stmts:                []statements.Statement[tree.Statement]{upsertQuery},
			needsOriginTimestamp: true,
			inputColumns:         inputColumnNames,
			scratchDatums:        make([]interface{}, len(inputColumnNames)+1),
		}
		return err
	}); err != nil {
		return nil, err
	}
	return queryBuilders, nil
}

func makeApplierDeleteQuery(
	dstTableDescID int32, td catalog.TableDescriptor,
) (queryBuilder, error) {
	var whereClause strings.Builder
	names := td.TableDesc().PrimaryIndex.KeyColumnNames
	for i := range names {
		colName := lexbase.EscapeSQLIdent(names[i])
		if i == 0 {
			fmt.Fprintf(&whereClause, "%s = $%d", colName, i+1)
		} else {
			fmt.Fprintf(&whereClause, "AND %s = $%d", colName, i+1)
		}
	}
	stmt, err := parser.ParseOne(
		fmt.Sprintf(applierDeleteQueryBase, dstTableDescID, whereClause.String()))
	if err != nil {
		return queryBuilder{}, err
	}

	return queryBuilder{
		stmts:         []statements.Statement[tree.Statement]{stmt},
		inputColumns:  names,
		scratchDatums: make([]interface{}, len(names)),
	}, nil
}
