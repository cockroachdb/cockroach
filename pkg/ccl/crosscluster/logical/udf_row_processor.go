// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
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
	"github.com/cockroachdb/redact"
)

type applierDecision string

const (
	noDecision applierDecision = ""
	// ignoreProposed indicates that the mutation should not be applied.
	ignoreProposed applierDecision = "ignore_proposed"
	// acceptProposed indicates that the mutation should be applied. An insert
	// or update will be upserted. A delete will be deleted.
	acceptProposed applierDecision = "accept_proposed"
	// upsertProposed indicates that an insert or update mutation should be applied.
	upsertProposed applierDecision = "upsert_proposed"
	deleteProposed applierDecision = "delete_proposed"
)

const (
	insertQuery = 0
	updateQuery = 1
	deleteQuery = 2
)

const (
	applierQueryBase = `
WITH data (%s)
AS (VALUES (%s))
SELECT [FUNCTION %d]('%s', data, existing, ROW(%s), existing.crdb_internal_mvcc_timestamp, existing.crdb_internal_origin_timestamp, $%d) AS decision
FROM data LEFT JOIN [%d as existing]
%s`
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
	settings    *cluster.Settings
	queryBuffer queryBuffer

	ieoInsert, ieoDelete, ieoApplyUDF sessiondata.InternalExecutorOverride

	// Used for the applier query to reduce allocations.
	proposedMVCCTs tree.DDecimal
}

func makeApplierQuerier(
	ctx context.Context,
	settings *cluster.Settings,
	tableConfigByDestID map[descpb.ID]sqlProcessorTableConfig,
	jobID jobspb.JobID,
	ie isql.Executor,
) *applierQuerier {
	return &applierQuerier{
		queryBuffer: queryBuffer{
			deleteQueries:  make(map[catid.DescID]queryBuilder, len(tableConfigByDestID)),
			insertQueries:  make(map[catid.DescID]map[catid.FamilyID]queryBuilder, len(tableConfigByDestID)),
			applierQueries: make(map[catid.DescID]map[catid.FamilyID]queryBuilder, len(tableConfigByDestID)),
		},
		settings:    settings,
		ieoInsert:   getIEOverride(replicatedInsertOpName, jobID),
		ieoDelete:   getIEOverride(replicatedDeleteOpName, jobID),
		ieoApplyUDF: getIEOverride(replicatedApplyUDFOpName, jobID),
	}
}

func (aq *applierQuerier) AddTable(targetDescID int32, tc sqlProcessorTableConfig) error {
	var err error
	td := tc.srcDesc
	if tc.dstOID == 0 {
		return errors.AssertionFailedf("empty function name")
	}
	aq.queryBuffer.applierQueries[td.GetID()], err = makeApplierApplyQueries(targetDescID, td, tc.dstOID)
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

func (aq *applierQuerier) RequiresParsedBeforeRow(catid.DescID) bool { return true }

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
	mutType replicationMutationType,
) (batchStats, error) {
	var kvTxn *kv.Txn
	if txn != nil {
		kvTxn = txn.KV()
	}
	decision, err := aq.applyUDF(ctx, kvTxn, ie, mutType, row, prevRow)
	if err != nil {
		return batchStats{}, err
	}
	return aq.applyDecision(ctx, kvTxn, ie, row, decision)
}

func (aq *applierQuerier) applyUDF(
	ctx context.Context,
	txn *kv.Txn,
	ie isql.Executor,
	mutType replicationMutationType,
	row cdcevent.Row,
	prevRow *cdcevent.Row,
) (applierDecision, error) {
	applyQueryBuilder, err := aq.queryBuffer.ApplierQueryForRow(row)
	if err != nil {
		return noDecision, err
	}
	if err := applyQueryBuilder.AddRowDefaultNull(&row); err != nil {
		return noDecision, err
	}
	if err := applyQueryBuilder.AddRowDefaultNull(prevRow); err != nil {
		return noDecision, err
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
		return noDecision, err
	}

	aq.proposedMVCCTs.Decimal = eval.TimestampToDecimal(row.MvccTimestamp)
	datums = append(datums, &aq.proposedMVCCTs)
	decisionRow, err := aq.queryRowExParsed(
		ctx, replicatedApplyUDFOpName, txn, ie, aq.ieoApplyUDF, stmt,
		datums...,
	)
	if err != nil {
		return noDecision, err
	}
	if len(decisionRow) != 1 {
		return noDecision, errors.Errorf("unexpected number of return values from custom UDF: %d", len(decisionRow))
	}
	decisionStr, ok := decisionRow[0].(*tree.DString)
	if !ok {
		return noDecision, errors.Errorf("unexpected return type for first return value from custom UDF: %v", decisionRow[0])
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
	return decision, nil
}

func (aq *applierQuerier) applyDecision(
	ctx context.Context, txn *kv.Txn, ie isql.Executor, row cdcevent.Row, decision applierDecision,
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
		sess := aq.ieoDelete
		sess.OriginTimestampForLogicalDataReplication = row.MvccTimestamp
		if err := aq.execParsed(ctx, replicatedDeleteOpName, txn, ie, sess, stmt, datums...); err != nil {
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
		sess := aq.ieoInsert
		sess.OriginTimestampForLogicalDataReplication = row.MvccTimestamp
		if err := aq.execParsed(ctx, replicatedInsertOpName, txn, ie, sess, stmt, datums...); err != nil {
			return batchStats{}, err
		}
		return batchStats{}, nil
	default:
		return batchStats{}, errors.Errorf("unimplemented or unknown decision: %s", decision)
	}
}

func (aq *applierQuerier) execParsed(
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	ie isql.Executor,
	o sessiondata.InternalExecutorOverride,
	stmt statements.Statement[tree.Statement],
	datums ...interface{},
) error {
	if _, err := ie.ExecParsed(ctx, opName, txn, o, stmt, datums...); err != nil {
		log.Warningf(ctx, "%s failed (query: %s): %s", opName, stmt.SQL, err.Error())
		return err
	}
	return nil
}

func (aq *applierQuerier) queryRowExParsed(
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	ie isql.Executor,
	o sessiondata.InternalExecutorOverride,
	stmt statements.Statement[tree.Statement],
	datums ...interface{},
) (tree.Datums, error) {
	if row, err := ie.QueryRowExParsed(ctx, opName, txn, o, stmt, datums...); err != nil {
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
	dstTableDescID int32, td catalog.TableDescriptor, udfOID uint32,
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

	var (
		colCount      = len(inputColumnNames)
		colNames      = escapedColumnNameList(inputColumnNames)
		valStr        = valueStringForNumItems(colCount, 1)
		prevValStr    = valueStringForNumItems(colCount, colCount+1)
		remoteMVCCIdx = (colCount * 2) + 1
	)

	joinClause := makeApplierJoinClause(td.TableDesc().PrimaryIndex.KeyColumnNames)

	statements := make([]statements.Statement[tree.Statement], 3)
	for statementIdx, mutType := range map[int]replicationMutationType{
		insertQuery: insertMutation,
		updateQuery: updateMutation,
		deleteQuery: deleteMutation,
	} {
		q := fmt.Sprintf(applierQueryBase,
			colNames,
			valStr,
			udfOID,
			mutType,
			prevValStr,
			remoteMVCCIdx,
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
		colNames := escapedColumnNameList(inputColumnNames)
		valStr := valueStringForNumItems(len(inputColumnNames), 1)
		upsertQuery, err := parser.ParseOne(fmt.Sprintf(applierUpsertQueryBase,
			dstTableDescID,
			colNames,
			valStr,
		))
		if err != nil {
			return err
		}

		queryBuilders[family.ID] = queryBuilder{
			stmts:         []statements.Statement[tree.Statement]{upsertQuery},
			inputColumns:  inputColumnNames,
			scratchDatums: make([]interface{}, len(inputColumnNames)),
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
		stmts:        []statements.Statement[tree.Statement]{stmt},
		inputColumns: names,
		// Extra datum slots for the proposed MVCC timestamp.
		scratchDatums: make([]interface{}, len(names)+1),
	}, nil
}
