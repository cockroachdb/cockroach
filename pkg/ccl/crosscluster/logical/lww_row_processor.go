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
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
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

// sqlLastWriteWinsRowProcessor is a row processor that implements partial
// last-write-wins semantics using SQL queries. We assume that the table has an
// crdb_internal_origin_timestamp column defined as:
//
//	crdb_internal_origin_timestamp DECIMAL NOT VISIBLE DEFAULT NULL ON UPDATE NULL
//
// This row is explicitly set by the INSERT query using the MVCC timestamp of
// the inbound write.
//
// Known issues:
//
//  1. An UPDATE and a DELETE may be applied out of order because we have no way
//     from SQL of knowing the write timestamp of the deletion tombstone.
//  2. The crdb_internal_origin_timestamp requires modifying the user's schema.
//
// See the design document for possible solutions to both of these problems.
type sqlLastWriteWinsRowProcessor struct {
	decoder     cdcevent.Decoder
	queryBuffer queryBuffer

	currentMutationGroup mutationGroup

	// scratch allows us to reuse some allocations between multiple calls to
	// insertRow and deleteRow.
	scratch struct {
		datums []interface{}
		ts     tree.DDecimal
	}
}

type mutationGroup struct {
	tableID  catid.DescID
	familyID catid.FamilyID
	rowCount int
	datums   []interface{}
}

func (m *mutationGroup) reset() {
	m.rowCount = 0
	m.datums = m.datums[:0]
}

func (m *mutationGroup) empty() bool {
	return m.rowCount == 0
}

func (m *mutationGroup) rows() int {
	return m.rowCount
}

const maxMutationGroupSize = 8

func (m *mutationGroup) canAddRowToGroup(row *cdcevent.Row) bool {
	if m.empty() || (row.TableID == m.tableID &&
		row.FamilyID == m.familyID &&
		m.rowCount <= maxMutationGroupSize) {
		m.familyID = row.FamilyID
		m.tableID = row.TableID
		m.rowCount++
		return true
	}
	return false

}

func (m *mutationGroup) addMutation(datums []interface{}) {
	m.rowCount++
}

type queryBuffer struct {
	deleteQueries map[catid.DescID]statements.Statement[tree.Statement]
	insertQueries map[catid.DescID]map[catid.FamilyID]map[int]statements.Statement[tree.Statement]
}

func makeSQLLastWriteWinsHandler(
	ctx context.Context, settings *cluster.Settings, tableDescs map[int32]descpb.TableDescriptor,
) (*sqlLastWriteWinsRowProcessor, error) {
	descs := make(map[catid.DescID]catalog.TableDescriptor)
	qb := queryBuffer{
		deleteQueries: make(map[catid.DescID]statements.Statement[tree.Statement], len(tableDescs)),
		insertQueries: make(map[catid.DescID]map[catid.FamilyID]map[int]statements.Statement[tree.Statement], len(tableDescs)),
	}
	cdcEventTargets := changefeedbase.Targets{}
	var err error
	for name, desc := range tableDescs {
		td := tabledesc.NewBuilder(&desc).BuildImmutableTable()
		descs[desc.ID] = td
		qb.deleteQueries[desc.ID], err = parser.ParseOne(makeDeleteQuery(name, td))
		if err != nil {
			return nil, err
		}
		qb.insertQueries[desc.ID], err = makeInsertQueries(name, td)
		if err != nil {
			return nil, err
		}
		cdcEventTargets.Add(changefeedbase.Target{
			Type:              jobspb.ChangefeedTargetSpecification_EACH_FAMILY,
			TableID:           td.GetID(),
			StatementTimeName: changefeedbase.StatementTimeName(td.GetName()),
		})
	}

	prefixlessCodec := keys.SystemSQLCodec
	rfCache, err := cdcevent.NewFixedRowFetcherCache(ctx, prefixlessCodec, settings, cdcEventTargets, descs)
	if err != nil {
		return nil, err
	}

	return &sqlLastWriteWinsRowProcessor{
		queryBuffer: qb,
		decoder:     cdcevent.NewEventDecoderWithCache(ctx, rfCache, false, false),
	}, nil
}

func (lww *sqlLastWriteWinsRowProcessor) insertQueryForCurrentMigrationGroup() (
	statements.Statement[tree.Statement],
	error,
) {
	var (
		tableID   = lww.currentMutationGroup.tableID
		familyID  = lww.currentMutationGroup.familyID
		groupSize = lww.currentMutationGroup.rows()
	)
	insertQueriesForTable, ok := lww.queryBuffer.insertQueries[tableID]
	if !ok {
		return statements.Statement[tree.Statement]{}, errors.Errorf("no pre-generated insert query for table %d", tableID)
	}
	insertQueriesForFamily, ok := insertQueriesForTable[familyID]
	if !ok {
		return statements.Statement[tree.Statement]{}, errors.Errorf("no pre-generated insert query for table %d column family %d",
			tableID,
			familyID)
	}

	// TODO(ssd): make these on demand
	insertQuery, ok := insertQueriesForFamily[groupSize]
	if !ok {
		return statements.Statement[tree.Statement]{}, errors.Errorf("no pre-generated insert query for table %d column family %d, size %d",
			tableID,
			familyID,
			groupSize,
		)
	}
	return insertQuery, nil
}

func (lww *sqlLastWriteWinsRowProcessor) flushCurrentMutationGroup(
	ctx context.Context, txn isql.Txn,
) error {
	if lww.currentMutationGroup.empty() {
		return nil
	}
	defer lww.currentMutationGroup.reset()
	insertQuery, err := lww.insertQueryForCurrentMigrationGroup()
	if err != nil {
		return err
	}
	if _, err := txn.ExecParsed(ctx, "replicated-insert", txn.KV(), attributeToUser, insertQuery, lww.currentMutationGroup.datums...); err != nil {
		log.Warningf(ctx, "replicated insert failed (query: %s): %s", insertQuery.SQL, err.Error())
		return err
	}
	return nil

}

func (lww *sqlLastWriteWinsRowProcessor) ProcessRow(
	ctx context.Context, txn isql.Txn, kv roachpb.KeyValue,
) error {
	var err error
	kv.Key, err = keys.StripTenantPrefix(kv.Key)
	if err != nil {
		return errors.Wrap(err, "stripping tenant prefix")
	}

	row, err := lww.decoder.DecodeKV(ctx, kv, cdcevent.CurrentRow, kv.Value.Timestamp, false)
	if err != nil {
		return errors.Wrap(err, "decoding KeyValue")
	}
	if row.IsDeleted() {
		if err := lww.flushCurrentMutationGroup(ctx, txn); err != nil {
			return err
		}
		return lww.deleteRow(ctx, txn, row)
	} else {
		return lww.insertRow(ctx, txn, row)
	}
}

func (lww *sqlLastWriteWinsRowProcessor) FinishBatch(ctx context.Context, txn isql.Txn) error {
	return lww.flushCurrentMutationGroup(ctx, txn)
}

var attributeToUser = sessiondata.InternalExecutorOverride{
	AttributeToUser: true,
}

func (lww *sqlLastWriteWinsRowProcessor) insertRow(
	ctx context.Context, txn isql.Txn, row cdcevent.Row,
) error {
	if !lww.currentMutationGroup.canAddRowToGroup(&row) {
		if err := lww.flushCurrentMutationGroup(ctx, txn); err != nil {
			return err
		}
		return lww.insertRow(ctx, txn, row)
	}

	err := row.ForAllColumns().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		if col.Computed {
			return nil
		}
		// Ignore crdb_internal_origin_timestamp
		if col.Name == "crdb_internal_origin_timestamp" {
			if d != tree.DNull {
				// We'd only see this if we are doing an initial-scan of a table that was previously ingested into.
				log.Infof(ctx, "saw non-null crdb_internal_origin_timestamp: %v", d)
			}
			return nil
		}

		lww.currentMutationGroup.datums = append(lww.currentMutationGroup.datums, d)
		return nil
	})
	if err != nil {
		return err
	}
	lww.currentMutationGroup.datums = append(lww.currentMutationGroup.datums,
		&tree.DDecimal{Decimal: eval.TimestampToDecimal(row.MvccTimestamp)})
	return nil
}

func (lww *sqlLastWriteWinsRowProcessor) deleteRow(
	ctx context.Context, txn isql.Txn, row cdcevent.Row,
) error {
	datums := lww.scratch.datums[:0]
	if err := row.ForEachKeyColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		datums = append(datums, d)
		return nil
	}); err != nil {
		return err
	}
	lww.scratch.ts.Decimal = eval.TimestampToDecimal(row.MvccTimestamp)
	datums = append(datums, &lww.scratch.ts)
	lww.scratch.datums = datums[:0]
	deleteQuery := lww.queryBuffer.deleteQueries[row.TableID]
	if _, err := txn.ExecParsed(ctx, "replicated-delete", txn.KV(), attributeToUser, deleteQuery, datums...); err != nil {
		log.Warningf(ctx, "replicated delete failed (query: %s): %s", deleteQuery.SQL, err.Error())
		return err
	}
	return nil
}

func makeInsertQueries(
	dstTableDescID int32, td catalog.TableDescriptor,
) (map[catid.FamilyID]map[int]statements.Statement[tree.Statement], error) {
	queries := make(map[catid.FamilyID]map[int]statements.Statement[tree.Statement], td.NumFamilies())
	if err := td.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
		var err error
		queries[family.ID], err = makeInsertQueriesForColumnFamily(dstTableDescID, td, family)
		return err
	}); err != nil {
		return nil, err
	}
	return queries, nil
}

func makeInsertQueriesForColumnFamily(
	dstTableDescID int32, td catalog.TableDescriptor, family *descpb.ColumnFamilyDescriptor,
) (map[int]statements.Statement[tree.Statement], error) {
	escapedColNames, err := insertColumnsForFamily(td, family)
	if err != nil {
		return nil, err
	}
	columnNames := strings.Join(escapedColNames, ", ")
	onConflictClause := makeOnConflictClause(escapedColNames)

	const baseQuery = `
INSERT INTO [%d AS t] (%s)
VALUES %s
ON CONFLICT ON CONSTRAINT %s
DO UPDATE SET
%s
WHERE (t.crdb_internal_mvcc_timestamp <= excluded.crdb_internal_origin_timestamp
       AND t.crdb_internal_origin_timestamp IS NULL)
   OR (t.crdb_internal_origin_timestamp <= excluded.crdb_internal_origin_timestamp
       AND t.crdb_internal_origin_timestamp IS NOT NULL)`
	queries := make(map[int]statements.Statement[tree.Statement], maxMutationGroupSize)
	for i := 1; i <= maxMutationGroupSize; i++ {
		valuesPlaceholders := makeValuesClause(len(escapedColNames), i)
		var err error
		queries[i], err = parser.ParseOne(fmt.Sprintf(baseQuery,
			dstTableDescID,
			columnNames,
			valuesPlaceholders,
			lexbase.EscapeSQLIdent(td.GetPrimaryIndex().GetName()),
			onConflictClause,
		))
		if err != nil {
			return nil, err
		}
	}
	return queries, err
}

// makeValuesClause creates a string used for referencing values by
// placeholder. For example, makeValuesClause(3, 2) should produce:
//
//	($1, $2, $3), ($4, $5, $6)
func makeValuesClause(numColumns, numRows int) string {
	argIdx := 1
	var valuesString strings.Builder
	for i := 0; i < numRows; i++ {
		if i == 0 {
			valuesString.WriteByte('(')
		} else {
			valuesString.WriteString(", (")
		}

		for j := 0; j < numColumns; j++ {
			if j == 0 {
				fmt.Fprintf(&valuesString, "$%d", argIdx)
			} else {
				fmt.Fprintf(&valuesString, ", $%d", argIdx)
			}
			argIdx++
		}
		valuesString.WriteByte(')')
	}
	return valuesString.String()
}

// makeOnConflictClause creates a string for use
// in an ON CONFLICT clause that sets all columns to the value
// in the excluded table.
func makeOnConflictClause(colNames []string) string {
	var onConflictUpdateClause strings.Builder
	for i, colName := range colNames {
		if i == 0 {
			fmt.Fprintf(&onConflictUpdateClause, "%s = excluded.%[1]s", colName)
		} else {
			fmt.Fprintf(&onConflictUpdateClause, ",\n%s = excluded.%[1]s", colName)
		}
	}
	return onConflictUpdateClause.String()
}

func insertColumnsForFamily(
	td catalog.TableDescriptor, family *descpb.ColumnFamilyDescriptor,
) ([]string, error) {
	var (
		publicColumns   = td.PublicColumns()
		primaryIndex    = td.GetPrimaryIndex()
		colOrd          = catalog.ColumnIDToOrdinalMap(publicColumns)
		seenIds         = make(map[string]struct{})
		escapedColNames = make([]string, 0, primaryIndex.NumKeyColumns()+len(family.ColumnNames))
	)
	addEscapedColumnName := func(colName string) {
		if _, seen := seenIds[colName]; !seen {
			seenIds[colName] = struct{}{}
			escapedColNames = append(escapedColNames, lexbase.EscapeSQLIdent(colName))
		}
	}

	addColumnByID := func(colID catid.ColumnID) error {
		ord, ok := colOrd.Get(colID)
		if !ok {
			return errors.AssertionFailedf("expected to find column %d", colID)
		}
		col := publicColumns[ord]
		if col.IsComputed() {
			return nil
		}
		colName := col.GetName()
		// We will set crdb_internal_origin_timestamp ourselves from the MVCC timestamp of the incoming datum.
		// We should never see this on the rangefeed as a non-null value as that would imply we've looped data around.
		if colName == "crdb_internal_origin_timestamp" {
			return nil
		}

		addEscapedColumnName(colName)
		return nil
	}

	for i := 0; i < primaryIndex.NumKeyColumns(); i++ {
		if err := addColumnByID(primaryIndex.GetKeyColumnID(i)); err != nil {
			return nil, err
		}
	}

	for i := range family.ColumnNames {
		if err := addColumnByID(family.ColumnIDs[i]); err != nil {
			return nil, err
		}
	}
	addEscapedColumnName("crdb_internal_origin_timestamp")
	return escapedColNames, nil
}

func makeDeleteQuery(dstTableDescID int32, td catalog.TableDescriptor) string {
	var whereClause strings.Builder
	names := td.TableDesc().PrimaryIndex.KeyColumnNames
	for i, name := range names {
		colName := lexbase.EscapeSQLIdent(name)
		if i == 0 {
			fmt.Fprintf(&whereClause, "%s = $%d", colName, i+1)
		} else {
			fmt.Fprintf(&whereClause, "AND %s = $%d", colName, i+1)
		}
	}
	originTSIdx := len(names) + 1
	baseQuery := `
DELETE FROM [%d as t] WHERE %s
   AND ((t.crdb_internal_mvcc_timestamp < $%[3]d
        AND t.crdb_internal_origin_timestamp IS NULL)
    OR (t.crdb_internal_origin_timestamp < $%[3]d
        AND t.crdb_internal_origin_timestamp IS NOT NULL))`

	return fmt.Sprintf(baseQuery, dstTableDescID, whereClause.String(), originTSIdx)
}
