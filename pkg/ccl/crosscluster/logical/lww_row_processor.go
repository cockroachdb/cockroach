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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/errors"
)

const originTimestampColumnName = "crdb_internal_origin_timestamp"

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
	settings    *cluster.Settings
	metrics     *Metrics
}

type queryBuilder struct {
	stmts []statements.Statement[tree.Statement]
	// TODO(ssd): It would almost surely be better to track this by column IDs than name.
	//
	// TODO(ssd): The management of MVCC Origin Timestamp column is a bit messy. The mess
	// is caused by column families that don't have that row in the datum.
	//
	// If the query requires the origin timestamp column, inputColumns should not include the column.
	// Rather, the query should set needsOriginTimestamp.
	inputColumns         []string
	needsOriginTimestamp bool

	// scratch allows us to reuse some allocations between multiple calls to
	// insertRow and deleteRow.
	scratch []interface{}
}

func (q *queryBuilder) AddRow(row cdcevent.Row) error {
	it, err := row.DatumsNamed(q.inputColumns)
	if err != nil {
		return err
	}
	if err := it.Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		q.scratch = append(q.scratch, d)
		return nil
	}); err != nil {
		return err
	}
	if q.needsOriginTimestamp {
		q.scratch = append(q.scratch, &tree.DDecimal{Decimal: eval.TimestampToDecimal(row.MvccTimestamp)})
	}
	return nil
}

func (q *queryBuilder) Query(
	variant int,
) (statements.Statement[tree.Statement], []interface{}, error) {
	expectedScratchSize := len(q.inputColumns)
	if q.needsOriginTimestamp {
		expectedScratchSize++
	}
	if len(q.scratch) != expectedScratchSize {
		return statements.Statement[tree.Statement]{}, nil, errors.Errorf("unexpected number of datums for query (have %d, expected %d)",
			len(q.scratch),
			len(q.inputColumns))
	}
	return q.stmts[variant], q.scratch, nil
}

func (q *queryBuilder) Reset() {
	q.scratch = q.scratch[:0]
}

type queryBuffer struct {
	deleteQueries map[catid.DescID]queryBuilder
	insertQueries map[catid.DescID]map[catid.FamilyID]queryBuilder
}

// insertQueries stores a mapping from the table ID to a mapping from the
// column family ID to two INSERT statements (one optimistic that assumes
// there is no conflicting row in the table, and another pessimistic one).
const (
	insertQueriesOptimisticIndex  = 0
	insertQueriesPessimisticIndex = 1

	deleteQueryStd = 0
)

func makeSQLLastWriteWinsHandler(
	ctx context.Context, settings *cluster.Settings, tableDescs map[int32]descpb.TableDescriptor,
) (*sqlLastWriteWinsRowProcessor, error) {
	descs := make(map[catid.DescID]catalog.TableDescriptor)
	qb := queryBuffer{
		deleteQueries: make(map[catid.DescID]queryBuilder, len(tableDescs)),
		insertQueries: make(map[catid.DescID]map[catid.FamilyID]queryBuilder, len(tableDescs)),
	}
	cdcEventTargets := changefeedbase.Targets{}
	var err error
	for name, desc := range tableDescs {
		td := tabledesc.NewBuilder(&desc).BuildImmutableTable()
		descs[desc.ID] = td
		qb.deleteQueries[desc.ID], err = makeDeleteQuery(name, td)
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
		settings:    settings,
	}, nil
}

func (lww *sqlLastWriteWinsRowProcessor) SetMetrics(metrics *Metrics) {
	lww.metrics = metrics
}

func (lww *sqlLastWriteWinsRowProcessor) ProcessRow(
	ctx context.Context, txn isql.Txn, kv roachpb.KeyValue, prevValue roachpb.Value,
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
		return lww.deleteRow(ctx, txn, row)
	} else {
		return lww.insertRow(ctx, txn, row, prevValue)
	}
}

var attributeToUser = sessiondata.InternalExecutorOverride{
	AttributeToUser: true,
}

var tryOptimisticInsertEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.try_optimistic_insert.enabled",
	"determines whether the consumer attempts to execute the 'optimistic' INSERT "+
		"first (when there was no previous value on the source) which will succeed only "+
		"if there is no conflict with an existing row",
	metamorphic.ConstantWithTestBool("logical_replication.consumer.try_optimistic_insert.enabled", true),
)

func (lww *sqlLastWriteWinsRowProcessor) insertRow(
	ctx context.Context, txn isql.Txn, row cdcevent.Row, prevValue roachpb.Value,
) error {
	insertQueriesForTable, ok := lww.queryBuffer.insertQueries[row.TableID]
	if !ok {
		return errors.Errorf("no pre-generated insert query for table %d", row.TableID)
	}
	insertQueryBuilder, ok := insertQueriesForTable[row.FamilyID]
	if !ok {
		return errors.Errorf("no pre-generated insert query for table %d column family %d", row.TableID, row.FamilyID)
	}
	insertQueryBuilder.Reset()
	if err := insertQueryBuilder.AddRow(row); err != nil {
		return err
	}

	if prevValue.RawBytes == nil && tryOptimisticInsertEnabled.Get(&lww.settings.SV) {
		stmt, datums, err := insertQueryBuilder.Query(insertQueriesOptimisticIndex)
		if err != nil {
			return err
		}
		if _, err = txn.ExecParsed(ctx, "replicated-optimistic-insert", txn.KV(), attributeToUser, stmt, datums...); err != nil {
			// If the optimistic insert failed with unique violation, we have to
			// fall back to the pessimistic path. If we got a different error,
			// then we bail completely.
			if pgerror.GetPGCode(err) != pgcode.UniqueViolation {
				log.Warningf(ctx, "replicated optimistic insert failed (query: %s): %s", stmt.SQL, err.Error())
				return err
			}
			lww.metrics.OptimisticInsertConflictCount.Inc(1)
		} else {
			// There was no conflict - we're done.
			return nil
		}
	}
	stmt, datums, err := insertQueryBuilder.Query(insertQueriesPessimisticIndex)
	if err != nil {
		return err
	}
	if _, err := txn.ExecParsed(ctx, "replicated-insert", txn.KV(), attributeToUser, stmt, datums...); err != nil {
		log.Warningf(ctx, "replicated insert failed (query: %s): %s", stmt.SQL, err.Error())
		return err
	}
	return nil
}

func (lww *sqlLastWriteWinsRowProcessor) deleteRow(
	ctx context.Context, txn isql.Txn, row cdcevent.Row,
) error {
	deleteQuery, ok := lww.queryBuffer.deleteQueries[row.TableID]
	if !ok {
		return errors.Errorf("no pre-generated delete query for table %d", row.TableID)
	}
	deleteQuery.Reset()
	if err := deleteQuery.AddRow(row); err != nil {
		return err
	}

	stmt, datums, err := deleteQuery.Query(deleteQueryStd)
	if err != nil {
		return err
	}

	if _, err := txn.ExecParsed(ctx, "replicated-delete", txn.KV(), attributeToUser, stmt, datums...); err != nil {
		log.Warningf(ctx, "replicated delete failed (query: %s): %s", stmt.SQL, err.Error())
		return err
	}
	return nil
}

const (
	insertQueryOptimistic  = `INSERT INTO [%d AS t] (%s) VALUES (%s)`
	insertQueryPessimistic = `
INSERT INTO [%d AS t] (%s)
VALUES (%s)
ON CONFLICT ON CONSTRAINT %s
DO UPDATE SET
%s
WHERE (t.crdb_internal_mvcc_timestamp <= excluded.crdb_internal_origin_timestamp
     AND t.crdb_internal_origin_timestamp IS NULL)
 OR (t.crdb_internal_origin_timestamp <= excluded.crdb_internal_origin_timestamp
     AND t.crdb_internal_origin_timestamp IS NOT NULL)`
)

func makeInsertQueries(
	dstTableDescID int32, td catalog.TableDescriptor,
) (map[catid.FamilyID]queryBuilder, error) {
	queryBuilders := make(map[catid.FamilyID]queryBuilder, td.NumFamilies())
	if err := td.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
		var columnNames strings.Builder
		var valueStrings strings.Builder
		var onConflictUpdateClause strings.Builder
		argIdx := 1
		inputColumNames := make([]string, 0)
		seenIds := make(map[catid.ColumnID]struct{})
		publicColumns := td.PublicColumns()
		colOrd := catalog.ColumnIDToOrdinalMap(publicColumns)
		addColumnByNameNoCheck := func(colName string) {
			if colName != originTimestampColumnName {
				inputColumNames = append(inputColumNames, colName)
			}
			colName = lexbase.EscapeSQLIdent(colName)
			if argIdx == 1 {
				columnNames.WriteString(colName)
				fmt.Fprintf(&valueStrings, "$%d", argIdx)
				fmt.Fprintf(&onConflictUpdateClause, "%s = excluded.%[1]s", colName)
			} else {
				fmt.Fprintf(&columnNames, ", %s", colName)
				fmt.Fprintf(&valueStrings, ", $%d", argIdx)
				fmt.Fprintf(&onConflictUpdateClause, ",\n%s = excluded.%[1]s", colName)
			}
			argIdx++
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
			if colName == originTimestampColumnName {
				return nil
			}
			if _, seen := seenIds[colID]; seen {
				return nil
			}
			addColumnByNameNoCheck(colName)
			seenIds[colID] = struct{}{}
			return nil
		}

		primaryIndex := td.GetPrimaryIndex()
		for i := 0; i < primaryIndex.NumKeyColumns(); i++ {
			if err := addColumnByID(primaryIndex.GetKeyColumnID(i)); err != nil {
				return err
			}
		}

		for i := range family.ColumnNames {
			if err := addColumnByID(family.ColumnIDs[i]); err != nil {
				return err
			}
		}
		addColumnByNameNoCheck(originTimestampColumnName)

		parsedOptimisticQuery, err := parser.ParseOne(fmt.Sprintf(insertQueryOptimistic,
			dstTableDescID,
			columnNames.String(),
			valueStrings.String(),
		))
		if err != nil {
			return err
		}

		parsedPessimisticQuery, err := parser.ParseOne(fmt.Sprintf(insertQueryPessimistic,
			dstTableDescID,
			columnNames.String(),
			valueStrings.String(),
			lexbase.EscapeSQLIdent(td.GetPrimaryIndex().GetName()),
			onConflictUpdateClause.String(),
		))
		if err != nil {
			return err
		}

		queryBuilders[family.ID] = queryBuilder{
			stmts: []statements.Statement[tree.Statement]{
				parsedOptimisticQuery,
				parsedPessimisticQuery,
			},
			needsOriginTimestamp: true,
			inputColumns:         inputColumNames,
			scratch:              make([]interface{}, len(inputColumNames)+1),
		}
		return err
	}); err != nil {
		return nil, err
	}
	return queryBuilders, nil
}

func makeDeleteQuery(dstTableDescID int32, td catalog.TableDescriptor) (queryBuilder, error) {
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
	originTSIdx := len(names) + 1
	baseQuery := `
DELETE FROM [%d as t] WHERE %s
   AND ((t.crdb_internal_mvcc_timestamp < $%[3]d
        AND t.crdb_internal_origin_timestamp IS NULL)
    OR (t.crdb_internal_origin_timestamp < $%[3]d
        AND t.crdb_internal_origin_timestamp IS NOT NULL))`
	stmt, err := parser.ParseOne(
		fmt.Sprintf(baseQuery, dstTableDescID, whereClause.String(), originTSIdx))
	if err != nil {
		return queryBuilder{}, err
	}

	return queryBuilder{
		stmts:                []statements.Statement[tree.Statement]{stmt},
		inputColumns:         names,
		needsOriginTimestamp: true,
		scratch:              make([]interface{}, len(names)+1),
	}, nil
}
