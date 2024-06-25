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
	"github.com/cockroachdb/cockroach/pkg/kv"
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
	ie          isql.Executor

	// scratch allows us to reuse some allocations between multiple calls to
	// insertRow and deleteRow.
	scratch struct {
		datums []interface{}
		ts     tree.DDecimal
	}
}

type queryBuffer struct {
	deleteQueries map[catid.DescID]statements.Statement[tree.Statement]
	// insertQueries stores a mapping from the table ID to a mapping from the
	// column family ID to two INSERT statements (one optimistic that assumes
	// there is no conflicting row in the table, and another pessimistic one).
	insertQueries map[catid.DescID]map[catid.FamilyID][2]statements.Statement[tree.Statement]
}

const (
	insertQueriesOptimisticIndex  = 0
	insertQueriesPessimisticIndex = 1
)

func makeSQLLastWriteWinsHandler(
	ctx context.Context,
	settings *cluster.Settings,
	tableDescs map[int32]descpb.TableDescriptor,
	ie isql.Executor,
) (*sqlLastWriteWinsRowProcessor, error) {
	descs := make(map[catid.DescID]catalog.TableDescriptor)
	qb := queryBuffer{
		deleteQueries: make(map[catid.DescID]statements.Statement[tree.Statement], len(tableDescs)),
		insertQueries: make(map[catid.DescID]map[catid.FamilyID][2]statements.Statement[tree.Statement], len(tableDescs)),
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
		settings:    settings,
		ie:          ie,
	}, nil
}

func (lww *sqlLastWriteWinsRowProcessor) ProcessRow(
	ctx context.Context, txn isql.Txn, kv roachpb.KeyValue, prevValue roachpb.Value,
) (batchStats, error) {
	var err error
	kv.Key, err = keys.StripTenantPrefix(kv.Key)
	if err != nil {
		return batchStats{}, errors.Wrap(err, "stripping tenant prefix")
	}

	row, err := lww.decoder.DecodeKV(ctx, kv, cdcevent.CurrentRow, kv.Value.Timestamp, false)
	if err != nil {
		return batchStats{}, errors.Wrap(err, "decoding KeyValue")
	}
	var stats batchStats
	if row.IsDeleted() {
		stats, err = lww.deleteRow(ctx, txn, row)
	} else {
		stats, err = lww.insertRow(ctx, txn, row, prevValue)
	}
	stats.byteSize = int64(kv.Size())
	return stats, err
}

func (lww *sqlLastWriteWinsRowProcessor) GetDecoder() cdcevent.Decoder {
	return lww.decoder
}

func (lww *sqlLastWriteWinsRowProcessor) GetSQLStatement(
	row cdcevent.Row, isInitialScan bool,
) string {
	if row.IsDeleted() {
		return lww.queryBuffer.deleteQueries[row.TableID].SQL
	} else {
		insertQueriesForTable, ok := lww.queryBuffer.insertQueries[row.TableID]
		if !ok {
			return ""
		}
		insertQueries, ok := insertQueriesForTable[row.FamilyID]
		if !ok {
			return ""
		}

		var insertTypeIndex int
		// As noted in lww.insertRow(), we attempt optimistic insert if the following condition is true,
		// hence why it's required to determine whether we attempted optimistic or pessimistic insert.
		if isInitialScan && tryOptimisticInsertEnabled.Get(&lww.settings.SV) {
			insertTypeIndex = insertQueriesOptimisticIndex
		} else {
			insertTypeIndex = insertQueriesPessimisticIndex
		}
		return insertQueries[insertTypeIndex].SQL
	}
}

var ieOverrides = sessiondata.InternalExecutorOverride{
	AttributeToUser: true,
	// TODO(ssd): we do this for now to prevent the data from being emitted back
	// to the source. However, I don't think we want to do this in the long run.
	// Rather, we want to store the inbound cluster ID and store that in a way
	// that allows us to choose to filter it out from or not. Doing it this way
	// means that you can't choose to run CDC just from one side and not the
	// other.
	DisableChangefeedReplication: true,
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
) (batchStats, error) {
	var kvTxn *kv.Txn
	if txn != nil {
		kvTxn = txn.KV()
	}
	datums := lww.scratch.datums[:0]
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

		datums = append(datums, d)
		return nil
	})
	if err != nil {
		return batchStats{}, err
	}
	lww.scratch.ts.Decimal = eval.TimestampToDecimal(row.MvccTimestamp)
	datums = append(datums, &lww.scratch.ts)
	lww.scratch.datums = datums[:0]
	insertQueriesForTable, ok := lww.queryBuffer.insertQueries[row.TableID]
	if !ok {
		return batchStats{}, errors.Errorf("no pre-generated insert query for table %d", row.TableID)
	}
	insertQueries, ok := insertQueriesForTable[row.FamilyID]
	if !ok {
		return batchStats{}, errors.Errorf("no pre-generated insert query for table %d column family %d", row.TableID, row.FamilyID)
	}
	var optimisticInsertConflicts int64
	if prevValue.RawBytes == nil && tryOptimisticInsertEnabled.Get(&lww.settings.SV) {
		// If there was no value before this change on the source, it means that
		// either we are processing an initial scan or the original stmt was an
		// insert that didn't hit a conflict. In both cases we'll try the
		// optimistic insert first.
		query := insertQueries[insertQueriesOptimisticIndex]
		if _, err = lww.ie.ExecParsed(ctx, "replicated-optimistic-insert", kvTxn, ieOverrides, query, datums...); err != nil {
			// If the optimistic insert failed with unique violation, we have to
			// fall back to the pessimistic path. If we got a different error,
			// then we bail completely.
			if pgerror.GetPGCode(err) != pgcode.UniqueViolation {
				log.Warningf(ctx, "replicated optimistic insert failed (query: %s): %s", query.SQL, err.Error())
				return batchStats{}, err
			}
			optimisticInsertConflicts++
		} else {
			// There was no conflict - we're done.
			return batchStats{}, nil
		}
	}
	query := insertQueries[insertQueriesPessimisticIndex]
	if _, err = lww.ie.ExecParsed(ctx, "replicated-insert", kvTxn, ieOverrides, query, datums...); err != nil {
		log.Warningf(ctx, "replicated insert failed (query: %s): %s", query.SQL, err.Error())
		return batchStats{}, err
	}
	return batchStats{optimisticInsertConflicts: optimisticInsertConflicts}, nil
}

func (lww *sqlLastWriteWinsRowProcessor) deleteRow(
	ctx context.Context, txn isql.Txn, row cdcevent.Row,
) (batchStats, error) {
	var kvTxn *kv.Txn
	if txn != nil {
		kvTxn = txn.KV()
	}
	datums := lww.scratch.datums[:0]
	if err := row.ForEachKeyColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		datums = append(datums, d)
		return nil
	}); err != nil {
		return batchStats{}, err
	}
	lww.scratch.ts.Decimal = eval.TimestampToDecimal(row.MvccTimestamp)
	datums = append(datums, &lww.scratch.ts)
	lww.scratch.datums = datums[:0]
	deleteQuery := lww.queryBuffer.deleteQueries[row.TableID]
	if _, err := lww.ie.ExecParsed(ctx, "replicated-delete", kvTxn, ieOverrides, deleteQuery, datums...); err != nil {
		log.Warningf(ctx, "replicated delete failed (query: %s): %s", deleteQuery.SQL, err.Error())
		return batchStats{}, err
	}
	return batchStats{}, nil
}

func makeInsertQueries(
	dstTableDescID int32, td catalog.TableDescriptor,
) (map[catid.FamilyID][2]statements.Statement[tree.Statement], error) {
	queries := make(map[catid.FamilyID][2]statements.Statement[tree.Statement], td.NumFamilies())
	if err := td.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
		var columnNames strings.Builder
		var valueStrings strings.Builder
		var onConflictUpdateClause strings.Builder
		argIdx := 1
		seenIds := make(map[catid.ColumnID]struct{})
		publicColumns := td.PublicColumns()
		colOrd := catalog.ColumnIDToOrdinalMap(publicColumns)
		addColumnByNameNoCheck := func(colName string) {
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
			if colName == "crdb_internal_origin_timestamp" {
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
		addColumnByNameNoCheck("crdb_internal_origin_timestamp")
		var insertQueries [2]statements.Statement[tree.Statement]
		var err error
		insertQueries[insertQueriesOptimisticIndex], err = parser.ParseOne(fmt.Sprintf(`
INSERT INTO [%d AS t] (%s)
VALUES (%s)`,
			dstTableDescID,
			columnNames.String(),
			valueStrings.String(),
		))
		if err != nil {
			return err
		}
		insertQueries[insertQueriesPessimisticIndex], err = parser.ParseOne(fmt.Sprintf(`
INSERT INTO [%d AS t] (%s)
VALUES (%s)
ON CONFLICT ON CONSTRAINT %s
DO UPDATE SET
%s
WHERE (t.crdb_internal_mvcc_timestamp <= excluded.crdb_internal_origin_timestamp
       AND t.crdb_internal_origin_timestamp IS NULL)
   OR (t.crdb_internal_origin_timestamp <= excluded.crdb_internal_origin_timestamp
       AND t.crdb_internal_origin_timestamp IS NOT NULL)`,
			dstTableDescID,
			columnNames.String(),
			valueStrings.String(),
			lexbase.EscapeSQLIdent(td.GetPrimaryIndex().GetName()),
			onConflictUpdateClause.String(),
		))
		queries[family.ID] = insertQueries
		return err
	}); err != nil {
		return nil, err
	}
	return queries, nil
}

func makeDeleteQuery(dstTableDescID int32, td catalog.TableDescriptor) string {
	var whereClause strings.Builder
	names := td.TableDesc().PrimaryIndex.KeyColumnNames
	for i := 0; i < len(names); i++ {
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

	return fmt.Sprintf(baseQuery, dstTableDescID, whereClause.String(), originTSIdx)
}
