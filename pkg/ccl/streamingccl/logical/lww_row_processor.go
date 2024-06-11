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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// sqlLastWriteWinsRowProcessor is a row handler that implements partial last-write-wins semantics using SQL queries. We assume that the table
// has an crdb_internal_origin_timestamp column defined as:
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
//  2. The crd_internal_origin_timestamp requires modifying the user's schema.
//
// See the design document for possible solutions to both of these problems.
type sqlLastWriteWinsRowProcessor struct {
	decoder     cdcevent.Decoder
	queryBuffer queryBuffer
}

type queryBuffer struct {
	deleteQueries map[catid.DescID]string
	insertQueries map[catid.DescID]string
}

func makeSQLLastWriteWinsHandler(
	ctx context.Context,
	codec keys.SQLCodec,
	settings *cluster.Settings,
	tableDescs map[string]descpb.TableDescriptor,
) (*sqlLastWriteWinsRowProcessor, error) {
	descs := make(map[catid.DescID]catalog.TableDescriptor)
	qb := queryBuffer{
		deleteQueries: make(map[catid.DescID]string, len(tableDescs)),
		insertQueries: make(map[catid.DescID]string, len(tableDescs)),
	}
	cdcEventTargets := changefeedbase.Targets{}
	for name, desc := range tableDescs {
		td := tabledesc.NewBuilder(&desc).BuildImmutableTable()
		descs[desc.ID] = td
		qb.deleteQueries[desc.ID] = makeDeleteQuery(name, td)
		qb.insertQueries[desc.ID] = makeInsertQuery(name, td)
		cdcEventTargets.Add(changefeedbase.Target{
			Type:              jobspb.ChangefeedTargetSpecification_EACH_FAMILY,
			TableID:           td.GetID(),
			StatementTimeName: changefeedbase.StatementTimeName(td.GetName()),
		})
	}

	rfCache, err := cdcevent.NewFixedRowFetcherCache(ctx, codec, settings, cdcEventTargets, descs)
	if err != nil {
		return nil, err
	}

	return &sqlLastWriteWinsRowProcessor{
		queryBuffer: qb,
		decoder:     cdcevent.NewEventDecoderWithCache(ctx, rfCache, false, false),
	}, nil
}

func (lww *sqlLastWriteWinsRowProcessor) ProcessRow(
	ctx context.Context, txn isql.Txn, kv roachpb.KeyValue,
) error {
	row, err := lww.decoder.DecodeKV(ctx, kv, cdcevent.CurrentRow, kv.Value.Timestamp, false)
	if err != nil {
		return err
	}
	if row.IsDeleted() {
		return lww.deleteRow(ctx, txn, row)
	} else {
		return lww.insertRow(ctx, txn, row)
	}
}

func (lww *sqlLastWriteWinsRowProcessor) insertRow(
	ctx context.Context, txn isql.Txn, row cdcevent.Row,
) error {
	datums := make([]interface{}, 0, len(row.EncDatums()))
	err := row.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
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
		return err
	}
	datums = append(datums, eval.TimestampToDecimalDatum(row.MvccTimestamp))
	insertQuery := lww.queryBuffer.insertQueries[row.TableID]
	if _, err := txn.Exec(ctx, "replicated-insert", txn.KV(), insertQuery, datums...); err != nil {
		log.Warningf(ctx, "replicated insert failed (query: %s): %s", insertQuery, err.Error())
		return err
	}
	return nil
}

func (lww *sqlLastWriteWinsRowProcessor) deleteRow(
	ctx context.Context, txn isql.Txn, row cdcevent.Row,
) error {
	datums := make([]interface{}, 0, len(row.TableDescriptor().TableDesc().PrimaryIndex.KeyColumnNames))
	if err := row.ForEachKeyColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		datums = append(datums, d)
		return nil
	}); err != nil {
		return err
	}
	deleteQuery := lww.queryBuffer.deleteQueries[row.TableID]
	if _, err := txn.Exec(ctx, "replicated-delete", txn.KV(), deleteQuery, datums...); err != nil {
		log.Warningf(ctx, "replicated delete failed (query: %s): %s", deleteQuery, err.Error())
		return err
	}
	return nil
}

func makeInsertQuery(fqTableName string, td catalog.TableDescriptor) string {
	// TODO(ssd): Column families
	var columnNames strings.Builder
	var valueStrings strings.Builder
	var onConflictUpdateClause strings.Builder
	argIdx := 1
	for _, col := range td.PublicColumns() {
		// Virtual columns are not present in the data written to disk and
		// thus not part of the rangefeed datum.
		if col.IsVirtual() {
			continue
		}
		// We will set crdb_internal_origin_timestamp ourselves from the MVCC timestamp of the incoming datum.
		// We should never see this on the rangefeed as a non-null value as that would imply we've looped data around.
		if col.GetName() == "crdb_internal_origin_timestamp" {
			continue
		}
		if argIdx == 1 {
			columnNames.WriteString(col.GetName())
			fmt.Fprintf(&valueStrings, "$%d", argIdx)
			fmt.Fprintf(&onConflictUpdateClause, "%s = $%d", col.GetName(), argIdx)
		} else {
			fmt.Fprintf(&columnNames, ", %s", col.GetName())
			fmt.Fprintf(&valueStrings, ", $%d", argIdx)
			fmt.Fprintf(&onConflictUpdateClause, ",\n%s = $%d", col.GetName(), argIdx)
		}
		argIdx++
	}
	originTSIdx := argIdx
	baseQuery := `
INSERT INTO %s (%s, crdb_internal_origin_timestamp)
VALUES (%s, $%d)
ON CONFLICT ON CONSTRAINT %s
DO UPDATE SET
%s,
crdb_internal_origin_timestamp=$%[4]d
WHERE (%[1]s.crdb_internal_mvcc_timestamp < $%[4]d
       AND %[1]s.crdb_internal_origin_timestamp IS NULL)
   OR (%[1]s.crdb_internal_origin_timestamp < $%[4]d
       AND %[1]s.crdb_internal_origin_timestamp IS NOT NULL)`
	return fmt.Sprintf(baseQuery,
		fqTableName,
		columnNames.String(),
		valueStrings.String(),
		originTSIdx,
		td.GetPrimaryIndex().GetName(),
		onConflictUpdateClause.String(),
	)
}

func makeDeleteQuery(fqTableName string, td catalog.TableDescriptor) string {
	var whereClause strings.Builder
	names := td.TableDesc().PrimaryIndex.KeyColumnNames
	for i := 0; i < len(names); i++ {
		if i == 0 {
			fmt.Fprintf(&whereClause, "%s = $%d", names[i], i+1)
		} else {
			fmt.Fprintf(&whereClause, "AND %s = $%d", names[i], i+1)
		}
	}
	originTSIdx := len(names) + 1
	baseQuery := `
DELETE FROM %s WHERE %s
   AND (%[1]s.crdb_internal_mvcc_timestamp < $%[3]d
        AND %[1]s.crdb_internal_origin_timestamp IS NULL)
    OR (%[1]s.crdb_internal_origin_timestamp < $%[3]d
        AND tab.crdb_internal_origin_timestamp IS NOT NULL)`

	return fmt.Sprintf(baseQuery, fqTableName, whereClause.String(), originTSIdx)
}
