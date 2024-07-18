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
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const (
	originTimestampColumnName = "crdb_replication_origin_timestamp"
)

type processorType string

// SafeValue implements the redact.SafeValue interface.
func (p processorType) SafeValue() {}

var _ redact.SafeValue = defaultSQLProcessor

const (
	lwwProcessor        processorType = "last-write-wins"
	udfApplierProcessor processorType = "applier-udf"
)

// TODO(ssd): Thread through from job
var udfName string
var defaultSQLProcessor = lwwProcessor

// A sqlRowProcessor is a RowProcessor that handles rows using the
// provided querier.
type sqlRowProcessor struct {
	decoder  cdcevent.Decoder
	querier  querier
	settings *cluster.Settings
	ie       isql.Executor
	lastRow  cdcevent.Row

	// testing knobs.
	testingInjectFailurePercent uint32
}

// A querier handles rows for any table that has previously been added
// to the querier using the passed isql.Txn and internal executor.
type querier interface {
	AddTable(targetDescID int32, td catalog.TableDescriptor) error
	InsertRow(ctx context.Context, txn isql.Txn, ie isql.Executor, row cdcevent.Row, prevRow *cdcevent.Row, likelyInsert bool) (batchStats, error)
	DeleteRow(ctx context.Context, txn isql.Txn, ie isql.Executor, row cdcevent.Row, prevRow *cdcevent.Row) (batchStats, error)
	RequiresParsedBeforeRow() bool
}

type queryBuilder struct {
	// stmts are parsed SQL statements. They should have the same number
	// of inputs.
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
	scratchDatums []interface{}
	scratchTS     tree.DDecimal
}

func (q *queryBuilder) Reset() {
	q.scratchDatums = q.scratchDatums[:0]
}

func (q *queryBuilder) AddRow(row cdcevent.Row) error {
	it, err := row.DatumsNamed(q.inputColumns)
	if err != nil {
		return err
	}
	if err := it.Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		q.scratchDatums = append(q.scratchDatums, d)
		return nil
	}); err != nil {
		return err
	}
	if q.needsOriginTimestamp {
		q.scratchTS.Decimal = eval.TimestampToDecimal(row.MvccTimestamp)
		q.scratchDatums = append(q.scratchDatums, &q.scratchTS)
	}
	return nil
}

func (q *queryBuilder) AddRowDefaultNull(row *cdcevent.Row) error {
	if row == nil {
		for range q.inputColumns {
			q.scratchDatums = append(q.scratchDatums, tree.DNull)
		}
	}

	for _, n := range q.inputColumns {
		it, err := row.DatumNamed(n)
		if err != nil {
			q.scratchDatums = append(q.scratchDatums, tree.DNull)
			continue
		}
		if err := it.Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
			q.scratchDatums = append(q.scratchDatums, d)
			return nil
		}); err != nil {
			return err
		}
	}

	if q.needsOriginTimestamp {
		q.scratchTS.Decimal = eval.TimestampToDecimal(row.MvccTimestamp)
		q.scratchDatums = append(q.scratchDatums, &q.scratchTS)
	}
	return nil
}

func (q *queryBuilder) Query(
	variant int,
) (statements.Statement[tree.Statement], []interface{}, error) {
	return q.stmts[variant], q.scratchDatums, nil
}

type queryBuffer struct {
	deleteQueries  map[catid.DescID]queryBuilder
	insertQueries  map[catid.DescID]map[catid.FamilyID]queryBuilder
	applierQueries map[catid.DescID]map[catid.FamilyID]queryBuilder
}

func (q *queryBuffer) DeleteQueryForRow(row cdcevent.Row) (queryBuilder, error) {
	dq, ok := q.deleteQueries[row.TableID]
	if !ok {
		return queryBuilder{}, errors.Errorf("no pre-generated delete query for table %d", row.TableID)
	}
	dq.Reset()
	return dq, nil
}

func (q *queryBuffer) InsertQueryForRow(row cdcevent.Row) (queryBuilder, error) {
	insertQueriesForTable, ok := q.insertQueries[row.TableID]
	if !ok {
		return queryBuilder{}, errors.Errorf("no pre-generated insert query for table %d", row.TableID)
	}
	insertQueryBuilder, ok := insertQueriesForTable[row.FamilyID]
	if !ok {
		return queryBuilder{}, errors.Errorf("no pre-generated insert query for table %d column family %d", row.TableID, row.FamilyID)
	}
	insertQueryBuilder.Reset()
	return insertQueryBuilder, nil
}

func (q *queryBuffer) ApplierQueryForRow(row cdcevent.Row) (queryBuilder, error) {
	applierQueriesForTable, ok := q.applierQueries[row.TableID]
	if !ok {
		return queryBuilder{}, errors.Errorf("no pre-generated apply query for table %d", row.TableID)
	}
	applierQueryBuilder, ok := applierQueriesForTable[row.FamilyID]
	if !ok {
		return queryBuilder{}, errors.Errorf("no pre-generated apply query for table %d column family %d", row.TableID, row.FamilyID)
	}
	applierQueryBuilder.Reset()
	return applierQueryBuilder, nil
}

func makeSQLProcessor(
	ctx context.Context,
	settings *cluster.Settings,
	tableDescs map[descpb.ID]catalog.TableDescriptor,
	ie isql.Executor,
) (*sqlRowProcessor, error) {
	switch defaultSQLProcessor {
	case lwwProcessor:
		return makeSQLLastWriteWinsHandler(ctx, settings, tableDescs, ie)
	case udfApplierProcessor:
		return makeUDFApplierProcessor(ctx, settings, tableDescs, ie)
	default:
		return nil, errors.AssertionFailedf("unknown SQL processor: %s", defaultSQLProcessor)
	}
}

func makeSQLProcessorFromQuerier(
	ctx context.Context,
	settings *cluster.Settings,
	tableDescs map[descpb.ID]catalog.TableDescriptor,
	ie isql.Executor,
	querier querier,
) (*sqlRowProcessor, error) {
	cdcEventTargets := changefeedbase.Targets{}
	tableDescsBySrcID := make(map[descpb.ID]catalog.TableDescriptor, len(tableDescs))

	for descID, desc := range tableDescs {
		tableDescsBySrcID[desc.GetID()] = desc
		if err := querier.AddTable(int32(descID), desc); err != nil {
			return nil, err
		}
		cdcEventTargets.Add(changefeedbase.Target{
			Type:              jobspb.ChangefeedTargetSpecification_EACH_FAMILY,
			TableID:           desc.GetID(),
			StatementTimeName: changefeedbase.StatementTimeName(desc.GetName()),
		})
	}

	prefixlessCodec := keys.SystemSQLCodec
	rfCache, err := cdcevent.NewFixedRowFetcherCache(ctx, prefixlessCodec, settings, cdcEventTargets, tableDescsBySrcID)
	if err != nil {
		return nil, err
	}

	return &sqlRowProcessor{
		querier:  querier,
		decoder:  cdcevent.NewEventDecoderWithCache(ctx, rfCache, false, false),
		settings: settings,
		ie:       ie,
	}, nil
}

var errInjected = errors.New("injected synthetic error")

func (srp *sqlRowProcessor) ProcessRow(
	ctx context.Context, txn isql.Txn, kv roachpb.KeyValue, prevValue roachpb.Value,
) (batchStats, error) {
	if srp.testingInjectFailurePercent != 0 {
		if randutil.FastUint32()%100 < srp.testingInjectFailurePercent {
			return batchStats{}, errInjected
		}
	}

	var err error
	kv.Key, err = keys.StripTenantPrefix(kv.Key)
	if err != nil {
		return batchStats{}, errors.Wrap(err, "stripping tenant prefix")
	}

	row, err := srp.decoder.DecodeKV(ctx, kv, cdcevent.CurrentRow, kv.Value.Timestamp, false)
	if err != nil {
		srp.lastRow = cdcevent.Row{}
		return batchStats{}, errors.Wrap(err, "decoding KeyValue")
	}
	srp.lastRow = row

	var parsedBeforeRow *cdcevent.Row
	if srp.querier.RequiresParsedBeforeRow() {
		before, err := srp.decoder.DecodeKV(ctx, roachpb.KeyValue{
			Key:   kv.Key,
			Value: prevValue,
		}, cdcevent.PrevRow, prevValue.Timestamp, false)
		if err != nil {
			return batchStats{}, err
		}
		parsedBeforeRow = &before
	}

	var stats batchStats
	if row.IsDeleted() {
		stats, err = srp.querier.DeleteRow(ctx, txn, srp.ie, row, parsedBeforeRow)
	} else {
		stats, err = srp.querier.InsertRow(ctx, txn, srp.ie, row, parsedBeforeRow, prevValue.RawBytes == nil)
	}
	return stats, err
}

func (srp *sqlRowProcessor) GetLastRow() cdcevent.Row {
	return srp.lastRow
}

func (srp *sqlRowProcessor) SetSyntheticFailurePercent(rate uint32) {
	srp.testingInjectFailurePercent = rate
}

var (
	forceGenericPlan = sessiondatapb.PlanCacheModeForceGeneric
	ieOverrideBase   = sessiondata.InternalExecutorOverride{
		// The OriginIDForLogicalDataReplication session variable will bind the
		// origin ID 1 to each per-statement batch request header sent by the
		// internal executor. This metadata will be plumbed to the MVCCValueHeader
		// of each written kv, and will be used by source side rangefeeds to filter
		// these replicated events, preventing data looping.
		//
		// Note that a similar ingestion side plumbing strategy will be used for
		// OriginTimestamp even though each ingested row may have a different
		// timestamp. We can still bind the OriginTimestamp to the Internal Executor
		// session before each query because 1) each IE query creates a new session;
		// 2) we do not plan to use multi row insert statements during LDR ingestion
		// via sql.
		OriginIDForLogicalDataReplication: 1,
		// Use generic query plans since our queries are extremely simple and
		// won't benefit from custom plans.
		PlanCacheMode: &forceGenericPlan,
		// We've observed in the CPU profiles that the default goroutine stack
		// of the connExecutor goroutine is insufficient for evaluation of the
		// ingestion queries, so we grow the stack right away to 32KiB.
		GrowStackSize: true,
		// We don't get any benefits from generating plan gists for internal
		// queries, so we disable them.
		DisablePlanGists: true,
		QualityOfService: &sessiondatapb.UserLowQoS,
	}
	// Have a separate override for each of the replicated queries.
	ieOverrideOptimisticInsert, ieOverrideInsert, ieOverrideDelete sessiondata.InternalExecutorOverride
)

const (
	replicatedOptimisticInsertOpName = "replicated-optimistic-insert"
	replicatedInsertOpName           = "replicated-insert"
	replicatedDeleteOpName           = "replicated-delete"
	replicatedApplyUDFOpName         = "replicated-apply-udf"
)

func getIEOverride(opName string) sessiondata.InternalExecutorOverride {
	o := ieOverrideBase
	// We want the ingestion queries to show up on the SQL Activity page
	// alongside with the foreground traffic by default. We can achieve this
	// by using the same naming scheme as AttributeToUser feature of the IE
	// override (effectively, we opt out of using the "external" metrics for
	// the ingestion queries).
	o.ApplicationName = catconstants.AttributedToUserInternalAppNamePrefix + "-" + opName
	return o
}

func init() {
	ieOverrideOptimisticInsert = getIEOverride(replicatedOptimisticInsertOpName)
	ieOverrideInsert = getIEOverride(replicatedInsertOpName)
	ieOverrideDelete = getIEOverride(replicatedDeleteOpName)
}

var tryOptimisticInsertEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.try_optimistic_insert.enabled",
	"determines whether the consumer attempts to execute the 'optimistic' INSERT "+
		"first (when there was no previous value on the source) which will succeed only "+
		"if there is no conflict with an existing row",
	metamorphic.ConstantWithTestBool("logical_replication.consumer.try_optimistic_insert.enabled", true),
)

// insertQueries stores a mapping from the table ID to a mapping from the
// column family ID to two INSERT statements (one optimistic that assumes
// there is no conflicting row in the table, and another pessimistic one).
const (
	defaultQuery = 0

	insertQueriesOptimisticIndex  = 0
	insertQueriesPessimisticIndex = 1
)

func makeSQLLastWriteWinsHandler(
	ctx context.Context,
	settings *cluster.Settings,
	tableDescs map[descpb.ID]catalog.TableDescriptor,
	ie isql.Executor,
) (*sqlRowProcessor, error) {
	var fallbackQuerier querier
	if udfName != "" {
		var err error
		fallbackQuerier, err = makeApplierQuerier(ctx, settings, tableDescs, ie)
		if err != nil {
			return nil, err
		}
	}

	qb := queryBuffer{
		deleteQueries: make(map[catid.DescID]queryBuilder, len(tableDescs)),
		insertQueries: make(map[catid.DescID]map[catid.FamilyID]queryBuilder, len(tableDescs)),
	}
	return makeSQLProcessorFromQuerier(ctx, settings, tableDescs, ie,
		&lwwQuerier{
			settings:        settings,
			queryBuffer:     qb,
			fallbackQuerier: fallbackQuerier,
		})
}

// lwwQuerier is a querier that implements partial
// last-write-wins semantics using SQL queries. We assume that the table has an
// crdb_replication_origin_timestamp column defined as:
//
//	crdb_replication_origin_timestamp DECIMAL NOT VISIBLE DEFAULT NULL ON UPDATE NULL
//
// This row is explicitly set by the INSERT query using the MVCC timestamp of
// the inbound write.
//
// Known issues:
//
//  1. An UPDATE and a DELETE may be applied out of order because we have no way
//     from SQL of knowing the write timestamp of the deletion tombstone.
//  2. The crdb_replication_origin_timestamp requires modifying the user's schema.
//
// See the design document for possible solutions to both of these problems.
type lwwQuerier struct {
	settings        *cluster.Settings
	queryBuffer     queryBuffer
	fallbackQuerier querier
}

func (lww *lwwQuerier) AddTable(targetDescID int32, td catalog.TableDescriptor) error {
	var err error
	lww.queryBuffer.insertQueries[td.GetID()], err = makeLWWInsertQueries(targetDescID, td)
	if err != nil {
		return err
	}
	lww.queryBuffer.deleteQueries[td.GetID()], err = makeLWWDeleteQuery(targetDescID, td)
	if err != nil {
		return err
	}

	if lww.fallbackQuerier != nil {
		if err := lww.fallbackQuerier.AddTable(targetDescID, td); err != nil {
			return err
		}
	}
	return nil
}

func (lww *lwwQuerier) RequiresParsedBeforeRow() bool {
	if lww.fallbackQuerier != nil {
		return lww.fallbackQuerier.RequiresParsedBeforeRow()
	}
	return false
}

func (lww *lwwQuerier) InsertRow(
	ctx context.Context,
	txn isql.Txn,
	ie isql.Executor,
	row cdcevent.Row,
	prevRow *cdcevent.Row,
	likelyInsert bool,
) (batchStats, error) {
	var kvTxn *kv.Txn
	if txn != nil {
		kvTxn = txn.KV()
	}
	insertQueryBuilder, err := lww.queryBuffer.InsertQueryForRow(row)
	if err != nil {
		return batchStats{}, err
	}
	if err := insertQueryBuilder.AddRow(row); err != nil {
		return batchStats{}, err
	}

	fallbackSpecified := lww.fallbackQuerier != nil
	shouldTryOptimisticInsert := likelyInsert && tryOptimisticInsertEnabled.Get(&lww.settings.SV)
	shouldTryOptimisticInsert = shouldTryOptimisticInsert || fallbackSpecified
	var optimisticInsertConflicts int64
	if shouldTryOptimisticInsert {
		stmt, datums, err := insertQueryBuilder.Query(insertQueriesOptimisticIndex)
		if err != nil {
			return batchStats{}, err
		}
		if _, err = ie.ExecParsed(ctx, replicatedOptimisticInsertOpName, kvTxn, ieOverrideOptimisticInsert, stmt, datums...); err != nil {
			// If the optimistic insert failed with unique violation, we have to
			// fall back to the pessimistic path. If we got a different error,
			// then we bail completely.
			if pgerror.GetPGCode(err) != pgcode.UniqueViolation {
				log.Warningf(ctx, "replicated optimistic insert failed (query: %s): %s", stmt.SQL, err.Error())
				return batchStats{}, err
			}
			optimisticInsertConflicts++
		} else {
			// There was no conflict - we're done.
			return batchStats{}, nil
		}
	}

	if fallbackSpecified {
		s, err := lww.fallbackQuerier.InsertRow(ctx, txn, ie, row, prevRow, likelyInsert)
		if err != nil {
			return batchStats{}, err
		}
		s.optimisticInsertConflicts += optimisticInsertConflicts
		return s, err
	}

	stmt, datums, err := insertQueryBuilder.Query(insertQueriesPessimisticIndex)
	if err != nil {
		return batchStats{}, err
	}
	if _, err = ie.ExecParsed(ctx, replicatedInsertOpName, kvTxn, ieOverrideInsert, stmt, datums...); err != nil {
		log.Warningf(ctx, "replicated insert failed (query: %s): %s", stmt.SQL, err.Error())
		return batchStats{}, err
	}
	return batchStats{optimisticInsertConflicts: optimisticInsertConflicts}, nil
}

func (lww *lwwQuerier) DeleteRow(
	ctx context.Context, txn isql.Txn, ie isql.Executor, row cdcevent.Row, prevRow *cdcevent.Row,
) (batchStats, error) {
	var kvTxn *kv.Txn
	if txn != nil {
		kvTxn = txn.KV()
	}
	deleteQuery, err := lww.queryBuffer.DeleteQueryForRow(row)
	if err != nil {
		return batchStats{}, err
	}

	if err := deleteQuery.AddRow(row); err != nil {
		return batchStats{}, err
	}

	stmt, datums, err := deleteQuery.Query(defaultQuery)
	if err != nil {
		return batchStats{}, err
	}

	if _, err := ie.ExecParsed(ctx, replicatedDeleteOpName, kvTxn, ieOverrideDelete, stmt, datums...); err != nil {
		log.Warningf(ctx, "replicated delete failed (query: %s): %s", stmt.SQL, err.Error())
		return batchStats{}, err
	}
	return batchStats{}, nil
}

const (
	insertQueryOptimistic  = `INSERT INTO [%d AS t] (%s) VALUES (%s)`
	insertQueryPessimistic = `
INSERT INTO [%d AS t] (%s)
VALUES (%s)
ON CONFLICT (%s)
DO UPDATE SET
%s
WHERE (t.crdb_internal_mvcc_timestamp <= excluded.crdb_replication_origin_timestamp
     AND t.crdb_replication_origin_timestamp IS NULL)
 OR (t.crdb_replication_origin_timestamp <= excluded.crdb_replication_origin_timestamp
     AND t.crdb_replication_origin_timestamp IS NOT NULL)`
)

func sqlEscapedJoin(parts []string, sep string) string {
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return lexbase.EscapeSQLIdent(parts[0])
	default:
		var s strings.Builder
		s.WriteString(lexbase.EscapeSQLIdent(parts[0]))
		for _, p := range parts[1:] {
			s.WriteString(sep)
			s.WriteString(lexbase.EscapeSQLIdent(p))
		}
		return s.String()
	}
}

func insertColumnNamesForFamily(
	td catalog.TableDescriptor, family *descpb.ColumnFamilyDescriptor, includeComputed bool,
) ([]string, error) {
	inputColumnNames := make([]string, 0)
	seenIds := make(map[catid.ColumnID]struct{})
	publicColumns := td.PublicColumns()
	colOrd := catalog.ColumnIDToOrdinalMap(publicColumns)
	addColumn := func(colID catid.ColumnID) error {
		ord, ok := colOrd.Get(colID)
		if !ok {
			return errors.AssertionFailedf("expected to find column %d", colID)
		}
		col := publicColumns[ord]
		if col.IsComputed() && !includeComputed {
			return nil
		}
		colName := col.GetName()
		// We will set crdb_replication_origin_timestamp ourselves from the MVCC timestamp of the incoming datum.
		// We should never see this on the rangefeed as a non-null value as that would imply we've looped data around.
		if colName == originTimestampColumnName {
			return nil
		}
		if _, seen := seenIds[colID]; seen {
			return nil
		}
		if colName != originTimestampColumnName {
			inputColumnNames = append(inputColumnNames, colName)
		}
		seenIds[colID] = struct{}{}
		return nil
	}

	primaryIndex := td.GetPrimaryIndex()
	for i := 0; i < primaryIndex.NumKeyColumns(); i++ {
		if err := addColumn(primaryIndex.GetKeyColumnID(i)); err != nil {
			return nil, err
		}
	}

	for i := range family.ColumnNames {
		if err := addColumn(family.ColumnIDs[i]); err != nil {
			return nil, err
		}
	}
	return inputColumnNames, nil
}

func valueStringForNumItems(count int, startIndex int) string {
	var valueString strings.Builder
	for i := 0; i < count; i++ {
		if i == 0 {
			fmt.Fprintf(&valueString, "$%d", i+startIndex)
		} else {
			fmt.Fprintf(&valueString, ", $%d", i+startIndex)
		}

	}
	return valueString.String()
}

func makeLWWInsertQueries(
	dstTableDescID int32, td catalog.TableDescriptor,
) (map[catid.FamilyID]queryBuilder, error) {
	queryBuilders := make(map[catid.FamilyID]queryBuilder, td.NumFamilies())
	if err := td.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
		var columnNames strings.Builder
		var onConflictUpdateClause strings.Builder
		argIdx := 1
		addColToQueryParts := func(colName string) {
			colName = lexbase.EscapeSQLIdent(colName)
			if argIdx == 1 {
				columnNames.WriteString(colName)
				fmt.Fprintf(&onConflictUpdateClause, "%s = excluded.%[1]s", colName)
			} else {
				fmt.Fprintf(&columnNames, ", %s", colName)
				fmt.Fprintf(&onConflictUpdateClause, ",\n%s = excluded.%[1]s", colName)
			}
			argIdx++
		}
		inputColumnNames, err := insertColumnNamesForFamily(td, family, false)
		if err != nil {
			return err
		}
		for _, name := range inputColumnNames {
			addColToQueryParts(name)
		}

		addColToQueryParts(originTimestampColumnName)
		valStr := valueStringForNumItems(len(inputColumnNames)+1, 1)
		parsedOptimisticQuery, err := parser.ParseOne(fmt.Sprintf(insertQueryOptimistic,
			dstTableDescID,
			columnNames.String(),
			valStr,
		))
		if err != nil {
			return err
		}

		parsedPessimisticQuery, err := parser.ParseOne(fmt.Sprintf(insertQueryPessimistic,
			dstTableDescID,
			columnNames.String(),
			valStr,
			sqlEscapedJoin(td.TableDesc().PrimaryIndex.KeyColumnNames, ","),
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
			inputColumns:         inputColumnNames,
			scratchDatums:        make([]interface{}, len(inputColumnNames)+1),
		}
		return err
	}); err != nil {
		return nil, err
	}
	return queryBuilders, nil
}

func makeLWWDeleteQuery(dstTableDescID int32, td catalog.TableDescriptor) (queryBuilder, error) {
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
        AND t.crdb_replication_origin_timestamp IS NULL)
    OR (t.crdb_replication_origin_timestamp < $%[3]d
        AND t.crdb_replication_origin_timestamp IS NOT NULL))`
	stmt, err := parser.ParseOne(
		fmt.Sprintf(baseQuery, dstTableDescID, whereClause.String(), originTSIdx))
	if err != nil {
		return queryBuilder{}, err
	}

	return queryBuilder{
		stmts:                []statements.Statement[tree.Statement]{stmt},
		inputColumns:         names,
		needsOriginTimestamp: true,
		scratchDatums:        make([]interface{}, len(names)+1),
	}, nil
}
