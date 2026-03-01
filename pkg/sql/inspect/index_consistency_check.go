// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var indexConsistencyHashEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.inspect.index_consistency_hash.enabled",
	"if false, the index consistency check skips the hash precheck and always runs the full join",
	true,
)

// indexConsistencyCheckApplicability is a lightweight version that only implements applicability logic.
type indexConsistencyCheckApplicability struct {
	tableID descpb.ID
}

var _ inspectCheckApplicability = (*indexConsistencyCheckApplicability)(nil)

// AppliesTo implements the inspectCheckApplicability interface.
func (c *indexConsistencyCheckApplicability) AppliesTo(
	codec keys.SQLCodec, span roachpb.Span,
) (bool, error) {
	return spanContainsTable(c.tableID, codec, span)
}

func (c *indexConsistencyCheckApplicability) IsSpanLevel() bool {
	return false
}

// indexConsistencyCheck verifies consistency between a table's primary index
// and a specified secondary index by streaming rows from both sides of a
// query. It reports an issue if a key exists in the primary but not the
// secondary, or vice versa.
type indexConsistencyCheck struct {
	indexConsistencyCheckApplicability

	execCfg *sql.ExecutorConfig
	indexID descpb.IndexID
	// tableVersion is the descriptor version recorded when the check was planned.
	// It is used to detect concurrent schema changes for non-AS OF inspections.
	tableVersion descpb.DescriptorVersion
	asOf         hlc.Timestamp

	tableDesc catalog.TableDescriptor
	secIndex  catalog.Index
	priIndex  catalog.Index
	rowIter   isql.Rows
	state     checkState

	// columns is a list of the columns returned by one side of the
	// queries join. The actual resulting rows from the RowContainer is
	// twice this plus the error_type column.
	columns []catalog.Column

	// lastQuery stores the SQL query executed for this check to help
	// debug internal errors by providing context about the span bounds.
	lastQuery string

	// lastQueryPlaceholders stores the placeholder values used in lastQuery.
	lastQueryPlaceholders []interface{}

	// rowCount stores the number of rows processed by the check.
	rowCount uint64
}

var _ inspectCheck = (*indexConsistencyCheck)(nil)
var _ inspectCheckApplicability = (*indexConsistencyCheck)(nil)
var _ inspectCheckRowCount = (*indexConsistencyCheck)(nil)

// Started implements the inspectCheck interface.
func (c *indexConsistencyCheck) Started() bool {
	return c.state != checkNotStarted
}

// Start implements the inspectCheck interface.
func (c *indexConsistencyCheck) Start(
	ctx context.Context, cfg *execinfra.ServerConfig, span roachpb.Span, workerIndex int,
) error {
	if err := assertCheckApplies(c, cfg.Codec, span); err != nil {
		return err
	}

	// Load up the index and table descriptors.
	if err := c.loadCatalogInfo(ctx); err != nil {
		return err
	}

	var colToIdx catalog.TableColMap
	for _, col := range c.tableDesc.PublicColumns() {
		colToIdx.Set(col.GetID(), col.Ordinal())
	}

	var pkColumns, otherColumns []catalog.Column

	for i := 0; i < c.tableDesc.GetPrimaryIndex().NumKeyColumns(); i++ {
		colID := c.tableDesc.GetPrimaryIndex().GetKeyColumnID(i)
		col := c.tableDesc.PublicColumns()[colToIdx.GetDefault(colID)]
		pkColumns = append(pkColumns, col)
		colToIdx.Set(colID, -1)
	}

	joinColumns := append([]catalog.Column(nil), pkColumns...)

	// Collect all of the columns we are fetching from the index. This
	// includes the columns involved in the index: columns, extra columns,
	// and store columns.
	colIDs := catalog.TableColSet{}
	colIDs.UnionWith(c.secIndex.CollectKeyColumnIDs())
	colIDs.UnionWith(c.secIndex.CollectSecondaryStoredColumnIDs())
	colIDs.UnionWith(c.secIndex.CollectKeySuffixColumnIDs())
	colIDs.ForEach(func(colID descpb.ColumnID) {
		pos := colToIdx.GetDefault(colID)
		if pos == -1 {
			return
		}
		col := c.tableDesc.PublicColumns()[pos]
		otherColumns = append(otherColumns, col)
		if !tree.EqCmpAllowedForEquivalentTypes(col.GetType(), col.GetType()) {
			// We cannot use types in join predicates that don't allow for equality comparisons.
			return
		}
		joinColumns = append(joinColumns, col)
	})

	c.columns = append(pkColumns, otherColumns...)

	colNames := func(cols []catalog.Column) []string {
		res := make([]string, len(cols))
		for i := range cols {
			res[i] = cols[i].GetName()
		}
		return res
	}

	pkColNames := colNames(pkColumns)
	otherColNames := colNames(otherColumns)
	allColNames := colNames(c.columns)

	// If no rows exist in the primary index span, we still need to check for dangling
	// secondary index entries. We run the check with an empty predicate, which will
	// scan the entire secondary index within the span. Any secondary index entries found
	// will be dangling since there are no corresponding primary index rows.
	predicate, queryArgs, err := getPredicateAndQueryArgs(ctx, cfg, span, c.tableDesc, c.priIndex, c.asOf, pkColNames, 1 /* endPlaceholderOffset */)
	if err != nil {
		return err
	}

	if indexConsistencyHashEnabled.Get(&c.execCfg.Settings.SV) && len(allColNames) > 0 {
		// The hash precheck uses crdb_internal.datums_to_bytes, which depends on
		// keyside.Encode. Skip if any column type isn’t encodable (i.e. TSQUERY, etc.).
		if !allColumnsDatumsToBytesCompatible(c.columns) {
			log.Dev.Infof(ctx, "skipping hash precheck for index %s: column type not compatible with datums_to_bytes",
				c.secIndex.GetName())
		} else {
			match, rowCount, hashErr := c.hashesMatch(ctx, allColNames, predicate, queryArgs)
			c.rowCount = uint64(rowCount)
			if hashErr != nil {
				if isQueryConstructionError(hashErr) {
					// If hashing fails and the error stems from query construction,
					// that's an internal bug and shouldn't be ignored.
					return errors.WithAssertionFailure(hashErr)
				}
				// For all other hash errors, log and fall back.
				log.Dev.Infof(ctx, "hash precheck failed; falling back to full check: %v", hashErr)
			}
			if match {
				// Hashes match, no corruption detected - skip the full check.
				c.state = checkHashMatched
				return nil
			}
			// Hashes don't match - corruption detected. Fall back to full check.
			log.Dev.Infof(ctx, "hash precheck detected mismatch for index %s; proceeding with full check",
				c.secIndex.GetName())
		}
	}

	joinColNames := colNames(joinColumns)
	checkQuery := c.createIndexCheckQuery(
		pkColNames, otherColNames, joinColNames,
		c.tableDesc.GetID(), c.secIndex, c.priIndex.GetID(), predicate,
	)

	// Wrap the query with AS OF SYSTEM TIME to ensure it uses the specified timestamp
	queryWithAsOf := fmt.Sprintf("SELECT * FROM (%s) AS OF SYSTEM TIME %s", checkQuery, c.asOf.AsOfSystemTime())

	// Store the query and placeholders for error reporting.
	c.lastQuery = queryWithAsOf
	c.lastQueryPlaceholders = queryArgs

	// Execute the query with AS OF SYSTEM TIME embedded in the SQL
	qos := getInspectQoS(&c.execCfg.Settings.SV)
	it, err := c.execCfg.DistSQLSrv.DB.Executor().QueryIteratorEx(
		ctx, "inspect-index-consistency-check", nil, /* txn */
		sessiondata.InternalExecutorOverride{
			User:             username.NodeUserName(),
			QualityOfService: &qos,
		},
		queryWithAsOf,
		queryArgs...,
	)
	if err != nil {
		return err
	}

	// This iterator is closed in Close(). Typically when using QueryIteratorEx, a
	// defer function is setup to automatically close the iterator. But we don't
	// do that here because the results of the iterator are used in the Next()
	// function.
	c.rowIter = it
	c.state = checkRunning
	return nil
}

// Next implements the inspectCheck interface.
func (c *indexConsistencyCheck) Next(
	ctx context.Context, cfg *execinfra.ServerConfig,
) (*inspectIssue, error) {
	// If hashes matched, there's no corruption to report.
	if c.state == checkHashMatched {
		return nil, nil
	}

	if c.rowIter == nil {
		return nil, errors.AssertionFailedf("nil rowIter unexpected")
	}

	ok, err := c.rowIter.Next(ctx)
	if err != nil {
		// Close the iterator to prevent further usage. The close may emit the
		// internal error too, but we only need to capture it once.
		_ = c.Close(ctx)
		c.state = checkDone

		issue := errorToInternalInspectIssue(err, c.asOf, c.tableDesc, c.secIndex, map[redact.RedactableString]interface{}{
			"query":              c.lastQuery, // Store the query that caused the error
			"query_placeholders": formatPlaceholders(c.lastQueryPlaceholders),
		})
		return issue, nil
	}
	if !ok {
		c.state = checkDone
		return nil, nil
	}

	// Read the error_type from the first column of the result (always index 0)
	errorTypeStr := c.rowIter.Cur()[0].String()
	// Remove quotes if present (SQL string literals come with quotes)
	errorTypeStr = strings.Trim(errorTypeStr, "'\"")

	var errorType inspectErrorType

	switch errorTypeStr {
	case string(MissingSecondaryIndexEntry):
		errorType = MissingSecondaryIndexEntry
	case string(DanglingSecondaryIndexEntry):
		errorType = DanglingSecondaryIndexEntry
	default:
		return nil, errors.AssertionFailedf("unknown error_type: %s", errorTypeStr)
	}

	// Calculate column start indices: [error_type, pri_col1, pri_col2, ..., sec_col1, sec_col2, ...]
	priColStartIdx := 1
	secColStartIdx := 1 + len(c.columns)

	// Count primary key columns
	pkColumnCount := c.tableDesc.GetPrimaryIndex().NumKeyColumns()

	var primaryKeyDatums tree.Datums
	if errorType == MissingSecondaryIndexEntry {
		// Fetch the primary index values from the primary index row data.
		for i := 0; i < pkColumnCount; i++ {
			primaryKeyDatums = append(primaryKeyDatums, c.rowIter.Cur()[priColStartIdx+i])
		}
	} else {
		// Fetch the primary index values from the secondary index row
		// data, because no primary index was found.
		for i := 0; i < pkColumnCount; i++ {
			primaryKeyDatums = append(primaryKeyDatums, c.rowIter.Cur()[secColStartIdx+i])
		}
	}
	primaryKey := tree.NewDString(primaryKeyDatums.String())

	// Extract column data based on error type
	var dataStartIdx int
	if errorType == MissingSecondaryIndexEntry {
		dataStartIdx = priColStartIdx
	} else {
		dataStartIdx = secColStartIdx
	}

	details := make(map[redact.RedactableString]interface{})
	details["row_data"] = extractRowData(c.rowIter.Cur(), c.columns, dataStartIdx)
	details["index_name"] = c.secIndex.GetName()
	details["query_placeholders"] = formatPlaceholders(c.lastQueryPlaceholders)

	return &inspectIssue{
		ErrorType:  errorType,
		AOST:       c.asOf.GoTime(),
		DatabaseID: c.tableDesc.GetParentID(),
		SchemaID:   c.tableDesc.GetParentSchemaID(),
		ObjectID:   c.tableDesc.GetID(),
		PrimaryKey: primaryKey.String(),
		Details:    details,
	}, nil
}

// Done implements the inspectCheck interface.
func (c *indexConsistencyCheck) Done(context.Context) bool {
	done := c.state == checkHashMatched || c.state == checkDone
	return done
}

// Close implements the inspectCheck interface.
func (c *indexConsistencyCheck) Close(context.Context) error {
	if c.rowIter != nil {
		// Clear the iter ahead of close to ensure we only attempt the close once.
		it := c.rowIter
		c.rowIter = nil
		if err := it.Close(); err != nil {
			return errors.Wrap(err, "closing index consistency check iterator")
		}
	}
	return nil
}

// Rows implements the inspectCheckRowCount interface.
func (c *indexConsistencyCheck) RowCount() uint64 {
	return c.rowCount
}

// loadCatalogInfo loads the table descriptor and validates the specified
// secondary index. It verifies that the index exists on the table and is
// eligible for consistency checking. If the index is valid, it stores the
// descriptor and index metadata in the indexConsistencyCheck struct.
func (c *indexConsistencyCheck) loadCatalogInfo(ctx context.Context) error {
	tableDesc, err := loadTableDesc(ctx, c.execCfg, c.tableID, c.tableVersion, c.asOf)
	if err != nil {
		return err
	}
	c.tableDesc = tableDesc

	c.priIndex = c.tableDesc.GetPrimaryIndex()

	idx, err := findIndexByID(c.tableDesc, c.indexID)
	if err != nil {
		return err
	}

	if reason := isSupportedIndexForIndexConsistencyCheck(idx, tableDesc); reason != "" {
		return errors.AssertionFailedf("unsupported index type for consistency check: %s", reason)
	}

	// We found the index and it is valid for checking.
	c.secIndex = idx

	return nil

}

// createIndexCheckQuery will make the index check query for a given
// table and secondary index using CTEs and streaming joins.
//
// The query strategy depends on whether the table has non-primary-key columns:
//
// Simple case (only primary key columns):
// - Uses CTEs as optimization barriers and two LEFT JOINs unioned together.
// - No need to handle NULL/non-NULL cases since primary key columns are non-nullable.
//
// Complex case (has non-primary-key columns):
//   - Uses CTEs as optimization barriers and four separate LEFT JOINs
//     (LOOKUP and MERGE) unioned together, split by NULL/non-NULL cases.
//
// For example, given the following table schema:
//
//	CREATE TABLE table (
//	  k INT, l INT, a INT, b INT, c INT,
//	  PRIMARY KEY (k, l),
//	  INDEX idx (a,b),
//	)
//
// The generated query will use CTEs to scan the indexes and then perform
// streaming joins to find inconsistencies between primary and secondary indexes.
//
// Complex case explanation:
//
//  1. CTEs scan both the primary index and secondary index, split by NULL/non-NULL cases.
//
//  2. Four separate LEFT JOINs find missing rows:
//     - LOOKUP JOIN for non-NULL cases (more efficient for point lookups)
//     - MERGE JOIN for NULL cases (efficient for sorted data)
//
//  3. Results are combined with UNION ALL to find all inconsistencies.
//
//  4. The results identify:
//     - Rows in primary index missing from secondary index
//     - Rows in secondary index missing from primary index
func (c *indexConsistencyCheck) createIndexCheckQuery(
	pkColumns, otherColumns, lookupColumns []string,
	tableID descpb.ID,
	index catalog.Index,
	primaryIndexID descpb.IndexID,
	predicate string,
) string {
	allColumns := append(pkColumns, otherColumns...)

	// Build join conditions using helper function
	lookupClause := buildJoinConditions(lookupColumns, "pri", "sec")
	mergeClause := buildJoinConditions(pkColumns, "pri", "sec")
	reverseLookupClause := buildJoinConditions(lookupColumns, "sec", "pri")
	reverseMergeClause := buildJoinConditions(pkColumns, "sec", "pri")

	// If there are no non-primary-key columns, we don't need to split by NULL/non-NULL
	// since there are no nullable columns to worry about. Using the complex flow
	// for this case would cause duplicate results because both the "null" and
	// "non-null" branches would scan identical data when otherColumns is empty.
	if len(otherColumns) == 0 {
		// Simple case: only use LOOKUP JOINs, no need to split by NULL/non-NULL
		var whereClause string
		if predicate != "" {
			whereClause = " WHERE " + predicate
		}
		const simpleCTEQuery = `
		WITH 
		pri_scan AS (
			SELECT %[1]s FROM [%[2]d AS table_pri]@{FORCE_INDEX=[%[3]d]}%[4]s
		),
		sec_scan AS (
			SELECT %[1]s FROM [%[2]d AS table_sec]@{FORCE_INDEX=[%[8]d]}%[4]s
		)
		
		-- 1. left join from pri to sec
		SELECT '%[19]s' AS error_type, %[9]s, %[13]s
		FROM pri_scan AS pri
		LEFT LOOKUP JOIN [%[2]d AS table_sec]@{FORCE_INDEX=[%[8]d]} AS sec
		ON %[11]s
		WHERE sec.%[12]s IS NULL
		
		UNION ALL
		
		-- 2. left join from sec to pri
		SELECT '%[20]s' AS error_type, %[15]s, %[10]s  
		FROM sec_scan AS sec
		LEFT LOOKUP JOIN [%[2]d AS table_pri]@{FORCE_INDEX=[%[3]d]} AS pri
		ON %[16]s
		WHERE pri.%[12]s IS NULL`

		// Generate NULL placeholders for missing columns
		nullCasts := generateNullPlaceholders(len(allColumns))

		return fmt.Sprintf(simpleCTEQuery,
			// 1: k, l (column list for SELECT)
			strings.Join(colRefs("", allColumns), ", "),
			// 2: tableID
			tableID,
			// 3: primaryIndexID
			primaryIndexID,
			// 4: whereClause (can be empty for non-partial indexes)
			whereClause,
			// 5-7: unused in simple case
			"", "", "",
			// 8: index.GetID()
			index.GetID(),
			// 9: pri.k, pri.l
			strings.Join(colRefs("pri", allColumns), ", "),
			// 10: sec.k, sec.l
			strings.Join(colRefs("sec", allColumns), ", "),
			// 11: lookup join conditions (pri.k = sec.k AND pri.l = sec.l)
			lookupClause,
			// 12: first PK column name for NULL checks
			encodeColumnName(pkColumns[0]),
			// 13: NULL placeholders for sec columns
			strings.Join(nullCasts, ", "),
			// 14: unused in simple case
			"",
			// 15: NULL placeholders for pri columns (in second query)
			strings.Join(nullCasts, ", "),
			// 16: reverse lookup conditions (sec.k = pri.k AND sec.l = pri.l)
			reverseLookupClause,
			// 17-18: unused in simple case
			"", "",
			// 19: MissingSecondaryIndexEntry constant
			MissingSecondaryIndexEntry,
			// 20: DanglingSecondaryIndexEntry constant
			DanglingSecondaryIndexEntry,
		)
	}

	// Build NULL filters for other columns
	nullFilters := make([]string, len(otherColumns))
	nonNullFilters := make([]string, len(otherColumns))
	for i, col := range otherColumns {
		encodedCol := encodeColumnName(col)
		nullFilters[i] = fmt.Sprintf("%s IS NULL", encodedCol)
		nonNullFilters[i] = fmt.Sprintf("%s IS NOT NULL", encodedCol)
	}

	// Build WHERE clauses using helper function
	nullFilter := buildWhereClause(predicate, nullFilters)
	nonNullFilter := buildWhereClause(predicate, nonNullFilters)
	// ORDER BY for MERGE JOIN on PK columns
	encodedPkColumns := make([]string, len(pkColumns))
	for i, col := range pkColumns {
		encodedPkColumns[i] = encodeColumnName(col)
	}
	orderBy := " ORDER BY " + strings.Join(encodedPkColumns, ", ")

	const cteStreamingQuery = `
	WITH 
	pri_nonnull AS (
		SELECT %[1]s FROM [%[2]d AS table_pri]@{FORCE_INDEX=[%[3]d]}%[4]s%[5]s
	),
	pri_null AS (
		SELECT %[1]s FROM [%[2]d AS table_pri]@{FORCE_INDEX=[%[3]d]}%[4]s%[6]s%[7]s
	),
	sec_nonnull AS (
		SELECT %[1]s FROM [%[2]d AS table_sec]@{FORCE_INDEX=[%[8]d]}%[4]s%[5]s
	),
	sec_null AS (
		SELECT %[1]s FROM [%[2]d AS table_sec]@{FORCE_INDEX=[%[8]d]}%[4]s%[6]s%[7]s
	)
	
	-- 1. left join from pri to sec, non-null cases
	SELECT '%[19]s' AS error_type, %[9]s, %[13]s
	FROM pri_nonnull AS pri
	LEFT LOOKUP JOIN [%[2]d AS table_sec]@{FORCE_INDEX=[%[8]d]} AS sec
	ON %[11]s
	WHERE sec.%[12]s IS NULL
	
	UNION ALL
	
	-- 2. left join from pri to sec, null cases  
	SELECT '%[19]s' AS error_type, %[9]s, %[13]s
	FROM pri_null AS pri
	LEFT MERGE JOIN (
		SELECT %[1]s FROM [%[2]d AS table_sec]@{FORCE_INDEX=[%[8]d]}%[6]s%[7]s
	) AS sec
	ON %[14]s
	WHERE sec.%[12]s IS NULL
	
	UNION ALL
	
	-- 3. left join from sec to pri, non-null cases
	SELECT '%[20]s' AS error_type, %[15]s, %[10]s  
	FROM sec_nonnull AS sec
	LEFT LOOKUP JOIN [%[2]d AS table_pri]@{FORCE_INDEX=[%[3]d]} AS pri
	ON %[16]s
	WHERE pri.%[12]s IS NULL
	
	UNION ALL
	
	-- 4. left join from sec to pri, null cases
	SELECT '%[20]s' AS error_type, %[17]s, %[10]s
	FROM sec_null AS sec  
	LEFT MERGE JOIN (
		SELECT %[1]s FROM [%[2]d AS table_pri]@{FORCE_INDEX=[%[3]d]}%[6]s%[7]s
	) AS pri
	ON %[18]s
	WHERE pri.%[12]s IS NULL`

	// Generate NULL placeholders for missing columns
	nullCasts := generateNullPlaceholders(len(allColumns))

	return fmt.Sprintf(cteStreamingQuery,
		// 1: k, l, a, b (column list for SELECT)
		strings.Join(colRefs("", allColumns), ", "),
		// 2: tableID
		tableID,
		// 3: primaryIndexID
		primaryIndexID,
		// 4: empty (predicate is now included in filters)
		"",
		// 5: non-null filter (WHERE col IS NOT NULL AND ...)
		nonNullFilter,
		// 6: null filter (WHERE col IS NULL AND ...)
		nullFilter,
		// 7: order by for merge join
		orderBy,
		// 8: index.GetID()
		index.GetID(),
		// 9: pri.k, pri.l, pri.a, pri.b
		strings.Join(colRefs("pri", allColumns), ", "),
		// 10: sec.k, sec.l, sec.a, sec.b
		strings.Join(colRefs("sec", allColumns), ", "),
		// 11: lookup join conditions (pri.k = sec.k AND pri.a = sec.a ...)
		lookupClause,
		// 12: first PK column name for NULL checks
		encodeColumnName(pkColumns[0]),
		// 13: NULL placeholders for sec columns
		strings.Join(nullCasts, ", "),
		// 14: merge join conditions (pri.k = sec.k ...)
		mergeClause,
		// 15: NULL placeholders for pri columns
		strings.Join(nullCasts, ", "),
		// 16: reverse lookup conditions (sec.k = pri.k ...)
		reverseLookupClause,
		// 17: NULL placeholders for pri columns
		strings.Join(nullCasts, ", "),
		// 18: reverse merge conditions (sec.k = pri.k ...)
		reverseMergeClause,
		// 19: MissingSecondaryIndexEntry constant
		MissingSecondaryIndexEntry,
		// 20: DanglingSecondaryIndexEntry constant
		DanglingSecondaryIndexEntry,
	)
}

type hashResult struct {
	rowCount int64
	hash     string
}

// hashesMatch performs a fast comparison of primary and secondary indexes by
// computing row counts and hash values. Returns true if both indexes have
// identical row counts and hash values, indicating no corruption. Returns the
// row count from the primary index.
func (c *indexConsistencyCheck) hashesMatch(
	ctx context.Context, columnNames []string, predicate string, queryArgs []interface{},
) (match bool, rowCount int64, err error) {
	primary, err := c.computeHashAndRowCount(ctx, c.priIndex, columnNames, predicate, queryArgs)
	if err != nil {
		return false, 0, errors.Wrapf(err, "computing hash for primary index %s", c.priIndex.GetName())
	}
	secondary, err := c.computeHashAndRowCount(ctx, c.secIndex, columnNames, predicate, queryArgs)
	if err != nil {
		return false, primary.rowCount, errors.Wrapf(err, "computing hash for secondary index %s", c.secIndex.GetName())
	}
	// Hashes match only if both row count and hash value are identical.
	return primary.rowCount == secondary.rowCount && primary.hash == secondary.hash, primary.rowCount, nil
}

// computeHashAndRowCount executes a hash query for the specified index and
// returns the row count and XOR aggregate hash value.
func (c *indexConsistencyCheck) computeHashAndRowCount(
	ctx context.Context,
	index catalog.Index,
	columnNames []string,
	predicate string,
	queryArgs []interface{},
) (hashResult, error) {
	query := buildIndexHashQuery(c.tableDesc.GetID(), index, columnNames, predicate)
	queryWithAsOf := fmt.Sprintf("SELECT * FROM (%s) AS OF SYSTEM TIME %s", query, c.asOf.AsOfSystemTime())

	qos := getInspectQoS(&c.execCfg.Settings.SV)
	row, err := c.execCfg.DistSQLSrv.DB.Executor().QueryRowEx(
		ctx, "inspect-index-consistency-hash", nil, /* txn */
		sessiondata.InternalExecutorOverride{
			User:             username.NodeUserName(),
			QualityOfService: &qos,
		},
		queryWithAsOf,
		queryArgs...,
	)
	if err != nil {
		return hashResult{}, err
	}
	if len(row) != 2 {
		return hashResult{}, errors.AssertionFailedf("hash query returned unexpected column count: %d", len(row))
	}
	return hashResult{
		rowCount: int64(tree.MustBeDInt(row[0])),
		hash:     string(tree.MustBeDBytes(row[1])),
	}, nil
}

// buildIndexHashQuery constructs a query that computes row count and XOR
// aggregate hash for the specified index and columns.
func buildIndexHashQuery(
	tableID descpb.ID, index catalog.Index, columnNames []string, predicate string,
) string {
	hashExpr := hashInputExpression(columnNames)
	whereClause := buildWhereClause(predicate, nil /* nullFilters */)
	return fmt.Sprintf(`
SELECT
  count(*) AS row_count,
  crdb_internal.datums_to_bytes(xor_agg(fnv64(%s))) AS hash_value
FROM [%d AS t]@{FORCE_INDEX=[%d]}%s`,
		hashExpr,
		tableID,
		index.GetID(),
		whereClause,
	)
}

// hashInputExpression creates a hash-friendly expression by encoding column
// values to bytes with NULL coalesced to empty bytes.
func hashInputExpression(columnNames []string) string {
	args := make([]string, len(columnNames))
	for i, col := range columnNames {
		args[i] = colRef("t", col)
	}
	encoded := fmt.Sprintf("crdb_internal.datums_to_bytes(%s)", strings.Join(args, ", "))
	return fmt.Sprintf("COALESCE(%s, ''::BYTES)", encoded)
}

// colRef returns the string for referencing a column, with a specific alias,
// e.g. "table.col".
func colRef(tableAlias string, columnName string) string {
	encodedColumnName := encodeColumnName(columnName)
	if tableAlias == "" {
		return encodedColumnName
	}
	return fmt.Sprintf("%s.%s", tableAlias, encodedColumnName)
}

// colRefs returns the strings for referencing a list of columns (as a list).
func colRefs(tableAlias string, columnNames []string) []string {
	res := make([]string, len(columnNames))
	for i := range res {
		res[i] = colRef(tableAlias, columnNames[i])
	}
	return res
}

// buildJoinConditions creates join conditions between two table aliases for the given columns.
// Includes a volatile predicate to prevent optimizer from simplifying the join.
func buildJoinConditions(columns []string, leftAlias, rightAlias string) string {
	conditions := make([]string, len(columns))
	for i, col := range columns {
		encodedCol := encodeColumnName(col)
		conditions[i] = fmt.Sprintf("%s.%s = %s.%s", leftAlias, encodedCol, rightAlias, encodedCol)
	}
	// Add volatile predicate to prevent optimizer simplifications. Without this
	// the SimplifyLeftJoin rule could optimize away the join between the primary
	// and secondary index.
	conditions = append(conditions, "crdb_internal.void_func() IS NOT NULL")
	return strings.Join(conditions, " AND ")
}

// generateNullPlaceholders creates a slice of NULL placeholders for missing columns.
func generateNullPlaceholders(count int) []string {
	placeholders := make([]string, count)
	for i := range placeholders {
		placeholders[i] = "NULL"
	}
	return placeholders
}

// extractRowData extracts column data from a row starting at the given index.
func extractRowData(
	row tree.Datums, columns []catalog.Column, startIdx int,
) map[string]interface{} {
	rowDetails := make(map[string]interface{})
	for rowIdx, col := range columns {
		rowDetails[col.GetName()] = row[startIdx+rowIdx].String()
	}
	return rowDetails
}

// buildWhereClause constructs WHERE clauses for the query based on predicate
// and null filters.
func buildWhereClause(predicate string, nullFilters []string) string {
	var buf strings.Builder
	hasConditions := false

	if predicate != "" {
		buf.WriteString(" WHERE ")
		buf.WriteString(predicate)
		hasConditions = true
	}

	if len(nullFilters) > 0 {
		if hasConditions {
			buf.WriteString(" AND (")
			buf.WriteString(strings.Join(nullFilters, " AND "))
			buf.WriteString(")")
		} else {
			buf.WriteString(" WHERE ")
			buf.WriteString(strings.Join(nullFilters, " AND "))
		}
		hasConditions = true
	}

	if !hasConditions {
		return ""
	}

	return buf.String()
}

// isQueryConstructionError checks if the given error is due to
// invalid syntax or references in the query construction.
func isQueryConstructionError(err error) bool {
	code := pgerror.GetPGCode(err)
	switch code {
	case pgcode.Syntax,
		pgcode.UndefinedColumn,
		pgcode.UndefinedTable,
		pgcode.UndefinedFunction,
		pgcode.DatatypeMismatch,
		pgcode.InvalidColumnReference:
		return true
	default:
		return false
	}
}

// formatPlaceholders converts query placeholder values to a string slice for JSON serialization.
func formatPlaceholders(placeholders []interface{}) []string {
	result := make([]string, len(placeholders))
	for i, placeholder := range placeholders {
		if datum, ok := placeholder.(tree.Datum); ok {
			result[i] = tree.AsStringWithFlags(datum, tree.FmtParsable)
		} else {
			result[i] = fmt.Sprintf("%v", placeholder)
		}
	}
	return result
}

// allColumnsDatumsToBytesCompatible reports whether all columns can be
// passed to crdb_internal.datums_to_bytes. Returns false if any column
// has a type that cannot be key-encoded using keyside.Encode, which
// datums_to_bytes relies on internally.
//
// REFCURSOR is technically supported by datums_to_bytes, but we still use
// ColumnTypeIsIndexable to avoid duplicating type checks. REFCURSOR is
// uncommon, so this trade-off keeps the code simpler.
func allColumnsDatumsToBytesCompatible(columns []catalog.Column) bool {
	for _, col := range columns {
		if !colinfo.ColumnTypeIsIndexable(col.GetType()) {
			return false
		}
	}
	return true
}
