// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/spanutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// indexConsistencyCheck verifies consistency between a table’s primary index
// and a specified secondary index by streaming rows from both sides of a
// query. It reports an issue if a key exists in the primary but not the
// secondary, or vice versa.
type indexConsistencyCheck struct {
	flowCtx *execinfra.FlowCtx
	tableID descpb.ID
	indexID descpb.IndexID

	tableDesc     catalog.TableDescriptor
	secIndex      catalog.Index
	priIndex      catalog.Index
	rowIter       isql.Rows
	exhaustedIter bool

	// columns is a list of the columns returned by one side of the
	// queries join. The actual resulting rows from the RowContainer is
	// twice this plus the error_type column.
	columns []catalog.Column
}

var _ inspectCheck = (*indexConsistencyCheck)(nil)

// Started implements the inspectCheck interface.
func (c *indexConsistencyCheck) Started() bool {
	return c.rowIter != nil
}

// Start implements the inspectCheck interface.
func (c *indexConsistencyCheck) Start(
	ctx context.Context, cfg *execinfra.ServerConfig, span roachpb.Span, workerIndex int,
) error {
	// Early out for spans that don't apply to the table we are running the check on.
	_, tableID, err := cfg.Codec.DecodeTablePrefix(span.Key)
	if err != nil {
		return err
	}
	if descpb.ID(tableID) != c.tableID {
		return nil
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
	})

	c.columns = append(pkColumns, otherColumns...)

	colNames := func(cols []catalog.Column) []string {
		res := make([]string, len(cols))
		for i := range cols {
			res[i] = cols[i].GetName()
		}
		return res
	}

	// Generate query bounds from the span to limit the query to the specified range
	var predicate string
	var queryArgs []interface{}

	// Assert that we get meaningful spans
	if span.Key.Equal(span.EndKey) || len(span.Key) == 0 || len(span.EndKey) == 0 {
		return errors.AssertionFailedf("received invalid span: Key=%x EndKey=%x", span.Key, span.EndKey)
	}

	// Get primary key metadata for span conversion
	pkColTypes, err := spanutils.GetPKColumnTypes(c.tableDesc, c.priIndex.IndexDesc())
	if err != nil {
		return errors.Wrap(err, "getting primary key column types")
	}

	pkColDirs := make([]catenumpb.IndexColumn_Direction, c.priIndex.NumKeyColumns())
	pkColIDs := catalog.TableColMap{}
	for i := 0; i < c.priIndex.NumKeyColumns(); i++ {
		colID := c.priIndex.GetKeyColumnID(i)
		pkColIDs.Set(colID, i)
		pkColDirs[i] = c.priIndex.GetKeyColumnDirection(i)
	}

	// Convert span to query bounds
	alloc := &tree.DatumAlloc{}
	bounds, hasRows, err := spanutils.SpanToQueryBounds(
		ctx, cfg.DB.KV(), cfg.Codec, pkColIDs, pkColTypes, pkColDirs,
		len(c.tableDesc.GetFamilies()), span, alloc,
	)
	if err != nil {
		return errors.Wrap(err, "converting span to query bounds")
	}

	// Nothing to do if no rows exist in the span.
	if !hasRows {
		return nil
	}

	if len(bounds.Start) == 0 || len(bounds.End) == 0 {
		return errors.AssertionFailedf("query bounds from span didn't produce start or end: %+v", bounds)
	}

	// Generate SQL predicate from the bounds
	pkColNames := colNames(pkColumns)
	predicate, err = spanutils.RenderQueryBounds(
		pkColNames, pkColDirs, pkColTypes,
		len(bounds.Start), len(bounds.End), true, 1,
	)
	if err != nil {
		return errors.Wrap(err, "rendering query bounds")
	}

	if strings.TrimSpace(predicate) == "" {
		return errors.AssertionFailedf("query bounds from span didn't produce predicate: %+v", bounds)
	}

	// Prepare query arguments: end bounds first, then start bounds
	queryArgs = make([]interface{}, 0, len(bounds.End)+len(bounds.Start))
	for _, datum := range bounds.End {
		queryArgs = append(queryArgs, datum)
	}
	for _, datum := range bounds.Start {
		queryArgs = append(queryArgs, datum)
	}

	checkQuery := c.createIndexCheckQuery(
		colNames(pkColumns), colNames(otherColumns), c.tableDesc.GetID(), c.secIndex, c.priIndex.GetID(), predicate,
	)

	it, err := c.flowCtx.Cfg.DB.Executor().QueryIteratorEx(
		ctx, "inspect-index-consistency-check", nil, /* txn */
		sessiondata.InternalExecutorOverride{
			User:             username.NodeUserName(),
			QualityOfService: &sessiondatapb.BulkLowQoS,
		},
		checkQuery,
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
	return nil
}

// Next implements the inspectCheck interface.
func (c *indexConsistencyCheck) Next(
	ctx context.Context, cfg *execinfra.ServerConfig,
) (*inspectIssue, error) {
	if c.rowIter == nil {
		return nil, errors.AssertionFailedf("nil rowIter unexpected")
	}

	ok, err := c.rowIter.Next(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fetching next row in index consistency check")
	}
	if !ok {
		c.exhaustedIter = true
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

	return &inspectIssue{
		ErrorType: errorType,
		// TODO(148573): Use the timestamp that we create a protected timestamp for.
		AOST:       timeutil.Now(),
		DatabaseID: c.tableDesc.GetParentID(),
		SchemaID:   c.tableDesc.GetParentSchemaID(),
		ObjectID:   c.tableDesc.GetID(),
		PrimaryKey: primaryKey.String(),
		Details:    details,
	}, nil
}

// Done implements the inspectCheck interface.
func (c *indexConsistencyCheck) Done(context.Context) bool {
	// If we never started (rowIter is nil), we're done
	if c.rowIter == nil {
		return true
	}
	// Otherwise, we're done when the iterator is exhausted
	return c.exhaustedIter
}

// Close implements the inspectCheck interface.
func (c *indexConsistencyCheck) Close(context.Context) error {
	if c.rowIter != nil {
		if err := c.rowIter.Close(); err != nil {
			return errors.Wrap(err, "closing index consistency check iterator")
		}
		c.rowIter = nil
	}
	return nil
}

// loadCatalogInfo loads the table descriptor and validates the specified
// secondary index. It verifies that the index exists on the table and is
// eligible for consistency checking. If the index is valid, it stores the
// descriptor and index metadata in the indexConsistencyCheck struct.
func (c *indexConsistencyCheck) loadCatalogInfo(ctx context.Context) error {
	return c.flowCtx.Cfg.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		var err error
		c.tableDesc, err = txn.Descriptors().ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, c.tableID)
		if err != nil {
			return err
		}

		c.priIndex = c.tableDesc.GetPrimaryIndex()

		for _, idx := range c.tableDesc.PublicNonPrimaryIndexes() {
			if idx.GetID() != c.indexID {
				continue
			}

			// We can only check a secondary index that has a 1-to-1 mapping between
			// keys in the primary index. Unsupported indexes should be filtered out
			// when the job is created.
			if idx.IsPartial() {
				return errors.AssertionFailedf(
					"unsupported index type for consistency check: partial index",
				)
			}
			switch idx.GetType() {
			case idxtype.INVERTED, idxtype.VECTOR:
				return errors.AssertionFailedf(
					"unsupported index type for consistency check: %s", idx.GetType(),
				)
			}

			// We found the index and it is valid for checking.
			c.secIndex = idx
			return nil
		}
		return errors.AssertionFailedf("no index with ID %d found in table %d", c.indexID, c.tableID)
	})
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
	pkColumns, otherColumns []string,
	tableID descpb.ID,
	index catalog.Index,
	primaryIndexID descpb.IndexID,
	predicate string,
) string {
	allColumns := append(pkColumns, otherColumns...)

	// Build join conditions using helper function
	lookupClause := buildJoinConditions(allColumns, "pri", "sec")
	mergeClause := buildJoinConditions(pkColumns, "pri", "sec")
	reverseLookupClause := buildJoinConditions(allColumns, "sec", "pri")
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

// encodeColumnName properly encodes a column name for use in SQL.
func encodeColumnName(columnName string) string {
	var buf bytes.Buffer
	lexbase.EncodeRestrictedSQLIdent(&buf, columnName, lexbase.EncNoFlags)
	return buf.String()
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
