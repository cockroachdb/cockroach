// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

func checkPrivilegesForShowRanges(d *delegator, table cat.Table) error {
	// Basic requirement is SELECT priviliges
	if err := d.catalog.CheckPrivilege(d.ctx, table, privilege.SELECT); err != nil {
		return err
	}
	hasAdmin, err := d.catalog.HasAdminRole(d.ctx)
	if err != nil {
		return err
	}
	// User needs to either have admin access or have the correct ZONECONFIG privilege
	if hasAdmin {
		return nil
	}
	if err := d.catalog.CheckPrivilege(d.ctx, table, privilege.ZONECONFIG); err != nil {
		return pgerror.Wrapf(err, pgcode.InsufficientPrivilege, "only users with the ZONECONFIG privilege or the admin role can use SHOW RANGES on %s", table.Name())
	}
	return nil
}

// The following columns are rendered for all the syntax forms of
// SHOW RANGES, regardless of whether WITH DETAILS is specified.
// It must be able to compute with just crdb_internal.ranges_no_leases.
const commonShowRangesRenderColumnsEnd = `
	replicas,
	replica_localities,
	voting_replicas,
	non_voting_replicas
`

// The following columns are rendered for all the syntax forms of
// SHOW RANGES, only when WITH DETAILS is specified.
// This relies on crdb_internal.ranges (expensive) under the hood.
const commonShowRangesRenderColumnsEndDetails = `
	range_size / 1000000 as range_size_mb,
	lease_holder,
	replica_localities[array_position(replicas, lease_holder)] as lease_holder_locality,
`

// delegateShowRanges implements the SHOW RANGES statement:
//
//	SHOW RANGES FROM TABLE t
//	SHOW RANGES FROM INDEX t@idx
//	SHOW RANGES FROM DATABASE db
//
// These statements show the ranges corresponding to the given table or index,
// along with the list of replicas and the lease holder.
func (d *delegator) delegateShowRanges(n *tree.ShowRanges) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Ranges)

	switch n.Source {
	case tree.ShowRangesDatabase, tree.ShowRangesCurrentDatabase, tree.ShowRangesCluster:
		return d.delegateShowRangesFromDatabase(n)
	case tree.ShowRangesTable, tree.ShowRangesIndex:
		return d.delegateShowRangesFromTableOrIndex(n)
	default:
		return nil, errors.AssertionFailedf("programming error: unsupported SHOW RANGES source %d", n.Source)
	}
}

func (d *delegator) delegateShowRangesFromDatabase(n *tree.ShowRanges) (tree.Statement, error) {
	var dbName tree.Name
	var joinMode string
	switch n.Source {
	case tree.ShowRangesCluster:
		// This selects "".crdb_internal.table_spans, which gathers spans
		// over all databases.
		dbName = ""
		// However, some ranges do not correspond to any table (tsd, etc).
		// For those, we still want the range entry but we will need
		// to leave the table/index details as NULL.
		joinMode = "LEFT OUTER JOIN"
	case tree.ShowRangesDatabase:
		dbName = n.DatabaseName
		joinMode = "JOIN"
	case tree.ShowRangesCurrentDatabase:
		// SHOW RANGES FROM CURRENT_CATALOG.
		dbName = tree.Name(d.evalCtx.SessionData().Database)
		joinMode = "JOIN"
	}

	// Are we producing details?
	detailColumns := ""
	rangeSource := "crdb_internal.ranges_no_leases"
	if n.Options.Details {
		rangeSource = "crdb_internal.ranges"
		detailColumns = ", range_size, lease_holder"
	}

	// Filter the list of ranges to include only those ranges
	// that contain spans from the target database.
	filterTable := `table_spans`
	extraColumn := ``
	if n.Options.Mode == tree.ExpandIndexes {
		filterTable = `index_spans`
		extraColumn = `s.index_id,`
	}
	rangesQ := fmt.Sprintf(`
  SELECT range_id,
         r.start_key     AS range_start_key,
         r.end_key       AS range_end_key,
         s.descriptor_id AS table_id, %[3]s
         s.start_key,
         s.end_key,
         replicas, replica_localities, voting_replicas,	non_voting_replicas
         %[5]s
    FROM "".%[6]s r
   %[4]s %[1]s.crdb_internal.%[2]s s
      ON s.start_key < r.end_key
     AND s.end_key > r.start_key`,
		// Note: dbName.String() != string(dbName)
		dbName.String(), filterTable, extraColumn, joinMode, detailColumns, rangeSource)

	// Expand table/index names if requested.
	var expandedRangesQ string
	switch n.Options.Mode {
	case tree.UniqueRanges:
		// One row per range: we can't compute table/index names.
		expandedRangesQ = fmt.Sprintf(`
SELECT DISTINCT range_id, range_start_key, range_end_key,
                replicas, replica_localities,	voting_replicas, non_voting_replicas
                %[1]s
FROM ranges`, detailColumns)

	case tree.ExpandTables:
		// One row per range-table intersection. We can compute
		// schema/table names.
		expandedRangesQ = fmt.Sprintf(`
   SELECT r.*, t.schema_name, t.name AS table_name
     FROM ranges r
 LEFT OUTER JOIN %[1]s.crdb_internal.tables t
       ON r.table_id = t.table_id`,
			// Note: dbName.String() != string(dbName)
			dbName.String())

	case tree.ExpandIndexes:
		// One row per range-index intersection. We can compute
		// schema/table and index names.
		expandedRangesQ = fmt.Sprintf(`
   SELECT r.*, t.schema_name, t.name AS table_name, ti.index_name
     FROM ranges r
 LEFT OUTER JOIN %[1]s.crdb_internal.table_indexes ti
       ON r.table_id = ti.descriptor_id
      AND r.index_id = ti.index_id
 LEFT OUTER JOIN %[1]s.crdb_internal.tables t
       ON r.table_id = t.table_id`,
			// Note: dbName.String() != string(dbName)
			dbName.String())
	}

	// Finally choose what to render.
	extraColumns := ``
	extraSort := ``
	switch n.Options.Mode {
	case tree.ExpandTables:
		extraColumns = `schema_name, table_name, table_id,
  crdb_internal.pretty_key(start_key, -1) AS table_start_key,
  crdb_internal.pretty_key(end_key, -1) AS table_end_key,`
		extraSort = `, table_start_key`

	case tree.ExpandIndexes:
		extraColumns = `schema_name, table_name, table_id, index_name, index_id,
  crdb_internal.pretty_key(start_key, -1) AS index_start_key,
  crdb_internal.pretty_key(end_key, -1) AS index_end_key,`
		extraSort = `, index_start_key`
	}

	renderRangeDetails := commonShowRangesRenderColumnsEnd
	if n.Options.Details {
		renderRangeDetails = commonShowRangesRenderColumnsEndDetails + renderRangeDetails
	}

	renderQ := fmt.Sprintf(`
SELECT
  crdb_internal.pretty_key(range_start_key, -1) AS start_key,
  crdb_internal.pretty_key(range_end_key, -1) AS end_key,
	range_id,
  %[1]s
  %[2]s
FROM named_ranges r
ORDER BY r.range_start_key %[3]s
		`,
		extraColumns,
		renderRangeDetails,
		extraSort)

	fullQuery := `WITH ranges AS (` + rangesQ + `), named_ranges AS (` + expandedRangesQ + `) ` + renderQ
	if n.Options.Explain {
		fullQuery = fmt.Sprintf(`SELECT %s AS query`, lexbase.EscapeSQLString(fullQuery))
	}
	return parse(fullQuery)
}

func (d *delegator) delegateShowRangesFromTableOrIndex(n *tree.ShowRanges) (tree.Statement, error) {
	fromTable := n.TableOrIndex.Index == ""                                     // SHOW RANGES FROM TABLE, regardless of WITH clause.
	fromIndex := !fromTable                                                     // SHOW RANGES FROM INDEX
	fromTableUniqueRanges := fromTable && n.Options.Mode == tree.UniqueRanges   // SHOW RANGES FROM TABLE
	fromTableExpandIndexes := fromTable && n.Options.Mode == tree.ExpandIndexes // SHOW RANGES FROM TABLE WITH INDEXES

	idx, resName, err := cat.ResolveTableIndex(
		d.ctx, d.catalog, cat.Flags{AvoidDescriptorCaches: true}, &n.TableOrIndex,
	)
	if err != nil {
		return nil, err
	}

	if idx.Table().IsVirtualTable() {
		return nil, errors.New("SHOW RANGES may not be called on a virtual table")
	}

	tabID := idx.Table().ID()
	idxID := idx.ID()

	// Filter the list of ranges to include only those ranges
	// that contain spans from within this table or index.
	var filterTable, extraColumn, extraFilter string
	switch {
	case fromTableUniqueRanges:
		filterTable = `table_spans`
	case fromIndex:
		filterTable = `index_spans`
		extraColumn = `s.descriptor_id AS table_id,`
		extraFilter = fmt.Sprintf(`AND s.index_id = %d`, idxID)
	case fromTableExpandIndexes:
		filterTable = `index_spans`
		extraColumn = `s.descriptor_id AS table_id, s.index_id,`
	}
	rangesQ := fmt.Sprintf(`
  SELECT range_id,
         r.start_key     AS range_start_key,
         r.end_key       AS range_end_key,
         %[5]s
         s.start_key,
         s.end_key,
         range_size, lease_holder, replicas, replica_localities,
	voting_replicas,
	non_voting_replicas
    FROM "".crdb_internal.ranges r,
         %[1]s.crdb_internal.%[2]s s
   WHERE s.start_key < r.end_key
     AND s.end_key > r.start_key
     AND s.descriptor_id = %[3]d %[4]s
`,
		// Note: dbName.String() != string(dbName)
		resName.CatalogName.String(),
		filterTable,
		tabID,
		extraFilter,
		extraColumn)

	// Expand index names if requested.
	var expandedRangesQ string
	switch {
	case fromTableUniqueRanges:
		// One row per range: we can't compute table/index names.
		expandedRangesQ = `
SELECT DISTINCT range_id, range_start_key, range_end_key, start_key, end_key,
                range_size, lease_holder, replicas, replica_localities,
	voting_replicas,
	non_voting_replicas
FROM ranges`

	case fromTableExpandIndexes:
		// One row per range-index intersection. We can compute
		// index names.
		expandedRangesQ = fmt.Sprintf(`
   SELECT r.*, ti.index_name
     FROM ranges r
     JOIN %[1]s.crdb_internal.table_indexes ti
       ON r.table_id = ti.descriptor_id
      AND r.index_id = ti.index_id`,
			// Note: dbName.String() != string(dbName)
			resName.CatalogName.String(),
		)

	case fromIndex:
		// The index name is already known. No need to report it.
		expandedRangesQ = `TABLE ranges`

	default:
		return nil, errors.AssertionFailedf("programming error: missing case for %#v", n)
	}

	// Finally choose what to render.
	var renderColumnsStart, extraSort string
	switch {
	case fromTableUniqueRanges:
		renderColumnsStart = `
CASE
  WHEN range_start_key = start_key THEN '…/<TableMin>'
  WHEN range_start_key < start_key THEN '<before:'||crdb_internal.pretty_key(range_start_key,-1)||'>'
  ELSE '…'||crdb_internal.pretty_key(range_start_key, 1)
END AS start_key,
CASE
  WHEN range_end_key = end_key THEN '…/<TableMax>'
  WHEN range_end_key > end_key THEN '<after:'||crdb_internal.pretty_key(range_end_key,-1)||'>'
  ELSE '…'||crdb_internal.pretty_key(range_end_key, 1)
END AS end_key,
range_id,`
		extraSort = ``

	case fromTableExpandIndexes:
		renderColumnsStart = `
CASE
  WHEN range_start_key = start_key THEN '…/<IndexMin>'
  WHEN range_start_key = crdb_internal.table_span(table_id)[1] THEN '…/<TableMin>'
  WHEN range_start_key < crdb_internal.table_span(table_id)[1] THEN '<before:'||crdb_internal.pretty_key(range_start_key,-1)||'>'
  ELSE '…'||crdb_internal.pretty_key(range_start_key, 1)
END AS start_key,
CASE
  WHEN range_end_key = end_key THEN '…/…/<IndexMax>'
  WHEN range_end_key = crdb_internal.table_span(table_id)[2] THEN '…/<TableMax>'
  WHEN range_end_key > crdb_internal.table_span(table_id)[2] THEN '<after:'||crdb_internal.pretty_key(range_end_key,-1)||'>'
  ELSE '…'||crdb_internal.pretty_key(range_end_key, 1)
END AS end_key,
  range_id, index_name, index_id,
  '…'||crdb_internal.pretty_key(start_key, 1) AS index_start_key,
  '…'||crdb_internal.pretty_key(end_key, 1) AS index_end_key,`
		extraSort = `, index_start_key`

	case fromIndex:
		renderColumnsStart = `
CASE
  WHEN range_start_key = start_key THEN '…/<IndexMin>'
  WHEN range_start_key = crdb_internal.table_span(table_id)[1] THEN '…/TableMin'
  WHEN range_start_key < start_key THEN '<before:'||crdb_internal.pretty_key(range_start_key,-1)||'>'
  ELSE '…'||crdb_internal.pretty_key(range_start_key, 2)
END AS start_key,
CASE
  WHEN range_end_key = end_key THEN '…/<IndexMax>'
  WHEN range_end_key = crdb_internal.table_span(table_id)[2] THEN '…/<TableMax>'
  WHEN range_end_key > end_key THEN '<after:'||crdb_internal.pretty_key(range_end_key,-1)||'>'
  ELSE '…'||crdb_internal.pretty_key(range_end_key, 2)
END AS end_key,
  range_id,`
		extraSort = ``
	}
	renderQ := fmt.Sprintf(`
SELECT
  %[1]s
  %[2]s
FROM named_ranges r
ORDER BY r.range_start_key %[3]s
		`,
		renderColumnsStart,
		commonShowRangesRenderColumnsEnd,
		extraSort)

	fullQuery := `WITH ranges AS (` + rangesQ + `), named_ranges AS (` + expandedRangesQ + `) ` + renderQ
	if n.Options.Explain {
		fullQuery = fmt.Sprintf(`SELECT %s AS query`, lexbase.EscapeSQLString(fullQuery))
	}
	return parse(fullQuery)
}
