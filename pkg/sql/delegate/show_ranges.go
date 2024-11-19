// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// The SHOW RANGES "statement" is really a family of related statements,
// selected via various syntax forms.
//
// In summary, we have:
// - SHOW CLUSTER RANGES which also includes ranges not belonging to
//   any table.
// - SHOW RANGES [FROM DATABASE] which includes only ranges
//   overlapping with any table in the target db.
// - SHOW RANGES FROM TABLE selects only ranges that overlap with the
//   given table.
// - SHOW RANGES FROM INDEX selects only ranges that overlap with the
//   given index.
//
// Then:
// - if WITH TABLES is specified, the rows are duplicated to detail
//   each table included in a range.
// - if WITH INDEXES is specified, the rows are duplicated to detail
//   each index included in a range.
// - otherwise, there is just 1 row per range.
//
// Then:
// - if WITH DETAILS is specified, extra _expensive_ information is
//   included in the result, as of crdb_internal.ranges.
//   (requires more roundtrips; makes the operation slower overall)
// - otherwise, only data from crdb_internal.ranges_no_leases is included.
//
// Then:
// - if WITH EXPLAIN is specified, the statement simply returns the
//   text of the SQL query it would use if WITH EXPLAIN was not
//   specified. This can be used for learning or troubleshooting.
//
// The overall semantics are best described using a diagram; see
// the interleaved diagram comments in the delegateShowRanges logic below.

// delegateShowRanges implements the SHOW RANGES statement:
//
//	SHOW RANGES FROM TABLE t
//	SHOW RANGES FROM INDEX t@idx
//	SHOW RANGES [ FROM DATABASE db | FROM CURRENT_CATALOG ]
//	SHOW CLUSTER RANGES
//
// These statements show the ranges corresponding to the given table or index,
// along with the list of replicas and the lease holder.
//
// See the explanatory comment at the top of this file for details.
func (d *delegator) delegateShowRanges(n *tree.ShowRanges) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Ranges)

	// Check option compatibility.
	switch n.Options.Mode {
	case tree.ExpandIndexes:
		switch n.Source {
		case tree.ShowRangesIndex:
			return nil, pgerror.Newf(pgcode.Syntax, "cannot use WITH INDEXES with SHOW RANGES FROM INDEX")
		}
	case tree.ExpandTables:
		switch n.Source {
		case tree.ShowRangesIndex, tree.ShowRangesTable:
			return nil, pgerror.Newf(pgcode.Syntax, "cannot use WITH TABLES with SHOW RANGES FROM [INDEX|TABLE]")
		}
	}

	// Resolve the target table/index if running FROM TABLE/INDEX.
	var idx cat.Index
	var resName tree.TableName
	switch n.Source {
	case tree.ShowRangesTable, tree.ShowRangesIndex:
		var err error
		idx, resName, err = cat.ResolveTableIndex(
			d.ctx, d.catalog, cat.Flags{
				AvoidDescriptorCaches: true,
				// SHOW RANGES is valid over non-public tables or indexes;
				// they may still have data worthy of inspection.
				IncludeNonActiveIndexes: true,
				IncludeOfflineTables:    true,
			}, &n.TableOrIndex,
		)
		if err != nil {
			return nil, err
		}
		if idx.Table().IsVirtualTable() {
			return nil, errors.New("SHOW RANGES may not be called on a virtual table")
		}
	}

	var buf strings.Builder

	// The first stage constructs the list of all ranges
	// augmented with rows corresponding to tables/indexes.

	//                       SHOW RANGES
	//                             |
	//                       query from
	//                crdb_internal.ranges_no_lease
	//                             |
	//             include range_id, range boundaries
	//                             |
	//                (we are going to later include
	//                table/index boundaries as well)
	//                             |
	//                    include table_id
	//                  and target object boundaries
	//                             |
	//             .---------------^-----------------.
	//             |                                  |
	//          syntax:                               |
	//       WITH INDEXES                  (no index details requested)
	//            or                                  |
	//       FROM INDEX                               |
	//             |                                  |
	//      also include index_id                     |
	//             |                                  |
	//             `---------------v------------------'
	//                             |
	//                            ...
	//

	buf.WriteString(`
WITH ranges AS (
SELECT
  r.range_id,
  r.start_key,
  r.end_key,
 `)

	// Start with all the ranges_no_leases columns. This takes
	// care of range_id, start/end_key etc.
	for _, c := range sharedRangesCols {
		fmt.Fprintf(&buf, " r.%s,", tree.NameString(c))
	}
	// Also include the object IDs from the span table included below.
	buf.WriteString("\n  s.descriptor_id AS table_id,")
	if n.Source == tree.ShowRangesIndex || n.Options.Mode == tree.ExpandIndexes {
		buf.WriteString("\n  s.index_id,")
	}
	// As well as the span boundaries, if any.
	buf.WriteString(`
  s.start_key AS object_start_key,
  s.end_key AS object_end_key,
  IF(s.start_key < r.start_key, r.start_key, s.start_key) AS per_range_start_key,
  IF(s.end_key < r.end_key, s.end_key, r.end_key) AS per_range_end_key
FROM crdb_internal.ranges_no_leases r
`)

	//
	//                            ...
	//                             |
	//            .----------------^-----------------.
	//            |                                  |
	//          syntax:                          syntax:
	//      SHOW CLUSTER RANGES               SHOW RANGES [FROM ...]
	//            |                                  |
	//         join using                        join using
	//       LEFT OUTER JOIN                     INNER JOIN
	//     (allows NULLs in join)                    |
	//             |                                 |
	//             `---------------v-----------------'
	//                             |
	//                            ...
	//

	if n.Source == tree.ShowRangesCluster {
		// The join with the spans table will _augment_ the range rows; if
		// there is no matching span for a given range, the span columns
		// will remain NULL.
		buf.WriteString(`LEFT OUTER JOIN `)
	} else {
		// The join with the spans table will _filter_ the range rows;
		// if there is no matching span for a given range, the range will
		// be omitted from the output.
		buf.WriteString(`INNER JOIN `)
	}

	//
	//                            ...
	//                             |
	//             .---------------^------------------.
	//             |                                  |
	//          syntax:                               |
	//       WITH INDEXES                  (no index details requested)
	//            or                                  |
	//       FROM INDEX                               |
	//             |                                  |
	//        join with                          join with
	//     crdb_internal.index_spans        crdb_internal.table_spans
	//             |                                 |
	//     result: at least one row           result: at least one row
	//     per range-index intersection       per range-table intersection
	//             |                                 |
	//             `---------------v-----------------'
	//                             |
	//                            ...
	//

	// Filter by the target database. We use the property that the
	// vtable "DB.crdb_internal.{table,index}_spans" only return spans
	// for objects in the database DB.
	var dbName tree.Name
	switch n.Source {
	case tree.ShowRangesCluster:
		// This selects "".crdb_internal.table_spans, which gathers spans
		// over all databases.
		dbName = ""
	case tree.ShowRangesDatabase:
		dbName = n.DatabaseName
	case tree.ShowRangesCurrentDatabase:
		dbName = tree.Name(d.evalCtx.SessionData().Database)
	case tree.ShowRangesTable, tree.ShowRangesIndex:
		dbName = resName.CatalogName
	default:
		return nil, errors.AssertionFailedf("programming error: missing db prefix %#v", n)
	}
	buf.WriteString(dbName.String())

	// Which span vtable to use.
	if n.Source == tree.ShowRangesIndex || n.Options.Mode == tree.ExpandIndexes {
		// SHOW RANGES FROM INDEX, or SHOW RANGES WITH INDEXES: we'll want index-level details.
		buf.WriteString(".crdb_internal.index_spans")
	} else {
		// Either WITH TABLES, or no details. Even when WITH TABLES is not
		// speciifed we still join on table_spans: this forces filtering
		// on the database in SHOW RANGES FROM DATABASE thanks to
		// the inner filtering performed by the vtable off the database
		// name prefix.
		buf.WriteString(".crdb_internal.table_spans")
	}

	//
	//                            ...
	//                             |
	//            .----------------^------------.---------------------------------.
	//            |                             |                                 |
	//         syntax:                      syntax:                            syntax:
	//       SHOW RANGES                   SHOW RANGES                        SHOW RANGES
	//      [FROM DATABASE]                FROM TABLE                         FROM INDEX
	//           or                             |                                 |
	//    SHOW CLUSTER RANGES                   |                                 |
	//            |                             |                                 |
	//        join condition:                join condition:                  join condition:
	//      span included in range      span included in range            span included in range
	//            |                            AND                              AND
	//            |                     span for selected table            span for selected index
	//            |                             |                                 |
	//            `----------------v------------^---------------------------------'
	//                             |
	//                            ...
	//

	// The "main" join condition: match each range to just the spans
	// that it intersects with.
	buf.WriteString(` s
 ON s.start_key < r.end_key
AND s.end_key > r.start_key`)

	// Do we also filter on a specific table or index?
	switch n.Source {
	case tree.ShowRangesTable, tree.ShowRangesIndex:
		// Yes: add the additional filter on the span table.
		fmt.Fprintf(&buf, "\n AND s.descriptor_id = %d", idx.Table().ID())
		if n.Source == tree.ShowRangesIndex {
			fmt.Fprintf(&buf, " AND s.index_id = %d", idx.ID())
		}
	}

	// Exclude dropped tables from .crdb_internal.table_spans
	if n.Source != tree.ShowRangesIndex && n.Options.Mode != tree.ExpandIndexes {
		buf.WriteString(" AND s.dropped = false")
	}

	buf.WriteString("\n)") // end of ranges CTE.

	// Now, enhance the result set so far with additional table/index
	// details if requested; or remove range duplicates if no
	// table/index details were requested.

	//
	//                            ...
	//                             |
	//            .----------------^------------.---------------------------------.
	//            |                             |                                 |
	//       WITH TABLES                   WITH INDEXES                   (no table/index details requested)
	//            |                             |                                 |
	//  LEFT OUTER JOIN using           LEFT OUTER JOIN using                     |
	//    crdb_internal.tables          crdb_internal.table_indexes            DISTINCT
	//            |                          and                                  |
	//            |                     crdb_internal.tables                      |
	//            |                             |                                 |
	//         result:                        result:                         result:
	//     at least one row              at least one row                   one row per range
	//     per range-table               per range-index                          |
	//     intersection                  intersection                             |
	//            |                             |                                 |
	//         add columns:              add columns:                             |
	//      schema_name, table_name    schema_name, table_name, index_name,       |
	//      table_start_key,           index_start_key,                           |
	//      table_end_key              index_end_key                              |
	//            |                             |                                 |
	//     also add database_name        also add database_name                   |
	//     if SHOW CLUSTER RANGES        if SHOW CLUSTER RANGES                   |
	//            |                             |                                 |
	//            `----------------v------------^---------------------------------'
	//                             |
	//                            ...
	//

	mode := n.Options.Mode
	if n.Source == tree.ShowRangesIndex {
		// The index view needs to see table_id/index_id propagate. We
		// won't have duplicate ranges anyways: there's just 1 index
		// selected.
		mode = tree.ExpandIndexes
	}

	// In SHOW RANGES FROM CLUSTER WITH TABLES/INDEXES, we also list the
	// database name.
	dbNameCol := ""
	if n.Source == tree.ShowRangesCluster && mode != tree.UniqueRanges {
		dbNameCol = ", database_name"
	}

	// Include all_span_stats if DETAILS is a requested option.
	if n.Options.Details {
		var arrayArgs string
		if mode == tree.UniqueRanges {
			arrayArgs = "SELECT DISTINCT (start_key, end_key)"
		} else {
			arrayArgs = "SELECT (per_range_start_key, per_range_end_key)"
		}
		fmt.Fprintf(&buf, `,
all_span_stats AS (
	SELECT
		start_key,
		end_key,
		jsonb_build_object(
			'approximate_disk_bytes', stats->'approximate_disk_bytes',
			'key_count', stats->'total_stats'->'key_count',
			'key_bytes', stats->'total_stats'->'key_bytes',
			'val_count', stats->'total_stats'->'val_count',
			'val_bytes', stats->'total_stats'->'val_bytes',
			'sys_count', stats->'total_stats'->'sys_count',
			'sys_bytes', stats->'total_stats'->'sys_bytes',
			'live_count', stats->'total_stats'->'live_count',
			'live_bytes', stats->'total_stats'->'live_bytes',
			'intent_count', stats->'total_stats'->'intent_count',
			'intent_bytes', stats->'total_stats'->'intent_bytes'
		) as stats
	FROM crdb_internal.tenant_span_stats(ARRAY(%s FROM ranges))
)`,
			arrayArgs,
		)
	}

	buf.WriteString(",\nnamed_ranges AS (")

	switch mode {
	case tree.UniqueRanges:
		// De-duplicate the range rows, keep only the range metadata.
		buf.WriteString("\nSELECT DISTINCT\n range_id, start_key, end_key")
		for _, c := range sharedRangesCols {
			fmt.Fprintf(&buf, ", %s", tree.NameString(c))
		}
		buf.WriteString("\nFROM ranges")

	case tree.ExpandTables:
		// Add table name details.
		fmt.Fprintf(&buf, `
          SELECT r.* %[2]s, t.schema_name, t.name AS table_name
            FROM ranges r
 LEFT OUTER JOIN %[1]s.crdb_internal.tables t
              ON r.table_id = t.table_id`, dbName.String(), dbNameCol)

	case tree.ExpandIndexes:
		// Add table/index name details.
		fmt.Fprintf(&buf, `
          SELECT r.* %[2]s, t.schema_name, t.name AS table_name, ti.index_name
            FROM ranges r
 LEFT OUTER JOIN %[1]s.crdb_internal.table_indexes ti
              ON r.table_id = ti.descriptor_id
             AND r.index_id = ti.index_id
 LEFT OUTER JOIN %[1]s.crdb_internal.tables t
              ON r.table_id = t.table_id`, dbName.String(), dbNameCol)
	}
	buf.WriteString("\n),\n") // end of named_ranges CTE.
	buf.WriteString("intermediate AS (SELECT r.*")
	// If details were requested, also include the extra
	// columns from crdb_internal.ranges.
	if n.Options.Details {
		fmt.Fprintf(&buf, ",\n  %s", colinfo.RangesExtraRenders)

		// When the row identifier is a range ID, we must find span stats
		// that match the range start and end keys.
		// Otherwise, we are free to use the "span_(end|start)_key" identifier.
		var startKey string
		var endKey string
		if mode == tree.UniqueRanges {
			startKey = "r.start_key"
			endKey = "r.end_key"
		} else {
			startKey = "r.per_range_start_key"
			endKey = "r.per_range_end_key"
		}

		fmt.Fprintf(&buf, `, (SELECT stats FROM all_span_stats sps WHERE %s = sps.start_key AND %s = sps.end_key LIMIT 1) AS span_stats`,
			startKey,
			endKey,
		)
	}
	buf.WriteString("\nFROM named_ranges r)\n")

	// Time to assemble the final projection.

	//
	//                            ...
	//                             |
	//            .----------------^------------.---------------------------------.
	//            |                             |                                 |
	//         syntax:                      syntax:                            syntax:
	//       SHOW RANGES                   SHOW RANGES                        SHOW RANGES
	//      [FROM DATABASE]                FROM TABLE                         FROM INDEX
	//           or                             |                                 |
	//    SHOW CLUSTER RANGES                   |                                 |
	//            |                             |                                 |
	//     leave start/end keys         strip the table prefix            strip the index prefix
	//        as-is                       from start/end keys              from start/end keys
	//            |                             |                                 |
	//            `----------------v------------^---------------------------------'
	//                             |
	//                            ...
	//

	buf.WriteString("SELECT")

	// First output is the range start/end keys.
	switch n.Source {
	case tree.ShowRangesDatabase, tree.ShowRangesCurrentDatabase, tree.ShowRangesCluster:
		// For the SHOW CLUSTER RANGES and SHOW RANGES FROM DATABASE variants,
		// the range keys are pretty-printed in full.
		buf.WriteString(`
  crdb_internal.pretty_key(r.start_key, -1) AS start_key,
  crdb_internal.pretty_key(r.end_key, -1) AS end_key,
`)

	case tree.ShowRangesTable:
		// With SHOW RANGES FROM TABLE, we elide the common table
		// prefix when possible.
		switch n.Options.Mode {
		case tree.UniqueRanges, tree.ExpandTables:
			// Note: we cannot use object_start/end_key reliably here, because
			// when Mode == tree.UniqueRanges the DISTINCT clause has
			// eliminated it. So we must recompute the span manually.
			fmt.Fprintf(&buf, `
  CASE
    WHEN r.start_key = crdb_internal.table_span(%[1]d)[1] THEN '…/<TableMin>'
    WHEN r.start_key < crdb_internal.table_span(%[1]d)[1] THEN '<before:'||crdb_internal.pretty_key(r.start_key,-1)||'>'
    ELSE '…'||crdb_internal.pretty_key(r.start_key, 1)
  END AS start_key,
  CASE
    WHEN r.end_key = crdb_internal.table_span(%[1]d)[2] THEN '…/<TableMax>'
    WHEN r.end_key > crdb_internal.table_span(%[1]d)[2] THEN '<after:'||crdb_internal.pretty_key(r.end_key,-1)||'>'
    ELSE '…'||crdb_internal.pretty_key(r.end_key, 1)
  END AS end_key,
`, idx.Table().ID())

		case tree.ExpandIndexes:
			buf.WriteString(`
  CASE
    WHEN r.start_key = object_start_key THEN '…/<IndexMin>'
    WHEN r.start_key = crdb_internal.table_span(table_id)[1] THEN '…/<TableMin>'
    WHEN r.start_key < crdb_internal.table_span(table_id)[1] THEN '<before:'||crdb_internal.pretty_key(r.start_key,-1)||'>'
    ELSE '…'||crdb_internal.pretty_key(r.start_key, 1)
  END AS start_key,
  CASE
    WHEN r.end_key = object_end_key THEN '…/…/<IndexMax>'
    WHEN r.end_key = crdb_internal.table_span(table_id)[2] THEN '…/<TableMax>'
    WHEN r.end_key > crdb_internal.table_span(table_id)[2] THEN '<after:'||crdb_internal.pretty_key(r.end_key,-1)||'>'
    ELSE '…'||crdb_internal.pretty_key(r.end_key, 1)
  END AS end_key,
`)
		default:
			return nil, errors.AssertionFailedf("programming error: missing start/end key renderer: %#v", n)
		}

	case tree.ShowRangesIndex:
		buf.WriteString(`
  CASE
    WHEN r.start_key = object_start_key THEN '…/<IndexMin>'
    WHEN r.start_key = crdb_internal.table_span(table_id)[1] THEN '…/TableMin'
    WHEN r.start_key < object_start_key THEN '<before:'||crdb_internal.pretty_key(r.start_key,-1)||'>'
    ELSE '…'||crdb_internal.pretty_key(r.start_key, 2)
  END AS start_key,
  CASE
    WHEN r.end_key = object_end_key THEN '…/<IndexMax>'
    WHEN r.end_key = crdb_internal.table_span(table_id)[2] THEN '…/<TableMax>'
    WHEN r.end_key > object_end_key THEN '<after:'||crdb_internal.pretty_key(r.end_key,-1)||'>'
    ELSE '…'||crdb_internal.pretty_key(r.end_key, 2)
  END AS end_key,
`)

	default:
		return nil, errors.AssertionFailedf("programming error: missing start/end key rendered: %#v", n)
	}

	//
	//                            ...
	//                             |
	//             .---------------^-----------------.
	//             |                                 |
	//           syntax:                             |
	//         WITH KEYS                     (no raw keys requested)
	//             |                                 |
	//         add columns:                          |
	//      raw_start_key                            |
	//      raw_end_key                              |
	//             |                                 |
	//             `---------------v-----------------'
	//                             |
	//                        also include range_id
	//                             |
	//                            ...
	//

	// If the raw keys are requested, include them now.
	if n.Options.Keys {
		buf.WriteString("  r.start_key AS raw_start_key, r.end_key AS raw_end_key,\n")
	}

	// Next output: range_id. We place the range_id close to the
	// start of the row because alongside the start/end keys
	// it is the most important piece of information.
	//
	// This is also where we switch from adding a comma at the end of
	// every column, to adding it at the beginning of new columns. We
	// need to switch because the column additions below are optional
	// and SQL is not happy with a standalone trailing comma.
	buf.WriteString("  range_id")

	//                            ...
	//                             |
	//             .---------------^-----------------.
	//             |                                 |
	//           syntax:                             |
	//         FROM TABLE                            |
	//            or                                 |
	//         CLUSTER/FROM DATABASE                 |
	//         WITH INDEXES                  (no table details requested,
	//             |                         or not needed in FROM TABLES)
	//      propagate columns:                       |
	//      schema_name, table_name,                 |
	//      table_id,                                |
	//      table_start_key,                         |
	//      table_end_key                            |
	//             |                                 |
	//             `---------------v-----------------'
	//                             |
	//                            ...
	//

	// Spell out the additional name/ID columns stemming from WITH TABLES / WITH INDEXES.
	// Note: we omit the table name in FROM TABLE because the table is already specified.
	if n.Options.Mode == tree.ExpandTables ||
		(n.Options.Mode == tree.ExpandIndexes && n.Source != tree.ShowRangesTable) {
		buf.WriteString(dbNameCol + `, schema_name, table_name, table_id`)
	}

	//                             |
	//             .---------------^-----------------.
	//             |                                 |
	//           syntax:                             |
	//         WITH INDEXES                  (no index details requested)
	//             |                                 |
	//      propagate columns:                       |
	//      index_name, index_id                     |
	//             |                                 |
	//             `---------------v-----------------'
	//                             |

	if n.Options.Mode == tree.ExpandIndexes {
		buf.WriteString(`, index_name, index_id`)
	}

	//                            ...
	//                             |
	//            .----------------^------------.---------------------------------.
	//            |                             |                                 |
	//         syntax:                      syntax:                       (no table/index details requested)
	//       WITH INDEXES                  WITH TABLES                            |
	//            |                             |                                 |
	//       propagate columns:          propagate columns:                       |
	//       index_start_key,            table_start_key                          |
	//       index_end_key               table_end_key                            |
	//            |                             |                                 |
	//    .-------^-----------------.           |                                 |
	//    |                         |           |                                 |
	//  syntax:                  syntax:        |                                 |
	//  FROM TABLE               CLUSTER        |                                 |
	//    |                         or          |                                 |
	//  elide table            FROM DATABASE    |                                 |
	//  prefix from keys            |           |                                 |
	//    |                         |           |                                 |
	//    `-------v-----------------'           |                                 |
	//            |                             |                                 |
	//         also include                   also include                        |
	//      raw index span boundaries      raw table span boundaries              |
	//            if                           if                                 |
	//       WITH KEYS is specified        WITH KEYS is specified                 |
	//            |                             |                                 |
	//            `----------------v------------^---------------------------------'
	//                             |
	//                            ...
	//

	// Spell out the spans of the included objets.
	switch n.Options.Mode {
	case tree.ExpandIndexes:
		// In SHOW CLUSTER RANGES / SHOW RANGES FROM DATABASE, we
		// spell out keys in full. In SHOW RANGES FROM TABLE, we trim the prefix.
		shift := -1
		prefix := ""
		if n.Source == tree.ShowRangesTable {
			shift = 1
			prefix = "'…'||"
		}
		fmt.Fprintf(&buf, `,
  %[2]scrdb_internal.pretty_key(per_range_start_key, %[1]d) AS index_start_key,
  %[2]scrdb_internal.pretty_key(per_range_end_key, %[1]d) AS index_end_key`, shift, prefix)

		if n.Options.Keys {
			buf.WriteString(`,
  per_range_start_key AS raw_index_start_key,
  per_range_end_key AS raw_index_end_key`)
		}

	case tree.ExpandTables:
		buf.WriteString(`,
  crdb_internal.pretty_key(per_range_start_key, -1) AS table_start_key,
  crdb_internal.pretty_key(per_range_end_key, -1) AS table_end_key`)

		if n.Options.Keys {
			buf.WriteString(`,
  per_range_start_key AS raw_table_start_key,
  per_range_end_key AS raw_table_end_key`)
		}
	}

	//
	//                            ...
	//                             |
	//                    include common projections
	//                from crdb_internal.ranges_no_leases
	//                             |
	//             .---------------^-----------------.
	//             |                                 |
	//           syntax:                             |
	//         WITH DETAILS                  (no details requested)
	//             |                                 |
	//         add columns:                          |
	//      extra projections from                   |
	//      crdb_internal.ranges                     |
	//          and extra                            |
	//      computed columns                         |
	//             |                                 |
	//             `---------------v-----------------'
	//                             |
	//                            ...
	//

	// If WITH DETAILS was specified, include some convenience
	// computed columns.
	if n.Options.Details {
		buf.WriteString(`,
  range_size / 1000000 as range_size_mb,
  lease_holder,
  replica_localities[array_position(replicas, lease_holder)] as lease_holder_locality`)
	}

	// Then include all the remaining columns from crdb_internal.ranges_no_leases.
	for _, c := range sharedRangesCols {
		fmt.Fprintf(&buf, ",\n  %s", tree.NameString(c))
	}

	// If details were requested, also include the extra
	// columns from crdb_internal.ranges.
	if n.Options.Details {
		for i := len(colinfo.RangesNoLeases); i < len(colinfo.Ranges); i++ {
			// NB: we've already output lease_holder above.
			if colinfo.Ranges[i].Name == "lease_holder" {
				continue
			}
			// Skip the errors column; it's used for internal purposes.
			if colinfo.Ranges[i].Name == "errors" {
				continue
			}
			fmt.Fprintf(&buf, ",\n  %s", tree.NameString(colinfo.Ranges[i].Name))
		}
		buf.WriteString(",\n  span_stats")
	}

	// Complete this CTE. and add an order if needed.
	buf.WriteString("\nFROM intermediate r ORDER BY r.start_key")
	switch n.Options.Mode {
	case tree.ExpandIndexes, tree.ExpandTables:
		buf.WriteString(", r.per_range_start_key")
	}

	//
	//                            ...
	//                             |
	//            .----------------^-----------------.
	//            |                                  |
	//          syntax:                              |
	//        WITH EXPLAIN                     (explain not requested)
	//            |                                  |
	//        quote the generated                 simply
	//        SQL and render it                execute the query
	//        as query result                        |
	//        for inspection                         |
	//             |                                 |
	//             `---------------v-----------------'
	//                             |
	//                            ...
	//

	// Finally, explain the query if requested.
	fullQuery := buf.String()
	if n.Options.Explain {
		// Note: don't add prettify_statement here. It would make
		// debugging invalid syntax more difficult.
		fullQuery = fmt.Sprintf(`SELECT %s AS query`, lexbase.EscapeSQLString(fullQuery))
	}
	return d.parse(fullQuery)
}

// In the shared logic above, we propagate the set of columns
// from crdb_internal.ranges_no_leases from one stage to another.
// Let's build it here.
var sharedRangesCols = func() (res []string) {
	for _, c := range colinfo.RangesNoLeases {
		if c.Hidden {
			continue
		}
		// Skip over start/end key and the range ID, which we handle manually.
		if c.Name == "range_id" || c.Name == "start_key" || c.Name == "end_key" {
			continue
		}
		// Skip over the prettified key boundaries, which we will override.
		if c.Name == "start_pretty" || c.Name == "end_pretty" {
			continue
		}
		res = append(res, c.Name)
	}
	return res
}()
