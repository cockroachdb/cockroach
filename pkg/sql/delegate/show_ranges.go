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
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

// delegateShowRanges implements the SHOW RANGES statement:
//   SHOW RANGES FROM TABLE t
//   SHOW RANGES FROM INDEX t@idx
//   SHOW RANGES FROM INDEX t@*
//   SHOW RANGES FROM DATABASE db
//
// These statements show the ranges corresponding to the given table or index,
// along with the list of replicas and the lease holder.
func (d *delegator) delegateShowRanges(n *tree.ShowRanges) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Ranges)
	if n.DatabaseName != "" {
		const dbQuery = `
		SELECT
			table_name,
			CASE WHEN index_name = '' THEN 'primary' ELSE index_name END AS index_name,
			CASE
				WHEN crdb_internal.pretty_key(r.start_key, 2) = '' THEN NULL
				ELSE crdb_internal.pretty_key(r.start_key, 2)
			END AS start_key,
			CASE
				WHEN crdb_internal.pretty_key(r.end_key, 2) = '' THEN NULL
				ELSE crdb_internal.pretty_key(r.end_key, 2)
			END AS end_key,
			range_id,
			range_size / 1000000 as range_size_mb,
			lease_holder,
    	gossip_nodes.locality as lease_holder_locality,
			replicas,
			replica_localities
		FROM %[1]s.crdb_internal.ranges AS r
	  LEFT JOIN %[1]s.crdb_internal.gossip_nodes ON lease_holder = node_id
		WHERE database_name=%[2]s
		ORDER BY table_name, index_name, r.start_key
		`
		// Note: n.DatabaseName.String() != string(n.DatabaseName)
		return parse(fmt.Sprintf(dbQuery, n.DatabaseName.String(), lex.EscapeSQLString(string(n.DatabaseName))))
	} else if n.AllIndexes {
		// If n.AllIndexes is true, the request is SHOW RANGES FROM INDEX t@*.
		flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}
		dataSource, resName, err := d.catalog.ResolveDataSource(d.ctx, flags, &n.TableOrIndex.Table)
		if err != nil {
			return nil, err
		}
		if err := d.catalog.CheckPrivilege(d.ctx, dataSource, privilege.SELECT); err != nil {
			return nil, err
		}

		// Get all indexes by taking all keys that are larger than the the smallest key from all indexes, and
		// smaller than the largest key of all the indexes.
		startKey := keys.MaxKey
		endKey := keys.MinKey
		table := dataSource.(cat.Table)
		for i := 0; i < table.IndexCount(); i++ {
			idx := table.Index(i)
			span := idx.Span()
			if bytes.Compare(span.Key, startKey) < 0 {
				startKey = span.Key
			}
			if bytes.Compare(span.EndKey, endKey) > 0 {
				endKey = span.EndKey
			}
		}

		fmt.Println(startKey, endKey)

		const query = `
		SELECT
			table_name,
			CASE WHEN index_name = '' THEN 'primary' ELSE index_name END AS index_name,
			CASE
				WHEN crdb_internal.pretty_key(r.start_key, 2) = '' THEN NULL
				ELSE crdb_internal.pretty_key(r.start_key, 2)
			END AS start_key,
			CASE
				WHEN crdb_internal.pretty_key(r.end_key, 2) = '' THEN NULL
				ELSE crdb_internal.pretty_key(r.end_key, 2)
			END AS end_key,
			range_id,
			range_size / 1000000 as range_size_mb,
			lease_holder,
    	gossip_nodes.locality as lease_holder_locality,
			replicas,
			replica_localities
		FROM %[1]s.crdb_internal.ranges AS r
	  LEFT JOIN %[1]s.crdb_internal.gossip_nodes ON lease_holder = node_id
		WHERE r.start_key < x'%[4]s' AND r.end_key > x'%[3]s'
		ORDER BY table_name, index_name, r.start_key
		`
		// note: CatalogName.String() != Catalog()
		return parse(
			fmt.Sprintf(
				query,
				resName.CatalogName.String(),
				lex.EscapeSQLString(resName.Table()),
				hex.EncodeToString([]byte(startKey)),
				hex.EncodeToString([]byte(endKey)),
			),
		)
	}

	idx, resName, err := cat.ResolveTableIndex(
		d.ctx, d.catalog, cat.Flags{AvoidDescriptorCaches: true}, &n.TableOrIndex,
	)
	if err != nil {
		return nil, err
	}
	if err := d.catalog.CheckPrivilege(d.ctx, idx.Table(), privilege.SELECT); err != nil {
		return nil, err
	}

	span := idx.Span()
	startKey := hex.EncodeToString([]byte(span.Key))
	endKey := hex.EncodeToString([]byte(span.EndKey))
	return parse(fmt.Sprintf(`
SELECT 
  CASE WHEN r.start_key <= x'%[1]s' THEN NULL ELSE crdb_internal.pretty_key(r.start_key, 2) END AS start_key,
  CASE WHEN r.end_key >= x'%[2]s' THEN NULL ELSE crdb_internal.pretty_key(r.end_key, 2) END AS end_key,
  range_id,
  range_size / 1000000 as range_size_mb,
  lease_holder,
  gossip_nodes.locality as lease_holder_locality,
  replicas,
  replica_localities
FROM %[3]s.crdb_internal.ranges AS r
LEFT JOIN %[3]s.crdb_internal.gossip_nodes ON lease_holder = node_id
WHERE (r.start_key < x'%[2]s')
  AND (r.end_key   > x'%[1]s') ORDER BY r.start_key
`,
		startKey, endKey, resName.CatalogName.String(), // note: CatalogName.String() != Catalog()
	))
}
