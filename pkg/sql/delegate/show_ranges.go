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
	"encoding/hex"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// delegateShowRanges implements the SHOW RANGES statement:
//   SHOW RANGES FROM TABLE t
//   SHOW RANGES FROM INDEX t@idx
//   SHOW RANGES FROM DATABASE db
//
// These statements show the ranges corresponding to the given table or index,
// along with the list of replicas and the lease holder.
func (d *delegator) delegateShowRanges(n *tree.ShowRanges) (tree.Statement, error) {
	if n.DatabaseName != "" {
		const dbQuery = `
		SELECT
			table_name,
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
			replicas,
			lease_holder,
			locality
		FROM %[1]s.crdb_internal.ranges AS r
		LEFT JOIN %[1]s.crdb_internal.gossip_nodes n ON
		r.lease_holder = n.node_id
		WHERE database_name=%[2]s
		ORDER BY table_name, r.start_key
		`
		return parse(fmt.Sprintf(dbQuery, n.DatabaseName, lex.EscapeSQLString(n.DatabaseName)))
	}

	idx, _, err := cat.ResolveTableIndex(
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
  CASE WHEN r.start_key <= x'%s' THEN NULL ELSE crdb_internal.pretty_key(r.start_key, 2) END AS start_key,
  CASE WHEN r.end_key >= x'%s' THEN NULL ELSE crdb_internal.pretty_key(r.end_key, 2) END AS end_key,
  range_id,
  range_size / 1000000 as range_size_mb,
  replicas,
  lease_holder,
  locality
FROM crdb_internal.ranges AS r
LEFT JOIN crdb_internal.gossip_nodes n ON
r.lease_holder = n.node_id
WHERE (r.start_key < x'%s')
  AND (r.end_key   > x'%s') ORDER BY r.start_key
`,
		startKey, endKey, endKey, startKey,
	))
}
