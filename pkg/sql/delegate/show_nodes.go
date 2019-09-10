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

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// delegateShowNodes implements the SHOW NODES statement.
func (d *delegator) delegateShowNodes(n *tree.ShowNodes) (tree.Statement, error) {
	// Note: Keep this query consistent with the one used in cockroach node status (cli/node.go).
	const query = `
		SELECT node_id AS id,
		address,
		sql_address,
		build_tag AS build,
		started_at,
		updated_at,
		locality,
		CASE WHEN split_part(expiration,',',1)::decimal > now()::decimal
			THEN true
			ELSE false
			END AS is_available,
		ifnull(is_live, false) as is_live
	FROM crdb_internal.gossip_liveness LEFT JOIN crdb_internal.gossip_nodes USING (node_id)`
	return parse(query)
}
