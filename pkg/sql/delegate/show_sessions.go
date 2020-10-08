// Copyright 2017 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (d *delegator) delegateShowSessions(n *tree.ShowSessions) (tree.Statement, error) {
	const query = `SELECT node_id, session_id, user_name, client_address, application_name, active_queries, last_active_query, session_start, oldest_query_start FROM crdb_internal.`
	table := `node_sessions`
	if n.Cluster {
		table = `cluster_sessions`
	}
	var filter string
	if !n.All {
		filter = " WHERE application_name NOT LIKE '" + catconstants.InternalAppNamePrefix + "%'"
	}
	return parse(query + table + filter)
}
