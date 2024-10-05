// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (d *delegator) delegateShowSessions(n *tree.ShowSessions) (tree.Statement, error) {
	columns := `node_id, session_id, status, user_name, client_address, application_name, active_queries, 
       last_active_query, session_start, active_query_start, num_txns_executed`
	if d.evalCtx.Settings.Version.IsActive(d.ctx, clusterversion.V24_1Start) {
		columns = fmt.Sprintf("%s, trace_id, goroutine_id", columns)
	}

	query := fmt.Sprintf(`SELECT %s FROM crdb_internal.`, columns)
	table := `node_sessions`
	if n.Cluster {
		table = `cluster_sessions`
	}
	var filter string
	if !n.All {
		filter = " WHERE application_name NOT LIKE '" + catconstants.InternalAppNamePrefix + "%'"
	}
	return d.parse(query + table + filter)
}
