// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

func (d *delegator) delegateShowQueries(n *tree.ShowQueries) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Queries)
	const query = `
SELECT
  query_id,
  node_id,
  session_id,
  user_name,
  start,
  query,
  client_address,
  application_name,
  distributed,
  full_scan,
  phase
FROM crdb_internal.`
	table := `node_queries`
	if n.Cluster {
		table = `cluster_queries`
	}
	var filter string
	if !n.All {
		filter = " WHERE application_name NOT LIKE '" + catconstants.InternalAppNamePrefix + "%'"
	}
	return d.parse(query + table + filter)
}
