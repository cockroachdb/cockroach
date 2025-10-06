// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (d *delegator) delegateShowTransactions(n *tree.ShowTransactions) (tree.Statement, error) {
	const query = `
SELECT
  node_id,
  id AS txn_id,
  application_name,
  num_stmts,
  num_retries,
  num_auto_retries, 
  last_auto_retry_reason
FROM `
	table := `"".crdb_internal.node_transactions`
	if n.Cluster {
		table = `"".crdb_internal.cluster_transactions`
	}
	var filter string
	if !n.All {
		filter = " WHERE application_name NOT LIKE '" + catconstants.InternalAppNamePrefix + "%'"
	}
	return d.parse(query + table + filter)
}
