// Copyright 2020 The Cockroach Authors.
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

func (d *delegator) delegateShowTransactions(n *tree.ShowTransactions) (tree.Statement, error) {
	const query = `SELECT node_id, id AS txn_id, application_name, num_stmts, num_retries, num_auto_retries FROM `
	table := `"".crdb_internal.node_transactions`
	if n.Cluster {
		table = `"".crdb_internal.cluster_transactions`
	}
	var filter string
	if !n.All {
		filter = " WHERE application_name NOT LIKE '" + catconstants.InternalAppNamePrefix + "%'"
	}
	return parse(query + table + filter)
}
