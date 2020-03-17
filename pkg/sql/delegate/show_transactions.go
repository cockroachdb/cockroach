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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

func (d *delegator) delegateShowTransactions(n *tree.ShowTransactions) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Transactions)
	const query = `SELECT id, node_id, session_id, start, application_name FROM crdb_internal.`
	table := `node_transactions`
	if n.Cluster {
		table = `cluster_transactions`
	}
	var filter string
	if !n.All {
		filter = " WHERE application_name NOT LIKE '" + sqlbase.InternalAppNamePrefix + "%'"
	}
	return parse(query + table + filter)
}
