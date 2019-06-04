// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package delegate

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

func (d *delegator) delegateShowQueries(n *tree.ShowQueries) (tree.Statement, error) {
	const query = `SELECT query_id, node_id, user_name, start, query, client_address, application_name, distributed, phase FROM crdb_internal.`
	table := `node_queries`
	if n.Cluster {
		table = `cluster_queries`
	}
	var filter string
	if !n.All {
		filter = " WHERE application_name NOT LIKE '" + sqlbase.InternalAppNamePrefix + "%'"
	}
	return parse(query + table + filter)
}
