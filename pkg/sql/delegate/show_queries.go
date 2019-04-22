// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
