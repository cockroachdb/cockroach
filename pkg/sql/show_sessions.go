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

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (p *planner) ShowSessions(ctx context.Context, n *tree.ShowSessions) (planNode, error) {
	const query = `SELECT node_id, session_id, user_name, client_address, application_name, active_queries, last_active_query, session_start, oldest_query_start FROM crdb_internal.`
	table := `node_sessions`
	if n.Cluster {
		table = `cluster_sessions`
	}
	return p.delegateQuery(ctx, "SHOW SESSIONS", query+table, nil, nil)
}
