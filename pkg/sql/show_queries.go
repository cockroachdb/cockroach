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

func (p *planner) ShowQueries(ctx context.Context, n *tree.ShowQueries) (planNode, error) {
	query := `TABLE crdb_internal.node_queries`
	if n.Cluster {
		query = `TABLE crdb_internal.cluster_queries`
	}
	return p.delegateQuery(ctx, "SHOW QUERIES", query, nil, nil)
}
