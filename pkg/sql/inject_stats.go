// Copyright 2018 The Cockroach Authors.
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
	"fmt"

	gojson "encoding/json"

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// InjectTableStats implements the
//   INJECT STATISTICSS FOR TABLE <table> <stats_json>
// statement, which deletes any existing statistics on the table and replaces
// them with the statistics in the given json object (in the same format as the
// result of SHOW STATISTICS AS JSON).
// k$Privileges: INSERT on table.
func (p *planner) InjectTableStats(
	ctx context.Context, n *tree.InjectTableStats,
) (planNode, error) {
	tn, err := n.Table.Normalize()
	if err != nil {
		return nil, err
	}
	desc, err := ResolveExistingObject(ctx, p, tn, true /*required*/, requireTableDesc)
	if err != nil {
		return nil, err
	}
	if err := p.CheckPrivilege(ctx, desc, privilege.INSERT); err != nil {
		return nil, err
	}

	return &delayedNode{
		name: n.String(),
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			typedExpr, err := tree.TypeCheckAndRequire(
				n.Stats, &p.semaCtx, types.JSON, "INJECT STATISTICS",
			)
			if err != nil {
				return nil, err
			}
			val, err := typedExpr.Eval(p.EvalContext())
			if err != nil {
				return nil, err
			}
			if val == tree.DNull {
				return nil, fmt.Errorf("statistics cannot be NULL")
			}
			jsonStr, err := json.Pretty(val.(*tree.DJSON).JSON)
			if err != nil {
				return nil, err
			}

			var stats []stats.JSONStatistic
			if err := gojson.Unmarshal([]byte(jsonStr), &stats); err != nil {
				return nil, err
			}

			// We will be doing multiple p.exec() calls; turn off auto-commit.
			if p.autoCommit {
				defer func() { p.autoCommit = true }()
				p.autoCommit = false
			}

			// First, delete all statistics for the table.
			_, err = p.exec(ctx, `DELETE FROM system.table_statistics WHERE "tableID" = $1`, desc.ID)
			if err != nil {
				return nil, err
			}

			// Insert each statistic.
			for i := range stats {
				s := &stats[i]
				h, err := s.GetHistogram(p.EvalContext())
				if err != nil {
					return nil, err
				}
				// histogram will be passed to the INSERT statement; we want it to be a
				// nil interface{} if we don't generate a histogram.
				var histogram interface{}
				if h != nil {
					histogram, err = protoutil.Marshal(h)
					if err != nil {
						return nil, err
					}
				}

				columnIDs := tree.NewDArray(types.Int)
				for _, colName := range s.Columns {
					colDesc, _, err := desc.FindColumnByName(tree.Name(colName))
					if err != nil {
						return nil, err
					}
					if err := columnIDs.Append(tree.NewDInt(tree.DInt(colDesc.ID))); err != nil {
						return nil, err
					}
				}
				var name interface{}
				if s.Name != "" {
					name = s.Name
				}
				_, err = p.exec(
					ctx,
					`INSERT INTO system.table_statistics (
					"tableID",
					"name",
					"columnIDs",
					"createdAt",
					"rowCount",
					"distinctCount",
					"nullCount",
					histogram
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
					desc.ID,
					name,
					columnIDs,
					s.CreatedAt,
					s.RowCount,
					s.DistinctCount,
					s.NullCount,
					histogram,
				)
				if err != nil {
					return nil, err
				}
			}

			return newZeroNode(nil /* columns */), nil
		},
	}, nil
}
