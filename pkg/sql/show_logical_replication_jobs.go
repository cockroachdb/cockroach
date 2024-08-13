// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

var showLogicalReplicationJobsCols = colinfo.ResultColumns{
	{Name: "job_id", Typ: types.Int},
	{Name: "targets", Typ: types.StringArray},
	{Name: "status", Typ: types.String},
	{Name: "replicated_time", Typ: types.TimestampTZ},
}

var withDetailsCols = colinfo.ResultColumns{
	{Name: "replicated_start_time", Typ: types.TimestampTZ},
}

type showLogicalReplicationJobsNode struct {
	name        string
	columns     colinfo.ResultColumns
	withDetails bool
}

func (p *planner) ShowLogicalReplicationJobs(
	ctx context.Context, n *tree.ShowLogicalReplicationJobs,
) (planNode, error) {
	// TODO(azhu): implement
	node := &showLogicalReplicationJobsNode{
		name:        n.String(),
		withDetails: n.WithDetails,
		columns:     showLogicalReplicationJobsCols,
	}
	if n.WithDetails {
		node.columns = append(node.columns, withDetailsCols...)
	}
	return node, nil

}

func (n *showLogicalReplicationJobsNode) startExec(params runParams) error {
	// TODO(azhu): implement
	return nil
}

func (n *showLogicalReplicationJobsNode) Next(params runParams) (bool, error) {
	// TODO(azhu): implement
	return false, nil
}

func (n *showLogicalReplicationJobsNode) Values() tree.Datums {
	// TODO(azhu): implement
	return tree.Datums{}
}

func (n *showLogicalReplicationJobsNode) Close(_ context.Context) {
	// TODO(azhu): implement
}
