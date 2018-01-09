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
	"regexp"
	"strconv"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

var nodeStoreRangeRE = struct {
	once sync.Once
	*regexp.Regexp
}{}

// showTraceReplicaNode is a planNode that wraps another node and uses session
// tracing (via SHOW TRACE) to report the replicas of all kv events that occur
// during its execution. It is used as the top-level node for SHOW
// EXPERIMENTAL_REPLICA TRACE FOR statements.
type showTraceReplicaNode struct {
	optColumnsSlot

	// plan is the wrapped execution plan that will be traced.
	plan planNode

	run struct {
		values tree.Datums
	}
}

// ShowTraceReplica shows the replicas of all kv events that occur during
// execution of a query.
//
// Privileges: None.
func (p *planner) ShowTraceReplica(ctx context.Context, n *tree.ShowTrace) (planNode, error) {
	// TODO(dan): This works by selecting trace lines matching certain event
	// logs in command execution, which is possibly brittle. A much better
	// system would be to set the `ReturnRangeInfo` flag on all kv requests and
	// use the `RangeInfo`s that come back. Unfortunately, we wanted to get
	// _some_ version of this into 2.0 for partitioning users, but the RangeInfo
	// plumbing would have sunk the project. It's also possible that the
	// sovereignty work may require the RangeInfo plumbing and we should revisit
	// this then.
	query := fmt.Sprintf(`
		SELECT timestamp, tag FROM [SHOW TRACE FOR %s]
			WHERE message = 'read-write path'
			OR message = 'read-only path'
			OR message = 'admin path'
	`, n.Statement)
	plan, err := p.delegateQuery(ctx, `SHOW TRACE`, query, nil, nil)
	if err != nil {
		return nil, err
	}
	return &showTraceReplicaNode{plan: plan}, nil
}

func (n *showTraceReplicaNode) startExec(params runParams) error {
	nodeStoreRangeRE.once.Do(func() {
		nodeStoreRangeRE.Regexp = regexp.MustCompile(`^\[n(\d+),s(\d+),r(\d+)/`)
	})
	return nil
}

func (n *showTraceReplicaNode) Next(params runParams) (bool, error) {
	ok, err := n.plan.Next(params)
	if !ok || err != nil {
		return ok, err
	}

	values := n.plan.Values()
	timestampD, tagD := values[0], values[1]
	tag := tagD.(*tree.DString)
	matches := nodeStoreRangeRE.FindStringSubmatch(string(*tag))
	if matches == nil {
		return false, errors.Errorf(`could not extract node, store, range from: %s`, tag)
	}
	nodeID, err := strconv.Atoi(matches[1])
	if err != nil {
		return false, err
	}
	storeID, err := strconv.Atoi(matches[2])
	if err != nil {
		return false, err
	}
	rangeID, err := strconv.Atoi(matches[3])
	if err != nil {
		return false, err
	}

	n.run.values = append(
		n.run.values[:0],
		timestampD,
		tree.NewDInt(tree.DInt(nodeID)),
		tree.NewDInt(tree.DInt(storeID)),
		tree.NewDInt(tree.DInt(rangeID)),
	)
	return true, nil
}

func (n *showTraceReplicaNode) Values() tree.Datums {
	return n.run.values
}

func (n *showTraceReplicaNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
}

var showTraceReplicaColumns = sqlbase.ResultColumns{
	{
		Name: `timestamp`,
		Typ:  types.TimestampTZ,
	},
	{
		Name: `node_id`,
		Typ:  types.Int,
	},
	{
		Name: `store_id`,
		Typ:  types.Int,
	},
	{
		Name: `replica_id`,
		Typ:  types.Int,
	},
}
