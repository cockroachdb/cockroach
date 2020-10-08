// Copyright 2018 The Cockroach Authors.
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
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// showTraceReplicaNode is a planNode that wraps another node and uses session
// tracing (via SHOW TRACE) to report the replicas of all kv events that occur
// during its execution. It is used as the top-level node for SHOW
// EXPERIMENTAL_REPLICA TRACE FOR statements.
//
// TODO(dan): This works by selecting trace lines matching certain event
// logs in command execution, which is possibly brittle. A much better
// system would be to set the `ReturnRangeInfo` flag on all kv requests and
// use the `RangeInfo`s that come back. Unfortunately, we wanted to get
// _some_ version of this into 2.0 for partitioning users, but the RangeInfo
// plumbing would have sunk the project. It's also possible that the
// sovereignty work may require the RangeInfo plumbing and we should revisit
// this then.
type showTraceReplicaNode struct {
	optColumnsSlot

	// plan is the wrapped execution plan that will be traced.
	plan planNode

	run struct {
		values tree.Datums
	}
}

func (n *showTraceReplicaNode) startExec(params runParams) error {
	return nil
}

func (n *showTraceReplicaNode) Next(params runParams) (bool, error) {
	var timestampD tree.Datum
	var tag string
	for {
		ok, err := n.plan.Next(params)
		if !ok || err != nil {
			return ok, err
		}
		values := n.plan.Values()
		// The rows are received from showTraceNode; see ShowTraceColumns.
		const (
			tsCol  = 0
			msgCol = 2
			tagCol = 3
		)
		if replicaMsgRE.MatchString(string(*values[msgCol].(*tree.DString))) {
			timestampD = values[tsCol]
			tag = string(*values[tagCol].(*tree.DString))
			break
		}
	}

	matches := nodeStoreRangeRE.FindStringSubmatch(tag)
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

var nodeStoreRangeRE = regexp.MustCompile(`^\[n(\d+),s(\d+),r(\d+)/`)

var replicaMsgRE = regexp.MustCompile(
	strings.Join([]string{
		"^read-write path$",
		"^read-only path$",
		"^admin path$",
	}, "|"),
)
