// Copyright 2015 The Cockroach Authors.
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

// This file implements the SHOW TESTING_RANGES statement:
//   SHOW TESTING_RANGES FROM TABLE t
//   SHOW TESTING_RANGES FROM INDEX t@idx
//
// These statements show the ranges corresponding to the given table or index,
// along with the list of replicas and the lease holder.

package sql

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

type showRangesNode struct {
	optColumnsSlot

	span roachpb.Span

	run showRangesRun
}

func (p *planner) ShowRanges(ctx context.Context, n *tree.ShowRanges) (planNode, error) {
	tableDesc, index, err := p.getTableAndIndex(ctx, n.Table, n.Index, privilege.SELECT)

	if err != nil {
		return nil, err
	}
	// Note: for interleaved tables, the ranges we report will include rows from
	// interleaving.
	return &showRangesNode{
		span: tableDesc.IndexSpan(index.ID),
		run: showRangesRun{
			values: make([]tree.Datum, len(showRangesColumns)),
		},
	}, nil
}

var showRangesColumns = sqlbase.ResultColumns{
	{
		Name: "Start Key",
		Typ:  types.String,
	},
	{
		Name: "End Key",
		Typ:  types.String,
	},
	{
		Name: "Range ID",
		Typ:  types.Int,
	},
	{
		Name: "Replicas",
		// The INTs in the array are Store IDs.
		Typ: types.TArray{Typ: types.Int},
	},
	{
		Name: "Lease Holder",
		// The store ID for the lease holder.
		Typ: types.Int,
	},
}

// showRangesRun contains the run-time state for showRangesNode during
// local execution.
type showRangesRun struct {
	// descriptorKVs are KeyValues returned from scanning the
	// relevant meta keys.
	descriptorKVs []client.KeyValue

	rowIdx int
	// values stores the current row, updated by Next().
	values []tree.Datum
}

func (n *showRangesNode) startExec(params runParams) error {
	var err error
	n.run.descriptorKVs, err = scanMetaKVs(params.ctx, params.p.txn, n.span)
	return err
}

func (n *showRangesNode) Next(params runParams) (bool, error) {
	if n.run.rowIdx >= len(n.run.descriptorKVs) {
		return false, nil
	}

	var desc roachpb.RangeDescriptor
	if err := n.run.descriptorKVs[n.run.rowIdx].ValueProto(&desc); err != nil {
		return false, err
	}
	for i := range n.run.values {
		n.run.values[i] = tree.DNull
	}

	// We do not attempt to identify the encoding directions for pretty
	// printing a split key since it's possible for a key from an arbitrary
	// table in the same interleaved hierarchy to appear in SHOW
	// TESTING_RANGE. Consider the interleaved hierarchy
	//    parent1		      (pid1)
	//	  child1	      (pid1, cid1)
	//	      grandchild1     (pid1, cid1, gcid1)
	//	  child2	      (pid1, cid2, cid3)
	//	      grandchild2     (pid1, cid2, cid3, gcid2)
	// and the result of
	//    SHOW TESTING_RANGES FROM TABLE grandchild1
	// It is possible for a split key for grandchild2 to show up.
	// Traversing up the InterleaveDescriptor is futile since we do not
	// know the SharedPrefixLen in between each interleaved sentinel of a
	// grandchild2 key.
	// We thus default the directions (such that '#' pretty prints for
	// interleaved sentinels).
	// TODO(richardwu): The one edge case we cannot deal with effectively
	// are split keys belonging to secondary indexes with descending
	// column(s).
	if n.run.rowIdx > 0 {
		n.run.values[0] = tree.NewDString(sqlbase.PrettyKey(nil /* valDirs */, desc.StartKey.AsRawKey(), 2))
	}

	if n.run.rowIdx < len(n.run.descriptorKVs)-1 {
		n.run.values[1] = tree.NewDString(sqlbase.PrettyKey(nil /* valDirs */, desc.EndKey.AsRawKey(), 2))
	}

	n.run.values[2] = tree.NewDInt(tree.DInt(desc.RangeID))

	var replicas []int
	for _, rd := range desc.Replicas {
		replicas = append(replicas, int(rd.StoreID))
	}
	sort.Ints(replicas)

	replicaArr := tree.NewDArray(types.Int)
	replicaArr.Array = make(tree.Datums, len(replicas))
	for i, r := range replicas {
		replicaArr.Array[i] = tree.NewDInt(tree.DInt(r))
	}
	n.run.values[3] = replicaArr

	// Get the lease holder.
	// TODO(radu): this will be slow if we have a lot of ranges; find a way to
	// make this part optional.
	b := &client.Batch{}
	b.AddRawRequest(&roachpb.LeaseInfoRequest{
		Span: roachpb.Span{
			Key: desc.StartKey.AsRawKey(),
		},
	})
	if err := params.p.txn.Run(params.ctx, b); err != nil {
		return false, errors.Wrap(err, "error getting lease info")
	}
	resp := b.RawResponse().Responses[0].GetInner().(*roachpb.LeaseInfoResponse)
	n.run.values[4] = tree.NewDInt(tree.DInt(resp.Lease.Replica.StoreID))

	n.run.rowIdx++
	return true, nil
}

func (n *showRangesNode) Values() tree.Datums {
	return n.run.values
}

func (n *showRangesNode) Close(_ context.Context) {
	n.run.descriptorKVs = nil
}

// scanMetaKVs returns the meta KVs for the ranges that touch the given span.
func scanMetaKVs(
	ctx context.Context, txn *client.Txn, span roachpb.Span,
) ([]client.KeyValue, error) {
	metaStart := keys.RangeMetaKey(keys.MustAddr(span.Key).Next())
	metaEnd := keys.RangeMetaKey(keys.MustAddr(span.EndKey))

	kvs, err := txn.Scan(ctx, metaStart, metaEnd, 0)
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 || !kvs[len(kvs)-1].Key.Equal(metaEnd.AsRawKey()) {
		// Normally we need to scan one more KV because the ranges are addressed by
		// the end key.
		extraKV, err := txn.Scan(ctx, metaEnd, keys.Meta2Prefix.PrefixEnd(), 1 /* one result */)
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, extraKV[0])
	}
	return kvs, nil
}
