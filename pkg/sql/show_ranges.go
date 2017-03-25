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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package sql

import (
	"sort"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

func (p *planner) ShowRanges(ctx context.Context, n *parser.ShowRanges) (planNode, error) {
	tableDesc, index, err := p.getTableAndIndex(ctx, n.Table, n.Index, privilege.SELECT)
	if err != nil {
		return nil, err
	}
	// Note: for interleaved tables, the ranges we report will include rows from
	// interleaving, but that's ok.
	keyPrefix := roachpb.Key(sqlbase.MakeIndexKeyPrefix(tableDesc, index.ID))
	return &showRangesNode{
		p: p,
		span: roachpb.Span{
			Key:    keyPrefix,
			EndKey: keyPrefix.PrefixEnd(),
		},
		values: make([]parser.Datum, len(showRangesColumns)),
	}, nil
}

type showRangesNode struct {
	p    *planner
	span roachpb.Span

	// descriptorKVs are KeyValues returned from scanning the
	// relevant meta keys.
	descriptorKVs []client.KeyValue

	rowIdx int
	// values stores the current row, updated by Next().
	values []parser.Datum
}

var showRangesColumns = ResultColumns{
	{
		Name: "Start Key",
		Typ:  parser.TypeString,
	},
	{
		Name: "End Key",
		Typ:  parser.TypeString,
	},
	{
		Name: "Replicas",
		Typ:  parser.TypeIntArray,
	},
	{
		Name: "Lease Holder",
		Typ:  parser.TypeInt,
	},
}

func (n *showRangesNode) Start(ctx context.Context) error {

	metaStart := keys.RangeMetaKey(keys.MustAddr(n.span.Key))
	metaEnd := keys.RangeMetaKey(keys.MustAddr(n.span.EndKey))

	kvs, err := n.p.txn.Scan(ctx, metaStart, metaEnd, 0)
	if err != nil {
		return err
	}
	if len(kvs) == 0 || !kvs[len(kvs)-1].Key.Equal(metaEnd) {
		// Normally we need to scan one more KV because the ranges are addressed by
		// the end key.
		extraKV, err := n.p.txn.Scan(ctx, metaEnd, keys.Meta2Prefix.PrefixEnd(), 1)
		if err != nil {
			return err
		}
		kvs = append(kvs, extraKV[0])
	}
	n.descriptorKVs = kvs
	return nil
}

func (n *showRangesNode) Next(ctx context.Context) (bool, error) {
	if n.rowIdx >= len(n.descriptorKVs) {
		return false, nil
	}

	var desc roachpb.RangeDescriptor
	if err := n.descriptorKVs[n.rowIdx].ValueProto(&desc); err != nil {
		return false, err
	}
	n.values[0] = parser.DNull
	n.values[1] = parser.DNull

	if n.rowIdx > 0 {
		n.values[0] = parser.NewDString(sqlbase.PrettyKey(desc.StartKey.AsRawKey(), 2))
	}

	if n.rowIdx < len(n.descriptorKVs)-1 {
		n.values[1] = parser.NewDString(sqlbase.PrettyKey(desc.EndKey.AsRawKey(), 2))
	}

	var replicas []int
	for _, rd := range desc.Replicas {
		replicas = append(replicas, int(rd.StoreID))
	}
	sort.Ints(replicas)

	replicaArr := parser.NewDArray(parser.TypeInt)
	replicaArr.Array = make(parser.Datums, len(replicas))
	for i, r := range replicas {
		replicaArr.Array[i] = parser.NewDInt(parser.DInt(r))
	}
	n.values[2] = replicaArr

	// Get the lease holder.
	// TODO(radu): this will be slow if we have a lot of ranges; find a way to
	// make this part optional.
	n.values[3] = parser.DNull
	b := &client.Batch{}
	b.AddRawRequest(&roachpb.LeaseInfoRequest{
		Span: roachpb.Span{
			Key: desc.StartKey.AsRawKey(),
		},
	})
	if err := n.p.txn.Run(ctx, b); err != nil {
		return false, errors.Wrap(err, "error getting lease info")
	}
	resp := b.RawResponse().Responses[0].GetInner().(*roachpb.LeaseInfoResponse)
	if resp.Lease != nil {
		n.values[3] = parser.NewDInt(parser.DInt(resp.Lease.Replica.StoreID))
	}

	n.rowIdx++
	return true, nil
}

func (n *showRangesNode) Values() parser.Datums {
	return n.values
}

func (n *showRangesNode) Close(_ context.Context) {
	n.descriptorKVs = nil
}

func (*showRangesNode) Columns() ResultColumns  { return showRangesColumns }
func (*showRangesNode) Ordering() orderingInfo  { return orderingInfo{} }
func (*showRangesNode) MarkDebug(_ explainMode) {}

func (n *showRangesNode) DebugValues() debugValues {
	return debugValues{
		rowIdx: 0,
		key:    "",
		value:  parser.DNull.String(),
		output: debugValueRow,
	}
}
