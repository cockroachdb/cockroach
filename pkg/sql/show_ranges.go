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

// This file implements the SHOW TESTING_RANGES statement:
//   SHOW TESTING_RANGES FROM TABLE t
//   SHOW TESTING_RANGES FROM INDEX t@idx
//
// These statements show the ranges corresponding to the given table or index,
// along with the list of replicas and the lease holder.

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
	// interleaving.
	return &showRangesNode{
		p:      p,
		span:   tableDesc.IndexSpan(index.ID),
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

var showRangesColumns = sqlbase.ResultColumns{
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
		// The INTs in the array are Store IDs.
		Typ: parser.TypeIntArray,
	},
	{
		Name: "Lease Holder",
		// The store ID for the lease holder.
		Typ: parser.TypeInt,
	},
}

func (n *showRangesNode) Start(ctx context.Context) error {
	var err error
	n.descriptorKVs, err = scanMetaKVs(ctx, n.p.txn, n.span)
	return err
}

func (n *showRangesNode) Next(ctx context.Context) (bool, error) {
	if n.rowIdx >= len(n.descriptorKVs) {
		return false, nil
	}

	var desc roachpb.RangeDescriptor
	if err := n.descriptorKVs[n.rowIdx].ValueProto(&desc); err != nil {
		return false, err
	}
	for i := range n.values {
		n.values[i] = parser.DNull
	}

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

func (*showRangesNode) Columns() sqlbase.ResultColumns { return showRangesColumns }
func (*showRangesNode) Ordering() orderingInfo         { return orderingInfo{} }
func (*showRangesNode) MarkDebug(_ explainMode)        {}
func (*showRangesNode) DebugValues() debugValues       { panic("unimplemented") }
func (*showRangesNode) Spans(context.Context) (_, _ roachpb.Spans, _ error) {
	panic("unimplemented")
}

// scanMetaKVs returns the meta KVs for the ranges that touch the given span.
func scanMetaKVs(
	ctx context.Context, txn *client.Txn, span roachpb.Span,
) ([]client.KeyValue, error) {
	metaStart := keys.RangeMetaKey(keys.MustAddr(span.Key))
	metaEnd := keys.RangeMetaKey(keys.MustAddr(span.EndKey))

	kvs, err := txn.Scan(ctx, metaStart, metaEnd, 0)
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 || !kvs[len(kvs)-1].Key.Equal(metaEnd) {
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
