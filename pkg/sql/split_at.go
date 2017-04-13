// Copyright 2016 The Cockroach Authors.
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
// Author: Matt Jibson

package sql

import (
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// getRowKey generates a key that corresponds to a row (or prefix of a row) in a table or index.
// Both tableDesc and index are required (index can be the primary index).
func getRowKey(
	tableDesc *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor, values []parser.Datum,
) ([]byte, error) {
	colMap := make(map[sqlbase.ColumnID]int)
	for i := range values {
		colMap[index.ColumnIDs[i]] = i
	}
	prefix := sqlbase.MakeIndexKeyPrefix(tableDesc, index.ID)
	key, _, err := sqlbase.EncodePartialIndexKey(
		tableDesc, index, len(values), colMap, values, prefix,
	)
	if err != nil {
		return nil, err
	}
	return keys.MakeRowSentinelKey(key), nil
}

// Split executes a KV split.
// Privileges: INSERT on table.
func (p *planner) Split(ctx context.Context, n *parser.Split) (planNode, error) {
	tableDesc, index, err := p.getTableAndIndex(ctx, n.Table, n.Index, privilege.INSERT)
	if err != nil {
		return nil, err
	}
	// Calculate the desired types for the select statement. It is OK if the
	// select statement returns fewer columns (the relevant prefix is used).
	desiredTypes := make([]parser.Type, len(index.ColumnIDs))
	for i, colID := range index.ColumnIDs {
		c, err := tableDesc.FindColumnByID(colID)
		if err != nil {
			return nil, err
		}
		desiredTypes[i] = c.Type.ToDatumType()
	}

	// Create the plan for the split rows source.
	rows, err := p.newPlan(ctx, n.Rows, desiredTypes, false /* auto commit */)
	if err != nil {
		return nil, err
	}

	cols := rows.Columns()
	if len(cols) == 0 {
		return nil, errors.Errorf("no columns in SPLIT AT data")
	}
	if len(cols) > len(index.ColumnIDs) {
		return nil, errors.Errorf("too many columns in SPLIT AT data")
	}
	for i := range cols {
		if !cols[i].Typ.Equivalent(desiredTypes[i]) {
			return nil, errors.Errorf(
				"SPLIT AT data column %d (%s) must be of type %s, not type %s",
				i+1, index.ColumnNames[i], desiredTypes[i], cols[i].Typ,
			)
		}
	}

	return &splitNode{
		p:         p,
		tableDesc: tableDesc,
		index:     index,
		rows:      rows,
	}, nil
}

type splitNode struct {
	p            *planner
	tableDesc    *sqlbase.TableDescriptor
	index        *sqlbase.IndexDescriptor
	rows         planNode
	lastSplitKey []byte
}

func (n *splitNode) Start(ctx context.Context) error {
	return n.rows.Start(ctx)
}

func (n *splitNode) Next(ctx context.Context) (bool, error) {
	// TODO(radu): instead of performing the splits sequentially, accumulate all
	// the split keys and then perform the splits in parallel (e.g. split at the
	// middle key and recursively to the left and right).

	if ok, err := n.rows.Next(ctx); err != nil || !ok {
		return ok, err
	}

	rowKey, err := getRowKey(n.tableDesc, n.index, n.rows.Values())
	if err != nil {
		return false, err
	}

	if err := n.p.session.execCfg.DB.AdminSplit(ctx, rowKey); err != nil {
		return false, err
	}

	n.lastSplitKey = rowKey

	return true, nil
}

func (n *splitNode) Values() parser.Datums {
	return parser.Datums{
		parser.NewDBytes(parser.DBytes(n.lastSplitKey)),
		parser.NewDString(keys.PrettyPrint(n.lastSplitKey)),
	}
}

func (n *splitNode) Close(ctx context.Context) {
	n.rows.Close(ctx)
}

func (*splitNode) Columns() ResultColumns {
	return ResultColumns{
		{
			Name: "key",
			Typ:  parser.TypeBytes,
		},
		{
			Name: "pretty",
			Typ:  parser.TypeString,
		},
	}
}

func (*splitNode) Ordering() orderingInfo   { return orderingInfo{} }
func (*splitNode) MarkDebug(_ explainMode)  { panic("unimplemented") }
func (*splitNode) DebugValues() debugValues { panic("unimplemented") }
func (*splitNode) Spans(context.Context) (_, _ roachpb.Spans, _ error) {
	panic("unimplemented")
}

// Relocate moves ranges to specific stores
// (`ALTER TABLE/INDEX ... TESTING_RELOCATE ...` statement)
// Privileges: INSERT on table.
func (p *planner) Relocate(ctx context.Context, n *parser.Relocate) (planNode, error) {
	tableDesc, index, err := p.getTableAndIndex(ctx, n.Table, n.Index, privilege.INSERT)
	if err != nil {
		return nil, err
	}

	// Calculate the desired types for the select statement:
	//  - int array (list of stores)
	//  - column values; it is OK if the select statement returns fewer columns
	//  (the relevant prefix is used).
	desiredTypes := make([]parser.Type, len(index.ColumnIDs)+1)
	desiredTypes[0] = parser.TypeIntArray
	for i, colID := range index.ColumnIDs {
		c, err := tableDesc.FindColumnByID(colID)
		if err != nil {
			return nil, err
		}
		desiredTypes[i+1] = c.Type.ToDatumType()
	}

	// Create the plan for the split rows source.
	rows, err := p.newPlan(ctx, n.Rows, desiredTypes, false /* auto commit */)
	if err != nil {
		return nil, err
	}

	cols := rows.Columns()
	if len(cols) < 2 {
		return nil, errors.Errorf("less than two columns in TESTING_RELOCATE data")
	}
	if len(cols) > len(index.ColumnIDs)+1 {
		return nil, errors.Errorf("too many columns in TESTING_RELOCATE data")
	}
	for i := range cols {
		if !cols[i].Typ.Equivalent(desiredTypes[i]) {
			colName := "relocation array"
			if i > 0 {
				colName = index.ColumnNames[i-1]
			}
			return nil, errors.Errorf(
				"TESTING_RELOCATE data column %d (%s) must be of type %s, not type %s",
				i+1, colName, desiredTypes[i], cols[i].Typ,
			)
		}
	}

	return &relocateNode{
		p:         p,
		tableDesc: tableDesc,
		index:     index,
		rows:      rows,
		storeMap:  make(map[roachpb.StoreID]roachpb.NodeID),
	}, nil
}

type relocateNode struct {
	p                 *planner
	tableDesc         *sqlbase.TableDescriptor
	index             *sqlbase.IndexDescriptor
	rows              planNode
	lastRangeStartKey []byte

	// storeMap caches information about stores seen in relocation strings (to
	// avoid looking them up for every row).
	storeMap map[roachpb.StoreID]roachpb.NodeID
}

func (n *relocateNode) Start(ctx context.Context) error {
	return n.rows.Start(ctx)
}

func lookupRangeDescriptor(
	ctx context.Context, db *client.DB, rowKey []byte,
) (roachpb.RangeDescriptor, error) {
	startKey := keys.RangeMetaKey(keys.MustAddr(rowKey))
	endKey := keys.Meta2Prefix.PrefixEnd()
	kvs, err := db.Scan(ctx, startKey, endKey, 1)
	if err != nil {
		return roachpb.RangeDescriptor{}, err
	}
	if len(kvs) != 1 {
		log.Fatalf(ctx, "expected 1 KV, got %v", kvs)
	}
	var desc roachpb.RangeDescriptor
	if err := kvs[0].ValueProto(&desc); err != nil {
		return roachpb.RangeDescriptor{}, err
	}
	if desc.EndKey.Equal(rowKey) {
		log.Fatalf(ctx, "row key should not be valid range split point: %s", keys.PrettyPrint(rowKey))
	}
	return desc, nil
}

func (n *relocateNode) Next(ctx context.Context) (bool, error) {
	// Each Next call relocates one range (corresponding to one row from n.rows).
	// TODO(radu): perform multiple relocations in parallel.

	if ok, err := n.rows.Next(ctx); err != nil || !ok {
		return ok, err
	}

	// First column is the relocation string; the rest of the columns indicate the
	// table/index row.
	data := n.rows.Values()

	if !data[0].ResolvedType().Equivalent(parser.TypeIntArray) {
		return false, errors.Errorf(
			"expected int array in the first TESTING_RELOCATE data column; got %s",
			data[0].ResolvedType(),
		)
	}
	relocation := data[0].(*parser.DArray)
	if len(relocation.Array) == 0 {
		return false, errors.Errorf("empty relocation array for TESTING_RELOCATE")
	}

	// Create an array of the desired replication targets.
	targets := make([]roachpb.ReplicationTarget, len(relocation.Array))
	for i, d := range relocation.Array {
		storeID := roachpb.StoreID(*d.(*parser.DInt))
		nodeID, ok := n.storeMap[storeID]
		if !ok {
			// Lookup the store in gossip.
			var storeDesc roachpb.StoreDescriptor
			gossipStoreKey := gossip.MakeStoreKey(storeID)
			if err := n.p.session.execCfg.Gossip.GetInfoProto(gossipStoreKey, &storeDesc); err != nil {
				return false, errors.Wrapf(err, "error looking up store %d", storeID)
			}
			nodeID = storeDesc.Node.NodeID
			n.storeMap[storeID] = nodeID
		}
		targets[i] = roachpb.ReplicationTarget{NodeID: nodeID, StoreID: storeID}
	}

	// Find the current list of replicas. This is inherently racy, so the
	// implementation is best effort; in tests, the replication queues should be
	// stopped to make this reliable.
	rowKey, err := getRowKey(n.tableDesc, n.index, data[1:])
	if err != nil {
		return false, err
	}

	rangeDesc, err := lookupRangeDescriptor(ctx, n.p.session.execCfg.DB, rowKey)
	if err != nil {
		return false, errors.Wrap(err, "error looking up range descriptor")
	}
	n.lastRangeStartKey = rangeDesc.StartKey.AsRawKey()

	if err := storage.RelocateRange(ctx, n.p.ExecCfg().DB, rangeDesc, targets); err != nil {
		return false, err
	}

	return true, nil
}

func (n *relocateNode) Values() parser.Datums {
	return parser.Datums{
		parser.NewDBytes(parser.DBytes(n.lastRangeStartKey)),
		parser.NewDString(keys.PrettyPrint(n.lastRangeStartKey)),
	}
}

func (n *relocateNode) Close(ctx context.Context) {
	n.rows.Close(ctx)
}

func (*relocateNode) Columns() ResultColumns {
	return ResultColumns{
		{
			Name: "key",
			Typ:  parser.TypeBytes,
		},
		{
			Name: "pretty",
			Typ:  parser.TypeString,
		},
	}
}

func (*relocateNode) Ordering() orderingInfo   { return orderingInfo{} }
func (*relocateNode) MarkDebug(_ explainMode)  { panic("unimplemented") }
func (*relocateNode) DebugValues() debugValues { panic("unimplemented") }
func (*relocateNode) Spans(context.Context) (_, _ roachpb.Spans, _ error) {
	panic("unimplemented")
}

// Scatter moves ranges to random stores
// (`ALTER TABLE/INDEX ... SCATTER ...` statement)
// Privileges: INSERT on table.
func (p *planner) Scatter(ctx context.Context, n *parser.Scatter) (planNode, error) {
	tableDesc, index, err := p.getTableAndIndex(ctx, n.Table, n.Index, privilege.INSERT)
	if err != nil {
		return nil, err
	}

	var span roachpb.Span
	if n.From == nil {
		// No FROM/TO specified; the span is the entire table/index.
		span = tableDesc.IndexSpan(index.ID)
	} else {
		switch {
		case len(n.From) == 0:
			return nil, errors.Errorf("no columns in SCATTER FROM expression")
		case len(n.From) > len(index.ColumnIDs):
			return nil, errors.Errorf("too many columns in SCATTER FROM expression")
		case len(n.To) == 0:
			return nil, errors.Errorf("no columns in SCATTER TO expression")
		case len(n.To) > len(index.ColumnIDs):
			return nil, errors.Errorf("too many columns in SCATTER TO expression")
		}

		// Calculate the desired types for the select statement:
		//  - column values; it is OK if the select statement returns fewer columns
		//  (the relevant prefix is used).
		desiredTypes := make([]parser.Type, len(index.ColumnIDs))
		for i, colID := range index.ColumnIDs {
			c, err := tableDesc.FindColumnByID(colID)
			if err != nil {
				return nil, err
			}
			desiredTypes[i] = c.Type.ToDatumType()
		}
		fromVals := make([]parser.Datum, len(n.From))
		for i, expr := range n.From {
			typedExpr, err := p.analyzeExpr(
				ctx, expr, nil, parser.IndexedVarHelper{}, desiredTypes[i], true, "SCATTER",
			)
			if err != nil {
				return nil, err
			}
			fromVals[i], err = typedExpr.Eval(&p.evalCtx)
			if err != nil {
				return nil, err
			}
		}
		toVals := make([]parser.Datum, len(n.From))
		for i, expr := range n.To {
			typedExpr, err := p.analyzeExpr(
				ctx, expr, nil, parser.IndexedVarHelper{}, desiredTypes[i], true, "SCATTER",
			)
			if err != nil {
				return nil, err
			}
			toVals[i], err = typedExpr.Eval(&p.evalCtx)
			if err != nil {
				return nil, err
			}
		}

		span.Key, err = getRowKey(tableDesc, index, fromVals)
		if err != nil {
			return nil, err
		}
		span.EndKey, err = getRowKey(tableDesc, index, toVals)
		if err != nil {
			return nil, err
		}
		// Tolerate reversing FROM and TO; this can be useful for descending
		// indexes.
		if span.Key.Compare(span.EndKey) > 0 {
			span.Key, span.EndKey = span.EndKey, span.Key
		}
	}

	return &scatterNode{
		p:    p,
		span: span,
	}, nil
}

type scatterNode struct {
	p    *planner
	span roachpb.Span

	rangeIdx int
	ranges   []roachpb.AdminScatterResponse_Range
}

func (n *scatterNode) Start(ctx context.Context) error {
	db := n.p.ExecCfg().DB
	req := &roachpb.AdminScatterRequest{
		Span: roachpb.Span{Key: n.span.Key, EndKey: n.span.EndKey},
	}
	res, pErr := client.SendWrapped(ctx, db.GetSender(), req)
	if pErr != nil {
		return pErr.GoError()
	}
	n.rangeIdx = -1
	n.ranges = res.(*roachpb.AdminScatterResponse).Ranges
	return nil
}

func (n *scatterNode) Next(ctx context.Context) (bool, error) {
	n.rangeIdx++
	hasNext := n.rangeIdx < len(n.ranges)
	return hasNext, nil
}

func (*scatterNode) Columns() ResultColumns {
	return ResultColumns{
		{
			Name: "key",
			Typ:  parser.TypeBytes,
		},
		{
			Name: "pretty",
			Typ:  parser.TypeString,
		},
		{
			Name: "error",
			Typ:  parser.TypeString,
		},
	}
}

func (n *scatterNode) Values() parser.Datums {
	r := n.ranges[n.rangeIdx]
	dErr := parser.DNull
	if r.Error != nil {
		dErr = parser.NewDString(r.Error.String())
	}

	return parser.Datums{
		parser.NewDBytes(parser.DBytes(r.Span.Key)),
		parser.NewDString(keys.PrettyPrint(r.Span.Key)),
		dErr,
	}
}

func (*scatterNode) Close(ctx context.Context) {}
func (*scatterNode) Ordering() orderingInfo    { return orderingInfo{} }
func (*scatterNode) MarkDebug(_ explainMode)   { panic("unimplemented") }
func (*scatterNode) DebugValues() debugValues  { panic("unimplemented") }
func (*scatterNode) Spans(context.Context) (_, _ roachpb.Spans, _ error) {
	panic("unimplemented")
}
