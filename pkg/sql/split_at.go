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
	"math/rand"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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

	if err := relocateRange(ctx, n.p.ExecCfg().DB, rangeDesc, targets); err != nil {
		return false, err
	}

	return true, nil
}

// relocateRange relocates a given range to a given set of stores. The first
// store in the slice becomes the new leaseholder.
//
// This is best-effort; if replication queues are enabled and a change in
// membership happens at the same time, there will be errors.
func relocateRange(
	ctx context.Context,
	db *client.DB,
	rangeDesc roachpb.RangeDescriptor,
	targets []roachpb.ReplicationTarget,
) error {
	// Step 1: Add any stores that don't already have a replica in of the range.
	//
	// TODO(radu): we can't have multiple replicas on different stores on the same
	// node, which can lead to some odd corner cases where we would have to first
	// remove some replicas (currently these cases fail).

	var addTargets []roachpb.ReplicationTarget
	for _, t := range targets {
		found := false
		for _, replicaDesc := range rangeDesc.Replicas {
			if replicaDesc.StoreID == t.StoreID && replicaDesc.NodeID == t.NodeID {
				found = true
				break
			}
		}
		if !found {
			addTargets = append(addTargets, t)
		}
	}
	if len(addTargets) > 0 {
		if err := db.AdminChangeReplicas(
			ctx, rangeDesc.StartKey.AsRawKey(), roachpb.ADD_REPLICA, addTargets,
		); err != nil {
			return err
		}
	}

	// Step 2: Transfer the lease to the first target. This needs to happen before
	// we remove replicas or we may try to remove the lease holder.

	if err := db.AdminTransferLease(
		ctx, rangeDesc.StartKey.AsRawKey(), targets[0].StoreID,
	); err != nil {
		return err
	}

	// Step 3: Remove any replicas that are not targets.

	var removeTargets []roachpb.ReplicationTarget
	for _, replicaDesc := range rangeDesc.Replicas {
		found := false
		for _, t := range targets {
			if replicaDesc.StoreID == t.StoreID && replicaDesc.NodeID == t.NodeID {
				found = true
				break
			}
		}
		if !found {
			removeTargets = append(removeTargets, roachpb.ReplicationTarget{
				StoreID: replicaDesc.StoreID,
				NodeID:  replicaDesc.NodeID,
			})
		}
	}
	if len(removeTargets) > 0 {
		if err := db.AdminChangeReplicas(
			ctx, rangeDesc.StartKey.AsRawKey(), roachpb.REMOVE_REPLICA, removeTargets,
		); err != nil {
			return err
		}
	}
	return nil
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
		span.Key = roachpb.Key(sqlbase.MakeIndexKeyPrefix(tableDesc, index.ID))
		span.EndKey = span.Key.PrefixEnd()
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

	stores := p.ExecCfg().ClusterStoresUpcall()
	if log.V(1) {
		log.Infof(ctx, "Stores: %v", stores)
	}
	if len(stores) == 0 {
		return nil, errors.Errorf("no known stores")
	}

	return &scatterNode{
		p:      p,
		span:   span,
		stores: stores,
		rng:    rand.New(rand.NewSource(rand.Int63())),
	}, nil
}

type scatterNode struct {
	p             *planner
	span          roachpb.Span
	descriptorKVs []client.KeyValue
	stores        []roachpb.ReplicationTarget
	rng           *rand.Rand

	rowIdx            int
	lastRangeStartKey []byte
	lastRelocateErr   error
}

func (n *scatterNode) Start(ctx context.Context) error {
	var err error
	n.descriptorKVs, err = scanMetaKVs(ctx, n.p.txn, n.span)
	return err
}

func (n *scatterNode) Next(ctx context.Context) (bool, error) {
	if n.rowIdx >= len(n.descriptorKVs) {
		return false, nil
	}

	var desc roachpb.RangeDescriptor
	if err := n.descriptorKVs[n.rowIdx].ValueProto(&desc); err != nil {
		return false, err
	}

	// Choose three random stores.
	// TODO(radu): this is a toy implementation; we need to get a real
	// recommendation based on the zone config.
	num := 3
	if num > len(n.stores) {
		num = len(n.stores)
	}
	for i := 0; i < num; i++ {
		j := i + n.rng.Intn(len(n.stores)-i)
		n.stores[i], n.stores[j] = n.stores[j], n.stores[i]
	}

	targets := n.stores[:num]

	n.lastRelocateErr = relocateRange(ctx, n.p.ExecCfg().DB, desc, targets)
	n.lastRangeStartKey = desc.StartKey
	n.rowIdx++
	return true, nil
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
			Name: "status",
			Typ:  parser.TypeString,
		},
	}
}

func (n *scatterNode) Values() parser.Datums {
	status := "OK"
	if n.lastRelocateErr != nil {
		status = n.lastRelocateErr.Error()
	}
	return parser.Datums{
		parser.NewDBytes(parser.DBytes(n.lastRangeStartKey)),
		parser.NewDString(keys.PrettyPrint(n.lastRangeStartKey)),
		parser.NewDString(status),
	}
}

func (*scatterNode) Close(ctx context.Context) {}
func (*scatterNode) Ordering() orderingInfo    { return orderingInfo{} }
func (*scatterNode) MarkDebug(_ explainMode)   { panic("unimplemented") }
func (*scatterNode) DebugValues() debugValues  { panic("unimplemented") }
func (*scatterNode) Spans(context.Context) (_, _ roachpb.Spans, _ error) {
	panic("unimplemented")
}
