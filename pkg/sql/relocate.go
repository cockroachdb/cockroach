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

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

type relocateNode struct {
	optColumnsSlot

	relocateLease bool
	tableDesc     *sqlbase.TableDescriptor
	index         *sqlbase.IndexDescriptor
	rows          planNode

	run relocateRun
}

// Relocate moves ranges and/or leases to specific stores.
// (`ALTER TABLE/INDEX ... EXPERIMENTAL_RELOCATE [LEASE] ...` statement)
// Privileges: INSERT on table.
func (p *planner) Relocate(ctx context.Context, n *tree.Relocate) (planNode, error) {
	tableDesc, index, err := p.getTableAndIndex(ctx, n.Table, n.Index, privilege.INSERT)
	if err != nil {
		return nil, err
	}

	// Calculate the desired types for the select statement:
	//  - int array (list of stores) if relocating a range, or just int (target
	//    storeID) if relocating a lease
	//  - column values; it is OK if the select statement returns fewer columns
	//    (the relevant prefix is used).
	desiredTypes := make([]*types.T, len(index.ColumnIDs)+1)
	if n.RelocateLease {
		desiredTypes[0] = types.Int
	} else {
		desiredTypes[0] = types.IntArray
	}
	for i, colID := range index.ColumnIDs {
		c, err := tableDesc.FindColumnByID(colID)
		if err != nil {
			return nil, err
		}
		desiredTypes[i+1] = &c.Type
	}

	// Create the plan for the split rows source.
	rows, err := p.newPlan(ctx, n.Rows, desiredTypes)
	if err != nil {
		return nil, err
	}

	cmdName := "EXPERIMENTAL_RELOCATE"
	if n.RelocateLease {
		cmdName += " LEASE"
	}
	cols := planColumns(rows)
	if len(cols) < 2 {
		return nil, errors.Errorf("less than two columns in %s data", cmdName)
	}
	if len(cols) > len(index.ColumnIDs)+1 {
		return nil, errors.Errorf("too many columns in %s data", cmdName)
	}
	for i := range cols {
		if !cols[i].Typ.Equivalent(desiredTypes[i]) {
			colName := "relocation array"
			if n.RelocateLease {
				colName = "target leaseholder"
			}
			if i > 0 {
				colName = index.ColumnNames[i-1]
			}
			return nil, errors.Errorf(
				"%s data column %d (%s) must be of type %s, not type %s",
				cmdName, i+1, colName, desiredTypes[i], cols[i].Typ,
			)
		}
	}

	return &relocateNode{
		relocateLease: n.RelocateLease,
		tableDesc:     tableDesc.TableDesc(),
		index:         index,
		rows:          rows,
		run: relocateRun{
			storeMap: make(map[roachpb.StoreID]roachpb.NodeID),
		},
	}, nil
}

var relocateNodeColumns = sqlbase.ResultColumns{
	{
		Name: "key",
		Typ:  types.Bytes,
	},
	{
		Name: "pretty",
		Typ:  types.String,
	},
}

// relocateRun contains the run-time state of
// relocateNode during local execution.
type relocateRun struct {
	lastRangeStartKey []byte

	// storeMap caches information about stores seen in relocation strings (to
	// avoid looking them up for every row).
	storeMap map[roachpb.StoreID]roachpb.NodeID
}

func (n *relocateNode) startExec(runParams) error {
	return nil
}

func (n *relocateNode) Next(params runParams) (bool, error) {
	// Each Next call relocates one range (corresponding to one row from n.rows).
	// TODO(radu): perform multiple relocations in parallel.

	if ok, err := n.rows.Next(params); err != nil || !ok {
		return ok, err
	}

	// First column is the relocation string or target leaseholder; the rest of
	// the columns indicate the table/index row.
	data := n.rows.Values()

	var relocationTargets []roachpb.ReplicationTarget
	var leaseStoreID roachpb.StoreID
	if n.relocateLease {
		leaseStoreID = roachpb.StoreID(tree.MustBeDInt(data[0]))
		if leaseStoreID <= 0 {
			return false, errors.Errorf("invalid target leaseholder store ID %d for EXPERIMENTAL_RELOCATE LEASE", leaseStoreID)
		}
	} else {
		if !data[0].ResolvedType().Equivalent(types.IntArray) {
			return false, errors.Errorf(
				"expected int array in the first EXPERIMENTAL_RELOCATE data column; got %s",
				data[0].ResolvedType(),
			)
		}
		relocation := data[0].(*tree.DArray)
		if len(relocation.Array) == 0 {
			return false, errors.Errorf("empty relocation array for EXPERIMENTAL_RELOCATE")
		}

		// Create an array of the desired replication targets.
		relocationTargets = make([]roachpb.ReplicationTarget, len(relocation.Array))
		for i, d := range relocation.Array {
			storeID := roachpb.StoreID(*d.(*tree.DInt))
			nodeID, ok := n.run.storeMap[storeID]
			if !ok {
				// Lookup the store in gossip.
				var storeDesc roachpb.StoreDescriptor
				gossipStoreKey := gossip.MakeStoreKey(storeID)
				if err := params.extendedEvalCtx.ExecCfg.Gossip.GetInfoProto(
					gossipStoreKey, &storeDesc,
				); err != nil {
					return false, pgerror.NewAssertionErrorWithWrappedErrf(err,
						"error looking up store %d", log.Safe(storeID))
				}
				nodeID = storeDesc.Node.NodeID
				n.run.storeMap[storeID] = nodeID
			}
			relocationTargets[i] = roachpb.ReplicationTarget{NodeID: nodeID, StoreID: storeID}
		}
	}

	// Find the current list of replicas. This is inherently racy, so the
	// implementation is best effort; in tests, the replication queues should be
	// stopped to make this reliable.
	// TODO(a-robinson): Get the lastRangeStartKey via the ReturnRangeInfo option
	// on the BatchRequest Header. We can't do this until v2.2 because admin
	// requests don't respect the option on versions earlier than v2.1.
	rowKey, err := getRowKey(n.tableDesc, n.index, data[1:])
	if err != nil {
		return false, err
	}
	rowKey = keys.MakeFamilyKey(rowKey, 0)

	rangeDesc, err := lookupRangeDescriptor(params.ctx, params.extendedEvalCtx.ExecCfg.DB, rowKey)
	if err != nil {
		return false, pgerror.Wrapf(err, pgerror.CodeDataExceptionError,
			"error looking up range descriptor")
	}
	n.run.lastRangeStartKey = rangeDesc.StartKey.AsRawKey()

	if n.relocateLease {
		if err := params.p.ExecCfg().DB.AdminTransferLease(params.ctx, rowKey, leaseStoreID); err != nil {
			return false, err
		}
	} else {
		if err := params.p.ExecCfg().DB.AdminRelocateRange(params.ctx, rowKey, relocationTargets); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (n *relocateNode) Values() tree.Datums {
	return tree.Datums{
		tree.NewDBytes(tree.DBytes(n.run.lastRangeStartKey)),
		tree.NewDString(keys.PrettyPrint(sqlbase.IndexKeyValDirs(n.index), n.run.lastRangeStartKey)),
	}
}

func (n *relocateNode) Close(ctx context.Context) {
	n.rows.Close(ctx)
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
		log.Fatalf(ctx, "row key should not be valid range split point: %s", keys.PrettyPrint(nil /* valDirs */, rowKey))
	}
	return desc, nil
}
