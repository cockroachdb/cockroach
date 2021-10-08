// Copyright 2017 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type relocateNode struct {
	optColumnsSlot

	relocateLease     bool
	relocateNonVoters bool
	tableDesc         catalog.TableDescriptor
	index             catalog.Index
	rows              planNode

	run relocateRun
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
	n.run.storeMap = make(map[roachpb.StoreID]roachpb.NodeID)
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
		if !n.relocateNonVoters && len(relocation.Array) == 0 {
			// We cannot remove all voters.
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
				g, err := params.extendedEvalCtx.ExecCfg.Gossip.OptionalErr(54250)
				if err != nil {
					return false, err
				}
				if err := g.GetInfoProto(
					gossipStoreKey, &storeDesc,
				); err != nil {
					return false, errors.Wrapf(err, "error looking up store %d", storeID)
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
	rowKey, err := getRowKey(params.ExecCfg().Codec, n.tableDesc, n.index, data[1:])
	if err != nil {
		return false, err
	}
	rowKey = keys.MakeFamilyKey(rowKey, 0)

	rangeDesc, err := lookupRangeDescriptor(params.ctx, params.extendedEvalCtx.ExecCfg.DB, rowKey)
	if err != nil {
		return false, errors.Wrapf(err, "error looking up range descriptor")
	}
	n.run.lastRangeStartKey = rangeDesc.StartKey.AsRawKey()

	existingVoters := rangeDesc.Replicas().Voters().ReplicationTargets()
	existingNonVoters := rangeDesc.Replicas().NonVoters().ReplicationTargets()
	if n.relocateLease {
		if err := params.p.ExecCfg().DB.AdminTransferLease(params.ctx, rowKey, leaseStoreID); err != nil {
			return false, err
		}
	} else if n.relocateNonVoters {
		if err := params.p.ExecCfg().DB.AdminRelocateRange(
			params.ctx, rowKey, existingVoters, relocationTargets,
		); err != nil {
			return false, err
		}
	} else {
		if err := params.p.ExecCfg().DB.AdminRelocateRange(
			params.ctx, rowKey, relocationTargets, existingNonVoters,
		); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (n *relocateNode) Values() tree.Datums {
	return tree.Datums{
		tree.NewDBytes(tree.DBytes(n.run.lastRangeStartKey)),
		tree.NewDString(keys.PrettyPrint(catalogkeys.IndexKeyValDirs(n.index), n.run.lastRangeStartKey)),
	}
}

func (n *relocateNode) Close(ctx context.Context) {
	n.rows.Close(ctx)
}

func lookupRangeDescriptor(
	ctx context.Context, db *kv.DB, rowKey []byte,
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
