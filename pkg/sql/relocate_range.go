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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type relocateRange struct {
	optColumnsSlot

	relocateLease     bool
	relocateNonVoters bool
	rangeID           roachpb.RangeID
	toStoreID         roachpb.StoreID
	fromStoreID       roachpb.StoreID
}

func (n *relocateRange) startExec(params runParams) error {
	if n.toStoreID <= 0 {
		return errors.Errorf("invalid target to store ID %d for RELOCATE", n.toStoreID)
	}
	if n.rangeID <= 0 {
		return errors.Errorf("invalid range for ID %d for RELOCATE", n.rangeID)
	}
	if !n.relocateLease {
		if n.fromStoreID <= 0 {
			return errors.Errorf("invalid target from store ID %d for RELOCATE", n.fromStoreID)
		}
	}

	toStoreDesc, err := lookupStoreDesc(n.toStoreID, params)
	if err != nil {
		return err
	}
	rangeDesc, err := lookupRangeDescriptorByRangeID(params.ctx, params.extendedEvalCtx.ExecCfg.DB, n.rangeID)
	if err != nil {
		return errors.Wrapf(err, "error looking up range descriptor")
	}

	if n.relocateLease {
		if err := params.p.ExecCfg().DB.AdminTransferLease(params.ctx, rangeDesc.StartKey, n.toStoreID); err != nil {
			return err
		}
	} else {
		fromStoreDesc, err := lookupStoreDesc(n.fromStoreID, params)
		if err != nil {
			return err
		}
		toTarget := roachpb.ReplicationTarget{NodeID: toStoreDesc.Node.NodeID, StoreID: toStoreDesc.StoreID}
		fromTarget := roachpb.ReplicationTarget{NodeID: fromStoreDesc.Node.NodeID, StoreID: fromStoreDesc.StoreID}
		if n.relocateNonVoters {
			if _, err := params.p.ExecCfg().DB.AdminChangeReplicas(
				params.ctx, rangeDesc.StartKey, rangeDesc, []roachpb.ReplicationChange{
					{ChangeType: roachpb.ADD_NON_VOTER, Target: toTarget},
					{ChangeType: roachpb.REMOVE_NON_VOTER, Target: fromTarget},
				},
			); err != nil {
				return err
			}
		} else {
			if _, err := params.p.ExecCfg().DB.AdminChangeReplicas(
				params.ctx, rangeDesc.StartKey, rangeDesc, []roachpb.ReplicationChange{
					{ChangeType: roachpb.ADD_VOTER, Target: toTarget},
					{ChangeType: roachpb.REMOVE_VOTER, Target: fromTarget},
				},
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *relocateRange) Next(runParams) (bool, error) { return false, nil }
func (n *relocateRange) Values() tree.Datums          { return tree.Datums{} }
func (n *relocateRange) Close(context.Context)        {}

func lookupRangeDescriptorByRangeID(
	ctx context.Context, db *kv.DB, rangeID roachpb.RangeID,
) (roachpb.RangeDescriptor, error) {
	var descriptor roachpb.RangeDescriptor
	sentinelErr := errors.Errorf("sentinel")
	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return txn.Iterate(ctx, keys.MetaMin, keys.MetaMax, 100,
			func(rows []kv.KeyValue) error {
				var desc roachpb.RangeDescriptor
				for _, row := range rows {
					err := row.ValueProto(&desc)
					if err != nil {
						return errors.Wrapf(err, "unable to unmarshal range descriptor from %s", row.Key)
					}
					// In small enough clusters it's possible for the same range
					// descriptor to be stored in both meta1 and meta2. This
					// happens when some range spans both the meta and the user
					// keyspace. Consider when r1 is [/Min,
					// /System/NodeLiveness); we'll store the range descriptor
					// in both /Meta2/<r1.EndKey> and in /Meta1/KeyMax[1].
					//
					// [1]: See kvserver.rangeAddressing.
					if desc.RangeID == rangeID {
						descriptor = desc
						return sentinelErr
					}
				}
				return nil
			})
	})
	if errors.Is(err, sentinelErr) {
		return descriptor, nil
	}
	if err != nil {
		return roachpb.RangeDescriptor{}, err
	}
	return roachpb.RangeDescriptor{}, errors.Errorf("Descriptor for range %d is not found", rangeID)
}
