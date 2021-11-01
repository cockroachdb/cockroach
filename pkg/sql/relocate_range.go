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

	rows              planNode
	relocateLease     bool
	relocateNonVoters bool
	toStoreID         roachpb.StoreID
	fromStoreID       roachpb.StoreID
	run               relocateRunState
}

// relocateRunState contains the run-time state of
// relocateRange during local execution.
type relocateRunState struct {
	toStoreDesc   *roachpb.StoreDescriptor
	fromStoreDesc *roachpb.StoreDescriptor
	results       relocateResults
}

// relocateResults captures the results of the last relocate run
type relocateResults struct {
	rangeID   roachpb.RangeID
	rangeDesc *roachpb.RangeDescriptor
	err       error
}

// relocateRequest is an internal data structure that describes a relocation.
type relocateRequest struct {
	rangeID           roachpb.RangeID
	relocateLease     bool
	relocateNonVoters bool
	toStoreDesc       *roachpb.StoreDescriptor
	fromStoreDesc     *roachpb.StoreDescriptor
}

func (n *relocateRange) startExec(params runParams) error {
	if n.toStoreID <= 0 {
		return errors.Errorf("invalid target to store ID %d for RELOCATE", n.toStoreID)
	}
	if !n.relocateLease && n.fromStoreID <= 0 {
		return errors.Errorf("invalid target from store ID %d for RELOCATE", n.fromStoreID)
	}
	// Lookup all the store descriptors upfront, so we dont have to do it for each
	// range we are working with.
	var err error
	n.run.toStoreDesc, err = lookupStoreDesc(n.toStoreID, params)
	if err != nil {
		return err
	}
	if !n.relocateLease {
		n.run.fromStoreDesc, err = lookupStoreDesc(n.fromStoreID, params)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *relocateRange) Next(params runParams) (bool, error) {
	if ok, err := n.rows.Next(params); err != nil || !ok {
		return ok, err
	}
	datum := n.rows.Values()[0]
	if datum == tree.DNull {
		return true, nil
	}
	rangeID := roachpb.RangeID(tree.MustBeDInt(datum))

	rangeDesc, err := relocate(params, relocateRequest{
		rangeID:           rangeID,
		relocateLease:     n.relocateLease,
		relocateNonVoters: n.relocateNonVoters,
		fromStoreDesc:     n.run.fromStoreDesc,
		toStoreDesc:       n.run.toStoreDesc,
	})

	// record the results of the relocation run, so we can output it.
	n.run.results = relocateResults{
		rangeID:   rangeID,
		rangeDesc: rangeDesc,
		err:       err,
	}
	return true, nil
}

func (n *relocateRange) Values() tree.Datums {
	result := "ok"
	if n.run.results.err != nil {
		result = n.run.results.err.Error()
	}
	pretty := ""
	if n.run.results.rangeDesc != nil {
		pretty = keys.PrettyPrint(nil /* valDirs */, n.run.results.rangeDesc.StartKey.AsRawKey())
	}
	return tree.Datums{
		tree.NewDInt(tree.DInt(n.run.results.rangeID)),
		tree.NewDString(pretty),
		tree.NewDString(result),
	}
}

func (n *relocateRange) Close(ctx context.Context) {
	n.rows.Close(ctx)
}

func relocate(params runParams, req relocateRequest) (*roachpb.RangeDescriptor, error) {
	rangeDesc, err := lookupRangeDescriptorByRangeID(params.ctx, params.extendedEvalCtx.ExecCfg.DB, req.rangeID)
	if err != nil {
		return nil, errors.Wrapf(err, "error looking up range descriptor")
	}

	if req.relocateLease {
		err := params.p.ExecCfg().DB.AdminTransferLease(params.ctx, rangeDesc.StartKey, req.toStoreDesc.StoreID)
		return rangeDesc, err
	}

	toTarget := roachpb.ReplicationTarget{NodeID: req.toStoreDesc.Node.NodeID, StoreID: req.toStoreDesc.StoreID}
	fromTarget := roachpb.ReplicationTarget{NodeID: req.fromStoreDesc.Node.NodeID, StoreID: req.fromStoreDesc.StoreID}
	if req.relocateNonVoters {
		_, err := params.p.ExecCfg().DB.AdminChangeReplicas(
			params.ctx, rangeDesc.StartKey, *rangeDesc, []roachpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: toTarget},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: fromTarget},
			},
		)
		return rangeDesc, err
	}
	_, err = params.p.ExecCfg().DB.AdminChangeReplicas(
		params.ctx, rangeDesc.StartKey, *rangeDesc, []roachpb.ReplicationChange{
			{ChangeType: roachpb.ADD_VOTER, Target: toTarget},
			{ChangeType: roachpb.REMOVE_VOTER, Target: fromTarget},
		},
	)
	return rangeDesc, err
}

func lookupRangeDescriptorByRangeID(
	ctx context.Context, db *kv.DB, rangeID roachpb.RangeID,
) (*roachpb.RangeDescriptor, error) {
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
					// For the purposes of this code, we just return the first range
					// descriptor we find.
					if desc.RangeID == rangeID {
						descriptor = desc
						return sentinelErr
					}
				}
				return nil
			})
	})
	if errors.Is(err, sentinelErr) {
		return &descriptor, nil
	}
	if err != nil {
		return nil, err
	}
	return nil, errors.Errorf("Descriptor for range %d is not found", rangeID)
}
