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
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type relocateRange struct {
	optColumnsSlot

	rows            planNode
	subjectReplicas tree.RelocateSubject
	toStoreID       tree.TypedExpr
	fromStoreID     tree.TypedExpr
	run             relocateRunState
}

// relocateRunState contains the run-time state of
// relocateRange during local execution.
type relocateRunState struct {
	toReplicationTarget   roachpb.ReplicationTarget
	fromReplicationTarget roachpb.ReplicationTarget
	results               relocateResults
}

// relocateResults captures the results of the last relocate run
type relocateResults struct {
	rangeID   roachpb.RangeID
	rangeDesc *roachpb.RangeDescriptor
	err       error
}

// relocateRequest is an internal data structure that describes a relocation.
type relocateRequest struct {
	rangeID               roachpb.RangeID
	subjectReplicas       tree.RelocateSubject
	toReplicationTarget   roachpb.ReplicationTarget
	fromReplicationTarget roachpb.ReplicationTarget
}

func (n *relocateRange) startExec(params runParams) error {

	typedExprToStoreID := func(expr tree.TypedExpr, name string) (roachpb.StoreID, error) {
		storeIDInt, err := paramparse.DatumAsInt(params.ctx, params.EvalContext(), name, expr)
		if err != nil {
			return 0, err
		}
		storeID := roachpb.StoreID(storeIDInt)
		if storeID <= 0 {
			return 0, errors.Errorf("invalid target %s store ID %d for RELOCATE", name, int32(storeID))
		}
		return storeID, nil
	}

	storeID, err := typedExprToStoreID(n.toStoreID, "TO")
	if err != nil {
		return err
	}
	n.run.toReplicationTarget.StoreID = storeID
	storeDesc, err := params.ExecCfg().DescCache.GetStoreDescriptor(storeID)
	if err != nil {
		return err
	}
	n.run.toReplicationTarget.NodeID = storeDesc.Node.NodeID
	if n.subjectReplicas != tree.RelocateLease {
		storeID, err := typedExprToStoreID(n.fromStoreID, "FROM")
		if err != nil {
			return err
		}
		n.run.fromReplicationTarget.StoreID = storeID
		storeDesc, err := params.ExecCfg().DescCache.GetStoreDescriptor(storeID)
		if err != nil {
			return err
		}
		n.run.fromReplicationTarget.NodeID = storeDesc.Node.NodeID
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
		rangeID:               rangeID,
		subjectReplicas:       n.subjectReplicas,
		toReplicationTarget:   n.run.toReplicationTarget,
		fromReplicationTarget: n.run.fromReplicationTarget,
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
	execCfg := params.ExecCfg()
	rangeDesc, err := lookupRangeDescriptorByRangeID(params.ctx, execCfg.DB, req.rangeID)
	if err != nil {
		return nil, errors.Wrapf(err, "error looking up range descriptor")
	}

	if req.subjectReplicas == tree.RelocateLease {
		err := execCfg.DB.AdminTransferLease(params.ctx, rangeDesc.StartKey, req.toReplicationTarget.StoreID)
		return rangeDesc, err
	}

	addChangeType := roachpb.ADD_VOTER
	removeChangeType := roachpb.REMOVE_VOTER
	if req.subjectReplicas == tree.RelocateNonVoters {
		addChangeType = roachpb.ADD_NON_VOTER
		addChangeType = roachpb.REMOVE_NON_VOTER
	}
	_, err = execCfg.DB.AdminChangeReplicas(
		params.ctx, rangeDesc.StartKey, *rangeDesc, []roachpb.ReplicationChange{
			{ChangeType: addChangeType, Target: req.toReplicationTarget},
			{ChangeType: removeChangeType, Target: req.fromReplicationTarget},
		},
	)
	// TODO(aayush): If the `AdminChangeReplicas`call failed because it found that
	// the range was already in the process of being rebalanced, we currently fail
	// the statement. We should consider instead force-removing these learners
	// when `AdminChangeReplicas` calls are issued by SQL.
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
