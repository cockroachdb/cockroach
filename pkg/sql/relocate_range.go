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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/oppurpose"
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
	toTarget   roachpb.ReplicationTarget
	fromTarget roachpb.ReplicationTarget
	results    relocateResults
}

// relocateResults captures the results of the last relocate run
type relocateResults struct {
	rangeID   roachpb.RangeID
	rangeDesc *roachpb.RangeDescriptor
	err       error
}

func (n *relocateRange) startExec(params runParams) (err error) {

	typedExprToReplicationTarget := func(typedExpr tree.TypedExpr, name string) (target roachpb.ReplicationTarget, _ error) {
		storeID, err := paramparse.DatumAsInt(params.ctx, params.EvalContext(), name, typedExpr)
		if err != nil {
			return target, err
		}
		if storeID <= 0 {
			return target, errors.Errorf("invalid target %s store ID %d for RELOCATE", name, typedExpr)
		}
		// Lookup all the store descriptors upfront, so we don't have to do it for each
		// range we are working with.
		storeDesc, err := params.ExecCfg().NodeDescs.GetStoreDescriptor(roachpb.StoreID(storeID))
		if err != nil {
			return target, err
		}
		target.NodeID = storeDesc.Node.NodeID
		target.StoreID = storeDesc.StoreID
		return target, nil
	}

	n.run.toTarget, err = typedExprToReplicationTarget(n.toStoreID, "TO")
	if err != nil {
		return err
	}
	if n.subjectReplicas != tree.RelocateLease {
		n.run.fromTarget, err = typedExprToReplicationTarget(n.fromStoreID, "FROM")
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
	rangeDesc, err := n.relocate(params, rangeID)

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

func (n *relocateRange) relocate(
	params runParams, rangeID roachpb.RangeID,
) (*roachpb.RangeDescriptor, error) {
	execCfg := params.ExecCfg()
	rangeDescIterator, err := execCfg.RangeDescIteratorFactory.NewIterator(params.ctx, execCfg.Codec.TenantSpan())
	if err != nil {
		return nil, err
	}
	found := false
	var rangeDesc roachpb.RangeDescriptor
	for rangeDescIterator.Valid() {
		rangeDesc = rangeDescIterator.CurRangeDescriptor()
		if rangeDesc.RangeID == rangeID {
			found = true
			break
		}
		rangeDescIterator.Next()
	}
	if !found {
		return nil, errors.Errorf("Descriptor for range %d is not found", rangeID)
	}

	if n.subjectReplicas == tree.RelocateLease {
		err := execCfg.DB.AdminTransferLease(params.ctx, rangeDesc.StartKey, n.run.toTarget.StoreID, oppurpose.TransferLeaseManual)
		return &rangeDesc, err
	}

	toChangeType := roachpb.ADD_VOTER
	fromChangeType := roachpb.REMOVE_VOTER
	if n.subjectReplicas == tree.RelocateNonVoters {
		toChangeType = roachpb.ADD_NON_VOTER
		fromChangeType = roachpb.REMOVE_NON_VOTER
	}
	_, err = execCfg.DB.AdminChangeReplicas(
		params.ctx, rangeDesc.StartKey, rangeDesc, []roachpb.ReplicationChange{
			{ChangeType: toChangeType, Target: n.run.toTarget},
			{ChangeType: fromChangeType, Target: n.run.fromTarget},
		},
	)
	// TODO(aayush): If the `AdminChangeReplicas` call failed because it found that
	//  the range was already in the process of being rebalanced, we currently fail
	//  the statement. We should consider instead force-removing these learners
	//  when `AdminChangeReplicas` calls are issued by SQL.
	return &rangeDesc, err
}
