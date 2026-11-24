// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type relocateRange struct {
	singleInputPlanNode
	optColumnsSlot

	subjectReplicas tree.RelocateSubject
	toStoreID       tree.TypedExpr
	fromStoreID     tree.TypedExpr
	run             relocateRunState
}

// relocateRunState contains the run-time state of
// relocateRange during local execution.
type relocateRunState struct {
	rangeDescMap map[roachpb.RangeID]roachpb.RangeDescriptor
	toTarget     roachpb.ReplicationTarget
	fromTarget   roachpb.ReplicationTarget
	results      relocateResults
}

// relocateResults captures the results of the last relocate run
type relocateResults struct {
	rangeID   roachpb.RangeID
	rangeDesc roachpb.RangeDescriptor
	err       error
}

func (n *relocateRange) startExec(params runParams) (err error) {
	execCfg := params.p.ExecCfg()
	rangeDescIterator, err := execCfg.RangeDescIteratorFactory.NewIterator(params.ctx, execCfg.Codec.TenantSpan())
	if err != nil {
		return err
	}
	rangeDescMap := make(map[roachpb.RangeID]roachpb.RangeDescriptor)
	for rangeDescIterator.Valid() {
		rangeDesc := rangeDescIterator.CurRangeDescriptor()
		rangeDescMap[rangeDesc.RangeID] = rangeDesc
		rangeDescIterator.Next()
	}
	n.run.rangeDescMap = rangeDescMap

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
	if ok, err := n.input.Next(params); err != nil || !ok {
		return ok, err
	}
	datum := n.input.Values()[0]
	if datum == tree.DNull {
		return true, nil
	}

	rangeID := roachpb.RangeID(tree.MustBeDInt(datum))
	results := &n.run.results
	results.rangeID = rangeID
	if rangeDesc, found := n.run.rangeDescMap[rangeID]; found {
		results.rangeDesc = rangeDesc
		results.err = n.relocate(params, rangeDesc)
	} else {
		results.err = errors.Errorf("Descriptor for range %d is not found", rangeID)
	}
	return true, nil
}

func (n *relocateRange) Values() tree.Datums {
	result := "ok"
	if n.run.results.err != nil {
		result = n.run.results.err.Error()
	}
	pretty := ""
	if n.run.results.rangeDesc.IsInitialized() {
		pretty = keys.PrettyPrint(nil /* valDirs */, n.run.results.rangeDesc.StartKey.AsRawKey())
	}
	return tree.Datums{
		tree.NewDInt(tree.DInt(n.run.results.rangeID)),
		tree.NewDString(pretty),
		tree.NewDString(result),
	}
}

func (n *relocateRange) Close(ctx context.Context) {
	n.input.Close(ctx)
}

func (n *relocateRange) relocate(params runParams, rangeDesc roachpb.RangeDescriptor) error {
	execCfg := params.ExecCfg()

	if n.subjectReplicas == tree.RelocateLease {
		err := execCfg.DB.AdminTransferLease(params.ctx, rangeDesc.StartKey, n.run.toTarget.StoreID)
		return err
	}

	toChangeType := roachpb.ADD_VOTER
	fromChangeType := roachpb.REMOVE_VOTER
	if n.subjectReplicas == tree.RelocateNonVoters {
		toChangeType = roachpb.ADD_NON_VOTER
		fromChangeType = roachpb.REMOVE_NON_VOTER
	}
	_, err := execCfg.DB.AdminChangeReplicas(
		params.ctx, rangeDesc.StartKey, rangeDesc, []kvpb.ReplicationChange{
			{ChangeType: toChangeType, Target: n.run.toTarget},
			{ChangeType: fromChangeType, Target: n.run.fromTarget},
		},
	)
	// TODO(aayush): If the `AdminChangeReplicas` call failed because it found that
	//  the range was already in the process of being rebalanced, we currently fail
	//  the statement. We should consider instead force-removing these learners
	//  when `AdminChangeReplicas` calls are issued by SQL.
	return err
}
