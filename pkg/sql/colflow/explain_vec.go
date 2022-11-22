// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colflow

import (
	"context"
	"reflect"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/cockroachdb/errors"
)

// convertToVecTree converts the flow to a tree of vectorized operators
// returning a list of the leap operators or an error if the flow vectorization
// is not supported. Note that it does so by setting up the full flow without
// running the components asynchronously, so it is pretty expensive. It also
// returns a non-nil cleanup function that closes all the closers as well as
// releases all execreleasable.Releasable objects which can *only* be performed
// once opChains are no longer needed.
func convertToVecTree(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	flow *execinfrapb.FlowSpec,
	localProcessors []execinfra.LocalProcessor,
	isPlanLocal bool,
) (opChains execopnode.OpChains, cleanup func(), err error) {
	if !isPlanLocal && len(localProcessors) > 0 {
		return nil, func() {}, errors.AssertionFailedf("unexpectedly non-empty LocalProcessors when plan is not local")
	}
	fuseOpt := flowinfra.FuseNormally
	if isPlanLocal && !execinfra.HasParallelProcessors(flow) {
		fuseOpt = flowinfra.FuseAggressively
	}
	// We optimistically assume that sql.DistSQLReceiver can be used as an
	// execinfra.BatchReceiver, so we always pass in a fakeBatchReceiver to the
	// creator.
	creator := newVectorizedFlowCreator(
		newNoopFlowCreatorHelper(), vectorizedRemoteComponentCreator{}, false, false,
		nil, &execinfra.RowChannel{}, &fakeBatchReceiver{}, flowCtx.Cfg.PodNodeDialer, execinfrapb.FlowID{}, colcontainer.DiskQueueCfg{},
		flowCtx.Cfg.VecFDSemaphore, flowCtx.NewTypeResolver(flowCtx.Txn),
		admission.WorkInfo{},
	)
	opChains, _, err = creator.setupFlow(ctx, flowCtx, flow.Processors, localProcessors, fuseOpt)
	cleanup = func() {
		creator.cleanup(ctx)
		creator.Release()
	}
	return opChains, cleanup, err
}

// fakeBatchReceiver exists for the sole purpose of convertToVecTree method. In
// the production code it would have been sql.DistSQLReceiver.
type fakeBatchReceiver struct{}

var _ execinfra.BatchReceiver = &fakeBatchReceiver{}

func (f fakeBatchReceiver) ProducerDone() {}

func (f fakeBatchReceiver) PushBatch(
	coldata.Batch, *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	return execinfra.ConsumerClosed
}

type flowWithNode struct {
	sqlInstanceID base.SQLInstanceID
	flow          *execinfrapb.FlowSpec
}

// ExplainVec converts the flows (that are assumed to be vectorizable) into the
// corresponding string representation.
//
// It also supports printing of already constructed operator chains which takes
// priority if non-nil (flows are ignored). All operators in opChains are
// assumed to be planned on the gateway.
func ExplainVec(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	flows map[base.SQLInstanceID]*execinfrapb.FlowSpec,
	localProcessors []execinfra.LocalProcessor,
	opChains execopnode.OpChains,
	gatewaySQLInstanceID base.SQLInstanceID,
	verbose bool,
	distributed bool,
) ([]string, error) {
	tp := treeprinter.NewWithStyle(treeprinter.CompactStyle)
	root := tp.Child("│")
	var err error
	var conversionErr error
	// It is possible that when iterating over execopnode.OpNodes we will hit a
	// panic (an input that doesn't implement OpNode interface), so we're
	// catching such errors.
	if err = colexecerror.CatchVectorizedRuntimeError(func() {
		if opChains != nil {
			formatChains(root, gatewaySQLInstanceID, opChains, verbose)
		} else {
			sortedFlows := make([]flowWithNode, 0, len(flows))
			for nodeID, flow := range flows {
				sortedFlows = append(sortedFlows, flowWithNode{sqlInstanceID: nodeID, flow: flow})
			}
			// Sort backward, since the first thing you add to a treeprinter will come
			// last.
			sort.Slice(sortedFlows, func(i, j int) bool { return sortedFlows[i].sqlInstanceID < sortedFlows[j].sqlInstanceID })
			for _, flow := range sortedFlows {
				var cleanup func()
				opChains, cleanup, err = convertToVecTree(ctx, flowCtx, flow.flow, localProcessors, !distributed)
				// We need to delay the cleanup until after the tree has been
				// formatted.
				defer cleanup()
				if err != nil {
					conversionErr = err
					return
				}
				formatChains(root, flow.sqlInstanceID, opChains, verbose)
			}
		}
	}); err != nil {
		return nil, err
	}
	if conversionErr != nil {
		return nil, conversionErr
	}
	return tp.FormattedRows(), nil
}

func formatChains(
	root treeprinter.Node,
	sqlInstanceID base.SQLInstanceID,
	opChains execopnode.OpChains,
	verbose bool,
) {
	node := root.Childf("Node %d", sqlInstanceID)
	for _, op := range opChains {
		formatOpChain(op, node, verbose)
	}
}

func shouldOutput(operator execopnode.OpNode, verbose bool) bool {
	_, nonExplainable := operator.(colexecop.NonExplainable)
	return !nonExplainable || verbose
}

func formatOpChain(operator execopnode.OpNode, node treeprinter.Node, verbose bool) {
	seenOps := make(map[reflect.Value]struct{})
	if shouldOutput(operator, verbose) {
		doFormatOpChain(operator, node.Child(reflect.TypeOf(operator).String()), verbose, seenOps)
	} else {
		doFormatOpChain(operator, node, verbose, seenOps)
	}
}
func doFormatOpChain(
	operator execopnode.OpNode,
	node treeprinter.Node,
	verbose bool,
	seenOps map[reflect.Value]struct{},
) {
	for i := 0; i < operator.ChildCount(verbose); i++ {
		child := operator.Child(i, verbose)
		childOpValue := reflect.ValueOf(child)
		childOpName := reflect.TypeOf(child).String()
		if _, seenOp := seenOps[childOpValue]; seenOp {
			// We have already seen this operator, so in order to not repeat the full
			// chain again, we will simply print out this operator's name and will
			// not recurse into its children. Note that we print out the name
			// unequivocally.
			node.Child(childOpName)
			continue
		}
		seenOps[childOpValue] = struct{}{}
		if shouldOutput(child, verbose) {
			doFormatOpChain(child, node.Child(childOpName), verbose, seenOps)
		} else {
			doFormatOpChain(child, node, verbose, seenOps)
		}
	}
}
