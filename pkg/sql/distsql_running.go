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
// Author: Radu Berinde (radu@cockroachlabs.com)

package sql

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// To allow queries to send out flow RPCs in parallel, we use a pool of workers
// that can issue the RPCs on behalf of the running code. The pool is shared by
// multiple queries.
const numRunners = 16

// runnerRequest is the request that is sent (via a channel) to a worker.
type runnerRequest struct {
	ctx         context.Context
	rpcContext  *rpc.Context
	flowReq     *distsqlrun.SetupFlowRequest
	nodeID      roachpb.NodeID
	nodeAddress string
	resultChan  chan<- runnerResult
}

// runnerResult is returned by a worker (via a channel) for each received
// request.
type runnerResult struct {
	nodeID roachpb.NodeID
	err    error
}

func (req runnerRequest) run() {
	res := runnerResult{nodeID: req.nodeID}

	conn, err := req.rpcContext.GRPCDial(req.nodeAddress)
	if err != nil {
		res.err = err
	} else {
		client := distsqlrun.NewDistSQLClient(conn)
		// TODO(radu): do we want a timeout here?
		resp, err := client.SetupFlow(req.ctx, req.flowReq)
		if err != nil {
			res.err = err
		} else {
			res.err = resp.Error.ErrorDetail()
		}
	}
	req.resultChan <- res
}

func (dsp *distSQLPlanner) initRunners() {
	// This channel has to be unbuffered because we want to only be able to send
	// requests if a worker is actually there to receive them.
	dsp.runnerChan = make(chan runnerRequest)
	for i := 0; i < numRunners; i++ {
		dsp.stopper.RunWorker(context.TODO(), func(context.Context) {
			runnerChan := dsp.runnerChan
			stopChan := dsp.stopper.ShouldStop()
			for {
				select {
				case req := <-runnerChan:
					req.run()

				case <-stopChan:
					return
				}
			}
		})
	}
}

// Run executes a physical plan. The plan should have been finalized using
// FinalizePlan.
//
// Note that errors that happen while actually running the flow are reported to
// recv, not returned by this function.
func (dsp *distSQLPlanner) Run(
	planCtx *planningCtx, txn *client.Txn, plan *physicalPlan, recv *distSQLReceiver,
) error {
	ctx := planCtx.ctx

	flows := plan.GenerateFlowSpecs()

	if logPlanDiagram {
		log.VEvent(ctx, 1, "creating plan diagram")
		json, url, err := distsqlrun.GeneratePlanDiagramWithURL(flows)
		if err != nil {
			log.Infof(ctx, "Error generating diagram: %s", err)
		} else {
			log.Infof(ctx, "Plan diagram JSON:\n%s", json)
			log.Infof(ctx, "Plan diagram URL:\n%s", url.String())
		}
	}

	log.VEvent(ctx, 1, "running DistSQL plan")

	recv.resultToStreamColMap = plan.planToStreamColMap
	thisNodeID := dsp.nodeDesc.NodeID

	// Start all the flows except the flow on this node (there is always a flow on
	// this node).
	var resultChan chan runnerResult
	if len(flows) > 1 {
		resultChan = make(chan runnerResult, len(flows)-1)
	}
	for nodeID, flowSpec := range flows {
		if nodeID == thisNodeID {
			// Skip this node.
			continue
		}
		req := &distsqlrun.SetupFlowRequest{
			Version: distsqlrun.Version,
			Txn:     *txn.Proto(),
			Flow:    flowSpec,
		}
		if err := distsqlrun.SetFlowRequestTrace(ctx, req); err != nil {
			return err
		}
		runReq := runnerRequest{
			ctx:         ctx,
			rpcContext:  dsp.rpcContext,
			flowReq:     req,
			nodeID:      nodeID,
			nodeAddress: planCtx.nodeAddresses[nodeID],
			resultChan:  resultChan,
		}
		// Send out a request to the workers; if no worker is available, run
		// directly.
		select {
		case dsp.runnerChan <- runReq:
		default:
			runReq.run()
		}
	}

	var firstErr error
	// Now wait for all the flows to be scheduled on remote nodes. Note that we
	// are not waiting for the flows themselves to complete.
	for i := 0; i < len(flows)-1; i++ {
		res := <-resultChan
		if firstErr == nil {
			firstErr = res.err
		}
		// TODO(radu): accumulate the flows that we failed to set up and move them
		// into the local flow.
	}
	if firstErr != nil {
		return firstErr
	}

	// Set up the flow on this node.
	localReq := distsqlrun.SetupFlowRequest{
		Version: distsqlrun.Version,
		Txn:     *txn.Proto(),
		Flow:    flows[thisNodeID],
	}
	if err := distsqlrun.SetFlowRequestTrace(ctx, &localReq); err != nil {
		return err
	}
	ctx, flow, err := dsp.distSQLSrv.SetupSyncFlow(ctx, &localReq, recv)
	if err != nil {
		return err
	}
	// TODO(radu): this should go through the flow scheduler.
	flow.Start(ctx, func() {})
	flow.Wait()
	flow.Cleanup(ctx)

	return nil
}

// distSQLReceiver is a RowReceiver that stores incoming rows in a RowContainer.
// This is where the DistSQL execution meets the SQL Session - the RowContainer
// comes from a client Session.
//
// distSQLReceiver also update the RangeDescriptorCache and the LeaseholderCache
// in response to DistSQL metadata about misplanned ranges.
type distSQLReceiver struct {
	ctx context.Context

	// rows is the container where we store the results; if we only need the count
	// of the rows, it is nil.
	rows *RowContainer
	// resultToStreamColMap maps result columns to columns in the distsqlrun results
	// stream.
	resultToStreamColMap []int
	// numRows counts the number of rows we received when rows is nil.
	numRows int64

	// err represents the error that we received either from a producer or
	// internally in the operation of the distSQLReceiver. If set, this will
	// ultimately be returned as the error for the SQL query.
	//
	// Once set, no more rows are accepted.
	err error

	row    parser.Datums
	status distsqlrun.ConsumerStatus
	alloc  sqlbase.DatumAlloc
	closed bool

	rangeCache *kv.RangeDescriptorCache
	leaseCache *kv.LeaseHolderCache
}

var _ distsqlrun.RowReceiver = &distSQLReceiver{}

func makeDistSQLReceiver(
	ctx context.Context,
	sink *RowContainer,
	rangeCache *kv.RangeDescriptorCache,
	leaseCache *kv.LeaseHolderCache,
) distSQLReceiver {
	return distSQLReceiver{
		ctx:        ctx,
		rows:       sink,
		rangeCache: rangeCache,
		leaseCache: leaseCache,
	}
}

// Push is part of the RowReceiver interface.
func (r *distSQLReceiver) Push(
	row sqlbase.EncDatumRow, meta distsqlrun.ProducerMetadata,
) distsqlrun.ConsumerStatus {
	if !meta.Empty() {
		if meta.Err != nil && r.err == nil {
			r.err = meta.Err
		}
		if len(meta.Ranges) > 0 {
			r.err = r.updateCaches(r.ctx, meta.Ranges)
		}
		return r.status
	}
	if r.err != nil {
		// TODO(andrei): We should drain here.
		return distsqlrun.ConsumerClosed
	}
	if r.status != distsqlrun.NeedMoreRows {
		return r.status
	}

	if r.rows == nil {
		// We only need the row count.
		r.numRows++
		return r.status
	}
	if r.row == nil {
		r.row = make(parser.Datums, len(r.resultToStreamColMap))
	}
	for i, resIdx := range r.resultToStreamColMap {
		err := row[resIdx].EnsureDecoded(&r.alloc)
		if err != nil {
			r.err = err
			r.status = distsqlrun.ConsumerClosed
			return r.status
		}
		r.row[i] = row[resIdx].Datum
	}
	// Note that AddRow accounts for the memory used by the Datums.
	if _, err := r.rows.AddRow(r.ctx, r.row); err != nil {
		r.err = err
		// TODO(andrei): We should drain here. Metadata from this query would be
		// useful, particularly as it was likely a large query (since AddRow()
		// above failed, presumably with an out-of-memory error).
		r.status = distsqlrun.ConsumerClosed
		return r.status
	}
	return r.status
}

// ProducerDone is part of the RowReceiver interface.
func (r *distSQLReceiver) ProducerDone() {
	if r.closed {
		panic("double close")
	}
	r.closed = true
}

// updateCaches takes information about some ranges that were mis-planned and
// updates the range descriptor and lease-holder caches accordingly.
//
// TODO(andrei): updating these caches is not perfect: we can clobber newer
// information that someone else has populated because there's no timing info
// anywhere. We also may fail to remove stale info from the LeaseHolderCache if
// the ids of the ranges that we get are different than the ids in that cache.
func (r *distSQLReceiver) updateCaches(ctx context.Context, ranges []roachpb.RangeInfo) error {
	// Update the RangeDescriptorCache.
	rngDescs := make([]roachpb.RangeDescriptor, len(ranges))
	for i, ri := range ranges {
		rngDescs[i] = ri.Desc
	}
	if err := r.rangeCache.InsertRangeDescriptors(ctx, rngDescs...); err != nil {
		return err
	}

	// Update the LeaseHolderCache.
	for _, ri := range ranges {
		r.leaseCache.Update(ctx, ri.Desc.RangeID, ri.Lease.Replica)
	}
	return nil
}

// PlanAndRun generates a physical plan from a planNode tree and executes it. It
// assumes that the tree is supported (see CheckSupport).
//
// Note that errors that happen while actually running the flow are reported to
// recv, not returned by this function.
func (dsp *distSQLPlanner) PlanAndRun(
	ctx context.Context, txn *client.Txn, tree planNode, recv *distSQLReceiver,
) error {
	planCtx := dsp.NewPlanningCtx(ctx, txn)

	log.VEvent(ctx, 1, "creating DistSQL plan")

	plan, err := dsp.createPlanForNode(&planCtx, tree)
	if err != nil {
		return err
	}
	dsp.FinalizePlan(&planCtx, &plan)
	return dsp.Run(&planCtx, txn, &plan, recv)
}
