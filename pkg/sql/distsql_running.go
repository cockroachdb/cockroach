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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// To allow queries to send out flow RPCs in parallel, we use a pool of workers
// that can issue the RPCs on behalf of the running code.
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

// runerResult is returned by a worker (via a channel) for each received
// request.
type runnerResult struct {
	nodeID   roachpb.NodeID
	err      error
	response *distsqlrun.SimpleResponse
}

func (dsp *distSQLPlanner) initRunners() {
	dsp.runnerChan = make(chan runnerRequest, numRunners)
	for i := 0; i < numRunners; i++ {
		dsp.stopper.RunWorker(func() {
			runnerChan := dsp.runnerChan
			stopChan := dsp.stopper.ShouldStop()
			for {
				select {
				case req := <-runnerChan:
					res := runnerResult{nodeID: req.nodeID}

					conn, err := req.rpcContext.GRPCDial(req.nodeAddress)
					if err != nil {
						res.err = err
					} else {
						client := distsqlrun.NewDistSQLClient(conn)
						res.response, res.err = client.SetupFlow(req.ctx, req.flowReq)
					}
					req.resultChan <- res

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

	var resultChan chan runnerResult
	if len(flows) > 1 {
		resultChan = make(chan runnerResult, len(flows)-1)
	}
	// Start the flows on all other nodes.
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
		// Send out a request to the workers.
		dsp.runnerChan <- runnerRequest{
			ctx:         ctx,
			rpcContext:  dsp.rpcContext,
			flowReq:     req,
			nodeID:      nodeID,
			nodeAddress: planCtx.nodeAddresses[nodeID],
			resultChan:  resultChan,
		}
	}

	var err error
	// Now wait for all RPCs to complete. Note that we are not waiting for the
	// flows themselves to complete.
	for i := 0; i < len(flows)-1; i++ {
		select {
		case <-dsp.stopper.ShouldStop():
			// The workers may be gone by now; just error out.
			err = errors.Errorf("node is stopping")
		case res := <-resultChan:
			if err != nil {
				continue
			}
			if res.err != nil {
				err = res.err
			} else if res.response.Error != nil {
				err = res.response.Error.ErrorDetail()
			}
			// TODO(radu): accumulate the flows that we couldn't set up and move them
			// into the local flow.
		}
	}
	if err != nil {
		return err
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
	err     error
	row     parser.Datums
	status  distsqlrun.ConsumerStatus
	alloc   sqlbase.DatumAlloc
	closed  bool
}

var _ distsqlrun.RowReceiver = &distSQLReceiver{}

func makeDistSQLReceiver(ctx context.Context, sink *RowContainer) distSQLReceiver {
	return distSQLReceiver{ctx: ctx, rows: sink}
}

// Push is part of the RowReceiver interface.
func (r *distSQLReceiver) Push(
	row sqlbase.EncDatumRow, meta distsqlrun.ProducerMetadata,
) distsqlrun.ConsumerStatus {
	if !meta.Empty() {
		if meta.Err != nil && r.err == nil {
			r.err = meta.Err
		}
		// TODO(andrei): do something with the metadata - update the descriptor
		// caches.
		return r.status
	}
	if r.err != nil {
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
