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

package sql

import (
	"sync/atomic"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
// TODO(andrei): Some errors ocurring during the "starting" phase are also
// reported to recv instead of being returned (see the flow.Start() call for the
// local flow). Perhaps we should push all errors to recv and have this function
// not return anything.
func (dsp *distSQLPlanner) Run(
	planCtx *planningCtx,
	txn *client.Txn,
	plan *physicalPlan,
	recv *distSQLReceiver,
	evalCtx parser.EvalContext,
) error {
	ctx := planCtx.ctx

	if err := planCtx.sanityCheckAddresses(); err != nil {
		return err
	}

	flows := plan.GenerateFlowSpecs(dsp.nodeDesc.NodeID /* gateway */)

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

	dsp.distSQLSrv.ServerConfig.Metrics.QueryStart()
	defer dsp.distSQLSrv.ServerConfig.Metrics.QueryStop()

	recv.resultToStreamColMap = plan.planToStreamColMap
	thisNodeID := dsp.nodeDesc.NodeID

	evalCtxProto := distsqlrun.MakeEvalContext(evalCtx)
	for _, s := range evalCtx.SearchPath {
		evalCtxProto.SearchPath = append(evalCtxProto.SearchPath, s)
	}

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
			Version:     distsqlrun.Version,
			Txn:         *txn.Proto(),
			Flow:        flowSpec,
			EvalContext: evalCtxProto,
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
		Version:     distsqlrun.Version,
		Txn:         *txn.Proto(),
		Flow:        flows[thisNodeID],
		EvalContext: evalCtxProto,
	}
	ctx, flow, err := dsp.distSQLSrv.SetupSyncFlow(ctx, &localReq, recv)
	if err != nil {
		return err
	}
	// TODO(radu): this should go through the flow scheduler.
	if err := flow.Start(ctx, func() {}); err != nil {
		log.Fatalf(ctx, "unexpected error from syncFlow.Start(): %s "+
			"The error should have gone to the consumer.", err)
	}
	flow.Wait()
	flow.Cleanup(ctx)

	return nil
}

// distSQLReceiver is a RowReceiver that writes results to a rowResultWriter.
// This is where the DistSQL execution meets the SQL Session - the RowContainer
// comes from a client Session.
//
// distSQLReceiver also update the RangeDescriptorCache and the LeaseholderCache
// in response to DistSQL metadata about misplanned ranges.
type distSQLReceiver struct {
	ctx context.Context

	// resultWriter is the interface which we sent results to.
	resultWriter rowResultWriter
	// resultToStreamColMap maps result columns to columns in the distsqlrun results
	// stream.
	resultToStreamColMap []int

	// err represents the error that we received either from a producer or
	// internally in the operation of the distSQLReceiver. If set, this will
	// ultimately be returned as the error for the SQL query.
	//
	// Once set, no more rows are accepted.
	err error

	// cancelled is atomically set to 1 when this distSQL receiver has been marked
	// as cancelled. Upon the next Push(), err is set to a non-nil
	// value, and ConsumerClosed is the ConsumerStatus.
	cancelled int32

	row    parser.Datums
	status distsqlrun.ConsumerStatus
	alloc  sqlbase.DatumAlloc
	closed bool

	rangeCache *kv.RangeDescriptorCache
	leaseCache *kv.LeaseHolderCache

	// The transaction in which the flow producing data for this receiver runs.
	// The distSQLReceiver updates the TransactionProto in response to
	// RetryableTxnError's. Nil if no transaction should be updated on errors
	// (i.e. if the flow overall doesn't run in a transaction).
	txn *client.Txn

	// A handler for clock signals arriving from remote nodes. This should update
	// this node's clock.
	updateClock func(observedTs hlc.Timestamp)
}

// rowResultWriter is a subset of StatementResult to be used with the
// distSQLReceiver. It's implemented by RowResultWriter.
type rowResultWriter interface {
	// AddRow takes the passed in row and adds it to the current result.
	AddRow(ctx context.Context, row parser.Datums) error
	// IncrementRowsAffected increments a counter by n. This is used for all
	// result types other than parser.Rows.
	IncrementRowsAffected(n int)
	// GetStatementType returns the StatementType that corresponds to the type of
	// results that should be sent to this interface.
	StatementType() parser.StatementType
}

var _ distsqlrun.RowReceiver = &distSQLReceiver{}
var _ distsqlrun.CancellableRowReceiver = &distSQLReceiver{}

// makeDistSQLReceiver creates a distSQLReceiver.
//
// ctx is the Context that the receiver will use throughput its lifetime.
// sink is the container where the results will be stored. If only the row count
// is needed, this can be nil.
//
// txn is the transaction in which the producer flow runs; it will be updated
// on errors. Nil if the flow overall doesn't run in a transaction.
func makeDistSQLReceiver(
	ctx context.Context,
	resultWriter rowResultWriter,
	rangeCache *kv.RangeDescriptorCache,
	leaseCache *kv.LeaseHolderCache,
	txn *client.Txn,
	updateClock func(observedTs hlc.Timestamp),
) (distSQLReceiver, error) {
	return distSQLReceiver{
		ctx:          ctx,
		resultWriter: resultWriter,
		rangeCache:   rangeCache,
		leaseCache:   leaseCache,
		txn:          txn,
		updateClock:  updateClock,
	}, nil
}

// Push is part of the RowReceiver interface.
func (r *distSQLReceiver) Push(
	row sqlbase.EncDatumRow, meta distsqlrun.ProducerMetadata,
) distsqlrun.ConsumerStatus {
	if !meta.Empty() {
		if meta.Err != nil && r.err == nil {
			if r.txn != nil {
				if retryErr, ok := meta.Err.(*roachpb.UnhandledRetryableError); ok {
					// Update the txn in response to remote errors. In the non-DistSQL
					// world, the TxnCoordSender does this, and the client.Txn updates
					// itself in non-error cases. Those updates are not necessary if we're
					// just doing reads. Once DistSQL starts performing writes, we'll need
					// to perform such updates too.
					r.txn.UpdateStateOnRemoteRetryableErr(r.ctx, retryErr.PErr)
					// Update the clock with information from the error. On non-DistSQL
					// code paths, the DistSender does this.
					// TODO(andrei): We don't propagate clock signals on success cases
					// through DistSQL; we should. We also don't propagate them through
					// non-retryable errors; we also should.
					r.updateClock(retryErr.PErr.Now)
					meta.Err = roachpb.NewHandledRetryableTxnError(
						meta.Err.Error(), r.txn.Proto().ID, *r.txn.Proto())
				}
			}
			r.err = meta.Err
		}
		if len(meta.Ranges) > 0 {
			if err := r.updateCaches(r.ctx, meta.Ranges); err != nil && r.err == nil {
				r.err = err
			}
		}
		if len(meta.TraceData) > 0 {
			span := opentracing.SpanFromContext(r.ctx)
			if span == nil {
				r.err = errors.New("trying to ingest remote spans but there is no recording span set up")
			} else if err := tracing.ImportRemoteSpans(span, meta.TraceData); err != nil {
				r.err = errors.Errorf("error ingesting remote spans: %s", err)
			}
		}
		return r.status
	}
	if r.err == nil && atomic.LoadInt32(&r.cancelled) == 1 {
		// Set the error to reflect query cancellation.
		r.err = sqlbase.NewQueryCanceledError()
	}
	if r.err != nil {
		// TODO(andrei): We should drain here.
		return distsqlrun.ConsumerClosed
	}
	if r.status != distsqlrun.NeedMoreRows {
		return r.status
	}

	if r.resultWriter.StatementType() != parser.Rows {
		// We only need the row count.
		r.resultWriter.IncrementRowsAffected(1)
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
	if err := r.resultWriter.AddRow(r.ctx, r.row); err != nil {
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

// SetCancelled is part of the CancellableRowReceiver interface.
func (r *distSQLReceiver) SetCancelled() {
	atomic.StoreInt32(&r.cancelled, 1)
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
	ctx context.Context,
	txn *client.Txn,
	tree planNode,
	recv *distSQLReceiver,
	evalCtx parser.EvalContext,
) error {
	planCtx := dsp.NewPlanningCtx(ctx, txn)

	log.VEvent(ctx, 1, "creating DistSQL plan")

	plan, err := dsp.createPlanForNode(&planCtx, tree)
	if err != nil {
		return err
	}
	dsp.FinalizePlan(&planCtx, &plan)
	return dsp.Run(&planCtx, txn, &plan, recv, evalCtx)
}
