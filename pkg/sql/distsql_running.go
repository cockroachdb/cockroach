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
	"context"
	"sync/atomic"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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

	conn, err := req.rpcContext.GRPCDial(req.nodeAddress).Connect(req.ctx)
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

func (dsp *DistSQLPlanner) initRunners() {
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
// txn is the transaction in which the plan will run. If nil, the different
// processors are expected to manage their own internal transactions.
//
// All errors encoutered are reported to the distSQLReceiver's resultWriter.
// Additionally, if the error is a "communication error" (an error encoutered
// while using that resultWriter), the error is also stored in
// distSQLReceiver.commErr. That can be tested to see if a client session needs
// to be closed.
func (dsp *DistSQLPlanner) Run(
	planCtx *planningCtx,
	txn *client.Txn,
	plan *physicalPlan,
	recv *distSQLReceiver,
	evalCtx *extendedEvalContext,
) {
	ctx := planCtx.ctx

	var txnProto *roachpb.Transaction
	if txn != nil {
		txnProto = txn.Proto()
	}

	if err := planCtx.sanityCheckAddresses(); err != nil {
		recv.SetError(err)
		return
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

	recv.outputTypes = plan.ResultTypes
	recv.resultToStreamColMap = plan.planToStreamColMap
	thisNodeID := dsp.nodeDesc.NodeID

	evalCtxProto := distsqlrun.MakeEvalContext(evalCtx.EvalContext)

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
			Txn:         txnProto,
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
		recv.SetError(firstErr)
		return
	}

	// Set up the flow on this node.
	localReq := distsqlrun.SetupFlowRequest{
		Version:     distsqlrun.Version,
		Txn:         txnProto,
		Flow:        flows[thisNodeID],
		EvalContext: evalCtxProto,
	}
	ctx, flow, err := dsp.distSQLSrv.SetupSyncFlow(ctx, &localReq, recv)
	if err != nil {
		recv.SetError(err)
		return
	}
	// TODO(radu): this should go through the flow scheduler.
	if err := flow.Start(ctx, func() {}); err != nil {
		log.Fatalf(ctx, "unexpected error from syncFlow.Start(): %s "+
			"The error should have gone to the consumer.", err)
	}
	flow.Wait()
	flow.Cleanup(ctx)
}

// distSQLReceiver is a RowReceiver that writes results to a rowResultWriter.
// This is where the DistSQL execution meets the SQL Session - the RowContainer
// comes from a client Session.
//
// distSQLReceiver also update the RangeDescriptorCache and the LeaseholderCache
// in response to DistSQL metadata about misplanned ranges.
type distSQLReceiver struct {
	ctx context.Context

	// resultWriter is the interface which we send results to.
	resultWriter rowResultWriter

	stmtType tree.StatementType

	// outputTypes are the types of the result columns produced by the plan.
	outputTypes []sqlbase.ColumnType

	// resultToStreamColMap maps result columns to columns in the distsqlrun results
	// stream.
	resultToStreamColMap []int

	// commErr keeps track of the error received from interacting with the
	// resultWriter. This represents a "communication error" and as such is unlike
	// query execution errors: when the distSQLReceiver is used within a SQL
	// session, such errors mean that we have to bail on the session.
	// Query execution errors are reported to the resultWriter. For some client's
	// convenience, communication errors are also reported to the resultWriter.
	//
	// Once set, no more rows are accepted.
	commErr error

	// txnAbortedErr is atomically set to an errWrap when the KV txn finishes
	// asynchronously. Further results should not be returned to the client, as
	// they risk missing seeing their own writes. Upon the next Push(), err is set
	// and ConsumerStatus is set to ConsumerClosed.
	txnAbortedErr atomic.Value

	row    tree.Datums
	status distsqlrun.ConsumerStatus
	alloc  sqlbase.DatumAlloc
	closed bool

	rangeCache *kv.RangeDescriptorCache
	leaseCache *kv.LeaseHolderCache

	// The transaction in which the flow producing data for this
	// receiver runs. The distSQLReceiver updates the transaction in
	// response to RetryableTxnError's and when distributed processors
	// pass back TxnCoordMeta objects via ProducerMetas. Nil if no
	// transaction should be updated on errors (i.e. if the flow overall
	// doesn't run in a transaction).
	txn *client.Txn

	// A handler for clock signals arriving from remote nodes. This should update
	// this node's clock.
	updateClock func(observedTs hlc.Timestamp)
}

// errWrap is a container for an error, for use with atomic.Value, which
// requires that all of things stored in it must have the same concrete type.
type errWrap struct {
	err error
}

// rowResultWriter is a subset of CommandResult to be used with the
// distSQLReceiver. It's implemented by RowResultWriter.
type rowResultWriter interface {
	AddRow(ctx context.Context, row tree.Datums) error
	IncrementRowsAffected(n int)
	SetError(error)
	Err() error
}

// errOnlyResultWriter is a rowResultWriter that only supports receiving an
// error. All other functions that deal with producing results panic.
type errOnlyResultWriter struct {
	err error
}

var _ rowResultWriter = &errOnlyResultWriter{}

func (w *errOnlyResultWriter) SetError(err error) {
	w.err = err
}
func (w *errOnlyResultWriter) Err() error {
	return w.err
}

func (w *errOnlyResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	panic("AddRow not supported by errOnlyResultWriter")
}
func (w *errOnlyResultWriter) IncrementRowsAffected(n int) {
	panic("IncrementRowsAffected not supported by errOnlyResultWriter")
}

var _ distsqlrun.RowReceiver = &distSQLReceiver{}

// makeDistSQLReceiver creates a distSQLReceiver.
//
// ctx is the Context that the receiver will use throughput its
// lifetime. resultWriter is the container where the results will be
// stored. If only the row count is needed, this can be nil.
//
// txn is the transaction in which the producer flow runs; it will be updated
// on errors. Nil if the flow overall doesn't run in a transaction.
func makeDistSQLReceiver(
	ctx context.Context,
	resultWriter rowResultWriter,
	stmtType tree.StatementType,
	rangeCache *kv.RangeDescriptorCache,
	leaseCache *kv.LeaseHolderCache,
	txn *client.Txn,
	updateClock func(observedTs hlc.Timestamp),
) *distSQLReceiver {
	r := &distSQLReceiver{
		ctx:          ctx,
		resultWriter: resultWriter,
		rangeCache:   rangeCache,
		leaseCache:   leaseCache,
		txn:          txn,
		updateClock:  updateClock,
		stmtType:     stmtType,
	}
	// When the root transaction finishes (i.e. it is abandoned, aborted, or
	// committed), ensure the flow is canceled so that we don't return results to
	// the client that might have missed seeing their own writes. The committed
	// case shouldn't happen.
	//
	// TODO(andrei): Instead of doing this, should we lift this transaction
	// monitoring to connExecutor and have it cancel the SQL txn's context? Or for
	// that matter, should the TxnCoordSender cancel the context itself?
	if r.txn != nil {
		r.txn.OnFinish(func(err error) {
			r.txnAbortedErr.Store(errWrap{err: err})
		})
	}
	return r
}

// SetError provides a convenient way for a client to pass in an error, thus
// pretending that a query execution error happened. The error is passed along
// to the resultWriter.
func (r *distSQLReceiver) SetError(err error) {
	r.resultWriter.SetError(err)
}

// Push is part of the RowReceiver interface.
func (r *distSQLReceiver) Push(
	row sqlbase.EncDatumRow, meta *distsqlrun.ProducerMetadata,
) distsqlrun.ConsumerStatus {
	if meta != nil {
		if meta.TxnMeta != nil {
			if r.txn != nil {
				if r.txn.ID() == meta.TxnMeta.Txn.ID {
					r.txn.AugmentTxnCoordMeta(*meta.TxnMeta)
				}
			} else {
				r.resultWriter.SetError(
					errors.Errorf("received a leaf TxnCoordMeta (%s); but have no root", meta.TxnMeta))
			}
		}
		if meta.Err != nil && r.resultWriter.Err() == nil {
			if r.txn != nil {
				if retryErr, ok := meta.Err.(*roachpb.UnhandledRetryableError); ok {
					// Update the txn in response to remote errors. In the non-DistSQL
					// world, the TxnCoordSender handles "unhandled" retryable errors,
					// but this one is coming from a distributed SQL node, which has
					// left the handling up to the root transaction.
					meta.Err = r.txn.UpdateStateOnRemoteRetryableErr(r.ctx, &retryErr.PErr)
					// Update the clock with information from the error. On non-DistSQL
					// code paths, the DistSender does this.
					// TODO(andrei): We don't propagate clock signals on success cases
					// through DistSQL; we should. We also don't propagate them through
					// non-retryable errors; we also should.
					r.updateClock(retryErr.PErr.Now)
				}
			}
			r.resultWriter.SetError(meta.Err)
		}
		if len(meta.Ranges) > 0 {
			if err := r.updateCaches(r.ctx, meta.Ranges); err != nil && r.resultWriter.Err() == nil {
				r.resultWriter.SetError(err)
			}
		}
		if len(meta.TraceData) > 0 {
			span := opentracing.SpanFromContext(r.ctx)
			if span == nil {
				r.resultWriter.SetError(
					errors.New("trying to ingest remote spans but there is no recording span set up"))
			} else if err := tracing.ImportRemoteSpans(span, meta.TraceData); err != nil {
				r.resultWriter.SetError(errors.Errorf("error ingesting remote spans: %s", err))
			}
		}
		return r.status
	}
	if r.resultWriter.Err() == nil && r.txnAbortedErr.Load() != nil {
		r.resultWriter.SetError(r.txnAbortedErr.Load().(errWrap).err)
	}
	if r.resultWriter.Err() == nil && r.ctx.Err() != nil {
		r.resultWriter.SetError(r.ctx.Err())
	}
	if r.resultWriter.Err() != nil {
		// TODO(andrei): We should drain here if we weren't canceled.
		return distsqlrun.ConsumerClosed
	}
	if r.status != distsqlrun.NeedMoreRows {
		return r.status
	}

	if r.stmtType != tree.Rows {
		// We only need the row count.
		r.resultWriter.IncrementRowsAffected(1)
		return r.status
	}
	if r.row == nil {
		r.row = make(tree.Datums, len(r.resultToStreamColMap))
	}
	for i, resIdx := range r.resultToStreamColMap {
		err := row[resIdx].EnsureDecoded(&r.outputTypes[resIdx], &r.alloc)
		if err != nil {
			r.resultWriter.SetError(err)
			r.status = distsqlrun.ConsumerClosed
			return r.status
		}
		r.row[i] = row[resIdx].Datum
	}
	// Note that AddRow accounts for the memory used by the Datums.
	if commErr := r.resultWriter.AddRow(r.ctx, r.row); commErr != nil {
		r.commErr = commErr
		// Set the error on the resultWriter too, for the convenience of some of the
		// clients. If clients don't care to differentiate between communication
		// errors and query execution errors, they can simply inspect
		// resultWriter.Err(). Also, this function itself doesn't care about the
		// distinction and just uses resultWriter.Err() to see if we're still
		// accepting results.
		r.resultWriter.SetError(commErr)
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
	if r.txn != nil {
		r.txn.OnFinish(nil)
	}
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
		r.leaseCache.Update(ctx, ri.Desc.RangeID, ri.Lease.Replica.StoreID)
	}
	return nil
}

// PlanAndRun generates a physical plan from a planNode tree and executes it. It
// assumes that the tree is supported (see CheckSupport).
//
// All errors encoutered are reported to the distSQLReceiver's resultWriter.
// Additionally, if the error is a "communication error" (an error encoutered
// while using that resultWriter), the error is also stored in
// distSQLReceiver.commErr. That can be tested to see if a client session needs
// to be closed.
func (dsp *DistSQLPlanner) PlanAndRun(
	ctx context.Context,
	txn *client.Txn,
	tree planNode,
	recv *distSQLReceiver,
	evalCtx *extendedEvalContext,
) {
	planCtx := dsp.newPlanningCtx(ctx, evalCtx, txn)

	log.VEvent(ctx, 1, "creating DistSQL plan")

	plan, err := dsp.createPlanForNode(&planCtx, tree)
	if err != nil {
		recv.SetError(err)
		return
	}
	dsp.FinalizePlan(&planCtx, &plan)
	dsp.Run(&planCtx, txn, &plan, recv, evalCtx)
}
