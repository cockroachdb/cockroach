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
	"strings"

	"golang.org/x/net/context"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
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
func (dsp *distSQLPlanner) Run(
	planCtx *planningCtx,
	txn *client.Txn,
	plan *physicalPlan,
	recv *distSQLReceiver,
	evalCtx parser.EvalContext,
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

	dsp.distSQLSrv.ServerConfig.Metrics.QueryStart()
	defer dsp.distSQLSrv.ServerConfig.Metrics.QueryStop()

	recv.resultToStreamColMap = plan.planToStreamColMap
	thisNodeID := dsp.nodeDesc.NodeID
	thisNodeAddr := planCtx.nodeAddresses[thisNodeID]

	// DistSQL needs to initialize the Transaction proto before we put it in the
	// FlowRequest's below. This is because we might not have used the txn do to
	// anything else (we might not have sent any requests through the client.Txn,
	// which normally does this init).
	txn.EnsureProto()

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

		addFallbackToOutputStreams(&flowSpec, thisNodeAddr)
		flows[nodeID] = flowSpec

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
	var flowsToRunLocally []roachpb.NodeID
	// Now wait for all the flows to be scheduled on remote nodes. Note that we
	// are not waiting for the flows themselves to complete.
	for i := 0; i < len(flows)-1; i++ {
		res := <-resultChan
		if res.err != nil {
			// Flows for which we've gotten an error will be run locally iff we know for
			// sure that the server where we attempted to schedule them did not, in
			// fact, schedule them (we don't want to schedule the same flow on two
			// nodes, as that might corrupt results in cases some producers connect to
			// one node and other to the other).
			// Note that admission control errors are not recovered in this way - we
			// don't want to overload the local node in case another node is overloaded.
			// TODO(andrei, radu): this should be reconsidered when we integrate the
			// local flow with local admission control.
			if isVersionMismatchErr(res.err) || netutil.ErrIsGRPCUnavailable(res.err) {
				// Flows that failed to schedule because of version mismatch will be run
				// locally.
				flowsToRunLocally = append(flowsToRunLocally, res.nodeID)
				continue
			}
			if firstErr == nil {
				firstErr = res.err
			}
		}
	}
	if firstErr != nil {
		// TODO(andrei): we should "poison" the local flow registry's entry for this
		// flow, such that remote flows that have been successfully schedule don't
		// block waiting for the gateway flow to start (until the connection timeout
		// expires).
		return firstErr
	}

	// Merge the flowsToRunLocally (if any) into the local flow.
	localFlow := flows[thisNodeID]
	if len(flowsToRunLocally) > 0 {
		addressesToRewrite := make(map[string]struct{})
		for _, failedNodeID := range flowsToRunLocally {
			log.VEventf(ctx, 2, "scheduling locally flow for node: %d", failedNodeID)
			// Copy over the processors to the local flow.
			localFlow.Processors = append(localFlow.Processors, flows[failedNodeID].Processors...)
			addressesToRewrite[planCtx.nodeAddresses[failedNodeID]] = struct{}{}
		}
		// Rewrite the streams that are now local.
		rewriteStreamsToLocal(&localFlow, addressesToRewrite)
	}

	// Set up the flow on this node.
	localReq := distsqlrun.SetupFlowRequest{
		Version:     distsqlrun.Version,
		Txn:         *txn.Proto(),
		Flow:        localFlow,
		EvalContext: evalCtxProto,
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

// rewriteStreamToLocal rewrites all the streams in flow that are either inbound
// from or outbound to any node in addressesToRewrite (identified by the node's
// address) to local streams. This is called after the respective processors
// have been moved to run locally.
func rewriteStreamsToLocal(flow *distsqlrun.FlowSpec, addressesToRewrite map[string]struct{}) {
	// Two steps:
	// 1. Rewrite the outgoing endpoints, identifying them by
	// addresses of failed nodes. Also collect all the stream IDs. These need
	// collecting because only outgoing endpoints have addresses in the spec, but
	// we also need to rewrite the incoming endpoints.
	// 2. Rewrite the incoming endpoints.
	streamIDs := make(map[distsqlrun.StreamID]struct{})
	for i := range flow.Processors {
		proc := &flow.Processors[i]
		for j := range proc.Output {
			output := &proc.Output[j]
			for k := range output.Streams {
				outputStream := &output.Streams[k]
				if _, ok := addressesToRewrite[outputStream.TargetAddr]; ok {
					outputStream.Type = distsqlrun.StreamEndpointSpec_LOCAL
					outputStream.TargetAddr = ""
					streamIDs[outputStream.StreamID] = struct{}{}
				}
			}
		}
	}

	for i := range flow.Processors {
		proc := &flow.Processors[i]
		for j := range proc.Input {
			input := &proc.Input[j]
			for k := range input.Streams {
				inputStream := &input.Streams[k]
				if _, ok := streamIDs[inputStream.StreamID]; ok {
					inputStream.Type = distsqlrun.StreamEndpointSpec_LOCAL
				}
			}
		}
	}
}

// addFallbackToOutputStreams fills in the FallbackAddr field of all the output
// streams in the flow to the specified gatewayAddr
func addFallbackToOutputStreams(flow *distsqlrun.FlowSpec, gatewayAddr string) {
	for i := range flow.Processors {
		proc := &flow.Processors[i]
		for j := range proc.Output {
			output := &proc.Output[j]
			for k := range output.Streams {
				outputStream := &output.Streams[k]
				if outputStream.Type == distsqlrun.StreamEndpointSpec_REMOTE {
					outputStream.FallbackAddr = &gatewayAddr
				}
			}
		}
	}
}

// isVersionMismatchErr checks whether the error is a DistSQL version mismatch
// error, indicating that a SetupFlow request failed because a remote node is
// incompatible.
func isVersionMismatchErr(err error) bool {
	// We check both the error string and the error type. We'd like to check just
	// the type, but 1.0 didn't have a typed error.
	if strings.HasPrefix(err.Error(), distsqlrun.VersionMismatchErrorPrefix) {
		return true
	}
	_, ok := err.(*distsqlrun.VersionMismatchError)
	return ok
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
	rows *sqlbase.RowContainer
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

	// The transaction in which the flow producing data for this receiver runs.
	// The distSQLReceiver updates the TransactionProto in response to
	// RetryableTxnError's. Nil if no transaction should be updated on errors
	// (i.e. if the flow overall doesn't run in a transaction).
	txn *client.Txn

	// A handler for clock signals arriving from remote nodes. This should update
	// this node's clock.
	updateClock func(observedTs hlc.Timestamp)
}

var _ distsqlrun.RowReceiver = &distSQLReceiver{}

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
	sink *sqlbase.RowContainer,
	rangeCache *kv.RangeDescriptorCache,
	leaseCache *kv.LeaseHolderCache,
	txn *client.Txn,
	updateClock func(observedTs hlc.Timestamp),
) (distSQLReceiver, error) {
	return distSQLReceiver{
		ctx:         ctx,
		rows:        sink,
		rangeCache:  rangeCache,
		leaseCache:  leaseCache,
		txn:         txn,
		updateClock: updateClock,
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
