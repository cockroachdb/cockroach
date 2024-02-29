// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (t *statusServer) DownloadSpan(
	ctx context.Context, req *serverpb.DownloadSpanRequest,
) (*serverpb.DownloadSpanResponse, error) {
	ctx = t.AnnotateCtx(ctx)

	// TODO(adityamaru): Figure out proper privileges, for now we restrict this
	// call to system tenant only in the tenant connector.
	return t.sqlServer.tenantConnect.DownloadSpan(ctx, req)
}

func (s *systemStatusServer) DownloadSpan(
	ctx context.Context, req *serverpb.DownloadSpanRequest,
) (*serverpb.DownloadSpanResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	resp := &serverpb.DownloadSpanResponse{
		ErrorsByNodeID: make(map[roachpb.NodeID]string),
	}
	if len(req.NodeID) > 0 {
		_, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}

		if local {
			return resp, s.localDownloadSpan(ctx, req)
		}

		return nil, errors.AssertionFailedf("requesting download on a specific node is not supported yet")
	}

	// Send DownloadSpan request to all stores on all nodes.
	remoteRequest := *req
	remoteRequest.NodeID = "local"
	nodeFn := func(ctx context.Context, status serverpb.StatusClient, _ roachpb.NodeID) (*serverpb.DownloadSpanResponse, error) {
		return status.DownloadSpan(ctx, &remoteRequest)
	}
	responseFn := func(nodeID roachpb.NodeID, downloadSpanResp *serverpb.DownloadSpanResponse) {}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		resp.ErrorsByNodeID[nodeID] = err.Error()
	}

	if err := iterateNodes(ctx, s.serverIterator, s.stopper, "download spans",
		noTimeout,
		s.dialNode,
		nodeFn,
		responseFn,
		errorFn,
	); err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	return resp, nil
}

func (s *systemStatusServer) localDownloadSpan(
	ctx context.Context, req *serverpb.DownloadSpanRequest,
) error {
	return s.stores.VisitStores(func(store *kvserver.Store) error {
		downloadSpansCh, stopTuningCh := make(chan roachpb.Span), make(chan struct{})
		return ctxgroup.GoAndWait(ctx,
			// Tune download concurrency until stopTuningCh closes.
			func(ctx context.Context) error {
				t := time.NewTicker(time.Second * 15)
				defer t.Stop()
				return tuneDownloadCurrency(ctx, t.C, stopTuningCh, store.StateEngine(),
					s.sqlServer.cfg.RuntimeStatSampler.GetCPUCombinedPercentNorm)
			},
			// Download until downloadSpansCh closes, then close stopTuningCh.
			func(ctx context.Context) error {
				defer close(stopTuningCh)
				return downloadSpans(ctx, store.StateEngine(), downloadSpansCh, req.ViaBackingFileDownload)
			},
			// Send spans to downloadSpansCh.
			func(ctx context.Context) error {
				defer close(downloadSpansCh)
				return sendDownloadSpans(ctx, req.Spans, downloadSpansCh)
			},
		)
	})
}

// sendDownloadSpans sends spans that cover all spans in spans to the passed ch.
func sendDownloadSpans(ctx context.Context, spans roachpb.Spans, out chan roachpb.Span) error {
	ctxDone := ctx.Done()
	for _, sp := range spans {
		select {
		case out <- sp:
		case <-ctxDone:
			return ctx.Err()
		}
	}
	log.Infof(ctx, "all %d download spans enqueued", len(spans))
	return nil
}

// downloadSpans instructs the passed engine, in parallel, to downloads spans
// received on the passed ch until it closes.
func downloadSpans(ctx context.Context, e storage.Engine, spans chan roachpb.Span, cp bool) error {
	const downloadWaiters = 16
	return ctxgroup.GroupWorkers(ctx, downloadWaiters, func(ctx context.Context, _ int) error {
		for sp := range spans {
			if err := e.Download(ctx, sp, cp); err != nil {
				return err
			}
		}
		log.Infof(ctx, "finished downloading %d spans", len(spans))
		return nil
	})
}

// tuneDownloadCurrency periodically adjusts download concurrency up or down on
// the passed engine based on the cpu load value read from the passed func until
// the passed done ch closes or is signaled.
func tuneDownloadCurrency(
	ctx context.Context,
	tick <-chan time.Time,
	done <-chan struct{},
	eng storage.Engine,
	readCPU func() float64,
) error {
	var added int64
	// Remove any additional concurrency we've added when we exit.
	//
	// TODO(dt,radu): Ideally we'd adjust a separate limit that applies only
	// to download compactions, so that this does not fight with manual calls
	// to SetConcurrentCompactions.
	defer func() {
		if added != 0 {
			adjusted := eng.AdjustCompactionConcurrency(-added)
			log.Infof(ctx, "downloads complete; reset compaction concurrency to %d", adjusted)
		}
	}()

	const maxAddedConcurrency, lowCPU, highCPU, initialIncrease = 16, 0.65, 0.8, 8

	// Begin by bumping up the concurrency by 8, then start watching the CPU
	// usage and adjusting up or down based on CPU until downloading finishes.
	eng.AdjustCompactionConcurrency(initialIncrease)
	added += initialIncrease

	ctxDone := ctx.Done()

	for {
		select {
		case <-ctxDone:
			return ctx.Err()
		case <-done:
			return nil
		case <-tick:
			cpu := readCPU()
			if cpu > highCPU && added > 0 {
				// If CPU is high and we have added any additional concurrency, we
				// should reduce our added concurrency to make sure CPU is available
				// for the execution of foreground traffic.
				adjusted := eng.AdjustCompactionConcurrency(-1)
				added--
				log.Infof(ctx, "decreasing additional compaction concurrency to %d (%d total) due cpu usage %.0f%% > %.0f%%", added, adjusted, cpu*100, highCPU*100)
			} else if cpu < lowCPU {
				// If CPU is low, we should use it to do additional downloading.
				if added < maxAddedConcurrency {
					adjusted := eng.AdjustCompactionConcurrency(1)
					added++
					log.Infof(ctx, "increasing additional compaction concurrency to %d (%d total) due cpu usage %.0f%% < %.0f%%", added, adjusted, cpu*100, lowCPU*100)
				}
			}
		}
	}
}
