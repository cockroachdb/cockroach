// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errorspb"
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
		Errors: make(map[roachpb.NodeID]errorspb.EncodedError),
	}
	if len(req.NodeID) > 0 {
		_, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}

		if local {
			if err := s.localDownloadSpan(ctx, req); err != nil {
				resp.Errors[s.node.Descriptor.NodeID] = errors.EncodeError(ctx, err)
			}
			return resp, nil
		}

		return nil, errors.AssertionFailedf("requesting download on a specific node is not supported yet")
	}

	// Send DownloadSpan request to all stores on all nodes.
	remoteRequest := *req
	remoteRequest.NodeID = "local"
	nodeFn := func(ctx context.Context, status serverpb.StatusClient, _ roachpb.NodeID) (*serverpb.DownloadSpanResponse, error) {
		return status.DownloadSpan(ctx, &remoteRequest)
	}
	responseFn := func(nodeID roachpb.NodeID, downloadSpanResp *serverpb.DownloadSpanResponse) {
		for i, e := range downloadSpanResp.Errors {
			resp.Errors[i] = e
		}
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		if _, ok := resp.Errors[nodeID]; !ok {
			resp.Errors[nodeID] = errors.EncodeError(ctx, err)
		}
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
		downloadSpansCh := make(chan roachpb.Span)
		stopDiskMonCh := make(chan struct{})
		e := store.StateEngine()
		return ctxgroup.GoAndWait(ctx,
			// Download until downloadSpansCh closes, then close stopMonCh.
			func(ctx context.Context) error {
				defer close(stopDiskMonCh)
				return downloadSpans(ctx, e, downloadSpansCh, req.ViaBackingFileDownload)
			},
			// Send spans to downloadSpansCh.
			func(ctx context.Context) error {
				defer close(downloadSpansCh)
				return sendDownloadSpans(ctx, req.Spans, downloadSpansCh)
			},
			func(ctx context.Context) error {
				tick := time.NewTicker(time.Second * 15)
				defer tick.Stop()

				for {
					select {
					case <-stopDiskMonCh:
						return nil
					case <-ctx.Done():
						return nil
					case <-tick.C:
						if min := storage.MinCapacityForBulkIngest.Get(&s.st.SV); min > 0 {
							cap, err := e.Capacity()
							if err != nil {
								return err
							}
							if 1-cap.FractionUsed() < min {
								return &kvpb.InsufficientSpaceError{
									StoreID: store.StoreID(), Op: "ingest data", Available: cap.Available, Capacity: cap.Capacity, Required: min,
								}
							}
						}
					}
				}
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
