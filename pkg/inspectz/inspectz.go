// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspectz

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/inspectz/inspectzpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// URLPrefix is the prefix for all inspectz endpoints hosted by the server.
const URLPrefix = "/inspectz/"

// Server is a concrete implementation of the InspectzServer interface,
// organizing everything under /inspectz/*. It's the top-level component that
// houses parsing logic for common inspectz URL parameters and maintains routing
// logic.
type Server struct {
	log.AmbientContext

	mux                                    *http.ServeMux
	handlesV1, handlesV2                   kvflowcontrol.InspectHandles
	kvflowControllerV1, kvflowControllerV2 kvflowcontrol.InspectController
	storeLiveness                          kvserver.InspectAllStoreLiveness
}

var _ inspectzpb.InspectzServer = &Server{}

// NewServer sets up an inspectz server.
func NewServer(
	ambient log.AmbientContext,
	handlesV1, handlesV2 kvflowcontrol.InspectHandles,
	kvflowControllerV1, kvflowControllerV2 kvflowcontrol.InspectController,
	storeLiveness kvserver.InspectAllStoreLiveness,
) *Server {
	mux := http.NewServeMux()
	server := &Server{
		AmbientContext: ambient,

		mux:                mux,
		handlesV1:          handlesV1,
		handlesV2:          handlesV2,
		kvflowControllerV1: kvflowControllerV1,
		kvflowControllerV2: kvflowControllerV2,
		storeLiveness:      storeLiveness,
	}
	mux.Handle("/inspectz/v1/kvflowhandles", server.makeKVFlowHandlesHandler(server.KVFlowHandles))
	mux.Handle("/inspectz/v1/kvflowcontroller", server.makeKVFlowControllerHandler(server.KVFlowController))
	mux.Handle("/inspectz/v2/kvflowhandles", server.makeKVFlowHandlesHandler(server.KVFlowHandlesV2))
	mux.Handle("/inspectz/v2/kvflowcontroller", server.makeKVFlowControllerHandler(server.KVFlowControllerV2))
	mux.Handle(
		"/inspectz/storeliveness/supportFrom",
		server.makeStoreLivenessHandler(server.StoreLivenessSupportFrom),
	)
	mux.Handle(
		"/inspectz/storeliveness/supportFor",
		server.makeStoreLivenessHandler(server.StoreLivenessSupportFor),
	)

	return server
}

func (s *Server) makeKVFlowHandlesHandler(
	impl func(
		ctx context.Context,
		request *kvflowinspectpb.HandlesRequest,
	) (*kvflowinspectpb.HandlesResponse, error),
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := s.AnnotateCtx(context.Background())

		req := &kvflowinspectpb.HandlesRequest{}
		if rangeIDs, ok := parseRangeIDs(r.URL.Query().Get("ranges"), w); ok {
			req.RangeIDs = rangeIDs
		}
		resp, err := impl(ctx, req)
		if err != nil {
			log.ErrorfDepth(ctx, 1, "%s", err)
			http.Error(w, "internal error: check logs for details", http.StatusInternalServerError)
			return
		}
		respond(ctx, w, http.StatusOK, resp)
	}
}

func (s *Server) makeKVFlowControllerHandler(
	impl func(
		ctx context.Context,
		request *kvflowinspectpb.ControllerRequest,
	) (*kvflowinspectpb.ControllerResponse, error),
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := s.AnnotateCtx(context.Background())

		req := &kvflowinspectpb.ControllerRequest{}
		resp, err := impl(ctx, req)
		if err != nil {
			log.ErrorfDepth(ctx, 1, "%s", err)
			http.Error(w, "internal error: check logs for details", http.StatusInternalServerError)
			return
		}
		respond(ctx, w, http.StatusOK, resp)
	}
}

func (s *Server) makeStoreLivenessHandler(
	impl func(ctx context.Context, request *slpb.InspectStoreLivenessRequest) (
		*slpb.InspectStoreLivenessResponse, error,
	),
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := s.AnnotateCtx(context.Background())
		req := &slpb.InspectStoreLivenessRequest{}
		resp, err := impl(ctx, req)
		if err != nil {
			log.ErrorfDepth(ctx, 1, "%s", err)
			http.Error(w, "internal error: check logs for details", http.StatusInternalServerError)
			return
		}
		respond(ctx, w, http.StatusOK, resp)
	}
}

// KVFlowController implements the InspectzServer interface.
func (s *Server) KVFlowController(
	ctx context.Context, request *kvflowinspectpb.ControllerRequest,
) (*kvflowinspectpb.ControllerResponse, error) {
	return kvFlowController(ctx, request, s.kvflowControllerV1)
}

// KVFlowHandles implements the InspectzServer interface.
func (s *Server) KVFlowHandles(
	ctx context.Context, request *kvflowinspectpb.HandlesRequest,
) (*kvflowinspectpb.HandlesResponse, error) {
	return kvFlowHandles(ctx, request, s.handlesV1)
}

// KVFlowControllerV2 implements the InspectzServer interface.
func (s *Server) KVFlowControllerV2(
	ctx context.Context, request *kvflowinspectpb.ControllerRequest,
) (*kvflowinspectpb.ControllerResponse, error) {
	return kvFlowController(ctx, request, s.kvflowControllerV2)
}

// KVFlowHandlesV2 implements the InspectzServer interface.
func (s *Server) KVFlowHandlesV2(
	ctx context.Context, request *kvflowinspectpb.HandlesRequest,
) (*kvflowinspectpb.HandlesResponse, error) {
	return kvFlowHandles(ctx, request, s.handlesV2)
}

// StoreLivenessSupportFrom implements the InspectzServer interface.
func (s *Server) StoreLivenessSupportFrom(
	_ context.Context, _ *slpb.InspectStoreLivenessRequest,
) (*slpb.InspectStoreLivenessResponse, error) {
	resp := &slpb.InspectStoreLivenessResponse{}
	support, err := s.storeLiveness.InspectAllSupportFrom()
	resp.SupportStatesPerStore = support
	return resp, err
}

// StoreLivenessSupportFor implements the InspectzServer interface.
func (s *Server) StoreLivenessSupportFor(
	_ context.Context, _ *slpb.InspectStoreLivenessRequest,
) (*slpb.InspectStoreLivenessResponse, error) {
	resp := &slpb.InspectStoreLivenessResponse{}
	support, err := s.storeLiveness.InspectAllSupportFor()
	resp.SupportStatesPerStore = support
	return resp, err
}

// ServeHTTP serves various tools under the /debug endpoint.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func kvFlowHandles(
	ctx context.Context,
	request *kvflowinspectpb.HandlesRequest,
	handles kvflowcontrol.InspectHandles,
) (*kvflowinspectpb.HandlesResponse, error) {
	resp := &kvflowinspectpb.HandlesResponse{}
	if len(request.RangeIDs) == 0 {
		request.RangeIDs = handles.Inspect()
	}
	for _, rangeID := range request.RangeIDs {
		handle, found := handles.LookupInspect(rangeID)
		if !found {
			continue // nothing to do
		}
		resp.Handles = append(resp.Handles, handle)
	}
	return resp, nil
}

func kvFlowController(
	ctx context.Context,
	request *kvflowinspectpb.ControllerRequest,
	controller kvflowcontrol.InspectController,
) (*kvflowinspectpb.ControllerResponse, error) {
	return &kvflowinspectpb.ControllerResponse{
		Streams: controller.Inspect(ctx),
	}, nil
}

func respond(ctx context.Context, w http.ResponseWriter, code int, payload interface{}) {
	res, err := json.Marshal(payload)
	if err != nil {
		log.ErrorfDepth(ctx, 1, "%s", err)
		http.Error(w, "internal error: check logs for details", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(res)
}

func parseRangeIDs(input string, w http.ResponseWriter) (ranges []roachpb.RangeID, ok bool) {
	if len(input) == 0 {
		return nil, true
	}
	for _, part := range strings.Split(input, ",") {
		rangeID, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			http.Error(w, "invalid range id", http.StatusBadRequest)
			return nil, false
		}

		ranges = append(ranges, roachpb.RangeID(rangeID))
	}
	return ranges, true
}
