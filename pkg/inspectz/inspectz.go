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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
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

	mux              *http.ServeMux
	handles          kvflowcontrol.Handles
	kvflowController kvflowcontrol.Controller
}

var _ inspectzpb.InspectzServer = &Server{}

// NewServer sets up an inspectz server.
func NewServer(
	ambient log.AmbientContext,
	handles kvflowcontrol.Handles,
	kvflowController kvflowcontrol.Controller,
) *Server {
	mux := http.NewServeMux()
	server := &Server{
		AmbientContext: ambient,

		mux:              mux,
		handles:          handles,
		kvflowController: kvflowController,
	}
	mux.Handle("/inspectz/kvflowhandles", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			ctx := server.AnnotateCtx(context.Background())

			req := &kvflowinspectpb.HandlesRequest{}
			if rangeIDs, ok := parseRangeIDs(r.URL.Query().Get("ranges"), w); ok {
				req.RangeIDs = rangeIDs
			}
			resp, err := server.KVFlowHandles(ctx, req)
			if err != nil {
				log.ErrorfDepth(ctx, 1, "%s", err)
				http.Error(w, "internal error: check logs for details", http.StatusInternalServerError)
				return
			}
			respond(ctx, w, http.StatusOK, resp)
		},
	))
	mux.Handle("/inspectz/kvflowcontroller", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			ctx := server.AnnotateCtx(context.Background())

			req := &kvflowinspectpb.ControllerRequest{}
			resp, err := server.KVFlowController(ctx, req)
			if err != nil {
				log.ErrorfDepth(ctx, 1, "%s", err)
				http.Error(w, "internal error: check logs for details", http.StatusInternalServerError)
				return
			}
			respond(ctx, w, http.StatusOK, resp)
		},
	))

	return server
}

// KVFlowController implements the InspectzServer interface.
func (s *Server) KVFlowController(
	ctx context.Context, request *kvflowinspectpb.ControllerRequest,
) (*kvflowinspectpb.ControllerResponse, error) {
	return &kvflowinspectpb.ControllerResponse{
		Streams: s.kvflowController.Inspect(ctx),
	}, nil
}

// KVFlowHandles implements the InspectzServer interface.
func (s *Server) KVFlowHandles(
	ctx context.Context, request *kvflowinspectpb.HandlesRequest,
) (*kvflowinspectpb.HandlesResponse, error) {
	resp := &kvflowinspectpb.HandlesResponse{}
	if len(request.RangeIDs) == 0 {
		request.RangeIDs = s.handles.Inspect()
	}
	for _, rangeID := range request.RangeIDs {
		handle, found := s.handles.Lookup(rangeID)
		if !found {
			continue // nothing to do
		}
		resp.Handles = append(resp.Handles, handle.Inspect(ctx))
	}
	return resp, nil
}

// ServeHTTP serves various tools under the /debug endpoint.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
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
