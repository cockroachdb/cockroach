// Copyright 2021 The Cockroach Authors.
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
	"bufio"
	"bytes"
	"context"
	"net/http"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type memoryWriter struct {
	data   bytes.Buffer
	status int
	header http.Header
}

func (w *memoryWriter) Header() http.Header {
	return w.header
}

func (w *memoryWriter) Write(in []byte) (int, error) {
	return w.data.Write(in)
}

func (w *memoryWriter) WriteHeader(statusCode int) {
	w.status = statusCode
}

var _ http.ResponseWriter = &memoryWriter{}

// HttpProxy is an endpoint that handles http requests for a specific
// node. The implementation delegates to the local server's `mux` to
// serve the request. The response is held in a `memoryWriter` defined
// above and then serialized into a proto to send back to the requesting
// node. You can see where `HttpProxy` is invoked below in `routeToNode`
// which will be invoked by the requesting node.
func (s *adminServer) HttpProxy(
	ctx context.Context, req *serverpb.HttpProxyRequest,
) (*serverpb.HttpProxyResponse, error) {
	w := memoryWriter{
		header: make(http.Header),
	}
	r, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(req.Req)))
	if err != nil {
		return nil, err
	}
	s.server.mux.ServeHTTP(&w, r)
	return &serverpb.HttpProxyResponse{
		Status: int32(w.status),
		Body:   w.data.Bytes(),
	}, nil
}

// nodeRouterMiddleware wraps a handler with middleware that inspects
// a cookie named `node_id` before handling the request using `next`.
// If the cookie is present and matches a NodeID that's not the current
// node, the request is routed via gRPC to the node identified in the
// cookie. The HTTP request is serialized into a byte array and the
// response is deserialized from gRPC and written into the response
// object here.
func (s *Server) nodeRouterMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		cookie, err := r.Cookie("node_id")
		if err == nil && cookie.Value != "" && cookie.Value != "local" {
			nodeIDStr := cookie.Value
			nodeIDInt, err := strconv.ParseInt(nodeIDStr, 10, 32)
			if err != nil {
				log.Errorf(ctx, "server: unable to parse nodeID from cookie node_id: %v", err)
				http.Error(w, err.Error(), 400)
				return
			}
			nodeID := roachpb.NodeID(nodeIDInt)
			if nodeID != s.gossip.NodeID.Get() {
				s.routeToNode(w, r, nodeID)
				return
			}
		}
		next.ServeHTTP(w, r)
	}
}

func (s *Server) routeToNode(w http.ResponseWriter, r *http.Request, nodeID roachpb.NodeID) {
	ctx := r.Context()

	addr, err := s.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		log.Errorf(ctx, "server: node router failed to get node address: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	conn, err := s.rpcContext.GRPCDialNode(addr.String(), nodeID, rpc.DefaultClass).Connect(ctx)
	if err != nil {
		log.Errorf(ctx, "server: node router failed to dial node: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	reqBuf := bytes.Buffer{}
	err = r.Write(&reqBuf)
	if err != nil {
		log.Errorf(ctx, "server: node router failed to serialize request: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	client := serverpb.NewAdminClient(conn)

	resp, err := client.HttpProxy(ctx, &serverpb.HttpProxyRequest{
		Req: reqBuf.Bytes(),
	})
	if err != nil {
		log.Errorf(ctx, "server: node router proxy request failed: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	// The HTTP API will write `200 OK` in the call to `.Write` so this
	// needs to be sequenced prior to mark the header as "written".
	if resp.Status != 0 {
		w.WriteHeader(int(resp.Status))
	}

	_, err = w.Write(resp.Body)
	if err != nil {
		log.Errorf(ctx, "server: node router failed to write response: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
}
