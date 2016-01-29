// Copyright 2014 The Cockroach Authors.
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
// Author: Tamir Duberstein (tamird@gmail.com)

package grpcutil

import (
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"strings"

	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/stop"
)

// ListenAndServeGRPC creates a listener and serves server on it, closing
// the listener when signalled by the stopper.
func ListenAndServeGRPC(stopper *stop.Stopper, server *grpc.Server, addr net.Addr, config *tls.Config) (net.Listener, error) {
	ln, err := util.Listen(addr, config)
	if err != nil {
		return nil, err
	}

	stopper.RunWorker(func() {
		if err := server.Serve(ln); err != nil && !util.IsClosedConnection(err) {
			log.Fatal(err)
		}
	})

	stopper.RunWorker(func() {
		<-stopper.ShouldStop()
		if err := ln.Close(); err != nil {
			log.Fatal(err)
		}
	})

	return ln, nil
}

// GRPCHandlerFunc returns an http.Handler that delegates to grpcServer on incoming gRPC
// connections or otherHandler otherwise.
func GRPCHandlerFunc(grpcServer *grpc.Server, otherHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// TODO(tamird): point to merged gRPC code rather than a PR.
		// This is a partial recreation of gRPC's internal checks https://github.com/grpc/grpc-go/pull/514/files#diff-95e9a25b738459a2d3030e1e6fa2a718R61
		if r.ProtoMajor == 2 && strings.Contains(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
		} else {
			otherHandler.ServeHTTP(w, r)
		}
	})
}
