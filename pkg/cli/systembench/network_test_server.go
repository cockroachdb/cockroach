// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package systembench

import (
	"fmt"
	"math"
	"net"

	"github.com/cockroachdb/cockroach/pkg/cli/systembench/systembenchpb"
	"google.golang.org/grpc"
)

// ServerOptions holds parameters for server part of
// the network test.
type ServerOptions struct {
	Port string
}

// 56 bytes is inline with ping.
var serverPayload = 56

// RunServer runs a server for network benchmarks.
func RunServer(serverOptions ServerOptions) error {
	lis, err := net.Listen("tcp", ":"+serverOptions.Port)
	if err != nil {
		return err
	}
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxConcurrentStreams(math.MaxInt32),
		grpc.InitialWindowSize(65535),
		grpc.InitialConnWindowSize(65535),
	)
	systembench.RegisterPingerServer(s, newPinger())
	fmt.Printf("server starting on %s", serverOptions.Port)
	return s.Serve(lis)
}
