// Copyright 2018 The Cockroach Authors.
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
