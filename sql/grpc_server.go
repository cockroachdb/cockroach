// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package sql

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc/credentials"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/util"
)

var _ driver.SqlServiceServer = GRPCServer{}

// GRPCServer implements the driver.SqlServiceServer interface.
type GRPCServer struct {
	context  *base.Context
	executor *Executor
}

// MakeGRPCServer creates an GRPCServer.
func MakeGRPCServer(ctx *base.Context, executor *Executor) GRPCServer {
	return GRPCServer{context: ctx, executor: executor}
}

// Execute implements the driver.SqlServiceServer interface.
func (s GRPCServer) Execute(ctx context.Context, args *driver.Request) (*driver.Response, error) {
	if !s.context.Insecure {
		authInfo, ok := credentials.FromContext(ctx)
		if ok {
			switch info := authInfo.(type) {
			case credentials.TLSInfo:
				// Extract user and verify.
				// TODO(marc): we may eventually need stricter user syntax rules.
				requestedUser := args.GetUser()
				if len(requestedUser) == 0 {
					return nil, util.Errorf("missing User in request: %s", args)
				}

				certUser, err := security.GetCertificateUser(&info.State)
				if err != nil {
					return nil, err
				}

				if certUser != requestedUser {
					return nil, util.Errorf("requested user is %s, but certificate is for %s", requestedUser, certUser)
				}
			default:
				return nil, util.Errorf("grpc: got unknown auth type %q", info.AuthType())
			}
		} else {
			return nil, util.Errorf("grpc: failed to get auth info")
		}
	}

	resp, _, err := s.executor.Execute(*args)
	return &resp, err
}
