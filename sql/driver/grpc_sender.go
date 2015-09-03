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
// Author: Tamir Duberstein

package driver

import (
	"net/url"

	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
)

func init() {
	f := func(u *url.URL, ctx *base.Context, retryOpts retry.Options) (Sender, error) {
		ctx.Insecure = (u.Scheme != "grpcs")
		return newGRPCSender(u.Host, ctx, retryOpts)
	}
	RegisterSender("grpc", f)
	RegisterSender("grpcs", f)
}

// grpcSender is an implementation of Sender which exposes the
// SQL database provided by a Cockroach cluster by connecting
// via GRPC to a Cockroach node.
type grpcSender struct {
	user       string
	grpcClient SqlServiceClient
}

// newGRPCSender returns a new instance of grpcSender.
func newGRPCSender(server string, ctx *base.Context, retryOpts retry.Options) (*grpcSender, error) {
	var opts []grpc.DialOption

	if ctx.Insecure {
		log.Warning("running in insecure mode, this is strongly discouraged. See --insecure and --certs.")
		opts = append(opts, grpc.WithInsecure())
	} else {
		tlsConfig, err := ctx.GetClientTLSConfig()
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	conn, err := grpc.Dial(server, opts...)
	if err != nil {
		return nil, err
	}

	return &grpcSender{
		grpcClient: NewSqlServiceClient(conn),
		user:       ctx.User,
	}, nil
}

// Send sends call to Cockroach via GRPC.
func (s *grpcSender) Send(args Request) (*Response, error) {
	// Prepare the args.
	if args.GetUser() == "" {
		args.User = s.user
	}
	resp, err := s.grpcClient.Execute(context.Background(), &args)
	return resp, err
}
