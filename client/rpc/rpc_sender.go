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
// Author: Peter Mattis (peter@cockroachlabs.com)

package rpc

import (
	"fmt"
	"net"
	"net/url"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	roachrpc "github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
)

func init() {
	f := func(u *url.URL, ctx *base.Context, retryOpts retry.Options) (client.Sender, error) {
		ctx.Insecure = (u.Scheme != "rpcs")
		return newSender(u.Host, ctx, retryOpts)
	}
	client.RegisterSender("rpc", f)
	client.RegisterSender("rpcs", f)
}

// Sender is an implementation of Sender which exposes the
// Key-Value database provided by a Cockroach cluster by connecting
// via RPC to a Cockroach node. Overly-busy nodes will redirect this
// client to other nodes.
//
// TODO(pmattis): This class is insufficiently tested and not intended
// for use outside of benchmarking.
type Sender struct {
	client    *roachrpc.Client
	retryOpts retry.Options
}

// newSender returns a new instance of Sender.
func newSender(server string, context *base.Context, retryOpts retry.Options) (*Sender, error) {
	addr, err := net.ResolveTCPAddr("tcp", server)
	if err != nil {
		return nil, err
	}

	if context.Insecure {
		log.Warning("running in insecure mode, this is strongly discouraged. See --insecure and --certs.")
	}
	ctx := roachrpc.NewContext(context, hlc.NewClock(hlc.UnixNano), nil)
	client := roachrpc.NewClient(addr, &retryOpts, ctx)
	return &Sender{
		client:    client,
		retryOpts: retryOpts,
	}, nil
}

// Send sends call to Cockroach via an RPC request. Errors which are retryable
// are retried with backoff in a loop using the default retry options. Other
// errors sending the request are retried indefinitely using the same client
// command ID to avoid reporting failure when in fact the command may have gone
// through and been executed successfully. We retry here to eventually get
// through with the same client command ID and be given the cached response.
func (s *Sender) Send(_ context.Context, call proto.Call) {
	retryOpts := s.retryOpts
	retryOpts.Tag = fmt.Sprintf("rpc %s", call.Method())

	if err := retry.WithBackoff(retryOpts, func() (retry.Status, error) {
		if !s.client.IsHealthy() {
			return retry.Continue, nil
		}

		method := call.Args.Method().String()
		c := s.client.Go("Server."+method, call.Args, call.Reply, nil)
		<-c.Done
		if c.Error != nil {
			// Assume all errors sending request are retryable. The actual
			// number of things that could go wrong is vast, but we don't
			// want to miss any which should in theory be retried with the
			// same client command ID. We log the error here as a warning so
			// there's visiblity that this is happening. Some of the errors
			// we'll sweep up in this net shouldn't be retried, but we can't
			// really know for sure which.
			log.Warningf("failed to send RPC request %s: %v", method, c.Error)
			return retry.Continue, nil
		}

		// On successful post, we're done with retry loop.
		return retry.Break, nil
	}); err != nil {
		call.Reply.Header().SetGoError(err)
	}
}
