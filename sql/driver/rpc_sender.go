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

package driver

import (
	"fmt"
	"net"
	"net/url"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
)

func init() {
	f := func(u *url.URL, ctx *base.Context, retryOpts retry.Options, stopper *stop.Stopper) (Sender, error) {
		ctx.Insecure = (u.Scheme != "rpcs")
		return newRPCSender(u.Host, ctx, retryOpts, stopper)
	}
	RegisterSender("rpc", f)
	RegisterSender("rpcs", f)
}

const RPCMethod = "Server.SQL"

// rpcSender is an implementation of Sender which exposes the SQL database
// provided by a Cockroach cluster by connecting via RPC to a Cockroach node.
type rpcSender struct {
	user      string
	client    *rpc.Client
	retryOpts retry.Options
}

// newRPCSender returns a new instance of rpcSender.
func newRPCSender(server string, context *base.Context, retryOpts retry.Options, stopper *stop.Stopper) (*rpcSender, error) {
	addr, err := net.ResolveTCPAddr("tcp", server)
	if err != nil {
		return nil, err
	}

	if context.Insecure {
		log.Warning("running in insecure mode, this is strongly discouraged. See --insecure and --certs.")
	} else {
		if _, err := context.GetClientTLSConfig(); err != nil {
			return nil, err
		}
	}

	ctx := rpc.NewContext(context, hlc.NewClock(hlc.UnixNano), stopper)
	client := rpc.NewClient(addr, ctx)
	return &rpcSender{
		user:      context.User,
		client:    client,
		retryOpts: retryOpts,
	}, nil
}

// Send sends call to Cockroach via an RPC.
func (s *rpcSender) Send(args Request) (Response, error) {
	if args.GetUser() == "" {
		args.User = s.user
	}

	var err error
	var reply Response
	for r := retry.Start(s.retryOpts); r.Next(); {
		select {
		case <-s.client.Healthy():
		default:
			err = fmt.Errorf("failed to send RPC request %s: client is unhealthy", RPCMethod)
			log.Warning(err)
			continue
		}

		if err = s.client.Call(RPCMethod, &args, &reply); err != nil {
			reply.Reset() // don't trust anyone.
			// Assume all errors sending request are retryable. The actual
			// number of things that could go wrong is vast, but we don't
			// want to miss any which should in theory be retried with the
			// same client command ID. We log the error here as a warning so
			// there's visiblity that this is happening. Some of the errors
			// we'll sweep up in this net shouldn't be retried, but we can't
			// really know for sure which.
			log.Warningf("failed to send RPC request %s: %s", RPCMethod, err)
			continue
		}

		// On successful post, we're done with retry loop.
		break
	}
	return reply, err
}
