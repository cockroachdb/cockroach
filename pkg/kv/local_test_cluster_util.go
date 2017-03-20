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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	opentracing "github.com/opentracing/opentracing-go"
)

// localTestClusterTransport augments senderTransport with an optional
// delay for each RPC, to simulate latency for benchmarking.
// TODO(bdarnell): there's probably a better place to put this.
type localTestClusterTransport struct {
	Transport
	latency time.Duration
}

func (l *localTestClusterTransport) SendNext(ctx context.Context, done chan<- BatchCall) {
	if l.latency > 0 {
		time.Sleep(l.latency)
	}
	l.Transport.SendNext(ctx, done)
}

// InitSenderForLocalTestCluster initializes a TxnCoordSender that can be used
// with LocalTestCluster.
func InitSenderForLocalTestCluster(
	nodeDesc *roachpb.NodeDescriptor,
	tracer opentracing.Tracer,
	clock *hlc.Clock,
	latency time.Duration,
	stores client.Sender,
	stopper *stop.Stopper,
	gossip *gossip.Gossip,
) client.Sender {
	retryOpts := base.DefaultRetryOptions()
	retryOpts.Closer = stopper.ShouldQuiesce()
	senderTransportFactory := SenderTransportFactory(tracer, stores)
	distSender := NewDistSender(DistSenderConfig{
		Clock:           clock,
		RPCRetryOptions: &retryOpts,
		nodeDescriptor:  nodeDesc,
		TransportFactory: func(
			opts SendOptions,
			rpcContext *rpc.Context,
			replicas ReplicaSlice,
			args roachpb.BatchRequest,
		) (Transport, error) {
			transport, err := senderTransportFactory(opts, rpcContext, replicas, args)
			if err != nil {
				return nil, err
			}
			return &localTestClusterTransport{transport, latency}, nil
		},
	}, gossip)

	ambient := log.AmbientContext{Tracer: tracer}
	return NewTxnCoordSender(
		ambient,
		distSender,
		clock,
		false, /* !linearizable */
		stopper,
		MakeTxnMetrics(metric.TestSampleInterval),
	)
}
