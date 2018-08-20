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

package kv

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

func (l *localTestClusterTransport) SendNext(ctx context.Context) (*roachpb.BatchResponse, error) {
	if l.latency > 0 {
		time.Sleep(l.latency)
	}
	return l.Transport.SendNext(ctx)
}

// InitFactoryForLocalTestCluster initializes a TxnCoordSenderFactory
// that can be used with LocalTestCluster.
func InitFactoryForLocalTestCluster(
	st *cluster.Settings,
	nodeDesc *roachpb.NodeDescriptor,
	tracer opentracing.Tracer,
	clock *hlc.Clock,
	latency time.Duration,
	stores client.Sender,
	stopper *stop.Stopper,
	gossip *gossip.Gossip,
) client.TxnSenderFactory {
	return NewTxnCoordSenderFactory(
		TxnCoordSenderFactoryConfig{
			AmbientCtx: log.AmbientContext{Tracer: st.Tracer},
			Settings:   st,
			Clock:      clock,
			Stopper:    stopper,
		},
		NewDistSenderForLocalTestCluster(st, nodeDesc, tracer, clock, latency, stores, stopper, gossip),
	)
}

// NewDistSenderForLocalTestCluster creates a DistSender for a LocalTestCluster.
func NewDistSenderForLocalTestCluster(
	st *cluster.Settings,
	nodeDesc *roachpb.NodeDescriptor,
	tracer opentracing.Tracer,
	clock *hlc.Clock,
	latency time.Duration,
	stores client.Sender,
	stopper *stop.Stopper,
	g *gossip.Gossip,
) *DistSender {
	retryOpts := base.DefaultRetryOptions()
	retryOpts.Closer = stopper.ShouldQuiesce()
	senderTransportFactory := SenderTransportFactory(tracer, stores)
	return NewDistSender(DistSenderConfig{
		AmbientCtx:      log.AmbientContext{Tracer: st.Tracer},
		Settings:        st,
		Clock:           clock,
		RPCRetryOptions: &retryOpts,
		nodeDescriptor:  nodeDesc,
		NodeDialer:      nodedialer.New(nil, gossip.AddressResolver(g)),
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: func(
				opts SendOptions,
				nodeDialer *nodedialer.Dialer,
				replicas ReplicaSlice,
				args roachpb.BatchRequest,
			) (Transport, error) {
				transport, err := senderTransportFactory(opts, nodeDialer, replicas, args)
				if err != nil {
					return nil, err
				}
				return &localTestClusterTransport{transport, latency}, nil
			},
		},
	}, g)
}
