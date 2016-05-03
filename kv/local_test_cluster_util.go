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

	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/stop"
)

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
	var rpcSend rpcSendFn = func(_ SendOptions, _ ReplicaSlice,
		args roachpb.BatchRequest, _ *rpc.Context) (*roachpb.BatchResponse, error) {
		if latency > 0 {
			time.Sleep(latency)
		}
		sp := tracer.StartSpan("node")
		defer sp.Finish()
		ctx := opentracing.ContextWithSpan(context.Background(), sp)
		log.Trace(ctx, args.String())
		br, pErr := stores.Send(ctx, args)
		if br == nil {
			br = &roachpb.BatchResponse{}
		}
		if br.Error != nil {
			panic(roachpb.ErrorUnexpectedlySet(stores, br))
		}
		br.Error = pErr
		if pErr != nil {
			log.Trace(ctx, "error: "+pErr.String())
		}
		return br, nil
	}
	retryOpts := GetDefaultDistSenderRetryOptions()
	retryOpts.Closer = stopper.ShouldDrain()
	distSender := NewDistSender(&DistSenderContext{
		Clock: clock,
		RangeDescriptorCacheSize: defaultRangeDescriptorCacheSize,
		RangeLookupMaxRanges:     defaultRangeLookupMaxRanges,
		LeaderCacheSize:          defaultLeaderCacheSize,
		RPCRetryOptions:          &retryOpts,
		nodeDescriptor:           nodeDesc,
		RPCSend:                  rpcSend,                    // defined above
		RangeDescriptorDB:        stores.(RangeDescriptorDB), // for descriptor lookup
	}, gossip)

	return NewTxnCoordSender(distSender, clock, false /* !linearizable */, tracer,
		stopper, NewTxnMetrics(metric.NewRegistry()))
}
