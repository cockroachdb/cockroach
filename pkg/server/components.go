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

package server

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TODO(spencer): store these values to temporary non-volatile storage.
type tracesStorage struct {
	syncutil.Mutex
	traces map[uint64]map[string]tracing.ComponentTraces
}

var tracesMap = tracesStorage{
	traces: map[uint64]map[string]tracing.ComponentTraces{},
}

func storeTraces(sampleTracesID uint64, traces map[string]tracing.ComponentTraces) {
	tracesMap.Lock()
	defer tracesMap.Unlock()
	tracesMap.traces[sampleTracesID] = traces
}

func loadTracesByComponent(sampleTracesID uint64, component string) tracing.ComponentTraces {
	tracesMap.Lock()
	defer tracesMap.Unlock()
	return tracesMap.traces[sampleTracesID][component]
}

func loadSamplesByTraceID(sampleTracesID, traceID uint64) map[string]tracing.ComponentSamples {
	tracesMap.Lock()
	defer tracesMap.Unlock()
	samplesMap := map[string][]tracing.ComponentSamples_Sample{}
	for k, v := range tracesMap.traces[sampleTracesID] {
		for _, s := range v.Samples {
			if s.Spans[0].TraceID == traceID {
				samplesMap[k] = append(samplesMap[k], s)
			}
		}
	}
	result := map[string]tracing.ComponentSamples{}
	for k, v := range samplesMap {
		result[k] = tracing.ComponentSamples{Samples: v}
	}
	return result
}

func (s *statusServer) Components(
	ctx context.Context, req *serverpb.ComponentsRequest,
) (*serverpb.ComponentsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	localReq := *req
	localReq.NodeID = "local"

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		if local {
			return &serverpb.ComponentsResponse{
				Nodes: []serverpb.ComponentsResponse_NodeResponse{
					{
						NodeID:     s.gossip.NodeID.Get(),
						StartTime:  s.admin.server.startTime,
						Components: tracing.GetComponentActivity(),
					},
				},
			}, nil
		}
		status, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}
		return status.Components(ctx, &localReq)
	}

	response := &serverpb.ComponentsResponse{}

	if err := s.iterateNodes(ctx, fmt.Sprintf("component activity for node %s", req.NodeID),
		func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
			client, err := s.dialNode(ctx, nodeID)
			return client, err
		},
		func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
			status := client.(serverpb.StatusClient)
			return status.Components(ctx, &localReq)
		},
		func(nodeID roachpb.NodeID, resp interface{}) {
			componentsResp := resp.(*serverpb.ComponentsResponse)
			response.Nodes = append(response.Nodes, componentsResp.Nodes...)
		},
		func(nodeID roachpb.NodeID, err error) {
			response.Nodes = append(response.Nodes, serverpb.ComponentsResponse_NodeResponse{
				NodeID:       nodeID,
				ErrorMessage: err.Error(),
			})
		},
	); err != nil {
		return nil, err
	}

	return response, nil
}

func (s *statusServer) SampleTraces(
	ctx context.Context, req *serverpb.SampleTracesRequest,
) (*serverpb.SampleTracesResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	localReq := *req
	localReq.NodeID = "local"
	for localReq.SampleTracesID == 0 {
		localReq.SampleTracesID = uint64(rand.Int63())
	}

	response := &serverpb.SampleTracesResponse{
		Nodes:          []serverpb.SampleTracesResponse_NodeResponse{{NodeID: s.gossip.NodeID.Get()}},
		SampleTracesID: localReq.SampleTracesID,
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		if local {
			storeTraces(localReq.SampleTracesID, tracing.Record(time.After(req.Duration), req.TargetCount))
			return response, nil
		}
		status, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}
		return status.SampleTraces(ctx, &localReq)
	}

	if err := s.iterateNodes(ctx, fmt.Sprintf("sample traces for node %s", req.NodeID),
		func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
			client, err := s.dialNode(ctx, nodeID)
			return client, err
		},
		func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
			status := client.(serverpb.StatusClient)
			return status.SampleTraces(ctx, &localReq)
		},
		func(nodeID roachpb.NodeID, resp interface{}) {
			sampleTracesResp := resp.(*serverpb.SampleTracesResponse)
			response.Nodes = append(response.Nodes, sampleTracesResp.Nodes...)
		},
		func(nodeID roachpb.NodeID, err error) {
			response.Nodes = append(response.Nodes, serverpb.SampleTracesResponse_NodeResponse{
				NodeID:       nodeID,
				ErrorMessage: err.Error(),
			})
		},
	); err != nil {
		return nil, err
	}

	return response, nil
}

func (s *statusServer) ComponentTraces(
	ctx context.Context, req *serverpb.ComponentTracesRequest,
) (*serverpb.ComponentTracesResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if len(req.NodeID) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "node ID not specified")
	}
	if req.SampleTracesID == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "sample traces id not specified")
	}

	requestedNodeID, local, err := s.parseNodeID(req.NodeID)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	if local {
		resp := &serverpb.ComponentTracesResponse{
			Traces: map[string]tracing.ComponentTraces{},
		}
		for _, component := range req.Components {
			resp.Traces[component] = loadTracesByComponent(req.SampleTracesID, component)
		}
		return resp, nil
	}
	status, err := s.dialNode(ctx, requestedNodeID)
	if err != nil {
		return nil, err
	}
	localReq := *req
	localReq.NodeID = "local"
	return status.ComponentTraces(ctx, &localReq)
}

func (s *statusServer) ComponentTrace(
	ctx context.Context, req *serverpb.ComponentTraceRequest,
) (*serverpb.ComponentTraceResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	localReq := *req
	localReq.NodeID = "local"

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		if local {
			return &serverpb.ComponentTraceResponse{
				Nodes: []serverpb.ComponentTraceResponse_NodeResponse{
					{
						NodeID:  s.gossip.NodeID.Get(),
						Samples: loadSamplesByTraceID(req.SampleTracesID, req.TraceID),
					},
				},
			}, nil
		}
		status, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}
		return status.ComponentTrace(ctx, &localReq)
	}

	response := &serverpb.ComponentTraceResponse{}

	if err := s.iterateNodes(ctx, fmt.Sprintf("component trace %d for node %s", req.TraceID, req.NodeID),
		func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
			client, err := s.dialNode(ctx, nodeID)
			return client, err
		},
		func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
			status := client.(serverpb.StatusClient)
			return status.ComponentTrace(ctx, &localReq)
		},
		func(nodeID roachpb.NodeID, resp interface{}) {
			sampleTracesResp := resp.(*serverpb.ComponentTraceResponse)
			response.Nodes = append(response.Nodes, sampleTracesResp.Nodes...)
		},
		func(nodeID roachpb.NodeID, err error) {
			response.Nodes = append(response.Nodes, serverpb.ComponentTraceResponse_NodeResponse{
				NodeID:       nodeID,
				ErrorMessage: err.Error(),
			})
		},
	); err != nil {
		return nil, err
	}

	return response, nil
}
