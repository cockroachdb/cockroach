// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/gogo/protobuf/jsonpb"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
)

var _ serverpb.SpanStatsServer = &spanStatsServer{}

type spanStatsServer struct {
	server *Server
}

func (s *spanStatsServer) SetSpanBoundaries(ctx context.Context, request *serverpb.SetSpanBoundariesRequest) (*serverpb.SetSpanBoundariesResponse, error) {
	s.server.node.stores.VisitStores(func(st *kvserver.Store) error {
		st.SetBucketBoundaries()
		return nil
	})
	return &serverpb.SetSpanBoundariesResponse{}, nil
}

func (s *spanStatsServer) RegisterService(g *grpc.Server) {
	serverpb.RegisterSpanStatsServer(g, s)
}

// RegisterGateway starts the gateway (i.e. reverse proxy) that proxies HTTP requests
// to the appropriate gRPC endpoints.
func (s *spanStatsServer) RegisterGateway(
	ctx context.Context, mux *gwruntime.ServeMux, conn *grpc.ClientConn,
) error {
	return serverpb.RegisterSpanStatsHandler(ctx, mux, conn)
}

func (s *spanStatsServer) dialNode(ctx context.Context, nodeID roachpb.NodeID) (serverpb.SpanStatsClient, error) {
	addr, err := s.server.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		return nil, err
	}
	log.Infof(ctx, "dial node %d with addr: %s", nodeID, addr.String())
	rpcConnection := s.server.rpcContext.GRPCDialNode(addr.String(), nodeID, rpc.DefaultClass)
	log.Infof(ctx, "Have connection for node %d", nodeID)
	conn, err := rpcConnection.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return serverpb.NewSpanStatsClient(conn), nil
}

type uniqueBucket struct {
	sp            *roachpb.Span
	batchRequests uint64
}

func (s *spanStatsServer) GetNodeSpanStats(
	ctx context.Context, req *serverpb.GetNodeSpanStatsRequest,
) (*serverpb.GetNodeSpanStatsResponse, error) {

	uniqueBuckets := make(map[string]uniqueBucket)

	if err := s.server.node.stores.VisitStores(func(store *kvserver.Store) error {
		store.VisitSpanStatsBuckets(func(span *roachpb.Span, batchRequests uint64) {
			spanAsString := span.String()
			if stat, ok := uniqueBuckets[spanAsString]; ok {
				stat.batchRequests += batchRequests
			} else {
				uniqueBuckets[spanAsString] = uniqueBucket{
					sp:            span,
					batchRequests: batchRequests,
				}
			}
		})
		return nil
	}); err != nil {
		return nil, err
	}

	res := serverpb.GetNodeSpanStatsResponse{}
	res.Stats = make([]*serverpb.GetNodeSpanStatsResponse_NodeSpanStat, 0)

	for _, bucket := range uniqueBuckets {
		res.Stats = append(res.Stats, &serverpb.GetNodeSpanStatsResponse_NodeSpanStat{
			Span:          bucket.sp,
			BatchRequests: bucket.batchRequests,
		})
	}

	return &res, nil
}

// GetSpanStatistics implements the SpanStatsServer interface.
// Get span statistics from all stores on this node.
// this will be called by the tenant's job.
func (s *spanStatsServer) GetSpanStatistics(
	ctx context.Context, req *serverpb.GetSpanStatisticsRequest,
) (*serverpb.GetSpanStatisticsResponse, error) {

	responsesByNodeID := make(map[roachpb.NodeID]serverpb.GetNodeSpanStatsResponse)

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}

	nodeFn := func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (interface{}, error) {
		spanStats := client.(serverpb.SpanStatsClient)
		// TODO: update bucket boundaries before returning.
		// TODO: use a time window.
		nodeSpanStats, err := spanStats.GetNodeSpanStats(ctx, &serverpb.GetNodeSpanStatsRequest{})
		// TODO check for error
		spanStats.SetSpanBoundaries(ctx, &serverpb.SetSpanBoundariesRequest{})
		return nodeSpanStats, err
	}

	responseFn := func(nodeID roachpb.NodeID, resp interface{}) {
		// TODO: this name is too similar to `SpanStatsRequest` and `SpanStatsResponse`
		nodeResponse := resp.(*serverpb.GetNodeSpanStatsResponse)
		responsesByNodeID[nodeID] = *nodeResponse
	}

	errorFn := func(nodeID roachpb.NodeID, err error) {
		log.Errorf(ctx, "COULD NOT GET SPAN STATS FOR NODE %d: %s", nodeID, err.Error())
	}

	err := s.server.status.iterateNodes(ctx, "iterating nodes for span stats", dialFn, nodeFn, responseFn, errorFn)

	if err != nil {
		panic("could not iterate nodes")
	}

	res := serverpb.GetSpanStatisticsResponse{}
	uniqueBuckets := make(map[string]uniqueBucket)

	// aggregate buckets across nodes
	for _, nodeResponse := range responsesByNodeID {
		for _, bucket := range nodeResponse.Stats {
			spanAsString := bucket.Span.String()
			if b, ok := uniqueBuckets[spanAsString]; ok {
				b.batchRequests += bucket.BatchRequests
			} else {
				uniqueBuckets[spanAsString] = uniqueBucket{
					sp:            bucket.Span,
					batchRequests: bucket.BatchRequests,
				}
			}
		}
	}

	// build response
	spanStats := make([]*serverpb.SpanStatistics, 0)
	for _, bucket := range uniqueBuckets {
		spanStats = append(spanStats, &serverpb.SpanStatistics{
			Pretty: &serverpb.SpanStatistics_SpanPretty{
				StartKey: bucket.sp.Key.String(),
				EndKey:   bucket.sp.EndKey.String(),
			},
			Span:          bucket.sp,
			BatchRequests: bucket.batchRequests,
		})
	}

	// TODO: proto namespace
	res.Samples = make([]*serverpb.Sample, 0)
	res.Samples = append(res.Samples, &serverpb.Sample{
		SampleTime: &hlc.Timestamp{WallTime: time.Now().UnixNano()},
		SpanStats:  spanStats,
	})

	return &res, nil
}

func loadSamples(ctx context.Context) []*serverpb.Sample {
	readPath := "./key-visualizer-read/"
	samples := make([]*serverpb.Sample, 0)
	fileNames, err := ioutil.ReadDir(readPath)

	if err != nil {
		log.Fatal(ctx, "could not read key-visualizer-data/")
	}

	for _, fileName := range fileNames {
		fName := fileName.Name()
		if !strings.Contains(fName, ".json") {
			continue
		}

		file, err := os.Open(fmt.Sprintf("%s%s", readPath, fName))

		if err != nil {
			fmt.Println(err)
			panic("could not load file!")
		}

		// build a string from the file contents
		var buf bytes.Buffer
		io.Copy(&buf, file)
		fileAsString := string(buf.Bytes())

		p := serverpb.GetSpanStatisticsResponse{}
		jsonpb.UnmarshalString(fileAsString, &p)

		samples = append(samples, p.Samples[0])
		file.Close()
	}

	return samples
}

// The visualization requires a lexicographically sorted set of keys referenced by all samples.
func buildKeyspace(samples []*serverpb.Sample) []string {

	uniqueKeys := map[string]bool{}
	prettyForEncoded := map[string]string{}

	for _, sample := range samples {
		for _, stat := range sample.SpanStats {

			start := string(stat.Span.Key)
			end := string(stat.Span.EndKey)

			prettyForEncoded[start] = stat.Pretty.StartKey
			prettyForEncoded[end] = stat.Pretty.EndKey

			uniqueKeys[start] = true
			uniqueKeys[end] = true
		}
	}

	uniqueKeysSlice := []string{}
	for key := range uniqueKeys {
		uniqueKeysSlice = append(uniqueKeysSlice, key)
	}

	sort.Strings(uniqueKeysSlice)

	keys := []string{}
	for _, key := range uniqueKeysSlice {
		keys = append(keys, prettyForEncoded[key])
	}

	return keys
}

func (s *spanStatsServer) GetSamples(ctx context.Context, req *serverpb.GetSamplesRequest) (*serverpb.GetSamplesResponse, error) {

	samples := loadSamples(ctx)
	keys := buildKeyspace(samples)

	res := serverpb.GetSamplesResponse{
		Samples: samples,
		Keys:    keys,
	}

	return &res, nil
}
