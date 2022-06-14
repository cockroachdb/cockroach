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

// GetSpanStatistics implements the SpanStatsServer interface.
func (s *spanStatsServer) GetSpanStatistics(
	ctx context.Context, req *serverpb.GetSpanStatisticsRequest,
) (*serverpb.GetSpanStatisticsResponse, error) {

	res := serverpb.GetSpanStatisticsResponse{Samples: make([]*serverpb.Sample, 0)}

	type uniqueStat struct {
		sp            *roachpb.Span
		startPretty   string
		endPretty     string
		batchRequests uint64
	}

	uniqueStats := make(map[string]uniqueStat)

	if err := s.server.node.stores.VisitStores(func(st *kvserver.Store) error {

		// visit the spanStatHistogram buckets on this store
		// accumulate each bucket's value.
		st.VisitSpanStatsBuckets(func(span *roachpb.Span, batchRequests uint64) {
			spanAsString := span.String()
			if stat, ok := uniqueStats[spanAsString]; ok {
				stat.batchRequests += batchRequests
			} else {
				uniqueStats[spanAsString] = uniqueStat{
					sp:            span,
					startPretty:   span.Key.String(),
					endPretty:     span.EndKey.String(),
					batchRequests: batchRequests,
				}
			}
		})

		return nil
	}); err != nil {
		return nil, err
	}

	// convert uniqueStats into a `GetSpanStatisticsResponse`
	stats := make([]*serverpb.SpanStatistics, 0)

	for _, value := range uniqueStats {
		stats = append(stats, &serverpb.SpanStatistics{
			Pretty: &serverpb.SpanStatistics_SpanPretty{
				StartKey: value.startPretty,
				EndKey:   value.endPretty,
			},
			Span:          value.sp,
			BatchRequests: value.batchRequests,
		})
	}

	t := hlc.Timestamp{WallTime: time.Now().UnixNano()}
	res.Samples = append(res.Samples, &serverpb.Sample{SampleTime: &t, SpanStats: stats})

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
