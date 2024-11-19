// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"sort"
	"strings"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvissettings"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// KeyVisualizerServer is a concrete implementation of the keyvispb.KeyVisualizerServer interface.
type KeyVisualizerServer struct {
	ie           *sql.InternalExecutor
	settings     *cluster.Settings
	kvNodeDialer *nodedialer.Dialer
	status       *systemStatusServer
	node         *Node
}

var _ keyvispb.KeyVisualizerServer = &KeyVisualizerServer{}

func (s *KeyVisualizerServer) saveBoundaries(
	ctx context.Context, req *keyvispb.UpdateBoundariesRequest,
) error {
	encoded, err := protoutil.Marshal(req)

	if err != nil {
		return err
	}
	// Nodes are notified about boundary changes via the keyvissubscriber.BoundarySubscriber.
	_, err = s.ie.ExecEx(
		ctx,
		"upsert tenant boundaries",
		nil,
		sessiondata.NodeUserSessionDataOverride,
		`UPSERT INTO system.span_stats_tenant_boundaries(
			tenant_id,
			boundaries
			) VALUES ($1, $2)
		`,
		roachpb.SystemTenantID.ToUint64(),
		encoded,
	)

	return err
}

func (s *KeyVisualizerServer) getSamplesFromFanOut(
	ctx context.Context, timestamp time.Time,
) (*keyvispb.GetSamplesResponse, error) {

	samplePeriod := keyvissettings.SampleInterval.Get(&s.settings.SV)

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		conn, err := s.kvNodeDialer.Dial(ctx, nodeID, rpc.DefaultClass)
		return keyvispb.NewKeyVisualizerClient(conn), err
	}

	nodeFn := func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (interface{}, error) {
		samples, err := client.(keyvispb.KeyVisualizerClient).GetSamples(ctx,
			&keyvispb.GetSamplesRequest{
				NodeID:             nodeID,
				CollectedOnOrAfter: timestamp,
			})
		if err != nil {
			return nil, err
		}
		return samples, err
	}

	globalSamples := make(map[int64][]keyvispb.Sample)

	responseFn := func(nodeID roachpb.NodeID, resp interface{}) {
		nodeResponse := resp.(*keyvispb.GetSamplesResponse)

		// Collection is spread across each node, so samples that belong to the
		// same sample period should be aggregated together.
		for _, sampleFragment := range nodeResponse.Samples {
			tNanos := sampleFragment.SampleTime.Truncate(samplePeriod).UnixNano()
			globalSamples[tNanos] = append(globalSamples[tNanos], sampleFragment)
		}
	}

	errorFn := func(nodeID roachpb.NodeID, err error) {
		log.Errorf(ctx, "could not get key visualizer sample for node %d: %v",
			nodeID, err)
	}

	err := iterateNodes(ctx,
		s.status.serverIterator,
		s.status.stopper,
		"iterating nodes for key visualizer samples",
		noTimeout,
		dialFn,
		nodeFn,
		responseFn,
		errorFn,
	)
	if err != nil {
		return nil, err
	}

	var samples []keyvispb.Sample
	for sampleTimeNanos, sampleFragments := range globalSamples {
		if !verifySampleBoundariesEqual(sampleFragments) {
			log.Warningf(ctx, "key visualizer sample boundaries differ between nodes")
		}
		samples = append(samples, keyvispb.Sample{
			SampleTime: timeutil.Unix(0, sampleTimeNanos),
			SpanStats:  cumulativeStats(sampleFragments),
		})
	}

	return &keyvispb.GetSamplesResponse{Samples: samples}, nil
}

// verifySampleBoundariesEqual returns true if all the samples collected
// from across the cluster belonging to the same sample period have identical
// spans.
func verifySampleBoundariesEqual(fragments []keyvispb.Sample) bool {
	f0 := fragments[0]
	sort.Slice(f0.SpanStats, func(a, b int) bool {
		return f0.SpanStats[a].Span.Key.Compare(f0.SpanStats[b].Span.Key) == -1
	})

	for i := 1; i < len(fragments); i++ {
		fi := fragments[i]

		if len(f0.SpanStats) != len(fi.SpanStats) {
			return false
		}

		sort.Slice(fi.SpanStats, func(a, b int) bool {
			return fi.SpanStats[a].Span.Key.Compare(fi.SpanStats[b].Span.Key) == -1
		})

		for b, bucket := range f0.SpanStats {
			if !bucket.Span.Equal(fi.SpanStats[b].Span) {
				return false
			}
		}
	}

	return true
}

// unsafeBytesToString constructs a string from a byte slice. It is
// critical that the byte slice not be modified.
func unsafeBytesToString(data []byte) string {
	return *(*string)(unsafe.Pointer(&data))
}

// cumulativeStats uniques and accumulates all of a sample's
// keyvispb.SpanStats from across the cluster. Stores collect statistics for
// the same spans, and the caller wants the cumulative statistics for those spans.
func cumulativeStats(fragments []keyvispb.Sample) []keyvispb.SpanStats {
	var stats []keyvispb.SpanStats
	for _, sampleFragment := range fragments {
		stats = append(stats, sampleFragment.SpanStats...)
	}

	unique := make(map[string]keyvispb.SpanStats)
	for _, stat := range stats {

		var sb strings.Builder
		sb.WriteString(unsafeBytesToString(stat.Span.Key))
		sb.WriteString(unsafeBytesToString(stat.Span.EndKey))
		spanAsString := sb.String()

		if uniqueStat, ok := unique[spanAsString]; ok {
			uniqueStat.Requests += stat.Requests
		} else {
			unique[spanAsString] = keyvispb.SpanStats{
				Span:     stat.Span,
				Requests: stat.Requests,
			}
		}
	}

	ret := make([]keyvispb.SpanStats, 0, len(unique))
	for _, stat := range unique {
		ret = append(ret, stat)
	}
	return ret
}

// GetSamples implements the keyvispb.KeyVisualizerServer interface.
func (s *KeyVisualizerServer) GetSamples(
	ctx context.Context, req *keyvispb.GetSamplesRequest,
) (*keyvispb.GetSamplesResponse, error) {

	if req.NodeID == 0 {
		return s.getSamplesFromFanOut(ctx, req.CollectedOnOrAfter)
	}

	samples := s.node.spanStatsCollector.GetSamples(
		req.CollectedOnOrAfter)

	return &keyvispb.GetSamplesResponse{Samples: samples}, nil
}

// UpdateBoundaries implements the keyvispb.KeyVisualizerServer interface.
func (s *KeyVisualizerServer) UpdateBoundaries(
	ctx context.Context, req *keyvispb.UpdateBoundariesRequest,
) (*keyvispb.UpdateBoundariesResponse, error) {
	if err := s.saveBoundaries(ctx, req); err != nil {
		return nil, err
	}
	return &keyvispb.UpdateBoundariesResponse{}, nil
}
