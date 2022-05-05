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
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var cache = serverpb.HistoricalHotRangesResponse{Samples: nil}

// GetHistoricalHotRangesCached returns cached historical hot ranges
func GetHistoricalHotRangesCached() (*serverpb.HistoricalHotRangesResponse, error) {
	if len(cache.Samples) == 0 {
		return nil, fmt.Errorf("no historical hot ranges available yet")
	}
	return &cache, nil
}

// collectHistoricalHotRanges periodically (once per 15 seconds) queries hot ranges and preserves to local cache
func (s *Server) collectHistoricalHotRanges() error {
	ctx := s.AnnotateCtx(context.Background())
	ticker := time.NewTicker(15 * time.Second)
	return s.stopper.RunAsyncTask(ctx, "collect-historical-hot-ranges", func(ctx context.Context) {
		for {
			select {
			case t := <-ticker.C:
				// TODO: request multiple times because of pagination
				resp, err := s.status.HotRangesV2(ctx, &serverpb.HotRangesRequest{})
				if err != nil {
					log.Warningf(ctx, "status.HotRangesV2 request failed: %+v", err)
				}
				hhr := make([]*serverpb.HistoricalHotRangesResponse_HHRSample_HotRange, len(resp.Ranges))
				for i, hr := range resp.Ranges {
					hhr[i] = &serverpb.HistoricalHotRangesResponse_HHRSample_HotRange{
						RangeID:   hr.RangeID,
						ReplicaId: hr.LeaseholderNodeID,
						Qps:       hr.QPS,
						StartKey:  hr.StartKey.String(),
						EndKey:    hr.EndKey.String(),
						NodeIds:   hr.ReplicaNodeIds,
						StoreIds:  []roachpb.StoreID{hr.StoreID},
						Schema: []*serverpb.HistoricalHotRangesResponse_HHRSample_HotRange_Schema{
							{
								Database: hr.DatabaseName,
								Tables:   []string{hr.TableName},
							},
						},
						Locality: "", // it can be computed on front-end side
						KeyBytes: 0,  // TODO: don't know how to get this info
					}
				}
				sample := &serverpb.HistoricalHotRangesResponse_HHRSample{
					Hotranges: hhr,
				}
				const samplesPerTwoWeeks = 1344
				// clean up old cached data to keep only most recent records for "2 weeks" time window
				if len(cache.Samples) >= samplesPerTwoWeeks {
					cache.Samples = cache.Samples[len(cache.Samples)-samplesPerTwoWeeks+1:]
				}
				cache.Samples = append(cache.Samples, sample)
				// update timestamps to most recent dates, it emulates moving time window.
				for i := len(cache.Samples) - 1; i >= 0; i-- {
					cache.Samples[i].Timestamp = t
					t = t.Add(-15 * time.Minute)
				}
			case <-s.stopper.ShouldQuiesce():
				return
			}
		}
	})
}
