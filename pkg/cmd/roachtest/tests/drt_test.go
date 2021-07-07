// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/logger"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/prometheus"
	gomock "github.com/golang/mock/gomock"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestTPCCChaosEventProcessor(t *testing.T) {
	ctx := context.Background()
	startTime := time.Date(2020, 12, 25, 0, 0, 0, 0, time.UTC)
	region1 := option.NodeListOption([]int{1, 2, 3})
	portRegion1 := 2110
	region2 := option.NodeListOption([]int{4, 5, 6})
	portRegion2 := 2111

	makeMetric := func(op string, errorOrSuccess string, port int) string {
		return fmt.Sprintf(`workload_tpcc_%s_%s_total{instance=":%d"}`, op, errorOrSuccess, port)
	}

	firstPreShutdown := startTime.Add(90 * time.Second)
	firstShutdownComplete := startTime.Add(95 * time.Second)
	scrapeAfterFirstShutdownComplete := firstShutdownComplete.Add(prometheus.DefaultScrapeInterval)
	firstPreStartup := startTime.Add(180 * time.Second)
	firstStartupComplete := startTime.Add(185 * time.Second)
	scrapeAfterFirstStartupComplete := firstStartupComplete.Add(prometheus.DefaultScrapeInterval)

	secondPreShutdown := startTime.Add(390 * time.Second)
	secondShutdownComplete := startTime.Add(395 * time.Second)
	scrapeAfterSecondShutdownComplete := secondShutdownComplete.Add(prometheus.DefaultScrapeInterval)
	secondPreStartup := startTime.Add(480 * time.Second)
	secondStartupComplete := startTime.Add(490 * time.Second)

	metricA := "newOtan"
	metricB := "otanLevel"

	type expectPromQuery struct {
		q      string
		t      time.Time
		retVal model.SampleValue
	}
	testCases := []struct {
		desc              string
		chaosEvents       []ChaosEvent
		ops               []string
		workloadInstances []workloadInstance
		mockPromQueries   []expectPromQuery
		expectedErrors    []string
	}{
		{
			desc: "everything is good",
			chaosEvents: []ChaosEvent{
				{Type: ChaosEventTypeStart, Time: startTime.Add(0 * time.Second)},

				// Shutdown and restart region1.
				{Type: ChaosEventTypePreShutdown, Time: firstPreShutdown, Target: region1},
				{Type: ChaosEventTypeShutdownComplete, Time: firstShutdownComplete, Target: region1},
				{Type: ChaosEventTypePreStartup, Time: firstPreStartup, Target: region1},
				{Type: ChaosEventTypeStartupComplete, Time: firstStartupComplete, Target: region1},

				// Shutdown and restart region2.
				{Type: ChaosEventTypePreShutdown, Time: secondPreShutdown, Target: region2},
				{Type: ChaosEventTypeShutdownComplete, Time: secondShutdownComplete, Target: region2},
				{Type: ChaosEventTypePreStartup, Time: secondPreStartup, Target: region2},
				{Type: ChaosEventTypeStartupComplete, Time: secondStartupComplete, Target: region2},

				{Type: ChaosEventTypeEnd, Time: startTime.Add(600 * time.Second)},
			},
			ops: []string{metricA, metricB},
			mockPromQueries: []expectPromQuery{
				// Restart region1.
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion1), t: firstPreStartup, retVal: 100},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 10},
				{q: makeMetric(metricA, "error", portRegion1), t: firstPreStartup, retVal: 1000},

				{q: makeMetric(metricA, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion2), t: firstPreStartup, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricA, "error", portRegion2), t: firstPreStartup, retVal: 0},

				{q: makeMetric(metricB, "success", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion1), t: firstPreStartup, retVal: 100},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 10},
				{q: makeMetric(metricB, "error", portRegion1), t: firstPreStartup, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion2), t: firstPreStartup, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricB, "error", portRegion2), t: firstPreStartup, retVal: 0},

				// Shutdown region2.
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion1), t: secondPreShutdown, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeAfterFirstStartupComplete, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion1), t: secondPreShutdown, retVal: 1000},

				{q: makeMetric(metricA, "success", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion2), t: secondPreShutdown, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: secondPreShutdown, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion1), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion1), t: secondPreShutdown, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeAfterFirstStartupComplete, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion1), t: secondPreShutdown, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion2), t: secondPreShutdown, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: secondPreShutdown, retVal: 1000},

				// Restart region2.
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricA, "success", portRegion1), t: secondPreStartup, retVal: 10000},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion1), t: secondPreStartup, retVal: 1000},

				{q: makeMetric(metricA, "success", portRegion2), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricA, "success", portRegion2), t: secondPreStartup, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: secondPreStartup, retVal: 10000},

				{q: makeMetric(metricB, "success", portRegion1), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricB, "success", portRegion1), t: secondPreStartup, retVal: 10000},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion1), t: secondPreStartup, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion2), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricB, "success", portRegion2), t: secondPreStartup, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: secondPreStartup, retVal: 10000},
			},
			workloadInstances: []workloadInstance{
				{
					nodes:          region1,
					prometheusPort: portRegion1,
				},
				{
					nodes:          region2,
					prometheusPort: portRegion2,
				},
			},
		},
		{
			desc: "unexpected node errors during shutdown",
			chaosEvents: []ChaosEvent{
				{Type: ChaosEventTypeStart, Time: startTime.Add(0 * time.Second)},

				// Shutdown and restart region1.
				{Type: ChaosEventTypePreShutdown, Time: firstPreShutdown, Target: region1},
				{Type: ChaosEventTypeShutdownComplete, Time: firstShutdownComplete, Target: region1},
				{Type: ChaosEventTypePreStartup, Time: firstPreStartup, Target: region1},
				{Type: ChaosEventTypeStartupComplete, Time: firstStartupComplete, Target: region1},

				{Type: ChaosEventTypeEnd, Time: startTime.Add(0 * time.Second)},
			},
			ops: []string{metricA, metricB},
			mockPromQueries: []expectPromQuery{
				// Restart region1.
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion1), t: firstPreStartup, retVal: 100},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 10},
				{q: makeMetric(metricA, "error", portRegion1), t: firstPreStartup, retVal: 1000},

				{q: makeMetric(metricA, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion2), t: firstPreStartup, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricA, "error", portRegion2), t: firstPreStartup, retVal: 0},

				{q: makeMetric(metricB, "success", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion1), t: firstPreStartup, retVal: 100},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 10},
				{q: makeMetric(metricB, "error", portRegion1), t: firstPreStartup, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion2), t: firstPreStartup, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricB, "error", portRegion2), t: firstPreStartup, retVal: 1000}, // should have had errors during shutdown.
			},
			workloadInstances: []workloadInstance{
				{
					nodes:          region1,
					prometheusPort: portRegion1,
				},
				{
					nodes:          region2,
					prometheusPort: portRegion2,
				},
			},
			expectedErrors: []string{
				fmt.Sprintf(
					`error at from 2020-12-25T00:01:45Z, to 2020-12-25T00:03:00Z on metric %s: expected 0 errors, found from 0.000000, to 1000.000000`,
					makeMetric(metricB, "error", portRegion2),
				),
			},
		},
		{
			desc: "unexpected node successes during shutdown",
			chaosEvents: []ChaosEvent{
				{Type: ChaosEventTypeStart, Time: startTime.Add(0 * time.Second)},

				// Shutdown and restart region1.
				{Type: ChaosEventTypePreShutdown, Time: firstPreShutdown, Target: region1},
				{Type: ChaosEventTypeShutdownComplete, Time: firstShutdownComplete, Target: region1},
				{Type: ChaosEventTypePreStartup, Time: firstPreStartup, Target: region1},
				{Type: ChaosEventTypeStartupComplete, Time: firstStartupComplete, Target: region1},

				{Type: ChaosEventTypeEnd, Time: startTime.Add(0 * time.Second)},
			},
			ops: []string{metricA, metricB},
			mockPromQueries: []expectPromQuery{
				// Restart region1.
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion1), t: firstPreStartup, retVal: 1000}, // should have had no successes whilst shutdown.

				{q: makeMetric(metricA, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion2), t: firstPreStartup, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricA, "error", portRegion2), t: firstPreStartup, retVal: 0},

				{q: makeMetric(metricB, "success", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion1), t: firstPreStartup, retVal: 100},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 10},
				{q: makeMetric(metricB, "error", portRegion1), t: firstPreStartup, retVal: 10}, // should have had errors whilst shutdown.

				{q: makeMetric(metricB, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion2), t: firstPreStartup, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricB, "error", portRegion2), t: firstPreStartup, retVal: 0},
			},
			workloadInstances: []workloadInstance{
				{
					nodes:          region1,
					prometheusPort: portRegion1,
				},
				{
					nodes:          region2,
					prometheusPort: portRegion2,
				},
			},
			expectedErrors: []string{
				fmt.Sprintf(
					`error at from 2020-12-25T00:01:45Z, to 2020-12-25T00:03:00Z on metric %s: expected successes to not increase, found from 100.000000, to 1000.000000`,
					makeMetric(metricA, "success", portRegion1),
				),
				fmt.Sprintf(
					`error at from 2020-12-25T00:01:45Z, to 2020-12-25T00:03:00Z on metric %s: expected errors, found from 10.000000, to 10.000000`,
					makeMetric(metricB, "error", portRegion1),
				),
			},
		},
		{
			desc: "nodes have unexpected blips after startup",
			chaosEvents: []ChaosEvent{
				{Type: ChaosEventTypeStart, Time: startTime.Add(0 * time.Second)},

				// Shutdown and restart region1.
				{Type: ChaosEventTypePreShutdown, Time: firstPreShutdown, Target: region1},
				{Type: ChaosEventTypeShutdownComplete, Time: firstShutdownComplete, Target: region1},
				{Type: ChaosEventTypePreStartup, Time: firstPreStartup, Target: region1},
				{Type: ChaosEventTypeStartupComplete, Time: firstStartupComplete, Target: region1},

				// Shutdown and restart region2.
				{Type: ChaosEventTypePreShutdown, Time: secondPreShutdown, Target: region2},
				{Type: ChaosEventTypeShutdownComplete, Time: secondShutdownComplete, Target: region2},
			},
			ops: []string{metricA, metricB},
			mockPromQueries: []expectPromQuery{
				// Restart region1.
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion1), t: firstPreStartup, retVal: 100},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 10},
				{q: makeMetric(metricA, "error", portRegion1), t: firstPreStartup, retVal: 1000},

				{q: makeMetric(metricA, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion2), t: firstPreStartup, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricA, "error", portRegion2), t: firstPreStartup, retVal: 0},

				{q: makeMetric(metricB, "success", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion1), t: firstPreStartup, retVal: 100},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 10},
				{q: makeMetric(metricB, "error", portRegion1), t: firstPreStartup, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion2), t: firstPreStartup, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricB, "error", portRegion2), t: firstPreStartup, retVal: 0},

				// Shutdown region2.
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion1), t: secondPreShutdown, retVal: 100}, // should have had successes as it was not the node shutdown.

				{q: makeMetric(metricA, "success", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion2), t: secondPreShutdown, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: secondPreShutdown, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion1), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion1), t: secondPreShutdown, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeAfterFirstStartupComplete, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion1), t: secondPreShutdown, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion2), t: secondPreShutdown, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: secondPreShutdown, retVal: 10000}, // unexpected errors during restart.
			},
			workloadInstances: []workloadInstance{
				{
					nodes:          region1,
					prometheusPort: portRegion1,
				},
				{
					nodes:          region2,
					prometheusPort: portRegion2,
				},
			},
			expectedErrors: []string{
				fmt.Sprintf(
					`error at from 2020-12-25T00:03:15Z, to 2020-12-25T00:06:30Z on metric %s: expected successes to be increasing, found from 100.000000, to 100.000000`,
					makeMetric(metricA, "success", portRegion1),
				),
				fmt.Sprintf(
					`error at from 2020-12-25T00:03:15Z, to 2020-12-25T00:06:30Z on metric %s: expected 0 errors, found from 1000.000000, to 10000.000000`,
					makeMetric(metricB, "error", portRegion2),
				),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ch := make(chan ChaosEvent)
			ep := tpccChaosEventProcessor{
				workloadInstances: tc.workloadInstances,
				ops:               tc.ops,
				ch:                ch,
				promClient: func(ctrl *gomock.Controller) promClient {
					c := NewMockpromClient(ctrl)
					e := c.EXPECT()
					for _, m := range tc.mockPromQueries {
						e.Query(ctx, m.q, m.t).Return(
							model.Value(model.Vector{&model.Sample{Value: m.retVal}}),
							nil,
							nil,
						)
					}
					return c
				}(ctrl),
			}

			l, err := (&logger.Config{}).NewLogger("")
			require.NoError(t, err)

			ep.listen(ctx, l)
			for _, chaosEvent := range tc.chaosEvents {
				ch <- chaosEvent
			}
			close(ch)

			if len(tc.expectedErrors) == 0 {
				require.NoError(t, ep.err())
			} else {
				require.Error(t, ep.err())
				// The first error found should be exposed.
				require.EqualError(t, ep.err(), tc.expectedErrors[0])
				// Check each other combined error matches.
				require.Len(t, ep.errs, len(tc.expectedErrors))
				for i, err := range ep.errs {
					require.EqualError(t, err, tc.expectedErrors[i])
				}
			}
		})
	}
}
