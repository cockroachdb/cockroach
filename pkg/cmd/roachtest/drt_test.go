// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/logger"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	gomock "github.com/golang/mock/gomock"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestTPCCChaosEventProcessor(t *testing.T) {
	ctx := context.Background()
	startTime := time.Date(2020, 12, 25, 0, 0, 0, 0, time.UTC)
	region1 := option.NodeListOption([]int{1, 2, 3})
	region2 := option.NodeListOption([]int{4, 5, 6})

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
				{Type: ChaosEventTypePreShutdown, Time: startTime.Add(90 * time.Second), Target: region1},
				{Type: ChaosEventTypeShutdownComplete, Time: startTime.Add(95 * time.Second), Target: region1},
				{Type: ChaosEventTypePreStartup, Time: startTime.Add(180 * time.Second), Target: region1},
				{Type: ChaosEventTypeStartupComplete, Time: startTime.Add(185 * time.Second), Target: region1},

				// Shutdown and restart region2.
				{Type: ChaosEventTypePreShutdown, Time: startTime.Add(290 * time.Second), Target: region2},
				{Type: ChaosEventTypeShutdownComplete, Time: startTime.Add(295 * time.Second), Target: region2},
				{Type: ChaosEventTypePreStartup, Time: startTime.Add(380 * time.Second), Target: region2},
				{Type: ChaosEventTypeStartupComplete, Time: startTime.Add(385 * time.Second), Target: region2},

				{Type: ChaosEventTypeEnd, Time: startTime.Add(0 * time.Second)},
			},
			ops: []string{"newOtan", "otanLevel"},
			mockPromQueries: []expectPromQuery{
				// Restart region1.
				{q: `workload_tpcc_newOtan_success_total{instance=":2110"}`, t: startTime.Add(105 * time.Second), retVal: 100},
				{q: `workload_tpcc_newOtan_success_total{instance=":2110"}`, t: startTime.Add(180 * time.Second), retVal: 100},
				{q: `workload_tpcc_newOtan_error_total{instance=":2110"}`, t: startTime.Add(105 * time.Second), retVal: 10},
				{q: `workload_tpcc_newOtan_error_total{instance=":2110"}`, t: startTime.Add(180 * time.Second), retVal: 1000},

				{q: `workload_tpcc_newOtan_success_total{instance=":2111"}`, t: startTime.Add(105 * time.Second), retVal: 100},
				{q: `workload_tpcc_newOtan_success_total{instance=":2111"}`, t: startTime.Add(180 * time.Second), retVal: 1000},
				{q: `workload_tpcc_newOtan_error_total{instance=":2111"}`, t: startTime.Add(105 * time.Second), retVal: 0},
				{q: `workload_tpcc_newOtan_error_total{instance=":2111"}`, t: startTime.Add(180 * time.Second), retVal: 0},

				{q: `workload_tpcc_otanLevel_success_total{instance=":2110"}`, t: startTime.Add(105 * time.Second), retVal: 100},
				{q: `workload_tpcc_otanLevel_success_total{instance=":2110"}`, t: startTime.Add(180 * time.Second), retVal: 100},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2110"}`, t: startTime.Add(105 * time.Second), retVal: 10},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2110"}`, t: startTime.Add(180 * time.Second), retVal: 1000},

				{q: `workload_tpcc_otanLevel_success_total{instance=":2111"}`, t: startTime.Add(105 * time.Second), retVal: 100},
				{q: `workload_tpcc_otanLevel_success_total{instance=":2111"}`, t: startTime.Add(180 * time.Second), retVal: 1000},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2111"}`, t: startTime.Add(105 * time.Second), retVal: 0},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2111"}`, t: startTime.Add(180 * time.Second), retVal: 0},

				// Shutdown region2.
				{q: `workload_tpcc_newOtan_success_total{instance=":2110"}`, t: startTime.Add(195 * time.Second), retVal: 100},
				{q: `workload_tpcc_newOtan_success_total{instance=":2110"}`, t: startTime.Add(290 * time.Second), retVal: 1000},
				{q: `workload_tpcc_newOtan_error_total{instance=":2110"}`, t: startTime.Add(195 * time.Second), retVal: 1000},
				{q: `workload_tpcc_newOtan_error_total{instance=":2110"}`, t: startTime.Add(290 * time.Second), retVal: 1000},

				{q: `workload_tpcc_newOtan_success_total{instance=":2111"}`, t: startTime.Add(195 * time.Second), retVal: 100},
				{q: `workload_tpcc_newOtan_success_total{instance=":2111"}`, t: startTime.Add(290 * time.Second), retVal: 1000},
				{q: `workload_tpcc_newOtan_error_total{instance=":2111"}`, t: startTime.Add(195 * time.Second), retVal: 1000},
				{q: `workload_tpcc_newOtan_error_total{instance=":2111"}`, t: startTime.Add(290 * time.Second), retVal: 1000},

				{q: `workload_tpcc_otanLevel_success_total{instance=":2110"}`, t: startTime.Add(195 * time.Second), retVal: 100},
				{q: `workload_tpcc_otanLevel_success_total{instance=":2110"}`, t: startTime.Add(290 * time.Second), retVal: 1000},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2110"}`, t: startTime.Add(195 * time.Second), retVal: 1000},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2110"}`, t: startTime.Add(290 * time.Second), retVal: 1000},

				{q: `workload_tpcc_otanLevel_success_total{instance=":2111"}`, t: startTime.Add(195 * time.Second), retVal: 100},
				{q: `workload_tpcc_otanLevel_success_total{instance=":2111"}`, t: startTime.Add(290 * time.Second), retVal: 1000},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2111"}`, t: startTime.Add(195 * time.Second), retVal: 1000},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2111"}`, t: startTime.Add(290 * time.Second), retVal: 1000},

				// Restart region2.
				{q: `workload_tpcc_newOtan_success_total{instance=":2110"}`, t: startTime.Add(305 * time.Second), retVal: 1000},
				{q: `workload_tpcc_newOtan_success_total{instance=":2110"}`, t: startTime.Add(380 * time.Second), retVal: 10000},
				{q: `workload_tpcc_newOtan_error_total{instance=":2110"}`, t: startTime.Add(305 * time.Second), retVal: 1000},
				{q: `workload_tpcc_newOtan_error_total{instance=":2110"}`, t: startTime.Add(380 * time.Second), retVal: 1000},

				{q: `workload_tpcc_newOtan_success_total{instance=":2111"}`, t: startTime.Add(305 * time.Second), retVal: 1000},
				{q: `workload_tpcc_newOtan_success_total{instance=":2111"}`, t: startTime.Add(380 * time.Second), retVal: 1000},
				{q: `workload_tpcc_newOtan_error_total{instance=":2111"}`, t: startTime.Add(305 * time.Second), retVal: 1000},
				{q: `workload_tpcc_newOtan_error_total{instance=":2111"}`, t: startTime.Add(380 * time.Second), retVal: 10000},

				{q: `workload_tpcc_otanLevel_success_total{instance=":2110"}`, t: startTime.Add(305 * time.Second), retVal: 1000},
				{q: `workload_tpcc_otanLevel_success_total{instance=":2110"}`, t: startTime.Add(380 * time.Second), retVal: 10000},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2110"}`, t: startTime.Add(305 * time.Second), retVal: 1000},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2110"}`, t: startTime.Add(380 * time.Second), retVal: 1000},

				{q: `workload_tpcc_otanLevel_success_total{instance=":2111"}`, t: startTime.Add(305 * time.Second), retVal: 1000},
				{q: `workload_tpcc_otanLevel_success_total{instance=":2111"}`, t: startTime.Add(380 * time.Second), retVal: 1000},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2111"}`, t: startTime.Add(305 * time.Second), retVal: 1000},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2111"}`, t: startTime.Add(380 * time.Second), retVal: 10000},
			},
			workloadInstances: []workloadInstance{
				{
					nodes:          region1,
					prometheusPort: 2110,
				},
				{
					nodes:          region2,
					prometheusPort: 2111,
				},
			},
		},
		{
			desc: "unexpected node errors during shutdown",
			chaosEvents: []ChaosEvent{
				{Type: ChaosEventTypeStart, Time: startTime.Add(0 * time.Second)},

				// Shutdown and restart region1.
				{Type: ChaosEventTypePreShutdown, Time: startTime.Add(90 * time.Second), Target: region1},
				{Type: ChaosEventTypeShutdownComplete, Time: startTime.Add(95 * time.Second), Target: region1},
				{Type: ChaosEventTypePreStartup, Time: startTime.Add(180 * time.Second), Target: region1},
				{Type: ChaosEventTypeStartupComplete, Time: startTime.Add(185 * time.Second), Target: region1},

				{Type: ChaosEventTypeEnd, Time: startTime.Add(0 * time.Second)},
			},
			ops: []string{"newOtan", "otanLevel"},
			mockPromQueries: []expectPromQuery{
				// Restart region1.
				{q: `workload_tpcc_newOtan_success_total{instance=":2110"}`, t: startTime.Add(105 * time.Second), retVal: 100},
				{q: `workload_tpcc_newOtan_success_total{instance=":2110"}`, t: startTime.Add(180 * time.Second), retVal: 100},
				{q: `workload_tpcc_newOtan_error_total{instance=":2110"}`, t: startTime.Add(105 * time.Second), retVal: 10},
				{q: `workload_tpcc_newOtan_error_total{instance=":2110"}`, t: startTime.Add(180 * time.Second), retVal: 1000},

				{q: `workload_tpcc_newOtan_success_total{instance=":2111"}`, t: startTime.Add(105 * time.Second), retVal: 100},
				{q: `workload_tpcc_newOtan_success_total{instance=":2111"}`, t: startTime.Add(180 * time.Second), retVal: 1000},
				{q: `workload_tpcc_newOtan_error_total{instance=":2111"}`, t: startTime.Add(105 * time.Second), retVal: 0},
				{q: `workload_tpcc_newOtan_error_total{instance=":2111"}`, t: startTime.Add(180 * time.Second), retVal: 0},

				{q: `workload_tpcc_otanLevel_success_total{instance=":2110"}`, t: startTime.Add(105 * time.Second), retVal: 100},
				{q: `workload_tpcc_otanLevel_success_total{instance=":2110"}`, t: startTime.Add(180 * time.Second), retVal: 100},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2110"}`, t: startTime.Add(105 * time.Second), retVal: 10},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2110"}`, t: startTime.Add(180 * time.Second), retVal: 1000},

				{q: `workload_tpcc_otanLevel_success_total{instance=":2111"}`, t: startTime.Add(105 * time.Second), retVal: 100},
				{q: `workload_tpcc_otanLevel_success_total{instance=":2111"}`, t: startTime.Add(180 * time.Second), retVal: 1000},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2111"}`, t: startTime.Add(105 * time.Second), retVal: 0},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2111"}`, t: startTime.Add(180 * time.Second), retVal: 1000}, // unexpected errors detected
			},
			workloadInstances: []workloadInstance{
				{
					nodes:          region1,
					prometheusPort: 2110,
				},
				{
					nodes:          region2,
					prometheusPort: 2111,
				},
			},
			expectedErrors: []string{
				`error at from 2020-12-25T00:01:45Z, to 2020-12-25T00:03:00Z on metric workload_tpcc_otanLevel_error_total{instance=":2111"}: expected 0 errors, found from 0.000000, to 1000.000000`,
			},
		},
		{
			desc: "unexpected node successes during shutdown",
			chaosEvents: []ChaosEvent{
				{Type: ChaosEventTypeStart, Time: startTime.Add(0 * time.Second)},

				// Shutdown and restart region1.
				{Type: ChaosEventTypePreShutdown, Time: startTime.Add(90 * time.Second), Target: region1},
				{Type: ChaosEventTypeShutdownComplete, Time: startTime.Add(95 * time.Second), Target: region1},
				{Type: ChaosEventTypePreStartup, Time: startTime.Add(180 * time.Second), Target: region1},
				{Type: ChaosEventTypeStartupComplete, Time: startTime.Add(185 * time.Second), Target: region1},

				{Type: ChaosEventTypeEnd, Time: startTime.Add(0 * time.Second)},
			},
			ops: []string{"newOtan", "otanLevel"},
			mockPromQueries: []expectPromQuery{
				// Restart region1.
				{q: `workload_tpcc_newOtan_success_total{instance=":2110"}`, t: startTime.Add(105 * time.Second), retVal: 100},
				{q: `workload_tpcc_newOtan_success_total{instance=":2110"}`, t: startTime.Add(180 * time.Second), retVal: 1000}, // successes unexpected

				{q: `workload_tpcc_newOtan_success_total{instance=":2111"}`, t: startTime.Add(105 * time.Second), retVal: 100},
				{q: `workload_tpcc_newOtan_success_total{instance=":2111"}`, t: startTime.Add(180 * time.Second), retVal: 1000},
				{q: `workload_tpcc_newOtan_error_total{instance=":2111"}`, t: startTime.Add(105 * time.Second), retVal: 0},
				{q: `workload_tpcc_newOtan_error_total{instance=":2111"}`, t: startTime.Add(180 * time.Second), retVal: 0},

				{q: `workload_tpcc_otanLevel_success_total{instance=":2110"}`, t: startTime.Add(105 * time.Second), retVal: 100},
				{q: `workload_tpcc_otanLevel_success_total{instance=":2110"}`, t: startTime.Add(180 * time.Second), retVal: 100},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2110"}`, t: startTime.Add(105 * time.Second), retVal: 10},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2110"}`, t: startTime.Add(180 * time.Second), retVal: 10}, // errors expected

				{q: `workload_tpcc_otanLevel_success_total{instance=":2111"}`, t: startTime.Add(105 * time.Second), retVal: 100},
				{q: `workload_tpcc_otanLevel_success_total{instance=":2111"}`, t: startTime.Add(180 * time.Second), retVal: 1000},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2111"}`, t: startTime.Add(105 * time.Second), retVal: 0},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2111"}`, t: startTime.Add(180 * time.Second), retVal: 0},
			},
			workloadInstances: []workloadInstance{
				{
					nodes:          region1,
					prometheusPort: 2110,
				},
				{
					nodes:          region2,
					prometheusPort: 2111,
				},
			},
			expectedErrors: []string{
				`error at from 2020-12-25T00:01:45Z, to 2020-12-25T00:03:00Z on metric workload_tpcc_newOtan_success_total{instance=":2110"}: expected successes to not increase, found from 100.000000, to 1000.000000`,
				`error at from 2020-12-25T00:01:45Z, to 2020-12-25T00:03:00Z on metric workload_tpcc_otanLevel_error_total{instance=":2110"}: expected errors, found from 10.000000, to 10.000000`,
			},
		},
		{
			desc: "nodes have unexpected blips after startup",
			chaosEvents: []ChaosEvent{
				{Type: ChaosEventTypeStart, Time: startTime.Add(0 * time.Second)},

				// Shutdown and restart region1.
				{Type: ChaosEventTypePreShutdown, Time: startTime.Add(90 * time.Second), Target: region1},
				{Type: ChaosEventTypeShutdownComplete, Time: startTime.Add(95 * time.Second), Target: region1},
				{Type: ChaosEventTypePreStartup, Time: startTime.Add(180 * time.Second), Target: region1},
				{Type: ChaosEventTypeStartupComplete, Time: startTime.Add(185 * time.Second), Target: region1},

				// Shutdown and restart region2.
				{Type: ChaosEventTypePreShutdown, Time: startTime.Add(290 * time.Second), Target: region2},
				{Type: ChaosEventTypeShutdownComplete, Time: startTime.Add(295 * time.Second), Target: region2},
			},
			ops: []string{"newOtan", "otanLevel"},
			mockPromQueries: []expectPromQuery{
				// Restart region1.
				{q: `workload_tpcc_newOtan_success_total{instance=":2110"}`, t: startTime.Add(105 * time.Second), retVal: 100},
				{q: `workload_tpcc_newOtan_success_total{instance=":2110"}`, t: startTime.Add(180 * time.Second), retVal: 100},
				{q: `workload_tpcc_newOtan_error_total{instance=":2110"}`, t: startTime.Add(105 * time.Second), retVal: 10},
				{q: `workload_tpcc_newOtan_error_total{instance=":2110"}`, t: startTime.Add(180 * time.Second), retVal: 1000},

				{q: `workload_tpcc_newOtan_success_total{instance=":2111"}`, t: startTime.Add(105 * time.Second), retVal: 100},
				{q: `workload_tpcc_newOtan_success_total{instance=":2111"}`, t: startTime.Add(180 * time.Second), retVal: 1000},
				{q: `workload_tpcc_newOtan_error_total{instance=":2111"}`, t: startTime.Add(105 * time.Second), retVal: 0},
				{q: `workload_tpcc_newOtan_error_total{instance=":2111"}`, t: startTime.Add(180 * time.Second), retVal: 0},

				{q: `workload_tpcc_otanLevel_success_total{instance=":2110"}`, t: startTime.Add(105 * time.Second), retVal: 100},
				{q: `workload_tpcc_otanLevel_success_total{instance=":2110"}`, t: startTime.Add(180 * time.Second), retVal: 100},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2110"}`, t: startTime.Add(105 * time.Second), retVal: 10},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2110"}`, t: startTime.Add(180 * time.Second), retVal: 1000},

				{q: `workload_tpcc_otanLevel_success_total{instance=":2111"}`, t: startTime.Add(105 * time.Second), retVal: 100},
				{q: `workload_tpcc_otanLevel_success_total{instance=":2111"}`, t: startTime.Add(180 * time.Second), retVal: 1000},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2111"}`, t: startTime.Add(105 * time.Second), retVal: 0},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2111"}`, t: startTime.Add(180 * time.Second), retVal: 0},

				// Shutdown region2.
				{q: `workload_tpcc_newOtan_success_total{instance=":2110"}`, t: startTime.Add(195 * time.Second), retVal: 100},
				{q: `workload_tpcc_newOtan_success_total{instance=":2110"}`, t: startTime.Add(290 * time.Second), retVal: 100}, // no successes

				{q: `workload_tpcc_newOtan_success_total{instance=":2111"}`, t: startTime.Add(195 * time.Second), retVal: 100},
				{q: `workload_tpcc_newOtan_success_total{instance=":2111"}`, t: startTime.Add(290 * time.Second), retVal: 1000},
				{q: `workload_tpcc_newOtan_error_total{instance=":2111"}`, t: startTime.Add(195 * time.Second), retVal: 1000},
				{q: `workload_tpcc_newOtan_error_total{instance=":2111"}`, t: startTime.Add(290 * time.Second), retVal: 1000},

				{q: `workload_tpcc_otanLevel_success_total{instance=":2110"}`, t: startTime.Add(195 * time.Second), retVal: 100},
				{q: `workload_tpcc_otanLevel_success_total{instance=":2110"}`, t: startTime.Add(290 * time.Second), retVal: 1000},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2110"}`, t: startTime.Add(195 * time.Second), retVal: 1000},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2110"}`, t: startTime.Add(290 * time.Second), retVal: 1000},

				{q: `workload_tpcc_otanLevel_success_total{instance=":2111"}`, t: startTime.Add(195 * time.Second), retVal: 100},
				{q: `workload_tpcc_otanLevel_success_total{instance=":2111"}`, t: startTime.Add(290 * time.Second), retVal: 1000},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2111"}`, t: startTime.Add(195 * time.Second), retVal: 1000},
				{q: `workload_tpcc_otanLevel_error_total{instance=":2111"}`, t: startTime.Add(290 * time.Second), retVal: 10000}, // error spike
			},
			workloadInstances: []workloadInstance{
				{
					nodes:          region1,
					prometheusPort: 2110,
				},
				{
					nodes:          region2,
					prometheusPort: 2111,
				},
			},
			expectedErrors: []string{
				`error at from 2020-12-25T00:03:15Z, to 2020-12-25T00:04:50Z on metric workload_tpcc_newOtan_success_total{instance=":2110"}: expected successes to be increasing, found from 100.000000, to 100.000000`,
				`error at from 2020-12-25T00:03:15Z, to 2020-12-25T00:04:50Z on metric workload_tpcc_otanLevel_error_total{instance=":2111"}: expected 0 errors, found from 1000.000000, to 10000.000000`,
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
