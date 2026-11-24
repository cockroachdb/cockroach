// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
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
	scrapeBeforeFirstPreStartup := firstPreStartup.Add(-prometheus.DefaultScrapeInterval)
	firstStartupComplete := startTime.Add(185 * time.Second)
	scrapeAfterFirstStartupComplete := firstStartupComplete.Add(prometheus.DefaultScrapeInterval)

	secondPreShutdown := startTime.Add(390 * time.Second)
	scrapeBeforeSecondPreShutdown := secondPreShutdown.Add(-prometheus.DefaultScrapeInterval)
	secondShutdownComplete := startTime.Add(395 * time.Second)
	scrapeAfterSecondShutdownComplete := secondShutdownComplete.Add(prometheus.DefaultScrapeInterval)
	secondPreStartup := startTime.Add(480 * time.Second)
	scrapeBeforeSecondPreStartup := secondPreStartup.Add(-prometheus.DefaultScrapeInterval)
	secondStartupComplete := startTime.Add(490 * time.Second)

	metricA := "newOtan"
	metricB := "otanLevel"

	type expectPromQuery struct {
		q      string
		t      time.Time
		retVal model.SampleValue
	}
	testCases := []struct {
		desc                         string
		chaosEvents                  []ChaosEvent
		ops                          []string
		workloadInstances            []workloadInstance
		mockPromQueries              []expectPromQuery
		expectedErrors               []string
		allowZeroSuccessDuringUptime bool
		maxErrorsDuringUptime        int
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
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 100},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 10},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 1000},

				{q: makeMetric(metricA, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 0},

				{q: makeMetric(metricB, "success", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 100},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 10},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 0},

				// Shutdown region2.
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeBeforeSecondPreShutdown, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeAfterFirstStartupComplete, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeBeforeSecondPreShutdown, retVal: 1000},

				{q: makeMetric(metricA, "success", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion2), t: scrapeBeforeSecondPreShutdown, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeBeforeSecondPreShutdown, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion1), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion1), t: scrapeBeforeSecondPreShutdown, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeAfterFirstStartupComplete, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeBeforeSecondPreShutdown, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion2), t: scrapeBeforeSecondPreShutdown, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeBeforeSecondPreShutdown, retVal: 1000},

				// Restart region2.
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeBeforeSecondPreStartup, retVal: 10000},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeBeforeSecondPreStartup, retVal: 1000},

				{q: makeMetric(metricA, "success", portRegion2), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricA, "success", portRegion2), t: scrapeBeforeSecondPreStartup, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeBeforeSecondPreStartup, retVal: 10000},

				{q: makeMetric(metricB, "success", portRegion1), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricB, "success", portRegion1), t: scrapeBeforeSecondPreStartup, retVal: 10000},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeBeforeSecondPreStartup, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion2), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricB, "success", portRegion2), t: scrapeBeforeSecondPreStartup, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeBeforeSecondPreStartup, retVal: 10000},
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
			desc: "tolerate errors and no successes during uptime",
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
			allowZeroSuccessDuringUptime: true,
			maxErrorsDuringUptime:        5,
			ops:                          []string{metricA, metricB},
			mockPromQueries: []expectPromQuery{
				// Restart region1.
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 100},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 10},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 1000},

				{q: makeMetric(metricA, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 5}, // test that it still works with <= maxErrorsDuringUptime errors

				{q: makeMetric(metricB, "success", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 100},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 10},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 0},

				// Shutdown region2.
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeBeforeSecondPreShutdown, retVal: 100}, // no success should still succeed as allowZeroSuccessDuringUptime = true
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeAfterFirstStartupComplete, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeBeforeSecondPreShutdown, retVal: 1000},

				{q: makeMetric(metricA, "success", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion2), t: scrapeBeforeSecondPreShutdown, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeBeforeSecondPreShutdown, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion1), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion1), t: scrapeBeforeSecondPreShutdown, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeAfterFirstStartupComplete, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeBeforeSecondPreShutdown, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion2), t: scrapeBeforeSecondPreShutdown, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeBeforeSecondPreShutdown, retVal: 1000},

				// Restart region2.
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeBeforeSecondPreStartup, retVal: 10000},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeBeforeSecondPreStartup, retVal: 1000},

				{q: makeMetric(metricA, "success", portRegion2), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricA, "success", portRegion2), t: scrapeBeforeSecondPreStartup, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeBeforeSecondPreStartup, retVal: 10000},

				{q: makeMetric(metricB, "success", portRegion1), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricB, "success", portRegion1), t: scrapeBeforeSecondPreStartup, retVal: 10000},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeBeforeSecondPreStartup, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion2), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricB, "success", portRegion2), t: scrapeBeforeSecondPreStartup, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeAfterSecondShutdownComplete, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeBeforeSecondPreStartup, retVal: 10000},
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
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 100},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 10},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 1000},

				{q: makeMetric(metricA, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 0},

				{q: makeMetric(metricB, "success", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 100},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 10},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 1000}, // should have had no errors during shutdown.
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
					`error at from 2020-12-25T00:01:45Z, to 2020-12-25T00:02:50Z on metric %s: expected <=0 errors, found from 0.000000, to 1000.000000`,
					makeMetric(metricB, "error", portRegion2),
				),
			},
		},
		{
			desc: "unexpected node errors during shutdown with tolerance",
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
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 100},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 10},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 1000},

				{q: makeMetric(metricA, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 0},

				{q: makeMetric(metricB, "success", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 100},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 10},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 6}, // should have had <= 5 errors during shutdown.
			},
			maxErrorsDuringUptime: 5,
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
					`error at from 2020-12-25T00:01:45Z, to 2020-12-25T00:02:50Z on metric %s: expected <=5 errors, found from 0.000000, to 6.000000`,
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
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 1000}, // should have had no successes whilst shutdown.

				{q: makeMetric(metricA, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 0},

				{q: makeMetric(metricB, "success", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 100},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 10},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 10}, // should have had errors whilst shutdown.

				{q: makeMetric(metricB, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 0},
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
					`error at from 2020-12-25T00:01:45Z, to 2020-12-25T00:02:50Z on metric %s: expected successes to not increase, found from 100.000000, to 1000.000000`,
					makeMetric(metricA, "success", portRegion1),
				),
				fmt.Sprintf(
					`error at from 2020-12-25T00:01:45Z, to 2020-12-25T00:02:50Z on metric %s: expected errors, found from 10.000000, to 10.000000`,
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
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 100},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 10},
				{q: makeMetric(metricA, "error", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 1000},

				{q: makeMetric(metricA, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 0},

				{q: makeMetric(metricB, "success", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 100},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeAfterFirstShutdownComplete, retVal: 10},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeBeforeFirstPreStartup, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeAfterFirstShutdownComplete, retVal: 0},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeBeforeFirstPreStartup, retVal: 0},

				// Shutdown region2.
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion1), t: scrapeBeforeSecondPreShutdown, retVal: 100}, // should have had successes as it was not the node shutdown.

				{q: makeMetric(metricA, "success", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricA, "success", portRegion2), t: scrapeBeforeSecondPreShutdown, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 1000},
				{q: makeMetric(metricA, "error", portRegion2), t: scrapeBeforeSecondPreShutdown, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion1), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion1), t: scrapeBeforeSecondPreShutdown, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeAfterFirstStartupComplete, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion1), t: scrapeBeforeSecondPreShutdown, retVal: 1000},

				{q: makeMetric(metricB, "success", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 100},
				{q: makeMetric(metricB, "success", portRegion2), t: scrapeBeforeSecondPreShutdown, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeAfterFirstStartupComplete, retVal: 1000},
				{q: makeMetric(metricB, "error", portRegion2), t: scrapeBeforeSecondPreShutdown, retVal: 10000}, // unexpected errors during restart.
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
					`error at from 2020-12-25T00:03:15Z, to 2020-12-25T00:06:20Z on metric %s: expected successes to be increasing, found from 100.000000, to 100.000000`,
					makeMetric(metricA, "success", portRegion1),
				),
				fmt.Sprintf(
					`error at from 2020-12-25T00:03:15Z, to 2020-12-25T00:06:20Z on metric %s: expected <=0 errors, found from 1000.000000, to 10000.000000`,
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
				workloadInstances:            tc.workloadInstances,
				ops:                          tc.ops,
				ch:                           ch,
				allowZeroSuccessDuringUptime: tc.allowZeroSuccessDuringUptime,
				maxErrorsDuringUptime:        tc.maxErrorsDuringUptime,

				promClient: func(ctrl *gomock.Controller) prometheus.Client {
					c := NewMockClient(ctrl)
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

			tasker := task.NewManager(ctx, l)
			ep.listen(ctx, tasker, l)
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
