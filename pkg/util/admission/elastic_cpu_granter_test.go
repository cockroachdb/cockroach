// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admission

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestElasticCPUGranter is a datadriven test with the following commands. For a
// set of these commands, if "print" is specified, we additionally print
// contents of rate limiter and underlying granter metrics.
//
// - "init"
//   Initializes the Elastic CPU Granter.
//
// - "advance" duration=<duration> [print]
//   Moves time forward, affecting the underlying refill rate.
//
// - "set-utilization-limit" limit=<percent> [print]
//   Configures the granter to enforce the specified utilization limit.
//
// - "took-without-permission" duration=<duration> [print]
//   Part of the granter interface; deduct the specified duration worth of
//   tokens from the granter without blocking.
//
// - "try-get" duration=<duration> [print]
//   Part of the granter interface; try and get the specified duration worth of
//   tokens from the granter without blocking (returns false if insufficient
//   quota).
//
// - "return-grant" duration=<duration> [print]
//   Part of the granter interface; returns the unused duration worth of tokens
//   to the granter and have it try to forward it to waiting requesters, if any.
//
// - "try-grant" [print]
//   Try to grant CPU tokens to waiting requests, if any.
//
// - "requester" has-waiting-requests=<bool> duration=<duration>
//   Configure the requester with the specified has-waiting-requests value
//   (something the granter checks when forwarding grants) and how much duration
//   of tokens it demands once granted (also something the granter accounts
//   for).
//
// - "print"
//   Print contents of rate limiter and underlying granter metrics.
//
func TestElasticCPUGranter(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(8))

	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	dir := testutils.TestDataPath(t, "elastic_cpu_granter")
	datadriven.Walk(t, dir, func(t *testing.T, path string) {

		var (
			elasticCPURequester      *testElasticCPURequester
			elasticCPUGranterMetrics *elasticCPUGranterMetrics
			elasticCPUGranter        *elasticCPUGranter
			mt                       *timeutil.ManualTime
			rateLimiter              *quotapool.RateLimiter
		)

		printRateLimiter := func() string {
			rate, available, burst := rateLimiter.Parameters()
			return fmt.Sprintf("rate-limiter:    refill=%s/s burst=%s available=%s across %d procs\n",
				time.Duration(rate), time.Duration(burst), time.Duration(available), runtime.GOMAXPROCS(0))
		}

		printMetrics := func() string {
			return fmt.Sprintf("metrics/granter: limit=%0.2f%% utilization=%0.2f%%\n",
				elasticCPUGranterMetrics.UtilizationLimit.Value()*100,
				elasticCPUGranterMetrics.Utilization.Value()*100,
			)
		}

		printRequester := func() string {
			if elasticCPURequester.buf.Len() == 0 {
				return ""
			}
			return fmt.Sprintf("requester:       %s", elasticCPURequester.buf.String())
		}

		printEverything := func() string {
			return strings.TrimSpace(printRateLimiter() + printMetrics() + printRequester())
		}

		datadriven.RunTest(t, path,
			func(t *testing.T, d *datadriven.TestData) (retStr string) {
				var duration time.Duration
				if d.Cmd == "advance" || d.Cmd == "took-without-permission" ||
					d.Cmd == "try-get" || d.Cmd == "return-grant" || d.Cmd == "requester" {
					var durationStr string
					d.ScanArgs(t, "duration", &durationStr)
					var err error
					duration, err = time.ParseDuration(durationStr)
					require.NoError(t, err)
				}

				defer func() {
					metricsUtilizationLimit := elasticCPUGranterMetrics.UtilizationLimit.Value() * 100
					granterUtilizationLimit := elasticCPUGranter.getUtilizationLimit() * 100

					metricsUtilization := elasticCPUGranterMetrics.Utilization.Value() * 100
					granterUtilization := elasticCPUGranter.getUtilization() * 100

					require.Equal(t, metricsUtilizationLimit, granterUtilizationLimit)
					require.Equal(t, metricsUtilization, granterUtilization)
				}()
				defer func() {
					if d.HasArg("print") {
						retStr = retStr + printEverything()
						return
					}
				}()

				switch d.Cmd {
				case "init":
					elasticCPURequester = &testElasticCPURequester{}
					elasticCPUGranterMetrics = makeElasticCPUGranterMetrics()
					mt = timeutil.NewManualTime(t0)
					rateLimiter = quotapool.NewRateLimiter("manual-rate-limiter", 0, 0,
						quotapool.WithTimeSource(mt))
					elasticCPUGranter = newElasticCPUGranterWithRateLimiter(
						log.MakeTestingAmbientCtxWithNewTracer(),
						cluster.MakeTestingClusterSettings(),
						elasticCPUGranterMetrics,
						rateLimiter,
					)
					elasticCPUGranter.setRequester(elasticCPURequester)
					return printEverything()

				case "advance":
					mt.Advance(duration)
					return ""

				case "set-utilization-limit":
					var limitUtilStr string
					d.ScanArgs(t, "limit", &limitUtilStr)
					limitUtilStr = strings.TrimSuffix(limitUtilStr, "%")
					limitUtilPercent, err := strconv.ParseFloat(limitUtilStr, 64)
					require.NoError(t, err)
					elasticCPUGranter.setUtilizationLimit(limitUtilPercent / 100)
					return ""

				case "took-without-permission":
					elasticCPUGranter.tookWithoutPermission(duration.Nanoseconds())
					return ""

				case "try-get":
					granted := elasticCPUGranter.tryGet(duration.Nanoseconds())
					return fmt.Sprintf("granted:         %t\n", granted)

				case "return-grant":
					elasticCPURequester.buf.Reset()
					elasticCPUGranter.returnGrant(duration.Nanoseconds())
					return ""

				case "try-grant":
					elasticCPURequester.buf.Reset()
					elasticCPUGranter.tryGrant()
					return ""

				case "requester":
					d.ScanArgs(t, "has-waiting-requests", &elasticCPURequester.hasWaitingRequestsVal)
					elasticCPURequester.grantVal = duration.Nanoseconds()
					return ""

				case "print":
					return printEverything()

				default:
					return fmt.Sprintf("unknown command: %s", d.Cmd)
				}
			},
		)
	})
}

type testElasticCPURequester struct {
	buf                   strings.Builder
	hasWaitingRequestsVal bool
	grantVal              int64
}

var _ requester = &testElasticCPURequester{}

func (t *testElasticCPURequester) hasWaitingRequests() bool {
	t.buf.WriteString(fmt.Sprintf("has-waiting-requests=%t ", t.hasWaitingRequestsVal))
	return t.hasWaitingRequestsVal
}

func (t *testElasticCPURequester) granted(grantChainID grantChainID) int64 {
	t.buf.WriteString(fmt.Sprintf("granted=%s", time.Duration(t.grantVal)))
	return t.grantVal
}

func (t *testElasticCPURequester) close() {
	panic("unimplemented")
}
