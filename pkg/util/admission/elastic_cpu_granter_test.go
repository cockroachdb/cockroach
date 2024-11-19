// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/tokenbucket"
	"github.com/stretchr/testify/require"
)

// TestElasticCPUGranter is a datadriven test with the following commands. For a
// set of these commands, if "print" is specified, we additionally print
// contents of rate limiter and underlying granter metrics.
//
//   - "init"
//     Initializes the Elastic CPU Granter.
//
//   - "advance" duration=<duration> [print]
//     Moves time forward, affecting the underlying refill rate.
//
//   - "set-utilization-limit" limit=<percent> [print]
//     Configures the granter to enforce the specified utilization limit.
//
//   - "took-without-permission" duration=<duration> [print]
//     Part of the granter interface; deduct the specified duration worth of
//     tokens from the granter without blocking.
//
//   - "try-get" duration=<duration> [print]
//     Part of the granter interface; try and get the specified duration worth of
//     tokens from the granter without blocking (returns false if insufficient
//     quota).
//
//   - "return-grant" duration=<duration> [print]
//     Part of the granter interface; returns the unused duration worth of tokens
//     to the granter and have it try to forward it to waiting requesters, if any.
//
//   - "try-grant" [print]
//     Try to grant CPU tokens to waiting requests, if any.
//
//   - "requester" num-waiting-requests=<int> duration=<duration>
//     Configure the requester with the specified number of waiting requests
//     (something the granter checks when forwarding grants) and the duration
//     each requests demands once granted (also something the granter accounts
//     for).
//
//   - "print"
//     Print contents of rate limiter and underlying granter metrics.
func TestElasticCPUGranter(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(8))

	var (
		elasticCPURequester      *testElasticCPURequester
		elasticCPUGranterMetrics *elasticCPUGranterMetrics
		elasticCPUGranter        *elasticCPUGranter
		mt                       *timeutil.ManualTime
		tokenBucket              *tokenbucket.TokenBucket
	)

	printTokenBucket := func() string {
		rate, burst, available := tokenBucket.TestingInternalParameters()
		return fmt.Sprintf("token-bucket:    refill=%s/s burst=%s available=%s across %d procs\n",
			time.Duration(rate), time.Duration(burst), time.Duration(available), runtime.GOMAXPROCS(0))
	}

	printMetrics := func() string {
		return fmt.Sprintf("metrics/granter: limit=%0.2f%%\n",
			elasticCPUGranterMetrics.UtilizationLimit.Value()*100,
		)
	}

	printRequester := func() string {
		if elasticCPURequester.buf.Len() == 0 {
			return ""
		}
		return fmt.Sprintf("requester:       %s", elasticCPURequester.buf.String())
	}

	printEverything := func() string {
		return strings.TrimSpace(printTokenBucket() + printMetrics() + printRequester())
	}

	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "elastic_cpu_granter"),
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
				require.Equal(t, metricsUtilizationLimit, granterUtilizationLimit)
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
				tokenBucket = &tokenbucket.TokenBucket{}
				tokenBucket.InitWithNowFn(0, 0, mt.Now)
				elasticCPUGranter = newElasticCPUGranterWithTokenBucket(
					log.MakeTestingAmbientCtxWithNewTracer(),
					cluster.MakeTestingClusterSettings(),
					elasticCPUGranterMetrics,
					tokenBucket,
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
				elasticCPURequester.buf.Reset()
				d.ScanArgs(t, "num-waiting-requests", &elasticCPURequester.numWaitingRequests)
				elasticCPURequester.grantVal = duration.Nanoseconds()
				return ""

			case "print":
				return printEverything()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		},
	)
}

type testElasticCPURequester struct {
	buf                strings.Builder
	numWaitingRequests int
	grantVal           int64
}

var _ requester = &testElasticCPURequester{}

func (t *testElasticCPURequester) hasWaitingRequests() bool {
	var padding string
	if t.buf.Len() > 0 {
		padding = "                 "
	}
	t.buf.WriteString(fmt.Sprintf("%shas-waiting=%t ", padding, t.numWaitingRequests > 0))
	return t.numWaitingRequests > 0
}

func (t *testElasticCPURequester) granted(grantChainID grantChainID) int64 {
	t.buf.WriteString(fmt.Sprintf("granted=%s\n", time.Duration(t.grantVal)))
	t.numWaitingRequests--
	return t.grantVal
}

func (t *testElasticCPURequester) close() {
	panic("unimplemented")
}
