// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

type testTenantTokensRequester struct {
	b *strings.Builder
}

func toMillisString(nanos int64) string {
	return fmt.Sprintf("%.1f millis", float64(nanos)/1e6)
}

func (tttr *testTenantTokensRequester) tenantCPUTokensTick(tokensToAdd int64) {
	fmt.Fprintf(tttr.b, "  tenantCPUTokensTick(%s)\n", toMillisString(tokensToAdd))
}

func (tttr *testTenantTokensRequester) setTenantCPUTokensBurstLimit(tokens int64, enabled bool) {
	fmt.Fprintf(tttr.b, "  setTenantCPUTokensBurstLimit(%s, %t)\n",
		toMillisString(tokens), enabled)
}

func TestCPUTimeTokenAdjuster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	st := cluster.MakeTestingClusterSettings()
	KVCPUTimeTokensEnabled.Override(context.Background(), &st.SV, true)
	KVCPUTimeUtilGoal.Override(context.Background(), &st.SV, 0.8)
	var ctta *cpuTimeTokenAdjuster
	var b strings.Builder
	printCTTA := func() {
		fmt.Fprintf(&b, "CTTA t=%s: total-cpu=%s, mult=%.1f\n",
			toMillisString(ctta.lastSampleTime.UnixNano()),
			toMillisString(ctta.totalCPUTimeMillis*1e6),
			ctta.tokenToCPUTimeMultiplier)
		fmt.Fprintf(&b, "  tokens (alloc): %s(%s) %s(%s), tenant (alloc): %s(%s)\n",
			toMillisString(ctta.tokenBucketRateOne), toMillisString(ctta.tokensAllocatedOne),
			toMillisString(ctta.tokenBucketRateTwo), toMillisString(ctta.tokensAllocatedTwo),
			toMillisString(ctta.tenantTokenBucketRate), toMillisString(ctta.tenantTokensAllocated))
		fmt.Fprintf(&b, "  granter: tokens: %s %s\n", toMillisString(ctta.granter.cpuTimeTokensOne),
			toMillisString(ctta.granter.cpuTimeTokensTwo))
		fmt.Fprintf(&b, "\n")
	}
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "cpu_time_token_adjuster"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				b.Reset()
				ctta = &cpuTimeTokenAdjuster{
					settings:              st,
					granter:               &slotAndCPUTimeTokenGranter{},
					tenantTokensRequester: &testTenantTokensRequester{b: &b},
				}
				return ""

			case "adjust":
				var ab strings.Builder
				fmt.Fprintf(&b, "adjust\n")
				if d.HasArg("int-tokens") {
					var intTokensUsed int64
					d.ScanArgs(t, "int-tokens", &intTokensUsed)
					ctta.granter.intTokensUsed = intTokensUsed * 1e6
				}
				if d.HasArg("cur-tokens-in-bucket") {
					var curTokensInBucket int64
					d.ScanArgs(t, "cur-tokens-in-bucket", &curTokensInBucket)
					ctta.granter.cpuTimeTokensTwo = curTokensInBucket * 1e6
					mult := 1.05
					if curTokensInBucket < 0 {
						mult = 0.95
					}
					ctta.granter.cpuTimeTokensOne = int64(float64(curTokensInBucket) * 1e6 * mult)
				}

				ctta.adjust(parseAdjustParams(t, d))
				printCTTA()
				ab.WriteString(b.String())
				b.Reset()
				for i := 1000; i > 0; i-- {
					printTick := i == 1000 || i == 999 || i == 1
					if printTick {
						fmt.Fprintf(&b, "remaining tick %d\n", i)
					}
					ctta.allocateTokensTick(int64(i))
					if printTick {
						printCTTA()
						ab.WriteString(b.String())
					}
					b.Reset()
				}
				return ab.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func parseAdjustParams(t *testing.T, d *datadriven.TestData) (time.Time, int64, float64) {
	var timeMillis int
	d.ScanArgs(t, "time-millis", &timeMillis)
	var cpuMillis int
	d.ScanArgs(t, "cpu-millis", &cpuMillis)
	var cpuCount int
	d.ScanArgs(t, "cpu-count", &cpuCount)
	return time.UnixMilli(int64(timeMillis)), int64(cpuMillis), float64(cpuCount)
}
