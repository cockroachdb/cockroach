// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestElasticCPUWorkQueue is a datadriven test with the following commands:
// - "init"
// - "admit" duration=<duration> [disabled=<bool>]
// - "admitted-work-done" running=<duration> allotted=<duration>
func TestElasticCPUWorkQueue(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(8))

	var (
		elasticWorkQ                *ElasticCPUWorkQueue
		elasticCPUGranter           *testElasticCPUGranter
		elasticCPUInternalWorkQueue *testElasticCPUInternalWorkQueue
	)

	ctx := context.Background()
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "elastic_cpu_work_queue"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				elasticCPUGranter = &testElasticCPUGranter{}
				elasticCPUInternalWorkQueue = &testElasticCPUInternalWorkQueue{}
				elasticWorkQ = makeElasticCPUWorkQueue(
					cluster.MakeTestingClusterSettings(),
					elasticCPUInternalWorkQueue,
					elasticCPUGranter,
					makeElasticCPUGranterMetrics(),
				)
				elasticWorkQ.testingEnabled = true

			case "admit":
				elasticCPUGranter.buf.Reset()
				elasticCPUInternalWorkQueue.buf.Reset()

				var durationStr string
				d.ScanArgs(t, "duration", &durationStr)
				duration, err := time.ParseDuration(durationStr)
				require.NoError(t, err)
				if d.HasArg("disabled") {
					d.ScanArgs(t, "disabled", &elasticCPUInternalWorkQueue.disabled)
				}

				handle, err := elasticWorkQ.Admit(ctx, duration, WorkInfo{TenantID: roachpb.SystemTenantID})
				require.NoError(t, err)

				var buf strings.Builder
				buf.WriteString(strings.TrimSpace(fmt.Sprintf("granter:     %s",
					elasticCPUGranter.buf.String())) + "\n")
				buf.WriteString(strings.TrimSpace(fmt.Sprintf("work-queue:  %s",
					elasticCPUInternalWorkQueue.buf.String())) + "\n")
				buf.WriteString(fmt.Sprintf("metrics:     acquired=%s returned=%s max-available=%s\n",
					time.Duration(elasticWorkQ.metrics.AcquiredNanos.Count()),
					time.Duration(elasticWorkQ.metrics.ReturnedNanos.Count()),
					time.Duration(elasticWorkQ.metrics.MaxAvailableNanos.Count())))
				if handle == nil {
					buf.WriteString("handle:      n/a\n")
				} else {
					buf.WriteString(fmt.Sprintf("handle:      %s\n", handle.allotted))
				}
				return buf.String()

			case "admitted-work-done":
				elasticCPUGranter.buf.Reset()
				elasticCPUInternalWorkQueue.buf.Reset()

				var runningStr string
				d.ScanArgs(t, "running", &runningStr)
				running, err := time.ParseDuration(runningStr)
				require.NoError(t, err)

				var allottedStr string
				d.ScanArgs(t, "allotted", &allottedStr)
				allotted, err := time.ParseDuration(allottedStr)
				require.NoError(t, err)

				handle := &ElasticCPUWorkHandle{tenantID: roachpb.SystemTenantID}
				handle.testingOverrideRunningTime = func() time.Duration {
					return running
				}
				handle.allotted = allotted
				elasticWorkQ.AdmittedWorkDone(handle)

				var buf strings.Builder
				buf.WriteString(fmt.Sprintf("granter:    %s\n",
					strings.TrimSpace(elasticCPUGranter.buf.String())))
				buf.WriteString(fmt.Sprintf("work-queue: %s\n",
					strings.TrimSpace(elasticCPUInternalWorkQueue.buf.String())))
				buf.WriteString(fmt.Sprintf("metrics:    acquired=%s returned=%s max-available=%s",
					time.Duration(elasticWorkQ.metrics.AcquiredNanos.Count()),
					time.Duration(elasticWorkQ.metrics.ReturnedNanos.Count()),
					time.Duration(elasticWorkQ.metrics.MaxAvailableNanos.Count())))
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}

			return ""
		},
	)
}

type testElasticCPUGranter struct {
	buf strings.Builder
}

var _ granter = &testElasticCPUGranter{}

func (t *testElasticCPUGranter) grantKind() grantKind {
	return token
}

func (t *testElasticCPUGranter) tryGet(count int64) (granted bool) {
	panic("unimplemented")
}

func (t *testElasticCPUGranter) returnGrant(count int64) {
	t.buf.WriteString(fmt.Sprintf("return-grant=%s ", time.Duration(count)))
}

func (t *testElasticCPUGranter) tookWithoutPermission(count int64) {
	t.buf.WriteString(fmt.Sprintf("took-without-permission=%s ", time.Duration(count)))
}

func (t *testElasticCPUGranter) continueGrantChain(grantChainID grantChainID) {
	panic("unimplemented")
}

type testElasticCPUInternalWorkQueue struct {
	buf      strings.Builder
	disabled bool
}

var _ elasticCPUInternalWorkQueue = &testElasticCPUInternalWorkQueue{}

func (t *testElasticCPUInternalWorkQueue) Admit(
	_ context.Context, info WorkInfo,
) (enabled bool, err error) {
	if !t.disabled {
		t.buf.WriteString(fmt.Sprintf("admitted=%s ", time.Duration(info.RequestedCount)))
	}
	return !t.disabled, nil
}

func (t *testElasticCPUInternalWorkQueue) SetTenantWeights(tenantWeights map[uint64]uint32) {
	panic("unimplemented")
}

func (t *testElasticCPUInternalWorkQueue) adjustTenantUsed(
	tenantID roachpb.TenantID, additionalUsed int64,
) {
	if !t.disabled {
		fmt.Fprintf(&t.buf, "adjust-tenant-used: tenant=%s additional-used=%s",
			tenantID.String(), time.Duration(additionalUsed).String())
	}
}

func (t *testElasticCPUInternalWorkQueue) hasWaitingRequests() bool {
	panic("unimplemented")
}

func (t *testElasticCPUInternalWorkQueue) granted(grantChainID grantChainID) int64 {
	panic("unimplemented")
}

func (t *testElasticCPUInternalWorkQueue) close() {
	panic("unimplemented")
}
