// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostclient_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl" // ccl init hooks
	"github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl/tenantcostclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
)

// TestDataDriven tests the tenant-side cost controller in an isolated setting.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		defer leaktest.AfterTest(t)()

		var ts testState
		ts.start(t)
		defer ts.stop()

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			fn, ok := testStateCommands[d.Cmd]
			if !ok {
				d.Fatalf(t, "unknown command %s", d.Cmd)
			}
			return fn(&ts, t, d)
		})
	})
}

type testState struct {
	settings   *cluster.Settings
	stopper    *stop.Stopper
	provider   *testProvider
	controller multitenant.TenantSideCostController

	// cpuUsage, accessed using atomic.
	cpuUsage time.Duration
}

func (ts *testState) start(t *testing.T) {
	ctx := context.Background()

	ts.settings = cluster.MakeTestingClusterSettings()
	tenantcostclient.TargetPeriodSetting.Override(ctx, &ts.settings.SV, time.Millisecond)
	tenantcostclient.CPUUsageAllowance.Override(ctx, &ts.settings.SV, 0)

	ts.stopper = stop.NewStopper()
	var err error
	ts.provider = newTestProvider()
	ts.controller, err = tenantcostclient.NewTenantSideCostController(
		ts.settings,
		roachpb.MakeTenantID(5),
		ts.provider,
	)
	if err != nil {
		t.Fatal(err)
	}
	cpuUsageFn := func(context.Context) float64 {
		usage := time.Duration(atomic.LoadInt64((*int64)(&ts.cpuUsage)))
		return usage.Seconds()
	}
	if err := ts.controller.Start(ctx, ts.stopper, cpuUsageFn); err != nil {
		t.Fatal(err)
	}
}

func (ts *testState) stop() {
	ts.stopper.Stop(context.Background())
}

var testStateCommands = map[string]func(*testState, *testing.T, *datadriven.TestData) string{
	"read-request":  (*testState).readRequest,
	"read-response": (*testState).readResponse,
	"write-request": (*testState).writeRequest,
	"cpu":           (*testState).cpu,
	"usage":         (*testState).usage,
}

func bytesArg(t *testing.T, d *datadriven.TestData) int64 {
	for _, args := range d.CmdArgs {
		if args.Key == "bytes" {
			if len(args.Vals) != 1 {
				d.Fatalf(t, "expected one value for bytes")
			}
			val, err := strconv.Atoi(args.Vals[0])
			if err != nil {
				d.Fatalf(t, "invalid bytes value")
			}
			return int64(val)
		}
	}
	d.Fatalf(t, "bytes argument required")
	return 0
}

func (ts *testState) readRequest(t *testing.T, d *datadriven.TestData) string {
	info := tenantcostmodel.TestingRequestInfo(false /* isWrite */, 0 /* writeBytes */)
	if err := ts.controller.OnRequestWait(context.Background(), info); err != nil {
		d.Fatalf(t, "%v", err)
	}
	return ""
}

func (ts *testState) readResponse(t *testing.T, d *datadriven.TestData) string {
	info := tenantcostmodel.TestingResponseInfo(bytesArg(t, d))
	ts.controller.OnResponse(context.Background(), info)
	return ""
}

func (ts *testState) writeRequest(t *testing.T, d *datadriven.TestData) string {
	info := tenantcostmodel.TestingRequestInfo(true /* isWrite */, bytesArg(t, d))
	if err := ts.controller.OnRequestWait(context.Background(), info); err != nil {
		d.Fatalf(t, "%v", err)
	}
	return ""
}

func (ts *testState) cpu(t *testing.T, d *datadriven.TestData) string {
	duration, err := time.ParseDuration(d.Input)
	if err != nil {
		d.Fatalf(t, "error parsing cpu duration: %v", err)
	}
	atomic.AddInt64((*int64)(&ts.cpuUsage), int64(duration))
	return ""
}

func (ts *testState) usage(t *testing.T, d *datadriven.TestData) string {
	c := ts.provider.waitForConsumption()
	return fmt.Sprintf(""+
		"RU:  %.2f\n"+
		"Reads:  %d requests (%d bytes)\n"+
		"Writes:  %d requests (%d bytes)\n"+
		"SQL Pods CPU seconds:  %.2f\n",
		c.RU,
		c.ReadRequests,
		c.ReadBytes,
		c.WriteRequests,
		c.WriteBytes,
		c.SQLPodsCPUSeconds,
	)
}

// testProvider is a testing implementation of kvtenant.TokenBucketProvider,
type testProvider struct {
	mu struct {
		syncutil.Mutex
		consumption roachpb.TenantConsumption
	}
	recvOnRequest chan struct{}
}

var _ kvtenant.TokenBucketProvider = (*testProvider)(nil)

func newTestProvider() *testProvider {
	return &testProvider{
		recvOnRequest: make(chan struct{}),
	}
}

// waitForRequest waits until the next TokenBucket request.
func (tp *testProvider) waitForRequest() {
	// Try to send through the unbuffered channel. This will block until
	// TokenBucket is called.
	tp.recvOnRequest <- struct{}{}
}

// waitForConsumption waits for the next TokenBucket request and returns the
// total consumption.
func (tp *testProvider) waitForConsumption() roachpb.TenantConsumption {
	tp.waitForRequest()
	// it is possible that the TokenBucket request was in the process of being
	// prepared; we have to wait for another one to make sure the latest
	// consumption is incorporated.
	tp.waitForRequest()
	tp.mu.Lock()
	defer tp.mu.Unlock()
	return tp.mu.consumption
}

// TokenBucket implements the kvtenant.TokenBucketProvider interface.
func (tp *testProvider) TokenBucket(
	ctx context.Context, in *roachpb.TokenBucketRequest,
) (*roachpb.TokenBucketResponse, error) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	select {
	case <-tp.recvOnRequest:
	default:
	}
	tp.mu.consumption.Add(&in.ConsumptionSinceLastRequest)
	return &roachpb.TokenBucketResponse{}, nil
}

// TestConsumption verifies consumption reporting from a tenant server process.
func TestConsumption(t *testing.T) {
	defer leaktest.AfterTest(t)()

	hostServer, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer hostServer.Stopper().Stop(context.Background())

	st := cluster.MakeTestingClusterSettings()
	tenantcostclient.TargetPeriodSetting.Override(context.Background(), &st.SV, time.Millisecond)
	tenantcostclient.CPUUsageAllowance.Override(context.Background(), &st.SV, 0)

	testProvider := newTestProvider()

	_, tenantDB := serverutils.StartTenant(t, hostServer, base.TestTenantArgs{
		TenantID:                    serverutils.TestTenantID(),
		Settings:                    st,
		AllowSettingClusterSettings: true,
		TestingKnobs: base.TestingKnobs{
			TenantTestingKnobs: &sql.TenantTestingKnobs{
				OverrideTokenBucketProvider: func(kvtenant.TokenBucketProvider) kvtenant.TokenBucketProvider {
					return testProvider
				},
			},
		},
	})
	r := sqlutils.MakeSQLRunner(tenantDB)
	r.Exec(t, "CREATE TABLE t (v STRING)")
	// Do some writes and reads and check the reported consumption. Repeat the
	// test a few times, since background requests can trick the test into
	// passing.
	for repeat := 0; repeat < 5; repeat++ {
		beforeWrite := testProvider.waitForConsumption()
		r.Exec(t, "INSERT INTO t SELECT repeat('1234567890', 1024) FROM generate_series(1, 10) AS g(i)")
		const expectedBytes = 10 * 10 * 1024

		afterWrite := testProvider.waitForConsumption()
		delta := afterWrite
		delta.Sub(&beforeWrite)
		if delta.WriteRequests < 1 || delta.WriteBytes < expectedBytes {
			t.Errorf("usage after write: %s", delta.String())
		}

		r.QueryStr(t, "SELECT min(v) FROM t")

		afterRead := testProvider.waitForConsumption()
		delta = afterRead
		delta.Sub(&afterWrite)
		if delta.ReadRequests < 1 || delta.ReadBytes < expectedBytes {
			t.Errorf("usage after read: %s", delta.String())
		}
		r.Exec(t, "DELETE FROM t WHERE true")
	}
	// Make sure some CPU usage is reported.
	testutils.SucceedsSoon(t, func() error {
		c := testProvider.waitForConsumption()
		if c.SQLPodsCPUSeconds == 0 {
			return errors.New("no CPU usage reported")
		}
		return nil
	})
}
