// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostclient_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl" // ccl init hooks
	"github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl/tenantcostclient"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

// TestDataDriven tests the tenant-side cost controller in an isolated setting.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		defer leaktest.AfterTest(t)()

		var ts testState
		ts.start(t)
		defer ts.stop()

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			args := parseArgs(t, d)
			fn, ok := testStateCommands[d.Cmd]
			if !ok {
				d.Fatalf(t, "unknown command %s", d.Cmd)
			}
			return fn(&ts, t, d, args)
		})
	})
}

type testState struct {
	timeSrc    *timeutil.ManualTime
	settings   *cluster.Settings
	stopper    *stop.Stopper
	provider   *testProvider
	controller multitenant.TenantSideCostController

	// external usage values, accessed using atomic.
	cpuUsage          time.Duration
	pgwireEgressBytes int64

	requestDoneCh map[string]chan struct{}

	eventsCh chan event
}

type event struct {
	time time.Time
	typ  tenantcostclient.TestEventType
}

var _ tenantcostclient.TestInstrumentation = (*testState)(nil)

var eventTypeStr = map[tenantcostclient.TestEventType]string{
	tenantcostclient.TickProcessed:                "tick",
	tenantcostclient.LowRUNotification:            "low-ru",
	tenantcostclient.TokenBucketResponseProcessed: "token-bucket-response",
	tenantcostclient.TokenBucketResponseError:     "token-bucket-response-error",
}

// Event is part of tenantcostclient.TestInstrumentation.
func (ts *testState) Event(now time.Time, typ tenantcostclient.TestEventType) {
	ev := event{
		time: now,
		typ:  typ,
	}
	select {
	case ts.eventsCh <- ev:
		if testing.Verbose() {
			log.Infof(context.Background(), "event %s at %s\n", eventTypeStr[typ], now.Format(timeFormat))
		}
	default:
		panic("events channel full")
	}
}

const timeFormat = "15:04:05.000"

var t0 = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

const timeout = 10 * time.Second

func (ts *testState) start(t *testing.T) {
	ctx := context.Background()

	ts.requestDoneCh = make(map[string]chan struct{})
	ts.eventsCh = make(chan event, 10000)

	ts.timeSrc = timeutil.NewManualTime(t0)

	ts.settings = cluster.MakeTestingClusterSettings()
	// Fix settings so that the defaults can be changed without updating the test.
	tenantcostclient.TargetPeriodSetting.Override(ctx, &ts.settings.SV, 10*time.Second)
	tenantcostclient.CPUUsageAllowance.Override(ctx, &ts.settings.SV, 10*time.Millisecond)

	ts.stopper = stop.NewStopper()
	var err error
	ts.provider = newTestProvider()
	ts.controller, err = tenantcostclient.TestingTenantSideCostController(
		ts.settings,
		roachpb.MakeTenantID(5),
		ts.provider,
		ts.timeSrc,
		ts,
	)
	if err != nil {
		t.Fatal(err)
	}
	externalUsageFn := func(context.Context) multitenant.ExternalUsage {
		return multitenant.ExternalUsage{
			CPUSecs:           time.Duration(atomic.LoadInt64((*int64)(&ts.cpuUsage))).Seconds(),
			PGWireEgressBytes: uint64(atomic.LoadInt64(&ts.pgwireEgressBytes)),
		}
	}
	nextLiveInstanceIDFn := func(ctx context.Context) base.SQLInstanceID {
		return 0
	}
	instanceID := base.SQLInstanceID(1)
	sessionID := sqlliveness.SessionID("foo")
	if err := ts.controller.Start(
		ctx, ts.stopper, instanceID, sessionID, externalUsageFn, nextLiveInstanceIDFn,
	); err != nil {
		t.Fatal(err)
	}
}

func (ts *testState) stop() {
	ts.stopper.Stop(context.Background())
}

type cmdArgs struct {
	count  int64
	bytes  int64
	repeat int64
	label  string
	wait   bool
}

func parseArgs(t *testing.T, d *datadriven.TestData) cmdArgs {
	var res cmdArgs
	res.count = 1
	for _, args := range d.CmdArgs {
		switch args.Key {
		case "count":
			if len(args.Vals) != 1 {
				d.Fatalf(t, "expected one value for count")
			}
			val, err := strconv.Atoi(args.Vals[0])
			if err != nil {
				d.Fatalf(t, "invalid count value")
			}
			res.count = int64(val)

		case "bytes":
			if len(args.Vals) != 1 {
				d.Fatalf(t, "expected one value for bytes")
			}
			val, err := strconv.Atoi(args.Vals[0])
			if err != nil {
				d.Fatalf(t, "invalid bytes value")
			}
			res.bytes = int64(val)

		case "repeat":
			if len(args.Vals) != 1 {
				d.Fatalf(t, "expected one value for repeat")
			}
			val, err := strconv.Atoi(args.Vals[0])
			if err != nil {
				d.Fatalf(t, "invalid repeat value")
			}
			res.repeat = int64(val)

		case "label":
			if len(args.Vals) != 1 || args.Vals[0] == "" {
				d.Fatalf(t, "label requires a value")
			}
			res.label = args.Vals[0]

		case "wait":
			if len(args.Vals) != 1 {
				d.Fatalf(t, "expected one value for wait")
			}
			switch args.Vals[0] {
			case "true":
				res.wait = true
			case "false":
			default:
				d.Fatalf(t, "invalid wait value")
			}
		}
	}
	return res
}

var testStateCommands = map[string]func(
	*testState, *testing.T, *datadriven.TestData, cmdArgs,
) string{
	"read":            (*testState).read,
	"write":           (*testState).write,
	"await":           (*testState).await,
	"not-completed":   (*testState).notCompleted,
	"advance":         (*testState).advance,
	"wait-for-event":  (*testState).waitForEvent,
	"unblock-request": (*testState).unblockRequest,
	"timers":          (*testState).timers,
	"cpu":             (*testState).cpu,
	"pgwire-egress":   (*testState).pgwireEgress,
	"usage":           (*testState).usage,
	"configure":       (*testState).configure,
	"token-bucket":    (*testState).tokenBucket,
}

func (ts *testState) fireRequest(
	t *testing.T, reqInfo tenantcostmodel.RequestInfo, respInfo tenantcostmodel.ResponseInfo,
) chan struct{} {
	ch := make(chan struct{})
	go func() {
		ctx := context.Background()
		if err := ts.controller.OnRequestWait(ctx); err != nil {
			t.Errorf("OnRequestWait error: %v", err)
		}
		ts.controller.OnResponseWait(ctx, reqInfo, respInfo)
		close(ch)
	}()
	return ch
}

func (ts *testState) syncRequest(
	t *testing.T,
	d *datadriven.TestData,
	reqInfo tenantcostmodel.RequestInfo,
	respInfo tenantcostmodel.ResponseInfo,
) {
	select {
	case <-ts.fireRequest(t, reqInfo, respInfo):
	case <-time.After(timeout):
		d.Fatalf(t, "request timed out")
	}
}

func (ts *testState) asyncRequest(
	t *testing.T,
	d *datadriven.TestData,
	reqInfo tenantcostmodel.RequestInfo,
	respInfo tenantcostmodel.ResponseInfo,
	label string,
) {
	if _, ok := ts.requestDoneCh[label]; ok {
		d.Fatalf(t, "label %v already in use", label)
	}

	ts.requestDoneCh[label] = ts.fireRequest(t, reqInfo, respInfo)
}

// request simulates processing a read or write. If a label is provided, the
// request is started in the background.
func (ts *testState) request(
	t *testing.T, d *datadriven.TestData, isWrite bool, args cmdArgs,
) string {
	repeat := args.repeat
	if repeat == 0 {
		repeat = 1
	}

	for ; repeat > 0; repeat-- {
		var writeCount, readCount, writeBytes, readBytes int64
		if isWrite {
			writeCount = args.count
			writeBytes = args.bytes
		} else {
			readCount = args.count
			readBytes = args.bytes
		}
		reqInfo := tenantcostmodel.TestingRequestInfo(1, writeCount, writeBytes)
		respInfo := tenantcostmodel.TestingResponseInfo(!isWrite, readCount, readBytes)
		if args.label == "" {
			ts.syncRequest(t, d, reqInfo, respInfo)
		} else {
			ts.asyncRequest(t, d, reqInfo, respInfo, args.label)
		}
	}
	return ""
}

// read simulates processing a read. If a label is provided, the request is
// started in the background.
func (ts *testState) read(t *testing.T, d *datadriven.TestData, args cmdArgs) string {
	return ts.request(t, d, false /* isWrite */, args)
}

// write simulates processing a write. If a label is provided, the request is
// started in the background.
func (ts *testState) write(t *testing.T, d *datadriven.TestData, args cmdArgs) string {
	return ts.request(t, d, true /* isWrite */, args)
}

// await waits until the given request completes.
func (ts *testState) await(t *testing.T, d *datadriven.TestData, args cmdArgs) string {
	ch, ok := ts.requestDoneCh[args.label]
	if !ok {
		d.Fatalf(t, "unknown label %q", args.label)
	}
	select {
	case <-ch:
	case <-time.After(timeout):
		d.Fatalf(t, "await(%q) timed out", args.label)
	}
	delete(ts.requestDoneCh, args.label)
	return ""
}

// notCompleted verifies that the request with the given label has not completed
// yet.
func (ts *testState) notCompleted(t *testing.T, d *datadriven.TestData, args cmdArgs) string {
	ch, ok := ts.requestDoneCh[args.label]
	if !ok {
		d.Fatalf(t, "unknown label %v", args.label)
	}
	// Sleep a bit to give a chance for a bug to manifest.
	time.Sleep(1 * time.Millisecond)
	select {
	case <-ch:
		d.Fatalf(t, "request %v completed unexpectedly", args.label)
	default:
	}
	return ""
}

// advance advances the clock by the provided duration and returns the new
// current time.
//
//  advance
//  2s
//  ----
//  00:00:02.000
//
// An optional "wait" argument will cause advance to block until it receives a
// tick event, indicating the clock change has been processed.
func (ts *testState) advance(t *testing.T, d *datadriven.TestData, args cmdArgs) string {
	dur, err := time.ParseDuration(d.Input)
	if err != nil {
		d.Fatalf(t, "failed to parse input as duration: %v", err)
	}
	ts.timeSrc.Advance(dur)
	if args.wait {
		// Wait for tick event.
		ts.waitForEvent(t, &datadriven.TestData{Input: "tick"}, cmdArgs{})
	}
	return ts.timeSrc.Now().Format(timeFormat)
}

// waitForEvent waits until the tenant controller reports the given event type,
// at the current time.
func (ts *testState) waitForEvent(t *testing.T, d *datadriven.TestData, args cmdArgs) string {
	typs := make(map[string]tenantcostclient.TestEventType)
	for ev, evStr := range eventTypeStr {
		typs[evStr] = ev
	}
	typ, ok := typs[d.Input]
	if !ok {
		d.Fatalf(t, "unknown event type %s (supported types: %v)", d.Input, typs)
	}

	now := ts.timeSrc.Now()
	for {
		select {
		case ev := <-ts.eventsCh:
			if ev.time == now && ev.typ == typ {
				return ""
			}
			// Else drop the event.

		case <-time.After(timeout):
			d.Fatalf(t, "did not receive event %s", d.Input)
		}
	}
}

// unblockRequest resumes a token bucket request that was blocked by the
// "blockRequest" configuration option.
func (ts *testState) unblockRequest(t *testing.T, d *datadriven.TestData, args cmdArgs) string {
	ts.provider.unblockRequest(t)
	return ""
}

// timers waits for the set of open timers to match the expected output.
// timers is critical to avoid synchronization problems in testing. The command
// outputs the set of timers in increasing order with each timer's deadline on
// its own line.
//
// The following example would wait for there to be two outstanding timers at
// 00:00:01.000 and 00:00:02.000.
//
//  timers
//  ----
//  00:00:01.000
//  00:00:02.000
//
func (ts *testState) timers(t *testing.T, d *datadriven.TestData, args cmdArgs) string {
	// If we are rewriting the test, just sleep a bit before returning the
	// timers.
	if d.Rewrite {
		time.Sleep(time.Second)
		return timesToString(ts.timeSrc.Timers())
	}

	exp := strings.TrimSpace(d.Expected)
	if err := testutils.SucceedsSoonError(func() error {
		got := timesToString(ts.timeSrc.Timers())
		if got != exp {
			return errors.Errorf("got: %q, exp: %q", got, exp)
		}
		return nil
	}); err != nil {
		d.Fatalf(t, "failed to find expected timers: %v", err)
	}
	return d.Expected
}

func timesToString(times []time.Time) string {
	strs := make([]string, len(times))
	for i, t := range times {
		strs[i] = t.Format(timeFormat)
	}
	return strings.Join(strs, "\n")
}

// configure the test provider.
func (ts *testState) configure(t *testing.T, d *datadriven.TestData, args cmdArgs) string {
	var cfg testProviderConfig
	if err := yaml.UnmarshalStrict([]byte(d.Input), &cfg); err != nil {
		d.Fatalf(t, "failed to parse request yaml: %v", err)
	}
	ts.provider.configure(cfg)
	return ""
}

// tokenBucket dumps the current state of the tenant's token bucket.
func (ts *testState) tokenBucket(t *testing.T, d *datadriven.TestData, args cmdArgs) string {
	s := tenantcostclient.TestingTokenBucketString(ts.controller)
	if d.Input != "include-details" {
		// Remove the parenthesized details, since they can be non-deterministic.
		s = strings.Split(s, " (")[0]
	}
	return s
}

// cpu adds CPU usage which will be observed by the controller on the next main
// loop tick.
func (ts *testState) cpu(t *testing.T, d *datadriven.TestData, args cmdArgs) string {
	duration, err := time.ParseDuration(d.Input)
	if err != nil {
		d.Fatalf(t, "error parsing cpu duration: %v", err)
	}
	atomic.AddInt64((*int64)(&ts.cpuUsage), int64(duration))
	return ""
}

// pgwire adds PGWire egress usage which will be observed by the controller on the next
// main loop tick.
func (ts *testState) pgwireEgress(t *testing.T, d *datadriven.TestData, args cmdArgs) string {
	bytes, err := strconv.Atoi(d.Input)
	if err != nil {
		d.Fatalf(t, "error parsing pgwire bytes value: %v", err)
	}
	atomic.AddInt64(&ts.pgwireEgressBytes, int64(bytes))
	return ""
}

// usage prints out the latest consumption. Callers are responsible for
// triggering calls to the token bucket provider and waiting for responses.
func (ts *testState) usage(t *testing.T, d *datadriven.TestData, args cmdArgs) string {
	c := ts.provider.consumption()
	return fmt.Sprintf(""+
		"RU:  %.2f\n"+
		"KVRU:  %.2f\n"+
		"Reads:  %d requests in %d batches (%d bytes)\n"+
		"Writes:  %d requests in %d batches (%d bytes)\n"+
		"SQL Pods CPU seconds:  %.2f\n"+
		"PGWire egress:  %d bytes\n",
		c.RU,
		c.KVRU,
		c.ReadRequests,
		c.ReadBatches,
		c.ReadBytes,
		c.WriteRequests,
		c.WriteBatches,
		c.WriteBytes,
		c.SQLPodsCPUSeconds,
		c.PGWireEgressBytes,
	)
}

// testProvider is a testing implementation of kvtenant.TokenBucketProvider,
type testProvider struct {
	mu struct {
		syncutil.Mutex
		consumption roachpb.TenantConsumption

		lastSeqNum int64

		cfg testProviderConfig
	}
	recvOnRequest chan struct{}
	sendOnRequest chan struct{}
	eventsCh      chan event
}

type testProviderConfig struct {
	// If zero, the provider always grants RUs immediately. If positive, the
	// provider grants RUs at this rate. If negative, the provider never grants
	// RUs.
	Throttle float64 `yaml:"throttle"`

	// If set, the provider always errors out.
	ProviderError bool `yaml:"error"`

	// If set, the provider blocks after receiving each TokenBucket request and
	// waits until unblockRequest is called.
	ProviderBlock bool `yaml:"block"`

	FallbackRate float64 `yaml:"fallback_rate"`
}

var _ kvtenant.TokenBucketProvider = (*testProvider)(nil)

func newTestProvider() *testProvider {
	return &testProvider{
		recvOnRequest: make(chan struct{}),
		sendOnRequest: make(chan struct{}),
	}
}

func (tp *testProvider) configure(cfg testProviderConfig) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	tp.mu.cfg = cfg
}

// waitForRequest waits until the next TokenBucket request.
func (tp *testProvider) waitForRequest(t *testing.T) {
	t.Helper()
	// Try to send through the unbuffered channel, which blocks until TokenBucket
	// is called.
	select {
	case tp.recvOnRequest <- struct{}{}:
	case <-time.After(timeout):
		t.Fatal("did not receive request")
	}
}

func (tp *testProvider) consumption() roachpb.TenantConsumption {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	return tp.mu.consumption
}

// waitForConsumption waits for the next TokenBucket request and returns the
// total consumption.
func (tp *testProvider) waitForConsumption(t *testing.T) roachpb.TenantConsumption {
	tp.waitForRequest(t)
	// it is possible that the TokenBucket request was in the process of being
	// prepared; we have to wait for another one to make sure the latest
	// consumption is incorporated.
	tp.waitForRequest(t)
	return tp.consumption()
}

// unblockRequest unblocks a TokenBucket request that was blocked by the "block"
// configuration option. This is used to test race conditions.
func (tp *testProvider) unblockRequest(t *testing.T) {
	t.Helper()
	// Try to receive through the unbuffered channel, which blocks until
	// TokenBucket sends.
	select {
	case <-tp.sendOnRequest:
	case <-time.After(timeout):
		t.Fatal("did not receive request")
	}
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

	if in.SeqNum <= tp.mu.lastSeqNum {
		panic("non-increasing sequence number")
	}
	tp.mu.lastSeqNum = in.SeqNum

	if tp.mu.cfg.ProviderError {
		return nil, errors.New("injected error")
	}
	if tp.mu.cfg.ProviderBlock {
		// Block until unblockRequest is called.
		select {
		case tp.sendOnRequest <- struct{}{}:
		case <-time.After(timeout):
			return nil, errors.New("TokenBucket was never unblocked")
		}
	}
	tp.mu.consumption.Add(&in.ConsumptionSinceLastRequest)
	res := &roachpb.TokenBucketResponse{}

	rate := tp.mu.cfg.Throttle
	if rate >= 0 {
		res.GrantedRU = in.RequestedRU
		if rate > 0 {
			res.TrickleDuration = time.Duration(in.RequestedRU / rate * float64(time.Second))
			if res.TrickleDuration > in.TargetRequestPeriod {
				res.GrantedRU *= in.TargetRequestPeriod.Seconds() / res.TrickleDuration.Seconds()
				res.TrickleDuration = in.TargetRequestPeriod
			}
		}
	}
	res.FallbackRate = tp.mu.cfg.FallbackRate

	return res, nil
}

// TestConsumption verifies consumption reporting from a tenant server process.
func TestConsumption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	// Create a secondary index to ensure that writes to both indexes are
	// recorded in metrics.
	r.Exec(t, "CREATE TABLE t (v STRING, w STRING, INDEX (w, v))")
	// Do some writes and reads and check the reported consumption. Repeat the
	// test a few times, since background requests can trick the test into
	// passing.
	for repeat := 0; repeat < 5; repeat++ {
		beforeWrite := testProvider.waitForConsumption(t)
		r.Exec(t, "INSERT INTO t (v) SELECT repeat('1234567890', 1024) FROM generate_series(1, 10) AS g(i)")
		const expectedBytes = 10 * 10 * 1024

		afterWrite := testProvider.waitForConsumption(t)
		delta := afterWrite
		delta.Sub(&beforeWrite)
		if delta.WriteBatches < 1 || delta.WriteRequests < 2 || delta.WriteBytes < expectedBytes*2 {
			t.Errorf("usage after write: %s", delta.String())
		}

		r.QueryStr(t, "SELECT min(v) FROM t")

		afterRead := testProvider.waitForConsumption(t)
		delta = afterRead
		delta.Sub(&afterWrite)
		if delta.ReadBatches < 1 || delta.ReadRequests < 1 || delta.ReadBytes < expectedBytes {
			t.Errorf("usage after read: %s", delta.String())
		}
		r.Exec(t, "DELETE FROM t WHERE true")
	}
	// Make sure some CPU usage is reported.
	testutils.SucceedsSoon(t, func() error {
		c := testProvider.waitForConsumption(t)
		if c.SQLPodsCPUSeconds == 0 {
			return errors.New("no CPU usage reported")
		}
		if c.PGWireEgressBytes == 0 {
			return errors.New("no pgwire egress bytes reported")
		}
		return nil
	})
}

// TestSQLLivenessExemption verifies that the operations done by the sqlliveness
// subsystem are exempt from cost control.
func TestSQLLivenessExemption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	hostServer, hostDB, hostKV := serverutils.StartServer(t, base.TestServerArgs{})
	defer hostServer.Stopper().Stop(context.Background())

	tenantID := serverutils.TestTenantID()

	// Create a tenant with ridiculously low resource limits.
	host := sqlutils.MakeSQLRunner(hostDB)
	host.Exec(t, "SELECT crdb_internal.create_tenant($1)", tenantID.ToUint64())
	host.Exec(t, "SELECT crdb_internal.update_tenant_resource_limits($1, 0, 0.001, 0, now(), 0)", tenantID.ToUint64())

	st := cluster.MakeTestingClusterSettings()
	// Make the tenant heartbeat like crazy.
	ctx := context.Background()
	//slinstance.DefaultTTL.Override(ctx, &st.SV, 20*time.Millisecond)
	slinstance.DefaultHeartBeat.Override(ctx, &st.SV, time.Millisecond)

	_, tenantDB := serverutils.StartTenant(t, hostServer, base.TestTenantArgs{
		Existing:                    true,
		TenantID:                    tenantID,
		Settings:                    st,
		AllowSettingClusterSettings: true,
	})

	r := sqlutils.MakeSQLRunner(tenantDB)
	_ = r

	codec := keys.MakeSQLCodec(tenantID)
	key := codec.IndexPrefix(keys.SqllivenessID, 1)

	// livenessValue returns the KV value for the one row in the
	// system.sqlliveness table. The value contains the session expiration time
	// which changes with every heartbeat.
	livenessValue := func() []byte {
		kvs, err := hostKV.Scan(ctx, key, key.PrefixEnd(), 1)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if len(kvs) != 1 {
			t.Fatal("no entry in system.liveness")
		}
		return kvs[0].Value.RawBytes
	}

	// Verify that heartbeats can go through and update the expiration time.
	val := livenessValue()
	time.Sleep(2 * time.Millisecond)
	testutils.SucceedsSoon(
		t,
		func() error {
			if newValue := livenessValue(); bytes.Equal(val, newValue) {
				return errors.New("liveness row did not change)")
			}
			return nil
		},
	)
}
