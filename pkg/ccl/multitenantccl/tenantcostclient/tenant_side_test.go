// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostclient_test

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl" // ccl init hooks
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl/tenantcostclient"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/cloud/nullsink"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant/multitenantio"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

// TestDataDriven tests the tenant-side cost controller in an isolated setting.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		defer leaktest.AfterTest(t)()
		defer log.Scope(t).Close(t)

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
	eventWait  *eventWaiter

	// external usage values, accessed using atomic.
	cpuUsage          time.Duration
	pgwireEgressBytes int64

	requestDoneCh map[string]chan struct{}
}

var eventTypeStr = map[tenantcostclient.TestEventType]string{
	tenantcostclient.TickProcessed:                "tick",
	tenantcostclient.LowTokensNotification:        "low-tokens",
	tenantcostclient.TokenBucketResponseProcessed: "token-bucket-response",
	tenantcostclient.TokenBucketResponseError:     "token-bucket-response-error",
}

const timeFormat = "15:04:05.000"

var t0 = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

const timeout = 10 * time.Second

func (ts *testState) start(t *testing.T) {
	ctx := context.Background()

	ts.requestDoneCh = make(map[string]chan struct{})

	ts.timeSrc = timeutil.NewManualTime(t0)
	ts.eventWait = newEventWaiter(ts.timeSrc)

	ts.settings = cluster.MakeTestingClusterSettings()
	// Fix settings so that the defaults can be changed without updating the test.
	tenantcostclient.TargetPeriodSetting.Override(ctx, &ts.settings.SV, 10*time.Second)
	tenantcostclient.CPUUsageAllowance.Override(ctx, &ts.settings.SV, 10*time.Millisecond)
	tenantcostclient.InitialRequestSetting.Override(ctx, &ts.settings.SV, 10000)

	networkCosts := `{"regionPairs": [
		{"fromRegion": "us-central1", "toRegion": "europe-west1", "cost": 0.01},
		{"fromRegion": "europe-west1", "toRegion": "us-central1", "cost": 0.00002}
	]}`
	tenantcostmodel.CrossRegionNetworkCostSetting.Override(ctx, &ts.settings.SV, networkCosts)

	ts.stopper = stop.NewStopper()
	var err error
	ts.provider = newTestProvider(ts.timeSrc)
	ts.controller, err = tenantcostclient.TestingTenantSideCostController(
		ts.settings,
		roachpb.MustMakeTenantID(5),
		ts.provider,
		&tenantcostclient.TestNodeDescStore{NodeDescs: nodeDescriptors},
		nodeDescriptors[0].Locality,
		ts.timeSrc,
		ts.eventWait,
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

	// Wait for main loop to start in order to avoid race conditions where a test
	// starts before the main loop has been initialized.
	if !ts.eventWait.WaitForEvent(tenantcostclient.MainLoopStarted) {
		t.Fatal("did not receive event MainLoopStarted")
	}
}

func (ts *testState) stop() {
	ts.stopper.Stop(context.Background())
}

type cmdArgs struct {
	count     int64
	bytes     int64
	repeat    int64
	label     string
	wait      bool
	rangeDesc *roachpb.RangeDescriptor
}

func parseBytesVal(arg datadriven.CmdArg) (int64, error) {
	if len(arg.Vals) != 1 {
		return 0, errors.Newf("expected one value for bytes")
	}
	val, err := strconv.ParseInt(arg.Vals[0], 10, 64)
	if err != nil {
		return 0, errors.Wrap(err, "could not convert value to integer")
	}
	return val, nil
}

func parseArgs(t *testing.T, d *datadriven.TestData) cmdArgs {
	var res cmdArgs
	res.count = 1
	res.rangeDesc = &rangeWithOneReplica
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
			v, err := parseBytesVal(args)
			if err != nil {
				d.Fatalf(t, "%s", err)
			}
			res.bytes = v

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

		case "localities":
			if len(args.Vals) != 1 {
				d.Fatalf(t, "expected one value for localities")
			}
			switch args.Vals[0] {
			case "remote-region":
				res.rangeDesc = &rangeWithRemoteReplica
			case "same-zone":
				res.rangeDesc = &rangeInSameZone
			case "cross-zone":
				res.rangeDesc = &rangeAcrossZones
			case "cross-region":
				res.rangeDesc = &rangeAcrossRegions
			default:
				d.Fatalf(t, "localities must be 'same-zone', 'cross-zone', or 'cross-region'")
			}

		default:
			d.Fatalf(t, "unknown command: '%s'", args.Key)
		}
	}
	return res
}

var testStateCommands = map[string]func(
	*testState, *testing.T, *datadriven.TestData, cmdArgs,
) string{
	"read":              (*testState).read,
	"write":             (*testState).write,
	"await":             (*testState).await,
	"not-completed":     (*testState).notCompleted,
	"advance":           (*testState).advance,
	"wait-for-event":    (*testState).waitForEvent,
	"timers":            (*testState).timers,
	"cpu":               (*testState).cpu,
	"pgwire-egress":     (*testState).pgwireEgress,
	"external-egress":   (*testState).externalEgress,
	"external-ingress":  (*testState).externalIngress,
	"usage":             (*testState).usage,
	"metrics":           (*testState).metrics,
	"configure":         (*testState).configure,
	"token-bucket":      (*testState).tokenBucket,
	"unblock-request":   (*testState).unblockRequest,
	"provisioned-vcpus": (*testState).provisionedVcpus,
}

// runOperation invokes the given operation function on a background goroutine.
// If label is empty, runOperation will synchronously wait for the operation to
// complete. Otherwise, it will enter the label in the requestDoneCh map so that
// the caller can wait for it to complete.
func (ts *testState) runOperation(t *testing.T, d *datadriven.TestData, label string, op func()) {
	runInBackground := func(op func()) chan struct{} {
		ch := make(chan struct{})
		go func() {
			op()
			close(ch)
		}()
		return ch
	}

	if label != "" {
		// Async case.
		if _, ok := ts.requestDoneCh[label]; ok {
			d.Fatalf(t, "label %v already in use", label)
		}

		ts.requestDoneCh[label] = runInBackground(op)
	} else {
		// Sync case.
		select {
		case <-runInBackground(op):
		case <-time.After(timeout):
			d.Fatalf(t, "request timed out")
		}
	}
}

// request simulates processing a read or write. If a label is provided, the
// request is started in the background.
func (ts *testState) request(
	t *testing.T, d *datadriven.TestData, isWrite bool, args cmdArgs,
) string {
	ctx := context.Background()
	repeat := args.repeat
	if repeat == 0 {
		repeat = 1
	}

	var batchInfo tenantcostmodel.BatchInfo
	if isWrite {
		batchInfo.WriteCount = args.count
		batchInfo.WriteBytes = args.bytes
	} else {
		batchInfo.ReadCount = args.count
		batchInfo.ReadBytes = args.bytes
	}
	req, resp := makeTestBatch(batchInfo)

	targetReplica := &args.rangeDesc.InternalReplicas[0]

	for ; repeat > 0; repeat-- {
		ts.runOperation(t, d, args.label, func() {
			if err := ts.controller.OnRequestWait(ctx); err != nil {
				t.Errorf("OnRequestWait error: %v", err)
			}
			if err := ts.controller.OnResponseWait(ctx, req, resp, args.rangeDesc, targetReplica); err != nil {
				t.Errorf("OnResponseWait error: %v", err)
			}
		})
	}
	return ""
}

func (ts *testState) externalIngress(t *testing.T, _ *datadriven.TestData, args cmdArgs) string {
	usage := multitenant.ExternalIOUsage{IngressBytes: args.bytes}
	if err := ts.controller.OnExternalIOWait(context.Background(), usage); err != nil {
		t.Errorf("OnExternalIOWait error: %s", err)
	}
	return ""
}

func (ts *testState) externalEgress(t *testing.T, d *datadriven.TestData, args cmdArgs) string {
	ctx := context.Background()
	usage := multitenant.ExternalIOUsage{EgressBytes: args.bytes}
	ts.runOperation(t, d, args.label, func() {
		if err := ts.controller.OnExternalIOWait(ctx, usage); err != nil {
			t.Errorf("OnExternalIOWait error: %s", err)
		}
	})
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
//	advance
//	2s
//	----
//	00:00:02.000
//
// An optional "wait" argument will cause advance to block until it receives a
// tick event, indicating the clock change has been processed.
func (ts *testState) advance(t *testing.T, d *datadriven.TestData, args cmdArgs) string {
	ctx := context.Background()
	dur, err := time.ParseDuration(d.Input)
	if err != nil {
		d.Fatalf(t, "failed to parse input as duration: %v", err)
	}
	if log.ExpensiveLogEnabled(ctx, 1) {
		log.Infof(ctx, "Advance %v", dur)
	}
	ts.timeSrc.Advance(dur)
	if args.wait {
		// Wait for tick event.
		ts.waitForEvent(t, &datadriven.TestData{Input: "tick"}, cmdArgs{})
	}
	return ts.timeSrc.Now().Format(timeFormat)
}

// waitForEvent waits until the tenant controller reports the given event
// type(s), at the current time.
func (ts *testState) waitForEvent(t *testing.T, d *datadriven.TestData, _ cmdArgs) string {
	typs := make(map[string]tenantcostclient.TestEventType)
	for ev, evStr := range eventTypeStr {
		typs[evStr] = ev
	}

	typ, ok := typs[d.Input]
	if !ok {
		d.Fatalf(t, "unknown event type %s (supported types: %v)", d.Input, typs)
	}

	if !ts.eventWait.WaitForEvent(typ) {
		d.Fatalf(t, "did not receive event %s", d.Input)
	}

	return ""
}

// unblockRequest resumes a token bucket request that was blocked by the
// "blockRequest" configuration option.
func (ts *testState) unblockRequest(t *testing.T, _ *datadriven.TestData, _ cmdArgs) string {
	ts.provider.unblockRequest(t)
	return ""
}

// provisionedVcpus sets the number of vCPUs available to the virtual cluster,
// for use in estimating CPU. If zero, the RU model is used instead.
func (ts *testState) provisionedVcpus(t *testing.T, _ *datadriven.TestData, args cmdArgs) string {
	ctx := context.Background()
	tenantcostclient.ProvisionedVcpusSetting.Override(ctx, &ts.settings.SV, args.count)
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
//	timers
//	----
//	00:00:01.000
//	00:00:02.000
func (ts *testState) timers(t *testing.T, d *datadriven.TestData, _ cmdArgs) string {
	// If we are rewriting the test, just sleep a bit before returning the
	// timers.
	if d.Rewrite {
		time.Sleep(time.Second)
		return timesToString(ts.timeSrc.Timers())
	}

	exp := strings.TrimSpace(d.Expected)
	if err := testutils.SucceedsWithinError(func() error {
		got := timesToString(ts.timeSrc.Timers())
		if got != exp {
			return errors.Errorf("got: %q, exp: %q", got, exp)
		}
		return nil
	}, timeout); err != nil {
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
func (ts *testState) configure(t *testing.T, d *datadriven.TestData, _ cmdArgs) string {
	var cfg testProviderConfig
	if err := yaml.UnmarshalStrict([]byte(d.Input), &cfg); err != nil {
		d.Fatalf(t, "failed to parse request yaml: %v", err)
	}
	ts.provider.configure(cfg)
	return ""
}

// tokenBucket dumps the current state of the tenant's token bucket.
func (ts *testState) tokenBucket(*testing.T, *datadriven.TestData, cmdArgs) string {
	return tenantcostclient.TestingTokenBucketString(ts.controller)
}

// cpu adds CPU usage which will be observed by the controller on the next main
// loop tick.
func (ts *testState) cpu(t *testing.T, d *datadriven.TestData, _ cmdArgs) string {
	duration, err := time.ParseDuration(d.Input)
	if err != nil {
		d.Fatalf(t, "error parsing cpu duration: %v", err)
	}
	atomic.AddInt64((*int64)(&ts.cpuUsage), int64(duration))
	return ""
}

// pgwire adds PGWire egress usage which will be observed by the controller on the next
// main loop tick.
func (ts *testState) pgwireEgress(t *testing.T, d *datadriven.TestData, _ cmdArgs) string {
	bytes, err := strconv.Atoi(d.Input)
	if err != nil {
		d.Fatalf(t, "error parsing pgwire bytes value: %v", err)
	}
	atomic.AddInt64(&ts.pgwireEgressBytes, int64(bytes))
	return ""
}

// usage prints out the latest consumption. Callers are responsible for
// triggering calls to the token bucket provider and waiting for responses.
func (ts *testState) usage(*testing.T, *datadriven.TestData, cmdArgs) string {
	c := ts.provider.consumption()
	return fmt.Sprintf(""+
		"RU: %.2f\n"+
		"KVRU: %.2f\n"+
		"CrossRegionNetworkRU: %.2f\n"+
		"Reads: %d requests in %d batches (%d bytes)\n"+
		"Writes: %d requests in %d batches (%d bytes)\n"+
		"SQL Pods CPU seconds: %.2f\n"+
		"PGWire egress: %d bytes\n"+
		"ExternalIO egress: %d bytes\n"+
		"ExternalIO ingress: %d bytes\n"+
		"Estimated CPU seconds: %.2f\n",
		c.RU,
		c.KVRU,
		c.CrossRegionNetworkRU,
		c.ReadRequests,
		c.ReadBatches,
		c.ReadBytes,
		c.WriteRequests,
		c.WriteBatches,
		c.WriteBytes,
		c.SQLPodsCPUSeconds,
		c.PGWireEgressBytes,
		c.ExternalIOEgressBytes,
		c.ExternalIOIngressBytes,
		c.EstimatedCPUSeconds,
	)
}

// metrics prints out cost client related consumption metrics. Callers are
// responsible for waiting on tick events since that is when metrics will be
// updated.
func (ts *testState) metrics(*testing.T, *datadriven.TestData, cmdArgs) string {
	metricNames := []string{
		"tenant.sql_usage.request_units",
		"tenant.sql_usage.kv_request_units",
		"tenant.sql_usage.read_batches",
		"tenant.sql_usage.read_requests",
		"tenant.sql_usage.read_bytes",
		"tenant.sql_usage.write_batches",
		"tenant.sql_usage.write_requests",
		"tenant.sql_usage.write_bytes",
		"tenant.sql_usage.sql_pods_cpu_seconds",
		"tenant.sql_usage.pgwire_egress_bytes",
		"tenant.sql_usage.external_io_ingress_bytes",
		"tenant.sql_usage.external_io_egress_bytes",
		"tenant.sql_usage.cross_region_network_ru",
		"tenant.sql_usage.estimated_kv_cpu_seconds",
		"tenant.sql_usage.estimated_cpu_seconds",
		"tenant.sql_usage.estimated_replication_bytes",
		"tenant.sql_usage.provisioned_vcpus",
	}
	var childMetrics string
	state := make(map[string]interface{})
	v := reflect.ValueOf(ts.controller.Metrics()).Elem()
	for i := 0; i < v.NumField(); i++ {
		vfield := v.Field(i)
		if !vfield.CanInterface() {
			continue
		}
		switch typ := vfield.Interface().(type) {
		case metric.Iterable:
			typ.Inspect(func(v interface{}) {
				switch it := v.(type) {
				case *metric.Gauge:
					state[typ.GetName()] = it.Value()
				case *metric.Counter:
					state[typ.GetName()] = it.Count()
				case *metric.CounterFloat64:
					state[typ.GetName()] = fmt.Sprintf("%.2f", it.Count())
				case *aggmetric.AggCounter:
					state[typ.GetName()] = it.Count()
					promIter, ok := v.(metric.PrometheusIterable)
					if !ok {
						return
					}
					promIter.Each(it.GetLabels(), func(m *prometheusgo.Metric) {
						childMetrics += fmt.Sprintf("%s{", typ.GetName())
						for i, l := range m.Label {
							if i > 0 {
								childMetrics += ","
							}
							childMetrics += fmt.Sprintf(`%s="%s"`, l.GetName(), l.GetValue())
						}
						childMetrics += fmt.Sprintf("}: %.0f\n", m.Counter.GetValue())
					})
				}
			})
		}
	}
	var output string
	for _, name := range metricNames {
		v, ok := state[name]
		if !ok {
			panic(fmt.Sprintf("missing data for metric %q", name))
		}
		output += fmt.Sprintf("%s: %v\n", name, v)
	}
	output += childMetrics
	return output
}

type event struct {
	time time.Time
	typ  tenantcostclient.TestEventType
}

type eventWaiter struct {
	timeSrc *timeutil.ManualTime
	ch      chan event
}

var _ tenantcostclient.TestInstrumentation = (*eventWaiter)(nil)

func newEventWaiter(timeSrc *timeutil.ManualTime) *eventWaiter {
	return &eventWaiter{timeSrc: timeSrc, ch: make(chan event, 10000)}
}

// Event implements the TestInstrumentation interface.
func (ew *eventWaiter) Event(now time.Time, typ tenantcostclient.TestEventType) {
	ev := event{
		time: now,
		typ:  typ,
	}
	select {
	case ew.ch <- ev:
		if testing.Verbose() {
			log.Infof(context.Background(), "event %s at %s\n",
				eventTypeStr[typ], now.Format(timeFormat))
		}
	default:
		panic("events channel full")
	}
}

// WaitForEvent returns true if it receives the given event type at the current
// time. If it fails to do this before timeout, it returns false.
func (ew *eventWaiter) WaitForEvent(typ tenantcostclient.TestEventType) bool {
	now := ew.timeSrc.Now()
	for {
		select {
		case ev := <-ew.ch:
			if ev.time == now && ev.typ == typ {
				return true
			}
			// Else drop the event.

		case <-time.After(timeout):
			return false
		}
	}
}

// testProvider is a testing implementation of kvtenant.TokenBucketProvider,
type testProvider struct {
	mu struct {
		syncutil.Mutex
		consumption kvpb.TenantConsumption

		lastSeqNum int64
		lastTime   time.Time

		cfg testProviderConfig
	}
	timeSource    timeutil.TimeSource
	recvOnRequest chan struct{}
	sendOnRequest chan struct{}
}

type testProviderConfig struct {
	// If zero, the provider always grants tokens immediately. If positive, the
	// provider grants tokens at this rate. If negative, the provider never grants
	// tokens.
	Throttle float64 `yaml:"throttle"`

	// If set, the provider always errors out.
	ProviderError bool `yaml:"error"`

	// If set, the provider blocks after receiving each TokenBucket request and
	// waits until unblockRequest is called.
	ProviderBlock bool `yaml:"block"`

	FallbackRate float64 `yaml:"fallback_rate"`

	WriteBatchRate   float64 `yaml:"write_batch_rate"`
	EstimatedCPURate float64 `yaml:"estimated_cpu_rate"`
}

var _ kvtenant.TokenBucketProvider = (*testProvider)(nil)

func newTestProvider(timeSource timeutil.TimeSource) *testProvider {
	return &testProvider{
		timeSource:    timeSource,
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

func (tp *testProvider) consumption() kvpb.TenantConsumption {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	return tp.mu.consumption
}

// waitForConsumption waits for the next TokenBucket request and returns the
// total consumption.
func (tp *testProvider) waitForConsumption(t *testing.T) kvpb.TenantConsumption {
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
	_ context.Context, in *kvpb.TokenBucketRequest,
) (*kvpb.TokenBucketResponse, error) {
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

	now := tp.timeSource.Now()
	elapsed := now.Sub(tp.mu.lastTime)
	if elapsed != 0 && !tp.mu.lastTime.IsZero() && in.ConsumptionPeriod == 0 {
		panic("zero-length consumption period")
	}
	tp.mu.lastTime = now

	tp.mu.consumption.Add(&in.ConsumptionSinceLastRequest)
	res := &kvpb.TokenBucketResponse{}

	rate := tp.mu.cfg.Throttle
	if rate >= 0 {
		res.GrantedTokens = in.RequestedTokens
		if rate > 0 {
			res.TrickleDuration = time.Duration(in.RequestedTokens / rate * float64(time.Second))
			if res.TrickleDuration > in.TargetRequestPeriod {
				res.GrantedTokens *= in.TargetRequestPeriod.Seconds() / res.TrickleDuration.Seconds()
				res.TrickleDuration = in.TargetRequestPeriod
			}
		}
	}
	res.FallbackRate = tp.mu.cfg.FallbackRate
	res.ConsumptionRates.WriteBatchRate = tp.mu.cfg.WriteBatchRate
	res.ConsumptionRates.EstimatedCPURate = tp.mu.cfg.EstimatedCPURate
	return res, nil
}

var nodeDescriptors = []roachpb.NodeDescriptor{
	// Starting region and zone.
	{NodeID: 1, Locality: roachpb.Locality{Tiers: []roachpb.Tier{
		{Key: "region", Value: "us-central1"},
		{Key: "zone", Value: "az1"},
	}}},
	// Different zone.
	{NodeID: 2, Locality: roachpb.Locality{Tiers: []roachpb.Tier{
		{Key: "region", Value: "us-central1"},
		{Key: "zone", Value: "az2"},
	}}},
	// Same zone.
	{NodeID: 3, Locality: roachpb.Locality{Tiers: []roachpb.Tier{
		{Key: "region", Value: "us-central1"},
		{Key: "zone", Value: "az1"},
	}}},
	// Different region.
	{NodeID: 4, Locality: roachpb.Locality{Tiers: []roachpb.Tier{
		{Key: "region", Value: "europe-west1"},
		{Key: "zone", Value: "az1"},
	}}},
	// Different region, another zone.
	{NodeID: 5, Locality: roachpb.Locality{Tiers: []roachpb.Tier{
		{Key: "region", Value: "europe-west1"},
		{Key: "zone", Value: "az2"},
	}}},
}

var rangeWithOneReplica = roachpb.RangeDescriptor{
	RangeID: 10,
	InternalReplicas: []roachpb.ReplicaDescriptor{
		{NodeID: 1, ReplicaID: 100},
	},
}

var rangeWithRemoteReplica = roachpb.RangeDescriptor{
	RangeID: 20,
	InternalReplicas: []roachpb.ReplicaDescriptor{
		{NodeID: 4, ReplicaID: 110},
	},
}

var rangeInSameZone = roachpb.RangeDescriptor{
	RangeID: 30,
	InternalReplicas: []roachpb.ReplicaDescriptor{
		{NodeID: 1, ReplicaID: 120},
		{NodeID: 1, ReplicaID: 121},
		{NodeID: 3, ReplicaID: 122},
	},
}

var rangeAcrossZones = roachpb.RangeDescriptor{
	RangeID: 40,
	InternalReplicas: []roachpb.ReplicaDescriptor{
		{NodeID: 1, ReplicaID: 130},
		{NodeID: 2, ReplicaID: 131},
		{NodeID: 3, ReplicaID: 132},
	},
}

var rangeAcrossRegions = roachpb.RangeDescriptor{
	RangeID: 50,
	InternalReplicas: []roachpb.ReplicaDescriptor{
		{NodeID: 1, ReplicaID: 140},
		{NodeID: 2, ReplicaID: 141},
		{NodeID: 3, ReplicaID: 142},
		{NodeID: 4, ReplicaID: 143},
		{NodeID: 5, ReplicaID: 144},
	},
}

// makeTestBatch constructs a batch request and response that has the given
// number of read/write requests and bytes.
func makeTestBatch(
	bi tenantcostmodel.BatchInfo,
) (req *kvpb.BatchRequest, resp *kvpb.BatchResponse) {
	req = &kvpb.BatchRequest{}
	resp = &kvpb.BatchResponse{}

	for writeCount := bi.WriteCount; writeCount > 0; writeCount-- {
		byteCount := bi.WriteBytes / bi.WriteCount
		if writeCount == 1 {
			byteCount += bi.WriteBytes % bi.WriteCount
		}

		putReq := &kvpb.PutRequest{Value: roachpb.Value{RawBytes: make([]byte, byteCount)}}
		req.Requests = append(req.Requests,
			kvpb.RequestUnion{Value: &kvpb.RequestUnion_Put{Put: putReq}})

		resp.Responses = append(resp.Responses,
			kvpb.ResponseUnion{Value: &kvpb.ResponseUnion_Put{Put: &kvpb.PutResponse{}}})
	}

	for readCount := bi.ReadCount; readCount > 0; readCount-- {
		byteCount := bi.ReadBytes / bi.ReadCount
		if readCount == 1 {
			byteCount += bi.ReadBytes % bi.ReadCount
		}

		req.Requests = append(req.Requests,
			kvpb.RequestUnion{Value: &kvpb.RequestUnion_Get{Get: &kvpb.GetRequest{}}})

		getResp := &kvpb.GetResponse{ResponseHeader: kvpb.ResponseHeader{NumBytes: byteCount}}
		resp.Responses = append(resp.Responses,
			kvpb.ResponseUnion{Value: &kvpb.ResponseUnion_Get{Get: getResp}})
	}

	return req, resp
}

// TestWaitingTokens verifies that multiple concurrent requests that stack up in
// the quota pool are reflected in AvailableTokens.
func TestWaitingTokens(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Disable CPU consumption so that it doesn't interfere with test.
	st := cluster.MakeTestingClusterSettings()
	tenantcostclient.CPUUsageAllowance.Override(ctx, &st.SV, time.Second)

	testProvider := newTestProvider(timeutil.DefaultTimeSource{})
	testProvider.configure(testProviderConfig{ProviderError: true})

	tenantID := serverutils.TestTenantID()
	timeSource := timeutil.NewManualTime(t0)
	eventWait := newEventWaiter(timeSource)
	ctrl, err := tenantcostclient.TestingTenantSideCostController(
		st, tenantID, testProvider, &tenantcostclient.TestNodeDescStore{NodeDescs: nodeDescriptors},
		roachpb.Locality{}, timeSource, eventWait)
	require.NoError(t, err)

	// Immediately consume the initial 5K tokens.
	req, resp := makeTestBatch(tenantcostmodel.BatchInfo{WriteCount: 1, WriteBytes: 5117945})
	require.NoError(t, ctrl.OnResponseWait(
		ctx, req, resp, &rangeWithOneReplica, &rangeWithOneReplica.InternalReplicas[0]))

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	externalUsage := func(ctx context.Context) multitenant.ExternalUsage {
		return multitenant.ExternalUsage{}
	}
	nextLiveInstanceID := func(ctx context.Context) base.SQLInstanceID { return 2 }
	require.NoError(t, ctrl.Start(
		ctx, stopper, 1, "test", externalUsage, nextLiveInstanceID))

	// Wait for the initial token bucket response.
	require.True(t, eventWait.WaitForEvent(tenantcostclient.TokenBucketResponseError))

	// Send 20 KV requests for 1K tokens each.
	const count = 20
	const fillRate = 100
	req, resp = makeTestBatch(tenantcostmodel.BatchInfo{WriteCount: 1, WriteBytes: 1021946})

	testutils.SucceedsWithin(t, func() error {
		// Refill the token bucket at a fixed 100 tokens/s so that we can limit
		// non-determinism in the test.
		tenantcostclient.TestingSetRate(ctrl, fillRate)

		var doneCount int64
		for i := 0; i < count; i++ {
			require.NoError(t, ctrl.OnRequestWait(ctx))
		}
		for i := 0; i < count; i++ {
			go func(i int) {
				require.NoError(t, ctrl.OnResponseWait(
					ctx, req, resp, &rangeWithOneReplica, &rangeWithOneReplica.InternalReplicas[0]))
				atomic.AddInt64(&doneCount, 1)
			}(i)
		}

		// Allow some responses to queue up before refilling the available tokens.
		time.Sleep(time.Millisecond)

		// If available tokens drop below -1K, then multiple responses must be
		// waiting.
		succeeded := false
		for i := 0; i < count; i++ {
			available := tenantcostclient.TestingAvailableTokens(ctrl)
			if available < -1000 {
				succeeded = true
			}

			timeSource.Advance(10 * time.Second)
			require.True(t, eventWait.WaitForEvent(tenantcostclient.TickProcessed))
		}

		// Wait for all background goroutines to finish. It's necessary to
		// advance time while waiting in order to resolve a race condition: the
		// quota pool gets a "tryAgainAfter" value that gets added to the current
		// time in order to set a retry timer. However, the current time might be
		// updated at any instant by the foreground goroutine. Therefore, it's
		// possible for there to be sufficient tokens in the bucket, and yet have
		// one or more background goroutines waiting for them.
		testutils.SucceedsWithin(t, func() error {
			if atomic.LoadInt64(&doneCount) == count {
				return nil
			}
			// Zero out bucket fill rate so that advancing time does not grant
			// new tokens.
			tenantcostclient.TestingSetRate(ctrl, 0)
			timeSource.Advance(10 * time.Second)
			time.Sleep(time.Millisecond)
			return errors.Errorf("waiting for background goroutines to finish (now=%v, timers=%v)",
				timeSource.Now(), timesToString(timeSource.Timers()))
		}, timeout)

		const allowedDelta = 0.01
		available := tenantcostclient.TestingAvailableTokens(ctrl)
		if succeeded {
			require.InDelta(t, 0, available, allowedDelta)
			return nil
		}

		return errors.Errorf("Tokens did not drop below 1K: %0.2f", available)
	}, 2*time.Minute)
}

// TestConsumption verifies consumption reporting from a tenant server process.
func TestConsumption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	hostServer := serverutils.StartServerOnly(t, base.TestServerArgs{DefaultTestTenant: base.TestControlsTenantsExplicitly})
	defer hostServer.Stopper().Stop(context.Background())

	const targetPeriod = time.Millisecond
	st := cluster.MakeTestingClusterSettings()
	tenantcostclient.TargetPeriodSetting.Override(context.Background(), &st.SV, targetPeriod)
	tenantcostclient.CPUUsageAllowance.Override(context.Background(), &st.SV, 0)

	testProvider := newTestProvider(timeutil.DefaultTimeSource{})

	_, tenantDB := serverutils.StartTenant(t, hostServer, base.TestTenantArgs{
		TenantID: serverutils.TestTenantID(),
		Settings: st,
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

	// Do some writes and reads and check reported consumption for each model.
	for _, useRUModel := range []bool{true, false} {
		t.Run(fmt.Sprintf("ru_model=%v", useRUModel), func(t *testing.T) {
			// Repeat the test a few times, since background requests can trick the
			// test into passing.
			for repeat := 0; repeat < 5; repeat++ {
				if !useRUModel {
					// Switch to estimated CPU model.
					tenantcostclient.ProvisionedVcpusSetting.Override(context.Background(), &st.SV, 12)
				}

				beforeWrite := testProvider.waitForConsumption(t)
				r.Exec(t, "INSERT INTO t (v) SELECT repeat('1234567890', 1024) FROM generate_series(1, 10) AS g(i)")
				const expectedBytes = 10 * 10 * 1024

				// Try a few times because background activity can trigger bucket
				// requests before the test query does.
				testutils.SucceedsSoon(t, func() error {
					afterWrite := testProvider.waitForConsumption(t)
					delta := afterWrite
					delta.Sub(&beforeWrite)
					if useRUModel {
						if delta.RU < 1 || delta.KVRU < 1 ||
							delta.WriteBatches < 1 || delta.WriteRequests < 2 || delta.WriteBytes < expectedBytes*2 {
							return errors.Newf("usage after write: %s", delta.String())
						}
					} else {
						if delta.EstimatedCPUSeconds == 0 {
							return errors.Newf("usage after write: %s", delta.String())
						}
					}
					return nil
				})

				beforeRead := testProvider.waitForConsumption(t)
				r.QueryStr(t, "SELECT min(v) FROM t")

				// Try a few times because background activity can trigger bucket
				// requests before the test query does.
				testutils.SucceedsSoon(t, func() error {
					afterRead := testProvider.waitForConsumption(t)
					delta := afterRead
					delta.Sub(&beforeRead)
					if useRUModel {
						if delta.ReadBatches < 1 || delta.ReadRequests < 1 || delta.ReadBytes < expectedBytes {
							return errors.Newf("usage after read: %s", delta.String())
						}
					} else {
						if delta.EstimatedCPUSeconds == 0 {
							return errors.Newf("usage after read: %s", delta.String())
						}
					}
					return nil
				})
				r.Exec(t, "DELETE FROM t WHERE true")
			}
		})
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

	// This test fails when run with the default test tenant. Disabling and
	// tracking with #76378.
	hostServer, hostDB, hostKV := serverutils.StartServer(t,
		base.TestServerArgs{DefaultTestTenant: base.TestControlsTenantsExplicitly})
	defer hostServer.Stopper().Stop(context.Background())

	tenantID := serverutils.TestTenantID()

	// Create a tenant with ridiculously low resource limits.
	host := sqlutils.MakeSQLRunner(hostDB)
	host.Exec(t, "SELECT crdb_internal.create_tenant($1::INT)", tenantID.ToUint64())
	host.Exec(t, "SELECT crdb_internal.update_tenant_resource_limits($1::INT, 0, 0.001, 0, now(), 0)", tenantID.ToUint64())

	st := cluster.MakeTestingClusterSettings()
	// Make the tenant heartbeat like crazy.
	ctx := context.Background()
	slbase.DefaultHeartBeat.Override(ctx, &st.SV, 10*time.Millisecond)

	_, tenantDB := serverutils.StartTenant(t, hostServer, base.TestTenantArgs{
		TenantID: tenantID,
		Settings: st,
	})

	r := sqlutils.MakeSQLRunner(tenantDB)
	_ = r

	codec := keys.MakeSQLCodec(tenantID)
	indexID := uint32(systemschema.SqllivenessTable().GetPrimaryIndexID())
	key := codec.IndexPrefix(keys.SqllivenessID, indexID)

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

// TestScheduledJobsConsumption verifies that the scheduled jobs system itself
// does not consume RUs, but that the jobs it runs do consume RUs.
func TestScheduledJobsConsumption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	makeSettings := func() *cluster.Settings {
		st := cluster.MakeTestingClusterSettings()
		stats.AutomaticStatisticsClusterMode.Override(ctx, &st.SV, false)
		stats.UseStatisticsOnSystemTables.Override(ctx, &st.SV, false)
		stats.AutomaticStatisticsOnSystemTables.Override(ctx, &st.SV, false)
		tenantcostclient.TargetPeriodSetting.Override(ctx, &st.SV, time.Millisecond*20)
		return st
	}

	hostServer := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Settings:          makeSettings(),
	})
	defer hostServer.Stopper().Stop(ctx)

	testProvider := newTestProvider(timeutil.DefaultTimeSource{})

	env := jobstest.NewJobSchedulerTestEnv(jobstest.UseSystemTables, timeutil.Now())
	var zeroDuration time.Duration
	var execSchedules func() error
	var tenantServer serverutils.ApplicationLayerInterface
	var tenantDB *gosql.DB
	tenantServer, tenantDB = serverutils.StartTenant(t, hostServer, base.TestTenantArgs{
		TenantID: serverutils.TestTenantID(),
		Settings: makeSettings(),
		TestingKnobs: base.TestingKnobs{
			TenantTestingKnobs: &sql.TenantTestingKnobs{
				OverrideTokenBucketProvider: func(kvtenant.TokenBucketProvider) kvtenant.TokenBucketProvider {
					return testProvider
				},
			},
			TTL: &sql.TTLTestingKnobs{
				// Don't wait until we can use a historical query.
				AOSTDuration: &zeroDuration,
			},
			JobsTestingKnobs: &jobs.TestingKnobs{
				JobSchedulerEnv: env,
				TakeOverJobsScheduling: func(fn func(ctx context.Context, maxSchedules int64) error) {
					execSchedules = func() error {
						defer tenantServer.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
						return fn(ctx, 0)
					}
				},
				IntervalOverrides: jobs.TestingIntervalOverrides{
					// Force fast adoption and resumption.
					Adopt:             &zeroDuration,
					RetryInitialDelay: &zeroDuration,
					RetryMaxDelay:     &zeroDuration,
				},
			},
			TableStatsKnobs: &stats.TableStatsTestingKnobs{
				DisableInitialTableCollection: true,
			},
		},
	})

	r := sqlutils.MakeSQLRunner(tenantDB)
	// Create a table with rows that expire after a TTL. This will trigger the
	// creation of a TTL job.
	r.Exec(t, "CREATE TABLE t (v INT PRIMARY KEY) WITH ("+
		"ttl_expire_after = '1 microsecond', ttl_job_cron = '* * * * ?', ttl_delete_batch_size = 1)")
	r.Exec(t, "INSERT INTO t SELECT x FROM generate_series(1,100) g(x)")
	before := testProvider.waitForConsumption(t)

	// Ensure the job system is not consuming RUs when scanning/claiming jobs.
	tenantServer.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
	env.AdvanceTime(24 * time.Hour)
	time.Sleep(100 * time.Millisecond)
	after := testProvider.waitForConsumption(t)
	after.Sub(&before)
	require.Zero(t, after.WriteBatches)
	require.Zero(t, after.WriteBytes)
	// Expect up to 2 batches for schema catalog fill and anything else that
	// happens once during server startup but might not be done by this point.
	require.LessOrEqual(t, after.ReadBatches, uint64(2))

	// Make sure that at least 100 writes (deletes) are reported. The TTL job
	// should not be exempt from cost control.
	testutils.SucceedsSoon(t, func() error {
		// Run all job schedules.
		env.AdvanceTime(time.Minute)
		require.NoError(t, execSchedules())

		// Check consumption.
		c := testProvider.waitForConsumption(t)
		c.Sub(&before)
		if c.WriteRequests < 100 {
			return errors.New("no write requests reported")
		}
		return nil
	})
}

// TestConsumption verifies consumption reporting from a tenant server process.
func TestConsumptionChangefeeds(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	hostServer := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer hostServer.Stopper().Stop(ctx)

	kvserver.RangefeedEnabled.Override(ctx, &hostServer.ClusterSettings().SV, true)

	st := cluster.MakeTestingClusterSettings()
	tenantcostclient.TargetPeriodSetting.Override(ctx, &st.SV, time.Millisecond*20)
	tenantcostclient.CPUUsageAllowance.Override(ctx, &st.SV, 0)
	kvserver.RangefeedEnabled.Override(ctx, &st.SV, true)

	testProvider := newTestProvider(timeutil.DefaultTimeSource{})

	_, tenantDB := serverutils.StartTenant(t, hostServer, base.TestTenantArgs{
		TenantID: serverutils.TestTenantID(),
		Settings: st,
		TestingKnobs: base.TestingKnobs{
			TenantTestingKnobs: &sql.TenantTestingKnobs{
				OverrideTokenBucketProvider: func(kvtenant.TokenBucketProvider) kvtenant.TokenBucketProvider {
					return testProvider
				},
			},
			DistSQL: &execinfra.TestingKnobs{
				Changefeed: &changefeedccl.TestingKnobs{
					NullSinkIsExternalIOAccounted: true,
				},
			},
		},
	})
	r := sqlutils.MakeSQLRunner(tenantDB)
	r.Exec(t, "CREATE TABLE t (v STRING)")
	r.Exec(t, "INSERT INTO t SELECT repeat('1234567890', 1024) FROM generate_series(1, 10) AS g(i)")
	beforeChangefeed := testProvider.waitForConsumption(t)
	r.Exec(t, "CREATE CHANGEFEED FOR t INTO 'null://'")

	// Make sure some external io usage is reported.
	testutils.SucceedsSoon(t, func() error {
		c := testProvider.waitForConsumption(t)
		c.Sub(&beforeChangefeed)
		if c.ExternalIOEgressBytes == 0 {
			return errors.New("no external io usage reported")
		}
		return nil
	})
}

// TestConsumption verifies consumption reporting from a tenant server process.
func TestConsumptionExternalStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testSink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			n, err := io.Copy(io.Discard, r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			t.Logf("read %d bytes", n)
		case http.MethodGet:
			_, _ = w.Write([]byte("some\ntest\ncsv\ndata\n"))
		}

	}))
	defer testSink.Close()

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	hostServer, hostDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		ExternalIODir:     dir,
	})
	defer hostServer.Stopper().Stop(context.Background())
	hostSQL := sqlutils.MakeSQLRunner(hostDB)

	st := cluster.MakeTestingClusterSettings()
	tenantcostclient.TargetPeriodSetting.Override(context.Background(), &st.SV, time.Millisecond*20)
	tenantcostclient.CPUUsageAllowance.Override(context.Background(), &st.SV, 0)

	testProvider := newTestProvider(timeutil.DefaultTimeSource{})
	_, tenantDB := serverutils.StartTenant(t, hostServer, base.TestTenantArgs{
		TenantID:      serverutils.TestTenantID(),
		Settings:      st,
		ExternalIODir: dir,
		TestingKnobs: base.TestingKnobs{
			TenantTestingKnobs: &sql.TenantTestingKnobs{
				OverrideTokenBucketProvider: func(kvtenant.TokenBucketProvider) kvtenant.TokenBucketProvider {
					return testProvider
				},
			},
		},
	})
	r := sqlutils.MakeSQLRunner(tenantDB)

	createTables := func() {
		r.Exec(t, "CREATE TABLE t (v STRING)")
		r.Exec(t, "INSERT INTO t SELECT repeat('1234567890', 1024) FROM generate_series(1, 10) AS g(i)")
		hostSQL.Exec(t, "CREATE TABLE t (v STRING)")
		hostSQL.Exec(t, "INSERT INTO t SELECT repeat('1234567890', 1024) FROM generate_series(1, 10) AS g(i)")
	}
	dropTables := func() {
		r.Exec(t, "DROP TABLE t")
		hostSQL.Exec(t, "DROP TABLE t")
	}
	t.Run("export in a tenant increments egress bytes", func(t *testing.T) {
		createTables()
		defer dropTables()
		before := testProvider.waitForConsumption(t)
		r.Exec(t, fmt.Sprintf("EXPORT INTO CSV '%s' FROM TABLE t", testSink.URL))
		c := testProvider.waitForConsumption(t)
		c.Sub(&before)
		require.NotEqual(t, uint64(0), c.ExternalIOEgressBytes)
	})
	t.Run("export from a host does not increment egress bytes", func(t *testing.T) {
		createTables()
		defer dropTables()
		before := testProvider.waitForConsumption(t)
		hostSQL.Exec(t, fmt.Sprintf("EXPORT INTO CSV '%s' FROM TABLE t", testSink.URL))
		c := testProvider.waitForConsumption(t)
		c.Sub(&before)
		require.Equal(t, uint64(0), c.ExternalIOEgressBytes)
		require.Equal(t, uint64(0), c.ExternalIOIngressBytes)
	})
	t.Run("import from a tenant increments ingress bytes", func(t *testing.T) {
		createTables()
		defer dropTables()
		before := testProvider.waitForConsumption(t)
		r.Exec(t, fmt.Sprintf("IMPORT INTO t CSV DATA('%s')", testSink.URL))
		c := testProvider.waitForConsumption(t)
		c.Sub(&before)
		t.Logf("%v", c)
		require.NotEqual(t, uint64(0), c.ExternalIOIngressBytes)
	})
	t.Run("export from a host does not increment ingress bytes", func(t *testing.T) {
		createTables()
		defer dropTables()
		before := testProvider.waitForConsumption(t)
		hostSQL.Exec(t, fmt.Sprintf("IMPORT INTO t CSV DATA('%s')", testSink.URL))
		c := testProvider.waitForConsumption(t)
		c.Sub(&before)
		require.Equal(t, uint64(0), c.ExternalIOIngressBytes)
		require.Equal(t, uint64(0), c.ExternalIOEgressBytes)

	})
	t.Run("backup from tenant increments egress bytes", func(t *testing.T) {
		createTables()
		defer dropTables()
		nodelocal.LocalRequiresExternalIOAccounting = true
		defer func() { nodelocal.LocalRequiresExternalIOAccounting = false }()
		before := testProvider.waitForConsumption(t)
		r.Exec(t, "BACKUP t INTO 'nodelocal://1/backups/tenant'")
		c := testProvider.waitForConsumption(t)
		c.Sub(&before)
		require.NotEqual(t, uint64(0), c.ExternalIOEgressBytes)
	})
	t.Run("backup from host does not increment ingress or egress bytes", func(t *testing.T) {
		createTables()
		defer dropTables()
		nodelocal.LocalRequiresExternalIOAccounting = true
		defer func() { nodelocal.LocalRequiresExternalIOAccounting = false }()
		before := testProvider.waitForConsumption(t)
		hostSQL.Exec(t, "BACKUP t INTO 'nodelocal://1/backups/host'")
		c := testProvider.waitForConsumption(t)
		c.Sub(&before)
		require.Equal(t, uint64(0), c.ExternalIOEgressBytes)
		require.Equal(t, uint64(0), c.ExternalIOIngressBytes)
	})
}

func BenchmarkExternalIOAccounting(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	hostServer := serverutils.StartServerOnly(b,
		base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		})
	defer hostServer.Stopper().Stop(context.Background())

	// Override the external I/O egress cost so that we don't run out of RUs
	// during this benchmark.
	st := cluster.MakeTestingClusterSettings()
	tenantcostmodel.ExternalIOEgressCostPerMiB.Override(context.Background(), &st.SV, 0.0)
	tenantS, _ := serverutils.StartTenant(b, hostServer, base.TestTenantArgs{
		TenantID: serverutils.TestTenantID(),
		Settings: st,
	})

	nullsink.NullRequiresExternalIOAccounting = true

	concurrently := func(b *testing.B, ctx context.Context, concurrency int, op func(context.Context) error) {
		g := ctxgroup.WithContext(ctx)
		for i := 0; i < concurrency; i++ {
			g.GoCtx(op)
		}
		require.NoError(b, g.Wait())
	}

	dataSize := 8 << 20
	data := bytes.Repeat([]byte{byte(1)}, dataSize)

	readFromExternalStorage := func(b *testing.B, interceptor cloud.ReadWriterInterceptor) (func(context.Context) error, func() error) {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet:
				_, _ = w.Write(data)
			}
		}))
		es, err := tenantS.DistSQLServer().(*distsql.ServerImpl).ExternalStorageFromURI(
			context.Background(), s.URL,
			username.RootUserName(),
			cloud.WithIOAccountingInterceptor(interceptor))
		require.NoError(b, err)
		return func(ctx context.Context) error {
				r, _, err := es.ReadFile(ctx, "", cloud.ReadOptions{NoFileSize: true})
				if err != nil {
					return err
				}
				_, err = io.Copy(io.Discard, ioctx.ReaderCtxAdapter(ctx, r))
				if err != nil {
					_ = r.Close(ctx)
					return err
				}
				return r.Close(ctx)
			}, func() error {
				s.Close()
				return es.Close()
			}
	}

	writeToExternalStorage := func(b *testing.B, interceptor cloud.ReadWriterInterceptor) (func(context.Context) error, func() error) {
		es, err := tenantS.DistSQLServer().(*distsql.ServerImpl).ExternalStorageFromURI(
			context.Background(),
			"null://",
			username.RootUserName(),
			cloud.WithIOAccountingInterceptor(interceptor))
		require.NoError(b, err)
		return func(ctx context.Context) error {
			w, err := es.Writer(ctx, "does-not-matter")
			if err != nil {
				return err
			}
			_, err = io.Copy(w, bytes.NewReader(data))
			if err != nil {
				_ = w.Close()
				return err
			}
			return w.Close()
		}, es.Close
	}

	funcForOp := func(b *testing.B, op string, interceptor cloud.ReadWriterInterceptor) (func(context.Context) error, func() error) {
		if op == "read" {
			return readFromExternalStorage(b, interceptor)
		} else {
			return writeToExternalStorage(b, interceptor)
		}
	}
	concurrencyCounts := []int{8}
	for _, op := range []string{"read", "write"} {
		b.Run(fmt.Sprintf("op=%s/without-interceptor", op), func(b *testing.B) {
			for _, c := range concurrencyCounts {
				b.Run(fmt.Sprintf("concurrency=%d", c), func(b *testing.B) {
					testOp, cleanup := funcForOp(b, op, nil)
					defer func() { _ = cleanup() }()

					b.SetBytes(int64(dataSize))
					b.ResetTimer()
					for n := 0; n < b.N; n++ {
						concurrently(b, context.Background(), c, testOp)
					}
				})
			}
		})
		b.Run(fmt.Sprintf("op=%s/with-interceptor", op), func(b *testing.B) {
			limits := []int{1024, 16384, 16777216}
			for _, limit := range limits {
				for _, c := range concurrencyCounts {
					testName := fmt.Sprintf("limit=%d/concurrency=%d", limit, c)
					b.Run(testName, func(b *testing.B) {
						interceptor := multitenantio.NewReadWriteAccounter(tenantS.DistSQLServer().(*distsql.ServerImpl).ExternalIORecorder, int64(limit))
						testOp, cleanup := funcForOp(b, op, interceptor)
						defer func() { _ = cleanup() }()

						b.ResetTimer()
						b.SetBytes(int64(dataSize))
						for n := 0; n < b.N; n++ {
							concurrently(b, context.Background(), c, testOp)
						}
					})
				}
			}
		})
	}
}

func TestRUSettingsChanged(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	params := base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	}

	s, mainDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	sysDB := sqlutils.MakeSQLRunner(mainDB)

	tenantID := serverutils.TestTenantID()
	tenant1, tenantDB1 := serverutils.StartTenant(t, s, base.TestTenantArgs{
		TenantID: tenantID,
	})
	defer tenant1.AppStopper().Stop(ctx)
	defer tenantDB1.Close()

	costClient, err := tenantcostclient.NewTenantSideCostController(tenant1.ClusterSettings(), tenantID, nil, nil, roachpb.Locality{})
	require.NoError(t, err)

	initialModel := costClient.GetRequestUnitModel()

	// Increase the RU cost of everything by 100x
	settings := []*settings.FloatSetting{
		tenantcostmodel.ReadBatchCost,
		tenantcostmodel.ReadRequestCost,
		tenantcostmodel.ReadPayloadCostPerMiB,
		tenantcostmodel.WriteBatchCost,
		tenantcostmodel.WriteRequestCost,
		tenantcostmodel.WritePayloadCostPerMiB,
		tenantcostmodel.SQLCPUSecondCost,
		tenantcostmodel.PgwireEgressCostPerMiB,
		tenantcostmodel.ExternalIOEgressCostPerMiB,
		tenantcostmodel.ExternalIOIngressCostPerMiB,
	}
	for _, setting := range settings {
		sysDB.Exec(t, fmt.Sprintf("ALTER TENANT ALL SET CLUSTER SETTING %s = $1", setting.Name()), setting.Default()*100)
	}

	// Check to make sure the cost of the query increased. Use SucceedsSoon
	// because the settings propagation is async.
	testutils.SucceedsSoon(t, func() error {
		currentModel := costClient.GetRequestUnitModel()

		expect100x := func(name string, getter func(model *tenantcostmodel.RequestUnitModel) tenantcostmodel.RU) error {
			before := getter(initialModel)
			after := getter(currentModel)
			expect := before * 100
			if after != expect {
				return errors.Newf("expected %s to be %f found %f", name, expect, after)
			}
			return nil
		}

		err = expect100x("KVReadBatch", func(m *tenantcostmodel.RequestUnitModel) tenantcostmodel.RU {
			return m.KVReadBatch
		})
		if err != nil {
			return err
		}

		err = expect100x("KVReadRequest", func(m *tenantcostmodel.RequestUnitModel) tenantcostmodel.RU {
			return m.KVReadRequest
		})
		if err != nil {
			return err
		}

		err = expect100x("KVReadByte", func(m *tenantcostmodel.RequestUnitModel) tenantcostmodel.RU {
			return m.KVReadByte
		})
		if err != nil {
			return err
		}

		err = expect100x("KVWriteBatch", func(m *tenantcostmodel.RequestUnitModel) tenantcostmodel.RU {
			return m.KVWriteBatch
		})
		if err != nil {
			return err
		}

		err = expect100x("KVWriteRequest", func(m *tenantcostmodel.RequestUnitModel) tenantcostmodel.RU {
			return m.KVWriteRequest
		})
		if err != nil {
			return err
		}

		err = expect100x("KVWriteByte", func(m *tenantcostmodel.RequestUnitModel) tenantcostmodel.RU {
			return m.KVWriteByte
		})
		if err != nil {
			return err
		}

		err = expect100x("PodCPUSecond", func(m *tenantcostmodel.RequestUnitModel) tenantcostmodel.RU {
			return m.PodCPUSecond
		})
		if err != nil {
			return err
		}

		err = expect100x("PGWireEgressByte", func(m *tenantcostmodel.RequestUnitModel) tenantcostmodel.RU {
			return m.PGWireEgressByte
		})
		if err != nil {
			return err
		}

		err = expect100x("ExternalIOEgressByte", func(m *tenantcostmodel.RequestUnitModel) tenantcostmodel.RU {
			return m.ExternalIOEgressByte
		})
		if err != nil {
			return err
		}

		err = expect100x("ExternalIOIngressByte", func(m *tenantcostmodel.RequestUnitModel) tenantcostmodel.RU {
			return m.ExternalIOIngressByte
		})
		if err != nil {
			return err
		}

		return nil
	})
}

func TestCPUModelSettingsChanged(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	params := base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	}

	s, mainDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	sysDB := sqlutils.MakeSQLRunner(mainDB)

	tenantID := serverutils.TestTenantID()
	tenant1, tenantDB1 := serverutils.StartTenant(t, s, base.TestTenantArgs{
		TenantID: tenantID,
	})
	defer tenant1.AppStopper().Stop(ctx)
	defer tenantDB1.Close()

	costClient, err := tenantcostclient.NewTenantSideCostController(
		tenant1.ClusterSettings(), tenantID, nil, nil, roachpb.Locality{})
	require.NoError(t, err)

	newModel := `
	{
	  "ReadBatchCost": 1,
	  "ReadRequestCost": {
		"BatchSize": [1, 2, 3],
		"CPUPerRequest": [0.1, 0.2, 0.3]
	  },
	  "ReadBytesCost": {
		"PayloadSize": [1, 2, 3],
		"CPUPerByte": [0.5, 1, 1.5]
	  },
	  "WriteBatchCost": {
		"RatePerNode": [100, 200, 300],
		"CPUPerBatch": [500, 600, 700]
	  },
	  "WriteRequestCost": {
		"BatchSize": [1, 2, 3],
		"CPUPerRequest": [0.1, 0.2, 0.3]
	  },
	  "WriteBytesCost": {
		"PayloadSize": [1000, 2000, 3000],
		"CPUPerByte": [0.2, 0.3, 0.4]
	  }
	}`

	// Get existing model, then update it to the new model.
	initialModel := costClient.GetEstimatedCPUModel()

	sysDB.Exec(t, "ALTER TENANT ALL SET CLUSTER SETTING tenant_cost_model.estimated_cpu = $1", newModel)

	// Check to make sure the cost of the query increased. Use SucceedsSoon
	// because the settings propagation is async.
	testutils.SucceedsSoon(t, func() error {
		currentModel := costClient.GetEstimatedCPUModel()
		if currentModel.ReadBatchCost != 1 {
			return errors.Newf("expected ReadBatchCost to be %f found %f",
				initialModel.ReadBatchCost, currentModel.ReadBatchCost)
		}
		if len(currentModel.WriteBatchCost.RatePerNode) != 3 {
			return errors.Newf("expected WriteBatchCost to be %f found %f",
				initialModel.WriteBatchCost, currentModel.WriteBatchCost)
		}
		return nil
	})
}
