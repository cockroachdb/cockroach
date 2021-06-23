// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantrate_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/tenantrate"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestCloser(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeTestingClusterSettings()
	start := timeutil.Now()
	timeSource := timeutil.NewManualTime(start)
	factory := tenantrate.NewLimiterFactory(&st.SV, &tenantrate.TestingKnobs{
		TimeSource: timeSource,
	})
	tenant := roachpb.MakeTenantID(2)
	closer := make(chan struct{})
	limiter := factory.GetTenant(tenant, closer)
	ctx := context.Background()
	// First Wait call will not block.
	require.NoError(t, limiter.Wait(ctx, true, 1))
	errCh := make(chan error, 1)
	go func() { errCh <- limiter.Wait(ctx, true, 1<<30) }()
	testutils.SucceedsSoon(t, func() error {
		if timers := timeSource.Timers(); len(timers) != 1 {
			return errors.Errorf("expected 1 timer, found %d", len(timers))
		}
		return nil
	})
	close(closer)
	require.Regexp(t, "closer", <-errCh)
}

func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		defer leaktest.AfterTest(t)()
		datadriven.RunTest(t, path, new(testState).run)
	})
}

type testState struct {
	initialized bool
	tenants     map[roachpb.TenantID][]tenantrate.Limiter
	running     map[string]*launchState
	rl          *tenantrate.LimiterFactory
	m           *metric.Registry
	clock       *timeutil.ManualTime
	settings    *cluster.Settings
	config      tenantrate.Config
}

type launchState struct {
	id         string
	tenantID   roachpb.TenantID
	ctx        context.Context
	cancel     context.CancelFunc
	isWrite    bool
	writeBytes int64
	reserveCh  chan error
}

func (s launchState) String() string {
	return s.id + "@" + s.tenantID.String()
}

var testStateCommands = map[string]func(*testState, *testing.T, *datadriven.TestData) string{
	"init":            (*testState).init,
	"update_settings": (*testState).updateSettings,
	"advance":         (*testState).advance,
	"launch":          (*testState).launch,
	"await":           (*testState).await,
	"cancel":          (*testState).cancel,
	"record_read":     (*testState).recordRead,
	"timers":          (*testState).timers,
	"metrics":         (*testState).metrics,
	"get_tenants":     (*testState).getTenants,
	"release_tenants": (*testState).releaseTenants,
	"estimate_iops":   (*testState).estimateIOPS,
}

func (ts *testState) run(t *testing.T, d *datadriven.TestData) string {
	if !ts.initialized && d.Cmd != "init" && d.Cmd != "estimate_iops" {
		d.Fatalf(t, "expected init as first command, got %q", d.Cmd)
	}
	if f, ok := testStateCommands[d.Cmd]; ok {
		return f(ts, t, d)
	}
	d.Fatalf(t, "unknown command %q", d.Cmd)
	return ""
}

const timeFormat = "15:04:05.000"

var t0 = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

// init is called at the beginning of a test. It must be the first command.
// The argument is a yaml serialization of LimitConfigs. It returns the time as
// of initialization (00:00:00.000). For example:
//
//  init
//  requests: { rate: 1, burst: 2 }
//  readbytes: { rate: 1024, burst: 2048 }
//  writebytes: { rate: 1024, burst: 2048 }
//  ----
//  00:00:00.000
//
func (ts *testState) init(t *testing.T, d *datadriven.TestData) string {
	if ts.initialized {
		d.Fatalf(t, "already ran init")
	}
	ts.initialized = true
	ts.running = make(map[string]*launchState)
	ts.tenants = make(map[roachpb.TenantID][]tenantrate.Limiter)
	ts.clock = timeutil.NewManualTime(t0)
	ts.settings = cluster.MakeTestingClusterSettings()
	ts.config = tenantrate.DefaultConfig()

	parseSettings(t, d, &ts.config)

	ts.rl = tenantrate.NewLimiterFactory(&ts.settings.SV, &tenantrate.TestingKnobs{
		TimeSource: ts.clock,
	})
	ts.rl.UpdateConfig(ts.config)
	ts.m = metric.NewRegistry()
	ts.m.AddMetricStruct(ts.rl.Metrics())
	return ts.clock.Now().Format(timeFormat)
}

// updateSettings allows setting the rate and burst limits. It takes as input
// yaml object representing the limits and updates accordingly. It returns
// the current time. See init for more details as the semantics are the same.
func (ts *testState) updateSettings(t *testing.T, d *datadriven.TestData) string {
	parseSettings(t, d, &ts.config)
	ts.rl.UpdateConfig(ts.config)
	return ts.formatTime()
}

// advance advances the clock by the provided duration and returns the new
// current time.
//
//  advance
//  2s
//  ----
//  00:00:02.000
//
func (ts *testState) advance(t *testing.T, d *datadriven.TestData) string {
	dur, err := time.ParseDuration(d.Input)
	if err != nil {
		d.Fatalf(t, "failed to parse input as duration: %v", err)
	}
	ts.clock.Advance(dur)
	return ts.formatTime()
}

// launch will launch requests with provided id, tenant, and writebytes.
// The argument is a yaml list of such request to launch. These requests
// are launched in parallel, no ordering should be assumed between them.
// It is an error to launch a request with an id of an outstanding request or
// with a tenant id that has not been previously created with at least one call
// to get_tenant. The return value is a serialization of all of the currently
// outstanding requests. Requests can be removed from the outstanding set with
// await. The set of outstanding requests is serialized as a list of
// [<id>@<tenant>, ...]
//
// The below example would launch two requests with ids "a" and "b"
// corresponding to tenants 2 and 3 respectively.
//
//  launch
//  - { id: a, tenant: 2, writebytes: 3}
//  - { id: b, tenant: 3}
//  ----
//  [a@2, b@3]
//
func (ts *testState) launch(t *testing.T, d *datadriven.TestData) string {
	var cmds []struct {
		ID         string
		Tenant     uint64
		IsWrite    bool
		WriteBytes int64
	}
	if err := yaml.UnmarshalStrict([]byte(d.Input), &cmds); err != nil {
		d.Fatalf(t, "failed to parse launch command: %v", err)
	}
	for _, cmd := range cmds {
		var s launchState
		s.id = cmd.ID
		s.tenantID = roachpb.MakeTenantID(cmd.Tenant)
		s.ctx, s.cancel = context.WithCancel(context.Background())
		s.reserveCh = make(chan error, 1)
		s.isWrite = cmd.IsWrite
		s.writeBytes = cmd.WriteBytes
		ts.running[s.id] = &s
		lims := ts.tenants[s.tenantID]
		if len(lims) == 0 {
			d.Fatalf(t, "no limiter exists for tenant %v", s.tenantID)
		}
		go func() {
			// We'll not worry about ever releasing tenant Limiters.
			s.reserveCh <- lims[0].Wait(s.ctx, s.isWrite, s.writeBytes)
		}()
	}
	return ts.FormatRunning()
}

// await will wait for an outstanding requests to complete. It is an error if
// no request with the given id exists. The input is a yaml list of request ids.
// The set of remaining requests will be returned. See launch for details on
// the serialization of the output. If the requests do not proceed soon, the
// test will fail.
//
// For example:
//
//  await
//  [a]
//  ----
//  [b@3]
//
func (ts *testState) await(t *testing.T, d *datadriven.TestData) string {
	ids := parseStrings(t, d)
	const awaitTimeout = time.Second
	ctx, cancel := context.WithTimeout(context.Background(), awaitTimeout)
	defer cancel()
	for _, id := range ids {
		ls, ok := ts.running[id]
		if !ok {
			d.Fatalf(t, "no running goroutine with id %s", id)
		}
		select {
		case <-ctx.Done():
			d.Fatalf(t, "goroutined %s failed to finish in time", id)
		case err := <-ls.reserveCh:
			if err != nil {
				d.Fatalf(t, "expected no error for id %s, got %q", id, err)
			}
		}
		delete(ts.running, id)
	}
	return ts.FormatRunning()
}

// cancel will cancel an outstanding request. It is an error if no request
// with the given id exists. The input is a yaml list of request ids. Cancel
// will wait for the cancellation to take effect and will remove the request
// from the set of outstanding requests. The set of remaining requests will be
// returned. See launch for details on the serialization of the output.
//
//  cancel
//  [b]
//  ----
//  [a@2]
//
func (ts *testState) cancel(t *testing.T, d *datadriven.TestData) string {
	ids := parseStrings(t, d)
	for _, id := range ids {
		ls, ok := ts.running[id]
		if !ok {
			d.Fatalf(t, "no running goroutine with id %s", id)
		}
		ls.cancel()
		err := <-ls.reserveCh
		if !errors.Is(err, context.Canceled) {
			d.Fatalf(t, "expected %v for id %s, got %q",
				context.Canceled, id, err)
		}
		delete(ts.running, id)
	}
	return ts.FormatRunning()
}

// recordRead accounts for bytes read from a request. It takes as input a
// yaml list with fields tenant and readbytes. It returns the set of tasks
// currently running like launch, await, and cancel.
//
// For example:
//
//  record_read
//  - { tenant: 2, readbytes: 32 }
//  ----
//  [a@2]
//
func (ts *testState) recordRead(t *testing.T, d *datadriven.TestData) string {
	var reads []struct {
		Tenant    uint64
		ReadBytes int64
	}
	if err := yaml.UnmarshalStrict([]byte(d.Input), &reads); err != nil {
		d.Fatalf(t, "failed to unmarshal reads: %v", err)
	}
	for _, r := range reads {
		tid := roachpb.MakeTenantID(r.Tenant)
		lims := ts.tenants[tid]
		if len(lims) == 0 {
			d.Fatalf(t, "no outstanding limiters for %v", tid)
		}
		lims[0].RecordRead(context.Background(), r.ReadBytes)
	}
	return ts.FormatRunning()
}

// metrics will print out the prometheus metric values. The command takes an
// argument as a regular expression over the values. The metrics are printed in
// lexicographical order. The command will retry until the output matches to
// make it more robust to races in metric recording.
//
// For example:
//
//  metrics
//  ----
//  kv_tenant_rate_limit_current_blocked 0
//  kv_tenant_rate_limit_current_blocked{tenant_id="2"} 0
//  kv_tenant_rate_limit_current_blocked{tenant_id="system"} 0
//  kv_tenant_rate_limit_num_tenants 0
//  kv_tenant_rate_limit_read_bytes_admitted 0
//  kv_tenant_rate_limit_read_bytes_admitted{tenant_id="2"} 0
//  kv_tenant_rate_limit_read_bytes_admitted{tenant_id="system"} 100
//  kv_tenant_rate_limit_read_requests_admitted 0
//  kv_tenant_rate_limit_read_requests_admitted{tenant_id="2"} 0
//  kv_tenant_rate_limit_read_requests_admitted{tenant_id="system"} 0
//  kv_tenant_rate_limit_write_bytes_admitted 50
//  kv_tenant_rate_limit_write_bytes_admitted{tenant_id="2"} 50
//  kv_tenant_rate_limit_write_bytes_admitted{tenant_id="system"} 0
//  kv_tenant_rate_limit_write_requests_admitted 0
//  kv_tenant_rate_limit_write_requests_admitted{tenant_id="2"} 0
//  kv_tenant_rate_limit_write_requests_admitted{tenant_id="system"} 0
//
// Or with a regular expression:
//
//  metrics
//  write_bytes_admitted\{tenant_id="2"\}
//  ----
//  kv_tenant_rate_limit_write_bytes_admitted{tenant_id="2"} 50
//
func (ts *testState) metrics(t *testing.T, d *datadriven.TestData) string {
	exp := strings.TrimSpace(d.Expected)
	if err := testutils.SucceedsSoonError(func() error {
		got := ts.getMetricsText(t, d)
		if got != exp {
			return errors.Errorf("got:\n%s\nexp:\n%s\n", got, exp)
		}
		return nil
	}); err != nil {
		d.Fatalf(t, "failed to find expected metrics: %v", err)
	}
	return d.Expected
}

func (ts *testState) getMetricsText(t *testing.T, d *datadriven.TestData) string {
	ex := metric.MakePrometheusExporter()
	ex.ScrapeRegistry(ts.m, true /* includeChildMetrics */)
	var in bytes.Buffer
	if err := ex.PrintAsText(&in); err != nil {
		d.Fatalf(t, "failed to print prometheus data: %v", err)
	}
	// We want to compile the input into a regular expression.
	re, err := regexp.Compile(d.Input)
	if err != nil {
		d.Fatalf(t, "failed to compile pattern: %v", err)
	}
	sc := bufio.NewScanner(&in)
	var outLines []string
	for sc.Scan() {
		if bytes.HasPrefix(sc.Bytes(), []byte{'#'}) || !re.Match(sc.Bytes()) {
			continue
		}
		outLines = append(outLines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		d.Fatalf(t, "failed to process metrics: %v", err)
	}
	sort.Strings(outLines)
	metricsText := strings.Join(outLines, "\n")
	return metricsText
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
func (ts *testState) timers(t *testing.T, d *datadriven.TestData) string {
	exp := strings.TrimSpace(d.Expected)
	if err := testutils.SucceedsSoonError(func() error {
		got := timesToStrings(ts.clock.Timers())
		gotStr := strings.Join(got, "\n")
		if gotStr != exp {
			return errors.Errorf("got: %q, exp: %q", gotStr, exp)
		}
		return nil
	}); err != nil {
		d.Fatalf(t, "failed to find expected timers: %v", err)
	}
	return d.Expected
}

func timesToStrings(times []time.Time) []string {
	strs := make([]string, len(times))
	for i, t := range times {
		strs[i] = t.Format(timeFormat)
	}
	return strs
}

// getTenants acquires references to tenants. It is a prerequisite to launching
// requests. The input is a yaml list of tenant ids. It returns the currently
// allocated limiters and their reference counts. The serialization of the
// return is a list of [<tenant id>#<ref count>, ...].
//
// For example:
//
//  get_tenants
//  [2, 3, 2]
//  ----
//  [2#2, 3#1]
//
func (ts *testState) getTenants(t *testing.T, d *datadriven.TestData) string {
	tenantIDs := parseTenantIDs(t, d)
	for i := range tenantIDs {
		id := roachpb.MakeTenantID(tenantIDs[i])
		ts.tenants[id] = append(ts.tenants[id], ts.rl.GetTenant(id, nil /* closer */))
	}
	return ts.FormatTenants()
}

// releaseTenants releases references to tenants. The input is a yaml list of
// tenant ids. It returns the currently allocated limiters and their reference
// counts. See getTenants for the serialization.
//
// For example:
//
//  release_tenants
//  [2, 3]
//  ----
//  [2#1]
//
func (ts *testState) releaseTenants(t *testing.T, d *datadriven.TestData) string {
	tenantIDs := parseTenantIDs(t, d)
	for i := range tenantIDs {
		id := roachpb.MakeTenantID(tenantIDs[i])
		lims := ts.tenants[id]
		if len(lims) == 0 {
			d.Fatalf(t, "no outstanding limiters for %v", id)
		}
		ts.rl.Release(lims[0])
		if lims = lims[1:]; len(lims) > 0 {
			ts.tenants[id] = lims
		} else {
			delete(ts.tenants, id)
		}
	}
	return ts.FormatTenants()
}

// estimateIOPS takes in the description of a workload and produces an estimate
// of the IOPS for that workload (under the default settings).
//
// For example:
//
//  estimate_iops
//  readpercentage: 50
//  readsize: 4096
//  writesize: 4096
//  ----
//  Mixed workload (50% reads; 4.0 KiB reads; 4.0 KiB writes): 256 sustained IOPS, 256 burst.
//
func (ts *testState) estimateIOPS(t *testing.T, d *datadriven.TestData) string {
	var workload struct {
		ReadPercentage int
		ReadSize       int64
		WriteSize      int64
	}
	if err := yaml.UnmarshalStrict([]byte(d.Input), &workload); err != nil {
		d.Fatalf(t, "failed to parse workload information: %v", err)
	}
	if workload.ReadPercentage < 0 || workload.ReadPercentage > 100 {
		d.Fatalf(t, "Invalid read percentage %d", workload.ReadPercentage)
	}
	config := tenantrate.DefaultConfig()

	calculateIOPS := func(rate float64) float64 {
		readCost := config.CostModel.KVReadCost(workload.ReadSize)
		writeCost := config.CostModel.KVWriteCost(workload.WriteSize)
		readFraction := tenantcostmodel.RU(workload.ReadPercentage) / 100.0
		avgCost := readFraction*readCost + (1-readFraction)*writeCost
		return rate / float64(avgCost)
	}

	sustained := calculateIOPS(config.Rate)
	burst := calculateIOPS(config.Burst)
	fmtFloat := func(val float64) string {
		if val < 10 {
			return fmt.Sprintf("%.1f", val)
		}
		return fmt.Sprintf("%.0f", val)
	}
	switch workload.ReadPercentage {
	case 0:
		return fmt.Sprintf(
			"Write-only workload (%s writes): %s sustained IOPS, %s burst.",
			humanize.IBytes(uint64(workload.WriteSize)), fmtFloat(sustained), fmtFloat(burst),
		)
	case 100:
		return fmt.Sprintf(
			"Read-only workload (%s reads): %s sustained IOPS, %s burst.",
			humanize.IBytes(uint64(workload.ReadSize)), fmtFloat(sustained), fmtFloat(burst),
		)
	default:
		return fmt.Sprintf(
			"Mixed workload (%d%% reads; %s reads; %s writes): %s sustained IOPS, %s burst.",
			workload.ReadPercentage,
			humanize.IBytes(uint64(workload.ReadSize)), humanize.IBytes(uint64(workload.WriteSize)),
			fmtFloat(sustained), fmtFloat(burst),
		)
	}
}

func (rs *testState) FormatRunning() string {
	var states []string
	for _, ls := range rs.running {
		states = append(states, ls.String())
	}
	sort.Strings(states)
	return "[" + strings.Join(states, ", ") + "]"
}

func (ts *testState) FormatTenants() string {
	var tenantCounts []string
	for id, lims := range ts.tenants {
		tenantCounts = append(tenantCounts, fmt.Sprintf("%s#%d", id, len(lims)))
	}
	sort.Strings(tenantCounts)
	return "[" + strings.Join(tenantCounts, ", ") + "]"
}

func (ts *testState) formatTime() string {
	return ts.clock.Now().Format(timeFormat)
}

func parseTenantIDs(t *testing.T, d *datadriven.TestData) []uint64 {
	var tenantIDs []uint64
	if err := yaml.UnmarshalStrict([]byte(d.Input), &tenantIDs); err != nil {
		d.Fatalf(t, "failed to parse getTenants command: %v", err)
	}
	return tenantIDs
}

// SettingValues is a struct that can be populated from test files, via YAML.
type SettingValues struct {
	Rate  float64
	Burst float64

	Read  Factors
	Write Factors
}

// Factors for reads and writes.
type Factors struct {
	Base    float64
	PerByte float64
}

// parseSettings parses a SettingValues yaml and updates the given config.
// Missing (zero) values are ignored.
func parseSettings(t *testing.T, d *datadriven.TestData, config *tenantrate.Config) {
	var vals SettingValues
	if err := yaml.UnmarshalStrict([]byte(d.Input), &vals); err != nil {
		d.Fatalf(t, "failed to unmarshal limits: %v", err)
	}

	override := func(dest interface{}, val float64) {
		if val == 0 {
			return
		}
		switch dest := dest.(type) {
		case *float64:
			*dest = val
		case *tenantcostmodel.RU:
			*dest = tenantcostmodel.RU(val)
		}
	}
	override(&config.Rate, vals.Rate)
	override(&config.Burst, vals.Burst)
	override(&config.CostModel.KVReadRequest, vals.Read.Base)
	override(&config.CostModel.KVReadByte, vals.Read.PerByte)
	override(&config.CostModel.KVWriteRequest, vals.Write.Base)
	override(&config.CostModel.KVWriteByte, vals.Write.PerByte)
}

func parseStrings(t *testing.T, d *datadriven.TestData) []string {
	var ids []string
	if err := yaml.UnmarshalStrict([]byte(d.Input), &ids); err != nil {
		d.Fatalf(t, "failed to parse strings: %v", err)
	}
	return ids
}
