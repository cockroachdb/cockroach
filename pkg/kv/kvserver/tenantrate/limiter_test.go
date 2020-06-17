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
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		defer leaktest.AfterTest(t)()
		datadriven.RunTest(t, path, new(testState).run)
	})
}

type testState struct {
	initialized bool
	tenants     map[roachpb.TenantID][]*tenantrate.Limiter
	running     map[string]*launchState
	rl          *tenantrate.LimiterFactory
	m           *metric.Registry
	clock       *quotapool.ManualTime
	settings    *cluster.Settings
}

type launchState struct {
	id         string
	tenantID   roachpb.TenantID
	ctx        context.Context
	cancel     context.CancelFunc
	writeBytes int64
	reserveCh  chan error
}

func (s launchState) String() string {
	return s.id + "@" + s.tenantID.String()
}

var testStateCommands = map[string]func(*testState, *testing.T, *datadriven.TestData) string{
	"init":            (*testState).init,
	"advance":         (*testState).advance,
	"launch":          (*testState).launch,
	"await":           (*testState).await,
	"metrics":         (*testState).metrics,
	"blocked":         (*testState).blocked,
	"get_tenants":     (*testState).getTenants,
	"release_tenants": (*testState).releaseTenants,
	"timers":          (*testState).timers,
	"cancel":          (*testState).cancel,
	"record_read":     (*testState).recordRead,
	"update_settings": (*testState).updateSettings,
}

func (ts *testState) run(t *testing.T, d *datadriven.TestData) string {
	if !ts.initialized && d.Cmd != "init" {
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

func (ts *testState) init(t *testing.T, d *datadriven.TestData) string {
	if ts.initialized {
		d.Fatalf(t, "already ran init")
	}
	ts.initialized = true
	ts.running = make(map[string]*launchState)
	ts.tenants = make(map[roachpb.TenantID][]*tenantrate.Limiter)
	ts.clock = quotapool.NewManualTime(t0)
	ts.settings = cluster.MakeTestingClusterSettings()
	limits := tenantrate.LimitConfigsFromSettings(ts.settings)
	parseLimits(t, d, &limits)
	tenantrate.UpdateSettingsWithRateLimits(ts.settings, limits)
	ts.rl = tenantrate.NewLimiterFactory(ts.settings, &tenantrate.TestingKnobs{
		TimeSource: ts.clock,
	})
	ts.m = metric.NewRegistry()
	ts.m.AddMetricStruct(ts.rl.Metrics())
	return ts.clock.Now().Format(timeFormat)
}

func (ts *testState) advance(t *testing.T, d *datadriven.TestData) string {
	dur, err := time.ParseDuration(d.Input)
	if err != nil {
		d.Fatalf(t, "failed to parse input as duration: %v", err)
	}
	ts.clock.Advance(dur)
	return ts.formatTime()
}

func (ts *testState) launch(t *testing.T, d *datadriven.TestData) string {
	var cmds []struct {
		ID         string
		Tenant     uint64
		WriteBytes int64
	}
	if err := yaml.UnmarshalStrict([]byte(d.Input), &cmds); err != nil {
		d.Fatalf(t, "failed to parse launch command: %v", err)
	}
	for _, cmd := range cmds {
		var s launchState
		s.id = cmd.ID
		s.tenantID = roachpb.MakeTenantID(uint64(cmd.Tenant))
		s.ctx, s.cancel = context.WithCancel(context.Background())
		s.reserveCh = make(chan error, 1)
		s.writeBytes = cmd.WriteBytes
		ts.running[s.id] = &s
		lims := ts.tenants[s.tenantID]
		if len(lims) == 0 {
			d.Fatalf(t, "no Limiter exists for tenant %v", s.tenantID)
		}
		go func() {
			// We'll not worry about ever releasing tenant Limiters.
			s.reserveCh <- lims[0].Wait(s.ctx, s.writeBytes)
		}()
	}
	return ts.FormatRunning()
}

func (ts *testState) await(t *testing.T, d *datadriven.TestData) string {
	ids := parseStrings(t, d)
	for _, id := range ids {
		ls, ok := ts.running[id]
		if !ok {
			d.Fatalf(t, "no running goroutine with id %s", id)
		}
		err := <-ls.reserveCh
		if err != nil {
			d.Fatalf(t, "expected no error for id %s, got %q",
				id, err)
		}
		delete(ts.running, id)
	}
	return ts.FormatRunning()
}

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

func (ts *testState) metrics(t *testing.T, d *datadriven.TestData) string {
	ex := metric.MakePrometheusExporter()
	ex.ScrapeRegistry(ts.m)
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
	return strings.Join(outLines, "\n")
}

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

func (ts *testState) blocked(t *testing.T, d *datadriven.TestData) string {
	var blocked struct {
		Tenants map[uint64]int
		Tasks   []string
	}
	if err := yaml.UnmarshalStrict([]byte(d.Input), &blocked); err != nil {
		d.Fatalf(t, "failed to unmarshal command: %v", err)
	}
	if err := testutils.SucceedsSoonError(func() error {
		for tid, exp := range blocked.Tenants {
			lims := ts.tenants[roachpb.MakeTenantID(tid)]
			if len(lims) == 0 {
				d.Fatalf(t, "tenant with id %d does not exist", tid)
			}
			if cur := lims[0].Len(); cur != exp {
				return errors.Errorf("expected %d blocked for id %d, got %d", exp, tid, cur)
			}
		}
		return nil
	}); err != nil {
		d.Fatalf(t, "failed to verify blocked counts: %v", err)
	}
	for _, id := range blocked.Tasks {
		ls, ok := ts.running[id]
		if !ok {
			d.Fatalf(t, "no running goroutine with id %q", id)
		}
		select {
		case err := <-ls.reserveCh:
			d.Fatalf(t, "expected %q to be blocked, got %v", id, err)
		case <-time.After(time.Millisecond):
		}
	}
	return ts.FormatRunning()
}

func (ts *testState) getTenants(t *testing.T, d *datadriven.TestData) string {
	tenantIDs := parseTenantIDs(t, d)
	for i := range tenantIDs {
		id := roachpb.MakeTenantID(tenantIDs[i])
		ts.tenants[id] = append(ts.tenants[id], ts.rl.GetTenant(id))
	}
	return ts.FormatTenants()
}

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

func (ts *testState) updateSettings(t *testing.T, d *datadriven.TestData) string {
	limits := tenantrate.LimitConfigsFromSettings(ts.settings)
	parseLimits(t, d, &limits)
	tenantrate.UpdateSettingsWithRateLimits(ts.settings, limits)
	return ts.formatTime()
}

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
		lims[0].RecordRead(r.ReadBytes)
	}
	return ts.FormatRunning()
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

func parseLimits(t *testing.T, d *datadriven.TestData, limits *tenantrate.LimitConfigs) {
	if err := yaml.UnmarshalStrict([]byte(d.Input), &limits); err != nil {
		d.Fatalf(t, "failed to unmarshal limits: %v", err)
	}
}

func parseStrings(t *testing.T, d *datadriven.TestData) []string {
	var ids []string
	if err := yaml.UnmarshalStrict([]byte(d.Input), &ids); err != nil {
		d.Fatalf(t, "failed to parse blocked command: %v", err)
	}
	return ids
}
