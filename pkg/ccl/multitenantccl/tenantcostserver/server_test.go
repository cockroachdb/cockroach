// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostserver_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl/tenantcostserver"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/metrictestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	s           serverutils.TestServerInterface
	db          *gosql.DB
	kvDB        *kv.DB
	r           *sqlutils.SQLRunner
	clock       *timeutil.ManualTime
	tenantUsage multitenant.TenantUsageServer
	metricsReg  *metric.Registry
}

const timeFormat = "15:04:05.000"

var t0 = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

func (ts *testState) start(t *testing.T) {
	// Set up a server that we use only for the system tables.
	ts.s, ts.db, ts.kvDB = serverutils.StartServer(t, base.TestServerArgs{})
	ts.r = sqlutils.MakeSQLRunner(ts.db)

	ts.clock = timeutil.NewManualTime(t0)
	ts.tenantUsage = tenantcostserver.NewInstance(
		ts.kvDB, ts.s.InternalExecutor().(*sql.InternalExecutor), ts.clock,
	)
	ts.metricsReg = metric.NewRegistry()
	ts.metricsReg.AddMetricStruct(ts.tenantUsage.Metrics())
}

func (ts *testState) stop() {
	ts.s.Stopper().Stop(context.Background())
}

func (ts *testState) formatTime(tm time.Time) string {
	return tm.Format(timeFormat)
}

var testStateCommands = map[string]func(*testState, *testing.T, *datadriven.TestData) string{
	"create-tenant":        (*testState).createTenant,
	"token-bucket-request": (*testState).tokenBucketRequest,
	"metrics":              (*testState).metrics,
	"configure":            (*testState).configure,
	"inspect":              (*testState).inspect,
	"advance":              (*testState).advance,
}

func (ts *testState) tenantID(t *testing.T, d *datadriven.TestData) uint64 {
	for _, arg := range d.CmdArgs {
		if arg.Key == "tenant" {
			if len(arg.Vals) != 1 {
				d.Fatalf(t, "tenant argument requires a value")
			}
			id, err := strconv.Atoi(arg.Vals[0])
			if err != nil || id < 1 {
				d.Fatalf(t, "invalid tenant argument")
			}
			return uint64(id)
		}
	}
	d.Fatalf(t, "command requires tenant=<id> argument")
	return 0
}

func (ts *testState) createTenant(t *testing.T, d *datadriven.TestData) string {
	ts.r.Exec(t, fmt.Sprintf("SELECT crdb_internal.create_tenant(%d)", ts.tenantID(t, d)))
	return ""
}

// tokenBucketRequest runs a TokenBucket request against a tenant (with ID
// specified in a tenant=X argument). The input is a yaml for the struct below.
func (ts *testState) tokenBucketRequest(t *testing.T, d *datadriven.TestData) string {
	tenantID := ts.tenantID(t, d)
	var args struct {
		InstanceID  uint32 `yaml:"instance_id"`
		Consumption struct {
			RU              float64 `yaml:"ru"`
			ReadReq         uint64  `yaml:"read_req"`
			ReadBytes       uint64  `yaml:"read_bytes"`
			WriteReq        uint64  `yaml:"write_req"`
			WriteBytes      uint64  `yaml:"write_bytes"`
			SQLPodsCPUUsage float64 `yaml:"sql_pods_cpu_usage"`
		}
		RU     float64 `yaml:"ru"`
		Period string  `yaml:"period"`
	}
	args.Period = "10s"
	if err := yaml.UnmarshalStrict([]byte(d.Input), &args); err != nil {
		d.Fatalf(t, "failed to parse request yaml: %v", err)
	}
	period, err := time.ParseDuration(args.Period)
	if err != nil {
		d.Fatalf(t, "failed to parse duration: %v", args.Period)
	}
	req := roachpb.TokenBucketRequest{
		TenantID:   uint64(tenantID),
		InstanceID: args.InstanceID,
		ConsumptionSinceLastRequest: roachpb.TenantConsumption{
			RU:                args.Consumption.RU,
			ReadRequests:      args.Consumption.ReadReq,
			ReadBytes:         args.Consumption.ReadBytes,
			WriteRequests:     args.Consumption.WriteReq,
			WriteBytes:        args.Consumption.WriteBytes,
			SQLPodsCPUSeconds: args.Consumption.SQLPodsCPUUsage,
		},
		RequestedRU:         args.RU,
		TargetRequestPeriod: period,
	}
	res := ts.tenantUsage.TokenBucketRequest(
		context.Background(), roachpb.MakeTenantID(tenantID), &req,
	)
	if res.Error != (errors.EncodedError{}) {
		return fmt.Sprintf("error: %v", errors.DecodeError(context.Background(), res.Error))
	}
	if res.GrantedRU == 0 {
		if res.TrickleDuration != 0 {
			d.Fatalf(t, "trickle duration set with 0 granted RUs")
		}
		return ""
	}
	if res.TrickleDuration == 0 {
		return fmt.Sprintf("%.10g RUs granted immediately.\n", res.GrantedRU)
	}
	return fmt.Sprintf("%.10g RUs granted over %s.\n", res.GrantedRU, res.TrickleDuration)
}

// metrics outputs all metrics that match the regex in the input.
func (ts *testState) metrics(t *testing.T, d *datadriven.TestData) string {
	re, err := regexp.Compile(d.Input)
	if err != nil {
		d.Fatalf(t, "failed to compile pattern: %v", err)
	}
	str, err := metrictestutils.GetMetricsText(ts.metricsReg, re)
	if err != nil {
		d.Fatalf(t, "failed to scrape metrics: %v", err)
	}
	return str
}

// configure reconfigures the token bucket for a tenant (specified in a tenant=X
// argument). The input is a yaml for the struct below.
func (ts *testState) configure(t *testing.T, d *datadriven.TestData) string {
	tenantID := ts.tenantID(t, d)
	var args struct {
		AvailableRU float64 `yaml:"available_ru"`
		RefillRate  float64 `yaml:"refill_rate"`
		MaxBurstRU  float64 `yaml:"max_burst_ru"`
		// TODO(radu): Add AsOf/AsOfConsumedRU.
	}
	if err := yaml.UnmarshalStrict([]byte(d.Input), &args); err != nil {
		d.Fatalf(t, "failed to parse request yaml: %v", err)
	}
	if err := ts.kvDB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		return ts.tenantUsage.ReconfigureTokenBucket(
			ctx, txn,
			roachpb.MakeTenantID(tenantID),
			args.AvailableRU, args.RefillRate, args.MaxBurstRU,
			time.Time{}, 0,
		)
	}); err != nil {
		d.Fatalf(t, "reconfigure error: %v", err)
	}
	return ""
}

// inspect shows all the metadata for a tenant (specified in a tenant=X
// argument), in a user-friendly format.
func (ts *testState) inspect(t *testing.T, d *datadriven.TestData) string {
	tenantID := ts.tenantID(t, d)
	res, err := tenantcostserver.InspectTenantMetadata(
		context.Background(),
		ts.s.InternalExecutor().(*sql.InternalExecutor),
		nil, /* txn */
		roachpb.MakeTenantID(tenantID),
		timeFormat,
	)
	if err != nil {
		d.Fatalf(t, "error inspecting tenant state: %v", err)
	}
	return res
}

// advance advances the clock by the provided duration and returns the new
// current time.
func (ts *testState) advance(t *testing.T, d *datadriven.TestData) string {
	dur, err := time.ParseDuration(d.Input)
	if err != nil {
		d.Fatalf(t, "failed to parse input as duration: %v", err)
	}
	// This is hacky, but we want to be able to test the time going back which is
	// not allowed by ManualTime.Advance(). This is safe to do because we are not
	// using timers.
	*ts.clock = *timeutil.NewManualTime(ts.clock.Now().Add(dur))
	return ts.formatTime(ts.clock.Now())
}
