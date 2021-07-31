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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl/tenantcostserver"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/metrictestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
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
	tenantUsage multitenant.TenantUsageServer
	metricsReg  *metric.Registry
}

func (ts *testState) start(t *testing.T) {
	// Set up a server that we use only for the system tables.
	ts.s, ts.db, ts.kvDB = serverutils.StartServer(t, base.TestServerArgs{})
	ts.r = sqlutils.MakeSQLRunner(ts.db)

	ts.tenantUsage = server.NewTenantUsageServer(ts.kvDB, ts.s.InternalExecutor().(*sql.InternalExecutor))
	ts.metricsReg = metric.NewRegistry()
	ts.metricsReg.AddMetricStruct(ts.tenantUsage.Metrics())
}

func (ts *testState) stop() {
	ts.s.Stopper().Stop(context.Background())
}

var testStateCommands = map[string]func(*testState, *testing.T, *datadriven.TestData) string{
	"create-tenant":        (*testState).createTenant,
	"token-bucket-request": (*testState).tokenBucketRequest,
	"metrics":              (*testState).metrics,
	"configure":            (*testState).configure,
	"inspect":              (*testState).inspect,
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
	}
	if err := yaml.UnmarshalStrict([]byte(d.Input), &args); err != nil {
		d.Fatalf(t, "failed to parse request yaml: %v", err)
	}
	req := roachpb.TokenBucketRequest{
		TenantID:   uint64(tenantID),
		InstanceID: args.InstanceID,
		ConsumptionSinceLastRequest: roachpb.TokenBucketRequest_Consumption{
			RU:               args.Consumption.RU,
			ReadRequests:     args.Consumption.ReadReq,
			ReadBytes:        args.Consumption.ReadBytes,
			WriteRequests:    args.Consumption.WriteReq,
			WriteBytes:       args.Consumption.WriteBytes,
			SQLPodCPUSeconds: args.Consumption.SQLPodsCPUUsage,
		},
	}
	res := ts.tenantUsage.TokenBucketRequest(
		context.Background(), roachpb.MakeTenantID(tenantID), &req,
	)
	if res.Error != (errors.EncodedError{}) {
		return fmt.Sprintf("error: %v", errors.DecodeError(context.Background(), res.Error))
	}
	return ""
}

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
	ts.r.Exec(
		t,
		`SELECT crdb_internal.update_tenant_resource_limits($1, $2, $3, $4, now(), 0)`,
		tenantID,
		args.AvailableRU,
		args.RefillRate,
		args.MaxBurstRU,
	)
	return ""
}

func (ts *testState) inspect(t *testing.T, d *datadriven.TestData) string {
	tenantID := ts.tenantID(t, d)
	res, err := tenantcostserver.InspectTenantMetadata(
		context.Background(),
		ts.s.InternalExecutor().(*sql.InternalExecutor),
		nil, /* txn */
		roachpb.MakeTenantID(tenantID),
	)
	if err != nil {
		d.Fatalf(t, "error inspecting tenant state: %v", err)
	}
	return res
}
