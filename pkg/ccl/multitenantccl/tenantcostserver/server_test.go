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
	"fmt"
	"regexp"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
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
	"gopkg.in/yaml.v2"
)

func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		defer leaktest.AfterTest(t)()

		// Set up a server that we use only for the system tables.
		ctx := context.Background()
		s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)
		r := sqlutils.MakeSQLRunner(db)

		tenantUsage := server.NewTenantUsageServer(kvDB, s.InternalExecutor().(*sql.InternalExecutor))
		metricsReg := metric.NewRegistry()
		metricsReg.AddMetricStruct(tenantUsage.Metrics())

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "create-tenant":
				if len(d.CmdArgs) != 1 {
					d.Fatalf(t, "expected tenant number")
				}
				r.Exec(t, fmt.Sprintf("SELECT crdb_internal.create_tenant(%s)", d.CmdArgs[0].Key))
				return ""

			case "token-bucket-request":
				if len(d.CmdArgs) != 1 {
					d.Fatalf(t, "expected tenant number")
				}
				tenantID, err := strconv.Atoi(d.CmdArgs[0].Key)
				if err != nil {
					d.Fatalf(t, "%v", err)
				}
				var args struct {
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
					ConsumptionSinceLastRequest: roachpb.TokenBucketRequest_Consumption{
						RU:               args.Consumption.RU,
						ReadRequests:     args.Consumption.ReadReq,
						ReadBytes:        args.Consumption.ReadBytes,
						WriteRequests:    args.Consumption.WriteReq,
						WriteBytes:       args.Consumption.WriteBytes,
						SQLPodCPUSeconds: args.Consumption.SQLPodsCPUUsage,
					},
				}
				_, err = tenantUsage.TokenBucketRequest(ctx, roachpb.MakeTenantID(uint64(tenantID)), &req)
				if err != nil {
					return fmt.Sprintf("error: %v", err)
				}
				return ""

			case "metrics":
				re, err := regexp.Compile(d.Input)
				if err != nil {
					d.Fatalf(t, "failed to compile pattern: %v", err)
				}
				str, err := metrictestutils.GetMetricsText(metricsReg, re)
				if err != nil {
					d.Fatalf(t, "failed to scrape metrics: %v", err)
				}
				return str

			default:
				d.Fatalf(t, "unknown command %q", d.Cmd)
				return ""
			}
		})
	})
}
