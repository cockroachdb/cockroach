// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestEndpointTelemetryBasic tests that the telemetry collection on the usage of
// CRDB's endpoints works as expected by recording the call counts of `Admin` &
// `Status` requests, for both gRPC and dRPC gateways.
func TestEndpointTelemetryBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name    string
		drpcOpt base.DefaultTestDRPCOption
		gateway string
	}{
		{"gRPC", base.TestDRPCDisabled, "grpc"},
		{"dRPC", base.TestDRPCEnabled, "drpc"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := serverutils.StartServerOnly(t, base.TestServerArgs{
				DefaultTestTenant: base.TestDoesNotWorkWithSecondaryTenantsButWeDontKnowWhyYet(81590),
				DefaultDRPCOption: tc.drpcOpt,
			})
			defer s.Stopper().Stop(context.Background())

			counter := func(method string) telemetry.Counter {
				return telemetry.GetCounter(rpc.GatewayEndpointCounterName(tc.gateway, method))
			}

			// Check that calls over HTTP are recorded.
			var details serverpb.LocationsResponse
			if err := srvtestutils.GetAdminJSONProto(s, "locations", &details); err != nil {
				t.Fatal(err)
			}
			require.GreaterOrEqual(t, telemetry.Read(counter(
				"/cockroach.server.serverpb.Admin/Locations",
			)), int32(1))

			var resp serverpb.StatementsResponse
			if err := srvtestutils.GetStatusJSONProto(s, "statements", &resp); err != nil {
				t.Fatal(err)
			}
			require.Equal(t, int32(1), telemetry.Read(counter(
				"/cockroach.server.serverpb.Status/Statements",
			)))
		})
	}
}
