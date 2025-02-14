// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage_api_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// TestStatusJson verifies that status endpoints return expected Json results.
// The content type of the responses is always httputil.JSONContentType.
func TestStatusJson(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(context.Background())
	s := srv.SystemLayer()

	nodeID := srv.StorageLayer().NodeID()
	addr := s.AdvRPCAddr()
	sqlAddr := s.AdvSQLAddr()

	var nodes serverpb.NodesResponse
	testutils.SucceedsSoon(t, func() error {
		if err := srvtestutils.GetStatusJSONProto(s, "nodes", &nodes); err != nil {
			t.Fatal(err)
		}

		if len(nodes.Nodes) == 0 {
			return errors.Errorf("expected non-empty node list, got: %v", nodes)
		}
		return nil
	})

	for _, path := range []string{
		apiconstants.StatusPrefix + "details/local",
		apiconstants.StatusPrefix + "details/" + strconv.FormatUint(uint64(nodeID), 10),
	} {
		var details serverpb.DetailsResponse
		if err := serverutils.GetJSONProto(s, path, &details); err != nil {
			t.Fatal(err)
		}
		if a, e := details.NodeID, nodeID; a != e {
			t.Errorf("expected: %d, got: %d", e, a)
		}
		if a, e := details.Address.String(), addr; a != e {
			t.Errorf("expected: %v, got: %v", e, a)
		}
		if a, e := details.SQLAddress.String(), sqlAddr; a != e {
			t.Errorf("expected: %v, got: %v", e, a)
		}
		if a, e := details.BuildInfo, build.GetInfo(); a != e {
			t.Errorf("expected: %v, got: %v", e, a)
		}
	}
}

// TestNodeStatusResponse verifies that node status returns the expected
// results.
func TestNodeStatusResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type testCase struct {
		name                     string
		defaultTestTenantOptions base.DefaultTestTenantOptions
		expectedErr              string
	}

	testCases := []testCase{
		{
			name:                     "test_node_status_reponse_shared_tenant",
			defaultTestTenantOptions: base.SharedTestTenantAlwaysEnabled,
		},
		{
			name:                     "test_node_status_reponse_external_tenant",
			defaultTestTenantOptions: base.ExternalTestTenantAlwaysEnabled,
			expectedErr:              "status: 500", // `can_view_node_info` capability is missing
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv := serverutils.StartServerOnly(t, base.TestServerArgs{
				// Test only shared tenants. More fixes are needed to make
				// it work for external tenants.
				DefaultTestTenant: tc.defaultTestTenantOptions,
			})
			defer srv.Stopper().Stop(context.Background())

			s := srv.ApplicationLayer()
			node := srv.StorageLayer().Node().(*server.Node)

			wrapper := serverpb.NodesResponse{}

			// Check that the node statuses cannot be accessed via a non-admin account.
			if err := srvtestutils.GetStatusJSONProtoWithAdminOption(s, "nodes", &wrapper, false /* isAdmin */); !testutils.IsError(err, "status: 403") {
				t.Fatalf("expected privilege error, got %v", err)
			}

			// Now fetch all the node statuses as admin.
			if err := srvtestutils.GetStatusJSONProto(s, "nodes", &wrapper); !testutils.IsError(err, tc.expectedErr) {
				t.Fatal(err)
			}

			if tc.expectedErr == "" {
				nodeStatuses := wrapper.Nodes

				if len(nodeStatuses) != 1 {
					t.Errorf("too many node statuses returned - expected:1 actual:%d", len(nodeStatuses))
				}
				if !node.Descriptor.Equal(&nodeStatuses[0].Desc) {
					t.Errorf("node status descriptors are not equal\nexpected:%+v\nactual:%+v\n", node.Descriptor, nodeStatuses[0].Desc)
				}

				// Now fetch each one individually. Loop through the nodeStatuses to use the
				// ids only.
				for _, oldNodeStatus := range nodeStatuses {
					nodeStatus := statuspb.NodeStatus{}
					nodeURL := "nodes/" + oldNodeStatus.Desc.NodeID.String()
					// Check that the node statuses cannot be accessed via a non-admin account.
					if err := srvtestutils.GetStatusJSONProtoWithAdminOption(s, nodeURL, &nodeStatus, false /* isAdmin */); !testutils.IsError(err, "status: 403") {
						t.Fatalf("expected privilege error, got %v", err)
					}

					// Now access that node's status.
					if err := srvtestutils.GetStatusJSONProto(s, nodeURL, &nodeStatus); err != nil {
						t.Fatal(err)
					}
					if !node.Descriptor.Equal(&nodeStatus.Desc) {
						t.Errorf("node status descriptors are not equal\nexpected:%+v\nactual:%+v\n", node.Descriptor, nodeStatus.Desc)
					}
				}
			}
		})
	}
}

// TestMetricsRecording verifies that Node statistics are periodically recorded
// as time series data.
func TestMetricsRecording(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer s.Stopper().Stop(ctx)

	// Verify that metrics for the current timestamp are recorded. This should
	// be true very quickly even though DefaultMetricsSampleInterval is large,
	// because the server writes an entry eagerly on startup.
	testutils.SucceedsSoon(t, func() error {
		now := s.Clock().PhysicalNow()

		var data roachpb.InternalTimeSeriesData
		for _, keyName := range []string{
			"cr.store.livebytes.1",
			"cr.node.sys.go.allocbytes.1",
		} {
			key := ts.MakeDataKey(keyName, "", ts.Resolution10s, now)
			if err := kvDB.GetProto(ctx, key, &data); err != nil {
				return err
			}
		}
		return nil
	})
}

// TestMetricsEndpoint retrieves the metrics endpoint, which is currently only
// used for development purposes. The metrics within the response are verified
// in other tests.
func TestMetricsEndpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(context.Background())

	s := srv.ApplicationLayer()

	if _, err := srvtestutils.GetText(s, s.AdminURL().WithPath(apiconstants.StatusPrefix+"metrics/"+srv.NodeID().String()).String()); err != nil {
		t.Fatal(err)
	}
}

func TestNodesGRPCResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(110023),
	})
	defer srv.Stopper().Stop(ctx)

	if srv.DeploymentMode().IsExternal() {
		// Enable access to the nodes endpoint for the test tenant.
		require.NoError(t, srv.GrantTenantCapabilities(
			ctx, serverutils.TestTenantID(),
			map[tenantcapabilities.ID]string{tenantcapabilities.CanViewNodeInfo: "true"}))
	}

	var request serverpb.NodesRequest

	s := srv.ApplicationLayer()
	client := s.GetStatusClient(t)

	response, err := client.Nodes(ctx, &request)
	if err != nil {
		t.Fatal(err)
	}

	if a, e := len(response.Nodes), 1; a != e {
		t.Errorf("expected %d node(s), found %d", e, a)
	}
}
