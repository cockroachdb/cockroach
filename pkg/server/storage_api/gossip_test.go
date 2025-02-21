// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage_api_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestStatusGossipJson(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestTenantProbabilisticOnly,
	})
	defer srv.Stopper().Stop(context.Background())

	if srv.TenantController().StartedDefaultTestTenant() {
		// explicitly enabling CanViewNodeInfo capability for the secondary/application tenant
		_, err := srv.SystemLayer().SQLConn(t).Exec(
			`ALTER TENANT [$1] GRANT CAPABILITY can_view_node_info=true`,
			serverutils.TestTenantID().ToUint64(),
		)
		require.NoError(t, err)
		serverutils.WaitForTenantCapabilities(t, srv, serverutils.TestTenantID(),
			map[tenantcapabilities.ID]string{tenantcapabilities.CanViewNodeInfo: "true"}, "")
	}
	s := srv.ApplicationLayer()
	require.NoError(t, validateGossipResponse(s))
}

func TestStatusGossipJsonWithoutTenantCapability(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		// Note: We're only testing external-process mode because shared service
		// mode tenants have all capabilities.
		DefaultTestTenant: base.ExternalTestTenantAlwaysEnabled,
	})
	defer srv.Stopper().Stop(context.Background())

	s := srv.ApplicationLayer()
	actualErr := validateGossipResponse(s)
	require.Error(t, actualErr)
	require.Contains(t, actualErr.Error(), "client tenant does not have capability to query cluster node metadata")
}

func validateGossipResponse(s serverutils.ApplicationLayerInterface) error {
	var data gossip.InfoStatus
	if err := srvtestutils.GetStatusJSONProto(s, "gossip/local", &data); err != nil {
		return err
	}
	if _, ok := data.Infos["first-range"]; !ok {
		return fmt.Errorf("no first-range info returned: %v", data)
	}
	if _, ok := data.Infos["cluster-id"]; !ok {
		return fmt.Errorf("no clusterID info returned: %v", data)
	}
	if _, ok := data.Infos["node:1"]; !ok {
		return fmt.Errorf("no node 1 info returned: %v", data)
	}
	return nil
}
