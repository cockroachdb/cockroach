package serverccl

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTenantWithDecommissionedID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := serverutils.StartNewTestCluster(t, 4, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	server := tc.Server(0)
	decommissionID := tc.Server(3).NodeID()
	require.NoError(t, server.Decommission(ctx, livenesspb.MembershipStatus_DECOMMISSIONING, []roachpb.NodeID{decommissionID}))
	require.NoError(t, server.Decommission(ctx, livenesspb.MembershipStatus_DECOMMISSIONED, []roachpb.NodeID{decommissionID}))

	tenantID := serverutils.TestTenantID()

	var tenantSQLServer serverutils.TestTenantInterface
	var tenantDB *sql.DB
	for instanceID := 1; instanceID <= int(decommissionID); instanceID++ {
		sqlServer, tenant := serverutils.StartTenant(t, server, base.TestTenantArgs{
			TenantID: tenantID,
			Existing: instanceID != 1,
		})
		defer tenant.Close()
		if sqlServer.RPCContext().NodeID.Get() == decommissionID {
			tenantSQLServer = sqlServer
			tenantDB = tenant
		}
	}
	require.NotNil(t, tenantSQLServer)

	// This sleep is here to give the tenantSQLServer an opportunity to
	// heartbeat the KV layer. If the KV heartbeat is rejected, the tenant will
	// be unable to execute sql.
	time.Sleep(5 * time.Second)

	_, err := tenantDB.Exec("CREATE ROLE test_user WITH PASSWORD 'password'")
	require.NoError(t, err)
}
