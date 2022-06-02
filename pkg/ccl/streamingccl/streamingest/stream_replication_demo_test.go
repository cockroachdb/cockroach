package streamingest

import (
	"context"
	gosql "database/sql"
	"flag"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

var tenant = flag.Uint64("tenant", 10, "source tenant ID")
var numNodes = flag.Int("num_nodes", 1, "number of nodes in the cluster")

func startTestClusterWithTenantDemo(
	ctx context.Context,
	t *testing.T,
	serverArgs base.TestServerArgs,
	tenantID roachpb.TenantID,
	numNodes int,
) (
	serverutils.TestClusterInterface,
	serverutils.TestTenantInterface,
	*gosql.DB,
	*gosql.DB,
	func(),
) {
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	c := testcluster.StartTestCluster(t, numNodes, params)
	// TODO(casper): support adding splits when we have multiple nodes.
	ten, tenantConn := serverutils.StartTenant(t, c.Server(0), base.TestTenantArgs{TenantID: tenantID})
	return c, ten, c.ServerConn(0), tenantConn, func() {
		tenantConn.Close()
		c.Stopper().Stop(ctx)
	}
}

func TestDemoCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	args := base.TestServerArgs{Knobs: base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
		DisableDefaultTestTenant: true,
	}

	tenantID := roachpb.MakeTenantID(*tenant)
	c, ten, sysSQL, _, cleanup := startTestClusterWithTenantDemo(ctx, t, args, tenantID, *numNodes)
	defer cleanup()

	cs := []string{
		`SET CLUSTER SETTING kv.rangefeed.enabled = true;`,
		`SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s';`,
		`SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms';`,
		// producer job liveness check timeout
		`SET CLUSTER SETTING stream_replication.job_liveness_timeout = '30m';`,
		// how frequent the producer job tracks its liveness
		`SET CLUSTER SETTING stream_replication.stream_liveness_track_frequency = '2s';`,
		// how frequent at least we emit checkpoint events on the source side
		`SET CLUSTER SETTING stream_replication.min_checkpoint_frequency = '2s';`,
		// how frequent consumer ingestion job send heartbeats
		`SET CLUSTER SETTING stream_replication.consumer_heartbeat_frequency = '3s';`,
		`SET CLUSTER SETTING bulkio.stream_ingestion.minimum_flush_interval = '10ms';`,
		`SET CLUSTER SETTING bulkio.stream_ingestion.cutover_signal_poll_interval = '500ms';`,
	}
	sqlutils.MakeSQLRunner(sysSQL).ExecMultiple(t, cs...)

	// Print some connection strings
	sysPGURL, cleanupSinkCert := sqlutils.PGUrl(t, c.Server(0).ServingSQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanupSinkCert()
	tenantPGURL, cleanupTenCert := sqlutils.PGUrl(t, ten.SQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanupTenCert()

	var startTime string
	require.NoError(t, sysSQL.QueryRow("SELECT cluster_logical_timestamp()").Scan(&startTime))
	go func() {
		time.Sleep(5 * time.Second)

		fmt.Printf("c2c: creating a cluster with %d nodes and creating tenant %d\n", *numNodes, *tenant)
		fmt.Println("c2c: system sql:")
		fmt.Printf("c2c: ./cockroach sql --url=\"%s\"\n", sysPGURL.String())
		fmt.Println("c2c: tenant sql:")
		fmt.Printf("c2c: ./cockroach sql --url=\"%s\"\n", tenantPGURL.String())

		fmt.Println("c2c: =====================================================")
		fmt.Printf("c2c: Initial stream replication using this cluster as source: \n")
		fmt.Printf("c2c: SET enable_experimental_stream_replication = true; "+
			"RESTORE TENANT {src_tenant_id} FROM REPLICATION STREAM FROM \"%s\" AS TENANT {dest_tenant_id};\n", sysPGURL.String())
		fmt.Println("c2c: =====================================================")
		fmt.Printf("c2c: Resume from previous stream replication: \n")
		fmt.Printf("c2c: RESUME JOB <ingestion_job_id>\n")
		fmt.Println("c2c: ==============CLUSTER SETTINGS===============")
		for _, s := range cs {
			fmt.Printf("c2c: %s\n", s)
		}
		fmt.Println("c2c: =====================================================")
		fmt.Printf("c2c: cut over the replication stream: (cutover timestamp can be retrieved by SELECT clock_timestamp() on the source)\n")
		fmt.Printf("c2c: SELECT crdb_internal.complete_stream_ingestion_job({ingestion_job_id}, {cutover_timestamp});\n")
	}()
	time.Sleep(2 * time.Hour)
}
