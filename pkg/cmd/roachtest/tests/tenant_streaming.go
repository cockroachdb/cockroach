package tests

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type streamTestArgs struct {
	workloadType     workloadType
	workloadDuration string
	crdbChaos        bool
}

func srcClusterSettings(t test.Test, db *sqlutils.SQLRunner) {
	db.ExecMultiple(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true;`,
		`SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s';`,
		`SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms';`,
		`SET CLUSTER SETTING stream_replication.job_liveness_timeout = '20s';`,
		`SET CLUSTER SETTING stream_replication.stream_liveness_track_frequency = '2s';`,
		`SET CLUSTER SETTING stream_replication.min_checkpoint_frequency = '1s';`,
		`SET CLUSTER SETTING jobs.registry.interval.adopt = '1s'`)
}

func destClusterSettings(t test.Test, db *sqlutils.SQLRunner) {
	db.ExecMultiple(t, `SET enable_experimental_stream_replication = true;`,
		`SET CLUSTER SETTING stream_replication.consumer_heartbeat_frequency = '100ms';`,
		`SET CLUSTER SETTING bulkio.stream_ingestion.minimum_flush_interval = '10ms';`,
		`SET CLUSTER SETTING bulkio.stream_ingestion.cutover_signal_poll_interval = '100ms';`,
		`SET CLUSTER SETTING jobs.registry.interval.adopt = '1s'`)
}

// Returns the ingestion job ID and the stream ID.
func startReplicationStream(t test.Test, srcSQL *sqlutils.SQLRunner, destSQL *sqlutils.SQLRunner, url string, srcTenant int, destTenant int) (int, int, error) {
	// With initial scan
	streamReplStmt := fmt.Sprintf("RESTORE TENANT %d FROM REPLICATION STREAM FROM '%s' AS TENANT %d",
		srcTenant, url, destTenant)

	fmt.Println("restore statement", streamReplStmt)
	var ingestionJobID, streamProducerJobID int
	destSQL.QueryRow(t, streamReplStmt).Scan(&ingestionJobID)
	t.Status(fmt.Sprintf("created stream ingestion job %d\n", ingestionJobID))
	srcSQL.SucceedsSoonDuration = 5 * time.Minute
	defer func() {
		srcSQL.SucceedsSoonDuration = testutils.DefaultSucceedsSoonDuration
	}()

	srcSQL.CheckQueryResultsRetry(t,
		"SELECT count(*) FROM [SHOW JOBS] WHERE job_type = 'STREAM REPLICATION'", [][]string{{"1"}})
	srcSQL.QueryRow(t, "SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'STREAM REPLICATION'").
		Scan(&streamProducerJobID)
	t.Status(fmt.Sprintf("created stream producer job %d\n", streamProducerJobID))
	return ingestionJobID, streamProducerJobID, nil
}

func stopReplicationStream(t test.Test, srcSQL *sqlutils.SQLRunner, destSQL *sqlutils.SQLRunner, ingestionJob int, producerJob int) {
	var cutoverTime time.Time
	srcSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&cutoverTime)

	// Cut over the ingestion job and the job will stop eventually.
	destSQL.Exec(t, `SELECT crdb_internal.complete_stream_ingestion_job($1, $2)`, ingestionJob, cutoverTime)

	destSQL.SucceedsSoonDuration = 5 * time.Minute
	srcSQL.SucceedsSoonDuration = 5 * time.Minute
	defer func() {
		destSQL.SucceedsSoonDuration = testutils.DefaultSucceedsSoonDuration
		srcSQL.SucceedsSoonDuration = testutils.DefaultSucceedsSoonDuration
	}()
	destSQL.CheckQueryResultsRetry(t,
		fmt.Sprintf("SELECT status FROM [SHOW JOBS] WHERE job_id = %d", ingestionJob), [][]string{{"succeeded"}})
	srcSQL.CheckQueryResultsRetry(t,
		fmt.Sprintf("SELECT status FROM [SHOW JOBS] WHERE job_id = %d", producerJob), [][]string{{"succeeded"}})
}

func verifyContent(t test.Test, srcSQL *sqlutils.SQLRunner, destSQL *sqlutils.SQLRunner, targets []string) int {
	verifiedRows := 0
	for _, target := range targets {
		// TODO(casper): make this iterative using cursor to avoid using too much memory
		query := fmt.Sprintf("SELECT * FROM %s LIMIT 10000", target)
		srcData := srcSQL.QueryStr(t, query)
		destData := destSQL.QueryStr(t, query)
		require.Equal(t, srcData, destData)
		verifiedRows += len(srcData)
	}
	return verifiedRows
}

// One tenant SQL node for source and dest cluster respectively and a workload node.
// At least one KV node for source host cluster and destination host cluster respectively.
const requiredNodeCount = 3

func convertPGUrlInline(ctx context.Context, t test.Test, c cluster.Cluster, node option.NodeListOption,
	pgUrlDir string, urlString string) string {
	// Copy the cert from source into dest
	tmpLocalDir, err := filepath.Abs("./certs")
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, t.L(), pgUrlDir, tmpLocalDir, node))
	pgUrl, err := url.Parse(urlString)
	require.NoError(t, err)
	options := pgUrl.Query()
	setKeyInline := func(key string, fileName string) {
		content, err := os.ReadFile(fmt.Sprintf("%s/certs/%s", tmpLocalDir, fileName))
		require.NoError(t, err)
		options.Set(key, string(content))
	}
	options.Set("sslinline", "true")
	options.Set("sslmode", "verify-full")
	setKeyInline("sslrootcert", "ca.crt")
	setKeyInline("sslcert", "client.root.crt")
	setKeyInline("sslkey", "client.root.key")
	pgUrl.RawQuery = options.Encode()
	return pgUrl.String()
}

func tenantStreamingTest(ctx context.Context, t test.Test, c cluster.Cluster, args streamTestArgs) {
	if nodeCount := c.Spec().NodeCount; nodeCount < requiredNodeCount {
		t.Fatal(errors.Errorf("tenant streaming tests require %d nodes at minimum but only have %d nodes",
			requiredNodeCount, nodeCount))
	}

	_, srcCrdbNodes := c.Node(1), c.Range(1, c.Spec().NodeCount/2)
	_, destCrdbNodes := c.Node(c.Spec().NodeCount/2+1), c.Range(c.Spec().NodeCount/2+1, c.Spec().NodeCount-1)
	workloadNode := c.Node(c.Spec().NodeCount)
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", workloadNode)

	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Put(ctx, t.Cockroach(), "./cockroach")

	// Create a source CRDB cluster
	srcStartOps := option.DefaultStartOpts()
	srcStartOps.RoachprodOpts.InitTarget = 1
	srcClusterSetting := install.MakeClusterSettings(install.SecureOption(true))
	c.Start(ctx, t.L(), srcStartOps, srcClusterSetting, srcCrdbNodes)

	addr, err := c.ExternalPGUrl(ctx, t.L(), srcCrdbNodes)
	if err != nil {
		t.Fatalf("error in retrieving source cluster PG URL: %s", err)
	}

	t.Status("converting pg url into sslinline mode")
	pgUrl := convertPGUrlInline(ctx, t, c, c.Node(1), srcClusterSetting.PGUrlCertsDir, addr[0])
	destStartOps := option.DefaultStartOpts()
	destStartOps.RoachprodOpts.InitTarget = c.Spec().NodeCount/2 + 1
	destClusterSetting := install.MakeClusterSettings(install.SecureOption(true))
	c.Start(ctx, t.L(), destStartOps, destClusterSetting, destCrdbNodes)

	srcSQL, destSQL := sqlutils.MakeSQLRunner(c.Conn(ctx, t.L(), 1)),
		sqlutils.MakeSQLRunner(c.Conn(ctx, t.L(), c.Spec().NodeCount/2+1))
	srcClusterSettings(t, srcSQL)
	destClusterSettings(t, destSQL)

	srcTenantID, destTenantID := 10, 20
	const (
		tenantHTTPPort = 8081
		tenantSQLPort  = 30258
	)
	// Create source tenant
	srcSQL.Exec(t, `SELECT crdb_internal.create_tenant($1)`, srcTenantID)
	srcTenant := createTenantNode(ctx, t, c, srcCrdbNodes, srcTenantID, 1, tenantHTTPPort, tenantSQLPort)
	srcTenant.start(ctx, t, c, "./cockroach")
	defer srcTenant.stop(ctx, t, c)

	// Create dest tenant
	destSQL.Exec(t, `SELECT crdb_internal.create_tenant($1)`, destTenantID)
	destTenant := createTenantNode(ctx, t, c, destCrdbNodes, destTenantID, c.Spec().NodeCount/2+1, tenantHTTPPort, tenantSQLPort)
	destTenant.start(ctx, t, c, "./cockroach")
	defer destTenant.stop(ctx, t, c)

	srcTenantSQL, err := srcTenant.sqlRunner()
	if err != nil {
		t.Fatalf(err.Error())
	}
	destTenantSQL, err := destTenant.sqlRunner()
	if err != nil {
		t.Fatalf(err.Error())
	}

	//fmt.Printf("SRC TENANTS JOBS: %s\n", srcTenantSQL.QueryStr(t, "SHOW JOBS"))
	//fmt.Printf("DEST TENANTS JOBS: %s\n", destTenantSQL.QueryStr(t, "SHOW JOBS"))

	m := c.NewMonitor(ctx, srcCrdbNodes)
	workloadCompleteCh := make(chan struct{}, 1)

	var wl cdcWorkload
	t.Status(fmt.Sprintf("installing workload: %s", args.workloadType))
	if args.workloadType == tpccWorkloadType {
		wl = &tpccWorkload{
			sqlNodes:           c.Node(1), // TODO(casper): cannot use this node to get pgurl
			sqlPgUrl:           srcTenant.pgURL,
			workloadNodes:      workloadNode,
			tpccWarehouseCount: 20,
			// TolerateErrors if crdbChaos is true; otherwise, the workload will fail
			// if it attempts to use the node which was brought down by chaos.
			// TODO(casper): introduce chaos into both clusters
			tolerateErrors: args.crdbChaos,
			t:              t,
		}
	} else if args.workloadType == ledgerWorkloadType {
		wl = &ledgerWorkload{
			sqlNodes:      c.Node(1),
			workloadNodes: workloadNode,
		}
	}

	wl.install(ctx, c)

	// TODO(dan,ajwerner): sleeping momentarily before running the workload
	// mitigates errors like "error in newOrder: missing stock row" from tpcc.
	if args.workloadType == tpccWorkloadType {
		time.Sleep(2 * time.Second)
	}
	t.Status("initiating workload")
	m.Go(func(ctx context.Context) error {
		now := timeutil.Now()
		defer func() {
			close(workloadCompleteCh)
			fmt.Printf("workload duration %s\n", timeutil.Since(now))
		}()
		wl.run(ctx, c, args.workloadDuration)
		return nil
	})

	m.Go(func(ctx context.Context) error {
		t.Status("starting stream replication")
		ingestionJob, producerJob, err := startReplicationStream(t, srcSQL, destSQL, pgUrl, srcTenantID, destTenantID)
		if err != nil {
			t.Fatalf("error in creating replication stream: %s", err)
		}
		// Wait for workload to complete
		<-workloadCompleteCh

		t.Status("workload completed, stopping stream replication")
		stopReplicationStream(t, srcSQL, destSQL, ingestionJob, producerJob)

		var targets []string
		if args.workloadType == tpccWorkloadType {
			targets = []string{"tpcc.warehouse", "tpcc.district", "tpcc.customer",
				"tpcc.history", "tpcc.order", "tpcc.new_order", "tpcc.item", "tpcc.stock", "tpcc.order_line"}
		} else if args.workloadType == ledgerWorkloadType {
			targets = []string{"ledger.customer", "ledger.transaction", "ledger.entry", "ledger.session"}
		}

		// verify correctness
		t.Status("verifying content between source cluster and destination cluster")
		verifiedRows := verifyContent(t, srcTenantSQL, destTenantSQL, targets)
		t.Status(fmt.Sprintf("finished verifying %d rows of data", verifiedRows))
		return nil
	})

	m.Wait()
}

func registerTenantStreaming(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:            "cdc/tenant-streaming-tpcc",
		Owner:           registry.OwnerCDC,
		Cluster:         r.MakeClusterSpec(3, spec.CPU(8)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			tenantStreamingTest(ctx, t, c, streamTestArgs{
				workloadType:     tpccWorkloadType,
				workloadDuration: "20s",
			})
		},
	})

	r.Add(registry.TestSpec{
		Name:            "cdc/tenant-streaming-ledger",
		Owner:           registry.OwnerCDC,
		Cluster:         r.MakeClusterSpec(5, spec.CPU(8)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			tenantStreamingTest(ctx, t, c, streamTestArgs{
				workloadDuration: "20s",
				workloadType:     ledgerWorkloadType,
			})
		},
	})
}
