// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/stretchr/testify/require"
)

// TODO: move this below cluster interface, maybe into roachprod.

// Map tenantId to instance count so we can uniquify each instance and
// run multiple instances on one node.
var tenantIds = make(map[int]int)

// tenantNode corresponds to a running tenant.
type tenantNode struct {
	tenantID          int
	instanceID        int
	httpPort, sqlPort int
	kvAddrs           []string
	pgURL             string
	envVars           []string
	// Same as pgURL but with relative ssl parameters.
	relativeSecureURL string

	binary string // the binary last passed to start()
	errCh  chan error
	node   int
	region string
}

type createTenantOptions struct {
	// Set this to expand the scope of the nodes added to the tenant certs.
	certNodes option.NodeListOption

	// Set this to add additional environment variables to the tenant.
	envVars []string

	// Set this to specify the region for the tenant.
	region string
}
type createTenantOpt func(*createTenantOptions)

func createTenantRegion(region string) createTenantOpt {
	return func(c *createTenantOptions) {
		c.region = region
	}
}

func createTenantNodeInternal(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	kvnodes option.NodeListOption,
	tenantID, node, httpPort, sqlPort int,
	certs bool,
	opts ...createTenantOpt,
) *tenantNode {
	var createOptions createTenantOptions
	for _, o := range opts {
		o(&createOptions)
	}
	instID := tenantIds[tenantID] + 1
	tenantIds[tenantID] = instID
	// In secure mode only the internal address works, possibly because its started
	// with --advertise-addr on internal address?
	kvAddrs, err := c.InternalAddr(ctx, t.L(), kvnodes)
	require.NoError(t, err)
	tn := &tenantNode{
		tenantID:   tenantID,
		instanceID: instID,
		httpPort:   httpPort,
		kvAddrs:    kvAddrs,
		node:       node,
		sqlPort:    sqlPort,
		envVars:    append(config.DefaultEnvVars(), createOptions.envVars...),
		region:     createOptions.region,
	}
	if certs {
		tn.createTenantCert(ctx, t, c, createOptions.certNodes)
	}
	return tn
}

// Deprecated: use Cluster.StartServiceForVirtualCluster instead.
func deprecatedCreateTenantNode(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	kvnodes option.NodeListOption,
	tenantID, node, httpPort, sqlPort int,
	opts ...createTenantOpt,
) *tenantNode {
	return createTenantNodeInternal(ctx, t, c, kvnodes, tenantID, node, httpPort, sqlPort, true /* certs */, opts...)
}

// Deprecated: use Cluster.StartServiceForVirtualCluster instead.
func deprecatedCreateTenantNodeNoCerts(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	kvnodes option.NodeListOption,
	tenantID, node, httpPort, sqlPort int,
	opts ...createTenantOpt,
) *tenantNode {
	return createTenantNodeInternal(ctx, t, c, kvnodes, tenantID, node, httpPort, sqlPort, false /* certs */, opts...)
}

func (tn *tenantNode) createTenantCert(
	ctx context.Context, t test.Test, c cluster.Cluster, certNodes option.NodeListOption,
) {

	if len(certNodes) == 0 {
		certNodes = c.Node(tn.node)
	}

	var names []string
	for _, node := range certNodes {
		eips, err := c.ExternalIP(ctx, t.L(), c.Node(node))
		require.NoError(t, err)
		names = append(names, eips...)
		iips, err := c.InternalIP(ctx, t.L(), c.Node(node))
		require.NoError(t, err)
		names = append(names, iips...)
	}
	names = append(names, "localhost", "127.0.0.1")

	cmd := fmt.Sprintf(
		"./cockroach cert create-tenant-client --certs-dir=%s --ca-key=%s/ca.key %d %s --overwrite",
		install.CockroachNodeCertsDir, install.CockroachNodeCertsDir, tn.tenantID, strings.Join(names, " "))
	c.Run(ctx, option.WithNodes(c.Node(tn.node)), cmd)
}

func (tn *tenantNode) stop(ctx context.Context, t test.Test, c cluster.Cluster) {
	if tn.errCh == nil {
		return
	}
	// Must use pkill because the context cancellation doesn't wait for the
	// process to exit.
	c.Run(ctx, option.WithNodes(c.Node(tn.node)),
		fmt.Sprintf("pkill -o -f '^%s mt start.*tenant-id=%d.*%d'", tn.binary, tn.tenantID, tn.sqlPort))
	t.L().Printf("mt cluster exited: %v", <-tn.errCh)
	tn.errCh = nil
}

func (tn *tenantNode) logDir() string {
	return fmt.Sprintf("logs/mt-%d-%d", tn.tenantID, tn.instanceID)
}

func (tn *tenantNode) storeDir() string {
	return fmt.Sprintf("cockroach-data-mt-%d-%d", tn.tenantID, tn.instanceID)
}

func (tn *tenantNode) start(ctx context.Context, t test.Test, c cluster.Cluster, binary string) {
	require.True(t, c.IsSecure())

	tn.binary = binary
	extraArgs := []string{
		"--log=\"file-defaults: {dir: '" + tn.logDir() + "', exit-on-error: false}\"",
		"--store=" + tn.storeDir()}
	if len(tn.region) > 0 {
		extraArgs = append(extraArgs, "--locality="+tn.region)
	}

	internalIPs, err := c.InternalIP(ctx, t.L(), c.Node(tn.node))
	randomSeed := rand.Int63()
	c.SetRandomSeed(randomSeed)
	require.NoError(t, err)
	tn.errCh = deprecatedStartTenantServer(
		ctx, c, c.Node(tn.node), internalIPs[0], binary, tn.kvAddrs, tn.tenantID,
		tn.httpPort, tn.sqlPort, tn.envVars, randomSeed,
		extraArgs...,
	)

	// The old multitenant API does not create a default admin user for virtual clusters, so root
	// authentication is used instead.
	externalUrls, err := c.ExternalPGUrl(ctx, t.L(), c.Node(tn.node), roachprod.PGURLOptions{Auth: install.AuthRootCert})
	require.NoError(t, err)
	u, err := url.Parse(externalUrls[0])
	require.NoError(t, err)
	externalIPs, err := c.ExternalIP(ctx, t.L(), c.Node(tn.node))
	require.NoError(t, err)
	u.Host = externalIPs[0] + ":" + strconv.Itoa(tn.sqlPort)

	tn.pgURL = u.String()

	// pgURL has full paths to local certs embedded, i.e.
	// /tmp/roachtest-certs3630333874/certs, on the cluster we want just certs
	// (i.e. to run workload on the tenant).
	//
	// The old multitenant API does not create a default admin user for virtual clusters, so root
	// authentication is used instead.
	secureUrls, err := roachprod.PgURL(ctx, t.L(), c.MakeNodes(c.Node(tn.node)), install.CockroachNodeCertsDir, roachprod.PGURLOptions{
		External: false,
		Secure:   true,
		Auth:     install.AuthRootCert,
	})
	require.NoError(t, err)
	u, err = url.Parse(strings.Trim(secureUrls[0], "'"))
	require.NoError(t, err)
	u.Host = externalIPs[0] + ":" + strconv.Itoa(tn.sqlPort)

	tn.relativeSecureURL = u.String()

	// The tenant is usually responsive ~right away, but it has on occasions
	// taken more than 3s for it to connect to the KV layer, and it won't open
	// the SQL port until it has.
	testutils.SucceedsSoon(t, func() error {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case err := <-tn.errCh:
			t.Fatal(err)
		default:
		}

		db, err := gosql.Open("postgres", tn.pgURL)
		if err != nil {
			return err
		}
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		_, err = db.ExecContext(ctx, `SELECT 1`)
		return err
	})

	t.L().Printf("sql server for tenant %d (instance %d) now running", tn.tenantID, tn.instanceID)
}

// Deprecated: use Cluster.StartServiceForVirtualCluster instead.
func deprecatedStartTenantServer(
	tenantCtx context.Context,
	c cluster.Cluster,
	node option.NodeListOption,
	internalIP string,
	binary string,
	kvAddrs []string,
	tenantID int,
	httpPort int,
	sqlPort int,
	envVars []string,
	randomSeed int64,
	extraFlags ...string,
) chan error {
	args := []string{
		"--certs-dir", install.CockroachNodeCertsDir,
		"--tenant-id=" + strconv.Itoa(tenantID),
		"--http-addr", ifLocal(c, "127.0.0.1", "0.0.0.0") + ":" + strconv.Itoa(httpPort),
		"--kv-addrs", strings.Join(kvAddrs, ","),
		// Don't bind to external interfaces when running locally.
		"--sql-addr", ifLocal(c, "127.0.0.1", internalIP) + ":" + strconv.Itoa(sqlPort),
	}
	args = append(args, extraFlags...)

	errCh := make(chan error, 1)
	go func() {
		// Set the same random seed for every tenant; this is picked up by
		// runs that use a build with runtime assertions enabled, and
		// ignored otherwise.
		envVars = append(envVars, fmt.Sprintf("COCKROACH_RANDOM_SEED=%d", randomSeed))
		errCh <- c.RunE(tenantCtx, option.WithNodes(node),
			append(append(append([]string{}, envVars...), binary, "mt", "start-sql"), args...)...,
		)
		close(errCh)
	}()
	return errCh
}

// deprecatedCreateTenantAdminRole creates a role that can be used to log into a secure cluster's db console.
// Deprecated: use Cluster.StartServiceForVirtualCluster instead.
func deprecatedCreateTenantAdminRole(
	t test.Test, tenantName string, tenantSQL *sqlutils.SQLRunner,
) {
	tenantSQL.Exec(t, fmt.Sprintf(`CREATE ROLE IF NOT EXISTS %s WITH LOGIN PASSWORD '%s'`, install.DefaultUser, install.DefaultPassword))
	tenantSQL.Exec(t, fmt.Sprintf(`GRANT ADMIN TO %s`, install.DefaultUser))
	t.L().Printf(`Log into %s db console with username "%s" and password "%s"`,
		tenantName, install.DefaultUser, install.DefaultPassword)
}

const appTenantName = "app"

// deprecatedCreateInMemoryTenant runs through the necessary steps to create an in-memory
// tenant without resource limits and full dbconsole viewing privileges.
// Deprecated: use Cluster.StartServiceForVirtualCluster instead.
func deprecatedCreateInMemoryTenant(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	tenantName string,
	nodes option.NodeListOption,
	secure bool,
) {
	db := deprecatedCreateInMemoryTenantWithConn(ctx, t, c, tenantName, nodes, secure)
	db.Close()
}

// deprecatedCreateInMemoryTenantWithConn runs through the necessary steps to create an
// in-memory tenant without resource limits and full dbconsole viewing
// privileges. As a convenience, it also returns a connection to the tenant (on
// a random node in the cluster).
// Deprecated: use Cluster.StartServiceForVirtualCluster instead.
func deprecatedCreateInMemoryTenantWithConn(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	tenantName string,
	nodes option.NodeListOption,
	secure bool,
) *gosql.DB {
	sysDB := c.Conn(ctx, t.L(), nodes.RandNode()[0])
	defer sysDB.Close()
	sysSQL := sqlutils.MakeSQLRunner(sysDB)
	sysSQL.Exec(t, "CREATE TENANT $1", tenantName)

	tenantConn := deprecatedStartInMemoryTenant(ctx, t, c, tenantName, nodes)
	tenantSQL := sqlutils.MakeSQLRunner(tenantConn)
	if secure {
		deprecatedCreateTenantAdminRole(t, tenantName, tenantSQL)
	}
	return tenantConn
}

// deprecatedStartInMemoryTenant starts an in memory tenant that has already been created.
// This function also removes tenant rate limiters and sets a few cluster
// settings on the tenant.  As a convenience, it also returns a connection to
// the tenant (on a random node in the cluster).
// Deprecated: use Cluster.StartServiceForVirtualCluster instead.
func deprecatedStartInMemoryTenant(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	tenantName string,
	nodes option.NodeListOption,
) *gosql.DB {
	sysDB := c.Conn(ctx, t.L(), nodes.RandNode()[0])
	defer sysDB.Close()
	sysSQL := sqlutils.MakeSQLRunner(sysDB)
	sysSQL.Exec(t, "ALTER TENANT $1 START SERVICE SHARED", tenantName)
	sysSQL.Exec(t, `ALTER TENANT $1 GRANT CAPABILITY can_view_node_info=true, can_admin_split=true,can_view_tsdb_metrics=true`, tenantName)
	sysSQL.Exec(t, `ALTER TENANT $1 SET CLUSTER SETTING sql.zone_configs.allow_for_secondary_tenant.enabled=true`, tenantName)
	sysSQL.Exec(t, `ALTER TENANT $1 SET CLUSTER SETTING sql.virtual_cluster.feature_access.multiregion.enabled=true`, tenantName)
	// The following two statements can be removed once this code only ever
	// runs for v23.2 and later.
	sysSQL.Exec(t, `ALTER TENANT $1 SET CLUSTER SETTING enterprise.license = $2`, tenantName, config.CockroachDevLicense)
	sysSQL.Exec(t, `ALTER TENANT $1 SET CLUSTER SETTING cluster.organization = 'Cockroach Labs - Production Testing'`, tenantName)
	removeTenantRateLimiters(t, sysSQL, tenantName)

	// Opening a SQL session to a newly created in-process tenant may require a
	// few retries. Unfortunately, the c.ConnE and MakeSQLRunner APIs do not make
	// it clear if they eagerly open a session with the tenant or wait until the
	// first query. Therefore, wrap connection opening and a ping to the tenant
	// server in a retry loop.
	var tenantConn *gosql.DB
	testutils.SucceedsSoon(t, func() error {
		var err error
		// The old multitenant API does not create a default admin user for virtual clusters, so root
		// authentication is used instead.
		tenantConn, err = c.ConnE(ctx, t.L(), nodes.RandNode()[0], option.VirtualClusterName(tenantName), option.AuthMode(install.AuthRootCert))
		if err != nil {
			return err
		}
		if err = tenantConn.Ping(); err != nil {
			tenantConn.Close()
			return err
		}
		return nil
	})

	return tenantConn
}

// removeTenantRateLimiters ensures the tenant is not throttled by limiters.
func removeTenantRateLimiters(t test.Test, systemSQL *sqlutils.SQLRunner, tenantName string) {
	systemSQL.Exec(t, `ALTER TENANT $1 GRANT CAPABILITY exempt_from_rate_limiting=true`, tenantName)
}
