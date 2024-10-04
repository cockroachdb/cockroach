// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
)

// TODO: move this below cluster interface, maybe into roachprod.

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
