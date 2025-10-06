// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregionccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// This is a rather contrived test to demonstrate that one can successfully,
// albeit with some arm-twisting, convince the sql layer to make regional-
// by-row tables in the system database.
func TestRegionalByRowTablesInTheSystemDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(t, 3, base.TestingKnobs{})
	defer cleanup()
	defer tc.Stopper().Stop(ctx)

	tenant, tenantDB := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
		TenantName:  "test",
		TenantID:    serverutils.TestTenantID(),
		UseDatabase: "defaultdb",
	})
	defer tenant.AppStopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(tenantDB)
	tdb.Exec(t, `ALTER DATABASE system SET PRIMARY REGION "us-east1";`)
	// Pick an arbitrary table that we don't interact with via KV to make
	// REGIONAL BY ROW.
	tdb.Exec(t, `ALTER TABLE system.web_sessions SET LOCALITY REGIONAL BY ROW`)
	tdb.Exec(t, `ALTER TABLE system.namespace SET LOCALITY GLOBAL`)
	tdb.CheckQueryResults(t, `
SELECT create_statement FROM [SHOW CREATE DATABASE system]
UNION ALL SELECT create_statement FROM [SHOW CREATE TABLE system.web_sessions]
UNION ALL SELECT create_statement FROM [SHOW CREATE TABLE system.namespace]
`, [][]string{
		{`CREATE DATABASE system PRIMARY REGION "us-east1" REGIONS = "us-east1" SURVIVE ZONE FAILURE`},
		{`CREATE TABLE public.web_sessions (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	"hashedSecret" BYTES NOT NULL,
	username STRING NOT NULL,
	"createdAt" TIMESTAMP NOT NULL DEFAULT now():::TIMESTAMP,
	"expiresAt" TIMESTAMP NOT NULL,
	"revokedAt" TIMESTAMP NULL,
	"lastUsedAt" TIMESTAMP NOT NULL DEFAULT now():::TIMESTAMP,
	"auditInfo" STRING NULL,
	user_id OID NOT NULL,
	crdb_region public.crdb_internal_region NOT VISIBLE NOT NULL DEFAULT default_to_database_primary_region(gateway_region())::public.crdb_internal_region,
	CONSTRAINT "primary" PRIMARY KEY (id ASC),
	INDEX "web_sessions_expiresAt_idx" ("expiresAt" ASC),
	INDEX "web_sessions_createdAt_idx" ("createdAt" ASC),
	INDEX "web_sessions_revokedAt_idx" ("revokedAt" ASC),
	INDEX "web_sessions_lastUsedAt_idx" ("lastUsedAt" ASC),
	FAMILY "fam_0_id_hashedSecret_username_createdAt_expiresAt_revokedAt_lastUsedAt_auditInfo" (id, "hashedSecret", username, "createdAt", "expiresAt", "revokedAt", "lastUsedAt", "auditInfo", user_id),
	FAMILY fam_10_crdb_region (crdb_region)
) LOCALITY REGIONAL BY ROW`},
		{`CREATE TABLE public.namespace (
	"parentID" INT8 NOT NULL,
	"parentSchemaID" INT8 NOT NULL,
	name STRING NOT NULL,
	id INT8 NULL,
	CONSTRAINT "primary" PRIMARY KEY ("parentID" ASC, "parentSchemaID" ASC, name ASC),
	FAMILY "primary" ("parentID", "parentSchemaID", name),
	FAMILY fam_4_id (id)
) LOCALITY GLOBAL`},
	})
}
