// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdcevent

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
)

func TestRowFetcherCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())

	s := srv.ApplicationLayer()

	serverCfg := s.DistSQLServer().(*distsql.ServerImpl).ServerConfig
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`)
	sqlDB.Exec(t, `CREATE TYPE priority AS ENUM ('high', 'low')`)
	sqlDB.Exec(t, `
		CREATE TABLE foo (
			a INT PRIMARY KEY, 
			b status, 
			c priority,
			FAMILY only_b (b),
			FAMILY only_c (c)
   )`)

	ctx := context.Background()

	tableDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")
	refreshDesc := func() {
		prevVersion := tableDesc.GetVersion()
		tableDesc = cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")
		// Sanity check that the tableDesc version does not change (which does not happen
		// after altering UDTs).
		assert.Equal(t, prevVersion, tableDesc.GetVersion())
	}

	targetType := jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY
	family := "only_c"
	targets := changefeedbase.Targets{}
	targets.Add(changefeedbase.Target{
		Type:       targetType,
		TableID:    tableDesc.GetID(),
		FamilyName: family,
	})
	cFamilyID := descpb.FamilyID(1)

	rfCache, err := newRowFetcherCache(ctx, serverCfg.Codec,
		serverCfg.LeaseManager.(*lease.Manager),
		serverCfg.CollectionFactory,
		serverCfg.DB.KV(),
		serverCfg.Settings,
		targets)
	if err != nil {
		t.Fatal(err)
	}

	// Ensure a cache hit using the same table descriptor, family, and targets.
	rf, _, err := rfCache.RowFetcherForColumnFamily(tableDesc, cFamilyID, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	rf2, _, err := rfCache.RowFetcherForColumnFamily(tableDesc, cFamilyID, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, rf == rf2)

	// Changing a type in another column family does not cause a cache eviction.
	sqlDB.Exec(t, `ALTER TYPE status ADD VALUE 'pending'`)
	refreshDesc()
	rf3, _, err := rfCache.RowFetcherForColumnFamily(tableDesc, cFamilyID, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, rf == rf3)

	// Changing a type in the same column family does cause a cache eviction.
	sqlDB.Exec(t, `ALTER TYPE priority ADD VALUE 'medium'`)
	refreshDesc()
	rf4, _, err := rfCache.RowFetcherForColumnFamily(tableDesc, cFamilyID, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, rf != rf4)
}
