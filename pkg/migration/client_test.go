package migration_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestHelperIterateRangeDescriptors(t *testing.T) {
	defer leaktest.AfterTest(t)

	cv := clusterversion.ClusterVersion{}
	ctx := context.Background()
	const numNodes = 1

	params, _ := tests.CreateTestServerParams()
	server, _, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	var numRanges int
	if err := server.GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
		numRanges = s.ReplicaCount()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	c := migration.TestingNewCluster(numNodes, kvDB, server.InternalExecutor().(sqlutil.InternalExecutor))
	h := migration.TestingNewHelper(c, cv)

	for _, blockSize := range []int{1, 5, 10, 50} {
		var numDescs int
		init := func() { numDescs = 0 }
		if err := h.IterateRangeDescriptors(ctx, blockSize, init, func(descriptors ...roachpb.RangeDescriptor) error {
			numDescs += len(descriptors)
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		// TODO(irfansharif): We always seem to include a second copy of r1's
		// desc. Unsure why.
		if numDescs != numRanges+1 {
			t.Fatalf("expected to find %d ranges, found %d", numRanges+1, numDescs)
		}

	}
}

func TestHelperWithMigrationTable(t *testing.T) {
	defer leaktest.AfterTest(t)

	// Sort above any real version.
	cv := clusterversion.ClusterVersion{
		Version: roachpb.Version{Major: 420, Minor: 7, Internal: 10},
	}
	ctx := context.Background()
	const numNodes = 1

	params, _ := tests.CreateTestServerParams()
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	c := migration.TestingNewCluster(numNodes, kvDB, server.InternalExecutor().(sqlutil.InternalExecutor))
	h := migration.TestingNewHelper(c, cv)

	dummyDesc := "dummy desc"
	if err := h.TestingInsertMigrationRecord(ctx, "dummy desc"); err != nil {
		t.Fatal(err)
	}
	{
		// Check to see that the initial migration record is what we expect.
		row := sqlDB.QueryRow(`
			SELECT version, status, description
			FROM system.migrations
			ORDER BY version DESC LIMIT 1`,
		)
		var version, status, desc string
		if err := row.Scan(&version, &status, &desc); err != nil {
			t.Fatal(err)
		}

		if version != cv.String() {
			t.Fatalf("expected %s, got %s", cv, version)
		}

		if status != string(migration.StatusRunning) {
			t.Fatalf("expected %s, got %s", migration.StatusSucceeded, status)
		}

		if desc != dummyDesc {
			t.Fatalf("expected %s, got %s", dummyDesc, desc)
		}
	}

	dummyProgress := "dummy progress"
	if err := h.UpdateProgress(ctx, dummyProgress); err != nil {
		t.Fatal(err)
	}

	{
		row := sqlDB.QueryRow(`
			SELECT progress
			FROM system.migrations
			ORDER BY version DESC LIMIT 1`,
		)
		var progress string
		if err := row.Scan(&progress); err != nil {
			t.Fatal(err)
		}

		if progress != dummyProgress {
			t.Fatalf("expected %s, got %s", dummyProgress, progress)
		}
	}

	if err := h.TestingUpdateStatus(ctx, migration.StatusFailed); err != nil {
		t.Fatal(err)
	}

	{
		row := sqlDB.QueryRow(`
			SELECT status, completed
			FROM system.migrations
			ORDER BY version DESC LIMIT 1`,
		)
		var status string
		var completed sql.NullTime
		if err := row.Scan(&status, &completed); err != nil {
			t.Fatal(err)
		}

		if status != string(migration.StatusFailed) {
			t.Fatalf("expected %s, got %s", dummyProgress, migration.StatusFailed)
		}
		if (completed != sql.NullTime{}) {
			t.Fatalf("expected empty completed timestamp, got %v", completed)
		}
	}
	if err := h.TestingUpdateStatus(ctx, migration.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	{
		row := sqlDB.QueryRow(`
			SELECT status, completed
			FROM system.migrations
			ORDER BY version DESC LIMIT 1`,
		)
		var status string
		var completed sql.NullTime
		if err := row.Scan(&status, &completed); err != nil {
			t.Fatal(err)
		}

		if status != string(migration.StatusSucceeded) {
			t.Fatalf("expected %s, got %s", dummyProgress, migration.StatusSucceeded)
		}
		if (completed == sql.NullTime{}) {
			t.Fatalf("expected non-empty completed timestamp")
		}
	}
}
