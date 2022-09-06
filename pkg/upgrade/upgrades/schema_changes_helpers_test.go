package upgrades

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// Target schema changes in the system.jobs table of the RetryJobsWithExponentialBackoff migration.
const (
	TestingCreateTableQueryBefore = `
CREATE TABLE test.test_table (
	id                INT8      DEFAULT unique_rowid() PRIMARY KEY,
	status            STRING    NOT NULL,
	created           TIMESTAMP NOT NULL DEFAULT now(),
	payload           BYTES     NOT NULL,
	progress          BYTES,
	created_by_type   STRING,
	created_by_id     INT,
	claim_session_id  BYTES,
	claim_instance_id INT8,
	INDEX (status, created),
	INDEX (created_by_type, created_by_id) STORING (status),
	FAMILY fam_0_id_status_created_payload (id, status, created, payload, created_by_type, created_by_id),
	FAMILY progress (progress),
	FAMILY claim (claim_session_id, claim_instance_id)
);
`
	TestingCreateTableAfter = `
CREATE TABLE test.test_table (
	id                INT8      DEFAULT unique_rowid() PRIMARY KEY,
	status            STRING    NOT NULL,
	created           TIMESTAMP NOT NULL DEFAULT now(),
	payload           BYTES     NOT NULL,
	progress          BYTES,
	created_by_type   STRING,
	created_by_id     INT,
	claim_session_id  BYTES,
	claim_instance_id INT8,
	num_runs          INT8,
	last_run          TIMESTAMP,
	INDEX (status, created),
	INDEX (created_by_type, created_by_id) STORING (status),
	INDEX jobs_run_stats_idx (
    claim_session_id,
    status,
    created
  ) STORING(last_run, num_runs, claim_instance_id)
    WHERE ` + systemschema.JobsRunStatsIdxPredicate + `,
	FAMILY fam_0_id_status_created_payload (id, status, created, payload, created_by_type, created_by_id),
	FAMILY progress (progress),
	FAMILY claim (claim_session_id, claim_instance_id, num_runs, last_run)
);
`
	TestingAddColsQuery = `
ALTER TABLE test.test_table
  ADD COLUMN num_runs INT8 FAMILY claim, 
  ADD COLUMN last_run TIMESTAMP FAMILY claim`
	TestingAddIndexQuery = `
CREATE INDEX jobs_run_stats_idx
		ON test.test_table (claim_session_id, status, created)
		STORING (last_run, num_runs, claim_instance_id)
		WHERE ` + systemschema.JobsRunStatsIdxPredicate
)

// MakeFakeTestingMigrationForSchemaChangesTest makes the migration function
// used in the
func MakeFakeTestingMigrationForSchemaChangesTest() (
	m upgrade.TenantUpgradeFunc,
	expectedTableDescriptor *atomic.Value,
) {
	expectedTableDescriptor = &atomic.Value{}
	return func(
		ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
	) error {
		row, err := d.InternalExecutor.QueryRow(ctx, "look-up-id", nil, /* txn */
			`select id from system.namespace where name = $1`, "test_table")
		if err != nil {
			return err
		}
		tableID := descpb.ID(tree.MustBeDInt(row[0]))
		for _, op := range []operation{
			{
				name:           "jobs-add-columns",
				schemaList:     []string{"num_runs", "last_run"},
				query:          TestingAddColsQuery,
				schemaExistsFn: hasColumn,
			},
			{
				name:           "jobs-add-index",
				schemaList:     []string{"jobs_run_stats_idx"},
				query:          TestingAddIndexQuery,
				schemaExistsFn: hasIndex,
			},
		} {
			expected := expectedTableDescriptor.Load().(catalog.TableDescriptor)
			if err := migrateTable(ctx, cs, d, op, tableID, expected); err != nil {
				return err
			}
		}
		return nil
	}, expectedTableDescriptor
}
