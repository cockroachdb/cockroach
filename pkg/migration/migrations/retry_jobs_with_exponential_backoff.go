// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
)

const (
	numRunsQuery  = `ALTER TABLE system.jobs ADD COLUMN num_runs INT8 FAMILY claim`
	lastRunQuery  = `ALTER TABLE system.jobs ADD COLUMN last_run TIMESTAMP FAMILY claim;`
	addIndexQuery = `CREATE INDEX jobs_run_stats_idx
		ON system.jobs (claim_session_id, status, created)
		STORING (last_run, num_runs, claim_instance_id)
		WHERE
			status
			IN (
					'running',
					'reverting',
					'pending',
					'pause-requested',
					'cancel-requested'
				)`
)

// retryJobsWithExponentialBackoff changes the schema of system.jobs table in
// two steps. It first adds two new columns and then an index.
func retryJobsWithExponentialBackoff(
	ctx context.Context, cs clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	ops := [...]struct {
		// Schema name.
		name string
		// Schema change query.
		query string
		// Function to check existing schema.
		schemaExists func(catalog.TableDescriptor, string) (bool, error)
	}{
		{"num_runs", numRunsQuery, hasColumn},
		{"last_run", lastRunQuery, hasColumn},
		{"jobs_run_stats_idx", addIndexQuery, hasIndex},
	}
	for _, op := range ops {
		if err := runJobsTableMigration(ctx, cs, d, "jobs-migration-"+op.name, op.query,
			func(table catalog.TableDescriptor) (bool, error) {
				return op.schemaExists(table, op.name)
			}); err != nil {
			return err
		}
	}
	return nil
}

// runJobsTableMigration changes the jobs table schema based on the given query.
// The change is ignored if the table already has the required changes, which are
// explicitly limited to two new columns (last_run and num_runs) and the index
// jobs_run_stats_idx.
func runJobsTableMigration(
	ctx context.Context,
	_ clusterversion.ClusterVersion,
	d migration.TenantDeps,
	opTag string,
	schemaChangeQuery string,
	schemaExists func(descriptor catalog.TableDescriptor) (bool, error),
) error {
	for {
		// Fetch the jobs table
		// Check if mutation job exists for system.jobs
		// if exists
		// 		wait for job to finish, continue the loop
		// otherwise
		// check if migration is complete (check new cols and ix), return
		// run schema change
		// on success continue the loop

		// Retrieve the jobs table.
		var jt catalog.TableDescriptor
		if err := descs.Txn(ctx, d.Settings, d.LeaseManager, d.InternalExecutor, d.DB, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) (err error) {
			jt, err = descriptors.GetImmutableTableByID(ctx, txn, keys.JobsTableID, tree.ObjectLookupFlags{
				CommonLookupFlags: tree.CommonLookupFlags{
					AvoidCached: true,
					Required:    true,
				},
			})
			return err
		}); err != nil {
			return err
		}

		// Wait for any in-flight schema changes to complete.
		if mutations := jt.GetMutationJobs(); len(mutations) > 0 {
			for _, mutation := range mutations {
				log.Infof(ctx, "[SR] waiting for a mutation job %v to complete before "+
					"modify the jobs table", mutation.JobID)
				if _, err := d.InternalExecutor.Exec(ctx, "migration-mutations-wait",
					nil, "SHOW JOB WHEN COMPLETE $1", mutation.JobID); err != nil {
					return err
				}
			}
			continue
		}

		// Ignore the schema change if the jobs table already has the required schema.
		if ok, err := schemaExists(jt); err != nil {
			return errors.Wrapf(err, "[SR] Error while validating descriptors during"+
				" operation %s", opTag)
		} else if ok {
			log.Info(ctx, fmt.Sprintf("[SR] skipping %s operation as the schema change already exists.", opTag))
			return nil
		}

		// Modify the jobs table.
		log.Info(ctx, fmt.Sprintf("[SR] Performing operation: %s", opTag))
		if _, err := d.InternalExecutor.ExecEx(
			ctx,
			"migration-alter-jobs-table",
			nil, /* txn */
			sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
			schemaChangeQuery); err != nil {
			return err
		}
		return nil
	}
}

// hasColumn return true if the table has the given column. It returns
// an error if the column exists but doesn't match with the table's
// descriptor defined in systemschema/system.go. The comparison is not strict
// as several descriptor fields are ignored.
func hasColumn(jobsTable catalog.TableDescriptor, colName string) (bool, error) {
	storedCol, err := jobsTable.FindColumnWithName(tree.Name(colName))
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return false, nil
		}
		return false, err
	}

	expectedCol, err := systemschema.JobsTable.FindColumnWithName(tree.Name(colName))
	if err != nil {
		return false, errors.Wrapf(err, "[SR] columns name %s is invalid.", colName)
	}

	expectedCopy := expectedCol.ColumnDescDeepCopy()
	storedCopy := storedCol.ColumnDescDeepCopy()

	storedCopy.ID = 0
	expectedCopy.ID = 0

	if err = mustBeEqual(&expectedCopy, &storedCopy); err != nil {
		return false, err
	}
	return true, nil
}

// hasIndex return true if the table has the given index. It returns
// an error if the index exists but doesn't match with the table's
// descriptor defined in systemschema/system.go. The comparison is not strict
// as several descriptor fields are ignored.
func hasIndex(jobsTable catalog.TableDescriptor, indexName string) (bool, error) {
	storedIdx, err := jobsTable.FindIndexWithName(indexName)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return false, nil
		}
		return false, err
	}
	expectedIdx, err := systemschema.JobsTable.FindIndexWithName(indexName)
	if err != nil {
		return false, errors.Wrapf(err, "[SR] index name %s is invalid", indexName)
	}
	storedCopy := storedIdx.IndexDescDeepCopy()
	expectedCopy := expectedIdx.IndexDescDeepCopy()
	// Ignore the fields that don't matter in the comparison.
	// TODO(sajjad): FIXME: Please check am I ingoring the right fields.
	storedCopy.ID = 0
	expectedCopy.ID = 0
	storedCopy.Version = 0
	expectedCopy.Version = 0
	storedCopy.CreatedExplicitly = false
	expectedCopy.CreatedExplicitly = false
	expectedCopy.StoreColumnIDs = []descpb.ColumnID{0, 0, 0}
	storedCopy.StoreColumnIDs = []descpb.ColumnID{0, 0, 0}
	expectedCopy.Predicate = neutralizeSQL(expectedCopy.Predicate)

	if err = mustBeEqual(&expectedCopy, &storedCopy); err != nil {
		return false, err
	}
	return true, nil
}

func mustBeEqual(expected, found protoutil.Message) error {
	expectedBytes, err := protoutil.Marshal(expected)
	if err != nil {
		return err
	}
	foundBytes, err := protoutil.Marshal(found)
	if err != nil {
		return err
	}
	if bytes.Equal(expectedBytes, foundBytes) {
		return nil
	}
	return errors.Errorf("[SR] expected descriptor doesn't match "+
		"with found descriptor: %s", strings.Join(pretty.Diff(expected, found), "\n"))
}

func neutralizeSQL(stmt string) string {
	stmt = strings.Replace(stmt, "system.jobs", "system.public.jobs", -1)
	stmt = strings.TrimSpace(regexp.MustCompile(`(\s+|;+)`).ReplaceAllString(stmt, " "))
	stmt = strings.ReplaceAll(stmt, "( ", "(")
	stmt = strings.ReplaceAll(stmt, " )", ")")
	return stmt
}
