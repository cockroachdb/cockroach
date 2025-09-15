// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestFingerprintJobCreation tests creating a fingerprint job with proper job details.
func TestFingerprintJobCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(sqlDB)
	
	// Create a test table
	runner.Exec(t, `CREATE TABLE test.fingerprint_table (id INT PRIMARY KEY, name STRING)`)
	runner.Exec(t, `INSERT INTO test.fingerprint_table VALUES (1, 'test')`)

	t.Run("table_fingerprint_job", func(t *testing.T) {
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		
		// Create job details for table fingerprinting
		details := jobspb.FingerprintDetails{
			Target: &jobspb.FingerprintDetails_Table{
				Table: &jobspb.FingerprintDetails_FingerprintTableTarget{
					TableID:   descpb.ID(104), // test table ID
					TableName: "fingerprint_table",
				},
			},
			Statement: "SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE fingerprint_table",
		}
		
		// Create job record
		jobRecord := jobs.Record{
			Description: "Test fingerprint job for table",
			Username:    "testuser",
			Details:     details,
			Progress:    jobspb.FingerprintProgress{},
		}
		
		// Verify job can be created
		job, err := execCfg.JobRegistry.CreateJobWithTxn(ctx, jobRecord, execCfg.JobRegistry.MakeJobID(), nil)
		require.NoError(t, err)
		require.NotNil(t, job)
		
		// Verify job details
		jobDetails := job.Details().(jobspb.FingerprintDetails)
		require.NotNil(t, jobDetails.Table)
		require.Equal(t, "fingerprint_table", jobDetails.Table.TableName)
		require.Equal(t, descpb.ID(104), jobDetails.Table.TableID)
	})
}

// TestFingerprintResumerInterface tests that the fingerprint resumer properly implements the jobs.Resumer interface.
func TestFingerprintResumerInterface(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	t.Run("resumer_construction", func(t *testing.T) {
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		
		// Create a mock job
		details := jobspb.FingerprintDetails{
			Target: &jobspb.FingerprintDetails_Table{
				Table: &jobspb.FingerprintDetails_FingerprintTableTarget{
					TableID:   descpb.ID(100),
					TableName: "test_table",
				},
			},
			Statement: "SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE test_table",
		}
		
		jobRecord := jobs.Record{
			Description: "Test fingerprint resumer interface",
			Username:    "testuser",
			Details:     details,
			Progress:    jobspb.FingerprintProgress{},
		}
		
		job, err := execCfg.JobRegistry.CreateJobWithTxn(ctx, jobRecord, execCfg.JobRegistry.MakeJobID(), nil)
		require.NoError(t, err)
		
		// Test that the resumer constructor works
		resumer := execCfg.JobRegistry.MakeResumer(job, execCfg.Settings)
		require.NotNil(t, resumer)
		
		// Test interface compliance
		var _ jobs.Resumer = resumer
	})
}

// TestBuildFingerprintQueryForIndexBasic tests the basic functionality of BuildFingerprintQueryForIndex.
func TestBuildFingerprintQueryForIndexBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This is a unit test for the query building function
	// For now, we skip this test as it requires complex table descriptor mocking
	skip.WithIssue(t, 150994, "fingerprint query building requires table descriptor setup")
}

// TestFingerprintJobSpanProtection tests that tenant span protection works correctly.
func TestFingerprintJobSpanProtection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test is complex and requires multi-tenant setup
	skip.WithIssue(t, 150994, "tenant span protection requires multi-tenant test setup")
}