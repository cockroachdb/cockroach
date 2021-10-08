// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"path"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestRenameColumnDuringConcurrentMutation tests the behavior of renaming
// a column that was created in a different, prior transaction but is not
// yet public.
func TestRenameColumnDuringConcurrentMutation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// The structure of the test is to intentionally block a complex
	// column addition schema change at various events and then issue
	// a rename while it is blocked. The events are hooked up via testing
	// knobs.
	type eventType int

	const (
		_ eventType = iota
		publishDeleteAndWriteOnly
		backfill
		resume
	)

	type event struct {
		unblock chan struct{}
	}
	var eventToBlockOn eventType
	eventChan := make(chan event)
	maybeBlockOnEvent := func(evType eventType) {
		if evType != eventToBlockOn {
			return
		}
		ev := event{
			unblock: make(chan struct{}),
		}
		eventChan <- ev
		<-ev.unblock
	}
	ctx := context.Background()
	var tc *testcluster.TestCluster
	tc = testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
					RunBeforePublishWriteAndDelete: func() {
						maybeBlockOnEvent(publishDeleteAndWriteOnly)
					},
					RunBeforeBackfill: func() error {
						maybeBlockOnEvent(backfill)
						return nil
					},
					RunBeforeResume: func(jobID jobspb.JobID) error {
						// Load the job to figure out if it's the rename or the
						// backfill.
						scJob, err := tc.Server(0).JobRegistry().(*jobs.Registry).LoadJob(ctx, jobID)
						if err != nil {
							return err
						}
						pl := scJob.Payload()
						if pl.GetSchemaChange().TableMutationID == descpb.InvalidMutationID {
							return nil
						}
						maybeBlockOnEvent(resume)
						return nil
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)
	for _, testCase := range []struct {
		name   string
		evType eventType
	}{
		{"publishDeleteAndWriteOnly", publishDeleteAndWriteOnly},
		{"backfill", backfill},
		{"resume", resume},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			eventToBlockOn = testCase.evType
			dbName := path.Base(t.Name())
			tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
			tdb.Exec(t, "CREATE DATABASE "+dbName)
			tdb.Exec(t, "CREATE TABLE "+dbName+".foo (i INT PRIMARY KEY)")
			scDone := make(chan error)
			go func() {
				_, err := tc.ServerConn(0).Exec(
					"ALTER TABLE " + dbName + ".foo ADD COLUMN j INT NOT NULL DEFAULT 7 CHECK (j > 0) REFERENCES " + dbName + ".foo(i)")
				scDone <- err
			}()

			ev := <-eventChan
			tdb.Exec(t, "ALTER TABLE "+dbName+".foo RENAME COLUMN j TO k")
			close(ev.unblock)
			require.NoError(t, <-scDone)
			tdb.Exec(t, "INSERT INTO "+dbName+".foo(i, k) VALUES (7, 7)")
		})
	}

}
