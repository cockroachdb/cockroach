// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcprogresspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestChangefeedJobInfoRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	jobID := jobspb.JobID(123456)

	// Create a basic progress record.
	uuid1 := uuid.MakeV4()
	uuid2 := uuid.MakeV4()
	ptsRecords := cdcprogresspb.ProtectedTimestampRecords{
		UserTables: map[descpb.ID]uuid.UUID{
			descpb.ID(100): uuid1,
			descpb.ID(200): uuid2,
		},
	}

	// Write the progress record.
	err := srv.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return writeChangefeedJobInfo(ctx,
			perTableProtectedTimestampsFilename, &ptsRecords, txn, jobID)
	})
	require.NoError(t, err)

	// Read the record back.
	var readPTSRecords cdcprogresspb.ProtectedTimestampRecords
	err = srv.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return readChangefeedJobInfo(ctx,
			perTableProtectedTimestampsFilename, &readPTSRecords, txn, jobID)
	})
	require.NoError(t, err)

	// Verify the read data matches the written data.
	require.Equal(t, ptsRecords, readPTSRecords)
}
