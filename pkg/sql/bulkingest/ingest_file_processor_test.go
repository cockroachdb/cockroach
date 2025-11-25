// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkingest

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestIngestFileProcessorSpanBoundaries verifies that the ingest file processor
// passes the correct span boundaries to AddSSTable.
func TestIngestFileProcessorSpanBoundaries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dirname, cleanup := testutils.TempDir(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Track all AddSSTable requests to verify correct span boundaries.
	var capturedRequests []kvpb.AddSSTableRequest
	requestInterceptor := func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
		for _, req := range ba.Requests {
			if addReq := req.GetAddSstable(); addReq != nil {
				capturedRequests = append(capturedRequests, *addReq)
			}
		}
		return nil
	}

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dirname,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: requestInterceptor,
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	execCfg := srv.ExecutorConfig().(sql.ExecutorConfig)
	runner := sqlutils.MakeSQLRunner(sqlDB)
	runner.Exec(t, "CREATE TABLE kv (k STRING PRIMARY KEY, v STRING)")
	tableEncoder := getEncoder(execCfg, "kv")

	// Write an SST with multiple keys. The SST's EndKey will be set to an
	// exclusive boundary (PrefixEnd of the last row).
	sst := writeSST(t, srv, &tableEncoder, []row{
		{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"},
	})

	// Clear any requests from table creation.
	capturedRequests = nil

	// Ingest the SST.
	jobExecCtx, cleanupJobCtx := sql.MakeJobExecContext(
		ctx, "test", username.RootUserName(), &sql.MemoryMetrics{}, &execCfg,
	)
	defer cleanupJobCtx()

	err := IngestFiles(
		ctx,
		jobExecCtx,
		[]roachpb.Span{tableEncoder.tableSpan()},
		[]execinfrapb.BulkMergeSpec_SST{sst},
	)
	require.NoError(t, err)

	// Find the AddSSTable request that matches our SST's EndKey.
	var matchingReq *kvpb.AddSSTableRequest
	for i := range capturedRequests {
		if capturedRequests[i].EndKey.Equal(sst.EndKey) {
			matchingReq = &capturedRequests[i]
			break
		}
	}
	require.NotNil(t, matchingReq, "expected to find an AddSSTable request")
	require.Equal(t, sst.EndKey, matchingReq.EndKey)

	// Verify the data was ingested correctly.
	content := runner.QueryStr(t, "SELECT k, v FROM kv ORDER BY k")
	require.Equal(t, [][]string{
		{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"},
	}, content)
}
