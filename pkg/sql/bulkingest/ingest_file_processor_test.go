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

	// Write multiple SSTs with disjoint key spans to ensure the processor
	// respect the per-file span boundaries.
	ssts := []execinfrapb.BulkMergeSpec_SST{
		writeSST(t, srv, &tableEncoder, []row{{"a", "1"}, {"b", "2"}}),
		writeSST(t, srv, &tableEncoder, []row{{"m", "10"}, {"n", "11"}}),
	}

	// Provide a span per SST so the ingest processor runs each file through the
	// AddSSTable path independently.
	tableSpan := tableEncoder.tableSpan()
	spans := make([]roachpb.Span, 0, len(ssts))
	spanStart := tableSpan.Key
	for i := 1; i < len(ssts); i++ {
		spans = append(spans, roachpb.Span{Key: spanStart, EndKey: ssts[i].StartKey})
		spanStart = ssts[i].StartKey
	}
	spans = append(spans, roachpb.Span{Key: spanStart, EndKey: tableSpan.EndKey})

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
		spans,
		ssts,
	)
	require.NoError(t, err)

	// Ensure every SST has a corresponding AddSSTable request with matching
	// boundaries.
	require.GreaterOrEqual(t, len(capturedRequests), len(ssts))
	for _, sst := range ssts {
		var matchingReq *kvpb.AddSSTableRequest
		for i := range capturedRequests {
			req := &capturedRequests[i]
			if req.Key.Equal(sst.StartKey) && req.EndKey.Equal(sst.EndKey) {
				matchingReq = req
				break
			}
		}
		require.NotNilf(t, matchingReq, "expected AddSSTable request for span %s-%s", sst.StartKey, sst.EndKey)
	}

	// Verify the data was ingested correctly.
	content := runner.QueryStr(t, "SELECT k, v FROM kv ORDER BY k")
	require.Equal(t, [][]string{
		{"a", "1"}, {"b", "2"}, {"m", "10"}, {"n", "11"},
	}, content)
}
