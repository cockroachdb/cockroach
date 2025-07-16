// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulk_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestBufferingAdderMemoryExhaustion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// This is a regression test for a bug that leaked the sstwriter within the
	// sstbatcher if allocation failed due to memory exhaustion.

	// Start a test server.
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	distSQLSrv := s.DistSQLServer().(*distsql.ServerImpl)
	bulkAdderFactory := distSQLSrv.ServerConfig.BulkAdder

	// Attempt to create a BufferingAdder while asking for 1 Gib more memory than is available.
	opts := kvserverbase.BulkAdderOptions{
		Name:          "test-adder",
		MinBufferSize: distSQLSrv.ServerConfig.ParentMemoryMonitor.AllocBytes() + (1024 * 1024 * 1024),
	}

	// This should fail due to memory exhaustion, but importantly should not leak goroutines.
	_, err := bulkAdderFactory(ctx, s.DB(), hlc.Timestamp{}, opts)
	require.ErrorContains(t, err, "not enough memory available")
}
