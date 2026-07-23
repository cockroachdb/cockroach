// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecann

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/stretchr/testify/require"
)

func TestVecann(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: `test`})
	defer srv.Stopper().Stop(ctx)

	vw := vectorMeta.New().(*vectorWorkload)
	vw.cacheFolder = "testdata"
	vw.searchPercent = 100
	vw.datasetName = "random-xs-20-euclidean"
	vw.connFlags.Concurrency = 2

	require.NoError(t, vw.Hooks().Validate())
	require.NoError(t, vw.Hooks().PreCreate(db))

	ql, err := vw.Ops(ctx, []string{}, &histogram.Registry{})
	require.NoError(t, err)
	require.Len(t, ql.WorkerFns, 2)
}
