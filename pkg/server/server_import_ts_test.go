// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestServerWithTimeseriesImport validates the functionality gated behind the
// COCKROACH_DEBUG_TS_IMPORT_FILE functionality and the associated testing knob
// by starting a server, dumping the time series, importing it into a new server
// (with time series disabled) and ensuring that there is now at least the same
// amount of data in there.
func TestServerWithTimeseriesImport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	path := filepath.Join(t.TempDir(), "dump.raw")

	var bytesDumped int64
	func() {
		srv := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
		defer srv.Stopper().Stop(ctx)

		cc, err := srv.Servers[0].RPCContext().GRPCUnvalidatedDial(srv.Servers[0].RPCAddr()).Connect(ctx)
		require.NoError(t, err)
		bytesDumped = dumpTSNonempty(t, cc, path)
		t.Logf("dumped %s bytes", humanizeutil.IBytes(bytesDumped))
	}()

	// Start a new server that will not write time series, and instruct it to
	// ingest the dump we just wrote.
	args := base.TestClusterArgs{}
	args.ServerArgs.Settings = cluster.MakeTestingClusterSettings()
	ts.TimeseriesStorageEnabled.Override(ctx, &args.ServerArgs.Settings.SV, false)
	args.ServerArgs.Knobs.Server = &server.TestingKnobs{
		ImportTimeseriesFile: path,
	}
	srv := testcluster.StartTestCluster(t, 1, args)
	defer srv.Stopper().Stop(ctx)
	cc, err := srv.Servers[0].RPCContext().GRPCUnvalidatedDial(srv.Servers[0].RPCAddr()).Connect(ctx)
	require.NoError(t, err)
	// This would fail if we didn't supply a dump. Just the fact that it returns
	// successfully proves that we ingested at least some time series (or that we
	// failed to disable time series).
	bytesDumpedAgain := dumpTSNonempty(t, cc, filepath.Join(t.TempDir(), "dump2.raw"))
	// We get the same number of bytes back, which serves as proximate proof
	// that we ingested the dump properly.
	require.Equal(t, bytesDumped, bytesDumpedAgain)
}

func dumpTSNonempty(t *testing.T, cc *grpc.ClientConn, dest string) (bytes int64) {
	c, err := tspb.NewTimeSeriesClient(cc).DumpRaw(context.Background(), &tspb.DumpRequest{})
	require.NoError(t, err)

	f, err := os.Create(dest)
	require.NoError(t, err)
	require.NoError(t, ts.DumpRawTo(c, f))
	require.NoError(t, f.Close())
	info, err := os.Stat(dest)
	require.NoError(t, err)
	require.NotZero(t, info.Size())
	return info.Size()
}
