// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestWriteInitialClusterData captures the initial cluster state written to
// storage. It also tests the output of KV pretty printers, and helps keeping
// them somewhat stable and human-readable. The pretty printers are used in a
// bunch of tooling such as 'cockroach debug range-data', and other tests.
func TestWriteInitialClusterData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	ctx := context.Background()
	cfg := TestStoreConfig(hlc.NewClock(timeutil.NewManualTime(timeutil.Unix(0, 123)),
		time.Millisecond /* maxOffset */, time.Millisecond /* toleratedOffset */, hlc.PanicLogger))

	// Use PreviousRelease so that we don't have to update this test on every
	// version gate addition.
	cv := clusterversion.ClusterVersion{Version: clusterversion.PreviousRelease.Version()}
	require.NoError(t, kvstorage.WriteClusterVersion(ctx, eng, cv))
	require.NoError(t, kvstorage.InitEngine(ctx, eng, roachpb.StoreIdent{NodeID: 1, StoreID: 1}))

	opts := testStoreOpts{createSystemRanges: true, bootstrapVersion: cv.Version}
	kvs, splits := opts.splits()
	require.NoError(t, WriteInitialClusterData(
		ctx, eng, kvs /* initialValues */, cv.Version,
		1 /* numStores */, splits, cfg.Clock.Now().WallTime, cfg.TestingKnobs,
	))
	require.NoError(t, eng.Compact())

	var b strings.Builder
	// TODO(pav-kv): clean up the pretty printers so that the output is fully
	// human readable.
	printKV := func(kv storage.MVCCKeyValue, _ storage.MVCCRangeKeyStack) error {
		b.WriteString(SprintMVCCKeyValue(kv, true /* printKey */))
		b.WriteByte('\n')
		return nil
	}
	require.NoError(t, eng.MVCCIterate(context.Background(), roachpb.KeyMin, keys.LocalMax,
		storage.MVCCKeyAndIntentsIterKind, storage.IterKeyTypePointsOnly,
		fs.UnknownReadCategory, printKV))
	require.NoError(t, eng.MVCCIterate(context.Background(), roachpb.LocalMax, roachpb.KeyMax,
		storage.MVCCKeyAndIntentsIterKind, storage.IterKeyTypePointsOnly,
		fs.UnknownReadCategory, printKV))

	output := strings.ReplaceAll(b.String(), "\n\n", "\n")
	echotest.Require(t, output, filepath.Join("testdata", "TestWriteInitialClusterData"))
}
