// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemesis

import (
	"context"
	gosql "database/sql"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestApplier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	db := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)
	env := &Env{SQLDBs: []*gosql.DB{sqlDB}}

	type testCase struct {
		name string
		a    string // actual output
	}

	a := MakeApplier(env, db, db)

	var tests []testCase
	addPass := func(t *testing.T, name string, s Step) {
		t.Helper()
		_ /* trace */, err := a.Apply(ctx, &s)
		require.NoError(t, err)
		actual := s.String()
		// Trim out the txn to avoid nondeterminism.
		actual = regexp.MustCompile(` txnpb:\(.*\)`).ReplaceAllLiteralString(actual, ` txnpb:<txn>`)
		// Replace timestamps.
		actual = regexp.MustCompile(`[0-9]+\.[0-9]+,[0-9]+`).ReplaceAllLiteralString(actual, `<ts>`)
		tests = append(tests, testCase{name: name, a: actual})
	}
	addErr := func(t *testing.T, name string, s Step) {
		t.Helper()
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()
		_ /* trace */, err := a.Apply(cancelledCtx, &s)
		require.NoError(t, err)
		actual := s.String()
		// Trim out context canceled location, which can be non-deterministic.
		// The wrapped string around the context canceled error depends on where
		// the context cancellation was noticed.
		actual = regexp.MustCompile(` aborted .*: context canceled`).ReplaceAllString(actual, ` context canceled`)
		tests = append(tests, testCase{name: name, a: actual})
	}

	// Basic operations
	addPass(t, "get", step(get(`a`)))
	addPass(t, "scan", step(scan(`a`, `c`)))

	addPass(t, "put", step(put(`a`, 1)))
	addPass(t, "get-for-update", step(getForUpdate(`a`)))
	addPass(t, "scan-for-update", step(scanForUpdate(`a`, `c`)))

	addPass(t, `batch`, step(batch(put(`a`, 21), delRange(`b`, `c`, 22))))

	addPass(t, "rscan", step(reverseScan(`a`, `c`)))
	addPass(t, "rscan-for-update", step(reverseScanForUpdate(`a`, `b`)))

	addPass(t, "del", step(del(`b`, 1)))
	addPass(t, "delrange", step(delRange(`a`, `c`, 6)))

	addPass(t, "txn-delrange", step(closureTxn(ClosureTxnType_Commit, delRange(`b`, `d`, 1))))

	addErr(t, "get-err", step(get(`a`)))
	addErr(t, "put-err", step(put(`a`, 1)))

	addErr(t, "scan-for-update-err", step(scanForUpdate(`a`, `c`)))
	addErr(t, "rscan-err", step(reverseScan(`a`, `c`)))
	addErr(t, "rscan-for-update-err", step(reverseScanForUpdate(`a`, `c`)))
	addErr(t, "del-err", step(del(`b`, 1)))
	addErr(t, "delrange-err", step(delRange(`b`, `c`, 12)))

	addErr(t, `txn-err`, step(closureTxn(ClosureTxnType_Commit, delRange(`b`, `d`, 1))))

	// Batch
	addPass(t, `batch-mixed`, step(batch(put(`b`, 2), get(`a`), del(`b`, 1), del(`c`, 1), scan(`a`, `c`), reverseScanForUpdate(`a`, `e`))))
	addErr(t, `batch-mixed-err`, step(batch(put(`b`, 2), getForUpdate(`a`), scanForUpdate(`a`, `c`), reverseScan(`a`, `c`))))

	// Txn commit
	addPass(t, `txn-commit-mixed`, step(closureTxn(ClosureTxnType_Commit, put(`e`, 5), batch(put(`f`, 6), delRange(`c`, `e`, 1)))))
	// Txn commit in batch
	addPass(t, `txn-commit-batch`, step(closureTxnCommitInBatch(opSlice(get(`a`), put(`f`, 6)), put(`e`, 5))))

	// Txn rollback
	addPass(t, `txn-rollback`, step(closureTxn(ClosureTxnType_Rollback, put(`e`, 5))))

	// Txn error
	addErr(t, `txn-error`, step(closureTxn(ClosureTxnType_Rollback, put(`e`, 5))))

	// Splits and merges
	addPass(t, `split`, step(split(`foo`)))
	addPass(t, `merge`, step(merge(`foo`)))
	addErr(t, `split-again`, step(split(`foo`)))
	addErr(t, `merge-again`, step(merge(`foo`)))

	// Lease transfers
	addPass(t, `transfer`, step(transferLease(`foo`, 1)))
	addErr(t, `transfer-again`, step(transferLease(`foo`, 1)))

	// Zone config changes
	addPass(t, `zcfg`, step(changeZone(ChangeZoneType_ToggleGlobalReads)))
	addErr(t, `zcfg-again`, step(changeZone(ChangeZoneType_ToggleGlobalReads)))

	w := echotest.Walk(t, testutils.TestDataPath(t, t.Name()))
	defer w.Check(t)
	for _, test := range tests {
		t.Run(test.name, w.Do(t, test.name, func(t *testing.T, path string) {
			echotest.Require(t, strings.TrimLeft(test.a, "\n"), path)
			t.Log(test.a)
		}))
	}
}

func TestUpdateZoneConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		before   zonepb.ZoneConfig
		change   ChangeZoneType
		expAfter zonepb.ZoneConfig
	}{
		{
			before:   zonepb.ZoneConfig{NumReplicas: proto.Int32(3)},
			change:   ChangeZoneType_ToggleGlobalReads,
			expAfter: zonepb.ZoneConfig{NumReplicas: proto.Int32(3), GlobalReads: proto.Bool(true)},
		},
		{
			before:   zonepb.ZoneConfig{NumReplicas: proto.Int32(3), GlobalReads: proto.Bool(false)},
			change:   ChangeZoneType_ToggleGlobalReads,
			expAfter: zonepb.ZoneConfig{NumReplicas: proto.Int32(3), GlobalReads: proto.Bool(true)},
		},
		{
			before:   zonepb.ZoneConfig{NumReplicas: proto.Int32(3), GlobalReads: proto.Bool(true)},
			change:   ChangeZoneType_ToggleGlobalReads,
			expAfter: zonepb.ZoneConfig{NumReplicas: proto.Int32(3), GlobalReads: proto.Bool(false)},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			zone := test.before
			updateZoneConfig(&zone, test.change)
			require.Equal(t, test.expAfter, zone)
		})
	}
}
