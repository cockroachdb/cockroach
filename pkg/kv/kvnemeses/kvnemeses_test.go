// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemeses

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestKVNemesesSingleNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	scope := log.Scope(t)
	defer scope.Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	db := tc.Server(0).DB()
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s'`)

	config := StepperConfig{
		OpPGetMissing:  1,
		OpPGetExisting: 1,
		OpPPutMissing:  1,
		OpPPutExisting: 1,
		// TODO(dan): This sometimes returns "TransactionStatusError: already
		// committed".
		OpPBatch:      0,
		OpPClosureTxn: 10,
		OpPSplit:      1,
		// TODO(dan): Merge seems to occasionally be hanging, presumably because the
		// merge txn is restarting. Investigate.
		//
		// TODO(dan): "merge failed: unexpected value". Nemeses's first bug find?
		//
		OpPMerge: 0,
	}

	rng, _ := randutil.NewPseudoRand()
	steps, kvs, err := RunNemeses(ctx, rng, db, config)
	require.NoError(t, err, `%+v`, err)

	for _, failure := range Validate(steps, kvs) {
		t.Errorf("failure:\n%+v", failure)
	}
	if t.Failed() {
		t.Logf("kvs:\n%s", kvs.DebugPrint())
	}
}
