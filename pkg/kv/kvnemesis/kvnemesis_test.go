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
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestKVNemesisSingleNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	scope := log.Scope(t)
	defer scope.Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	db := tc.Server(0).DB()
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)

	config := GeneratorConfig{
		OpPGetMissing:  1,
		OpPGetExisting: 1,
		OpPPutMissing:  1,
		OpPPutExisting: 1,
		// TODO(dan): This sometimes returns "TransactionStatusError: already
		// committed".
		//
		// TODO(dan): This fails with a WriteTooOld error if the same key is Put
		// twice in a single batch. However, if the same Batch is committed using
		// txn.Run, then it works and only the last one is materialized. We could
		// make the db.Run behavior match txn.Run by ensuring that all requests in a
		// nontransactional batch are disjoint and upgrading to a transactional
		// batch (see CrossRangeTxnWrapperSender) if they are. roachpb.SpanGroup can
		// be used to efficiently check this.
		OpPBatch:                   0,
		OpPClosureTxn:              5,
		OpPClosureTxnCommitInBatch: 5,
		OpPSplitNew:                1,
		OpPSplitAgain:              1,
		OpPMergeNotSplit:           1,
		OpPMergeIsSplit:            1,
	}

	rng, _ := randutil.NewPseudoRand()
	ct := sqlClosedTimestampTargetInterval{tc.ServerConn(0)}
	failures, err := RunNemesis(ctx, rng, db, ct, config)
	require.NoError(t, err, `%+v`, err)

	for _, failure := range failures {
		t.Errorf("failure:\n%+v", failure)
	}
}

type sqlClosedTimestampTargetInterval struct {
	*gosql.DB
}

func (db sqlClosedTimestampTargetInterval) Set(d time.Duration) error {
	q := fmt.Sprintf(`SET CLUSTER SETTING kv.closed_timestamp.target_duration = '%s'`, d)
	_, err := db.DB.Exec(q)
	return err
}

func (db sqlClosedTimestampTargetInterval) ResetToDefault() error {
	_, err := db.DB.Exec(`SET CLUSTER SETTING kv.closed_timestamp.target_duration TO DEFAULT`)
	return err
}
