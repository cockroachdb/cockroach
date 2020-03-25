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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestKVNemesisSingleNode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	db := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)
	sqlutils.MakeSQLRunner(sqlDB).Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)

	config := NewDefaultConfig()
	config.NumNodes, config.NumReplicas = 1, 1
	rng, _ := randutil.NewPseudoRand()
	ct := sqlClosedTimestampTargetInterval{sqlDBs: []*gosql.DB{sqlDB}}
	failures, err := RunNemesis(ctx, rng, ct, config, db)
	require.NoError(t, err, `%+v`, err)

	for _, failure := range failures {
		t.Errorf("failure:\n%+v", failure)
	}
}

func TestKVNemesisMultiNode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// 4 nodes so we have somewhere to move 3x replicated ranges to.
	const numNodes = 4
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	dbs, sqlDBs := make([]*kv.DB, numNodes), make([]*gosql.DB, numNodes)
	for i := 0; i < numNodes; i++ {
		dbs[i] = tc.Server(i).DB()
		sqlDBs[i] = tc.ServerConn(i)
	}
	sqlutils.MakeSQLRunner(sqlDBs[0]).Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)

	config := NewDefaultConfig()
	config.NumNodes, config.NumReplicas = numNodes, 3
	// kvnemesis found a rare bug with closed timestamps when merges happen
	// on a multinode cluster. Disable the combo for now to keep the test
	// from flaking. See #44878.
	config.Ops.Merge = MergeConfig{}
	rng, _ := randutil.NewPseudoRand()
	ct := sqlClosedTimestampTargetInterval{sqlDBs: sqlDBs}
	failures, err := RunNemesis(ctx, rng, ct, config, dbs...)
	require.NoError(t, err, `%+v`, err)

	for _, failure := range failures {
		t.Errorf("failure:\n%+v", failure)
	}
}

type sqlClosedTimestampTargetInterval struct {
	sqlDBs []*gosql.DB
}

func (x sqlClosedTimestampTargetInterval) Set(ctx context.Context, d time.Duration) error {
	var err error
	for i, sqlDB := range x.sqlDBs {
		q := fmt.Sprintf(`SET CLUSTER SETTING kv.closed_timestamp.target_duration = '%s'`, d)
		if _, err = sqlDB.Exec(q); err == nil {
			return nil
		}
		log.Infof(ctx, "node %d could not set target duration: %+v", i, err)
	}
	log.Infof(ctx, "all nodes could not set target duration: %+v", err)
	return err
}

func (x sqlClosedTimestampTargetInterval) ResetToDefault(ctx context.Context) error {
	var err error
	for i, sqlDB := range x.sqlDBs {
		q := fmt.Sprintf(`SET CLUSTER SETTING kv.closed_timestamp.target_duration TO DEFAULT`)
		if _, err = sqlDB.Exec(q); err == nil {
			return nil
		}
		log.Infof(ctx, "node %d could not reset target duration: %+v", i, err)
	}
	log.Infof(ctx, "all nodes could not reset target duration: %+v", err)
	return err
}
