// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestRaftFlatBuffers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	var args base.TestClusterArgs
	args.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{ExperimentalUseRaftVersionFlatBuffer: true}
	tc := testcluster.StartTestCluster(t, 3, args)
	defer tc.Stopper().Stop(ctx)

	r := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	r.ExecMultiple(t,
		`CREATE DATABASE foo`,
		`CREATE TABLE foo.bar(k INT PRIMARY KEY, v string)`,
		`INSERT INTO foo.bar VALUES(1, 'flatbuffer!')`,
	)
	require.Equal(t, [][]string{
		{"1", "flatbuffer!"},
	}, r.QueryStr(t, `SELECT * FROM foo.bar`))
}
