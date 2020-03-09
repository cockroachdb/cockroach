// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestConcurrentProcessorsReadEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	params := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLEvalContext: &tree.EvalContextTestingKnobs{
				CallbackGenerators: map[string]*tree.CallbackValueGenerator{
					"my_callback": tree.NewCallbackValueGenerator(
						func(ctx context.Context, prev int, _ *kv.Txn) (int, error) {
							if prev < 10 {
								return prev + 1, nil
							}
							return -1, nil
						}),
				},
			},
		},
	}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	rows, err := db.Query(` select * from crdb_internal.testing_callback('my_callback')`)
	require.NoError(t, err)
	exp := 1
	for rows.Next() {
		var got int
		require.NoError(t, rows.Scan(&got))
		require.Equal(t, exp, got)
		exp++
	}
}
