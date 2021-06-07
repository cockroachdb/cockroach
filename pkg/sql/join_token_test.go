// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCreateJoinToken(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	settings := cluster.MakeTestingClusterSettings()
	FeatureTLSAutoJoinEnabled.Override(ctx, &settings.SV, true)
	s, sqldb, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: settings,
	})
	defer s.Stopper().Stop(context.Background())

	rows, err := sqldb.Query("SELECT crdb_internal.create_join_token();")
	require.NoError(t, err)
	count := 0
	var token string
	for rows.Next() {
		count++
		require.NoError(t, rows.Scan(&token))
		require.NotEmpty(t, token)
	}
	var jt security.JoinToken
	require.NoError(t, jt.UnmarshalText([]byte(token)))
	require.Equal(t, 1, count)
	require.NoError(t, rows.Close())

	rows, err = sqldb.Query("SELECT id FROM system.join_tokens;")
	require.NoError(t, err)
	count = 0
	for rows.Next() {
		count++
		var tokenID string
		require.NoError(t, rows.Scan(&tokenID))
		require.NotEmpty(t, jt)
		require.Equal(t, jt.TokenID.String(), tokenID)
	}
	require.Equal(t, 1, count)
	require.NoError(t, rows.Close())
}
