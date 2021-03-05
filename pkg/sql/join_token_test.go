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
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type fakeStatusServer struct {
	token string
	err   error
}

func (f *fakeStatusServer) Nodes(
	ctx context.Context, request *serverpb.NodesRequest,
) (*serverpb.NodesResponse, error) {
	panic("unimplemented")
}

func (f *fakeStatusServer) GenerateJoinToken(ctx context.Context) (string, error) {
	return f.token, f.err
}

var _ serverpb.NodesStatusServer = &fakeStatusServer{}

func TestCreateJoinTokenNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	f := &fakeStatusServer{}
	c := &createJoinTokenNode{
		status: f,
	}

	errInjected := errors.New("injected error")
	f.err = errInjected
	err := c.startExec(runParams{})
	require.EqualError(t, err, errInjected.Error())

	f.err = nil
	f.token = "testtoken"
	*c = createJoinTokenNode{
		status: f,
	}
	require.NoError(t, c.startExec(runParams{}))
	ok, err := c.Next(runParams{})
	require.True(t, ok)
	require.NoError(t, err)
	val := c.Values()
	require.NotEmpty(t, val)
	require.Equal(t, tree.DString("testtoken"), *(val[0].(*tree.DString)))
	ok, err = c.Next(runParams{})
	require.False(t, ok)
	require.NoError(t, err)
}

func TestCreateJoinTokenStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	settings := cluster.MakeTestingClusterSettings()
	FeatureTLSAutoJoinEnabled.Override(&settings.SV, true)
	s, sqldb, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings:    settings,
	})
	defer s.Stopper().Stop(context.Background())

	rows, err := sqldb.Query("CREATE JOINTOKEN;")
	require.NoError(t, err)
	count := 0
	for rows.Next() {
		count++
		var jt string
		require.NoError(t, rows.Scan(&jt))
		require.NotEmpty(t, jt)
	}
	require.Equal(t, 1, count)
	require.NoError(t, rows.Close())
}
