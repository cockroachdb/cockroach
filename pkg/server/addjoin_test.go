// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestConsumeJoinToken(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	sql.FeatureTLSAutoJoinEnabled.Override(ctx, &settings.SV, true)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: settings,
	})
	defer s.Stopper().Stop(context.Background())

	rows, err := db.Query("SELECT crdb_internal.create_join_token();")
	require.NoError(t, err)
	var token string
	for rows.Next() {
		require.NoError(t, rows.Scan(&token))
		require.NotEmpty(t, token)
	}
	var jt security.JoinToken
	require.NoError(t, jt.UnmarshalText([]byte(token)))

	dialOpts := rpc.GetAddJoinDialOptions(nil)
	conn, err := grpc.DialContext(ctx, s.RPCAddr(), dialOpts...)
	require.NoError(t, err)
	defer func() {
		_ = conn.Close() // nolint:grpcconnclose
	}()
	adminClient := serverpb.NewAdminClient(conn)

	req := serverpb.CARequest{}
	resp, err := adminClient.RequestCA(ctx, &req)
	require.NoError(t, err)

	require.True(t, jt.VerifySignature(resp.CaCert))

	// Parse them to an x509.Certificate then add them to a pool.
	pemBlock, _ := pem.Decode(resp.CaCert)
	require.NotNil(t, pemBlock)
	cert, err := x509.ParseCertificate(pemBlock.Bytes)
	require.NoError(t, err)
	certPool := x509.NewCertPool()
	certPool.AddCert(cert)

	dialOpts = rpc.GetAddJoinDialOptions(certPool)
	conn2, err := grpc.DialContext(ctx, s.RPCAddr(), dialOpts...)
	require.NoError(t, err)
	defer func() {
		_ = conn2.Close() // nolint:grpcconnclose
	}()
	adminClient = serverpb.NewAdminClient(conn2)

	cbReq := serverpb.CertBundleRequest{
		TokenID:      jt.TokenID.String(),
		SharedSecret: jt.SharedSecret,
	}
	_, err = adminClient.RequestCertBundle(ctx, &cbReq)
	require.NoError(t, err)
	// Try consuming the token a second time. This should fail.
	_, err = adminClient.RequestCertBundle(ctx, &cbReq)
	require.Error(t, err)
	require.Contains(t, err.Error(), ErrInvalidAddJoinToken.Error())
}
