// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"context"
	"crypto/tls"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	pgproto3 "github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

func TestBackendDialTLS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	startupMsg := &pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber}
	tlsConfig := &tls.Config{InsecureSkipVerify: true}

	t.Run("insecure server", func(t *testing.T) {
		sql, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
		defer sql.Stopper().Stop(ctx)

		conn, err := BackendDial(startupMsg, sql.ServingSQLAddr(), tlsConfig)
		require.Error(t, err)
		require.Regexp(t, "target server refused TLS connection", err)
		require.Nil(t, conn)
	})

	t.Run("secure server", func(t *testing.T) {
		sql, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: false})
		defer sql.Stopper().Stop(ctx)

		conn, err := BackendDial(startupMsg, sql.ServingSQLAddr(), tlsConfig)

		require.NoError(t, err)
		require.NotNil(t, conn)
	})
}
