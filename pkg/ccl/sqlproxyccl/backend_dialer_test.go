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
	"crypto/x509"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	pgproto3 "github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

func TestBackendDialTLSInsecure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	startupMsg := &pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber}

	sql, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer sql.Stopper().Stop(ctx)

	conn, err := BackendDial(startupMsg, sql.ServingSQLAddr(), &tls.Config{})
	require.Error(t, err)
	require.Regexp(t, "target server refused TLS connection", err)
	require.Nil(t, conn)
}

func TestBackendDialTLS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	startupMsg := &pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber}

	tenantCA, err := securitytest.Asset(filepath.Join(certnames.EmbeddedCertsDir, certnames.EmbeddedTenantCACert))
	require.NoError(t, err)

	ca := x509.NewCertPool()
	require.True(t, ca.AppendCertsFromPEM(tenantCA))
	tlsConfig := &tls.Config{
		RootCAs: ca,
	}

	ctx := context.Background()

	storageServer, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Insecure: false,
		// StartServer will sometimes start a tenant. This test requires
		// storage server to be the system tenant, otherwise the
		// tenant10ToStorage test will fail, since the storage server will
		// server tenant 10.
		DisableDefaultTestTenant: true,
	})
	defer storageServer.Stopper().Stop(ctx)

	tenant10 := roachpb.MustMakeTenantID(10)
	sql10, _ := serverutils.StartTenant(t, storageServer, base.TestTenantArgs{TenantID: tenant10})
	defer sql10.Stopper().Stop(ctx)

	tenant11 := roachpb.MustMakeTenantID(11)
	sql11, _ := serverutils.StartTenant(t, storageServer, base.TestTenantArgs{TenantID: tenant11})
	defer sql11.Stopper().Stop(ctx)

	tests := []struct {
		name     string
		addr     string
		tenantID uint64
		errCode  errorCode
	}{{
		name:     "tenant10",
		addr:     sql10.SQLAddr(),
		tenantID: 10,
	}, {
		name:     "tenant11",
		addr:     sql11.SQLAddr(),
		tenantID: 11,
	}, {
		name:     "tenant10To11",
		addr:     sql11.SQLAddr(),
		tenantID: 10,
		errCode:  codeBackendDown,
	}, {
		name:     "tenant11To10",
		addr:     sql10.SQLAddr(),
		tenantID: 11,
		errCode:  codeBackendDown,
	}, {
		name:     "tenant10ToStorage",
		addr:     storageServer.ServingSQLAddr(),
		tenantID: 10,
		errCode:  codeBackendDown,
	}, {
		name:     "tenantWithNodeIDToStoage",
		addr:     storageServer.ServingSQLAddr(),
		tenantID: uint64(storageServer.NodeID()),
		errCode:  codeBackendDown,
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tenantID := roachpb.MustMakeTenantID(tc.tenantID)

			tenantConfig, err := tlsConfigForTenant(tenantID, tc.addr, tlsConfig)
			require.NoError(t, err)

			conn, err := BackendDial(startupMsg, tc.addr, tenantConfig)

			if tc.errCode != codeNone {
				require.Equal(t, tc.errCode, getErrorCode(err))
			} else {
				require.NoError(t, err)
				require.NotNil(t, conn)
			}
		})
	}
}
