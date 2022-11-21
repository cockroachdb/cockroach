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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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
	ctx := context.Background()

	// This test wants to assert that the SQL server cert used for the
	// storage cluster does not contain tenantIDs; and the SQL server
	// cert used for the tenant SQL servers do. It also wants that the
	// CA be different for both.
	//
	// Howerver, the default (in-memory) embedded certs contain a
	// sql-server.crt which always contains tenant IDs and is
	// signed by the same CA as the storage cluster.
	//
	// So this test uses a custom cert directory without the
	// embedded sql-server.crt, and regenerates the latter
	// manually when starting secondary SQL servers.

	// We use the filesystem in this test.
	defer func(al securityassets.Loader) {
		securityassets.SetLoader(al)
	}(securityassets.GetLoader())
	securityassets.ResetLoader()

	testutils.RunTrueAndFalse(t, "separate SQL server cert", func(t *testing.T, separateSQLServerCert bool) {
		tmpDir, err := os.MkdirTemp("", "certs")
		require.NoError(t, err)

		var certsDir string
		asset := func(f string) []byte {
			a, err := securitytest.Asset(filepath.Join(certnames.EmbeddedCertsDir, f))
			require.NoError(t, err)
			return a
		}
		copy := func(f string) {
			src := asset(f)
			dstname := filepath.Join(certsDir, f)
			err := os.WriteFile(dstname, src, 0600)
			require.NoError(t, err)
		}

		// We start defining the certs for the storage cluster.
		certsDir = filepath.Join(tmpDir, "storage")
		require.NoError(t, os.MkdirAll(certsDir, 0700))
		t.Logf("storage server certs here: %s", certsDir)
		copy(certnames.EmbeddedCACert)
		copy(certnames.EmbeddedTenantKVClientCACert)
		copy(certnames.EmbeddedNodeCert)
		copy(certnames.EmbeddedNodeKey)
		copy(certnames.EmbeddedRootCert)
		copy(certnames.EmbeddedRootKey)

		// Start the storage cluster (server).  This uses the certs defined
		// above.
		storageServer, _, _ := serverutils.StartServer(t, base.TestServerArgs{
			Insecure:    false,
			SSLCertsDir: certsDir,
			// StartServer will sometimes start a tenant. This test requires
			// storage server to be the system tenant, otherwise the
			// tenant10ToStorage test will fail, since the storage server will
			// server tenant 10.
			DisableDefaultTestTenant: true,
		})
		defer storageServer.Stopper().Stop(ctx)
		t.Logf("storage server started")

		// We then define the certs for the tenant servers.
		certsDir = filepath.Join(tmpDir, "tenant")
		require.NoError(t, os.MkdirAll(certsDir, 0700))
		t.Logf("tenant server certs here: %s", certsDir)
		copy(certnames.EmbeddedCACert)
		copy(certnames.TenantKVClientCertFilename("10"))
		copy(certnames.TenantKVClientKeyFilename("10"))
		copy(certnames.TenantKVClientCertFilename("11"))
		copy(certnames.TenantKVClientKeyFilename("11"))
		copy(certnames.EmbeddedRootCert)
		copy(certnames.EmbeddedRootKey)

		// The separate SQL server certificate should be tenant-scoped.
		// Alas, the pre-defined cert in the embedded assets includes
		// both tenant IDs 10 and 11, which prevents us from checking
		// the cross-tenant connection error below.
		// So we mint our own cert pair on the fly instead.
		genSQLCertForTenant := func(tid uint64) {
			copy(certnames.EmbeddedCAKey)
			caKey := filepath.Join(certsDir, certnames.EmbeddedCAKey)
			const keySize = 2048 // lower causes TLS error.
			require.NoError(t, security.CreateSQLServerPair(
				certsDir, caKey, keySize,
				time.Hour, /* certificateLifetime */
				true,      /* overwriteFiles */
				[]roachpb.TenantID{roachpb.MustMakeTenantID(tid)}, /* tenantScope */
				[]string{"localhost", "127.0.0.1"}))
		}

		if separateSQLServerCert {
			genSQLCertForTenant(10)
		}

		tenant10 := roachpb.MustMakeTenantID(10)
		sql10, _ := serverutils.StartTenant(t, storageServer, base.TestTenantArgs{
			SSLCertsDir: certsDir,
			TenantID:    tenant10,
		})
		defer sql10.Stopper().Stop(ctx)

		if separateSQLServerCert {
			genSQLCertForTenant(11)
		}

		tenant11 := roachpb.MustMakeTenantID(11)
		sql11, _ := serverutils.StartTenant(t, storageServer, base.TestTenantArgs{
			SSLCertsDir: certsDir,
			TenantID:    tenant11,
		})
		defer sql11.Stopper().Stop(ctx)

		t.Logf("tenant servers started")

		ca := x509.NewCertPool()
		if separateSQLServerCert {
			sqlCA, err := securitytest.Asset(filepath.Join(certnames.EmbeddedCertsDir, certnames.EmbeddedCACert))
			require.NoError(t, err)
			require.True(t, ca.AppendCertsFromPEM(sqlCA))
		} else {
			// Used to sign client-tenant certs.
			tenantCA, err := securitytest.Asset(filepath.Join(certnames.EmbeddedCertsDir, certnames.EmbeddedTenantKVClientCACert))
			require.NoError(t, err)
			require.True(t, ca.AppendCertsFromPEM(tenantCA))
		}
		tlsConfig := &tls.Config{
			RootCAs: ca,
		}

		tests := []struct {
			name     string
			addr     string
			tenantID uint64
			err      bool
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
			err:      true,
		}, {
			name:     "tenant11To10",
			addr:     sql10.SQLAddr(),
			tenantID: 11,
			err:      true,
		}, {
			name:     "tenant10ToStorage",
			addr:     storageServer.ServingSQLAddr(),
			tenantID: 10,
			err:      true,
		}, {
			name:     "tenantWithNodeIDToStorage",
			addr:     storageServer.ServingSQLAddr(),
			tenantID: uint64(storageServer.NodeID()),
			err:      true,
		}}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				tenantID := roachpb.MustMakeTenantID(tc.tenantID)

				tenantConfig, err := tlsConfigForTenant(tenantID, tc.addr, tlsConfig)
				require.NoError(t, err)

				conn, err := BackendDial(startupMsg, tc.addr, tenantConfig)

				if tc.err {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.NotNil(t, conn)
				}
			})
		}
	})
}
