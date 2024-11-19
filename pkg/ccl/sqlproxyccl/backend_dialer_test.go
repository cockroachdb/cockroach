// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlproxyccl

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	pgproto3 "github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

func TestBackendDialTLSInsecure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	startupMsg := &pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber}

	sql := serverutils.StartServerOnly(t, base.TestServerArgs{Insecure: true})
	defer sql.Stopper().Stop(ctx)

	conn, err := BackendDial(context.Background(), startupMsg, sql.ApplicationLayer().AdvSQLAddr(), &tls.Config{})
	require.Error(t, err)
	require.Regexp(t, "target server refused TLS connection", err)
	require.Nil(t, conn)
}

func TestBackendDialBlackhole(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	defer log.Scope(t).Close(t)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	conChannel := make(chan net.Conn, 1)
	go func() {
		// accept then ignore the connection
		conn, err := listener.Accept()
		require.NoError(t, err)
		conChannel <- conn
	}()

	startupMsg := &pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = BackendDial(ctx, startupMsg, listener.Addr().String(), &tls.Config{})
	require.Error(t, err)
	require.ErrorIs(t, err, ctx.Err())
	(<-conChannel).Close()
}

func TestBackendDialTLS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
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

	storageServer := serverutils.StartServerOnly(t, base.TestServerArgs{
		Insecure:          false,
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer storageServer.Stopper().Stop(ctx)

	tenant10 := roachpb.MustMakeTenantID(10)
	sql10, _ := serverutils.StartTenant(t, storageServer, base.TestTenantArgs{TenantID: tenant10})
	defer sql10.AppStopper().Stop(ctx)

	tenant11 := roachpb.MustMakeTenantID(11)
	sql11, _ := serverutils.StartTenant(t, storageServer, base.TestTenantArgs{TenantID: tenant11})
	defer sql11.AppStopper().Stop(ctx)

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
		errCode:  codeBackendDialFailed,
	}, {
		name:     "tenant11To10",
		addr:     sql10.SQLAddr(),
		tenantID: 11,
		errCode:  codeBackendDialFailed,
	}, {
		name:     "tenant10ToStorage",
		addr:     storageServer.SystemLayer().AdvSQLAddr(),
		tenantID: 10,
		errCode:  codeBackendDialFailed,
	}, {
		name:     "tenantWithNodeIDToStoage",
		addr:     storageServer.SystemLayer().AdvSQLAddr(),
		tenantID: uint64(storageServer.NodeID()),
		errCode:  codeBackendDialFailed,
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tenantID := roachpb.MustMakeTenantID(tc.tenantID)

			tenantConfig, err := tlsConfigForTenant(tenantID, tc.addr, tlsConfig)
			require.NoError(t, err)

			conn, err := BackendDial(context.Background(), startupMsg, tc.addr, tenantConfig)

			if tc.errCode != codeNone {
				require.Equal(t, tc.errCode, getErrorCode(err))
			} else {
				require.NoError(t, err)
				require.NotNil(t, conn)
			}
		})
	}
}

type closeCounter struct {
	net.Conn
	closeCount int32
}

func (n *closeCounter) Close() error {
	_ = atomic.AddInt32(&n.closeCount, 1)
	return nil
}

func (n *closeCounter) CloseCount() int {
	return int(atomic.LoadInt32(&n.closeCount))
}

func TestCloseOnCancelCleanupBeforeCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	defer log.Scope(t).Close(t)

	conn := &closeCounter{}
	for i := 0; i < 1000; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cleanup := closeWhenCancelled(ctx, conn)
		cleanup()
		cancel()
	}
	require.Equal(t, 0, conn.CloseCount())
}

func TestCloseOnCancelCancelBeforeCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	defer log.Scope(t).Close(t)

	conn := &closeCounter{}
	for i := 0; i < 1000; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cleanup := closeWhenCancelled(ctx, conn)
		cancel()
		cleanup()
	}
	testutils.SucceedsSoon(t, func() error {
		count := conn.CloseCount()
		if count != 1000 {
			return errors.Newf("expected 1000 closes found %d", count)
		}
		return nil
	})
}
