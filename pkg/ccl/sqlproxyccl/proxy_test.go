// Copyright 2020 The Cockroach Authors.
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
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

func setupTestProxyWithCerts(t *testing.T, opts *Options) (addr string, done func()) {
	// Created via:
	const _ = `
openssl genrsa -out testserver.key 2048
# Enter * as Common Name below, rest can be empty.
openssl req -new -x509 -sha256 -key testserver.key -out testserver.crt -days 3650
`
	cer, err := tls.LoadX509KeyPair("testserver.crt", "testserver.key")
	require.NoError(t, err)
	opts.IncomingTLSConfig = &tls.Config{
		Certificates: []tls.Certificate{cer},
		ServerName:   "localhost",
	}
	opts.OutgoingTLSConfig = &tls.Config{
		// NB: this would be false in production.
		InsecureSkipVerify: true,
	}

	const listenAddress = "127.0.0.1:0"
	// NB: ln closes before wg.Wait or we deadlock.
	ln, err := net.Listen("tcp", listenAddress)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)

	done = func() {
		_ = ln.Close()
		wg.Wait()
	}

	go func() {
		defer wg.Done()
		_ = Serve(ln, *opts)
	}()

	return ln.Addr().String(), done
}

func testingTenantIDFromDatabaseForAddr(
	addr string, validTenant string,
) func(map[string]string) (string, error) {
	return func(p map[string]string) (_ string, clientErr error) {
		const dbKey = "database"
		db, ok := p[dbKey]
		if !ok {
			return "", errors.Newf("need to specify database")
		}
		sl := strings.SplitN(db, "_", 2)
		if len(sl) != 2 {
			return "", errors.Newf("malformed database name")
		}
		db, tenantID := sl[0], sl[1]

		if tenantID != validTenant {
			return "", errors.Newf("invalid tenantID")
		}

		p[dbKey] = db
		return addr, nil
	}
}

type assertCtx struct {
	emittedCode *ErrorCode
}

func makeAssertCtx() assertCtx {
	var emittedCode ErrorCode = -1
	return assertCtx{
		emittedCode: &emittedCode,
	}
}

func (ac *assertCtx) onSendErrToClient(code ErrorCode, msg string) string {
	*ac.emittedCode = code
	return msg
}

func (ac *assertCtx) assertConnectErr(
	t *testing.T, prefix, suffix string, expCode ErrorCode, expErr string,
) {
	t.Helper()
	*ac.emittedCode = -1
	t.Run(suffix, func(t *testing.T) {
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, prefix+suffix)
		if err == nil {
			_ = conn.Close(ctx)
		}
		require.Contains(t, err.Error(), expErr)
		require.Equal(t, expCode, *ac.emittedCode)

	})
}

func TestLongDBName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ac := makeAssertCtx()

	var m map[string]string
	opts := Options{
		OutgoingAddrFromParams: func(mm map[string]string) (string, error) {
			m = mm
			return "", errors.New("boom")
		},
		OnSendErrToClient: ac.onSendErrToClient,
	}
	addr, done := setupTestProxyWithCerts(t, &opts)
	defer done()

	longDB := strings.Repeat("x", 70) // 63 is limit
	pgurl := fmt.Sprintf("postgres://unused:unused@%s/%s", addr, longDB)
	ac.assertConnectErr(t, pgurl, "" /* suffix */, CodeParamsRoutingFailed, "boom")
	require.Equal(t, longDB, m["database"])
}

func TestFailedConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// TODO(asubiotto): consider using datadriven for these, especially if the
	// proxy becomes more complex.

	ac := makeAssertCtx()
	opts := Options{
		OutgoingAddrFromParams: testingTenantIDFromDatabaseForAddr("undialable%$!@$", "29"),
		OnSendErrToClient:      ac.onSendErrToClient,
	}
	addr, done := setupTestProxyWithCerts(t, &opts)
	defer done()

	_, p, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	u := fmt.Sprintf("postgres://unused:unused@localhost:%s/", p)
	// Valid connections, but no backend server running.
	for _, sslmode := range []string{"require", "prefer"} {
		ac.assertConnectErr(
			t, u, "defaultdb_29?sslmode="+sslmode,
			CodeBackendDown, "unable to reach backend SQL server",
		)
	}
	ac.assertConnectErr(
		t, u, "defaultdb_29?sslmode=verify-ca&sslrootcert=testserver.crt",
		CodeBackendDown, "unable to reach backend SQL server",
	)
	ac.assertConnectErr(
		t, u, "defaultdb_29?sslmode=verify-full&sslrootcert=testserver.crt",
		CodeBackendDown, "unable to reach backend SQL server",
	)

	// Unencrypted connections bounce.
	for _, sslmode := range []string{"disable", "allow"} {
		ac.assertConnectErr(
			t, u, "defaultdb_29?sslmode="+sslmode,
			CodeUnexpectedInsecureStartupMessage, "server requires encryption",
		)
	}

	// TenantID rejected by test hook.
	ac.assertConnectErr(
		t, u, "defaultdb_28?sslmode=require",
		CodeParamsRoutingFailed, "invalid tenantID",
	)

	// No TenantID.
	ac.assertConnectErr(
		t, u, "defaultdb?sslmode=require",
		CodeParamsRoutingFailed, "malformed database name",
	)
}

func TestProxyAgainstSecureCRDB(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	skip.IgnoreLint(t, "this test needs a running (secure) CockroachDB instance at the given address")
	const crdbSQL = "127.0.0.1:52966"
	// TODO(asubiotto): use an in-mem test server once this code lives in the CRDB
	// repo.
	//
	// TODO(tbg): if I use the https (!) port of ./cockroach demo, the
	// connection hangs instead of failing. Why? Probably both ends end up waiting
	// for the other side due to protocol mismatch. Should set deadlines on all
	// the read/write ops to avoid this failure mode.

	opts := Options{
		OutgoingAddrFromParams: testingTenantIDFromDatabaseForAddr(crdbSQL, "29"),
	}
	addr, done := setupTestProxyWithCerts(t, &opts)
	defer done()

	url := fmt.Sprintf("postgres://root:admin@%s/defaultdb_29?sslmode=require", addr)
	conn, err := pgx.Connect(context.Background(), url)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close(ctx))
	}()

	var n int
	err = conn.QueryRow(context.Background(), "SELECT $1::int", 1).Scan(&n)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
}
