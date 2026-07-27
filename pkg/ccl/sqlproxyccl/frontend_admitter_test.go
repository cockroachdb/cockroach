// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlproxyccl

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

func tlsConfig() (*tls.Config, error) {
	cer, err := tls.LoadX509KeyPair(filepath.Join("testdata", "testserver.crt"), filepath.Join("testdata", "testserver.key"))
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cer},
		ServerName:   "localhost",
	}, nil
}

func TestFrontendAdmitWithNoBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	cli, srv := net.Pipe()
	require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(9e9)))
	require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(9e9)))

	// Close the connection to simulate no bytes.
	cli.Close()

	fe := FrontendAdmit(srv, nil)
	require.EqualError(t, fe.Err, noStartupMessage.Error())
	require.NotNil(t, fe.Conn)
	require.Nil(t, fe.Msg)
}

func TestFrontendAdmitWithClientSSLDisableAndCustomParam(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	cli, srv := net.Pipe()
	require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(9e9)))
	require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(9e9)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		cfg, err := pgconn.ParseConfig(
			"postgres://localhost?sslmode=disable&p1=a",
		)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		cfg.DialFunc = func(
			ctx context.Context, network, addr string,
		) (net.Conn, error) {
			return cli, nil
		}
		_, _ = pgconn.ConnectConfig(ctx, cfg)
		fmt.Printf("Done\n")
	}()

	fe := FrontendAdmit(srv, nil)
	require.NoError(t, fe.Err)
	require.Equal(t, srv, fe.Conn)
	require.NotNil(t, fe.Msg)
	require.Contains(t, fe.Msg.Parameters, "p1")
	require.Equal(t, fe.Msg.Parameters["p1"], "a")
	require.Contains(t, fe.Msg.Parameters, remoteAddrStartupParam)
}

func TestFrontendAdmitWithClientSSLRequire(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	cli, srv := net.Pipe()
	require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(9e9)))
	require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(9e9)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		cfg, err := pgconn.ParseConfig(fmt.Sprintf(
			"postgres://localhost?sslmode=require&sslrootcert=%s",
			datapathutils.TestDataPath(t, "testserver.crt"),
		))
		cfg.TLSConfig.ServerName = "test"
		require.NoError(t, err)
		require.NotNil(t, cfg)
		cfg.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return cli, nil
		}
		_, _ = pgconn.ConnectConfig(ctx, cfg)
	}()

	tlsConfig, err := tlsConfig()
	require.NoError(t, err)
	fe := FrontendAdmit(srv, tlsConfig)
	require.NoError(t, err)
	defer func() { _ = fe.Conn.Close() }()
	require.NotEqual(t, srv, fe.Conn) // The connection was replaced by SSL
	require.NotNil(t, fe.Msg)
	require.Contains(t, fe.Msg.Parameters, remoteAddrStartupParam)
	require.Equal(t, fe.SniServerName, "test")
}

// TestFrontendAdmitRequireEncryption sends StartupRequest when SSlRequest is
// expected.
func TestFrontendAdmitRequireEncryption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	cli, srv := net.Pipe()
	require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(9e9)))
	require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(9e9)))

	go func() {
		startup := pgproto3.StartupMessage{
			ProtocolVersion: pgproto3.ProtocolVersionNumber,
			Parameters:      map[string]string{"key": "val"},
		}
		buf, err := startup.Encode([]byte{})
		require.NoError(t, err)
		_, err = cli.Write(buf)
		require.NoError(t, err)
	}()

	tlsConfig, err := tlsConfig()
	require.NoError(t, err)
	fe := FrontendAdmit(srv, tlsConfig)
	require.EqualError(t, fe.Err,
		"codeUnexpectedInsecureStartupMessage: "+
			"unsupported startup message: *pgproto3.StartupMessage")
	require.NotNil(t, fe.Conn)
	require.Nil(t, fe.Msg)
}

// TestFrontendAdmitWithCancel sends CancelRequest.
func TestFrontendAdmitWithCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	cli, srvPipe := net.Pipe()
	srv := &fakeTCPConn{
		Conn:       srvPipe,
		remoteAddr: &net.TCPAddr{IP: net.IP{1, 2, 3, 4}},
		localAddr:  &net.TCPAddr{IP: net.IP{4, 5, 6, 7}},
	}
	require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(9e9)))
	require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(9e9)))

	go func() {
		cancelRequest := pgproto3.CancelRequest{ProcessID: 1, SecretKey: 2}
		buf, err := cancelRequest.Encode([]byte{})
		require.NoError(t, err)
		_, err = cli.Write(buf)
		require.NoError(t, err)
	}()

	fe := FrontendAdmit(srv, nil)
	require.NoError(t, fe.Err)
	require.NotNil(t, fe.Conn)
	require.NotNil(t, fe.CancelRequest)
	require.Nil(t, fe.Msg)
}

// TestFrontendAdmitWithSSLAndCancel sends SSLRequest followed by CancelRequest.
func TestFrontendAdmitWithSSLAndCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	cli, srvPipe := net.Pipe()
	srv := &fakeTCPConn{
		Conn:       srvPipe,
		remoteAddr: &net.TCPAddr{IP: net.IP{1, 2, 3, 4}},
		localAddr:  &net.TCPAddr{IP: net.IP{4, 5, 6, 7}},
	}
	require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(9e9)))
	require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(9e9)))

	go func() {
		sslRequest := pgproto3.SSLRequest{}
		buf, err := sslRequest.Encode([]byte{})
		require.NoError(t, err)
		_, err = cli.Write(buf)
		require.NoError(t, err)
		b := []byte{0}
		n, err := cli.Read(b)
		require.Equal(t, n, 1)
		require.NoError(t, err)
		cli = tls.Client(cli, &tls.Config{InsecureSkipVerify: true})
		cancelRequest := pgproto3.CancelRequest{ProcessID: 1, SecretKey: 2}
		buf, err = cancelRequest.Encode([]byte{})
		require.NoError(t, err)
		_, err = cli.Write(buf)
		require.NoError(t, err)
	}()

	tlsConfig, err := tlsConfig()
	require.NoError(t, err)
	fe := FrontendAdmit(srv, tlsConfig)
	require.NoError(t, fe.Err)
	require.NotNil(t, fe.Conn)
	require.NotNil(t, fe.CancelRequest)
	require.Nil(t, fe.Msg)
}

func TestFrontendAdmitSessionRevivalToken(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	cli, srv := net.Pipe()
	require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(9e9)))
	require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(9e9)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		cfg, err := pgconn.ParseConfig(
			"postgres://localhost?sslmode=disable&crdb:session_revival_token_base64=abc",
		)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		cfg.DialFunc = func(
			ctx context.Context, network, addr string,
		) (net.Conn, error) {
			return cli, nil
		}
		_, _ = pgconn.ConnectConfig(ctx, cfg)
		fmt.Printf("Done\n")
	}()

	fe := FrontendAdmit(srv, nil)
	require.EqualError(t, fe.Err, "codeUnexpectedStartupMessage: parameter crdb:session_revival_token_base64 is not allowed")
	require.NotNil(t, fe.Conn)
	require.Nil(t, fe.Msg)
}

// writeStartupMessage encodes a StartupMessage carrying params and writes it to
// conn from a new goroutine, so a FrontendAdmit call reading the other end of
// the pipe can make progress. It injects raw, exact-case parameter keys; a pg
// client library would normalize keys before sending and so could not exercise
// the mixed-case handling under test.
func writeStartupMessage(t *testing.T, conn net.Conn, params map[string]string) {
	go func() {
		startup := pgproto3.StartupMessage{
			ProtocolVersion: pgproto3.ProtocolVersionNumber,
			Parameters:      params,
		}
		buf, err := startup.Encode([]byte{})
		require.NoError(t, err)
		_, err = conn.Write(buf)
		require.NoError(t, err)
	}()
}

// TestFrontendAdmitMixedCaseSessionRevivalTokenRejected verifies that the
// session revival token is rejected regardless of the case the client uses for
// the key. The backend folds keys to lowercase, so a case-sensitive check would
// let a mixed-case variant through to be honored by the backend.
func TestFrontendAdmitMixedCaseSessionRevivalTokenRejected(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.ServerlessOnly(t)

	for _, key := range []string{
		"crdb:session_revival_token_base64", // canonical case (regression)
		"CRDB:session_revival_token_base64",
		"Crdb:Session_Revival_Token_Base64",
		"CRDB:SESSION_REVIVAL_TOKEN_BASE64",
	} {
		t.Run(key, func(t *testing.T) {
			cli, srv := net.Pipe()
			require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(9e9)))
			require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(9e9)))

			writeStartupMessage(t, cli, map[string]string{key: "abc"})

			fe := FrontendAdmit(srv, nil)
			require.EqualError(t, fe.Err, "codeUnexpectedStartupMessage: "+
				"parameter crdb:session_revival_token_base64 is not allowed")
			require.Nil(t, fe.Msg)
		})
	}
}

// TestFrontendAdmitMixedCaseRemoteAddrOverwritten verifies that a client cannot
// spoof its remote address through a mixed-case variant of crdb:remote_addr.
// Every case variant of the key is dropped and the real address is then set, so
// neither the spoofed value nor a leftover mixed-case key survives. The client
// sends two case variants at once, and the loop repeats so the result is proven
// independent of map iteration order.
func TestFrontendAdmitMixedCaseRemoteAddrOverwritten(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.ServerlessOnly(t)

	realAddr := &net.TCPAddr{IP: net.IP{1, 2, 3, 4}, Port: 26257}
	for i := 0; i < 50; i++ {
		cli, srvPipe := net.Pipe()
		srv := &fakeTCPConn{
			Conn:       srvPipe,
			remoteAddr: realAddr,
			localAddr:  &net.TCPAddr{IP: net.IP{4, 5, 6, 7}},
		}
		require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(9e9)))
		require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(9e9)))

		writeStartupMessage(t, cli, map[string]string{
			"crdb:remote_addr": "9.9.9.9:9999",
			"CRDB:remote_addr": "8.8.8.8:8888",
		})

		fe := FrontendAdmit(srv, nil)
		require.NoError(t, fe.Err)
		require.NotNil(t, fe.Msg)
		require.Equal(t, realAddr.String(), fe.Msg.Parameters[remoteAddrStartupParam])
		_, hasVariant := fe.Msg.Parameters["CRDB:remote_addr"]
		require.False(t, hasVariant)
	}
}

// TestFrontendAdmitForwardsNonReservedKeysVerbatim verifies that the
// case-insensitive handling is scoped to the reserved parameters only: a
// non-reserved key is forwarded to the backend with its original case
// preserved. The backend folds keys to lowercase itself, and callers such as
// CockroachCloud's BackendDial read client parameters by their original case
// (see TestProxyModifyRequestParams), so the proxy must not fold them.
func TestFrontendAdmitForwardsNonReservedKeysVerbatim(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.ServerlessOnly(t)

	cli, srv := net.Pipe()
	require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(9e9)))
	require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(9e9)))

	writeStartupMessage(t, cli, map[string]string{"Application_Name": "myapp"})

	fe := FrontendAdmit(srv, nil)
	require.NoError(t, fe.Err)
	require.NotNil(t, fe.Msg)
	require.Equal(t, "myapp", fe.Msg.Parameters["Application_Name"])
	_, hasFolded := fe.Msg.Parameters["application_name"]
	require.False(t, hasFolded)
}
