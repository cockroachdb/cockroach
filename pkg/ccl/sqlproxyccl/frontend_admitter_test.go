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
	require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(3e9)))
	require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(3e9)))

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
	require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(3e9)))
	require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(3e9)))

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
	require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(3e9)))
	require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(3e9)))

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
	require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(3e9)))
	require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(3e9)))

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
	require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(3e9)))
	require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(3e9)))

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
	require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(3e9)))
	require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(3e9)))

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
	require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(3e9)))
	require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(3e9)))

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
