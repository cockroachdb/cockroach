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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

func tlsConfig() (*tls.Config, error) {
	cer, err := tls.LoadX509KeyPair("testdata/testserver.crt", "testdata/testserver.key")
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cer},
		ServerName:   "localhost",
	}, nil
}

func TestFrontendAdmitWithClientSSLDisableAndCustomParam(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	frontendCon, msg, err := FrontendAdmit(srv, nil)
	require.NoError(t, err)
	require.Equal(t, srv, frontendCon)
	require.NotNil(t, msg)
	require.Contains(t, msg.Parameters, "p1")
	require.Equal(t, msg.Parameters["p1"], "a")
}

func TestFrontendAdmitWithClientSSLRequire(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cli, srv := net.Pipe()
	require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(3e9)))
	require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(3e9)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		cfg, err := pgconn.ParseConfig("postgres://localhost?sslmode=require")
		require.NoError(t, err)
		require.NotNil(t, cfg)
		cfg.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return cli, nil
		}
		_, _ = pgconn.ConnectConfig(ctx, cfg)
	}()

	tlsConfig, err := tlsConfig()
	require.NoError(t, err)
	frontendCon, msg, err := FrontendAdmit(srv, tlsConfig)
	require.NoError(t, err)
	defer func() { _ = frontendCon.Close() }()
	require.NotEqual(t, srv, frontendCon) // The connection was replaced by SSL
	require.NotNil(t, msg)
}

func TestFrontendAdmitWithCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cli, srv := net.Pipe()
	require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(3e9)))
	require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(3e9)))

	go func() {
		cancelRequest := pgproto3.CancelRequest{ProcessID: 1, SecretKey: 2}
		_, err := cli.Write(cancelRequest.Encode([]byte{}))
		require.NoError(t, err)
	}()

	frontendCon, msg, err := FrontendAdmit(srv, nil)
	require.EqualError(t, err,
		"codeUnexpectedStartupMessage: "+
			"unsupported post-TLS startup message: *pgproto3.CancelRequest",
	)
	require.NotNil(t, frontendCon)
	require.Nil(t, msg)
}

func TestFrontendAdmitWithSSLAndCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cli, srv := net.Pipe()
	require.NoError(t, srv.SetReadDeadline(timeutil.Now().Add(3e9)))
	require.NoError(t, cli.SetReadDeadline(timeutil.Now().Add(3e9)))

	go func() {
		sslRequest := pgproto3.SSLRequest{}
		_, err := cli.Write(sslRequest.Encode([]byte{}))
		require.NoError(t, err)
		b := []byte{0}
		n, err := cli.Read(b)
		require.Equal(t, n, 1)
		require.NoError(t, err)
		cli = tls.Client(cli, &tls.Config{InsecureSkipVerify: true})
		cancelRequest := pgproto3.CancelRequest{ProcessID: 1, SecretKey: 2}
		_, err = cli.Write(cancelRequest.Encode([]byte{}))
		require.NoError(t, err)
	}()

	tlsConfig, err := tlsConfig()
	require.NoError(t, err)
	frontendCon, msg, err := FrontendAdmit(srv, tlsConfig)
	require.EqualError(t, err,
		"codeUnexpectedStartupMessage: "+
			"unsupported post-TLS startup message: *pgproto3.CancelRequest",
	)
	require.NotNil(t, frontendCon)
	require.Nil(t, msg)
}
