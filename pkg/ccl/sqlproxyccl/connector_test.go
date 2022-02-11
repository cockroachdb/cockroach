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
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/throttler"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

func TestRetriableConnectorError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	err := errors.New("foobar")
	require.False(t, IsRetriableConnectorError(err))
	err = MarkAsRetriableConnectorError(err)
	require.True(t, IsRetriableConnectorError(err))
}

func TestConnector_OpenClusterConnWithToken(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const token = "foobarbaz"
	ctx := context.Background()

	t.Run("error", func(t *testing.T) {
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
		}
		c.testingKnobs.openClusterConnInternal = func(ctx context.Context) (net.Conn, error) {
			return nil, errors.New("foo")
		}

		crdbConn, err := c.OpenClusterConnWithToken(ctx, token)
		require.EqualError(t, err, "foo")
		require.Nil(t, crdbConn)

		// Ensure that token is deleted.
		str, ok := c.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
		require.False(t, ok)
		require.Equal(t, "", str)
	})

	t.Run("successful", func(t *testing.T) {
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
		}
		conn, _ := net.Pipe()
		defer conn.Close()

		var openCalled bool
		c.testingKnobs.openClusterConnInternal = func(ctx context.Context) (net.Conn, error) {
			openCalled = true

			// Validate that token is set.
			str, ok := c.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
			require.True(t, ok)
			require.Equal(t, token, str)

			return conn, nil
		}

		crdbConn, err := c.OpenClusterConnWithToken(ctx, token)
		require.True(t, openCalled)
		require.NoError(t, err)
		require.Equal(t, conn, crdbConn)

		// Ensure that token is deleted.
		_, ok := c.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
		require.False(t, ok)
	})

	t.Run("idle monitor wrapper is called", func(t *testing.T) {
		var wrapperCalled bool
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
			IdleMonitorWrapperFn: func(crdbConn net.Conn) net.Conn {
				wrapperCalled = true
				return crdbConn
			},
		}

		conn, _ := net.Pipe()
		defer conn.Close()

		var openCalled bool
		c.testingKnobs.openClusterConnInternal = func(ctx context.Context) (net.Conn, error) {
			openCalled = true

			// Validate that token is set.
			str, ok := c.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
			require.True(t, ok)
			require.Equal(t, token, str)

			return conn, nil
		}

		crdbConn, err := c.OpenClusterConnWithToken(ctx, token)
		require.True(t, wrapperCalled)
		require.True(t, openCalled)
		require.NoError(t, err)
		require.Equal(t, conn, crdbConn)

		// Ensure that token is deleted.
		_, ok := c.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
		require.False(t, ok)
	})
}

func TestConnector_OpenClusterConnWithAuth(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	dummyHook := func(throttler.AttemptStatus) error {
		return nil
	}

	t.Run("error during open", func(t *testing.T) {
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
		}
		c.testingKnobs.openClusterConnInternal = func(ctx context.Context) (net.Conn, error) {
			return nil, errors.New("foo")
		}

		crdbConn, sentToClient, err := c.OpenClusterConnWithAuth(ctx,
			nil /* clientConn */, nil /* throttleHook */)
		require.EqualError(t, err, "foo")
		require.False(t, sentToClient)
		require.Nil(t, crdbConn)
	})

	t.Run("error during auth", func(t *testing.T) {
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: make(map[string]string),
			},
			AuthenticateFn: func(
				client net.Conn,
				server net.Conn,
				throttleHook func(throttler.AttemptStatus) error,
			) error {
				return errors.New("bar")
			},
		}

		conn, _ := net.Pipe()
		defer conn.Close()

		var openCalled bool
		c.testingKnobs.openClusterConnInternal = func(ctx context.Context) (net.Conn, error) {
			openCalled = true
			return conn, nil
		}

		crdbConn, sentToClient, err := c.OpenClusterConnWithAuth(ctx,
			nil /* clientConn */, nil /* throttleHook */)
		require.True(t, openCalled)
		require.EqualError(t, err, "bar")
		require.True(t, sentToClient)
		require.Nil(t, crdbConn)

		// Connection should be closed.
		_, err = conn.Write([]byte("foo"))
		require.Regexp(t, "closed pipe", err)
	})

	t.Run("successful", func(t *testing.T) {
		clientConn, _ := net.Pipe()
		defer clientConn.Close()

		var authCalled bool
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: map[string]string{
					// Passing in a token should have no effect.
					sessionRevivalTokenStartupParam: "foo",
				},
			},
			AuthenticateFn: func(
				client net.Conn,
				server net.Conn,
				throttleHook func(throttler.AttemptStatus) error,
			) error {
				authCalled = true
				require.Equal(t, clientConn, client)
				require.NotNil(t, server)
				require.Equal(t, reflect.ValueOf(dummyHook).Pointer(),
					reflect.ValueOf(throttleHook).Pointer())
				return nil
			},
		}

		conn, _ := net.Pipe()
		defer conn.Close()

		var openCalled bool
		c.testingKnobs.openClusterConnInternal = func(ctx context.Context) (net.Conn, error) {
			openCalled = true

			// Validate that token is not set.
			_, ok := c.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
			require.False(t, ok)

			return conn, nil
		}

		crdbConn, sentToClient, err := c.OpenClusterConnWithAuth(ctx, clientConn, dummyHook)
		require.True(t, openCalled)
		require.True(t, authCalled)
		require.NoError(t, err)
		require.False(t, sentToClient)
		require.Equal(t, conn, crdbConn)
	})

	t.Run("idle monitor wrapper is called", func(t *testing.T) {
		clientConn, _ := net.Pipe()
		defer clientConn.Close()

		var authCalled, wrapperCalled bool
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{
				Parameters: map[string]string{
					// Passing in a token should have no effect.
					sessionRevivalTokenStartupParam: "foo",
				},
			},
			AuthenticateFn: func(
				client net.Conn,
				server net.Conn,
				throttleHook func(throttler.AttemptStatus) error,
			) error {
				authCalled = true
				require.Equal(t, clientConn, client)
				require.NotNil(t, server)
				require.Equal(t, reflect.ValueOf(dummyHook).Pointer(),
					reflect.ValueOf(throttleHook).Pointer())
				return nil
			},
			IdleMonitorWrapperFn: func(crdbConn net.Conn) net.Conn {
				wrapperCalled = true
				return crdbConn
			},
		}

		conn, _ := net.Pipe()
		defer conn.Close()

		var openCalled bool
		c.testingKnobs.openClusterConnInternal = func(ctx context.Context) (net.Conn, error) {
			openCalled = true

			// Validate that token is not set.
			_, ok := c.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
			require.False(t, ok)

			return conn, nil
		}

		crdbConn, sentToClient, err := c.OpenClusterConnWithAuth(ctx, clientConn, dummyHook)
		require.True(t, openCalled)
		require.True(t, wrapperCalled)
		require.True(t, authCalled)
		require.NoError(t, err)
		require.False(t, sentToClient)
		require.Equal(t, conn, crdbConn)
	})
}

func TestConnector_openClusterConnInternal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	bgCtx := context.Background()

	t.Run("context canceled at boundary", func(t *testing.T) {
		// This is a short test, and is expected to finish within ms.
		ctx, cancel := context.WithTimeout(bgCtx, 2*time.Second)
		defer cancel()

		var onLookupEventCalled bool
		c := &connector{
			AddrLookupFn: func(ctx context.Context) (string, error) {
				return "", MarkAsRetriableConnectorError(errors.New("baz"))
			},
			OnLookupEvent: func(ctx context.Context, err error) {
				onLookupEventCalled = true
				require.EqualError(t, err, "baz")

				// Cancel context to trigger loop exit.
				cancel()
			},
		}
		conn, err := c.openClusterConnInternal(ctx)
		require.EqualError(t, err, "baz")
		require.True(t, errors.Is(err, context.Canceled))
		require.Nil(t, conn)

		require.True(t, onLookupEventCalled)
	})

	t.Run("context canceled within loop", func(t *testing.T) {
		// This is a short test, and is expected to finish within ms.
		ctx, cancel := context.WithTimeout(bgCtx, 2*time.Second)
		defer cancel()

		var onLookupEventCalled bool
		c := &connector{
			AddrLookupFn: func(ctx context.Context) (string, error) {
				return "", errors.Wrap(context.Canceled, "foobar")
			},
			OnLookupEvent: func(ctx context.Context, err error) {
				onLookupEventCalled = true
				require.EqualError(t, err, "foobar: context canceled")
			},
		}
		conn, err := c.openClusterConnInternal(ctx)
		require.EqualError(t, err, "foobar: context canceled")
		require.True(t, errors.Is(err, context.Canceled))
		require.Nil(t, conn)

		require.True(t, onLookupEventCalled)
	})

	t.Run("non-transient error", func(t *testing.T) {
		// This is a short test, and is expected to finish within ms.
		ctx, cancel := context.WithTimeout(bgCtx, 2*time.Second)
		defer cancel()

		c := &connector{
			AddrLookupFn: func(ctx context.Context) (string, error) {
				return "", errors.New("baz")
			},
		}
		conn, err := c.openClusterConnInternal(ctx)
		require.EqualError(t, err, "baz")
		require.Nil(t, conn)
	})

	t.Run("successful", func(t *testing.T) {
		// This should not take more than 5 seconds.
		ctx, cancel := context.WithTimeout(bgCtx, 5*time.Second)
		defer cancel()

		crdbConn, _ := net.Pipe()
		defer crdbConn.Close()

		// We will exercise the following events:
		// 1. retriable error on Lookup.
		// 2. retriable error on Dial.
		var addrLookupFnCount, dialOutgoingAddrCount int
		var onLookupEventCalled, onDialEventCalled bool
		c := &connector{
			AddrLookupFn: func(ctx context.Context) (string, error) {
				addrLookupFnCount++
				if addrLookupFnCount == 1 {
					return "", MarkAsRetriableConnectorError(errors.New("foo"))
				}
				return "127.0.0.10:42", nil
			},
			OnLookupEvent: func(ctx context.Context, err error) {
				onLookupEventCalled = true
				if addrLookupFnCount == 1 {
					require.EqualError(t, err, "foo")
				} else {
					require.NoError(t, err)
				}
			},
			OnDialEvent: func(ctx context.Context, outgoingAddr string, err error) {
				onDialEventCalled = true
				require.Equal(t, "127.0.0.10:42", outgoingAddr)
				if dialOutgoingAddrCount == 1 {
					require.EqualError(t, err, "bar")
				} else {
					require.NoError(t, err)
				}
			},
		}
		c.testingKnobs.dialOutgoingAddr = func(outgoingAddr string) (net.Conn, error) {
			dialOutgoingAddrCount++
			if dialOutgoingAddrCount == 1 {
				return nil, MarkAsRetriableConnectorError(errors.New("bar"))
			}
			return crdbConn, nil
		}
		conn, err := c.openClusterConnInternal(ctx)
		require.NoError(t, err)
		require.Equal(t, crdbConn, conn)

		// Assert existing calls.
		require.Equal(t, 3, addrLookupFnCount)
		require.Equal(t, 2, dialOutgoingAddrCount)
		require.True(t, onLookupEventCalled)
		require.True(t, onDialEventCalled)
	})
}

func TestConnector_dialOutgoingAddr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("with tlsConfig", func(t *testing.T) {
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{},
			TLSConfig:  &tls.Config{InsecureSkipVerify: true},
		}
		crdbConn, _ := net.Pipe()
		defer crdbConn.Close()

		defer testutils.TestingHook(&BackendDial,
			func(msg *pgproto3.StartupMessage, outgoingAddress string,
				tlsConfig *tls.Config) (net.Conn, error) {
				require.Equal(t, c.StartupMsg, msg)
				require.Equal(t, "10.11.12.13:80", outgoingAddress)
				require.Equal(t, "10.11.12.13", tlsConfig.ServerName)
				return crdbConn, nil
			},
		)()
		conn, err := c.dialOutgoingAddr("10.11.12.13:80")
		require.NoError(t, err)
		require.Equal(t, crdbConn, conn)
	})

	t.Run("invalid outgoingAddr with tlsConfig", func(t *testing.T) {
		c := &connector{
			StartupMsg: &pgproto3.StartupMessage{},
			TLSConfig:  &tls.Config{InsecureSkipVerify: true},
		}
		conn, err := c.dialOutgoingAddr("!@#$::")
		require.Error(t, err)
		require.Regexp(t, "invalid address format", err)
		require.False(t, IsRetriableConnectorError(err))
		require.Nil(t, conn)
	})

	t.Run("without tlsConfig", func(t *testing.T) {
		c := &connector{StartupMsg: &pgproto3.StartupMessage{}}
		crdbConn, _ := net.Pipe()
		defer crdbConn.Close()

		defer testutils.TestingHook(&BackendDial,
			func(msg *pgproto3.StartupMessage, outgoingAddress string,
				tlsConfig *tls.Config) (net.Conn, error) {
				require.Equal(t, c.StartupMsg, msg)
				require.Equal(t, "10.11.12.13:1234", outgoingAddress)
				require.Nil(t, tlsConfig)
				return crdbConn, nil
			},
		)()
		conn, err := c.dialOutgoingAddr("10.11.12.13:1234")
		require.NoError(t, err)
		require.Equal(t, crdbConn, conn)
	})

	t.Run("failed to dial with non-transient error", func(t *testing.T) {
		c := &connector{StartupMsg: &pgproto3.StartupMessage{}}
		defer testutils.TestingHook(&BackendDial,
			func(msg *pgproto3.StartupMessage, outgoingAddress string,
				tlsConfig *tls.Config) (net.Conn, error) {
				require.Equal(t, c.StartupMsg, msg)
				require.Equal(t, "127.0.0.1:1234", outgoingAddress)
				require.Nil(t, tlsConfig)
				return nil, errors.New("foo")
			},
		)()
		conn, err := c.dialOutgoingAddr("127.0.0.1:1234")
		require.EqualError(t, err, "foo")
		require.False(t, IsRetriableConnectorError(err))
		require.Nil(t, conn)
	})

	t.Run("failed to dial with transient error", func(t *testing.T) {
		c := &connector{StartupMsg: &pgproto3.StartupMessage{}}
		defer testutils.TestingHook(&BackendDial,
			func(msg *pgproto3.StartupMessage, outgoingAddress string,
				tlsConfig *tls.Config) (net.Conn, error) {
				require.Equal(t, c.StartupMsg, msg)
				require.Equal(t, "127.0.0.2:4567", outgoingAddress)
				require.Nil(t, tlsConfig)
				return nil, newErrorf(codeBackendDown, "bar")
			},
		)()
		conn, err := c.dialOutgoingAddr("127.0.0.2:4567")
		require.EqualError(t, err, "codeBackendDown: bar")
		require.True(t, IsRetriableConnectorError(err))
		require.Nil(t, conn)
	})
}
