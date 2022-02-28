// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	pgproto3 "github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

// TestForward is a simple test for message forwarding without connection
// migration. For in-depth tests, see proxy_handler_test.go.
func TestForward(t *testing.T) {
	defer leaktest.AfterTest(t)()

	bgCtx := context.Background()

	t.Run("closed_when_processors_error", func(t *testing.T) {
		p1, p2 := net.Pipe()
		// Close the connection right away. p2 is owned by the forwarder.
		p1.Close()

		f := forward(bgCtx, nil /* connector */, nil /* metrics */, p1, p2)
		defer f.Close()

		// We have to wait for the goroutine to run. Once the forwarder stops,
		// we're good.
		testutils.SucceedsSoon(t, func() error {
			if f.ctx.Err() != nil {
				return nil
			}
			return errors.New("forwarder is still running")
		})
	})

	t.Run("client_to_server", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(bgCtx, 5*time.Second)
		defer cancel()

		clientW, clientR := net.Pipe()
		serverW, serverR := net.Pipe()
		// We don't close clientW and serverR here since we have no control
		// over those. serverW is not closed since the forwarder is responsible
		// for that.
		defer clientR.Close()

		f := forward(ctx, nil /* connector */, nil /* metrics */, clientR, serverW)
		defer f.Close()
		require.Nil(t, f.ctx.Err())

		// Client writes some pgwire messages.
		errChan := make(chan error, 1)
		go func() {
			_, err := clientW.Write((&pgproto3.Query{
				String: "SELECT 1",
			}).Encode(nil))
			if err != nil {
				errChan <- err
				return
			}

			if _, err := clientW.Write((&pgproto3.Execute{
				Portal:  "foobar",
				MaxRows: 42,
			}).Encode(nil)); err != nil {
				errChan <- err
				return
			}

			if _, err := clientW.Write((&pgproto3.Close{
				ObjectType: 'P',
			}).Encode(nil)); err != nil {
				errChan <- err
				return
			}
		}()

		// Server should receive messages in order.
		backend := pgproto3.NewBackend(pgproto3.NewChunkReader(serverR), serverR)

		msg, err := backend.Receive()
		require.NoError(t, err)
		m1, ok := msg.(*pgproto3.Query)
		require.True(t, ok)
		require.Equal(t, "SELECT 1", m1.String)

		msg, err = backend.Receive()
		require.NoError(t, err)
		m2, ok := msg.(*pgproto3.Execute)
		require.True(t, ok)
		require.Equal(t, "foobar", m2.Portal)
		require.Equal(t, uint32(42), m2.MaxRows)

		msg, err = backend.Receive()
		require.NoError(t, err)
		m3, ok := msg.(*pgproto3.Close)
		require.True(t, ok)
		require.Equal(t, byte('P'), m3.ObjectType)

		select {
		case err = <-errChan:
			t.Fatalf("require no error, but got %v", err)
		default:
		}
	})

	t.Run("server_to_client", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(bgCtx, 5*time.Second)
		defer cancel()

		clientW, clientR := net.Pipe()
		serverW, serverR := net.Pipe()
		// We don't close clientW and serverR here since we have no control
		// over those. serverW is not closed since the forwarder is responsible
		// for that.
		defer clientR.Close()

		f := forward(ctx, nil /* connector */, nil /* metrics */, clientR, serverW)
		defer f.Close()
		require.Nil(t, f.ctx.Err())

		// Server writes some pgwire messages.
		errChan := make(chan error, 1)
		go func() {
			if _, err := serverR.Write((&pgproto3.ErrorResponse{
				Code:    "100",
				Message: "foobarbaz",
			}).Encode(nil)); err != nil {
				errChan <- err
				return
			}

			if _, err := serverR.Write((&pgproto3.ReadyForQuery{
				TxStatus: 'I',
			}).Encode(nil)); err != nil {
				errChan <- err
				return
			}
		}()

		// Client should receive messages in order.
		frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientW), clientW)

		msg, err := frontend.Receive()
		require.NoError(t, err)
		m1, ok := msg.(*pgproto3.ErrorResponse)
		require.True(t, ok)
		require.Equal(t, "100", m1.Code)
		require.Equal(t, "foobarbaz", m1.Message)

		msg, err = frontend.Receive()
		require.NoError(t, err)
		m2, ok := msg.(*pgproto3.ReadyForQuery)
		require.True(t, ok)
		require.Equal(t, byte('I'), m2.TxStatus)

		select {
		case err = <-errChan:
			t.Fatalf("require no error, but got %v", err)
		default:
		}
	})
}

func TestForwarder_Close(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p1, p2 := net.Pipe()
	defer p1.Close() // p2 is owned by the forwarder.

	f := forward(context.Background(), nil /* connector */, nil /* metrics */, p1, p2)
	defer f.Close()
	require.Nil(t, f.ctx.Err())

	f.Close()
	require.EqualError(t, f.ctx.Err(), context.Canceled.Error())
}

func TestForwarder_RequestTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	metrics := makeProxyMetrics()
	f := &forwarder{metrics: &metrics}
	require.Equal(t, "", f.mu.transferKey)
	require.Nil(t, f.mu.transferCloserCh)
	require.Equal(t, stateReady, f.mu.state)

	f.RequestTransfer()
	require.Equal(t, int64(1), f.metrics.ConnMigrationRequestedCount.Count())

	key, closer, state := f.mu.transferKey, f.mu.transferCloserCh, f.mu.state

	// Call again to test idempotency.
	f.RequestTransfer()
	require.Equal(t, key, f.mu.transferKey)
	require.Equal(t, closer, f.mu.transferCloserCh)
	require.Equal(t, state, f.mu.state)
	require.Equal(t, int64(1), f.metrics.ConnMigrationRequestedCount.Count())
}

func TestForwarder_isSafeTransferPoint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name     string
		sent     pgwirebase.ClientMessageType
		ready    bool
		expected bool
	}{
		// Case 1.
		{"sync_with_ready", pgwirebase.ClientMsgSync, true, true},
		{"sync_without_ready", pgwirebase.ClientMsgSync, false, false},
		// Case 1.
		{"query_with_ready", pgwirebase.ClientMsgSimpleQuery, true, true},
		{"query_without_ready", pgwirebase.ClientMsgSimpleQuery, false, false},
		// Case 2.
		{"copy_done_with_ready", pgwirebase.ClientMsgCopyDone, true, true},
		{"copy_done_without_ready", pgwirebase.ClientMsgCopyDone, false, false},
		// Case 3.
		{"copy_fail_with_ready", pgwirebase.ClientMsgCopyFail, true, true},
		{"copy_fail_without_ready", pgwirebase.ClientMsgCopyFail, false, false},
		// Other.
		{"random_sent_with_ready", pgwirebase.ClientMsgExecute, true, false},
		{"random_sent_without_ready", pgwirebase.ClientMsgExecute, false, false},
		{"initial_state", clientMsgAny, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &forwarder{}
			f.clientMessageTypeSent = tc.sent
			f.mu.isServerMsgReadyReceived = tc.ready
			if tc.expected {
				require.True(t, f.isSafeTransferPoint())
			} else {
				require.False(t, f.isSafeTransferPoint())
			}
		})
	}
}

func TestForwarder_prepareAndFinishTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("successful", func(t *testing.T) {
		f := &forwarder{}
		require.Equal(t, "", f.mu.transferKey)
		require.Nil(t, f.mu.transferCloserCh)
		require.Equal(t, stateReady, f.mu.state)
		require.Nil(t, f.mu.transferCtx)
		require.False(t, f.mu.transferConnRecoverable)

		require.NoError(t, f.prepareTransfer())

		require.NotEqual(t, "", f.mu.transferKey)
		require.NotNil(t, f.mu.transferCloserCh)
		require.Equal(t, stateTransferRequested, f.mu.state)
		require.Nil(t, f.mu.transferCtx)
		require.False(t, f.mu.transferConnRecoverable)

		ch := f.mu.transferCloserCh
		select {
		case <-ch:
			t.Fatalf("transferCh is closed, which should not happen")
		default:
		}

		require.NoError(t, f.finishTransfer())

		require.Equal(t, "", f.mu.transferKey)
		require.Nil(t, f.mu.transferCloserCh)
		require.Equal(t, stateReady, f.mu.state)
		require.Nil(t, f.mu.transferCtx)
		require.False(t, f.mu.transferConnRecoverable)

		select {
		case <-ch:
		default:
			t.Fatalf("transferCh is still open")
		}
	})

	t.Run("error", func(t *testing.T) {
		f := &forwarder{}

		f.mu.state = stateTransferRequested
		require.EqualError(t, f.prepareTransfer(), "transfer is already in-progress")

		f.mu.state = stateReady
		require.EqualError(t, f.finishTransfer(), "no transfer in-progress")
	})
}

func TestForwarder_processTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	t.Run("transfer_response_error", func(t *testing.T) {
		f := &forwarder{}
		f.mu.transferKey = "foo-bar-baz"
		f.mu.transferCtx = ctx

		defer testutils.TestingHook(&waitForShowTransferState, func(
			fnCtx context.Context,
			serverInterceptor *interceptor.FrontendInterceptor,
			clientConn io.Writer,
			transferKey string,
		) (string, string, error) {
			require.Equal(t, ctx, fnCtx)
			require.Nil(t, serverInterceptor)
			require.Nil(t, clientConn)
			require.Equal(t, "foo-bar-baz", transferKey)
			return "", "", errors.New("bar")
		})()

		err := f.processTransfer()
		require.EqualError(t, err, "bar")
		require.False(t, isConnRecoverableError(err))
		require.False(t, f.mu.transferConnRecoverable)
	})

	t.Run("connection_error", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		f := &forwarder{
			connector: &connector{
				StartupMsg: &pgproto3.StartupMessage{
					Parameters: make(map[string]string),
				},
			},
		}
		f.setClientConn(client)
		f.setServerConn(server)

		f.mu.transferKey = "foo-bar-baz"
		f.mu.transferCtx = ctx

		defer testutils.TestingHook(&waitForShowTransferState, func(
			fnCtx context.Context,
			serverInterceptor *interceptor.FrontendInterceptor,
			clientConn io.Writer,
			transferKey string,
		) (string, string, error) {
			require.Equal(t, ctx, fnCtx)
			require.NotNil(t, serverInterceptor)
			require.Equal(t, client, clientConn)
			require.Equal(t, "foo-bar-baz", transferKey)
			return "state-string", "token-string", nil
		})()
		f.connector.testingKnobs.dialTenantCluster = func(ctx context.Context) (net.Conn, error) {
			return nil, errors.New("foo")
		}

		err := f.processTransfer()
		require.EqualError(t, err, "foo")
		require.True(t, isConnRecoverableError(err))
		require.True(t, f.mu.transferConnRecoverable)
	})

	t.Run("deserialization_error", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		f := &forwarder{
			connector: &connector{
				StartupMsg: &pgproto3.StartupMessage{
					Parameters: make(map[string]string),
				},
			},
		}
		f.setClientConn(client)
		f.setServerConn(server)

		f.mu.transferKey = "foo-bar-baz"
		f.mu.transferCtx = ctx

		defer testutils.TestingHook(&waitForShowTransferState, func(
			fnCtx context.Context,
			serverInterceptor *interceptor.FrontendInterceptor,
			clientConn io.Writer,
			transferKey string,
		) (string, string, error) {
			require.Equal(t, ctx, fnCtx)
			require.NotNil(t, serverInterceptor)
			require.Equal(t, client, clientConn)
			require.Equal(t, "foo-bar-baz", transferKey)
			return "state-string", "token-string", nil
		})()
		f.connector.testingKnobs.dialTenantCluster = func(ctx context.Context) (net.Conn, error) {
			str, ok := f.connector.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
			require.True(t, ok)
			require.Equal(t, "token-string", str)
			return server, nil
		}
		defer testutils.TestingHook(
			&implicitAuthenticate,
			func(serverConn net.Conn) error {
				return nil
			},
		)()
		defer testutils.TestingHook(&runAndWaitForDeserializeSession, func(
			fnCtx context.Context,
			serverConn io.Writer,
			serverInterceptor *interceptor.FrontendInterceptor,
			state string,
		) error {
			require.Equal(t, ctx, fnCtx)
			require.Equal(t, server, serverConn)
			require.NotNil(t, serverInterceptor)
			require.Equal(t, "state-string", state)
			return errors.New("bar")
		})()

		err := f.processTransfer()
		require.EqualError(t, err, "bar")
		require.True(t, isConnRecoverableError(err))
		require.True(t, f.mu.transferConnRecoverable)

		// Ensure that conn gets closed when we fail on deserialization so that
		// there won't be leaks.
		_, err = server.Write([]byte("foo"))
		require.Regexp(t, "closed pipe", err)
	})

	t.Run("successful", func(t *testing.T) {
		initClient, initServer := net.Pipe()
		defer initClient.Close()
		defer initServer.Close()

		newServer, _ := net.Pipe()
		defer newServer.Close()

		f := &forwarder{
			connector: &connector{
				StartupMsg: &pgproto3.StartupMessage{
					Parameters: make(map[string]string),
				},
			},
		}
		f.setClientConn(initClient)
		f.setServerConn(initServer)

		f.mu.transferKey = "foo-bar-baz"
		f.mu.transferCtx = ctx

		defer testutils.TestingHook(&waitForShowTransferState, func(
			fnCtx context.Context,
			serverInterceptor *interceptor.FrontendInterceptor,
			clientConn io.Writer,
			transferKey string,
		) (string, string, error) {
			require.Equal(t, ctx, fnCtx)
			require.NotNil(t, serverInterceptor)
			require.Equal(t, initClient, clientConn)
			require.Equal(t, "foo-bar-baz", transferKey)
			return "state-string", "token-string", nil
		})()
		f.connector.testingKnobs.dialTenantCluster = func(ctx context.Context) (net.Conn, error) {
			str, ok := f.connector.StartupMsg.Parameters[sessionRevivalTokenStartupParam]
			require.True(t, ok)
			require.Equal(t, "token-string", str)
			return newServer, nil
		}
		defer testutils.TestingHook(
			&implicitAuthenticate,
			func(serverConn net.Conn) error {
				return nil
			},
		)()
		defer testutils.TestingHook(&runAndWaitForDeserializeSession, func(
			fnCtx context.Context,
			serverConn io.Writer,
			serverInterceptor *interceptor.FrontendInterceptor,
			state string,
		) error {
			require.Equal(t, ctx, fnCtx)
			require.Equal(t, newServer, serverConn)
			require.NotNil(t, serverInterceptor)
			require.Equal(t, "state-string", state)
			return nil
		})()

		err := f.processTransfer()
		require.NoError(t, err)
		require.True(t, f.mu.transferConnRecoverable)

		// Ensure that old serverConn is closed.
		_, err = initServer.Write([]byte("foo"))
		require.Regexp(t, "closed pipe", err)
		require.Equal(t, initClient, f.clientConn)
		require.Equal(t, newServer, f.serverConn)
		require.NotNil(t, f.serverInterceptor)
	})
}

func TestForwarder_runTransferTimeoutHandler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	bgCtx := context.Background()

	t.Run("cancelled_externally", func(t *testing.T) {
		var started, finished int32
		ctx, cancel := context.WithCancel(bgCtx)
		defer cancel()
		errCh := make(chan error, 1)
		transferCh := make(chan struct{})

		f := &forwarder{
			ctx:       ctx,
			ctxCancel: cancel,
			errCh:     errCh,
		}
		f.mu.transferCloserCh = transferCh
		f.testingKnobs.onTransferTimeoutHandlerStart = func() {
			atomic.StoreInt32(&started, 1)
		}
		f.testingKnobs.onTransferTimeoutHandlerFinish = func() {
			atomic.StoreInt32(&finished, 1)
		}

		f.runTransferTimeoutHandler(2 * time.Second)
		f.mu.Lock()
		require.NotNil(t, f.mu.transferCtx)
		f.mu.Unlock()

		// Wait until the handler starts before cancelling.
		require.Eventually(t, func() bool {
			return atomic.LoadInt32(&started) == 1
		}, 2*time.Second, 50*time.Millisecond, "timed out waiting for timeout handler to run")

		cancel()

		require.Eventually(t, func() bool {
			return atomic.LoadInt32(&finished) == 1
		}, 2*time.Second, 50*time.Millisecond, "timed out waiting for timeout handler to return")

		select {
		case err := <-errCh:
			t.Fatalf("require no error, but got %v", err)
		default:
		}
	})

	t.Run("transfer_completed", func(t *testing.T) {
		var started, finished int32
		ctx, cancel := context.WithCancel(bgCtx)
		defer cancel()
		errCh := make(chan error, 1)
		transferCh := make(chan struct{})

		f := &forwarder{
			ctx:       ctx,
			ctxCancel: cancel,
			errCh:     errCh,
		}
		f.mu.transferCloserCh = transferCh
		f.testingKnobs.onTransferTimeoutHandlerStart = func() {
			atomic.StoreInt32(&started, 1)
		}
		f.testingKnobs.onTransferTimeoutHandlerFinish = func() {
			atomic.StoreInt32(&finished, 1)
		}

		f.runTransferTimeoutHandler(2 * time.Second)
		f.mu.Lock()
		require.NotNil(t, f.mu.transferCtx)
		f.mu.Unlock()

		// Wait until the handler starts before closing transferCh.
		require.Eventually(t, func() bool {
			return atomic.LoadInt32(&started) == 1
		}, 2*time.Second, 50*time.Millisecond, "timed out waiting for timeout handler to run")

		close(transferCh)

		require.Eventually(t, func() bool {
			return atomic.LoadInt32(&finished) == 1
		}, 2*time.Second, 50*time.Millisecond, "timed out waiting for timeout handler to return")

		select {
		case err := <-errCh:
			t.Fatalf("require no error, but got %v", err)
		default:
		}
		require.Nil(t, f.ctx.Err())
	})

	t.Run("timeout_with_non_recoverable_conn", func(t *testing.T) {
		w, _ := net.Pipe()
		defer w.Close()

		var finished int32
		ctx, cancel := context.WithCancel(bgCtx)
		defer cancel()
		errCh := make(chan error, 1)
		transferCh := make(chan struct{})

		metrics := makeProxyMetrics()
		f := &forwarder{
			ctx:        ctx,
			ctxCancel:  cancel,
			errCh:      errCh,
			serverConn: w,
			metrics:    &metrics,
		}
		f.mu.transferCloserCh = transferCh
		f.testingKnobs.onTransferTimeoutHandlerFinish = func() {
			atomic.StoreInt32(&finished, 1)
		}

		f.runTransferTimeoutHandler(500 * time.Millisecond)
		f.mu.Lock()
		require.NotNil(t, f.mu.transferCtx)
		f.mu.Unlock()

		// Wait until handler finishes, which will be triggered after 500ms.
		require.Eventually(t, func() bool {
			return atomic.LoadInt32(&finished) == 1
		}, 2*time.Second, 50*time.Millisecond, "timed out waiting for timeout handler to return")

		select {
		case err := <-errCh:
			require.EqualError(t, err, errTransferTimeout.Error())
		default:
			t.Fatalf("require error, but got none")
		}
		// Parent context is cancelled as well because we closed the forwarder.
		require.NotNil(t, f.ctx.Err())
		require.NotNil(t, f.mu.transferCtx.Err())
		require.Equal(t, int64(1), f.metrics.ConnMigrationTimeoutClosedCount.Count())
	})

	t.Run("timeout_with_recoverable_conn", func(t *testing.T) {
		w, _ := net.Pipe()
		defer w.Close()

		var finished int32
		ctx, cancel := context.WithCancel(bgCtx)
		defer cancel()
		errCh := make(chan error, 1)
		transferCh := make(chan struct{})

		metrics := makeProxyMetrics()
		f := &forwarder{
			ctx:        ctx,
			ctxCancel:  cancel,
			errCh:      errCh,
			serverConn: w,
			metrics:    &metrics,
		}
		f.mu.transferCloserCh = transferCh
		f.mu.transferConnRecoverable = true
		f.testingKnobs.onTransferTimeoutHandlerFinish = func() {
			atomic.StoreInt32(&finished, 1)
		}

		f.runTransferTimeoutHandler(500 * time.Millisecond)
		f.mu.Lock()
		require.NotNil(t, f.mu.transferCtx)
		f.mu.Unlock()

		// Wait until handler finishes, which will be triggered after 500ms.
		require.Eventually(t, func() bool {
			return atomic.LoadInt32(&finished) == 1
		}, 2*time.Second, 50*time.Millisecond, "timed out waiting for timeout handler to return")

		select {
		case err := <-errCh:
			t.Fatalf("require no error, but got %v", err)
		default:
		}
		// Parent context should not be cancelled in a recoverable conn case.
		require.Nil(t, f.ctx.Err())
		require.NotNil(t, f.mu.transferCtx.Err())
		require.Equal(t, int64(1), f.metrics.ConnMigrationTimeoutRecoverableCount.Count())
	})
}

func TestForwarder_setClientConn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	f := &forwarder{serverConn: nil, serverInterceptor: nil}

	w, r := net.Pipe()
	defer w.Close()
	defer r.Close()

	f.setClientConn(r)
	require.Equal(t, r, f.clientConn)

	dst := new(bytes.Buffer)
	errChan := make(chan error, 1)
	go func() {
		_, err := f.clientInterceptor.ForwardMsg(dst)
		errChan <- err
	}()

	_, err := w.Write((&pgproto3.Query{String: "SELECT 1"}).Encode(nil))
	require.NoError(t, err)

	// Block until message has been forwarded. This checks that we are creating
	// our interceptor properly.
	err = <-errChan
	require.NoError(t, err)
	require.Equal(t, 14, dst.Len())
}

func TestForwarder_setServerConn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	f := &forwarder{serverConn: nil, serverInterceptor: nil}

	w, r := net.Pipe()
	defer w.Close()
	defer r.Close()

	f.setServerConn(r)
	require.Equal(t, r, f.serverConn)
	require.NotNil(t, f.serverInterceptor)
	require.Equal(t, clientMsgAny, f.clientMessageTypeSent)
	require.True(t, f.mu.isServerMsgReadyReceived)

	dst := new(bytes.Buffer)
	errChan := make(chan error, 1)
	go func() {
		_, err := f.serverInterceptor.ForwardMsg(dst)
		errChan <- err
	}()

	_, err := w.Write((&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(nil))
	require.NoError(t, err)

	// Block until message has been forwarded. This checks that we are creating
	// our interceptor properly.
	err = <-errChan
	require.NoError(t, err)
	require.Equal(t, 6, dst.Len())
}

func TestForwarder_setServerConnAndInterceptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	f := &forwarder{serverConn: nil, serverInterceptor: nil}

	_, r1 := net.Pipe()
	defer r1.Close()

	w2, r2 := net.Pipe()
	defer w2.Close()
	defer r2.Close()

	// Use a different interceptor from the one for r1.
	fi := interceptor.NewFrontendInterceptor(r2)

	f.setServerConnAndInterceptor(r1, fi)
	require.Equal(t, r1, f.serverConn)
	require.Equal(t, fi, f.serverInterceptor)
	require.Equal(t, clientMsgAny, f.clientMessageTypeSent)
	require.True(t, f.mu.isServerMsgReadyReceived)

	dst := new(bytes.Buffer)
	errChan := make(chan error, 1)
	go func() {
		_, err := f.serverInterceptor.ForwardMsg(dst)
		errChan <- err
	}()

	_, err := w2.Write((&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(nil))
	require.NoError(t, err)

	// Block until message has been forwarded. This checks that we are using
	// the right interceptor.
	err = <-errChan
	require.NoError(t, err)
	require.Equal(t, 6, dst.Len())
}

func TestWrapClientToServerError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		input  error
		output error
	}{
		// Nil errors.
		{nil, nil},
		{context.Canceled, nil},
		{context.DeadlineExceeded, nil},
		{errors.Mark(errors.New("foo"), context.Canceled), nil},
		{errors.Wrap(context.DeadlineExceeded, "foo"), nil},
		// Forwarding errors.
		{errors.New("foo"), newErrorf(
			codeClientDisconnected,
			"copying from client to target server: foo",
		)},
	} {
		err := wrapClientToServerError(tc.input)
		if tc.output == nil {
			require.NoError(t, err)
		} else {
			require.EqualError(t, err, tc.output.Error())
		}
	}
}

func TestWrapServerToClientError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		input  error
		output error
	}{
		// Nil errors.
		{nil, nil},
		{context.Canceled, nil},
		{context.DeadlineExceeded, nil},
		{errors.Mark(errors.New("foo"), context.Canceled), nil},
		{errors.Wrap(context.DeadlineExceeded, "foo"), nil},
		// Forwarding errors.
		{errors.New("foo"), newErrorf(
			codeBackendDisconnected,
			"copying from target server to client: foo",
		)},
	} {
		err := wrapServerToClientError(tc.input)
		if tc.output == nil {
			require.NoError(t, err)
		} else {
			require.EqualError(t, err, tc.output.Error())
		}
	}
}

func TestConnectionRecoverableError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	err := errors.New("foobar")
	require.False(t, isConnRecoverableError(err))
	err = markAsConnRecoverableError(err)
	require.True(t, isConnRecoverableError(err))
	require.True(t, errors.Is(err, errConnRecoverableSentinel))
}
