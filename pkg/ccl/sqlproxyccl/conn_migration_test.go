// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlproxyccl

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/balancer"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

func TestTransferContext(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	t.Run("new_context", func(t *testing.T) {
		ctx, cancel := newTransferContext(context.Background())
		defer cancel()

		require.True(t, ctx.isRecoverable())

		ctx.markRecoverable(false)
		require.False(t, ctx.isRecoverable())

		ctx.markRecoverable(true)
		require.True(t, ctx.isRecoverable())
	})

	t.Run("timeout", func(t *testing.T) {
		defer testutils.TestingHook(&defaultTransferTimeout, 2*time.Second)()

		ctx, cancel := newTransferContext(context.Background())
		defer cancel()

		require.Eventually(t, func() bool {
			return ctx.Err() != nil
		}, 5*time.Second, 100*time.Millisecond)
		require.True(t, ctx.isRecoverable())
	})
}

func TestForwarder_tryBeginTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	t.Run("not_initialized", func(t *testing.T) {
		defer testutils.TestingHook(&isSafeTransferPointLocked,
			func(req *processor, res *processor) bool {
				return false
			},
		)()

		f := &forwarder{}

		started, cleanupFn := f.tryBeginTransfer()
		require.False(t, started)
		require.Nil(t, cleanupFn)
	})

	t.Run("isTransferring=true", func(t *testing.T) {
		f := &forwarder{}
		f.mu.isTransferring = true

		started, cleanupFn := f.tryBeginTransfer()
		require.False(t, started)
		require.Nil(t, cleanupFn)
	})

	t.Run("isSafeTransferPointLocked=false", func(t *testing.T) {
		defer testutils.TestingHook(&isSafeTransferPointLocked,
			func(req *processor, res *processor) bool {
				return false
			},
		)()

		f := &forwarder{}
		f.mu.request = &processor{}
		f.mu.response = &processor{}
		f.mu.isInitialized = true

		started, cleanupFn := f.tryBeginTransfer()
		require.False(t, started)
		require.Nil(t, cleanupFn)
	})

	t.Run("successful", func(t *testing.T) {
		defer testutils.TestingHook(&isSafeTransferPointLocked,
			func(req *processor, res *processor) bool {
				return true
			},
		)()

		f := &forwarder{}
		f.mu.request = &processor{}
		f.mu.response = &processor{}
		f.mu.isInitialized = true

		started, cleanupFn := f.tryBeginTransfer()
		require.True(t, started)
		require.NotNil(t, cleanupFn)

		require.True(t, f.mu.isTransferring)
		cleanupFn()
		require.False(t, f.mu.isTransferring)
	})
}

func TestTransferConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	p1, p2 := net.Pipe()
	defer p1.Close()
	defer p2.Close()

	t.Run("context_cancelled", func(t *testing.T) {
		ctx, cancel := newTransferContext(context.Background())
		cancel()

		conn, err := transferConnection(ctx, nil, nil, nil, nil, nil)
		require.EqualError(t, err, context.Canceled.Error())
		require.Nil(t, conn)
		require.True(t, ctx.isRecoverable())
	})

	t.Run("transfer_request_failed", func(t *testing.T) {
		ctx, cancel := newTransferContext(context.Background())
		defer cancel()

		defer testutils.TestingHook(&runShowTransferState,
			func(w io.Writer, transferKey string) error {
				require.NotNil(t, w)
				require.NotEqual(t, "", transferKey)
				return errors.New("foo")
			},
		)()

		conn, err := transferConnection(
			ctx,
			nil,
			nil,
			nil,
			interceptor.NewPGConn(p1),
			interceptor.NewPGConn(p2),
		)
		require.Regexp(t, "foo", err)
		require.Nil(t, conn)
		require.False(t, ctx.isRecoverable())
	})

	t.Run("wait_transfer_response_failed", func(t *testing.T) {
		ctx, cancel := newTransferContext(context.Background())
		defer cancel()

		defer testutils.TestingHook(&runShowTransferState,
			func(w io.Writer, transferKey string) error {
				require.NotNil(t, w)
				require.NotEqual(t, "", transferKey)
				return nil
			},
		)()

		defer testutils.TestingHook(&waitForShowTransferState,
			func(
				tCtx context.Context,
				serverConn *interceptor.FrontendConn,
				clientConn io.Writer,
				transferKey string,
				_ *metrics,
			) (string, string, string, error) {
				require.Equal(t, ctx, tCtx)
				require.NotNil(t, serverConn)
				require.NotNil(t, clientConn)
				require.NotEqual(t, "", transferKey)
				return "", "", "", errors.New("foobar")
			},
		)()

		conn, err := transferConnection(
			ctx,
			nil,
			nil,
			nil,
			interceptor.NewPGConn(p1),
			interceptor.NewPGConn(p2),
		)
		require.Regexp(t, "foobar", err)
		require.Nil(t, conn)
		require.False(t, ctx.isRecoverable())
	})

	t.Run("transfer_state_error", func(t *testing.T) {
		ctx, cancel := newTransferContext(context.Background())
		defer cancel()

		defer testutils.TestingHook(&runShowTransferState,
			func(w io.Writer, transferKey string) error {
				require.NotNil(t, w)
				require.NotEqual(t, "", transferKey)
				return nil
			},
		)()

		defer testutils.TestingHook(&waitForShowTransferState,
			func(
				tCtx context.Context,
				serverConn *interceptor.FrontendConn,
				clientConn io.Writer,
				transferKey string,
				_ *metrics,
			) (string, string, string, error) {
				require.Equal(t, ctx, tCtx)
				require.NotNil(t, serverConn)
				require.NotNil(t, clientConn)
				require.NotEqual(t, "", transferKey)
				return "foobaz", "", "", nil
			},
		)()

		conn, err := transferConnection(
			ctx,
			nil,
			nil,
			nil,
			interceptor.NewPGConn(p1),
			interceptor.NewPGConn(p2),
		)
		require.Regexp(t, "foobaz", err)
		require.Nil(t, conn)
		require.True(t, ctx.isRecoverable())
	})

	t.Run("connector_error", func(t *testing.T) {
		ctx, cancel := newTransferContext(context.Background())
		defer cancel()

		defer testutils.TestingHook(&runShowTransferState,
			func(w io.Writer, transferKey string) error {
				require.NotNil(t, w)
				require.NotEqual(t, "", transferKey)
				return nil
			},
		)()

		defer testutils.TestingHook(&waitForShowTransferState,
			func(
				tCtx context.Context,
				serverConn *interceptor.FrontendConn,
				clientConn io.Writer,
				transferKey string,
				_ *metrics,
			) (string, string, string, error) {
				require.Equal(t, ctx, tCtx)
				require.NotNil(t, serverConn)
				require.NotNil(t, clientConn)
				require.NotEqual(t, "", transferKey)
				return "", "state", "token", nil
			},
		)()

		defer testutils.TestingHook(&transferConnectionConnectorTestHook,
			func(
				tCtx context.Context,
				requestor balancer.ConnectionHandle,
				token string,
			) (net.Conn, error) {
				require.Equal(t, ctx, tCtx)
				require.Equal(t, "token", token)
				return nil, errors.New("foobarbaz")
			},
		)()

		conn, err := transferConnection(
			ctx,
			nil,
			&connector{},
			nil,
			interceptor.NewPGConn(p1),
			interceptor.NewPGConn(p2),
		)
		require.Regexp(t, "foobarbaz", err)
		require.Nil(t, conn)
		require.True(t, ctx.isRecoverable())
	})

	t.Run("deserialization_error", func(t *testing.T) {
		ctx, cancel := newTransferContext(context.Background())
		defer cancel()

		defer testutils.TestingHook(&runShowTransferState,
			func(w io.Writer, transferKey string) error {
				require.NotNil(t, w)
				require.NotEqual(t, "", transferKey)
				return nil
			},
		)()

		defer testutils.TestingHook(&waitForShowTransferState,
			func(
				tCtx context.Context,
				serverConn *interceptor.FrontendConn,
				clientConn io.Writer,
				transferKey string,
				_ *metrics,
			) (string, string, string, error) {
				require.Equal(t, ctx, tCtx)
				require.NotNil(t, serverConn)
				require.NotNil(t, clientConn)
				require.NotEqual(t, "", transferKey)
				return "", "state", "token", nil
			},
		)()

		netConn, _ := net.Pipe()
		defer netConn.Close()

		defer testutils.TestingHook(&transferConnectionConnectorTestHook,
			func(
				tCtx context.Context,
				requestor balancer.ConnectionHandle,
				token string,
			) (net.Conn, error) {
				require.Equal(t, ctx, tCtx)
				require.Equal(t, "token", token)
				return netConn, nil
			},
		)()

		defer testutils.TestingHook(&runAndWaitForDeserializeSession,
			func(
				tCtx context.Context,
				serverConn *interceptor.FrontendConn,
				state string,
			) error {
				require.Equal(t, ctx, tCtx)
				require.NotNil(t, serverConn)
				require.Equal(t, "state", state)
				return errors.New("foobar")
			},
		)()

		conn, err := transferConnection(
			ctx,
			nil,
			&connector{},
			nil,
			interceptor.NewPGConn(p1),
			interceptor.NewPGConn(p2),
		)
		require.Regexp(t, "foobar", err)
		require.Nil(t, conn)
		require.True(t, ctx.isRecoverable())

		// netConn should be closed.
		_, err = netConn.Write([]byte("foobarbaz"))
		require.Regexp(t, "closed pipe", err)
	})

	t.Run("successful", func(t *testing.T) {
		ctx, cancel := newTransferContext(context.Background())
		defer cancel()

		defer testutils.TestingHook(&runShowTransferState,
			func(w io.Writer, transferKey string) error {
				require.NotNil(t, w)
				require.NotEqual(t, "", transferKey)
				return nil
			},
		)()

		defer testutils.TestingHook(&waitForShowTransferState,
			func(
				tCtx context.Context,
				serverConn *interceptor.FrontendConn,
				clientConn io.Writer,
				transferKey string,
				_ *metrics,
			) (string, string, string, error) {
				require.Equal(t, ctx, tCtx)
				require.NotNil(t, serverConn)
				require.NotNil(t, clientConn)
				require.NotEqual(t, "", transferKey)
				return "", "state", "token", nil
			},
		)()

		netConn, _ := net.Pipe()
		defer netConn.Close()

		defer testutils.TestingHook(&transferConnectionConnectorTestHook,
			func(
				tCtx context.Context,
				requestor balancer.ConnectionHandle,
				token string,
			) (net.Conn, error) {
				require.Equal(t, ctx, tCtx)
				require.Equal(t, "token", token)
				return netConn, nil
			},
		)()

		defer testutils.TestingHook(&runAndWaitForDeserializeSession,
			func(
				tCtx context.Context,
				serverConn *interceptor.FrontendConn,
				state string,
			) error {
				require.Equal(t, ctx, tCtx)
				require.NotNil(t, serverConn)
				require.Equal(t, "state", state)
				return nil
			},
		)()

		conn, err := transferConnection(
			ctx,
			nil,
			&connector{},
			nil,
			interceptor.NewPGConn(p1),
			interceptor.NewPGConn(p2),
		)
		require.NoError(t, err)
		require.NotNil(t, conn)
		require.True(t, ctx.isRecoverable())
	})
}

func TestIsSafeTransferPointLocked(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	makeProc := func(typ byte, transferredAt int) *processor {
		p := &processor{}
		p.mu.lastMessageType = typ
		p.mu.lastMessageTransferredAt = uint64(transferredAt)
		return p
	}

	for _, tc := range []struct {
		name             string
		reqType          pgwirebase.ClientMessageType
		resType          pgwirebase.ServerMessageType
		reqTransferredAt int
		resTransferredAt int
		expected         bool
	}{
		// Case 1.
		{"sync_with_ready", pgwirebase.ClientMsgSync, pgwirebase.ServerMsgReady, 1, 2, true},
		{"sync_without_ready", pgwirebase.ClientMsgSync, pgwirebase.ServerMsgReady, 2, 1, false},
		{"query_with_ready", pgwirebase.ClientMsgSimpleQuery, pgwirebase.ServerMsgReady, 1, 2, true},
		{"query_without_ready", pgwirebase.ClientMsgSimpleQuery, pgwirebase.ServerMsgReady, 2, 1, false},
		// Case 2.
		{"copy_done_with_ready", pgwirebase.ClientMsgCopyDone, pgwirebase.ServerMsgReady, 1, 2, true},
		{"copy_done_without_ready", pgwirebase.ClientMsgCopyDone, pgwirebase.ServerMsgReady, 2, 1, false},
		// Case 3.
		{"copy_fail_with_ready", pgwirebase.ClientMsgCopyFail, pgwirebase.ServerMsgReady, 1, 2, true},
		{"copy_fail_without_ready", pgwirebase.ClientMsgCopyFail, pgwirebase.ServerMsgReady, 2, 1, false},
		// Other.
		{"initial_state", pgwirebase.ClientMessageType(0), pgwirebase.ServerMessageType(0), 0, 0, true},
		{"random_sent_with_ready", pgwirebase.ClientMsgExecute, pgwirebase.ServerMsgReady, 1, 2, false},
		{"random_sent_without_ready", pgwirebase.ClientMsgExecute, pgwirebase.ServerMsgNoData, 1, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := makeProc(byte(tc.reqType), tc.reqTransferredAt)
			res := makeProc(byte(tc.resType), tc.resTransferredAt)
			safe := isSafeTransferPointLocked(req, res)
			if tc.expected {
				require.True(t, safe)
			} else {
				require.False(t, safe)
			}
		})
	}
}

func TestRunShowTransferState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	t.Run("successful", func(t *testing.T) {
		buf := new(bytes.Buffer)
		err := runShowTransferState(buf, "foo-bar-baz")
		require.NoError(t, err)

		backend := pgproto3.NewBackend(pgproto3.NewChunkReader(buf), buf)
		msg, err := backend.Receive()
		require.NoError(t, err)
		m, ok := msg.(*pgproto3.Query)
		require.True(t, ok)
		require.Equal(t, "SHOW TRANSFER STATE WITH 'foo-bar-baz'", m.String)
	})

	t.Run("error", func(t *testing.T) {
		err := runShowTransferState(&errWriter{}, "foo")
		require.Regexp(t, "unexpected Write call", err)
	})
}

func TestWaitForShowTransferState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	ctx := context.Background()

	t.Run("context_cancelled", func(t *testing.T) {
		tCtx, cancel := context.WithCancel(ctx)
		cancel()

		transferErr, state, token, err := waitForShowTransferState(tCtx, nil, nil, "", nil)
		require.True(t, errors.Is(err, context.Canceled))
		require.Equal(t, "", transferErr)
		require.Equal(t, "", state)
		require.Equal(t, "", token)
	})

	transferStateDataRow := &pgproto3.DataRow{
		Values: [][]byte{
			{},
			[]byte("foo-state"),
			[]byte("foo-token"),
			[]byte("foo-transfer-key"),
		},
	}

	for _, tc := range []struct {
		name         string
		sendSequence []pgproto3.BackendMessage
		postValidate func(*testing.T, <-chan pgproto3.BackendMessage)
		err          string
		transferErr  string
	}{
		{
			// All irrelevant messages are forwarded to the client. This returns
			// an error when we see ErrorResponse.
			name: "RowDescription/candidate_mismatch",
			sendSequence: []pgproto3.BackendMessage{
				// not RowDescription.
				&pgproto3.BackendKeyData{},
				// Too large (> 4k).
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("foo1")},
						{Name: make([]byte, 1<<13 /* 8K */)},
					},
				},
				// Invalid number of columns.
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("foo2")},
					},
				},
				// Invalid column names.
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("error")},
						{Name: []byte("session_state_foo")},
						{Name: []byte("session_revival_token_bar")},
						{Name: []byte("apple")},
					},
				},
				&pgproto3.ErrorResponse{},
			},
			err: "ambiguous ErrorResponse",
			postValidate: func(t *testing.T, msgCh <-chan pgproto3.BackendMessage) {
				t.Helper()
				expectMsg(t, msgCh, `"Type":"BackendKeyData"`)
				expectMsg(t, msgCh, `"Type":"RowDescription".*"Name":"foo1"`)
				expectMsg(t, msgCh, `"Type":"RowDescription".*"Name":"foo2"`)
				expectMsg(t, msgCh, `"Type":"RowDescription".*session_state_foo.*session_revival_token_bar`)
			},
		},
		{
			name: "DataRow/type_mismatch",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("error")},
						{Name: []byte("session_state_base64")},
						{Name: []byte("session_revival_token_base64")},
						{Name: []byte("transfer_key")},
					},
				},
				&pgproto3.ReadyForQuery{},
			},
			err: `DataRow: unexpected message:.*"Type":"ReadyForQuery"`,
		},
		{
			name: "DataRow/invalid_response",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("error")},
						{Name: []byte("session_state_base64")},
						{Name: []byte("session_revival_token_base64")},
						{Name: []byte("transfer_key")},
					},
				},
				&pgproto3.DataRow{
					// 3 columns instead of 4.
					Values: [][]byte{
						{},
						{},
						[]byte("bar"),
					},
				},
			},
			err: "DataRow: validation failed for message",
		},
		{
			name: "DataRow/invalid_transfer_key",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("error")},
						{Name: []byte("session_state_base64")},
						{Name: []byte("session_revival_token_base64")},
						{Name: []byte("transfer_key")},
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						{},
						{},
						{},
						[]byte("bar"),
					},
				},
			},
			err: "DataRow: validation failed for message",
		},
		{
			name: "CommandComplete/type_mismatch",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("error")},
						{Name: []byte("session_state_base64")},
						{Name: []byte("session_revival_token_base64")},
						{Name: []byte("transfer_key")},
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						{},
						{},
						{},
						[]byte("foo-transfer-key"),
					},
				},
				&pgproto3.ReadyForQuery{},
			},
			err: `CommandComplete: unexpected message:.*"Type":"ReadyForQuery"`,
		},
		{
			name: "CommandComplete/value_mismatch",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("error")},
						{Name: []byte("session_state_base64")},
						{Name: []byte("session_revival_token_base64")},
						{Name: []byte("transfer_key")},
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						{},
						{},
						{},
						[]byte("foo-transfer-key"),
					},
				},
				&pgproto3.CommandComplete{CommandTag: []byte("SHOW TRANSFER STATE 2")},
			},
			err: `CommandComplete: unexpected message:.*"Type":"CommandComplete".*"CommandTag":"SHOW TRANSFER STATE 2"`,
		},
		{
			name: "ReadyForQuery/type_mismatch",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("error")},
						{Name: []byte("session_state_base64")},
						{Name: []byte("session_revival_token_base64")},
						{Name: []byte("transfer_key")},
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						{},
						{},
						{},
						[]byte("foo-transfer-key"),
					},
				},
				&pgproto3.CommandComplete{CommandTag: []byte("SHOW TRANSFER STATE 1")},
				&pgproto3.CommandComplete{},
			},
			err: `ReadyForQuery: unexpected message:.*"Type":"CommandComplete"`,
		},
		{
			// This should be a common case with open transactions.
			name: "transfer_state_error",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("error")},
						{Name: []byte("session_state_base64")},
						{Name: []byte("session_revival_token_base64")},
						{Name: []byte("transfer_key")},
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("serialization error"),
						{},
						{},
						[]byte("foo-transfer-key"),
					},
				},
				&pgproto3.CommandComplete{CommandTag: []byte("SHOW TRANSFER STATE 1")},
				&pgproto3.ReadyForQuery{},
			},
			transferErr: "serialization error",
		},
		{
			name: "successful",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.BackendKeyData{},
				&pgproto3.RowDescription{},
				&pgproto3.CommandComplete{},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("error")},
						{Name: []byte("session_state_base64")},
						{Name: []byte("session_revival_token_base64")},
						{Name: []byte("transfer_key")},
					},
				},
				transferStateDataRow,
				&pgproto3.CommandComplete{CommandTag: []byte("SHOW TRANSFER STATE 1")},
				&pgproto3.ReadyForQuery{},
			},
			postValidate: func(t *testing.T, msgCh <-chan pgproto3.BackendMessage) {
				t.Helper()
				expectMsg(t, msgCh, `"Type":"BackendKeyData"`)
				expectMsg(t, msgCh, `"Type":"RowDescription"`)
				expectMsg(t, msgCh, `"Type":"CommandComplete"`)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serverProxy, server := net.Pipe()
			defer serverProxy.Close()
			defer server.Close()

			clientProxy, client := net.Pipe()
			defer clientProxy.Close()
			defer client.Close()

			doneCh := make(chan struct{})
			go func(sequence []pgproto3.BackendMessage) {
				for _, m := range sequence {
					writeServerMsg(server, m)
				}
				close(doneCh)
			}(tc.sendSequence)

			msgCh := make(chan pgproto3.BackendMessage, 10)
			go func() {
				fi := interceptor.NewFrontendConn(client)
				for {
					msg, err := fi.ReadMsg()
					if err != nil {
						return
					}
					msgCh <- msg
				}
			}()

			transferErr, state, token, err := waitForShowTransferState(
				ctx,
				interceptor.NewFrontendConn(serverProxy),
				clientProxy,
				"foo-transfer-key",
				nil,
			)
			if tc.err == "" {
				require.NoError(t, err)
				if tc.transferErr != "" {
					require.Equal(t, tc.transferErr, transferErr)
				} else {
					require.Equal(t, "", transferErr)
					require.Equal(t, "foo-state", state)
					require.Equal(t, "foo-token", token)

					// Ensure that returned strings are copied. Alternatively,
					// we could also check pointers using encoding.UnsafeConvertStringToBytes.
					transferStateDataRow.Values[0] = []byte("x")
					transferStateDataRow.Values[1][1] = '-'
					transferStateDataRow.Values[2][1] = '-'
					require.Equal(t, "", transferErr)
					require.Equal(t, "foo-state", state)
					require.Equal(t, "foo-token", token)
				}
				require.Eventually(t, func() bool {
					select {
					case <-doneCh:
						return true
					default:
						return false
					}
				}, 5*time.Second, 100*time.Millisecond, "require doneCh to be closed")
			} else {
				require.Regexp(t, tc.err, err)
			}

			// Verify that forwarding was correct.
			if tc.postValidate != nil {
				tc.postValidate(t, msgCh)
			}
		})
	}
}

func TestRunAndWaitForDeserializeSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	ctx := context.Background()

	t.Run("write_failed", func(t *testing.T) {
		r, w := net.Pipe()
		r.Close()
		w.Close()
		err := runAndWaitForDeserializeSession(ctx,
			interceptor.NewFrontendConn(r), "foo")
		require.Regexp(t, "closed pipe", err)
	})

	for _, tc := range []struct {
		name         string
		sendSequence []pgproto3.BackendMessage
		err          string
	}{
		{
			name: "RowDescription/type_mismatch",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.BackendKeyData{},
			},
			err: "RowDescription: forwarding message: unexpected Write call",
		},
		{
			name: "RowDescription/column_mismatch/length",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.RowDescription{},
			},
			err: "RowDescription: writing message: unexpected Write call",
		},
		{
			name: "RowDescription/column_mismatch/name",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{{Name: []byte("bar")}}},
			},
			err: "RowDescription: writing message: unexpected Write call",
		},
		{
			name: "DataRow/type_mismatch",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("crdb_internal.deserialize_session")},
					},
				},
				&pgproto3.ReadyForQuery{},
			},
			err: `DataRow: unexpected message:.*"Type":"ReadyForQuery"`,
		},
		{
			name: "DataRow/column_mismatch/length",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("crdb_internal.deserialize_session")},
					},
				},
				&pgproto3.DataRow{},
			},
			err: `DataRow: validation failed for message`,
		},
		{
			name: "DataRow/column_mismatch/value",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("crdb_internal.deserialize_session")},
					},
				},
				&pgproto3.DataRow{Values: [][]byte{[]byte("temp")}},
			},
			err: "DataRow: validation failed for message",
		},
		{
			name: "CommandComplete/type_mismatch",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("crdb_internal.deserialize_session")},
					},
				},
				&pgproto3.DataRow{Values: [][]byte{[]byte("t")}},
				&pgproto3.ReadyForQuery{},
			},
			err: `CommandComplete: unexpected message:.*"Type":"ReadyForQuery"`,
		},
		{
			name: "CommandComplete/value_mismatch",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("crdb_internal.deserialize_session")},
					},
				},
				&pgproto3.DataRow{Values: [][]byte{[]byte("t")}},
				&pgproto3.CommandComplete{CommandTag: []byte("SELECT 2")},
			},
			err: `CommandComplete: unexpected message:.*"Type":"CommandComplete".*"CommandTag":"SELECT 2"`,
		},
		{
			name: "ReadyForQuery/type_mismatch",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("crdb_internal.deserialize_session")},
					},
				},
				&pgproto3.DataRow{Values: [][]byte{[]byte("t")}},
				&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
				&pgproto3.CommandComplete{},
			},
			err: `ReadyForQuery: unexpected message:.*"Type":"CommandComplete"`,
		},
		{
			name: "successful",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("crdb_internal.deserialize_session")},
					},
				},
				&pgproto3.DataRow{Values: [][]byte{[]byte("t")}},
				&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
				&pgproto3.ReadyForQuery{},
			},
			err: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serverProxy, server := net.Pipe()
			defer serverProxy.Close()
			defer server.Close()
			doneCh := make(chan struct{})
			go func(sequence []pgproto3.BackendMessage) {
				for _, m := range sequence {
					writeServerMsg(server, m)
				}
				close(doneCh)
			}(tc.sendSequence)

			msgCh := make(chan pgproto3.FrontendMessage, 1)
			go func() {
				backend := interceptor.NewBackendConn(server)
				msg, _ := backend.ReadMsg()
				msgCh <- msg
			}()

			err := runAndWaitForDeserializeSession(ctx,
				interceptor.NewFrontendConn(serverProxy), "foo-transfer-key")
			if tc.err == "" {
				require.NoError(t, err)
			} else {
				require.Regexp(t, tc.err, err)
			}

			require.Eventually(t, func() bool {
				select {
				case <-doneCh:
					return true
				default:
					return false
				}
			}, 5*time.Second, 100*time.Millisecond, "require doneCh to be closed")

			msg := <-msgCh
			m, ok := msg.(*pgproto3.Query)
			require.True(t, ok)
			const queryStr = "SELECT crdb_internal.deserialize_session(decode('foo-transfer-key', 'base64'))"
			require.Equal(t, queryStr, m.String)
		})
	}
}

func TestWaitForSmallRowDescription(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	ctx := context.Background()

	t.Run("context_cancelled", func(t *testing.T) {
		tCtx, cancel := context.WithCancel(ctx)
		cancel()

		err := waitForSmallRowDescription(tCtx, nil, nil, nil)
		require.EqualError(t, err, context.Canceled.Error())
	})

	t.Run("peek_error", func(t *testing.T) {
		r, w := net.Pipe()
		r.Close()
		w.Close()

		err := waitForSmallRowDescription(ctx, interceptor.NewFrontendConn(r), nil, nil)
		require.Regexp(t, "peeking message", err)
	})

	t.Run("ambiguous_error", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.ErrorResponse{})
		}()

		err := waitForSmallRowDescription(ctx, interceptor.NewFrontendConn(r), nil, nil)
		require.Regexp(t, "ambiguous ErrorResponse.*ErrorResponse", err)
	})

	t.Run("successful", func(t *testing.T) {
		serverProxy, server := net.Pipe()
		defer serverProxy.Close()
		defer server.Close()

		clientProxy, client := net.Pipe()
		defer clientProxy.Close()
		defer client.Close()

		go func() {
			// Not RowDescription.
			writeServerMsg(server, &pgproto3.BackendKeyData{ProcessID: 42})
			// Too large (> 4k bytes).
			writeServerMsg(server, &pgproto3.RowDescription{
				Fields: []pgproto3.FieldDescription{
					{Name: []byte("foo1")},
					{Name: make([]byte, 1<<13 /* 8K */)},
				},
			})
			// Mismatch.
			writeServerMsg(server, &pgproto3.RowDescription{
				Fields: []pgproto3.FieldDescription{
					{Name: []byte("foo2")},
					{Name: []byte("foo3")},
				},
			})
			// Match.
			writeServerMsg(server, &pgproto3.RowDescription{
				Fields: []pgproto3.FieldDescription{
					{Name: []byte("foo1")},
				},
			})
		}()

		msgCh := make(chan pgproto3.BackendMessage, 10)
		go func() {
			fi := interceptor.NewFrontendConn(client)
			for {
				msg, err := fi.ReadMsg()
				if err != nil {
					return
				}
				msgCh <- msg
			}
		}()

		err := waitForSmallRowDescription(
			ctx,
			interceptor.NewFrontendConn(serverProxy),
			clientProxy,
			func(m *pgproto3.RowDescription) bool {
				return len(m.Fields) == 1 && string(m.Fields[0].Name) == "foo1"
			},
		)
		require.Nil(t, err)

		// Verify that forwarding was correct.
		expectMsg(t, msgCh, `"Type":"BackendKeyData".*"ProcessID":42`)
		expectMsg(t, msgCh, `"Type":"RowDescription".*"Name":"foo1"`)
		expectMsg(t, msgCh, `"Type":"RowDescription".*"Name":"foo2".*"Name":"foo3"`)
	})
}

func TestExpectDataRow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	ctx := context.Background()

	falseValidateFn := func(m *pgproto3.DataRow, s int) (bool, error) { return false, nil }

	t.Run("context_cancelled", func(t *testing.T) {
		tCtx, cancel := context.WithCancel(ctx)
		cancel()

		err := expectDataRow(tCtx, nil, falseValidateFn)
		require.EqualError(t, err, context.Canceled.Error())
	})

	t.Run("read_error", func(t *testing.T) {
		r, w := net.Pipe()
		r.Close()
		w.Close()

		err := expectDataRow(ctx, interceptor.NewFrontendConn(r), falseValidateFn)
		require.Regexp(t, "peeking message", err)
	})

	t.Run("type_mismatch", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.ReadyForQuery{})
		}()

		err := expectDataRow(ctx, interceptor.NewFrontendConn(r), falseValidateFn)
		require.Regexp(t, "unexpected message.*ReadyForQuery", err)
	})

	t.Run("validation_failed", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.DataRow{})
		}()

		err := expectDataRow(ctx, interceptor.NewFrontendConn(r), falseValidateFn)
		require.Regexp(t, "validation failed for message.*DataRow", err)
	})

	t.Run("successful", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		msg := &pgproto3.DataRow{Values: [][]byte{[]byte("foo")}}
		go func() {
			writeServerMsg(w, msg)
		}()

		err := expectDataRow(
			ctx,
			interceptor.NewFrontendConn(r),
			func(m *pgproto3.DataRow, size int) (bool, error) {
				buf, err := msg.Encode(nil)
				if err != nil {
					return false, err
				}
				return len(m.Values) == 1 &&
					string(m.Values[0]) == "foo" &&
					len(buf) == size, nil
			},
		)
		require.Nil(t, err)
	})
}

func TestExpectCommandComplete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	ctx := context.Background()

	t.Run("context_cancelled", func(t *testing.T) {
		tCtx, cancel := context.WithCancel(ctx)
		cancel()

		err := expectCommandComplete(tCtx, nil, "")
		require.EqualError(t, err, context.Canceled.Error())
	})

	t.Run("read_error", func(t *testing.T) {
		r, w := net.Pipe()
		r.Close()
		w.Close()

		err := expectCommandComplete(ctx, interceptor.NewFrontendConn(r), "")
		require.Regexp(t, "reading message", err)
	})

	t.Run("type_mismatch", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.ReadyForQuery{})
		}()

		err := expectCommandComplete(ctx, interceptor.NewFrontendConn(r), "")
		require.Regexp(t, "unexpected message.*ReadyForQuery", err)
	})

	t.Run("tag_mismatch", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.CommandComplete{CommandTag: []byte("foo")})
		}()

		err := expectCommandComplete(ctx, interceptor.NewFrontendConn(r), "bar")
		require.Regexp(t, "unexpected message.*CommandComplete.*CommandTag.*foo", err)
	})

	t.Run("successful", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		}()

		err := expectCommandComplete(ctx, interceptor.NewFrontendConn(r), "SELECT 1")
		require.Nil(t, err)
	})
}

func TestExpectReadyForQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	ctx := context.Background()

	t.Run("context_cancelled", func(t *testing.T) {
		tCtx, cancel := context.WithCancel(ctx)
		cancel()

		err := expectReadyForQuery(tCtx, nil)
		require.EqualError(t, err, context.Canceled.Error())
	})

	t.Run("read_error", func(t *testing.T) {
		r, w := net.Pipe()
		r.Close()
		w.Close()

		err := expectReadyForQuery(ctx, interceptor.NewFrontendConn(r))
		require.Regexp(t, "reading message", err)
	})

	t.Run("type_mismatch", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.ErrorResponse{})
		}()

		err := expectReadyForQuery(ctx, interceptor.NewFrontendConn(r))
		require.Regexp(t, "unexpected message.*ErrorResponse", err)
	})

	t.Run("successful", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.ReadyForQuery{TxStatus: 'I'})
		}()

		err := expectReadyForQuery(ctx, interceptor.NewFrontendConn(r))
		require.Nil(t, err)
	})
}

func writeServerMsg(w io.Writer, msg pgproto3.BackendMessage) {
	buf, _ := msg.Encode(nil)
	_, _ = w.Write(buf)
}

func expectMsg(t *testing.T, msgCh <-chan pgproto3.BackendMessage, match string) {
	t.Helper()
	msg := <-msgCh
	require.Regexp(t, match, jsonOrRaw(msg))
}
