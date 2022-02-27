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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

func TestRunShowTransferState(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
	ctx := context.Background()

	t.Run("context_cancelled", func(t *testing.T) {
		tCtx, cancel := context.WithCancel(ctx)
		cancel()

		state, token, err := waitForShowTransferState(tCtx, nil, nil, "")
		require.EqualError(t, err, context.Canceled.Error())
		require.Equal(t, "", state)
		require.Equal(t, "", token)
	})

	expectMsg := func(fi *interceptor.FrontendInterceptor, match string) error {
		msg, err := fi.ReadMsg()
		if err != nil {
			return err
		}
		j := jsonOrRaw(msg)
		if !strings.Contains(j, match) {
			return errors.Newf("require message includes '%s', found none in '%s'", match, j)
		}
		return nil
	}

	for _, tc := range []struct {
		name           string
		sendSequence   []pgproto3.BackendMessage
		postValidate   func(*interceptor.FrontendInterceptor) error
		err            string
		recoverableErr bool
	}{
		{
			// All irrelevant messages are forwarded to the client. This returns
			// an error when we see ErrorResponse.
			name: "RowDescription/candidate_mismatch",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.BackendKeyData{}, // not RowDescription
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("foo1")},
						{Name: make([]byte, 4096)},
					},
				}, // too large
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{Name: []byte("foo2")},
					},
				}, // invalid
				&pgproto3.ErrorResponse{},
			},
			err: "ambiguous ErrorResponse",
			postValidate: func(fi *interceptor.FrontendInterceptor) error {
				if err := expectMsg(fi, `"Type":"BackendKeyData"`); err != nil {
					return err
				}
				if err := expectMsg(fi, `"Type":"RowDescription","Fields":[{"Name":"foo1"`); err != nil {
					return err
				}
				if err := expectMsg(fi, `"Type":"RowDescription","Fields":[{"Name":"foo2"`); err != nil {
					return err
				}
				return nil
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
			err: "type mismatch: expected 'D', but found 'Z'",
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
			err: "expected 'foo-transfer-key' as transfer key, found 'bar'",
		},
		{
			// Large state should abort transfers.
			name: "DataRow/large_state",
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
						make([]byte, 5000),
						{},
						{},
					},
				},
			},
			err: "too many bytes",
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
			err: "type mismatch: expected 'C', but found 'Z'",
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
				&pgproto3.CommandComplete{CommandTag: []byte("SELECT 2")},
			},
			err: "invalid CommandComplete",
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
			err: "type mismatch: expected 'Z', but found 'C'",
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
			err:            "serialization error",
			recoverableErr: true,
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
				&pgproto3.DataRow{
					Values: [][]byte{
						{},
						[]byte("foo-state"),
						[]byte("foo-token"),
						[]byte("foo-transfer-key"),
					},
				},
				&pgproto3.CommandComplete{CommandTag: []byte("SHOW TRANSFER STATE 1")},
				&pgproto3.ReadyForQuery{},
			},
			postValidate: func(fi *interceptor.FrontendInterceptor) error {
				if err := expectMsg(fi, `"Type":"BackendKeyData"`); err != nil {
					return err
				}
				if err := expectMsg(fi, `"Type":"RowDescription"`); err != nil {
					return err
				}
				if err := expectMsg(fi, `"Type":"CommandComplete"`); err != nil {
					return err
				}
				return nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			for _, m := range tc.sendSequence {
				writeServerMsg(buf, m)
			}
			toClient := new(bytes.Buffer)

			state, token, err := waitForShowTransferState(ctx,
				interceptor.NewFrontendInterceptor(buf), toClient, "foo-transfer-key")
			if tc.err == "" {
				require.NoError(t, err)
				require.Equal(t, "foo-state", state)
				require.Equal(t, "foo-token", token)
			} else {
				require.Regexp(t, tc.err, err)
				if tc.recoverableErr {
					require.True(t, isConnRecoverableError(err))
				} else {
					require.False(t, isConnRecoverableError(err))
				}
			}

			// Verify that all messages were read, and forwarding was correct.
			require.Equal(t, 0, buf.Len())
			if tc.postValidate != nil {
				frontend := interceptor.NewFrontendInterceptor(toClient)
				require.NoError(t, tc.postValidate(frontend))
				_, _, err := frontend.PeekMsg()
				require.Regexp(t, "EOF", err)
			}
			require.Equal(t, 0, toClient.Len())
		})
	}
}

func TestIsValidStartTransferStateResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name     string
		msg      *pgproto3.RowDescription
		expected bool
	}{
		{
			name:     "nil_message",
			msg:      nil,
			expected: false,
		},
		{
			name: "invalid_number_of_columns",
			msg: &pgproto3.RowDescription{
				// 3 columns instead of 4.
				Fields: []pgproto3.FieldDescription{
					{Name: []byte("foo")},
					{Name: []byte("bar")},
					{Name: []byte("baz")},
				},
			},
			expected: false,
		},
		{
			name: "invalid_column_names",
			msg: &pgproto3.RowDescription{
				Fields: []pgproto3.FieldDescription{
					{Name: []byte("error")},
					{Name: []byte("session_state_foo")},
					{Name: []byte("session_revival_token_bar")},
					{Name: []byte("apple")},
				},
			},
			expected: false,
		},
		{
			name: "valid",
			msg: &pgproto3.RowDescription{
				Fields: []pgproto3.FieldDescription{
					{Name: []byte("error")},
					{Name: []byte("session_state_base64")},
					{Name: []byte("session_revival_token_base64")},
					{Name: []byte("transfer_key")},
				},
			},
			expected: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			valid := isValidStartTransferStateResponse(tc.msg)
			if tc.expected {
				require.True(t, valid)
			} else {
				require.False(t, valid)
			}
		})
	}
}

func TestIsValidEndTransferStateResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name     string
		msg      *pgproto3.CommandComplete
		expected bool
	}{
		{
			name:     "nil_message",
			msg:      nil,
			expected: false,
		},
		{
			name:     "unsupported_command_tag",
			msg:      &pgproto3.CommandComplete{CommandTag: []byte("foobarbaz")},
			expected: false,
		},
		{
			name:     "invalid_row_count",
			msg:      &pgproto3.CommandComplete{CommandTag: []byte("SHOW TRANSFER STATE 2")},
			expected: false,
		},
		{
			name:     "valid",
			msg:      &pgproto3.CommandComplete{CommandTag: []byte("SHOW TRANSFER STATE 1")},
			expected: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			valid := isValidEndTransferStateResponse(tc.msg)
			if tc.expected {
				require.True(t, valid)
			} else {
				require.False(t, valid)
			}
		})
	}
}

func TestParseTransferStateResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name        string
		msg         *pgproto3.DataRow
		transferKey string
		expectedErr string
	}{
		{
			name:        "nil_message",
			msg:         nil,
			expectedErr: "DataRow message is nil",
		},
		{
			name: "invalid_response",
			msg: &pgproto3.DataRow{
				// 3 columns instead of 4.
				Values: [][]byte{[]byte("foo"), []byte("bar"), []byte("baz")},
			},
			expectedErr: "unexpected 3 columns in DataRow",
		},
		{
			name: "invalid_transfer_key",
			msg: &pgproto3.DataRow{
				Values: [][]byte{[]byte("foo"), []byte("bar"), []byte("baz"), []byte("carl")},
			},
			transferKey: "car",
			expectedErr: "expected 'car' as transfer key, found 'carl'",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transferErr, state, revivalToken, err := parseTransferStateResponse(tc.msg, tc.transferKey)
			require.Regexp(t, tc.expectedErr, err)
			require.Equal(t, "", transferErr)
			require.Equal(t, "", state)
			require.Equal(t, "", revivalToken)
		})
	}

	t.Run("valid", func(t *testing.T) {
		msg := &pgproto3.DataRow{
			Values: [][]byte{[]byte("foo"), []byte("bar"), []byte("baz"), []byte("carl")},
		}
		transferErr, state, revivalToken, err := parseTransferStateResponse(msg, "carl")
		require.NoError(t, err)
		require.Equal(t, "foo", transferErr)
		require.Equal(t, "bar", state)
		require.Equal(t, "baz", revivalToken)

		// Ensure that returned strings are copied. Alternatively, we could also
		// check pointers using encoding.UnsafeConvertStringToBytes.
		msg.Values[0][1] = '-'
		msg.Values[1][1] = '-'
		msg.Values[2][1] = '-'
		require.Equal(t, "foo", transferErr)
		require.Equal(t, "bar", state)
		require.Equal(t, "baz", revivalToken)
	})
}

func TestRunAndWaitForDeserializeSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	t.Run("write_failed", func(t *testing.T) {
		err := runAndWaitForDeserializeSession(ctx, &errWriter{}, nil, "foo")
		require.Regexp(t, "unexpected Write call", err)
	})

	for _, tc := range []struct {
		name         string
		sendSequence []pgproto3.BackendMessage
		err          string
	}{
		{
			name: "RowDescription/type_mismatch",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.ErrorResponse{},
			},
			err: "type mismatch: expected 'T', but found 'E'",
		},
		{
			name: "RowDescription/column_mismatch/length",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.RowDescription{},
			},
			err: "invalid RowDescription",
		},
		{
			name: "RowDescription/column_mismatch/name",
			sendSequence: []pgproto3.BackendMessage{
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{{Name: []byte("bar")}}},
			},
			err: "invalid RowDescription",
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
			err: "type mismatch: expected 'D', but found 'Z'",
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
			err: "invalid DataRow",
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
			err: "invalid DataRow",
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
			err: "type mismatch: expected 'C', but found 'Z'",
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
			err: "invalid CommandComplete",
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
			err: "type mismatch: expected 'Z', but found 'C'",
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
			r, w := net.Pipe()
			defer r.Close()
			defer w.Close()

			msgChan := make(chan pgproto3.FrontendMessage, 1)
			go func() {
				backend := interceptor.NewBackendInterceptor(w)
				msg, _ := backend.ReadMsg()
				msgChan <- msg

				for _, m := range tc.sendSequence {
					writeServerMsg(w, m)
				}
			}()

			err := runAndWaitForDeserializeSession(ctx, r,
				interceptor.NewFrontendInterceptor(r), "foo-transfer-key")
			if tc.err == "" {
				require.NoError(t, err)
			} else {
				require.Regexp(t, tc.err, err)
			}

			msg := <-msgChan
			m, ok := msg.(*pgproto3.Query)
			require.True(t, ok)
			const queryStr = "SELECT crdb_internal.deserialize_session(decode('foo-transfer-key', 'base64'))"
			require.Equal(t, queryStr, m.String)
		})
	}
}

func TestExpectNextServerMessage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	t.Run("context_cancelled", func(t *testing.T) {
		tCtx, cancel := context.WithCancel(ctx)
		cancel()

		msg, err := expectNextServerMessage(tCtx, nil, pgwirebase.ServerMsgReady, false)
		require.EqualError(t, err, context.Canceled.Error())
		require.Nil(t, msg)
	})

	t.Run("peek_error", func(t *testing.T) {
		r, w := net.Pipe()
		r.Close()
		w.Close()

		msg, err := expectNextServerMessage(
			ctx,
			interceptor.NewFrontendInterceptor(r),
			pgwirebase.ServerMsgReady,
			false,
		)
		require.Regexp(t, "peeking message", err)
		require.Nil(t, msg)
	})

	t.Run("type_mismatch", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.ErrorResponse{})
		}()

		msg, err := expectNextServerMessage(
			ctx,
			interceptor.NewFrontendInterceptor(r),
			pgwirebase.ServerMsgRowDescription,
			false,
		)
		require.Regexp(t, "type mismatch", err)
		require.Nil(t, msg)
		require.True(t, isTypeMismatchError(err))
		require.True(t, isErrorResponseError(err))
	})

	t.Run("too_many_bytes", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.DataRow{Values: make([][]byte, 2<<12+1)})
		}()

		msg, err := expectNextServerMessage(
			ctx,
			interceptor.NewFrontendInterceptor(r),
			pgwirebase.ServerMsgDataRow,
			false,
		)
		require.Regexp(t, "too many bytes", err)
		require.Nil(t, msg)
		require.True(t, isLargeMessageError(err))
	})

	t.Run("skipRead=true", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.ReadyForQuery{})
		}()

		msg, err := expectNextServerMessage(
			ctx,
			interceptor.NewFrontendInterceptor(r),
			pgwirebase.ServerMsgReady,
			true,
		)
		require.Nil(t, err)
		require.Nil(t, msg)
	})

	t.Run("skipRead=false", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.RowDescription{
				Fields: []pgproto3.FieldDescription{{Name: []byte("foo")}},
			})
		}()

		msg, err := expectNextServerMessage(
			ctx,
			interceptor.NewFrontendInterceptor(r),
			pgwirebase.ServerMsgRowDescription,
			false,
		)
		require.Nil(t, err)
		pgMsg, ok := msg.(*pgproto3.RowDescription)
		require.True(t, ok)
		require.Equal(t, []byte("foo"), pgMsg.Fields[0].Name)
	})
}

var _ io.Writer = &errWriter{}

// errWriter is an io.Writer that fails whenever a Write call is made.
type errWriter struct{}

// Write implements the io.Writer interface.
func (w *errWriter) Write(p []byte) (int, error) {
	return 0, errors.AssertionFailedf("unexpected Write call")
}

func writeServerMsg(w io.Writer, msg pgproto3.BackendMessage) {
	_, _ = w.Write(msg.Encode(nil))
}
