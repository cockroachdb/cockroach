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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
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

		transferErr, state, token, err := waitForShowTransferState(tCtx, nil, nil, "")
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
		postValidate func(*testing.T, *interceptor.FrontendInterceptor)
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
			postValidate: func(t *testing.T, fi *interceptor.FrontendInterceptor) {
				t.Helper()
				expectMsg(t, fi, `"Type":"BackendKeyData"`)
				expectMsg(t, fi, `"Type":"RowDescription".*"Name":"foo1"`)
				expectMsg(t, fi, `"Type":"RowDescription".*"Name":"foo2"`)
				expectMsg(t, fi, `"Type":"RowDescription".*session_state_foo.*session_revival_token_bar`)
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
			postValidate: func(t *testing.T, fi *interceptor.FrontendInterceptor) {
				t.Helper()
				expectMsg(t, fi, `"Type":"BackendKeyData"`)
				expectMsg(t, fi, `"Type":"RowDescription"`)
				expectMsg(t, fi, `"Type":"CommandComplete"`)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			for _, m := range tc.sendSequence {
				writeServerMsg(buf, m)
			}
			toClient := new(bytes.Buffer)

			transferErr, state, token, err := waitForShowTransferState(
				ctx,
				interceptor.NewFrontendInterceptor(buf),
				toClient,
				"foo-transfer-key",
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
				require.Equal(t, 0, buf.Len())
			} else {
				require.Regexp(t, tc.err, err)
			}

			// Verify that forwarding was correct.
			if tc.postValidate != nil {
				frontend := interceptor.NewFrontendInterceptor(toClient)
				tc.postValidate(t, frontend)
				_, _, err := frontend.PeekMsg()
				require.Regexp(t, "EOF", err)
			}
			require.Equal(t, 0, toClient.Len())
		})
	}
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
			readBuf := new(bytes.Buffer)
			for _, m := range tc.sendSequence {
				writeServerMsg(readBuf, m)
			}
			writeBuf := new(bytes.Buffer)

			err := runAndWaitForDeserializeSession(ctx, writeBuf,
				interceptor.NewFrontendInterceptor(readBuf), "foo-transfer-key")
			if tc.err == "" {
				require.NoError(t, err)
			} else {
				require.Regexp(t, tc.err, err)
			}

			backend := interceptor.NewBackendInterceptor(writeBuf)
			msg, err := backend.ReadMsg()
			require.NoError(t, err)
			m, ok := msg.(*pgproto3.Query)
			require.True(t, ok)
			const queryStr = "SELECT crdb_internal.deserialize_session(decode('foo-transfer-key', 'base64'))"
			require.Equal(t, queryStr, m.String)
		})
	}
}

func TestWaitForSmallRowDescription(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

		err := waitForSmallRowDescription(ctx, interceptor.NewFrontendInterceptor(r), nil, nil)
		require.Regexp(t, "peeking message", err)
	})

	t.Run("ambiguous_error", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.ErrorResponse{})
		}()

		err := waitForSmallRowDescription(ctx, interceptor.NewFrontendInterceptor(r), nil, nil)
		require.Regexp(t, "ambiguous ErrorResponse.*ErrorResponse", err)
	})

	t.Run("successful", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		toClient := new(bytes.Buffer)

		go func() {
			// Not RowDescription.
			writeServerMsg(w, &pgproto3.BackendKeyData{ProcessID: 42})
			// Too large (> 4k bytes).
			writeServerMsg(w, &pgproto3.RowDescription{
				Fields: []pgproto3.FieldDescription{
					{Name: []byte("foo1")},
					{Name: make([]byte, 1<<13 /* 8K */)},
				},
			})
			// Mismatch.
			writeServerMsg(w, &pgproto3.RowDescription{
				Fields: []pgproto3.FieldDescription{
					{Name: []byte("foo2")},
					{Name: []byte("foo3")},
				},
			})
			// Match.
			writeServerMsg(w, &pgproto3.RowDescription{
				Fields: []pgproto3.FieldDescription{
					{Name: []byte("foo1")},
				},
			})
		}()

		err := waitForSmallRowDescription(
			ctx,
			interceptor.NewFrontendInterceptor(r),
			toClient,
			func(m *pgproto3.RowDescription) bool {
				return len(m.Fields) == 1 && string(m.Fields[0].Name) == "foo1"
			},
		)
		require.Nil(t, err)

		// Verify that forwarding was correct.
		fi := interceptor.NewFrontendInterceptor(toClient)
		expectMsg(t, fi, `"Type":"BackendKeyData".*"ProcessID":42`)
		expectMsg(t, fi, `"Type":"RowDescription".*"Name":"foo1"`)
		expectMsg(t, fi, `"Type":"RowDescription".*"Name":"foo2".*"Name":"foo3"`)
		_, _, err = fi.PeekMsg()
		require.Regexp(t, "EOF", err)
		require.Equal(t, 0, toClient.Len())
	})
}

func TestExpectDataRow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	falseValidateFn := func(m *pgproto3.DataRow) bool { return false }

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

		err := expectDataRow(ctx, interceptor.NewFrontendInterceptor(r), falseValidateFn)
		require.Regexp(t, "reading message", err)
	})

	t.Run("type_mismatch", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.ReadyForQuery{})
		}()

		err := expectDataRow(ctx, interceptor.NewFrontendInterceptor(r), falseValidateFn)
		require.Regexp(t, "unexpected message.*ReadyForQuery", err)
	})

	t.Run("validation_failed", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.DataRow{})
		}()

		err := expectDataRow(ctx, interceptor.NewFrontendInterceptor(r), falseValidateFn)
		require.Regexp(t, "validation failed for message.*DataRow", err)
	})

	t.Run("successful", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.DataRow{Values: [][]byte{[]byte("foo")}})
		}()

		err := expectDataRow(
			ctx,
			interceptor.NewFrontendInterceptor(r),
			func(m *pgproto3.DataRow) bool {
				return len(m.Values) == 1 && string(m.Values[0]) == "foo"
			},
		)
		require.Nil(t, err)
	})
}

func TestExpectCommandComplete(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

		err := expectCommandComplete(ctx, interceptor.NewFrontendInterceptor(r), "")
		require.Regexp(t, "reading message", err)
	})

	t.Run("type_mismatch", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.ReadyForQuery{})
		}()

		err := expectCommandComplete(ctx, interceptor.NewFrontendInterceptor(r), "")
		require.Regexp(t, "unexpected message.*ReadyForQuery", err)
	})

	t.Run("tag_mismatch", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.CommandComplete{CommandTag: []byte("foo")})
		}()

		err := expectCommandComplete(ctx, interceptor.NewFrontendInterceptor(r), "bar")
		require.Regexp(t, "unexpected message.*CommandComplete.*CommandTag.*foo", err)
	})

	t.Run("successful", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		}()

		err := expectCommandComplete(ctx, interceptor.NewFrontendInterceptor(r), "SELECT 1")
		require.Nil(t, err)
	})
}

func TestExpectReadyForQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

		err := expectReadyForQuery(ctx, interceptor.NewFrontendInterceptor(r))
		require.Regexp(t, "reading message", err)
	})

	t.Run("type_mismatch", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.ErrorResponse{})
		}()

		err := expectReadyForQuery(ctx, interceptor.NewFrontendInterceptor(r))
		require.Regexp(t, "unexpected message.*ErrorResponse", err)
	})

	t.Run("successful", func(t *testing.T) {
		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		go func() {
			writeServerMsg(w, &pgproto3.ReadyForQuery{TxStatus: 'I'})
		}()

		err := expectReadyForQuery(ctx, interceptor.NewFrontendInterceptor(r))
		require.Nil(t, err)
	})
}

func writeServerMsg(w io.Writer, msg pgproto3.BackendMessage) {
	_, _ = w.Write(msg.Encode(nil))
}

func expectMsg(t *testing.T, fi *interceptor.FrontendInterceptor, match string) {
	t.Helper()
	msg, err := fi.ReadMsg()
	require.NoError(t, err)
	require.Regexp(t, match, jsonOrRaw(msg))
}
