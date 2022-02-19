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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

func TestForwarder_awaitTransferStateResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	bgCtx := context.Background()

	// Test that context was cancelled.
	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(bgCtx)
		cancel()
		f := &forwarder{}
		state, token, err := f.awaitTransferStateResponse(ctx, serverMsgAny)
		require.EqualError(t, err, context.Canceled.Error())
		require.Equal(t, "", state)
		require.Equal(t, "", token)
	})

	// Test that ErrorResponse was returned during the transfer.
	// Server sends:
	//   1. BackendKeyData
	//   2. ErrorResponse
	t.Run("ambiguous ErrorResponse", func(t *testing.T) {
		buf := new(bytes.Buffer)
		_, _ = buf.Write((&pgproto3.BackendKeyData{}).Encode(nil))
		_, _ = buf.Write((&pgproto3.ErrorResponse{}).Encode(nil))

		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		msgChan := make(chan pgproto3.BackendMessage, 1)
		go func() {
			frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(r), r)
			msg, _ := frontend.Receive()
			msgChan <- msg
		}()

		f := &forwarder{
			serverInterceptor: interceptor.NewFrontendInterceptor(buf),
			clientConn:        w,
		}
		state, token, err := f.awaitTransferStateResponse(bgCtx, serverMsgAny)
		require.EqualError(t, err, "ambiguous ErrorResponse message")
		require.Equal(t, "", state)
		require.Equal(t, "", token)

		// Validate that the first message was forwarded before getting an
		// ErrorResponse message.
		msg := <-msgChan
		_, ok := msg.(*pgproto3.BackendKeyData)
		require.True(t, ok)
	})

	// Test expectedServerMsgType != typ.
	// Server sends:
	//   1. RowDescription that is valid
	//   2. ReadyForQuery
	t.Run("unexpected message from server", func(t *testing.T) {
		buf := new(bytes.Buffer)
		_, _ = buf.Write((&pgproto3.RowDescription{}).Encode(nil))
		_, _ = buf.Write((&pgproto3.ReadyForQuery{}).Encode(nil))

		f := &forwarder{
			serverInterceptor: interceptor.NewFrontendInterceptor(buf),
		}
		f.testingKnobs.isValidStartTransferStateResponse = func(m *pgproto3.RowDescription) bool {
			require.NotNil(t, m)
			return true
		}
		state, token, err := f.awaitTransferStateResponse(bgCtx, serverMsgAny)
		require.Regexp(t, "expected message with type 'D', but found 'Z'", err)
		require.True(t, errors.Is(err, errTransferProtocol))
		require.Equal(t, "", state)
		require.Equal(t, "", token)
	})

	// Test that the transfer gets aborted with a large state (> 8K).
	// Server sends:
	//   1. DataRow that is extremely large
	t.Run("large state data", func(t *testing.T) {
		buf := new(bytes.Buffer)
		_, _ = buf.Write((&pgproto3.DataRow{Values: make([][]byte, 5000)}).Encode(nil))

		f := &forwarder{
			serverInterceptor: interceptor.NewFrontendInterceptor(buf),
		}
		state, token, err := f.awaitTransferStateResponse(bgCtx, pgwirebase.ServerMsgDataRow)
		require.EqualError(t, err, "transfer rejected due to large state")
		require.Equal(t, "", state)
		require.Equal(t, "", token)
	})

	// Test that parsing fails (e.g. invalid transfer key).
	t.Run("parsing fails", func(t *testing.T) {
		buf := new(bytes.Buffer)
		_, _ = buf.Write((&pgproto3.DataRow{}).Encode(nil))

		f := &forwarder{
			serverInterceptor: interceptor.NewFrontendInterceptor(buf),
		}
		f.mu.transferKey = "foo-bar-baz"
		f.testingKnobs.parseTransferStateResponse = func(
			m *pgproto3.DataRow, transferKey string,
		) (string, string, string, error) {
			require.NotNil(t, m)
			require.Equal(t, "foo-bar-baz", transferKey)
			return "", "", "", errors.New("apple")
		}
		state, token, err := f.awaitTransferStateResponse(
			bgCtx, pgwirebase.ServerMsgDataRow,
		)
		require.EqualError(t, err, "parsing transfer response: apple")
		require.True(t, errors.Is(err, errTransferProtocol))
		require.Equal(t, "", state)
		require.Equal(t, "", token)
	})

	// Test that end message validation fails.
	t.Run("invalid end transfer state", func(t *testing.T) {
		buf := new(bytes.Buffer)
		_, _ = buf.Write((&pgproto3.CommandComplete{CommandTag: []byte("foo")}).Encode(nil))

		f := &forwarder{
			serverInterceptor: interceptor.NewFrontendInterceptor(buf),
		}
		f.testingKnobs.isValidEndTransferStateResponse = func(m *pgproto3.CommandComplete) bool {
			require.Equal(t, "foo", string(m.CommandTag))
			return false
		}
		state, token, err := f.awaitTransferStateResponse(
			bgCtx, pgwirebase.ServerMsgCommandComplete,
		)
		require.Regexp(t, "validating end transfer response", err)
		require.True(t, errors.Is(err, errTransferProtocol))
		require.Equal(t, "", state)
		require.Equal(t, "", token)
	})

	// Test that SHOW TRANSFER STATE returns a serialization error. This should
	// be a common case with open transactions.
	t.Run("transfer with recoverable connection error", func(t *testing.T) {
		buf := new(bytes.Buffer)
		_, _ = buf.Write((&pgproto3.DataRow{}).Encode(nil))
		_, _ = buf.Write((&pgproto3.CommandComplete{}).Encode(nil))
		_, _ = buf.Write((&pgproto3.ReadyForQuery{}).Encode(nil))

		f := &forwarder{
			serverInterceptor: interceptor.NewFrontendInterceptor(buf),
		}
		f.mu.transferKey = "foo-bar-baz"
		f.testingKnobs.parseTransferStateResponse = func(
			m *pgproto3.DataRow, transferKey string,
		) (string, string, string, error) {
			require.NotNil(t, m)
			require.Equal(t, "foo-bar-baz", transferKey)
			return "serialization error", "", "", nil
		}
		f.testingKnobs.isValidEndTransferStateResponse = func(
			m *pgproto3.CommandComplete,
		) bool {
			require.NotNil(t, m)
			return true
		}
		state, token, err := f.awaitTransferStateResponse(
			bgCtx, pgwirebase.ServerMsgDataRow,
		)
		require.EqualError(t, err, "serialization error")
		require.True(t, isConnRecoverableError(err))
		require.Equal(t, "", state)
		require.Equal(t, "", token)
	})

	// Server sends the following messages:
	//   1. BackendKeyData (should not match RowDescription)
	//   2. RowDescription with a large size (> 512 bytes)
	//   3. RowDescription that is invalid
	//   4. RowDescription that is valid
	//   5. DataRow
	//   6. CommandComplete
	//   7. ReadyForQuery
	//
	// 1 to 3 will all be forwarded to the client.
	t.Run("successful transfer", func(t *testing.T) {
		buf := new(bytes.Buffer)
		_, _ = buf.Write((&pgproto3.BackendKeyData{}).Encode(nil))
		_, _ = buf.Write((&pgproto3.RowDescription{
			Fields: make([]pgproto3.FieldDescription, 1000),
		}).Encode(nil))
		_, _ = buf.Write((&pgproto3.RowDescription{}).Encode(nil))
		_, _ = buf.Write((&pgproto3.RowDescription{}).Encode(nil))
		_, _ = buf.Write((&pgproto3.DataRow{}).Encode(nil))
		_, _ = buf.Write((&pgproto3.CommandComplete{}).Encode(nil))
		_, _ = buf.Write((&pgproto3.ReadyForQuery{}).Encode(nil))

		r, w := net.Pipe()
		defer r.Close()
		defer w.Close()

		msgChan := make(chan pgproto3.BackendMessage, 3)
		go func() {
			frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(r), r)
			msg, _ := frontend.Receive()
			msgChan <- msg
			msg, _ = frontend.Receive()
			msgChan <- msg
			msg, _ = frontend.Receive()
			msgChan <- msg
		}()

		f := &forwarder{
			serverInterceptor: interceptor.NewFrontendInterceptor(buf),
			clientConn:        w,
		}
		f.mu.transferKey = "foo-bar-baz"

		var startCalledCount, endCalledCount int
		f.testingKnobs.isValidStartTransferStateResponse = func(
			m *pgproto3.RowDescription,
		) bool {
			require.NotNil(t, m)
			startCalledCount++
			return startCalledCount >= 2
		}
		f.testingKnobs.parseTransferStateResponse = func(
			m *pgproto3.DataRow, transferKey string,
		) (string, string, string, error) {
			require.NotNil(t, m)
			require.Equal(t, "foo-bar-baz", transferKey)
			return "", "foo-state", "bar-token", nil
		}
		f.testingKnobs.isValidEndTransferStateResponse = func(
			m *pgproto3.CommandComplete,
		) bool {
			require.NotNil(t, m)
			endCalledCount++
			return true
		}
		state, token, err := f.awaitTransferStateResponse(bgCtx, serverMsgAny)
		require.NoError(t, err)
		require.Equal(t, "foo-state", state)
		require.Equal(t, "bar-token", token)
		require.Equal(t, 2, startCalledCount)
		require.Equal(t, 1, endCalledCount)

		// Validate forwarded messages.
		msg := <-msgChan
		_, ok := msg.(*pgproto3.BackendKeyData)
		require.True(t, ok)
		msg = <-msgChan
		_, ok = msg.(*pgproto3.RowDescription)
		require.True(t, ok)
		msg = <-msgChan
		_, ok = msg.(*pgproto3.RowDescription)
		require.True(t, ok)
	})
}

func TestWriteTransferStateRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("successful", func(t *testing.T) {
		buf := new(bytes.Buffer)
		err := writeTransferStateRequest(buf, "foo")
		require.NoError(t, err)

		backend := pgproto3.NewBackend(pgproto3.NewChunkReader(buf), buf)
		msg, err := backend.Receive()
		require.NoError(t, err)
		m, ok := msg.(*pgproto3.Query)
		require.True(t, ok)
		require.Equal(t, "SHOW TRANSFER STATE WITH 'foo'", m.String)
	})

	t.Run("error", func(t *testing.T) {
		err := writeTransferStateRequest(&errWriter{}, "foo")
		require.Regexp(t, "unexpected Write call", err)
	})
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

func TestDeserializeSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// TODO(jaylim-crl): Add tests once implementation is in.
}

var _ io.Writer = &errWriter{}

// errWriter is an io.Writer that fails whenever a Write call is made.
type errWriter struct{}

// Write implements the io.Writer interface.
func (w *errWriter) Write(p []byte) (int, error) {
	return 0, errors.AssertionFailedf("unexpected Write call")
}
