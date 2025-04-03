// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package interceptor_test

import (
	"encoding/json"
	"io"
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

// TestSimpleProxy illustrates how the frontend and backend connections can be
// used as a proxy.
func TestSimpleProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	t.Run("client to server", func(t *testing.T) {
		// These represents connections for client<->proxy and proxy<->server.
		clientProxy, client := net.Pipe()
		serverProxy, server := net.Pipe()
		defer clientProxy.Close()
		defer client.Close()
		defer serverProxy.Close()
		defer server.Close()

		// Create client and server interceptors.
		clientConn := interceptor.NewBackendConn(clientProxy)
		serverConn := interceptor.NewFrontendConn(serverProxy)

		// Client sends a list of SQL queries.
		queries := []pgproto3.FrontendMessage{
			&pgproto3.Query{String: "SELECT 1"},
			&pgproto3.Query{String: "SELECT 2 FROM foobar"},
			&pgproto3.Query{String: "UPDATE foo SET x = 42"},
			&pgproto3.Sync{},
			&pgproto3.Terminate{},
		}
		errCh := make(chan error, len(queries))
		go func() {
			for _, msg := range queries {
				buf, err := msg.Encode(nil)
				require.NoError(t, err)
				_, err = client.Write(buf)
				errCh <- err
			}
		}()
		msgCh := make(chan pgproto3.FrontendMessage, 10)
		go func() {
			backend := interceptor.NewBackendConn(server)
			for {
				msg, err := backend.ReadMsg()
				if err != nil {
					return
				}
				msgCh <- msg
			}
		}()

		customQuery := &pgproto3.Query{
			String: "SELECT * FROM crdb_internal.serialize_session()"}

		for {
			typ, _, err := clientConn.PeekMsg()
			require.NoError(t, err)

			// Forward message to server.
			_, err = clientConn.ForwardMsg(serverConn)
			require.NoError(t, err)

			if typ == pgwirebase.ClientMsgTerminate {
				// Right before we terminate, we could also craft a custom
				// message, and send it to the server.
				buf, err := customQuery.Encode(nil)
				require.NoError(t, err)
				_, err = serverConn.Write(buf)
				require.NoError(t, err)
				break
			}
		}

		expectedMsg := []string{
			`"Type":"Query","String":"SELECT 1"`,
			`"Type":"Query","String":"SELECT 2 FROM foobar"`,
			`"Type":"Query","String":"UPDATE foo SET x = 42"`,
			`"Type":"Sync"`,
			`"Type":"Terminate"`,
			`"Type":"Query","String":"SELECT \* FROM crdb_internal.serialize_session\(\)"`,
		}
		var m1 []byte
		for _, m2 := range expectedMsg {
			msg := <-msgCh
			m1, _ = json.Marshal(msg)
			require.Regexp(t, m2, string(m1))
		}
	})

	t.Run("server to client", func(t *testing.T) {
		// These represents connections for client<->proxy and proxy<->server.
		clientProxy, client := net.Pipe()
		serverProxy, server := net.Pipe()
		defer clientProxy.Close()
		defer client.Close()
		defer serverProxy.Close()
		defer server.Close()

		// Create client and server interceptors.
		clientConn := interceptor.NewBackendConn(clientProxy)
		serverConn := interceptor.NewFrontendConn(serverProxy)

		// Server sends back responses.
		queries := []pgproto3.BackendMessage{
			// Forward these back to the client.
			&pgproto3.CommandComplete{CommandTag: []byte("averylongstring")},
			&pgproto3.BackendKeyData{ProcessID: 100, SecretKey: 42},
			// Do not forward back to the client.
			&pgproto3.CommandComplete{CommandTag: []byte("short")},
			// Terminator.
			&pgproto3.ReadyForQuery{},
		}
		errCh := make(chan error, len(queries))
		go func() {
			for _, msg := range queries {
				buf, err := msg.Encode(nil)
				require.NoError(t, err)
				_, err = server.Write(buf)
				errCh <- err
			}
		}()
		msgCh := make(chan pgproto3.BackendMessage, 10)
		go func() {
			frontend := interceptor.NewFrontendConn(client)
			for {
				msg, err := frontend.ReadMsg()
				if err != nil {
					return
				}
				msgCh <- msg
			}
		}()

		for {
			typ, size, err := serverConn.PeekMsg()
			require.NoError(t, err)

			switch typ {
			case pgwirebase.ServerMsgCommandComplete:
				// Assuming that we're only interested in small messages, then
				// we could skip all the large ones.
				if size > 12 {
					_, err := serverConn.ForwardMsg(clientConn)
					require.NoError(t, err)
					continue
				}

				// Decode message.
				msg, err := serverConn.ReadMsg()
				require.NoError(t, err)

				// Once we've decoded the message, we could store the message
				// somewhere, and not forward it back to the client.
				dmsg, ok := msg.(*pgproto3.CommandComplete)
				require.True(t, ok)
				require.Equal(t, "short", string(dmsg.CommandTag))
			case pgwirebase.ServerMsgBackendKeyData:
				msg, err := serverConn.ReadMsg()
				require.NoError(t, err)

				dmsg, ok := msg.(*pgproto3.BackendKeyData)
				require.True(t, ok)

				// We could even rewrite the message before sending it back to
				// the client.
				dmsg.SecretKey = 100

				buf, err := dmsg.Encode(nil)
				require.NoError(t, err)
				_, err = clientConn.Write(buf)
				require.NoError(t, err)
			default:
				// Forward message that we're not interested to the client.
				_, err := serverConn.ForwardMsg(clientConn)
				require.NoError(t, err)
			}

			if typ == pgwirebase.ServerMsgReady {
				break
			}
		}

		expectedMsg := []string{
			`"Type":"CommandComplete","CommandTag":"averylongstring"`,
			`"Type":"BackendKeyData","ProcessID":100,"SecretKey":100`,
			`"Type":"ReadyForQuery"`,
		}
		var m1 []byte
		for _, m2 := range expectedMsg {
			msg := <-msgCh
			m1, _ = json.Marshal(msg)
			require.Regexp(t, m2, string(m1))
		}
	})
}

func writeAsync(t *testing.T, w io.Writer, data []byte) <-chan error {
	t.Helper()
	errCh := make(chan error, 1)
	go func() {
		_, err := w.Write(data)
		errCh <- err
	}()
	return errCh
}
