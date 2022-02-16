// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package interceptor_test

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

// TestSimpleProxy illustrates how the frontend and backend interceptors can be
// used as a proxy.
func TestSimpleProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// These represents connections for client<->proxy and proxy<->server.
	fromClient := new(bytes.Buffer)
	toClient := new(bytes.Buffer)
	fromServer := new(bytes.Buffer)
	toServer := new(bytes.Buffer)

	// Create client and server interceptors.
	clientInt := interceptor.NewBackendInterceptor(fromClient)
	serverInt := interceptor.NewFrontendInterceptor(fromServer)

	t.Run("client to server", func(t *testing.T) {
		// Client sends a list of SQL queries.
		queries := []pgproto3.FrontendMessage{
			&pgproto3.Query{String: "SELECT 1"},
			&pgproto3.Query{String: "SELECT * FROM foo.bar"},
			&pgproto3.Query{String: "UPDATE foo SET x = 42"},
			&pgproto3.Sync{},
			&pgproto3.Terminate{},
		}
		for _, msg := range queries {
			_, err := fromClient.Write(msg.Encode(nil))
			require.NoError(t, err)
		}
		totalBytes := fromClient.Len()

		customQuery := &pgproto3.Query{
			String: "SELECT * FROM crdb_internal.serialize_session()"}

		for {
			typ, _, err := clientInt.PeekMsg()
			require.NoError(t, err)

			// Forward message to server.
			_, err = clientInt.ForwardMsg(toServer)
			require.NoError(t, err)

			if typ == pgwirebase.ClientMsgTerminate {
				// Right before we terminate, we could also craft a custom
				// message, and send it to the server.
				_, err := toServer.Write(customQuery.Encode(nil))
				require.NoError(t, err)
				break
			}
		}
		require.Equal(t, 0, fromClient.Len())
		require.Equal(t, totalBytes+len(customQuery.Encode(nil)), toServer.Len())
	})

	t.Run("server to client", func(t *testing.T) {
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
		for _, msg := range queries {
			_, err := fromServer.Write(msg.Encode(nil))
			require.NoError(t, err)
		}
		// Exclude bytes from second message.
		totalBytes := fromServer.Len() - len(queries[2].Encode(nil))

		for {
			typ, size, err := serverInt.PeekMsg()
			require.NoError(t, err)

			switch typ {
			case pgwirebase.ServerMsgCommandComplete:
				// Assuming that we're only interested in small messages, then
				// we could skip all the large ones.
				if size > 12 {
					_, err := serverInt.ForwardMsg(toClient)
					require.NoError(t, err)
					continue
				}

				// Decode message.
				msg, err := serverInt.ReadMsg()
				require.NoError(t, err)

				// Once we've decoded the message, we could store the message
				// somewhere, and not forward it back to the client.
				dmsg, ok := msg.(*pgproto3.CommandComplete)
				require.True(t, ok)
				require.Equal(t, "short", string(dmsg.CommandTag))
			case pgwirebase.ServerMsgBackendKeyData:
				msg, err := serverInt.ReadMsg()
				require.NoError(t, err)

				dmsg, ok := msg.(*pgproto3.BackendKeyData)
				require.True(t, ok)

				// We could even rewrite the message before sending it back to
				// the client.
				dmsg.SecretKey = 100

				_, err = toClient.Write(dmsg.Encode(nil))
				require.NoError(t, err)
			default:
				// Forward message that we're not interested to the client.
				_, err := serverInt.ForwardMsg(toClient)
				require.NoError(t, err)
			}

			if typ == pgwirebase.ServerMsgReady {
				break
			}
		}
		require.Equal(t, 0, fromServer.Len())
		require.Equal(t, totalBytes, toClient.Len())
	})
}
