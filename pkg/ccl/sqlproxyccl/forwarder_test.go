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
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

func TestForwarder_Run(t *testing.T) {
	defer leaktest.AfterTest(t)()

	bgCtx := context.Background()

	t.Run("early_errors", func(t *testing.T) {
		f := newForwarder(bgCtx, nil /* conn */, nil /* crdbConn */)

		// Forwarder started.
		f.mu.started = true
		require.EqualError(t, f.Run(), ErrForwarderStarted.Error())

		// Forwarder closed.
		f.mu.closed = true
		require.EqualError(t, f.Run(), ErrForwarderClosed.Error())
	})

	t.Run("closed_when_processors_error", func(t *testing.T) {
		p1, p2 := net.Pipe()
		f := newForwarder(bgCtx, p1, p2)

		// Close the connections right away.
		p1.Close()
		p2.Close()
		require.NoError(t, f.Run())

		// We have to wait for the goroutine to run.
		testutils.SucceedsSoon(t, func() error {
			if f.IsClosed() {
				return nil
			}
			return errors.New("forwarder is not closed yet")
		})

		select {
		case err := <-f.errChan:
			require.Error(t, err)
		default:
			t.Fatalf("require error, but not none")
		}
	})

	t.Run("client_to_server", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(bgCtx, 5*time.Second)
		defer cancel()

		clientW, clientR := net.Pipe()
		serverW, serverR := net.Pipe()
		// We don't close clientW and serverR here since we have no control
		// over those.
		defer clientR.Close()
		defer serverW.Close()

		f := newForwarder(ctx, clientR, serverW)
		defer f.Close()
		require.NoError(t, f.Run())
		require.True(t, f.mu.started)
		require.False(t, f.mu.closed)

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
		// over those.
		defer clientR.Close()
		defer serverW.Close()

		f := newForwarder(ctx, clientR, serverW)
		defer f.Close()
		require.NoError(t, f.Run())
		require.True(t, f.mu.started)
		require.False(t, f.mu.closed)

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

	f := newForwarder(context.Background(), nil /* conn */, nil /* crdbConn */)
	require.False(t, f.mu.closed)

	f.Close()
	require.True(t, f.mu.closed)
	require.EqualError(t, f.ctx.Err(), context.Canceled.Error())
}

func TestForwarder_IsClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := newForwarder(context.Background(), nil /* conn */, nil /* crdbConn */)
	require.False(t, f.mu.closed)
	require.False(t, f.IsClosed())
	f.mu.closed = true
	require.True(t, f.IsClosed())
}

func TestForwarder_IsStarted(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := newForwarder(context.Background(), nil /* conn */, nil /* crdbConn */)
	require.False(t, f.mu.started)
	require.False(t, f.IsStarted())
	f.mu.started = true
	require.True(t, f.IsStarted())
}
