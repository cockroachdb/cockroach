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
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

func TestForward(t *testing.T) {
	defer leaktest.AfterTest(t)()

	bgCtx := context.Background()

	t.Run("closed_when_processors_error", func(t *testing.T) {
		p1, p2 := net.Pipe()
		// Close the connection right away. p2 is owned by the forwarder.
		p1.Close()

		f := forward(bgCtx, p1, p2)
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

		f := forward(ctx, clientR, serverW)
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

		f := forward(ctx, clientR, serverW)
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

	f := forward(context.Background(), p1, p2)
	defer f.Close()
	require.Nil(t, f.ctx.Err())

	f.Close()
	require.EqualError(t, f.ctx.Err(), context.Canceled.Error())
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
