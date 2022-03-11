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
	"math/rand"
	"net"
	"testing"
	"testing/iotest"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
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

		// Close the connection right away to simulate processor error.
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

		// We don't close clientW and serverR here since we have no control
		// over those. The rest are handled by the forwarder.
		clientW, clientR := net.Pipe()
		serverW, serverR := net.Pipe()

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

		// We don't close clientW and serverR here since we have no control
		// over those. The rest are handled by the forwarder.
		clientW, clientR := net.Pipe()
		serverW, serverR := net.Pipe()

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

	f := forward(context.Background(), p1, p2)
	defer f.Close()
	require.Nil(t, f.ctx.Err())

	f.Close()
	require.EqualError(t, f.ctx.Err(), context.Canceled.Error())
}

func TestForwarder_tryReportError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p1, p2 := net.Pipe()

	f := forward(context.Background(), p1, p2)
	defer f.Close()

	select {
	case err := <-f.errCh:
		t.Fatalf("expected no error, but got %v", err)
	default:
		// We are good.
	}

	// Report an error.
	f.tryReportError(errors.New("foo"))

	select {
	case err := <-f.errCh:
		require.EqualError(t, err, "foo")
	default:
		t.Fatal("expected error, but got none")
	}

	// Forwarder should be closed.
	_, err := p1.Write([]byte("foobarbaz"))
	require.Regexp(t, "closed pipe", err)
	require.EqualError(t, f.ctx.Err(), context.Canceled.Error())
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

func TestSuspendResumeProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("context_cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		clientProxy, serverProxy := net.Pipe()
		defer clientProxy.Close()
		defer serverProxy.Close()

		p := newProcessor(
			interceptor.NewPGConn(clientProxy),
			interceptor.NewPGConn(serverProxy),
		)
		require.EqualError(t, p.resume(ctx), context.Canceled.Error())

		// Set resumed to true to simulate suspend loop.
		p.mu.Lock()
		p.mu.resumed = true
		p.mu.Unlock()
		require.EqualError(t, p.suspend(ctx), context.Canceled.Error())
	})

	// This tests that resume() and suspend() can be called multiple times.
	// As an aside, we also check that we can suspend when blocked on PeekMsg
	// because there are no messages.
	t.Run("already_resumed_or_suspended", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		clientProxy, serverProxy := net.Pipe()
		defer clientProxy.Close()
		defer serverProxy.Close()

		p := newProcessor(
			interceptor.NewPGConn(clientProxy),
			interceptor.NewPGConn(serverProxy),
		)

		// Ensure that everything will return a resumed error except 1.
		errCh := make(chan error, 2)
		go func() { errCh <- p.resume(ctx) }()
		go func() { errCh <- p.resume(ctx) }()
		go func() { errCh <- p.resume(ctx) }()
		err := <-errCh
		require.EqualError(t, err, errProcessorResumed.Error())
		err = <-errCh
		require.EqualError(t, err, errProcessorResumed.Error())

		// Suspend the last goroutine.
		err = p.suspend(ctx)
		require.NoError(t, err)

		// Validate suspension.
		err = <-errCh
		require.Nil(t, err)
		p.mu.Lock()
		require.False(t, p.mu.resumed)
		require.False(t, p.mu.inPeek)
		require.False(t, p.mu.suspendReq)
		p.mu.Unlock()

		// Suspend a second time.
		err = p.suspend(ctx)
		require.NoError(t, err)

		// If already suspended, do nothing.
		p.mu.Lock()
		require.False(t, p.mu.resumed)
		require.False(t, p.mu.inPeek)
		require.False(t, p.mu.suspendReq)
		p.mu.Unlock()
	})

	// Multiple suspend/resumes calls should not cause issues. ForwardMsg should
	// not be interrupted.
	t.Run("multiple_resume_suspend_race", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		clientProxy, client := net.Pipe()
		defer clientProxy.Close()
		defer client.Close()
		serverProxy, server := net.Pipe()
		defer serverProxy.Close()
		defer server.Close()

		p := newProcessor(
			interceptor.NewPGConn(clientProxy),
			interceptor.NewPGConn(serverProxy),
		)

		const (
			queryCount  = 100
			concurrency = 200
		)

		// Client writes messages to be forwarded.
		buf := new(bytes.Buffer)
		q := (&pgproto3.Query{String: "SELECT 1"}).Encode(nil)
		for i := 0; i < queryCount; i++ {
			// Alternate between SELECT 1 and 2 to ensure correctness.
			if i%2 == 0 {
				q[12] = '1'
			} else {
				q[12] = '2'
			}
			_, _ = buf.Write(q)
		}
		go func() {
			// Simulate slow writes by writing byte by byte.
			_, _ = io.Copy(client, iotest.OneByteReader(buf))
		}()

		// Server reads messages that are forwarded.
		msgCh := make(chan pgproto3.FrontendMessage, queryCount)
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

		// Start the suspend/resume calls.
		errResumeCh := make(chan error, concurrency)
		errSuspendCh := make(chan error, concurrency)
		for i := 1; i <= concurrency; i++ {
			go func(p *processor, i int) {
				time.Sleep(jitteredInterval(time.Duration((i*2)+500) * time.Millisecond))
				errResumeCh <- p.resume(ctx)
			}(p, i)
			go func(p *processor, i int) {
				time.Sleep(jitteredInterval(time.Duration((i*2)+500) * time.Millisecond))
				errSuspendCh <- p.suspend(ctx)
			}(p, i)
		}

		// Wait until all resume calls except 1 have returned.
		for i := 0; i < concurrency-1; i++ {
			err := <-errResumeCh
			// If error is not nil, it has to be an already resumed error.
			if err != nil {
				require.EqualError(t, err, errProcessorResumed.Error())
			}
		}

		// Wait until the last one returns. We can guarantee that this is for
		// the last resume because all the other resume calls have returned.
		var lastErr error
		require.Eventually(t, func() bool {
			select {
			case lastErr = <-errResumeCh:
				return true
			default:
				_ = p.suspend(ctx)
				return false
			}
		}, 10*time.Second, 100*time.Millisecond)
		// If error is not nil, it has to be an already resumed error. This
		// would happen if suspend was the last to be called within all those
		// suspend/resume calls.
		if lastErr != nil {
			require.EqualError(t, lastErr, errProcessorResumed.Error())
		}

		// Wait until all initial suspend calls have returned.
		for i := 0; i < concurrency; i++ {
			err := <-errSuspendCh
			// If error is not nil, it has to be a suspend in-progress error.
			if err != nil {
				require.EqualError(t, err, errSuspendInProgress.Error())
			}
		}

		// At this point, we know that all pending resume and suspend calls
		// have returned. Run the final resumption to make sure all packets
		// have been forwarded.
		go func(p *processor) { _ = p.resume(ctx) }(p)

		// Now read all the messages on the server for correctness.
		for i := 0; i < queryCount; i++ {
			msg := <-msgCh
			q := msg.(*pgproto3.Query)

			expectedStr := "SELECT 1"
			if i%2 == 1 {
				expectedStr = "SELECT 2"
			}
			require.Equal(t, expectedStr, q.String)
		}

		// Suspend the final goroutine.
		err := p.suspend(ctx)
		require.NoError(t, err)
	})
}

// jitteredInterval returns a randomly jittered (+/-50%) duration from interval.
func jitteredInterval(interval time.Duration) time.Duration {
	return time.Duration(float64(interval) * (0.5 + 0.5*rand.Float64()))
}
