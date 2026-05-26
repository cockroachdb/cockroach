// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlproxyccl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"testing"
	"testing/iotest"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

func TestForward(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	bgCtx := context.Background()

	t.Run("closed_when_processors_error", func(t *testing.T) {
		p1, p2 := net.Pipe()

		f := newForwarder(bgCtx, nil /* connector */, nil /* metrics */, nil /* timeSource */)
		defer f.Close()

		err := f.run(p1, p2)
		require.NoError(t, err)

		func() {
			f.mu.Lock()
			defer f.mu.Unlock()
			require.True(t, f.mu.isInitialized)
		}()

		// Close the connection right away to simulate processor error.
		p1.Close()

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

		// We don't close client and server here since we have no control
		// over those. The rest are handled by the forwarder.
		clientProxy, client := net.Pipe()
		serverProxy, server := net.Pipe()

		// Use a custom time source for testing.
		t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
		timeSource := timeutil.NewManualTime(t0)

		f := newForwarder(ctx, nil /* connector */, nil /* metrics */, timeSource)
		defer f.Close()

		err := f.run(clientProxy, serverProxy)
		require.NoError(t, err)
		require.Nil(t, f.ctx.Err())
		require.False(t, f.IsIdle())
		func() {
			f.mu.Lock()
			defer f.mu.Unlock()
			require.True(t, f.mu.isInitialized)
		}()

		f.mu.Lock()
		requestProc := f.mu.request
		f.mu.Unlock()

		initialClock := requestProc.logicalClockFn()
		barrierStart, barrierEnd := make(chan struct{}), make(chan struct{})
		requestProc.testingKnobs.beforeForwardMsg = func() {
			// Use two barriers here to ensure that we don't get a race when
			// asserting the last message.
			<-barrierStart
			<-barrierEnd
		}

		requestProc.mu.Lock()
		require.True(t, requestProc.mu.resumed)
		requestProc.mu.Unlock()

		// Client writes some pgwire messages.
		sendEventCh := make(chan struct{}, 1)
		errChan := make(chan error, 1)
		go func() {
			<-sendEventCh
			if buf, err := (&pgproto3.Query{
				String: "SELECT 1",
			}).Encode(nil); err != nil {
				errChan <- err
				return
			} else if _, err := client.Write(buf); err != nil {
				errChan <- err
				return
			}
			<-sendEventCh
			if buf, err := (&pgproto3.Execute{
				Portal:  "foobar",
				MaxRows: 42,
			}).Encode(nil); err != nil {
				errChan <- err
				return
			} else if _, err := client.Write(buf); err != nil {
				errChan <- err
				return
			}
			<-sendEventCh
			if buf, err := (&pgproto3.Close{
				ObjectType: 'P',
			}).Encode(nil); err != nil {
				errChan <- err
				return
			} else if _, err := client.Write(buf); err != nil {
				errChan <- err
				return
			}
		}()

		// Server should receive messages in order.
		backend := pgproto3.NewBackend(pgproto3.NewChunkReader(server), server)

		// Send SELECT 1. Before we send, advance the timesource, and forwarder
		// should be idle.
		timeSource.Advance(idleTimeout)
		require.True(t, f.IsIdle())
		sendEventCh <- struct{}{}

		barrierStart <- struct{}{}
		requestProc.mu.Lock()
		require.Equal(t, byte(pgwirebase.ClientMsgSimpleQuery), requestProc.mu.lastMessageType)
		require.Equal(t, initialClock+1, requestProc.mu.lastMessageTransferredAt)
		requestProc.mu.Unlock()
		require.False(t, f.IsIdle())
		barrierEnd <- struct{}{}

		// Server should receive SELECT 1.
		msg, err := backend.Receive()
		require.NoError(t, err)
		m1, ok := msg.(*pgproto3.Query)
		require.True(t, ok)
		require.Equal(t, "SELECT 1", m1.String)

		// Send Execute message. Before we send, advance the timesource, and
		// forwarder should be idle.
		timeSource.Advance(idleTimeout)
		require.True(t, f.IsIdle())
		sendEventCh <- struct{}{}

		barrierStart <- struct{}{}
		requestProc.mu.Lock()
		require.Equal(t, byte(pgwirebase.ClientMsgExecute), requestProc.mu.lastMessageType)
		require.Equal(t, initialClock+2, requestProc.mu.lastMessageTransferredAt)
		requestProc.mu.Unlock()
		require.False(t, f.IsIdle())
		barrierEnd <- struct{}{}

		// Server receives Execute.
		msg, err = backend.Receive()
		require.NoError(t, err)
		m2, ok := msg.(*pgproto3.Execute)
		require.True(t, ok)
		require.Equal(t, "foobar", m2.Portal)
		require.Equal(t, uint32(42), m2.MaxRows)

		// Send Close message. Before we send, advance the timesource, and
		// forwarder should be idle.
		timeSource.Advance(idleTimeout)
		require.True(t, f.IsIdle())
		sendEventCh <- struct{}{}

		barrierStart <- struct{}{}
		requestProc.mu.Lock()
		require.Equal(t, byte(pgwirebase.ClientMsgClose), requestProc.mu.lastMessageType)
		require.Equal(t, initialClock+3, requestProc.mu.lastMessageTransferredAt)
		requestProc.mu.Unlock()
		require.False(t, f.IsIdle())
		barrierEnd <- struct{}{}

		// Server receives Close.
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

		// We don't close client and server here since we have no control
		// over those. The rest are handled by the forwarder.
		clientProxy, client := net.Pipe()
		serverProxy, server := net.Pipe()

		// Use a custom time source for testing.
		t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
		timeSource := timeutil.NewManualTime(t0)

		f := newForwarder(ctx, nil /* connector */, nil /* metrics */, timeSource)
		defer f.Close()

		err := f.run(clientProxy, serverProxy)
		require.NoError(t, err)
		require.Nil(t, f.ctx.Err())
		require.False(t, f.IsIdle())
		func() {
			f.mu.Lock()
			defer f.mu.Unlock()
			require.True(t, f.mu.isInitialized)
		}()

		f.mu.Lock()
		responseProc := f.mu.response
		f.mu.Unlock()

		initialClock := responseProc.logicalClockFn()
		barrierStart, barrierEnd := make(chan struct{}), make(chan struct{})
		responseProc.testingKnobs.beforeForwardMsg = func() {
			// Use two barriers here to ensure that we don't get a race when
			// asserting the last message.
			<-barrierStart
			<-barrierEnd
		}

		responseProc.mu.Lock()
		require.True(t, responseProc.mu.resumed)
		responseProc.mu.Unlock()

		// Server writes some pgwire messages.
		recvEventCh := make(chan struct{}, 1)
		errChan := make(chan error, 1)
		go func() {
			<-recvEventCh
			if buf, err := (&pgproto3.ErrorResponse{
				Code:    "100",
				Message: "foobarbaz",
			}).Encode(nil); err != nil {
				errChan <- err
				return
			} else if _, err := server.Write(buf); err != nil {
				errChan <- err
				return
			}
			<-recvEventCh
			if buf, err := (&pgproto3.ReadyForQuery{
				TxStatus: 'I',
			}).Encode(nil); err != nil {
				errChan <- err
				return
			} else if _, err := server.Write(buf); err != nil {
				errChan <- err
				return
			}
		}()

		// Client should receive messages in order.
		frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(client), client)

		// Receive an ErrorResponse. Before we receive, advance the timesource,
		// and forwarder should be idle.
		timeSource.Advance(idleTimeout)
		require.True(t, f.IsIdle())
		recvEventCh <- struct{}{}

		barrierStart <- struct{}{}
		responseProc.mu.Lock()
		require.Equal(t, byte(pgwirebase.ServerMsgErrorResponse), responseProc.mu.lastMessageType)
		require.Equal(t, initialClock+1, responseProc.mu.lastMessageTransferredAt)
		responseProc.mu.Unlock()
		require.False(t, f.IsIdle())
		barrierEnd <- struct{}{}

		// Client receives ErrorResponse.
		msg, err := frontend.Receive()
		require.NoError(t, err)
		m1, ok := msg.(*pgproto3.ErrorResponse)
		require.True(t, ok)
		require.Equal(t, "100", m1.Code)
		require.Equal(t, "foobarbaz", m1.Message)

		// Receive a ReadyForQuery. Before we receive, advance the timesource,
		// and forwarder should be idle.
		timeSource.Advance(idleTimeout)
		require.True(t, f.IsIdle())
		recvEventCh <- struct{}{}

		barrierStart <- struct{}{}
		responseProc.mu.Lock()
		require.Equal(t, byte(pgwirebase.ServerMsgReady), responseProc.mu.lastMessageType)
		require.Equal(t, initialClock+2, responseProc.mu.lastMessageTransferredAt)
		responseProc.mu.Unlock()
		require.False(t, f.IsIdle())
		barrierEnd <- struct{}{}

		// Client receives ReadyForQuery.
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

func TestForwarder_Context(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	ctx := logtags.AddTag(context.Background(), "foo", "bar")
	f := newForwarder(ctx, nil /* connector */, nil /* metrics */, nil /* timeSource */)
	defer f.Close()

	// Check that the right context was returned.
	b := logtags.FromContext(f.Context())
	require.NotNil(t, b)
	require.Equal(t, "foo=bar", b.String())
}

func TestForwarder_Close(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	ctx := context.Background()
	for _, withRun := range []bool{true, false} {
		t.Run(fmt.Sprintf("withRun=%t", withRun), func(t *testing.T) {
			f := newForwarder(ctx, nil /* connector */, nil /* metrics */, nil /* timeSource */)
			defer f.Close()

			if withRun {
				p1, p2 := net.Pipe()
				err := f.run(p1, p2)
				require.NoError(t, err)
			}

			require.Nil(t, f.ctx.Err())
			f.Close()
			require.EqualError(t, f.ctx.Err(), context.Canceled.Error())
		})
	}
}

func TestForwarder_tryReportError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	ctx := context.Background()
	p1, p2 := net.Pipe()

	f := newForwarder(ctx, nil /* connector */, nil /* metrics */, nil /* timeSource */)
	defer f.Close()

	err := f.run(p1, p2)
	require.NoError(t, err)

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
	_, err = p1.Write([]byte("foobarbaz"))
	require.Regexp(t, "closed pipe", err)
	require.EqualError(t, f.ctx.Err(), context.Canceled.Error())
}

func TestForwarder_replaceServerConn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	ctx := context.Background()
	clientProxy, client := net.Pipe()
	serverProxy, server := net.Pipe()

	f := newForwarder(ctx, nil /* connector */, nil /* metrics */, nil /* timeSource */)
	defer f.Close()

	err := f.run(clientProxy, serverProxy)
	require.NoError(t, err)

	c, s := f.getConns()
	require.Equal(t, clientProxy, c.Conn)
	require.Equal(t, serverProxy, s.Conn)

	req, res := f.getProcessors()
	require.NoError(t, req.suspend(ctx))
	require.NoError(t, res.suspend(ctx))

	newServerProxy, newServer := net.Pipe()
	f.replaceServerConn(interceptor.NewPGConn(newServerProxy))
	require.NoError(t, f.resumeProcessors())

	// Check that we can receive messages from newServer.
	q, err := (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(nil)
	require.NoError(t, err)
	go func() {
		_, _ = newServer.Write(q)
	}()
	frontend := interceptor.NewFrontendConn(client)
	msg, err := frontend.ReadMsg()
	require.NoError(t, err)
	rmsg, ok := msg.(*pgproto3.ReadyForQuery)
	require.True(t, ok)
	require.Equal(t, byte('I'), rmsg.TxStatus)

	// Check that old connection was closed.
	_, err = server.Write([]byte("foobarbaz"))
	require.Regexp(t, "closed pipe", err)
}

func TestWrapClientToServerError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

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
		{errors.New("foo"), withCode(errors.New(
			"unexpected error copying from client to target server: foo"),
			codeClientDisconnected,
		)},
		{errors.Mark(errors.New("some write error"), errClientWrite),
			withCode(errors.New(
				"unable to write to client: some write error"),
				codeClientWriteFailed,
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
	testutilsccl.ServerlessOnly(t)

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
		{errors.New("foo"), withCode(errors.New(
			"unexpected error copying from target server to client: foo"),
			codeBackendDisconnected,
		)},
		{errors.Mark(errors.New("some read error"), errServerRead),
			withCode(errors.New(
				"unable to read from sql server: some read error"),
				codeBackendReadFailed,
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

func TestMakeLogicalClockFn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	clockFn := makeLogicalClockFn()

	require.Equal(t, uint64(1), clockFn())
	require.Equal(t, uint64(2), clockFn())
	require.Equal(t, uint64(3), clockFn())

	const concurrency = 200
	for i := 0; i < concurrency; i++ {
		go clockFn()
	}

	// Allow some time for the goroutines to complete.
	expected := uint64(3 + concurrency)
	testutils.SucceedsSoon(t, func() error {
		expected++
		clock := clockFn()
		if expected == clock {
			return nil
		}
		return errors.Newf("require clock=%d, but got %d", expected, clock)
	})
}

func TestSuspendResumeProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	t.Run("context_cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		clientProxy, serverProxy := net.Pipe()
		defer clientProxy.Close()
		defer serverProxy.Close()

		p := newProcessor(
			makeLogicalClockFn(),
			interceptor.NewPGConn(clientProxy),
			interceptor.NewPGConn(serverProxy),
		)
		require.EqualError(t, p.resume(ctx), context.Canceled.Error())
		p.mu.Lock()
		require.True(t, p.mu.closed)
		p.mu.Unlock()

		// Set resumed to true to simulate suspend loop.
		p.mu.Lock()
		p.mu.resumed = true
		p.mu.Unlock()
		require.EqualError(t, p.suspend(ctx), errProcessorClosed.Error())
	})

	t.Run("wait_for_resumed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		clientProxy, serverProxy := net.Pipe()
		defer clientProxy.Close()
		defer serverProxy.Close()

		p := newProcessor(
			makeLogicalClockFn(),
			interceptor.NewPGConn(clientProxy),
			interceptor.NewPGConn(serverProxy),
		)

		errCh := make(chan error)
		go func() {
			errCh <- p.waitResumed(ctx)
		}()

		select {
		case <-errCh:
			t.Fatal("expected not resumed")
		default:
			// We're good.
		}

		go func() { _ = p.resume(ctx) }()

		var lastErr error
		require.Eventually(t, func() bool {
			select {
			case lastErr = <-errCh:
				return true
			default:
				return false
			}
		}, 10*time.Second, 100*time.Millisecond)
		require.NoError(t, lastErr)
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
			makeLogicalClockFn(),
			interceptor.NewPGConn(clientProxy),
			interceptor.NewPGConn(serverProxy),
		)

		// Ensure that two resume calls will return right away.
		errCh := make(chan error, 2)
		go func() { errCh <- p.resume(ctx) }()
		go func() { errCh <- p.resume(ctx) }()
		go func() { errCh <- p.resume(ctx) }()
		err := <-errCh
		require.NoError(t, err)
		err = <-errCh
		require.NoError(t, err)

		// Suspend the last goroutine.
		err = p.waitResumed(ctx)
		require.NoError(t, err)
		err = p.suspend(ctx)
		require.NoError(t, err)

		// Validate suspension.
		err = <-errCh
		require.NoError(t, err)
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
			makeLogicalClockFn(),
			interceptor.NewPGConn(clientProxy),
			interceptor.NewPGConn(serverProxy),
		)

		const (
			queryCount  = 100
			concurrency = 200
		)

		// Client writes messages to be forwarded.
		buf := new(bytes.Buffer)
		q, err := (&pgproto3.Query{String: "SELECT 1"}).Encode(nil)
		require.NoError(t, err)
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
			require.NoError(t, err)
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
			require.NoError(t, err)
		}

		// At this point, we know that all pending resume and suspend calls
		// have returned. Run the final resumption to make sure all packets
		// have been forwarded.
		go func(p *processor) { _ = p.resume(ctx) }(p)

		err = p.waitResumed(ctx)
		require.NoError(t, err)

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
		err = p.suspend(ctx)
		require.NoError(t, err)
	})
}

// jitteredInterval returns a randomly jittered (+/-50%) duration from interval.
func jitteredInterval(interval time.Duration) time.Duration {
	return time.Duration(float64(interval) * (0.5 + 0.5*rand.Float64()))
}
