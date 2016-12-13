// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package pgwire

import (
	"io"
	"io/ioutil"
	"net"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

func makeTestV3Conn(c net.Conn) v3Conn {
	metrics := makeServerMetrics(nil)
	mon := mon.MakeUnlimitedMonitor(context.Background(), "test", nil, nil, 1000)
	exec := sql.NewExecutor(
		sql.ExecutorConfig{
			AmbientCtx:            log.AmbientContext{Tracer: tracing.NewTracer()},
			MetricsSampleInterval: metric.TestSampleInterval,
		},
		nil, /* stopper */
	)
	return makeV3Conn(c, &metrics, &mon, exec)
}

// TestMaliciousInputs verifies that known malicious inputs sent to
// a v3Conn don't crash the server.
func TestMaliciousInputs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, data := range [][]byte{
		// This byte string sends a clientMsgClose message type. When
		// readBuffer.readUntypedMsg is called, the 4 bytes is subtracted
		// from the size, leaving a 0-length readBuffer. Following this,
		// handleClose is called with the empty buffer, which calls
		// getPrepareType. Previously, getPrepareType would crash on an
		// empty buffer. This is now fixed.
		{byte(clientMsgClose), 0x00, 0x00, 0x00, 0x04},
		// This byte string exploited the same bug using a clientMsgDescribe
		// message type.
		{byte(clientMsgDescribe), 0x00, 0x00, 0x00, 0x04},
		// This would cause readBuffer.getInt16 to overflow, resulting in a
		// negative value being used for an allocation size.
		{byte(clientMsgParse), 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0xff, 0xff},
	} {
		testMaliciousInput(t, data)
	}
}

func testMaliciousInput(t *testing.T, data []byte) {
	w, r := net.Pipe()
	defer w.Close()
	defer r.Close()

	go func() {
		// This io.Copy will discard all bytes from w until w is closed.
		// This is needed because sends on the net.Pipe are synchronous, so
		// the v3Conn will block if we don't read whatever it tries to send.
		// The reason this works is that ioutil.devNull implements ReadFrom
		// as an infinite loop, so it will Read continuously until it hits an
		// error (on w.Close()).
		_, _ = io.Copy(ioutil.Discard, w)
	}()

	go func() {
		// Write the malicious data.
		if _, err := w.Write(data); err != nil {
			panic(err)
		}

		// Sync and terminate if a panic did not occur to stop the server.
		// We append a 4-byte trailer to each to signify a zero length message. See
		// lib/pq.conn.sendSimpleMessage for a similar approach to simple messages.
		_, _ = w.Write([]byte{byte(clientMsgSync), 0x00, 0x00, 0x00, 0x04})
		_, _ = w.Write([]byte{byte(clientMsgTerminate), 0x00, 0x00, 0x00, 0x04})
	}()

	v3Conn := makeTestV3Conn(r)
	defer v3Conn.finish(context.Background())
	_ = v3Conn.serve(context.Background(), mon.BoundAccount{})
}

// TestContextCancellationClosesConn starts a pgwire client that checks that a
// v3Conn is blocked on the read loop and then cancels the v3Conn's session's
// context. This should result in v3Conn.serve(...) returning an error
// specifying that the associated session's context was canceled.
func TestContextCancellationClosesConn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())

	// Cannot use net.Pipe because deadlines are not supported.
	ln, err := net.Listen(util.TestAddr.Network(), util.TestAddr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ln.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	serverErr := make(chan error)
	go func() {
		defer close(serverErr)
		// c is closed by v3Conn.finish(...)
		c, err := ln.Accept()
		if err != nil {
			// Force main goroutine to fail.
			_ = ln.Close()
			return
		}

		v3Conn := makeTestV3Conn(c)
		defer v3Conn.finish(ctx)
		serverErr <- v3Conn.serve(ctx, mon.BoundAccount{})
	}()

	c, err := net.Dial(ln.Addr().Network(), ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	response := make([]byte, 1, 1)
	// Skip over initialization messages sent by v3Conn.
	for serverMessageType(response[0]) != serverMsgReady {
		if _, err := c.Read(response); err != nil {
			t.Fatal(err)
		}
	}

	// Skip over the rest of the serverMsgReady message.
	_, _ = c.Read(make([]byte, 5, 5))

	// clientMsgSync is used here because it is a simple, parameterless
	// message that expects a serverReadyMsg as response. It is usually sent
	// for error recovery during a series of extended-query messages.
	if _, err := c.Write([]byte{byte(clientMsgSync), 0x00, 0x00, 0x00, 0x04}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Read(response); err != nil {
		t.Fatal(err)
	}
	if serverMessageType(response[0]) != serverMsgReady {
		t.Fatalf("unexpected response to sync request: %s", serverMessageType(response[0]))
	}
	cancel()
	if err := <-serverErr; err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}
}
