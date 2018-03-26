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

package pgwire

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

func makeTestV3Conn(c net.Conn) v3Conn {
	metrics := makeServerMetrics(nil, metric.TestSampleInterval)
	st := cluster.MakeTestingClusterSettings()
	mon := mon.MakeUnlimitedMonitor(
		context.Background(), "test", mon.MemoryResource, nil, nil, 1000, st,
	)
	physicalClock := func() int64 { return timeutil.Now().UnixNano() }
	exec := sql.NewExecutor(
		sql.ExecutorConfig{
			AmbientCtx:              log.AmbientContext{Tracer: st.Tracer},
			Settings:                st,
			HistogramWindowInterval: metric.TestSampleInterval,
			TestingKnobs:            &sql.ExecutorTestingKnobs{},
			SessionRegistry:         sql.MakeSessionRegistry(),
			Clock:                   hlc.NewClock(physicalClock, 1),
			NodeInfo: sql.NodeInfo{
				NodeID: &base.NodeIDContainer{},
			},
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
		// This byte string sends a pgwirebase.ClientMsgClose message type. When
		// ReadBuffer.readUntypedMsg is called, the 4 bytes is subtracted
		// from the size, leaving a 0-length ReadBuffer. Following this,
		// handleClose is called with the empty buffer, which calls
		// getPrepareType. Previously, getPrepareType would crash on an
		// empty buffer. This is now fixed.
		{byte(pgwirebase.ClientMsgClose), 0x00, 0x00, 0x00, 0x04},
		// This byte string exploited the same bug using a pgwirebase.ClientMsgDescribe
		// message type.
		{byte(pgwirebase.ClientMsgDescribe), 0x00, 0x00, 0x00, 0x04},
		// This would cause ReadBuffer.getInt16 to overflow, resulting in a
		// negative value being used for an allocation size.
		{byte(pgwirebase.ClientMsgParse), 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0xff, 0xff},
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
		_, _ = w.Write([]byte{byte(pgwirebase.ClientMsgSync), 0x00, 0x00, 0x00, 0x04})
		_, _ = w.Write([]byte{byte(pgwirebase.ClientMsgTerminate), 0x00, 0x00, 0x00, 0x04})
	}()

	v3Conn := makeTestV3Conn(r)
	defer v3Conn.finish(context.Background())
	_ = v3Conn.serve(
		context.Background(),
		func() bool { return false }, /* draining */
		mon.BoundAccount{},
	)
}

// TestReadTimeoutConn asserts that a readTimeoutConn performs reads normally
// and exits with an appropriate error when exit conditions are satisfied.
func TestReadTimeoutConnExits(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	ctx, cancel := context.WithCancel(context.Background())
	expectedRead := []byte("expectedRead")

	// Start a goroutine that performs reads using a readTimeoutConn.
	errChan := make(chan error)
	go func() {
		defer close(errChan)
		errChan <- func() error {
			c, err := ln.Accept()
			if err != nil {
				return err
			}
			defer c.Close()

			readTimeoutConn := newReadTimeoutConn(c, ctx.Err)
			// Assert that reads are performed normally.
			readBytes := make([]byte, len(expectedRead))
			if _, err := readTimeoutConn.Read(readBytes); err != nil {
				return err
			}
			if !bytes.Equal(readBytes, expectedRead) {
				return errors.Errorf("expected %v got %v", expectedRead, readBytes)
			}

			// The main goroutine will cancel the context, which should abort
			// this read with an appropriate error.
			_, err = readTimeoutConn.Read(make([]byte, 1))
			return err
		}()
	}()

	c, err := net.Dial(ln.Addr().Network(), ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.Write(expectedRead); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errChan:
		t.Fatalf("goroutine unexpectedly returned: %v", err)
	default:
	}
	cancel()
	if err := <-errChan; err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}
}
