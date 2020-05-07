// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/errors"
)

// RunEchoServer runs a network server that accepts one connection from ln and
// echos the data sent on it.
//
// If serverSideCh != nil, every slice of data received by the server is also
// sent on this channel before being echoed back on the connection it came on.
// Useful to observe what the server has received when this server is used with
// partitioned connections.
func RunEchoServer(ln net.Listener, serverSideCh chan<- []byte) error {
	conn, err := ln.Accept()
	if err != nil {
		if grpcutil.IsClosedConnection(err) {
			return nil
		}
		return err
	}
	if _, err := copyWithSideChan(conn, conn, serverSideCh); err != nil {
		return err
	}
	return nil
}

// copyWithSideChan is like io.Copy(), but also takes a channel on which data
// read from src is sent before being written to dst.
func copyWithSideChan(dst io.Writer, src io.Reader, ch chan<- []byte) (written int64, err error) {
	buf := make([]byte, 32*1024)
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			if ch != nil {
				ch <- buf[:nr]
			}

			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}

func TestPartitionableConnBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	addr := util.TestAddr
	ln, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		if err := RunEchoServer(ln, nil); err != nil {
			t.Error(err)
		}
	}()
	defer func() {
		netutil.FatalIfUnexpected(ln.Close())
	}()

	serverConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	pConn := NewPartitionableConn(serverConn)
	defer pConn.Close()

	exp := "let's see if this value comes back\n"
	fmt.Fprint(pConn, exp)
	got, err := bufio.NewReader(pConn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if got != exp {
		t.Fatalf("expecting: %q , got %q", exp, got)
	}
}

func TestPartitionableConnPartitionC2S(t *testing.T) {
	defer leaktest.AfterTest(t)()

	addr := util.TestAddr
	ln, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		t.Fatal(err)
	}
	serverSideCh := make(chan []byte)
	go func() {
		if err := RunEchoServer(ln, serverSideCh); err != nil {
			t.Error(err)
		}
	}()
	defer func() {
		netutil.FatalIfUnexpected(ln.Close())
	}()

	serverConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	pConn := NewPartitionableConn(serverConn)
	defer pConn.Close()

	// Partition the client->server connection. Afterwards, we're going to send
	// something and assert that the server doesn't get it (within a timeout) by
	// snooping on the server's side channel. Then we'll resolve the partition and
	// expect that the server gets the message that was pending and echoes it
	// back.

	pConn.PartitionC2S()

	// Client sends data.
	exp := "let's see when this value comes back\n"
	fmt.Fprint(pConn, exp)

	// In the background, the client waits on a read.
	clientDoneCh := make(chan error)
	go func() {
		clientDoneCh <- func() error {
			got, err := bufio.NewReader(pConn).ReadString('\n')
			if err != nil {
				return err
			}
			if got != exp {
				return errors.Errorf("expecting: %q , got %q", exp, got)
			}
			return nil
		}()
	}()

	timerDoneCh := make(chan error)
	time.AfterFunc(3*time.Millisecond, func() {
		var err error
		select {
		case err = <-clientDoneCh:
			err = errors.Errorf("unexpected reply while partitioned: %v", err)
		case buf := <-serverSideCh:
			err = errors.Errorf("server was not supposed to have received data while partitioned: %q", buf)
		default:
		}
		timerDoneCh <- err
	})

	if err := <-timerDoneCh; err != nil {
		t.Fatal(err)
	}

	// Now unpartition and expect the pending data to be sent and a reply to be
	// received.

	pConn.UnpartitionC2S()

	// Expect the server to receive the data.
	<-serverSideCh

	if err := <-clientDoneCh; err != nil {
		t.Fatal(err)
	}
}

func TestPartitionableConnPartitionS2C(t *testing.T) {
	defer leaktest.AfterTest(t)()

	addr := util.TestAddr
	ln, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		t.Fatal(err)
	}
	serverSideCh := make(chan []byte)
	go func() {
		if err := RunEchoServer(ln, serverSideCh); err != nil {
			t.Error(err)
		}
	}()
	defer func() {
		netutil.FatalIfUnexpected(ln.Close())
	}()

	serverConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	pConn := NewPartitionableConn(serverConn)
	defer pConn.Close()

	// We're going to partition the server->client connection. Then we'll send
	// some data and assert that the server gets it (by snooping on the server's
	// side-channel). Then we'll assert that the client doesn't get the reply
	// (with a timeout). Then we resolve the partition and assert that the client
	// gets the reply.

	pConn.PartitionS2C()

	// Client sends data.
	exp := "let's see when this value comes back\n"
	fmt.Fprint(pConn, exp)

	if s := <-serverSideCh; string(s) != exp {
		t.Fatalf("expected server to receive %q, got %q", exp, s)
	}

	// In the background, the client waits on a read.
	clientDoneCh := make(chan error)
	go func() {
		clientDoneCh <- func() error {
			got, err := bufio.NewReader(pConn).ReadString('\n')
			if err != nil {
				return err
			}
			if got != exp {
				return errors.Errorf("expecting: %q , got %q", exp, got)
			}
			return nil
		}()
	}()

	// Check that the client does not get the server's response.
	time.AfterFunc(3*time.Millisecond, func() {
		select {
		case err := <-clientDoneCh:
			t.Errorf("unexpected reply while partitioned: %v", err)
		default:
		}
	})

	// Now unpartition and expect the pending data to be sent and a reply to be
	// received.

	pConn.UnpartitionS2C()

	if err := <-clientDoneCh; err != nil {
		t.Fatal(err)
	}
}

// Test that, while partitioned, a sender doesn't block while the internal
// buffer is not full.
func TestPartitionableConnBuffering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	addr := util.TestAddr
	ln, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		t.Fatal(err)
	}

	// In the background, the server reads everything.
	exp := 5 * (bufferSize / 10)
	serverDoneCh := make(chan error)
	go func() {
		serverDoneCh <- func() error {
			conn, err := ln.Accept()
			if err != nil {
				return err
			}
			received := 0
			for {
				data := make([]byte, 1024*1024)
				nr, err := conn.Read(data)
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}
				received += nr
			}
			if received != exp {
				return errors.Errorf("server expecting: %d , got %d", exp, received)
			}
			return nil
		}()
	}()

	serverConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	pConn := NewPartitionableConn(serverConn)
	defer pConn.Close()

	pConn.PartitionC2S()
	defer pConn.Finish()

	// Send chunks such that they don't add up to the buffer size exactly.
	data := make([]byte, bufferSize/10)
	for i := 0; i < 5; i++ {
		nw, err := pConn.Write(data)
		if err != nil {
			t.Fatal(err)
		}
		if nw != len(data) {
			t.Fatal("unexpected partial write; PartitionableConn always writes fully")
		}
	}
	pConn.UnpartitionC2S()
	pConn.Close()

	if err := <-serverDoneCh; err != nil {
		t.Fatal(err)
	}
}

// Test that, while partitioned, a party can close the connection and the other
// party will not observe this until after the partition is lifted.
func TestPartitionableConnCloseDeliveredAfterPartition(t *testing.T) {
	defer leaktest.AfterTest(t)()

	addr := util.TestAddr
	ln, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		t.Fatal(err)
	}

	// In the background, the server reads everything.
	serverDoneCh := make(chan error)
	go func() {
		serverDoneCh <- func() error {
			conn, err := ln.Accept()
			if err != nil {
				return err
			}
			received := 0
			for {
				data := make([]byte, 1<<20 /* 1 MiB */)
				nr, err := conn.Read(data)
				if err != nil {
					if err == io.EOF {
						return nil
					}
					return err
				}
				received += nr
			}
		}()
	}()

	serverConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	pConn := NewPartitionableConn(serverConn)
	defer pConn.Close()

	pConn.PartitionC2S()
	defer pConn.Finish()

	pConn.Close()

	timerDoneCh := make(chan error)
	time.AfterFunc(3*time.Millisecond, func() {
		var err error
		select {
		case err = <-serverDoneCh:
			err = errors.Wrap(err, "server was not supposed to see the closing while partitioned")
		default:
		}
		timerDoneCh <- err
	})

	if err := <-timerDoneCh; err != nil {
		t.Fatal(err)
	}

	pConn.UnpartitionC2S()

	if err := <-serverDoneCh; err != nil {
		t.Fatal(err)
	}
}
