// Copyright 2017 The Cockroach Authors.
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
// Author: Andrei Matei (andreimatei1@gmail.com)

package testutils

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/pkg/errors"
)

// RunEchoServer runs a network server that accepts all the connections from ln
// and echos the data sent on them.
//
// If serverSideCh != nil, every slice of data received by the server is sent
// also sent on this channel before being echoed back on the connection it came
// on. Useful to observe what the server has received when this server is used
// with partitioned connections.
func RunEchoServer(ln net.Listener, serverSideCh chan<- []byte) {
	for {
		conn, err := ln.Accept()
		netutil.FatalIfUnexpected(err)
		if err != nil {
			return
		}
		go handleEchoConnection(conn, serverSideCh)
	}
}

func handleEchoConnection(conn net.Conn, connSideCh chan<- []byte) {
	_, err := copyWithSideChan(conn, conn, connSideCh)

	if err != nil {
		log.Warning(context.TODO(), err)
	}
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
		RunEchoServer(ln, nil)
	}()
	defer func() {
		netutil.FatalIfUnexpected(ln.Close())
	}()

	serverConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	pConn := NewPartitionableConn(serverConn)

	exp := "let's see if this value comes back\n"
	fmt.Fprintf(pConn, exp)
	got, err := bufio.NewReader(pConn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if got != exp {
		t.Fatalf("expecting: %q , got %q", exp, got)
	}
	pConn.Close()
}

func TestPartitionableConnPartitionC2S(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("short flag")
	}

	addr := util.TestAddr
	ln, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		t.Fatal(err)
	}
	serverSideCh := make(chan []byte)
	go func() {
		RunEchoServer(ln, serverSideCh)
	}()
	defer func() {
		netutil.FatalIfUnexpected(ln.Close())
	}()

	serverConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	pConn := NewPartitionableConn(serverConn)

	// Partition the client->server connection. Afterwards, we're going to send
	// something and assert that the server doesn't get it (within a timeout) by
	// snooping on the server's side channel. Then we'll resolve the partition and
	// expect that the server gets the message that was pending and echoes it
	// back.

	pConn.PartitionC2S()

	// Client sends data.
	exp := "let's see when this value comes back\n"
	fmt.Fprintf(pConn, exp)

	// In the background, the client waits on a read.
	clientDoneCh := make(chan error)
	go func() {
		got, err := bufio.NewReader(pConn).ReadString('\n')
		if err != nil {
			clientDoneCh <- err
			return
		}
		if got != exp {
			clientDoneCh <- errors.Errorf("expecting: %q , got %q", exp, got)
			return
		}
		clientDoneCh <- nil
	}()

	timerDoneCh := make(chan error)
	time.AfterFunc(3*time.Millisecond, func() {
		var err error
		select {
		case err = <-clientDoneCh:
			err = errors.Errorf("unexpected reply while partitioned: %s", err)
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

	pConn.Close()
}

func TestPartitionableConnPartitionS2C(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("short flag")
	}

	addr := util.TestAddr
	ln, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		t.Fatal(err)
	}
	serverSideCh := make(chan []byte)
	go func() {
		RunEchoServer(ln, serverSideCh)
	}()
	defer func() {
		netutil.FatalIfUnexpected(ln.Close())
	}()

	serverConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	pConn := NewPartitionableConn(serverConn)

	// We're going to partition the server->client connection. Then we'll send
	// some data and assert that the server gets it (by snooping on the server's
	// side-channel). Then we'll assert that the client doesn't get the reply
	// (with a timeout). Then we resolve the partition and assert that the client
	// gets the reply.

	pConn.PartitionS2C()

	// Client sends data.
	exp := "let's see when this value comes back\n"
	fmt.Fprintf(pConn, exp)

	if s := <-serverSideCh; string(s) != exp {
		t.Fatalf("expected server to receive %q, got %q", exp, s)
	}

	// In the background, the client waits on a read.
	clientDoneCh := make(chan error)
	go func() {
		got, err := bufio.NewReader(pConn).ReadString('\n')
		if err != nil {
			clientDoneCh <- err
			return
		}
		if got != exp {
			clientDoneCh <- errors.Errorf("expecting: %q , got %q", exp, got)
			return
		}
		clientDoneCh <- nil
	}()

	// Check that the client does not get the server's response.
	time.AfterFunc(3*time.Millisecond, func() {
		select {
		case err := <-clientDoneCh:
			t.Fatalf("unexpected reply while partitioned: %s", err)
		default:
		}
	})

	// Now unpartition and expect the pending data to be sent and a reply to be
	// received.

	pConn.UnpartitionS2C()

	if err := <-clientDoneCh; err != nil {
		t.Fatal(err)
	}

	pConn.Close()
}
