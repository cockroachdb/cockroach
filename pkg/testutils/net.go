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
	"io"
	"net"
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// PartitionableConn is an implementation of net.Conn that allows the
// client->server and/or the server->client directions to be temporarily
// partitioned.
//
// A PartitionableConn wraps a provided net.Conn (the serverConn member) and
// pipes every read and write to it.
//
// While a direction is partitioned, data send in that direction doesn't flow. A
// write done while partitioned may block. Data written to the conn after the
// partition has been established is not delivered to the remote party until the
// partition is lifted. Data written before the partition is established may or
// may not be delivered: every write that returns before the partition except
// the last one will be delivered. The last one may or may not be; use
// application-level ACKs if that's important.
type PartitionableConn struct {
	// We embed a net.Conn so that we inherit the interface. Note that we override
	// Read() and Write() though.
	// This embedded Conn is half of a net.Pipe(). The other half is clientConn.
	net.Conn

	clientConn net.Conn
	serverConn net.Conn

	mu struct {
		syncutil.Mutex

		// err, if set, is returned by any subsequent call to Read or Write.
		err error

		// Are any of the two direction (client-to-server, server-to-client)
		// currently partitioned?
		c2sPartitioned bool
		s2cPartitioned bool

		c2sWaiter *sync.Cond
		s2cWaiter *sync.Cond
	}
}

// NewPartitionableConn wraps serverConn in a PartitionableConn.
func NewPartitionableConn(serverConn net.Conn) *PartitionableConn {
	clientEnd, clientConn := net.Pipe()
	c := &PartitionableConn{
		Conn:       clientEnd,
		clientConn: clientConn,
		serverConn: serverConn,
	}
	c.mu.c2sWaiter = sync.NewCond(&c.mu.Mutex)
	c.mu.s2cWaiter = sync.NewCond(&c.mu.Mutex)

	// Start copying from client to server.
	go func() {
		err := c.copy(copyArgs{
			src:         c.clientConn,
			dst:         c.serverConn,
			mu:          &c.mu.Mutex,
			partitioned: &c.mu.c2sPartitioned,
			wait:        c.mu.c2sWaiter,
		})
		c.mu.Lock()
		c.mu.err = err
		c.mu.Unlock()
		if err := c.clientConn.Close(); err != nil {
			log.Fatalf(context.TODO(), "unexpected error closing internal pipe: %s", err)
		}
		if err := c.serverConn.Close(); err != nil {
			log.Errorf(context.TODO(), "error closing server conn: %s", err)
		}
	}()

	// Start copying from server to client.
	go func() {
		err := c.copy(copyArgs{
			src:         c.serverConn,
			dst:         c.clientConn,
			mu:          &c.mu.Mutex,
			partitioned: &c.mu.s2cPartitioned,
			wait:        c.mu.s2cWaiter,
		})
		c.mu.Lock()
		c.mu.err = err
		c.mu.Unlock()
		if err := c.clientConn.Close(); err != nil {
			log.Fatalf(context.TODO(), "unexpected error closing internal pipe: %s", err)
		}
		if err := c.serverConn.Close(); err != nil {
			log.Errorf(context.TODO(), "error closing server conn: %s", err)
		}
	}()

	return c
}

// Finish removes any partitions that may exist so that blocked goroutines can
// finish.
// Finish() must be called if a connection may have been left in a partitioned
// state.
func (c *PartitionableConn) Finish() {
	c.mu.Lock()
	c.mu.c2sPartitioned = false
	c.mu.c2sWaiter.Signal()
	c.mu.s2cPartitioned = false
	c.mu.s2cWaiter.Signal()
	c.mu.Unlock()
}

// PartitionC2S partitions the client-to-server direction.
// If UnpartitionC2S() is not called, Finish() must be called.
func (c *PartitionableConn) PartitionC2S() {
	c.mu.Lock()
	c.mu.c2sPartitioned = true
	c.mu.Unlock()
}

// UnpartitionC2S lifts an existing client-to-server partition.
func (c *PartitionableConn) UnpartitionC2S() {
	c.mu.Lock()
	if !c.mu.c2sPartitioned {
		panic("not partitioned")
	}
	c.mu.c2sPartitioned = false
	c.mu.c2sWaiter.Signal()
	c.mu.Unlock()
}

// PartitionS2C partitions the server-to-client direction.
// If UnpartitionS2C() is not called, Finish() must be called.
func (c *PartitionableConn) PartitionS2C() {
	c.mu.Lock()
	c.mu.s2cPartitioned = true
	c.mu.Unlock()
}

// UnpartitionS2C lifts an existing server-to-client partition.
func (c *PartitionableConn) UnpartitionS2C() {
	c.mu.Lock()
	if !c.mu.s2cPartitioned {
		panic("not partitioned")
	}
	c.mu.s2cPartitioned = false
	c.mu.s2cWaiter.Signal()
	c.mu.Unlock()
}

// Read is part of the net.Conn interface.
func (c *PartitionableConn) Read(b []byte) (n int, err error) {
	c.mu.Lock()
	err = c.mu.err
	c.mu.Unlock()
	if err != nil {
		return 0, err
	}

	// Forward to the embedded connection.
	return c.Conn.Read(b)
}

// Write is part of the net.Conn interface.
func (c *PartitionableConn) Write(b []byte) (n int, err error) {
	c.mu.Lock()
	err = c.mu.err
	c.mu.Unlock()
	if err != nil {
		return 0, err
	}

	// Forward to the embedded connection.
	return c.Conn.Write(b)
}

type copyArgs struct {
	src net.Conn
	dst net.Conn

	mu          *syncutil.Mutex
	partitioned *bool
	// When partitioned is set, wait can be used to be signaled when the partition
	// is lifted. mu needs to be held before waiting.
	wait *sync.Cond
}

// copy copies data from args.src to args.dst until args.src.Read() returns EOF.
// The EOF is returned (i.e. the return value is always != nil). This is because
// the PartitionableConn wants to hold on to any error, including EOF.
func (c *PartitionableConn) copy(args copyArgs) error {
	buf := make([]byte, 1000)
	var written int64
	for {
		nr, err := args.src.Read(buf)

		args.mu.Lock()
		for *args.partitioned {
			args.wait.Wait()
		}
		args.mu.Unlock()

		if nr > 0 {
			nw, ew := args.dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
			}
			if nr != nw {
				err = io.ErrShortWrite
			}
		}
		if err != nil {
			return err
		}
	}
}
