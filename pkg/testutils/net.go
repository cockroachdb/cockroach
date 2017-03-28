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
	"errors"
	"io"
	"net"
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// bufferSize is the size of the buffer used by PartitionableConn. Writes to a
// partitioned connection will block after the buffer gets filled.
const bufferSize = 16 << 10 // 16 KB

// PartitionableConn is an implementation of net.Conn that allows the
// client->server and/or the server->client directions to be temporarily
// partitioned.
//
// A PartitionableConn wraps a provided net.Conn (the serverConn member) and
// forwards every read and write to it. It interposes an arbiter in front of it
// that's used to block reads/writes while the PartitionableConn is in the
// partitioned mode.
//
// While a direction is partitioned, data sent in that direction doesn't flow. A
// write while partitioned will block after an internal buffer gets filled. Data
// written to the conn after the partition has been established is not delivered
// to the remote party until the partition is lifted. At that time, all the
// buffered data is delivered. Since data is delivered async, data written
// before the partition is established may or may not be blocked by the
// partition; use application-level ACKs if that's important.
type PartitionableConn struct {
	// We embed a net.Conn so that we inherit the interface. Note that we override
	// Read() and Write().
	//
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

		c2sBuffer buf
		s2cBuffer buf

		c2sWaiter *sync.Cond
		s2cWaiter *sync.Cond
	}
}

type buf struct {
	// A mutex used to synchronize access to all the fields. It will be set to the
	// parent PartitionableConn's mutex.
	*syncutil.Mutex

	data     []byte
	capacity int
	closed   bool
	// The error that caused the buffer to be closed.
	closedErr error
	name      string // A human-readable name, useful for debugging.

	// wait is signaled when new data is put in the buffer, when data is taken out
	// of the buffer, when the buffer is closed and whenever the PartitionableConn
	// wants to unblock all reads (i.e. on partition).
	wait *sync.Cond
}

func makeBuf(name string, capacity int, mu *syncutil.Mutex) buf {
	b := buf{
		Mutex:    mu,
		name:     name,
		capacity: capacity,
	}
	b.wait = sync.NewCond(b.Mutex)
	return b
}

// Write adds data to the buffer. If there's zero free capacity, it will
// block. If there's non-zero insufficient capacity, it will perform a partial
// write.
//
// The number of bytes written is returned.
func (b *buf) Write(data []byte) (int, error) {
	b.Lock()
	for b.capacity == len(b.data) && !b.closed {
		// Block for capacity.
		b.wait.Wait()
	}
	if b.closed {
		return 0, b.closedErr
	}
	available := b.capacity - len(b.data)
	toCopy := available
	if len(data) < available {
		toCopy = len(data)
	}
	b.data = append(b.data, data[:toCopy]...)
	b.wait.Broadcast()
	b.Unlock()
	return toCopy, nil
}

// errEAgain is returned by buf.read() when the read was blocked at the time
// when buf.signal() was called - the signal interrupted the read. The caller is
// expected to try the read again after the partition is gone.
var errEAgain = errors.New("try read again")

// readLocked returns data from buf, up to "size" bytes.
func (b *buf) readLocked(size int) ([]byte, error) {
	if len(b.data) == 0 && !b.closed {
		b.wait.Wait()
		// We were unblocked either by data arrving, or by a partition, or by
		// another uninteresting reason. Return to the caller, in case it's because
		// of a partition.
		return nil, errEAgain
	}
	if b.closed && len(b.data) == 0 {
		return nil, b.closedErr
	}
	var ret []byte
	if len(b.data) < size {
		ret = b.data
		b.data = nil
	} else {
		ret = b.data[:size]
		b.data = b.data[size:]
	}
	b.wait.Broadcast()
	return ret, nil
}

func (b *buf) close(err error) {
	b.Lock()
	b.closed = true
	b.closedErr = err
	b.wait.Broadcast()
	b.Unlock()
}

// signalLocked interrupts all the reads that are currently blocked on the
// producer. This is used by PartitionableConn to signal that a partition has
// been created and therefor a blocked read call should be interrupted to give
// control back to the PartitionableConn.
//
// This needs to be called while holding the mutex that was used to create the
// buffer with.
func (b *buf) signalLocked() {
	b.wait.Broadcast()
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
	c.mu.c2sBuffer = makeBuf("c2sBuf", bufferSize, &c.mu.Mutex)
	c.mu.s2cBuffer = makeBuf("s2cBuf", bufferSize, &c.mu.Mutex)

	// Start copying from client to server.
	go func() {
		err := c.copy(
			c.clientConn, // src
			c.serverConn, // dst
			&c.mu.c2sBuffer,
			func() { // waitForNoPartitionLocked
				for c.mu.c2sPartitioned {
					c.mu.c2sWaiter.Wait()
				}
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
		err := c.copy(
			c.serverConn, // src
			c.clientConn, // dst
			&c.mu.s2cBuffer,
			func() { // waitForNoPartitionLocked
				for c.mu.s2cPartitioned {
					c.mu.s2cWaiter.Wait()
				}
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
	c.mu.c2sWaiter.Broadcast()
	c.mu.s2cPartitioned = false
	c.mu.s2cWaiter.Broadcast()
	c.mu.Unlock()
}

// PartitionC2S partitions the client-to-server direction.
// If UnpartitionC2S() is not called, Finish() must be called.
func (c *PartitionableConn) PartitionC2S() {
	c.mu.Lock()
	if c.mu.c2sPartitioned {
		panic("already partitioned")
	}
	c.mu.c2sPartitioned = true
	c.mu.c2sBuffer.signalLocked()
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
	if c.mu.s2cPartitioned {
		panic("already partitioned")
	}
	c.mu.s2cPartitioned = true
	c.mu.s2cBuffer.signalLocked()
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

// ReadFrom copies data from src into the buffer until src.Read() return an
// error (e.g. io.EOF).
func (b *buf) ReadFrom(src net.Conn) error {
	data := make([]byte, 1024)
	for {
		nr, err := src.Read(data)
		if err != nil {
			b.close(err)
			return err
		}
		toSend := data[:nr]
		for {
			nw, ew := b.Write(toSend)
			if ew != nil {
				return ew
			}
			if nw == len(toSend) {
				break
			}
			toSend = toSend[nw:]
		}
	}
}

// copyFromBuffer copies data from src to dst until src.Read() returns EOF.
// The EOF is returned (i.e. the return value is always != nil). This is because
// the PartitionableConn wants to hold on to any error, including EOF.
//
// waitForNoPartitionLocked is a function to be called before consuming data
// from src, in order to make sure that we only consume data when we're not
// partitioned. It needs to be called under src.Mutex, as the check needs to be
// done atomically with consuming the buffer's data.
func (c *PartitionableConn) copyFromBuffer(
	src *buf, dst net.Conn, waitForNoPartitionLocked func(),
) error {
	for {
		// Don't read from the buffer while we're partitioned.
		src.Mutex.Lock()
		waitForNoPartitionLocked()
		data, err := src.readLocked(1024 * 1024)
		src.Mutex.Unlock()

		if len(data) > 0 {
			nw, ew := dst.Write(data)
			if ew != nil {
				err = ew
			}
			if len(data) != nw {
				err = io.ErrShortWrite
			}
		} else if err == nil {
			err = io.EOF
		} else if err == errEAgain {
			continue
		}
		if err != nil {
			return err
		}
	}
}

// copy copies data from src to dst while we're not partitioned and stops doing
// so while partitioned.
//
// It runs two goroutines internally: one copying from src to an internal buffer
// and one copying from the buffer to dst. The 2nd one deals with partitions.
func (c *PartitionableConn) copy(
	src net.Conn, dst net.Conn, buf *buf, waitForNoPartitionLocked func(),
) error {
	tasks := make(chan error, 2)
	go func() {
		tasks <- buf.ReadFrom(src)
	}()
	go func() {
		tasks <- c.copyFromBuffer(buf, dst, waitForNoPartitionLocked)
	}()
	err := <-tasks
	err2 := <-tasks
	if err == nil {
		err = err2
	}
	return err
}
