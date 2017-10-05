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

package rpc

import (
	"bufio"
	"io"
	"net"
	"sync"

	"golang.org/x/net/context"
	"golang.org/x/net/http2"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const crpcVersion = "CRPC 1.0\n"

// CRPCMatch matches CRPC connections.
func CRPCMatch(rd io.Reader) bool {
	buf := make([]byte, len(crpcVersion))
	if n, err := io.ReadFull(rd, buf); err != nil || n != len(buf) {
		return false
	}
	return string(buf) == crpcVersion
}

type crpcServerResp struct {
	streamID uint32
	resp     protoutil.Message
	err      error
}

type crpcParser func([]byte) (protoutil.Message, error)
type crpcHandler func(protoutil.Message) (protoutil.Message, error)

type crpcServerConn struct {
	conn    net.Conn
	rd      *bufio.Reader
	wr      *bufio.Writer
	fr      *http2.Framer
	parser  crpcParser
	handler crpcHandler

	sender struct {
		syncutil.Mutex
		cond    sync.Cond
		closed  bool
		pending []crpcServerResp
	}
}

func newCRPCServerConn(conn net.Conn, parser crpcParser, handler crpcHandler) *crpcServerConn {
	c := &crpcServerConn{
		conn:    conn,
		rd:      bufio.NewReader(conn),
		wr:      bufio.NewWriter(conn),
		parser:  parser,
		handler: handler,
	}
	c.fr = http2.NewFramer(c.wr, c.rd)
	c.fr.SetReuseFrames()
	c.sender.cond.L = &c.sender.Mutex
	return c
}

func (c *crpcServerConn) readLoop() {
	defer c.Close()

	vers := make([]byte, len(crpcVersion))
	if _, err := io.ReadFull(c.rd, vers); err != nil {
		c.Close()
		return
	}

	// Only start the write loop after we've parsed the version.
	go c.writeLoop()
	ctx := context.Background()

	for {
		frame, err := c.fr.ReadFrame()
		if err != nil {
			log.Error(ctx, err)
			return
		}

		switch frame := frame.(type) {
		case *http2.DataFrame:
			req, err := c.parser(frame.Data())
			go func(streamID uint32, req protoutil.Message, err error) {
				var resp protoutil.Message
				if err == nil {
					resp, err = c.handler(req)
				}
				c.send(streamID, resp, err)
			}(frame.StreamID, req, err)
		default:
			log.Fatalf(ctx, "unhandled frame type %T: %v", frame, frame)
		}
	}
}

func (c *crpcServerConn) writeLoop() {
	defer c.Close()

	ctx := context.Background()
	s := &c.sender
	var tmpbuf []byte

	for {
		s.Lock()
		for len(s.pending) == 0 && !s.closed {
			s.cond.Wait()
		}
		if s.closed {
			s.Unlock()
			return
		}
		pending := s.pending
		s.pending = nil
		s.Unlock()

		for _, p := range pending {
			if p.resp != nil {
				size := p.resp.Size()
				if cap(tmpbuf) < size {
					tmpbuf = make([]byte, size)
				}
				tmpbuf = tmpbuf[:size]
				if _, err := protoutil.MarshalToWithoutFuzzing(p.resp, tmpbuf); err != nil {
					log.Warning(ctx, err)
					continue
				}
			} else {
				tmpbuf = tmpbuf[:0]
			}
			if err := c.fr.WriteData(p.streamID, true, tmpbuf); err != nil {
				log.Warning(ctx, err)
			}
		}

		if err := c.wr.Flush(); err != nil {
			log.Warning(ctx, err)
			return
		}
	}
}

func (c *crpcServerConn) send(streamID uint32, resp protoutil.Message, err error) {
	s := &c.sender
	s.Lock()
	s.pending = append(s.pending, crpcServerResp{
		streamID: streamID,
		resp:     resp,
		err:      err,
	})
	s.cond.Signal()
	s.Unlock()
}

// Close ...
func (c *crpcServerConn) Close() {
	c.sender.Lock()
	c.sender.closed = true
	c.sender.cond.Signal()
	c.sender.Unlock()
	if err := c.conn.Close(); err != nil {
		log.Warning(context.Background(), err)
	}
}

// CRPCServe ...
func CRPCServe(l net.Listener, parser crpcParser, handler crpcHandler) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		c := newCRPCServerConn(conn, parser, handler)
		go c.readLoop()
	}
}

type crpcClientReq struct {
	streamID uint32
	req      protoutil.Message
	resp     protoutil.Message
	err      error
	wg       sync.WaitGroup
}

// CRPCClientConn ...
type CRPCClientConn struct {
	conn net.Conn
	rd   *bufio.Reader
	wr   *bufio.Writer
	fr   *http2.Framer

	sender struct {
		syncutil.Mutex
		cond    sync.Cond
		closed  bool
		pending []*crpcClientReq
	}
	receiver struct {
		syncutil.Mutex
		nextID  uint32
		pending map[uint32]*crpcClientReq
	}
}

func newCRPCClientConn(conn net.Conn) *CRPCClientConn {
	c := &CRPCClientConn{
		conn: conn,
		rd:   bufio.NewReader(conn),
		wr:   bufio.NewWriter(conn),
	}
	c.fr = http2.NewFramer(c.wr, c.rd)
	c.fr.SetReuseFrames()
	c.sender.cond.L = &c.sender.Mutex
	c.receiver.nextID = 1
	c.receiver.pending = make(map[uint32]*crpcClientReq)
	go c.readLoop()
	go c.writeLoop()
	return c
}

func (c *CRPCClientConn) readLoop() {
	defer c.Close()

	r := &c.receiver
	ctx := context.Background()

	for {
		frame, err := c.fr.ReadFrame()
		if err != nil {
			log.Error(ctx, err)
			return
		}

		switch frame := frame.(type) {
		case *http2.DataFrame:
			r.Lock()
			p := r.pending[frame.StreamID]
			delete(r.pending, frame.StreamID)
			r.Unlock()
			p.err = protoutil.Unmarshal(frame.Data(), p.resp)
			p.wg.Done()
		default:
			log.Fatalf(ctx, "unhandled frame type %v.", frame)
		}
	}
}

func (c *CRPCClientConn) writeLoop() {
	defer c.Close()

	if _, err := c.conn.Write([]byte(crpcVersion)); err != nil {
		return
	}

	s := &c.sender
	var tmpbuf []byte

	for {
		s.Lock()
		for len(s.pending) == 0 && !s.closed {
			s.cond.Wait()
		}
		if s.closed {
			s.Unlock()
			return
		}
		pending := s.pending
		s.pending = nil
		s.Unlock()

		for _, p := range pending {
			size := p.req.Size()
			if cap(tmpbuf) < size {
				tmpbuf = make([]byte, size)
			}
			tmpbuf = tmpbuf[:size]
			if _, err := protoutil.MarshalToWithoutFuzzing(p.req, tmpbuf); err != nil {
				p.err = err
				p.wg.Done()
				continue
			}
			if err := c.fr.WriteData(p.streamID, true, tmpbuf); err != nil {
				return
			}
		}
		if err := c.wr.Flush(); err != nil {
			return
		}
	}
}

// Send ...
func (c *CRPCClientConn) Send(req, resp protoutil.Message) error {
	p := &crpcClientReq{req: req, resp: resp}
	p.wg.Add(1)

	r := &c.receiver
	r.Lock()
	p.streamID = r.nextID
	r.nextID++
	r.pending[p.streamID] = p
	r.Unlock()

	s := &c.sender
	s.Lock()
	s.pending = append(s.pending, p)
	s.cond.Signal()
	s.Unlock()

	p.wg.Wait()
	return p.err
}

// Close ...
func (c *CRPCClientConn) Close() {
	c.sender.Lock()
	c.sender.closed = true
	c.sender.cond.Signal()
	c.sender.Unlock()
	if err := c.conn.Close(); err != nil {
		log.Warning(context.Background(), err)
	}
}
