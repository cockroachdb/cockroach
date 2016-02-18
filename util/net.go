// Copyright 2014 The Cockroach Authors.
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
// Author: Tamir Duberstein (tamird@gmail.com)

package util

import (
	"bytes"
	"crypto/tls"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"

	"golang.org/x/net/http2"

	"github.com/cockroachdb/cockroach/util/stop"
)

const eol = "\r\n"
const hostHeader = eol + "Host: CRDB"

var http2ClientPrefaceFirstLineSlice = []byte(http2.ClientPreface[:strings.Index(http2.ClientPreface, eol)])

type replayableConn struct {
	net.Conn
	buf    bytes.Buffer
	reader io.Reader
}

func newReplayableConn(conn net.Conn) *replayableConn {
	rc := replayableConn{Conn: conn}
	rc.reader = io.LimitReader(io.TeeReader(conn, &rc.buf), int64(len(http2ClientPrefaceFirstLineSlice)))
	return &rc
}

func (rc *replayableConn) replay() *replayableConn {
	rc.reader = io.MultiReader(&rc.buf, rc.Conn)
	return rc
}

func (rc *replayableConn) Read(p []byte) (int, error) {
	if limitReader, ok := rc.reader.(*io.LimitedReader); ok {
		// rc.reader is a LimitedReader wrapping a TeeReader.
		off := int64(len(http2ClientPrefaceFirstLineSlice)) - limitReader.N
		n, err := rc.reader.Read(p)
		if !bytes.HasPrefix(http2ClientPrefaceFirstLineSlice[off:], p[:n]) {
			// The incoming request is not an HTTP2 request, so buffering is no
			// longer required; send all reads directly to the underlying net.Conn.
			rc.reader = rc.Conn
		} else if err == io.EOF {
			// We've exhausted our LimitedReader, which means he incoming request is
			// an HTTP2 request. Remember, that we are in this code path means that
			// TLS is not in use, which means the caller (net/http machinery) won't
			// be able to parse anything after http2ClientPrefaceFirstLineSlice.
			// However, Go 1.6 introduced strict Host header checking for HTTP >= 1.1
			// requests (see https://github.com/golang/go/commit/6e11f45), and
			// http2ClientPrefaceFirstLineSlice contains enough information for the
			// caller to identify this first request as an HTTP 2 request, but not
			// enough for the caller to determine the value of the Host header. On
			// the next line, we're going to help the caller out by providing a bogus
			// HTTP 1.x-style Host header. This will get us past Host header
			// verification.
			//
			// Note that this bogus header won't reappear after replay is called.
			rc.reader = io.MultiReader(strings.NewReader(hostHeader), limitReader.R)
		} else {
			// The LimitedReader isn't exhausted yet, or we hit an error.
			return n, err
		}

		// We've replaced our LimitedReader with rc.Conn or a new MultiReader - we
		// might be able to read some more.
		if err == io.EOF {
			var m int
			m, err = rc.reader.Read(p[n:])
			n += m
		}
		return n, err
	}
	// Pseudocode:
	// if rc.IsHTTP2() {
	// 	if rc.replayCalled {
	// 		rc.reader == io.MultiReader(&rc.buf, rc.Conn)
	// 	} else {
	// 		rc.reader == io.TeeReader(conn, &rc.buf)
	// 	}
	// } else {
	// 	rc.reader == rc.Conn
	// }
	return rc.reader.Read(p)
}

type replayableConnListener struct {
	net.Listener
}

func (ml *replayableConnListener) Accept() (net.Conn, error) {
	conn, err := ml.Listener.Accept()
	if err == nil {
		conn = newReplayableConn(conn)
	}
	return conn, err
}

// Listen delegates to `net.Listen` and, if tlsConfig is not nil, to `tls.NewListener`.
func Listen(addr net.Addr, tlsConfig *tls.Config) (net.Listener, error) {
	ln, err := net.Listen(addr.Network(), addr.String())
	if err == nil {
		if tlsConfig != nil {
			ln = tls.NewListener(ln, tlsConfig)
		} else {
			ln = &replayableConnListener{ln}
		}
	}

	return ln, err
}

// ListenAndServe creates a listener and serves handler on it, closing
// the listener when signalled by the stopper.
func ListenAndServe(stopper *stop.Stopper, handler http.Handler, addr net.Addr, tlsConfig *tls.Config) (net.Listener, error) {
	ln, err := Listen(addr, tlsConfig)
	if err != nil {
		return nil, err
	}

	var mu sync.Mutex
	activeConns := make(map[net.Conn]struct{})

	httpServer := http.Server{
		TLSConfig: tlsConfig,
		Handler:   handler,
		ConnState: func(conn net.Conn, state http.ConnState) {
			mu.Lock()
			switch state {
			case http.StateNew:
				activeConns[conn] = struct{}{}
			case http.StateClosed:
				delete(activeConns, conn)
			}
			mu.Unlock()
		},
	}

	var http2Server http2.Server

	if tlsConfig == nil {
		connOpts := http2.ServeConnOpts{
			BaseConfig: &httpServer,
			Handler:    handler,
		}

		httpServer.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ProtoMajor == 2 {
				if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
					http2Server.ServeConn(conn.(*replayableConn).replay(), &connOpts)
				} else {
					log.Fatal(err)
				}
			} else {
				handler.ServeHTTP(w, r)
			}
		})
	}

	if err := http2.ConfigureServer(&httpServer, &http2Server); err != nil {
		return nil, err
	}

	stopper.RunWorker(func() {
		<-stopper.ShouldDrain()
		// Some unit tests manually close `ln`, so it may already be closed
		// when we get here.
		if err := ln.Close(); err != nil && !IsClosedConnection(err) {
			log.Fatal(err)
		}
	})

	stopper.RunWorker(func() {
		if err := httpServer.Serve(ln); err != nil && !IsClosedConnection(err) {
			log.Fatal(err)
		}

		<-stopper.ShouldStop()

		mu.Lock()
		for conn := range activeConns {
			conn.Close()
		}
		mu.Unlock()
	})

	return ln, nil
}

// IsClosedConnection returns true if err is the net package's errClosed.
func IsClosedConnection(err error) bool {
	return strings.Contains(err.Error(), "use of closed network connection")
}
