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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package rpc

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"reflect"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/rpc/codec"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/gogo/protobuf/proto"
)

type method struct {
	handler func(proto.Message, func(proto.Message, error))
	reqType reflect.Type
	public  bool
}

type serverResponse struct {
	req   rpc.Request
	reply proto.Message
	err   error
}

type syncAdapter func(proto.Message) (proto.Message, error)

func (s syncAdapter) exec(args proto.Message, callback func(proto.Message, error)) {
	go func() {
		callback(s(args))
	}()
}

// Server is a Cockroach-specific RPC server. By default it handles a simple
// heartbeat protocol to measure link health. It also supports close callbacks.
//
// TODO(spencer): heartbeat protocol should also measure link latency.
type Server struct {
	listener net.Listener // Server listener

	activeConns map[net.Conn]struct{}
	handler     http.Handler

	context *Context

	mu             sync.RWMutex          // Mutex protects the fields below
	addr           net.Addr              // Server address; may change if picking unused port
	closed         bool                  // Set upon invocation of Close()
	closeCallbacks []func(conn net.Conn) // Slice of callbacks to invoke on conn close
	methods        map[string]method
}

// NewServer creates a new instance of Server.
func NewServer(addr net.Addr, context *Context) *Server {
	s := &Server{
		context: context,
		addr:    addr,
		methods: map[string]method{},
	}
	heartbeat := &HeartbeatService{
		clock:              context.localClock,
		remoteClockMonitor: context.RemoteClocks,
	}
	if err := heartbeat.Register(s); err != nil {
		log.Fatalf("unable to register heartbeat service with RPC server: %s", err)
	}
	return s
}

// Register a new method handler. `name` is a qualified name of the
// form "Service.Name". `handler` is a function that takes an
// argument of the same type as `reqPrototype`. Both the argument and
// return value of 'handler' should be a pointer to a protocol message
// type. The handler function will be executed in a new goroutine.
// Only the "node" system user is allowed to use these endpoints.
func (s *Server) Register(name string,
	handler func(proto.Message) (proto.Message, error),
	reqPrototype proto.Message) error {
	return s.RegisterAsync(name, false /*not public*/, syncAdapter(handler).exec, reqPrototype)
}

// RegisterPublic is similar to Register, but allows non-system users.
func (s *Server) RegisterPublic(name string,
	handler func(proto.Message) (proto.Message, error),
	reqPrototype proto.Message) error {
	return s.RegisterAsync(name, true /*public*/, syncAdapter(handler).exec, reqPrototype)
}

// RegisterAsync registers an asynchronous method handler. Instead of
// returning a (proto.Message, error) tuple, an asynchronous handler
// receives a callback which it must execute when it is complete. Note
// that async handlers are started in the RPC server's goroutine and
// must not block (i.e. they must start a goroutine or write to a
// channel promptly). However, the fact that they are started in the
// RPC server's goroutine guarantees that the order of requests as
// they were read from the connection is preserved.
// If 'public' is true, all users may call this method, otherwise
// "node" users only.
func (s *Server) RegisterAsync(name string, public bool,
	handler func(proto.Message, func(proto.Message, error)),
	reqPrototype proto.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.methods[name]; ok {
		return util.Errorf("method %s already registered", name)
	}
	reqType := reflect.TypeOf(reqPrototype)
	if reqType.Kind() != reflect.Ptr {
		// net/rpc supports non-pointer requests, but we always use pointers
		// and things are a little simpler this way.
		return util.Errorf("request type not a pointer")
	}
	s.methods[name] = method{
		handler: handler,
		reqType: reqType,
		public:  public,
	}
	return nil
}

// AddCloseCallback adds a callback to the closeCallbacks slice to
// be invoked when a connection is closed.
func (s *Server) AddCloseCallback(cb func(conn net.Conn)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCallbacks = append(s.closeCallbacks, cb)
}

// Can connect to RPC service using HTTP CONNECT to rpcPath.
var connected = "200 Connected to Go RPC"

// ServeHTTP implements an http.Handler that answers RPC requests.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != rpc.DefaultRPCPath {
		if s.handler != nil {
			s.handler.ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
		return
	}

	// Note: this code was adapted from net/rpc.Server.ServeHTTP.
	if r.Method != "CONNECT" {
		http.Error(w, "405 must CONNECT", http.StatusMethodNotAllowed)
		return
	}

	// Construct an authentication hook for this security mode and TLS state.
	authHook, err := security.AuthenticationHook(s.context.Insecure, r.TLS)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Infof("rpc hijacking %s: %s", r.RemoteAddr, err)
		return
	}
	security.LogTLSState("RPC", r.TLS)
	if _, err := io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n"); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	codec := codec.NewServerCodec(conn)
	responses := make(chan serverResponse)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		s.sendResponses(codec, responses)
		wg.Done()
	}()
	s.readRequests(codec, authHook, responses)
	wg.Wait()

	codec.Close()

	s.mu.Lock()
	if s.closeCallbacks != nil {
		for _, cb := range s.closeCallbacks {
			cb(conn)
		}
	}
	s.mu.Unlock()
	conn.Close()
}

// Listen listens on the configured address but does not start
// accepting connections until Serve is called.
func (s *Server) Listen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tlsConfig, err := s.context.GetServerTLSConfig()
	if err != nil {
		return err
	}
	ln, err := tlsListen(s.addr.Network(), s.addr.String(), tlsConfig)
	if err != nil {
		return err
	}
	s.listener = ln

	addr, err := updatedAddr(s.addr, ln.Addr())
	if err != nil {
		s.Close()
		return err
	}
	s.addr = addr

	return nil
}

// Serve accepts and services connections on the already started
// listener.
func (s *Server) Serve(handler http.Handler) {
	s.handler = handler
	s.activeConns = make(map[net.Conn]struct{})

	server := &http.Server{
		Handler: s,
		ConnState: func(conn net.Conn, state http.ConnState) {
			s.mu.Lock()
			defer s.mu.Unlock()

			switch state {
			case http.StateNew:
				if s.closed {
					conn.Close()
					return
				}
				s.activeConns[conn] = struct{}{}
			case http.StateClosed:
				delete(s.activeConns, conn)
			}
		},
	}

	s.context.Stopper.RunWorker(func() {
		if err := server.Serve(s.listener); err != nil && !isClosedConnection(err) {
			log.Fatal(err)
		}
	})

	s.context.Stopper.RunWorker(func() {
		<-s.context.Stopper.ShouldStop()
		s.Close()
	})
}

// Start runs the RPC server. After this method returns, the socket
// will have been bound. Use Server.Addr() to ascertain server address.
func (s *Server) Start() error {
	if err := s.Listen(); err != nil {
		return err
	}
	s.Serve(s)
	return nil
}

// updatedAddr returns our "official" address based on the address we asked for
// (oldAddr) and the address we successfully bound to (newAddr). It's kind of
// hacky, but necessary to make TLS work.
func updatedAddr(oldAddr, newAddr net.Addr) (net.Addr, error) {
	switch oldAddr.Network() {
	case "tcp", "tcp4", "tcp6":
		// After binding, it's possible that our host and/or port will be
		// different from what we requested. If the hostname is different, we
		// want to keep the original one since it's more likely to match our
		// TLS certificate. But if the port is different, it should be because
		// we asked for ":0" and got an arbitrary unused port; that needs to be
		// reflected in our addr.
		host, oldPort, err := net.SplitHostPort(util.EnsureHost(oldAddr.String()))
		if err != nil {
			return nil, fmt.Errorf("unable to parse original addr '%s': %v",
				oldAddr.String(), err)
		}
		_, newPort, err := net.SplitHostPort(newAddr.String())
		if err != nil {
			return nil, fmt.Errorf("unable to parse new addr '%s': %v",
				newAddr.String(), err)
		}

		if newPort != oldPort && oldPort != "0" {
			log.Warningf("asked for port %s, got %s", oldPort, newPort)
		}

		return util.MakeUnresolvedAddr("tcp", net.JoinHostPort(host, newPort)), nil

	case "unix":
		if oldAddr.String() != newAddr.String() {
			return nil, fmt.Errorf("asked for unix addr %s, got %s", oldAddr, newAddr)
		}
		return newAddr, nil

	default:
		return nil, fmt.Errorf("unexpected network type: %s", oldAddr.Network())
	}
}

// Addr returns the server's network address.
func (s *Server) Addr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.addr
}

// Close closes the listener.
func (s *Server) Close() {
	// If the server didn't start properly, it might not have a listener.
	if s.listener != nil {
		s.listener.Close()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true

	for conn := range s.activeConns {
		conn.Close()
	}
}

// readRequests synchronously reads a stream of requests from a
// connection. Each request is handled in a new background goroutine;
// when the handler finishes the response is written to the responses
// channel. When the connection is closed (and any pending requests
// have finished), we close the responses channel.
func (s *Server) readRequests(codec rpc.ServerCodec, authHook func(proto.Message, bool) error, responses chan<- serverResponse) {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		close(responses)
	}()

	for {
		req, meth, args, err := s.readRequest(codec)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF || isClosedConnection(err) {
				return
			}
			log.Warningf("rpc: server cannot decode request: %s", err)
			return
		}

		if meth.handler == nil {
			responses <- serverResponse{
				req: req,
				err: util.Errorf("rpc: couldn't find method: %s", req.ServiceMethod),
			}
			continue
		}

		if err := authHook(args, meth.public); err != nil {
			responses <- serverResponse{
				req: req,
				err: err,
			}
			// We got an unauthorized request. For now, leave the connection
			// open. We may want to close it in the future because security.
			continue
		}

		wg.Add(1)
		meth.handler(args, func(reply proto.Message, err error) {
			responses <- serverResponse{
				req:   req,
				reply: reply,
				err:   err,
			}
			wg.Done()
		})
	}
}

// readRequest reads a single request from a connection.
func (s *Server) readRequest(codec rpc.ServerCodec) (rpc.Request, method, proto.Message, error) {
	var req rpc.Request
	if err := codec.ReadRequestHeader(&req); err != nil {
		return req, method{}, nil, err
	}

	s.mu.RLock()
	m, ok := s.methods[req.ServiceMethod]
	s.mu.RUnlock()

	// If we found the method, construct a request protobuf, parse into
	// it, and authenticate it.
	if ok {
		args := reflect.New(m.reqType.Elem()).Interface().(proto.Message)

		return req, m, args, codec.ReadRequestBody(args)
	}

	// If not, consume and discard the input by passing nil to ReadRequestBody.
	return req, m, nil, codec.ReadRequestBody(nil)
}

// sendResponses sends a stream of responses on a connection, and
// exits when the channel is closed.
func (s *Server) sendResponses(codec rpc.ServerCodec, responses <-chan serverResponse) {
	for resp := range responses {
		rpcResp := rpc.Response{
			ServiceMethod: resp.req.ServiceMethod,
			Seq:           resp.req.Seq,
		}
		if resp.err != nil {
			rpcResp.Error = resp.err.Error()
		}
		if err := codec.WriteResponse(&rpcResp, resp.reply); err != nil {
			log.Warningf("rpc: write response failed: %s", err)
			// TODO(bdarnell): what to do at this point? close the connection?
			// net/rpc just swallows the error.
		}
	}
}

// isClosedConnection returns true if err is the net package's errClosed.
func isClosedConnection(err error) bool {
	return err != nil && strings.HasSuffix(err.Error(), "use of closed network connection")
}
