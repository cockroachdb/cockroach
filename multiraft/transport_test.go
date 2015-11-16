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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package multiraft

import (
	"sync"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

type localTransport struct {
	mu        sync.Mutex
	listeners map[roachpb.StoreID]ServerInterface
}

// NewLocalTransport creates a minimal local in-memory transport. It
// has lower overhead than LocalRPCTransport, but does not catch all
// errors that LocalRPCTransport does.
func NewLocalTransport() Transport {
	return &localTransport{
		listeners: map[roachpb.StoreID]ServerInterface{},
	}
}

func (l *localTransport) Listen(id roachpb.StoreID, server ServerInterface) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.listeners[id] = server
	return nil
}

func (l *localTransport) Stop(id roachpb.StoreID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.listeners, id)
}

func (l *localTransport) Send(req *RaftMessageRequest) error {
	l.mu.Lock()
	s, ok := l.listeners[req.ToReplica.StoreID]
	l.mu.Unlock()
	if !ok {
		return util.Errorf("listener %d not found", req.ToReplica.StoreID)
	}
	_, err := s.RaftMessage(req)
	return err
}

func (*localTransport) Close() {
}

type localInterceptableTransport struct {
	mu        sync.Mutex
	listeners map[roachpb.StoreID]ServerInterface
	messages  chan *RaftMessageRequest
	Events    chan *interceptMessage
	stopper   *stop.Stopper
}

// NewLocalInterceptableTransport creates a Transport for local testing use.
// MultiRaft instances sharing the same instance of this Transport can find and
// communicate with each other by ID. Messages are transmitted in the order in
// which they are queued, intercepted and blocked until acknowledged.
func NewLocalInterceptableTransport(stopper *stop.Stopper) Transport {
	lt := &localInterceptableTransport{
		listeners: make(map[roachpb.StoreID]ServerInterface),
		messages:  make(chan *RaftMessageRequest, 100),
		Events:    make(chan *interceptMessage),
		stopper:   stopper,
	}
	lt.start()
	return lt
}

func (lt *localInterceptableTransport) start() {
	lt.stopper.RunWorker(func() {
		for {
			select {
			case msg := <-lt.messages:
				lt.stopper.RunTask(func() {
					lt.handleMessage(msg)
				})

			case <-lt.stopper.ShouldStop():
				return
			}
		}
	})
}

func (lt *localInterceptableTransport) handleMessage(msg *RaftMessageRequest) {
	ack := make(chan struct{})
	iMsg := &interceptMessage{
		args: msg,
		ack:  ack,
	}
	// The following channel ops are not protected by a select with
	// ShouldStop since we are running under a StartTask and leaving
	// things partially complete here could prevent other components
	// from shutting down cleanly.
	lt.Events <- iMsg
	<-ack
	lt.mu.Lock()
	srv, ok := lt.listeners[msg.ToReplica.StoreID]
	lt.mu.Unlock()
	if !ok {
		return
	}
	_, err := srv.RaftMessage(msg)
	if err == ErrStopped {
		return
	} else if err != nil {
		log.Fatal(err)
	}
}

func (lt *localInterceptableTransport) Listen(id roachpb.StoreID, server ServerInterface) error {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.listeners[id] = server
	return nil
}

func (lt *localInterceptableTransport) Stop(id roachpb.StoreID) {
	lt.mu.Lock()
	delete(lt.listeners, id)
	lt.mu.Unlock()
}

func (lt *localInterceptableTransport) Send(req *RaftMessageRequest) error {
	select {
	case lt.messages <- req:
	case <-lt.stopper.ShouldStop():
	}
	return nil
}

// an interceptMessage is sent by an interceptableClient when a message is to
// be sent.
type interceptMessage struct {
	args interface{}
	ack  chan<- struct{}
}

func (lt *localInterceptableTransport) Close() {
}
