// Copyright 2015 The Cockroach Authors.
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
// Author: Matt Tracy (matt@cockroachlabs.com)

package util

import (
	"sync"

	"github.com/cockroachdb/cockroach/util/stop"
)

// A Feed is used to publish a stream of events to a set of Subscribers. Events
// are values of arbitrary type, and are received and published as an empty
// interface. Each Subscriber will receive, in order, each entry published to
// the Feed.
//
// Entries are published by via the Publish method of the Feed. Slow
// subscribers can cause this to block, so it is the subscriber's
// responsibility not to congest the Feed.
//
// A Feed can be initialized by simply instantiating an empty feed object:
//
//		feed := NewFeed(stopper)
//	  feed.subscribe(func(event interface{}) {
// 		  doStuff(event)
//    })
//		feed.Publish(someEvent())
//
// The Feed does not keep historical events; individual Subscribers will only
// receive events published after they Subscribe.
type Feed struct {
	stopper *stop.Stopper
	ch      chan interface{}

	mu       sync.Mutex
	handlers []func(interface{})
}

// NewFeed returns a new Feed.
func NewFeed(stopper *stop.Stopper) *Feed {
	feed := Feed{
		stopper: stopper,
		ch:      make(chan interface{}),
	}

	feed.stopper.RunWorker(func() {
		for {
			select {
			case event := <-feed.ch:
				if ch, ok := event.(eof); ok {
					close(ch)
				} else {
					feed.dispatch(event)
				}
			case <-feed.stopper.ShouldStop():
				return
			}
		}
	})

	return &feed
}

type eof chan struct{}

// Publish publishes an event into the Feed, which will eventually be received by
// all Subscribers to the feed. Events published to a closed feed, or to a feed
// with no Subscribers, will be ignored.
func (f *Feed) Publish(event interface{}) {
	if f == nil {
		return
	}

	f.stopper.RunTask(func() {
		f.ch <- event
	})
}

func (f *Feed) dispatch(event interface{}) {
	f.mu.Lock()
	handlers := f.handlers
	f.mu.Unlock()

	for _, handler := range handlers {
		handler(event)
	}
}

// Subscribe registers a handler that will be called with each event
// published on the Feed.
func (f *Feed) Subscribe(handler func(event interface{})) {
	if f == nil {
		return
	}

	f.mu.Lock()
	f.handlers = append(f.handlers, handler)
	f.mu.Unlock()
}

// Flush blocks until all events published before the call to Flush are
// consumed.
func (f *Feed) Flush() {
	if f == nil {
		return
	}

	done := make(eof)
	f.Publish(done)
	<-done
}
