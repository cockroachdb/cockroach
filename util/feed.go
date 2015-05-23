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
	"fmt"
	"sync"
)

// subscriptionChannelSize is the buffer size of the Events channel for each
// subscriber. This value is fairly large for a channel, and is used to ensure
// that writing to a subscriber channel will not block under normal
// circumstances.
const subscriptionChannelSize = 2048

// A Subscription is used to receive events from a specific Feed. A Subscription
// should only be instantiated via a call to a Feed's Subscribe() method. Once
// created, events can be read directly from the Events channel provided by this
// structure.
//
// An example of a typical usage of a Subscription:
//
//		subscriber := feed.Subscribe()
//		for event := range subscriber.Events() {
//			// Process event...
//		}
//
// A Subscription cannot block other Subscriptions to the same feed, and each
// Subscription will receive all events published by the Feed. The user of a
// Subscription should not modify Events received over the channel.
//
// A Subscription can be closed via the Unsubscribe() method, which will result
// in the Events channel being closed. The Events channel will also be closed if
// the Feed itself is closed.
type Subscription struct {
	feed   *Feed
	events chan interface{}
}

// Events returns a recieve only channel for reading events from this
// Subscriber.
func (s *Subscription) Events() <-chan interface{} {
	return s.events
}

// A Feed is used to publish a stream of events to a set of Subscribers.  Events
// are values of arbitrary type, and are received and published as an empty
// interface.  Each Subscriber will receive, in order, each entry published to
// the Feed.
//
// Entries are published by via the Publish method of the Feed; this cannot be
// blocked by Subscriber activities and will always return quickly.  Subscribers
// receive events by reading from their Events channel.
//
// A Feed can be initialized by simply instantiating an empty feed object:
//
//		feed := &Feed{}
//		subscriber := feed.Subscribe()
//		feed.Publish(someEvent())
//
// The Feed does not keep historical events; individual Subscribers will only
// receive events published after they Subscribe. Events can be published to a
// Feed until its Close() method is called.
//
// The non-blocking property of the feed is achieved by giving each Subscriber's
// Events channel a very large buffer. If a Subscriber does not read from its
// channel, then its buffer will fill; if a call to Publish() attempts to write
// to a full Subscriber channel, it will panic.
type Feed struct {
	sync.Mutex
	closed      bool
	subscribers []*Subscription
}

// Publish publishes a event into the Feed, which will eventually be received by
// all Subscribers to the feed. Events published to a closed feed, or to a feed
// with no Subscribers, will be ignored.
func (f *Feed) Publish(event interface{}) {
	f.Lock()
	defer f.Unlock()
	if f.closed || len(f.subscribers) == 0 {
		return
	}

	for _, sub := range f.subscribers {
		select {
		case sub.events <- event:
			// Subscription successfully sent.
		default:
			// Subscription buffer was exceeded; this is a panic situation.
			panic(fmt.Sprintf("event subscriber had full buffer, %d entries unread.",
				subscriptionChannelSize))
		}
	}
}

// Subscribe returns a Subscription object which can immediately recieve events
// which were published to this feed. An event is an arbitrary interface.
//
// Events are read from the Subscription's Events channel. Subscribers cannot
// block each other from receiving events, but should still attempt to consume
// events in a timely fashion; if a Subscriber's (very large) Events channel
// fills up, a panic may result.
func (f *Feed) Subscribe() *Subscription {
	sub := &Subscription{
		feed:   f,
		events: make(chan interface{}, subscriptionChannelSize),
	}

	f.Lock()
	defer f.Unlock()
	if !f.closed {
		f.subscribers = append(f.subscribers, sub)
	} else {
		close(sub.events)
	}
	return sub
}

// Close closes the given Feed. All existing Subscribers will be closed
// immediately when the Feed is closed. After closure, any new Subscribers will
// be closed immediately and attempts to Publish will be ignored.
func (f *Feed) Close() {
	f.Lock()
	defer f.Unlock()
	if !f.closed {
		f.closed = true
		for _, sub := range f.subscribers {
			close(sub.events)
		}
		f.subscribers = nil
	}
}

// Unsubscribe stops the Subscriber. This will close the Subscriber's Events
// channel; however, there may still be unprocessed Events remaining in the
// channel.
func (s *Subscription) Unsubscribe() {
	s.feed.Lock()
	defer s.feed.Unlock()
	if !s.feed.closed {
		for i, sub := range s.feed.subscribers {
			if s == sub {
				s.feed.subscribers = append(s.feed.subscribers[:i], s.feed.subscribers[i+1:]...)
				break
			}
		}
	}
}
