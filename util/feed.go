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

import "sync"

// A feedEntry represents a single entry in a feed. A feed entry usually an
// event published to the feed, but may also be a management action used by the
// feed itself to manage subscribers.
//
// Each feedEntry structure is a node in a "linked channel", which is analogous
// to a linked list but using channels instead of pointers. Each feedEntry
// contains a channel which can be used to get the next feedEntry in the
// sequence.
//
// The "linked channel" is designed to be traversed by multiple consumers
// simultaneously, with each consumer visiting each entry once. This is
// accomplished with the following algorithm:
//
// 1. Each consumer attempts to read the next feedEntry from the "next" channel
// of the last feedEntry.
// 2. When the next entry is placed into the channel, one consumer will actually
// obtain it.
// 3. The obtaining consumer copies the values from the feedEntry, and
// immediately places it back into the channel from which it was obtained. This
// will allow another waiting consumer to obtain it.
// 4. The obtaining consumer returns to step 1, but now reads from the
// "next" channel of the feedEntry it had just obtained.
//
// Code example:
//
//		func traverseLinkedChannel(next chan *feedEntry) {
//			for {
//				nextEntry := <-next
//				// Copy values from nextEntry
//				last := next
//				next = nextEntry.next
//				last <- nextEntry
//			}
//		}
//
// The "next" channel of each feedEntry is given a buffer size of one. Since
// that channel will only ever contain one feedEntry, the act of placing an
// entry back into the channel can never block, meaning that consumers cannot
// block each other as long as they behave correctly.
//
// The above implementation is sufficient for a linked channel: each consumer
// will read each entry in the feed, after which the garbage collector would
// clean up any entries which have been read by all subscribers.
//
// However, in order to improve memory performance each feedEntry maintains a
// reference counter to determine when it can be safely returned to a shared
// pool.
type feedEntry struct {
	entryType entryType
	value     interface{}
	next      chan *feedEntry

	// consumerCount is the number of subscribers still waiting to receive this
	// entry. This is a reference counter used to determine when entries can be
	// released to the entry pool; however, an entry is not released when its
	// own consumerCount is zero, but rather when the consumerCount of the
	// *next* entry in the linked channel is zero. When the next entry has zero
	// remaining consumers, that signifies that no more consumers are listening
	// on the next channel of this entry, and thus it can be released.
	consumerCount int
}

type entryType int

const (
	valueEntry entryType = iota
	unsubEntry
	closedEntry
)

var feedEntryPool = sync.Pool{
	New: func() interface{} {
		return &feedEntry{
			next: make(chan *feedEntry, 1),
		}
	},
}

// A Subscription is used to receive events from a specific Feed. A Subscription
// should only be instantiated via a call to a Feed's Subscribe() method. Once
// created, events can be read directly from the Events channel provided by this
// structure.
//
// An example of a typical usage of a Subscription:
//
//		subscriber := feed.Subscribe()
//		for {
//			select {
//			case event, ok := <-subscriber.Events:
//				// Process event...
//			}
//		}
//
// A Subscription cannot block other Subscriptions to the same feed, and each
// Subscription will receive all events published by the Feed. A user of a
// Subscription should not modify Events received over the channel.
//
// A Subscription can be closed via the Unsubscribe() method.
//
// If the Feed is stopped, the Events channel will be closed after reading all
// remaining events that were published to the Feed before it was stopped.
type Subscription struct {
	Events <-chan interface{}
	closer chan<- struct{}
}

// Unsubscribe immediately stops the given Subscriber. After this method is
// called, the Events channel will be closed.
func (s *Subscription) Unsubscribe() {
	close(s.closer)
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
// The Feed will continue running until its associated Stopper is closed.
//
// The Feed's non-blocking properties are achieved with a linked channel instead
// of buffered channels, and thus there is no limit (other than available
// memory) to how many unread events the Feed can maintain. This is desirable
// for Feeds which may publish data in quick bursts.
//
// The Feed does not keep historical events; individual Subscribers will only
// receive events published after they subscribe.
type Feed struct {
	events  chan interface{}
	stopper *Stopper

	// subscriberCount counts the total number of subscribers that have
	// ever been started. activeCount counts the number of subscribers still
	// active (i.e. that have not been unsubscribed).
	subscriberCount int
	activeCount     int

	// these channels are used for synchronizing different subscription
	// lifecycle tasks.
	subscribe   chan *publisher
	unsubscribe chan *publisher
	closed      chan struct{}
}

// StartFeed instatiates and starts a new Feed. The Feed can immediately accept
// Subscribers and publish events.
func StartFeed(stopper *Stopper) *Feed {
	f := &Feed{
		events:      make(chan interface{}),
		stopper:     stopper,
		subscribe:   make(chan *publisher),
		unsubscribe: make(chan *publisher),
		closed:      make(chan struct{}),
	}
	stopper.RunWorker(func() {
		// head is the most recent entry added to the feed. This is also the
		// current head of the linked channel of entries. The first entry
		// will never be consumed by any subscribers, but initial
		// subscribers may listen on its "next" channel.
		head := feedEntryPool.Get().(*feedEntry)
		// addEntry is used to correctly add a new entry to the linked channel.
		addEntry := func(et entryType, value interface{}) {
			entry := feedEntryPool.Get().(*feedEntry)
			entry.entryType = et
			entry.consumerCount = f.activeCount
			switch et {
			case valueEntry:
				entry.value = value
			case unsubEntry:
				entry.value = value.(int)
			}
			head.next <- entry
			head = entry
		}
		for {
			select {
			case pub := <-f.subscribe:
				// A new Subscription is requested, and we have received the
				// publisher associated with it. Give the publisher the channel
				// for the next entry and start it's process() goroutine.
				f.subscriberCount++
				f.activeCount++
				pub.id = f.subscriberCount
				pub.current = head
				stopper.RunWorker(pub.process)
			case pub := <-f.unsubscribe:
				// A Subscription has unsubscribed; ackowledge this with an
				// "unsubscribed" entry and decrease the number of activeSubs
				// for future entries.
				addEntry(unsubEntry, pub.id)
				f.activeCount--
			case event := <-f.events:
				// A new event has been published to the feed, which will
				// eventually be read by all active subscribers.
				if f.activeCount == 0 {
					break
				}
				addEntry(valueEntry, event)
			case <-stopper.ShouldStop():
				// The stopper has been activated and the feed should cease.
				if f.activeCount != 0 {
					// Add a 'closed' entry to the linked channel, which will
					// cause consuming publishers to stop.
					addEntry(closedEntry, nil)
				}
				close(f.closed)
				return
			}
		}
	})
	return f
}

// Publish publishes a event into the Feed, which will eventually be received by
// all Subscribers to the feed. Publishing to a closed feed will panic.
func (f *Feed) Publish(event interface{}) {
	select {
	case f.events <- event:
		// Event was successfully published.
	case <-f.closed:
		panic("attempt to publish value to a closed Feed")
	}
}

// Subscribe returns a Subscription object which can immediately recieve events
// which were published to this feed. An event is an arbitrary interface.
//
// Events are read from the Subscription's Events channel. Subscribers cannot
// block each other from receiving events, but should still attempt to consume
// events in a timely fashion; the Feed will not release the memory for an
// individual event until every Subscriber has seen it.
//
// Attempting to subscribe to a closed Feed will panic.
func (f *Feed) Subscribe() *Subscription {
	// events and closer are used to manage the interaction between a
	// Subscription and its backing publisher.
	events := make(chan interface{})
	closer := make(chan struct{})
	sub := &Subscription{
		Events: events,
		closer: closer,
	}
	p := &publisher{
		events: events,
		closer: closer,
		feed:   f,
	}
	select {
	case f.subscribe <- p:
		// Subscription was successfully logged.
	case <-f.closed:
		panic("attempt to publish value to a closed Feed")
	}
	return sub
}

// A publisher reads values from a Feed and passes them to a Subscriber; each
// Subscriber is backed by a publisher. The publisher correctly reads from the
// linked channel provided by the Feed, ensuring that other publishers cannot be
// blocked.
type publisher struct {
	id      int
	current *feedEntry
	events  chan<- interface{}
	closer  <-chan struct{}
	feed    *Feed
}

// process is a method which reads the linked channel provided by a Feed,
// passing relevant events to a Subscription. It is
// intended to be run as a goroutine.
func (p *publisher) process() {
	// If the Subscriber has unsubscribed, the publisher must still
	// read events from the Feed until the unsubscription event has
	// been acknowledged by the Feed: this ensures that each
	// feedEntry reaches its consumerCount count and is returned to
	// the pool. This is the 'draining' phase, during which the publisher
	// reads events but no longer sends them to the subscriber.
	draining := false
	// readEntry is called on every entry read from the next channel of the
	// current entry. This method properly maintains the consumerCount
	// reference counter on feedEntries.
	readEntry := func(entry *feedEntry) {
		entry.consumerCount--
		if entry.consumerCount > 0 {
			// The next entry still has remaining consumers, so place it
			// back on the 'next' channel of the current entry. Buffer size
			// is 1, so this will not block.
			p.current.next <- entry
		} else {
			// The next entry has no remaining consumers, and thus nothing
			// is listening on the 'next' channel of the current entry.
			// Release the current entry to the entry pool.
			feedEntryPool.Put(p.current)
		}
		p.current = entry
	}
	for !draining {
		select {
		case entry := <-p.current.next:
			readEntry(entry)
			switch p.current.entryType {
			case valueEntry:
				select {
				case p.events <- p.current.value:
					// Event successfully sent.
				case <-p.closer:
					// Prevent blocking if the subscriber has stopped
					// reading events in order to close.
				}
			case closedEntry:
				// The Feed has been closed.
				close(p.events)
				return
			}
		case <-p.closer:
			// The Subscriber wants to initiate a closure. Inform the Feed of
			// the unsubscription, close the channel to the Subscription and
			// begin draining.
			select {
			case p.feed.unsubscribe <- p:
				// Successfully unsubscribed.
			case <-p.feed.closed:
				// Feed closed first - this might happen due to a race a
				// shutdown scenario. The publisher will simply run until the
				// inevitable 'closed' entry stops this publisher.
			}
			draining = true
		}
	}
	for {
		entry := <-p.current.next
		readEntry(entry)
		switch p.current.entryType {
		case unsubEntry:
			if p.current.value.(int) == p.id {
				// The feed has acknowledged that this subscriber has
				// unsubscriber. The publisher can safely stop.
				close(p.events)
				return
			}
		case closedEntry:
			// The Feed has been closed.
			close(p.events)
			return
		}
	}
}
