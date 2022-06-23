// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scheduler

import (
	"errors"
	"sync"
)

// ReceiveScheduler is a scheduler which is designed for Pub/Sub's Receive flow.
//
// Each item is added with a given key. Items added to the empty string key are
// handled in random order. Items added to any other key are handled
// sequentially.
type ReceiveScheduler struct {
	// workers is a channel that represents workers. Rather than a pool, where
	// worker are "removed" until the pool is empty, the channel is more like a
	// set of work desks, where workers are "added" until all the desks are full.
	//
	// A worker taking an item from the unordered queue (key="") completes a
	// single item and then goes back to the pool.
	//
	// A worker taking an item from an ordered queue (key="something") completes
	// all work in that queue until the queue is empty, then deletes the queue,
	// then goes back to the pool.
	workers chan struct{}
	done    chan struct{}

	mu sync.Mutex
	m  map[string][]func()
}

// NewReceiveScheduler creates a new ReceiveScheduler.
//
// The workers arg is the number of concurrent calls to handle. If the workers
// arg is 0, then a healthy default of 10 workers is used. If less than 0, this
// will be set to an large number, similar to PublishScheduler's handler limit.
func NewReceiveScheduler(workers int) *ReceiveScheduler {
	if workers == 0 {
		workers = 10
	} else if workers < 0 {
		workers = 1e9
	}

	return &ReceiveScheduler{
		workers: make(chan struct{}, workers),
		done:    make(chan struct{}),
		m:       make(map[string][]func()),
	}
}

// Add adds the item to be handled. Add may block.
//
// Buffering happens above the ReceiveScheduler in the form of a flow controller
// that requests batches of messages to pull. A backed up ReceiveScheduler.Add
// call causes pushback to the pubsub service (less Receive calls on the
// long-lived stream), which keeps memory footprint stable.
func (s *ReceiveScheduler) Add(key string, item interface{}, handle func(item interface{})) error {
	select {
	case <-s.done:
		return errors.New("draining")
	default:
	}

	if key == "" {
		// Spawn a worker.
		s.workers <- struct{}{}
		go func() {
			// Unordered keys can be handled immediately.
			handle(item)
			<-s.workers
		}()
		return nil
	}

	// Add it to the queue. This has to happen before we enter the goroutine
	// below to prevent a race from the next iteration of the key-loop
	// adding another item before this one gets queued.

	s.mu.Lock()
	_, ok := s.m[key]
	s.m[key] = append(s.m[key], func() {
		handle(item)
	})
	s.mu.Unlock()
	if ok {
		// Someone is already working on this key.
		return nil
	}

	// Spawn a worker.
	s.workers <- struct{}{}

	go func() {
		defer func() { <-s.workers }()

		// Key-Loop: loop through the available items in the key's queue.
		for {
			s.mu.Lock()
			if len(s.m[key]) == 0 {
				// We're done processing items - the queue is empty. Delete
				// the queue from the map and free up the worker.
				delete(s.m, key)
				s.mu.Unlock()
				return
			}
			// Pop an item from the queue.
			next := s.m[key][0]
			s.m[key] = s.m[key][1:]
			s.mu.Unlock()

			next() // Handle next in queue.
		}
	}()

	return nil
}

// Shutdown begins flushing messages and stops accepting new Add calls. Shutdown
// does not block, or wait for all messages to be flushed.
func (s *ReceiveScheduler) Shutdown() {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
}
