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
	"reflect"
	"sync"
	"time"

	"google.golang.org/api/support/bundler"
)

// PublishScheduler is a scheduler which is designed for Pub/Sub's Publish flow.
// It bundles items before handling them. All items in this PublishScheduler use
// the same handler.
//
// Each item is added with a given key. Items added to the empty string key are
// handled in random order. Items added to any other key are handled
// sequentially.
type PublishScheduler struct {
	// Settings passed down to each bundler that gets created.
	DelayThreshold       time.Duration
	BundleCountThreshold int
	BundleByteThreshold  int
	BundleByteLimit      int
	BufferedByteLimit    int

	mu          sync.Mutex
	bundlers    sync.Map // keys -> *bundler.Bundler
	outstanding sync.Map // keys -> num outstanding messages

	keysMu sync.RWMutex
	// keysWithErrors tracks ordering keys that cannot accept new messages.
	// A bundler might not accept new messages if publishing has failed
	// for a specific ordering key, and can be resumed with topic.ResumePublish().
	keysWithErrors map[string]struct{}

	// workers is a channel that represents workers. Rather than a pool, where
	// worker are "removed" until the pool is empty, the channel is more like a
	// set of work desks, where workers are "added" until all the desks are full.
	//
	// workers does not restrict the amount of goroutines in the bundlers.
	// Rather, it acts as the flow control for completion of bundler work.
	workers chan struct{}
	handle  func(bundle interface{})
	done    chan struct{}
}

// NewPublishScheduler returns a new PublishScheduler.
//
// The workers arg is the number of workers that will operate on the queue of
// work. A reasonably large number of workers is highly recommended. If the
// workers arg is 0, then a healthy default of 10 workers is used.
//
// The scheduler does not use a parent context. If it did, canceling that
// context would immediately stop the scheduler without waiting for
// undelivered messages.
//
// The scheduler should be stopped only with FlushAndStop.
func NewPublishScheduler(workers int, handle func(bundle interface{})) *PublishScheduler {
	if workers == 0 {
		workers = 10
	}

	s := PublishScheduler{
		keysWithErrors: make(map[string]struct{}),
		workers:        make(chan struct{}, workers),
		handle:         handle,
		done:           make(chan struct{}),
	}

	return &s
}

// Add adds an item to the scheduler at a given key.
//
// Add never blocks. Buffering happens in the scheduler's publishers. There is
// no flow control.
//
// Since ordered keys require only a single outstanding RPC at once, it is
// possible to send ordered key messages to Topic.Publish (and subsequently to
// PublishScheduler.Add) faster than the bundler can publish them to the
// Pub/Sub service, resulting in a backed up queue of Pub/Sub bundles. Each
// item in the bundler queue is a goroutine.
func (s *PublishScheduler) Add(key string, item interface{}, size int) error {
	select {
	case <-s.done:
		return errors.New("draining")
	default:
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	var b *bundler.Bundler
	bInterface, ok := s.bundlers.Load(key)

	if !ok {
		s.outstanding.Store(key, 1)
		b = bundler.NewBundler(item, func(bundle interface{}) {
			s.workers <- struct{}{}
			s.handle(bundle)
			<-s.workers

			nlen := reflect.ValueOf(bundle).Len()
			s.mu.Lock()
			outsInterface, _ := s.outstanding.Load(key)
			s.outstanding.Store(key, outsInterface.(int)-nlen)
			if v, _ := s.outstanding.Load(key); v == 0 {
				s.outstanding.Delete(key)
				s.bundlers.Delete(key)
			}
			s.mu.Unlock()
		})
		b.DelayThreshold = s.DelayThreshold
		b.BundleCountThreshold = s.BundleCountThreshold
		b.BundleByteThreshold = s.BundleByteThreshold
		b.BundleByteLimit = s.BundleByteLimit
		b.BufferedByteLimit = s.BufferedByteLimit

		if b.BufferedByteLimit == 0 {
			b.BufferedByteLimit = 1e9
		}

		if key == "" {
			// There's no way to express "unlimited" in the bundler, so we use
			// some high number.
			b.HandlerLimit = 1e9
		} else {
			// HandlerLimit=1 causes the bundler to act as a sequential queue.
			b.HandlerLimit = 1
		}

		s.bundlers.Store(key, b)
	} else {
		b = bInterface.(*bundler.Bundler)
		oi, _ := s.outstanding.Load(key)
		s.outstanding.Store(key, oi.(int)+1)
	}

	return b.Add(item, size)
}

// FlushAndStop begins flushing items from bundlers and from the scheduler. It
// blocks until all items have been flushed.
func (s *PublishScheduler) FlushAndStop() {
	close(s.done)
	s.bundlers.Range(func(_, bi interface{}) bool {
		bi.(*bundler.Bundler).Flush()
		return true
	})
}

// Flush waits until all bundlers are sent.
func (s *PublishScheduler) Flush() {
	var wg sync.WaitGroup
	s.bundlers.Range(func(_, bi interface{}) bool {
		wg.Add(1)
		go func(b *bundler.Bundler) {
			defer wg.Done()
			b.Flush()
		}(bi.(*bundler.Bundler))
		return true
	})
	wg.Wait()

}

// IsPaused checks if the bundler associated with an ordering keys is
// paused.
func (s *PublishScheduler) IsPaused(orderingKey string) bool {
	s.keysMu.RLock()
	defer s.keysMu.RUnlock()
	_, ok := s.keysWithErrors[orderingKey]
	return ok
}

// Pause pauses the bundler associated with the provided ordering key,
// preventing it from accepting new messages. Any outstanding messages
// that haven't been published will error. If orderingKey is empty,
// this is a no-op.
func (s *PublishScheduler) Pause(orderingKey string) {
	if orderingKey != "" {
		s.keysMu.Lock()
		defer s.keysMu.Unlock()
		s.keysWithErrors[orderingKey] = struct{}{}
	}
}

// Resume resumes accepting message with the provided ordering key.
func (s *PublishScheduler) Resume(orderingKey string) {
	s.keysMu.Lock()
	defer s.keysMu.Unlock()
	delete(s.keysWithErrors, orderingKey)
}
