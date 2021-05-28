// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package throttler

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

const (
	// The maximum size of localService's address map.
	maxMapSize = 1e6 // 1 million
)

var errRequestDenied = errors.New("request denied")

type timeNow func() time.Time

type limiter struct {
	// The next time an operation on this limiter can proceed.
	nextTime time.Time
	// The number of operation attempts that have been performed. On success, the
	// limiter will be removed.
	attempts int
	// The index of the limiter in the addresses array.
	index int
}

// localService is an throttler service that manages state purely in local
// memory. Internally, it maintains a map from IP address to rate limiting info
// for that address. In order to put a cap on memory usage, the map is capped
// at a maximum size, at which point a random IP address will be evicted.
//
// TODO(peter): Rather than a per-IP count, at some point we should use a
// count-min-sketch: https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch.
//
// The count-min-sketch structure is essentially a counting bloom filter and
// provides a probabilistic count of the number of times a key (i.e. an IP
// address) has been seen. A challenge with using count-min-sketch is that they
// do not support eviction. The general idea for handling this (courtesy of
// Aaron) is to maintain a laddered set of sketches. For example, 1 for each of
// the past 30 days. Each day, we would discard the oldest sketch and
// instantiated a new one. Whenever an IP address performs an action we update
// all of the sketched, but checks for activity are only performed against the
// oldest sketch. The end result is that the oldest sketch has the activity for
// the past 30 days.
type localService struct {
	clock      timeNow
	baseDelay  time.Duration
	maxDelay   time.Duration
	maxMapSize int

	mu struct {
		syncutil.Mutex
		// Map from IP address to limiter.
		limiters map[string]*limiter
		// Array of addresses, used for randomly evicting an address when the max
		// entries is reached.
		addrs []string
	}
}

// LocalOption allows configuration of a local admission service.
type LocalOption func(s *localService)

// WithBaseDelay specifies the base delay for rate limiting repeated accesses.
func WithBaseDelay(d time.Duration) LocalOption {
	return func(s *localService) {
		s.baseDelay = d
	}
}

// NewLocalService returns an throttler service that manages state purely in
// local memory.
func NewLocalService(opts ...LocalOption) Service {
	s := &localService{
		clock:      time.Now,
		maxDelay:   60 * 60 * time.Second,
		maxMapSize: maxMapSize,
	}
	WithBaseDelay(2 * time.Second)(s)
	s.mu.limiters = make(map[string]*limiter)

	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *localService) LoginCheck(ipAddress string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	l := s.mu.limiters[ipAddress]
	if l == nil {
		l = s.addLocked(ipAddress)
	}
	if now.Before(l.nextTime) {
		return errRequestDenied
	}
	s.nextLimitLocked(l)
	return nil
}

func (s *localService) addLocked(addr string) *limiter {
	if len(s.mu.addrs) >= s.maxMapSize {
		addrToEvict := s.mu.addrs[rand.Intn(len(s.mu.limiters))]
		log.Infof(context.Background(), "evicting due to map size limit - addr: %s", addrToEvict)
		s.evictLocked(addrToEvict)
	}

	l := &limiter{
		index: len(s.mu.limiters),
	}
	s.mu.limiters[addr] = l
	s.mu.addrs = append(s.mu.addrs, addr)
	return l
}

func (s *localService) evictLocked(addr string) {
	l := s.mu.limiters[addr]
	if l == nil {
		return
	}

	// Swap the address we're evicting to the end of the address array.
	n := len(s.mu.addrs) - 1
	s.mu.addrs[l.index], s.mu.addrs[n] = s.mu.addrs[n], s.mu.addrs[l.index]
	// Fix-up the index of the limiter we're keeping.
	s.mu.limiters[s.mu.addrs[l.index]].index = l.index
	// Trim the evicted address from the address array.
	s.mu.addrs = s.mu.addrs[:n]
	// Delete the address from the limiters map.
	delete(s.mu.limiters, addr)
}

func (s *localService) nextLimitLocked(l *limiter) {
	// This calculation implements a simple capped exponential backoff. No
	// randomization is done. We could use github.com/cenkalti/backoff, but this
	// gives us a more control over the precise calculation and is about half the
	// size in terms of memory usage. The latter part may be important for IP
	// address based admission control.
	delay := s.baseDelay * (1 << l.attempts)
	if delay >= s.maxDelay {
		delay = s.maxDelay
	}
	l.attempts++

	l.nextTime = s.clock().Add(delay)
}
