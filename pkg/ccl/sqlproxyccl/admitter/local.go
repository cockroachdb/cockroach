// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package admitter

import (
	"errors"
	"math/rand"
	"sync"
	"time"
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

// localService is an admitter Service that manages state purely in local
// memory. Internally, it maintains a map from IP address to rate limiting info
// for that address. In order to put a cap on memory usage, the map is capped
// at a maximum size, at which point a random IP address will be evicted.
type localService struct {
	clock      timeNow
	baseDelay  time.Duration
	maxDelay   time.Duration
	maxMapSize int

	mu struct {
		sync.Mutex
		// Map from IP address to limiter.
		limiters map[string]*limiter
		// Map from IP address to known tenant ids for this IP.
		knownTenants map[string]map[uint64]bool
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

// NewLocalService returns an admitter Service that manages state purely in
// local memory.
func NewLocalService(opts ...LocalOption) Service {
	s := &localService{
		clock:      time.Now,
		baseDelay:  2 * time.Second,
		maxDelay:   60 * 60 * time.Second,
		maxMapSize: maxMapSize,
	}
	s.mu.limiters = make(map[string]*limiter)
	s.mu.knownTenants = make(map[string]map[uint64]bool)

	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *localService) AllowRequest(ipAddress string, now time.Time) error {
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

func (s *localService) RequestSuccess(ipAddress string, tenID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictLocked(ipAddress)
	if _, ok := s.mu.knownTenants[ipAddress]; !ok {
		s.mu.knownTenants[ipAddress] = map[uint64]bool{tenID: true}
	} else {
		s.mu.knownTenants[ipAddress][tenID] = true
	}
}

func (s *localService) KnownClient(ipAddress string, tenID uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.mu.knownTenants[ipAddress]
	if ok {
		_, ok = s.mu.knownTenants[ipAddress][tenID]
	}
	return ok
}

func (s *localService) addLocked(addr string) *limiter {
	if len(s.mu.addrs) >= s.maxMapSize {
		addr := s.mu.addrs[rand.Intn(len(s.mu.limiters))]
		s.evictLocked(addr)
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
