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

// localService is an throttler service that manages state purely in local
// memory. Internally, it maintains a map from IP address to rate limiting info
// for that address. In order to put a cap on memory usage, the map is capped
// at a maximum size, at which point a random IP address will be evicted.
//
// Each IP address is throttled using a token bucket. Connections remove a token
// from the bucket and return the token after a successful connection attempt.
type localService struct {
	clock      timeNow
	maxMapSize int

	authenticatedPolicy   BucketPolicy
	unauthenticatedPolicy BucketPolicy

	mu struct {
		syncutil.Mutex
		// Map from IP address to limiter.
		limiters map[string]*tokenBucket
		// Array of addresses, used for randomly evicting an address when the max
		// entries is reached.
		addrs []string
	}
}

// LocalOption allows configuration of a local admission service.
type LocalOption func(s *localService)

// WithAuthenticatedPolicy configures the token bucket used by clients with a history
// of successful connections.
func WithAuthenticatedPolicy(policy BucketPolicy) LocalOption {
	return func(s *localService) {
		s.authenticatedPolicy = policy
	}
}

// WithUnauthenticatedPolicy configures the token buckets used by clients that have never
// successfully authenticated a connection.
func WithUnauthenticatedPolicy(policy BucketPolicy) LocalOption {
	return func(s *localService) {
		s.unauthenticatedPolicy = policy
	}
}

// NewLocalService returns an throttler service that manages state purely in
// local memory.
func NewLocalService(opts ...LocalOption) Service {
	s := &localService{
		clock:      time.Now,
		maxMapSize: maxMapSize,
	}
	s.mu.limiters = make(map[string]*tokenBucket)

	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *localService) LoginCheck(ipAddress string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b := s.getBucketLocked(ipAddress)
	b.fill(s.clock())
	if !b.tryConsume() {
		return errRequestDenied
	}
	return nil
}

func (s *localService) ReportSuccess(ipAddress string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	bucket := s.getBucketLocked(ipAddress)
	bucket.policy = s.authenticatedPolicy
	bucket.returnToken()
}

// getBucketedLocked returns a bucket if one exists and creates one
// if it does not.
func (s *localService) getBucketLocked(addr string) *tokenBucket {
	b := s.mu.limiters[addr]
	if b != nil {
		return b
	}
	if len(s.mu.addrs) >= s.maxMapSize {
		s.evictLocked()
	}
	b = newBucket(s.clock(), s.unauthenticatedPolicy)
	s.mu.limiters[addr] = b
	s.mu.addrs = append(s.mu.addrs, addr)
	return b
}

// evictLocked removes a random ip address from the throttler.
func (s *localService) evictLocked() {
	index := rand.Intn(len(s.mu.limiters))
	addr := s.mu.addrs[index]
	log.Infof(context.Background(), "evicting due to map size limit - addr: %s", addr)

	// Swap the address we're evicting with the address on the end of the array.
	n := len(s.mu.addrs) - 1
	s.mu.addrs[index], s.mu.addrs[n] = s.mu.addrs[n], s.mu.addrs[index]
	s.mu.addrs = s.mu.addrs[:n]

	delete(s.mu.limiters, addr)
}
