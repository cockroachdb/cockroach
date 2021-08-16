// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package throttler

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/cache"
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
// memory. Internally, it maintains two datastructures:
//
// successCache: Tracks (IP, TenantID) pairs that have successfully connected. If
// a connection request matches an entry in the successCache it is allowed through
// without deducting a token.
//
// ipCache: An IP -> TokenBucket map. Each connection request for unknown (IP, TenantID)
// pairs deducts a token from that IP's token bucket. After a successful connection the
// token is returned and the (IP, TenantID) pair is added tot he success cache.
type localService struct {
	clock        timeNow
	maxCacheSize int

	policy BucketPolicy

	mu struct {
		syncutil.Mutex
		// ipCache is approximately a map[string]*tokenBucket
		ipCache *cache.UnorderedCache
		// successCache is approximately a map[ConnectionTags]nil
		successCache *cache.UnorderedCache
	}
}

// LocalOption allows configuration of a local admission service.
type LocalOption func(s *localService)

// WithPolicy configures the token bucket used by clients with no history of
// successful connection.
func WithPolicy(policy BucketPolicy) LocalOption {
	return func(s *localService) {
		s.policy = policy
	}
}

// NewLocalService returns an throttler service that manages state purely in
// local memory.
func NewLocalService(opts ...LocalOption) Service {
	s := &localService{
		clock:        time.Now,
		maxCacheSize: maxMapSize,
	}
	cacheConfig := cache.Config{
		Policy:      cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool { return s.maxCacheSize < size },
	}
	s.mu.ipCache = cache.NewUnorderedCache(cacheConfig)
	s.mu.successCache = cache.NewUnorderedCache(cacheConfig)

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (s *localService) LoginCheck(connection ConnectionTags) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.mu.successCache.Get(connection); ok {
		return nil
	}

	bucket, ok := s.mu.ipCache.Get(connection.IP)
	if !ok {
		bucket = newBucket(s.clock(), s.policy)
		s.mu.ipCache.Add(connection.IP, bucket)
	}

	b := bucket.(*tokenBucket)
	b.fill(s.clock())
	if !b.tryConsume() {
		return errRequestDenied
	}

	return nil
}

func (s *localService) ReportSuccess(connection ConnectionTags) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if bucket, ok := s.mu.ipCache.Get(connection.IP); ok {
		b := bucket.(*tokenBucket)
		b.returnToken()
	}

	if _, ok := s.mu.successCache.Get(connection); !ok {
		s.mu.successCache.Add(connection, nil)
	}
}
