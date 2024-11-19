// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package throttler

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

var (
	errRequestDeniedAtLogin   = errors.New("throttler refused connection due to too many failed authentication attempts")
	errRequestDeniedAfterAuth = errors.New("throttler refused connection after authentication")
)

type timeNow func() time.Time

// localService is an throttler service that manages state purely in local
// memory.
//
// localService tracks throttling state for (ip, tenant) pairs. Exponential backoff
// is used to limit authentication attempts for a given (ip, tenant). The connection
// limit for an (ip, tenant) is removed once there is a successful connection between
// the ip address and the tenant. The primary intent of this mechanism is to limit
// the number of credential guesses an ip address can make.
type localService struct {
	clock        timeNow
	maxCacheSize int
	baseDelay    time.Duration
	maxDelay     time.Duration

	mu struct {
		syncutil.Mutex
		// throttleCache is effectively a map[ConnectionTags]*throttle
		throttleCache *cache.UnorderedCache
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
		clock:        time.Now,
		maxCacheSize: 1e6, /* 1 million */
		baseDelay:    time.Second,
		maxDelay:     time.Hour,
	}
	cacheConfig := cache.Config{
		Policy:      cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool { return s.maxCacheSize < size },
	}
	s.mu.throttleCache = cache.NewUnorderedCache(cacheConfig)

	for _, opt := range opts {
		opt(s)
	}

	return s
}

var _ Service = (*localService)(nil)

func (s *localService) lockedGetThrottle(connection ConnectionTags) *throttle {
	l, ok := s.mu.throttleCache.Get(connection)
	if ok && l != nil {
		return l.(*throttle)
	}
	return nil
}

func (s *localService) lockedInsertThrottle(connection ConnectionTags) *throttle {
	l := newThrottle(s.baseDelay)
	s.mu.throttleCache.Add(connection, l)
	return l
}

// LoginCheck implements the Service interface.
func (s *localService) LoginCheck(
	ctx context.Context, connection ConnectionTags,
) (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock()
	throttle := s.lockedGetThrottle(connection)
	if throttle != nil && throttle.isThrottled(now) {
		return now, errRequestDeniedAtLogin
	}
	return now, nil
}

// ReportAttempt implements the Service interface.
func (s *localService) ReportAttempt(
	ctx context.Context, connection ConnectionTags, throttleTime time.Time, status AttemptStatus,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	throttle := s.lockedGetThrottle(connection)
	if throttle == nil {
		throttle = s.lockedInsertThrottle(connection)
	}

	if throttle.isThrottled(throttleTime) {
		return errRequestDeniedAfterAuth
	}

	switch {
	case status == AttemptInvalidCredentials:
		throttle.triggerThrottle(s.clock(), s.maxDelay)
		if throttle.nextBackoff == s.maxDelay {
			log.Warningf(ctx, "connection %v at max throttle delay %s", connection, s.maxDelay)
		}
	case status == AttemptOK:
		throttle.disable()
	}

	return nil
}
