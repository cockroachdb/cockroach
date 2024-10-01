// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import (
	"context"
	"time"
)

func (s *Server) DrainImpl(
	ctx context.Context, queryWait time.Duration, cancelWait time.Duration,
) error {
	return s.drainImpl(ctx, queryWait, cancelWait, nil, nil)
}

// OverwriteCancelMap overwrites all active connections' context.CancelFuncs so
// that the cancellation of any context.CancelFunc in s.mu.connCancelMap does
// not trigger a response by the associated connection. A slice of the original
// context.CancelFuncs is returned.
func (s *Server) OverwriteCancelMap() []context.CancelFunc {
	s.mu.Lock()
	defer s.mu.Unlock()
	cancel := func() {}
	originalCancels := make([]context.CancelFunc, 0, len(s.mu.connCancelMap))
	for done, originalCancel := range s.mu.connCancelMap {
		s.mu.connCancelMap[done] = cancel
		originalCancels = append(originalCancels, originalCancel)
	}
	return originalCancels
}
