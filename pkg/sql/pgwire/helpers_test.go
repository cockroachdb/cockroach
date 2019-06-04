// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package pgwire

import (
	"context"
	"time"
)

func (s *Server) DrainImpl(drainWait time.Duration, cancelWait time.Duration) error {
	return s.drainImpl(drainWait, cancelWait)
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
