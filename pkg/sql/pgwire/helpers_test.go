// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License.

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
