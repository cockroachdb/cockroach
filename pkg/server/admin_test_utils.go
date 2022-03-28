// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type stubUnusedIndexTime struct {
	syncutil.RWMutex
	current   time.Time
	lastRead  time.Time
	createdAt *time.Time
}

func (s *stubUnusedIndexTime) setCurrent(t time.Time) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.current = t
}

func (s *stubUnusedIndexTime) setLastRead(t time.Time) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.lastRead = t
}

func (s *stubUnusedIndexTime) setCreatedAt(t *time.Time) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.createdAt = t
}

func (s *stubUnusedIndexTime) getCurrent() time.Time {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.current
}

func (s *stubUnusedIndexTime) getLastRead() time.Time {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.lastRead
}

func (s *stubUnusedIndexTime) getCreatedAt() *time.Time {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.createdAt
}
