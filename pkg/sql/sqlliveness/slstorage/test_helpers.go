// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package slstorage

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// FakeStorage implements the sqlliveness.Reader interface, and the
// slinstace.Writer interface.
type FakeStorage struct {
	mu struct {
		syncutil.Mutex
		sessions map[sqlliveness.SessionID]hlc.Timestamp

		// Used to inject errors into the storage layer.
		insertError func(sid sqlliveness.SessionID, expiration hlc.Timestamp) error
		updateError func(sid sqlliveness.SessionID, expiration hlc.Timestamp) error
	}
}

// NewFakeStorage creates a new FakeStorage.
func NewFakeStorage() *FakeStorage {
	fs := &FakeStorage{}
	fs.mu.sessions = make(map[sqlliveness.SessionID]hlc.Timestamp)
	return fs
}

// SetInjectedFailure adds support for injecting failures for different
// operations.
func (s *FakeStorage) SetInjectedFailure(
	insertError func(sid sqlliveness.SessionID, expiration hlc.Timestamp) error,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.insertError = insertError
}

// SetUpdateInjectedFailure adds support for injecting failures during Update
// operations.
func (s *FakeStorage) SetUpdateInjectedFailure(
	updateError func(sid sqlliveness.SessionID, expiration hlc.Timestamp) error,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.updateError = updateError
}

// IsAlive implements the sqlliveness.Reader interface.
func (s *FakeStorage) IsAlive(
	_ context.Context, sid sqlliveness.SessionID,
) (alive bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.mu.sessions[sid]
	return ok, nil
}

// Insert implements the slinstance.Writer interface.
func (s *FakeStorage) Insert(
	_ context.Context, sid sqlliveness.SessionID, expiration hlc.Timestamp,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Support injecting errors during the initial creation of the session.
	if s.mu.insertError != nil {
		if err := s.mu.insertError(sid, expiration); err != nil {
			return err
		}
	}
	if _, ok := s.mu.sessions[sid]; ok {
		return errors.Errorf("session %s already exists", sid)
	}
	s.mu.sessions[sid] = expiration
	return nil
}

// Update implements the slinstance.Storage interface.
func (s *FakeStorage) Update(
	_ context.Context, sid sqlliveness.SessionID, expiration hlc.Timestamp,
) (bool, hlc.Timestamp, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Support injecting errors during update.
	if s.mu.updateError != nil {
		if err := s.mu.updateError(sid, expiration); err != nil {
			return false, hlc.Timestamp{}, err
		}
	}
	if _, ok := s.mu.sessions[sid]; !ok {
		return false, hlc.Timestamp{}, nil
	}
	s.mu.sessions[sid] = expiration
	return true, expiration, nil
}

// Delete is needed to manually delete a session for testing purposes.
func (s *FakeStorage) Delete(_ context.Context, sid sqlliveness.SessionID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.mu.sessions, sid)
	return nil
}

// InstrumentedStorage wraps FakeStorage to track concurrency metrics for
// benchmarking. It tracks the total number of operations and peak concurrent
// operations to measure thundering herd effects.
type InstrumentedStorage struct {
	*FakeStorage
	totalUpdates atomic.Int64
	currentConc  atomic.Int64
	peakConc     atomic.Int64
}

func NewInstrumentedStorage() *InstrumentedStorage {
	return &InstrumentedStorage{
		FakeStorage: NewFakeStorage(),
	}
}

// Update implements the slinstance.Writer interface with concurrency tracking.
func (s *InstrumentedStorage) Update(
	ctx context.Context, sid sqlliveness.SessionID, expiration hlc.Timestamp,
) (bool, hlc.Timestamp, error) {
	cur := s.currentConc.Add(1)
	defer s.currentConc.Add(-1)
	for {
		peak := s.peakConc.Load()
		if cur <= peak || s.peakConc.CompareAndSwap(peak, cur) {
			break
		}
	}
	s.totalUpdates.Add(1)
	return s.FakeStorage.Update(ctx, sid, expiration)
}

// TotalUpdates returns the total number of Update operations.
func (s *InstrumentedStorage) TotalUpdates() int64 {
	return s.totalUpdates.Load()
}

// PeakConcurrency returns the peak number of concurrent storage operations.
func (s *InstrumentedStorage) PeakConcurrency() int64 {
	return s.peakConc.Load()
}

// ResetMetrics resets all concurrency tracking metrics.
func (s *InstrumentedStorage) ResetMetrics() {
	s.totalUpdates.Store(0)
	s.currentConc.Store(0)
	s.peakConc.Store(0)
}
