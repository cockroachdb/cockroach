// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package slstorage

import (
	"context"

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
