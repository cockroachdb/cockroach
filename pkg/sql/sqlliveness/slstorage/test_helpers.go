// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// FakeStorage implements the sqlliveness.Storage interface.
type FakeStorage struct {
	mu struct {
		syncutil.Mutex
		sessions map[string]sqlliveness.Session
	}
}

// NewFakeStorage creates a new FakeStorage.
func NewFakeStorage() *FakeStorage {
	fs := &FakeStorage{}
	fs.mu.sessions = make(map[string]sqlliveness.Session)
	return fs
}

// IsAlive implements the sqlliveness.Storage interface.
func (s *FakeStorage) IsAlive(
	_ context.Context, _ *kv.Txn, sid sqlliveness.SessionID,
) (alive bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.mu.sessions[string(sid)]
	return ok, nil
}

// Insert implements the sqlliveness.Storage interface.
func (s *FakeStorage) Insert(_ context.Context, session sqlliveness.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(session.ID())
	if _, ok := s.mu.sessions[key]; ok {
		return errors.Errorf("Session %s already exists", session.ID())
	}
	s.mu.sessions[key] = session
	return nil
}

// Update implements the sqlliveness.Storage interface.
func (s *FakeStorage) Update(_ context.Context, session sqlliveness.Session) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(session.ID())
	if _, ok := s.mu.sessions[key]; !ok {
		return false, nil
	}
	s.mu.sessions[key] = session
	return true, nil
}

// Delete is needed to manually delete a session for testing purposes.
func (s *FakeStorage) Delete(_ context.Context, session sqlliveness.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.mu.sessions, string(session.ID()))
	return nil
}
