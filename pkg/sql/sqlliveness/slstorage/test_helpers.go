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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/errors"
)

// FakeStorage implements the sqlliveness.Storage interface.
type FakeStorage struct {
	mu       sync.Mutex
	sessions map[string]sqlliveness.Session
}

func NewFakeStorage() *FakeStorage {
	fs := &FakeStorage{}
	fs.sessions = make(map[string]sqlliveness.Session)
	return fs
}

func (s *FakeStorage) IsAlive(
	_ context.Context, _ *kv.Txn, sid sqlliveness.SessionID,
) (alive bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.sessions[string(sid)]
	return ok, nil
}

func (s *FakeStorage) Insert(_ context.Context, session sqlliveness.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(session.ID())
	if _, ok := s.sessions[string(key)]; ok {
		return errors.Errorf("Session %s already exists", session.ID())
	}
	s.sessions[key] = session
	return nil
}

func (s *FakeStorage) Update(_ context.Context, session sqlliveness.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(session.ID())
	if _, ok := s.sessions[string(key)]; !ok {
		return errors.Errorf("Session %s does not exist", session.ID())
	}
	s.sessions[key] = session
	return nil
}

func (s *FakeStorage) Delete(_ context.Context, session sqlliveness.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, string(session.ID()))
	return nil
}
