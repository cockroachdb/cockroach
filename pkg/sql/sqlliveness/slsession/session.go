// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slsession

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type session struct {
	id sqlliveness.SessionID
	mu struct {
		syncutil.RWMutex
		exp                    hlc.Timestamp
		sessionExpiryCallbacks []func(ctx context.Context)
	}
}

// ID implements the Session interface method ID.
func (s *session) ID() sqlliveness.SessionID { return s.id }

// Expiration implements the Session interface method Expiration.
func (s *session) Expiration() hlc.Timestamp {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.exp
}

// RegisterCallbackForSessionExpiry adds the given function to the list
// of functions called after a session expires. The functions are
// executed in a goroutine.
func (s *session) RegisterCallbackForSessionExpiry(sExp func(context.Context)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.sessionExpiryCallbacks = append(s.mu.sessionExpiryCallbacks, sExp)
}

func (s *session) invokeSessionExpiryCallbacks(ctx context.Context) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, callback := range s.mu.sessionExpiryCallbacks {
		go callback(ctx)
	}
}

func (s *session) setExpiration(exp hlc.Timestamp) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.exp = exp
}
