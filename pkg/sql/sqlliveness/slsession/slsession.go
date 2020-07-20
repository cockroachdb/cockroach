// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type Session struct {
	Sid sqlliveness.SessionID
	Exp hlc.Timestamp
}

// ID implements the Session interface method ID.
func (s *Session) ID() sqlliveness.SessionID { return s.Sid }

// Expiration implements the Session interface method Expiration.
func (s *Session) Expiration() hlc.Timestamp { return s.Exp }
