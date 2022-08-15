// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqllivenesstestutils

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// ExpiringSession is a session that can be forcibly expired for testing purposes.
type ExpiringSession struct {
	name       string
	expiration hlc.Timestamp
}

// NewExpiringSession constructs and returns an expiring session.
func NewExpiringSession(name string) ExpiringSession {
	return ExpiringSession{name, hlc.MaxTimestamp}
}

// ID implements the sqlliveness.Session interface.
func (f ExpiringSession) ID() sqlliveness.SessionID {
	return sqlliveness.SessionID(f.name)
}

// Expiration implements the sqlliveness.Session interface.
func (f *ExpiringSession) Expiration() hlc.Timestamp { return f.expiration }

// Start implements the sqlliveness.Session interface.
func (f ExpiringSession) Start() hlc.Timestamp { return hlc.MinTimestamp }

// RegisterCallbackForSessionExpiry implements the sqlliveness.Session interface.
func (f ExpiringSession) RegisterCallbackForSessionExpiry(func(context.Context)) {}

// Expire is a testing hack to force the session to expire.
func (f *ExpiringSession) Expire() {
	f.expiration = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
}
