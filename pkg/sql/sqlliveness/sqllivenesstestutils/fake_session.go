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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// FakeSession is an implementation of sqlliveness.Session for testing.
type FakeSession struct {
	SessionID sqlliveness.SessionID
	ExpTS     hlc.Timestamp
	StartTS   hlc.Timestamp
}

var _ sqlliveness.Session = (*FakeSession)(nil)

// ID returns f.SessionID.
func (f *FakeSession) ID() sqlliveness.SessionID { return f.SessionID }

// Expiration returns f.ExpTS.
func (f *FakeSession) Expiration() hlc.Timestamp { return f.ExpTS }

// Start return f.StartTS.
func (f *FakeSession) Start() hlc.Timestamp { return f.StartTS }
