// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package instancestorage

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
)

type instance struct {
	id        base.SQLInstanceID
	httpAddr  string
	sessionID sqlliveness.SessionID
}

func newSQLInstance(
	id base.SQLInstanceID, httpAddr string, sessionID sqlliveness.SessionID,
) *instance {
	return &instance{
		id:        id,
		httpAddr:  httpAddr,
		sessionID: sessionID,
	}
}

// InstanceID returns the base.SQLInstanceID
// associated with the sqlinstance.Instance.
func (i *instance) InstanceID() base.SQLInstanceID {
	return i.id
}

// InstanceAddr returns the HTTP address
// associated with the sqlinstance.Instance
func (i *instance) InstanceAddr() string {
	return i.httpAddr
}

// SessionID returns the sqlliveness.SessionID
// associated with the sqlinstance.Instance
func (i *instance) SessionID() sqlliveness.SessionID {
	return i.sessionID
}
