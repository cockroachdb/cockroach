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

// Instance implements the sqlinstance.SQLInstance
// interface.
type Instance struct {
	id        base.SQLInstanceID
	httpAddr  string
	sessionID sqlliveness.SessionID
}

// NewSQLInstance constructs a new instancestorage.Instance.
func NewSQLInstance(
	id base.SQLInstanceID, httpAddr string, sessionID sqlliveness.SessionID,
) *Instance {
	return &Instance{
		id:        id,
		httpAddr:  httpAddr,
		sessionID: sessionID,
	}
}

// InstanceID returns the base.SQLInstanceID
// associated with the sqlinstance.Instance.
func (i *Instance) InstanceID() base.SQLInstanceID {
	return i.id
}

// InstanceAddr returns the HTTP address
// associated with the sqlinstance.Instance
func (i *Instance) InstanceAddr() string {
	return i.httpAddr
}

// SessionID returns the sqlliveness.SessionID
// associated with the sqlinstance.Instance
func (i *Instance) SessionID() sqlliveness.SessionID {
	return i.sessionID
}
