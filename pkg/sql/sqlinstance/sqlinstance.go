// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package sqlinstance provides interfaces that will be exposed
// to interact with the sqlinstance subsystem. This subsystem will
// initialize and maintain unique instance ids per SQL instance
// along with mapping of an active instance id to its http addresses.
package sqlinstance

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/errors"
)

// SQLInstance exposes information on a SQL
// instance such as ID, network address and
// the associated sqlliveness Session.
type SQLInstance interface {
	InstanceID() base.SQLInstanceID
	InstanceAddr() string
	SessionID() sqlliveness.SessionID
}

// AddressResolver exposes API for retrieving the instance
// address and all live instances for a tenant.
type AddressResolver interface {
	GetInstanceAddr(ctx context.Context, id base.SQLInstanceID) (string, error)
	GetAllInstancesForTenant(ctx context.Context) ([]SQLInstance, error)
}

// NotStartedError can be returned from calls to the sqlinstance subsystem
// prior to its being started.
var NotStartedError = errors.Errorf("sqlinstance subsystem has not yet been started")
