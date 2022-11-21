// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package sqlinstance provides interfaces that will be exposed
// to interact with the sqlinstance subsystem.
// This subsystem will initialize and maintain a unique instance ID
// per SQL instance along with mapping of an active instance ID to
// its SQL address.
package sqlinstance

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/errors"
)

// InstanceInfo exposes information on a SQL instance such as ID, network
// address, the associated sqlliveness.SessionID, and the instance's locality.
type InstanceInfo struct {
	Region       []byte
	InstanceID   base.SQLInstanceID
	InstanceAddr string
	SessionID    sqlliveness.SessionID
	Locality     roachpb.Locality
}

// AddressResolver exposes API for retrieving the instance address and all live instances for a tenant.
type AddressResolver interface {
	// GetInstance returns the InstanceInfo for a pod given the instance ID.
	// Returns a NonExistentInstanceError if an instance with the given ID
	// does not exist.
	GetInstance(context.Context, base.SQLInstanceID) (InstanceInfo, error)
	// GetAllInstances returns a list of all SQL instances for the tenant.
	GetAllInstances(context.Context) ([]InstanceInfo, error)
}

// NonExistentInstanceError can be returned if a SQL instance does not exist.
var NonExistentInstanceError = errors.Errorf("non existent SQL instance")

// NotStartedError can be returned if the sqlinstance subsystem has not been started yet.
var NotStartedError = errors.Errorf("sqlinstance subsystem not started")

// NotASQLInstanceError can be returned if a function is is not supported for
// non-SQL instances.
var NotASQLInstanceError = errors.Errorf("not supported for non-SQL instance")
