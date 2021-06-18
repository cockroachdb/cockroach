// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optionalnodeliveness

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
)

// Interface is the interface used in Container.
type Interface interface {
	Self() (livenesspb.Liveness, bool)
	GetLivenesses() []livenesspb.Liveness
	GetLivenessesFromKV(ctx context.Context) ([]livenesspb.Liveness, error)
	IsAvailable(roachpb.NodeID) bool
	IsAvailableNotDraining(roachpb.NodeID) bool
	IsLive(roachpb.NodeID) (bool, error)
}

// Container optionally gives access to liveness information about
// the KV nodes. It is typically not available to anyone but the system tenant.
type Container struct {
	w errorutil.TenantSQLDeprecatedWrapper
}

// MakeContainer initializes an Container wrapping a
// (possibly nil) *NodeLiveness.
//
// Use of node liveness from within the SQL layer is **deprecated**. Please do
// not introduce new uses of it.
//
// See TenantSQLDeprecatedWrapper for details.
func MakeContainer(nl Interface) Container {
	return Container{
		w: errorutil.MakeTenantSQLDeprecatedWrapper(nl, nl != nil),
	}
}

// OptionalErr returns the NodeLiveness instance if available. Otherwise, it
// returns an error referring to the optionally passed in issues.
//
// Use of NodeLiveness from within the SQL layer is **deprecated**. Please do
// not introduce new uses of it.
func (nl *Container) OptionalErr(issue int) (Interface, error) {
	v, err := nl.w.OptionalErr(issue)
	if err != nil {
		return nil, err
	}
	return v.(Interface), nil
}

var _ = (*Container)(nil).OptionalErr // silence unused lint

// Optional returns the NodeLiveness instance and true if available.
// Otherwise, returns nil and false. Prefer OptionalErr where possible.
//
// Use of NodeLiveness from within the SQL layer is **deprecated**. Please do
// not introduce new uses of it.
func (nl *Container) Optional(issue int) (Interface, bool) {
	v, ok := nl.w.Optional()
	if !ok {
		return nil, false
	}
	return v.(Interface), true
}
