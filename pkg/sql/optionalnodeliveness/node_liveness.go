// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optionalnodeliveness

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
)

// Container optionally gives access to liveness information about
// the KV nodes. It is typically not available to anyone but the system tenant.
type Container struct {
	w errorutil.TenantSQLDeprecatedWrapper
}

// MakeContainer initializes an Container wrapping a
// (possibly nil) *NodeVitalityInterface.
//
// Use of node liveness from within the SQL layer is **deprecated**. Please do
// not introduce new uses of it.
//
// See TenantSQLDeprecatedWrapper for details.
func MakeContainer(nl livenesspb.NodeVitalityInterface) Container {
	return Container{
		w: errorutil.MakeTenantSQLDeprecatedWrapper(nl, nl != nil),
	}
}

// OptionalErr returns the NodeLiveness instance if available. Otherwise, it
// returns an error referring to the optionally passed in issues.
//
// Use of NodeLiveness from within the SQL layer is **deprecated**. Please do
// not introduce new uses of it.
func (nl *Container) OptionalErr(issue int) (livenesspb.NodeVitalityInterface, error) {
	v, err := nl.w.OptionalErr(issue)
	if err != nil {
		return nil, err
	}
	return v.(livenesspb.NodeVitalityInterface), nil
}

var _ = (*Container)(nil).OptionalErr // silence unused lint

// Optional returns the NodeLiveness instance and true if available.
// Otherwise, returns nil and false. Prefer OptionalErr where possible.
//
// Use of NodeLiveness from within the SQL layer is **deprecated**. Please do
// not introduce new uses of it.
func (nl *Container) Optional(issue int) (livenesspb.NodeVitalityInterface, bool) {
	v, ok := nl.w.Optional()
	if !ok {
		return nil, false
	}
	return v.(livenesspb.NodeVitalityInterface), true
}
