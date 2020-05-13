// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storagepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
)

type optionalNodeLivenessI interface {
	Self() (storagepb.Liveness, error)
	GetLivenesses() []storagepb.Liveness
	IsLive(roachpb.NodeID) (bool, error)
}

type OptionalNodeLiveness struct {
	w errorutil.TenantSQLDeprecatedWrapper
}

// MakeOptionalNodeLiveness initializes an OptionalNodeLiveness wrapping a
// (possibly nil) *NodeLiveness.
//
// Use of node liveness from within the SQL layer is **deprecated**. Please do
// not introduce new uses of it.
//
// See TenantSQLDeprecatedWrapper for details.
func MakeOptionalNodeLiveness(nl optionalNodeLivenessI) OptionalNodeLiveness {
	const exposed = false
	return OptionalNodeLiveness{
		w: errorutil.MakeTenantSQLDeprecatedWrapper(nl, nl != nil),
	}
}

// OptionalErr returns the NodeLiveness instance if available. Otherwise, it
// returns an error referring to the optionally passed in issues.
//
// Use of NodeLiveness from within the SQL layer is **deprecated**. Please do
// not introduce new uses of it.
func (nl *OptionalNodeLiveness) OptionalErr(issueNos ...int) (optionalNodeLivenessI, error) {
	v, err := nl.w.OptionalErr(issueNos...)
	if err != nil {
		return nil, err
	}
	return v.(optionalNodeLivenessI), nil
}

// OptionalErr returns the NodeLiveness instance and true if available.
// Otherwise, returns nil and false. Prefer OptionalErr where possible.
//
// Use of NodeLiveness from within the SQL layer is **deprecated**. Please do
// not introduce new uses of it.
func (nl *OptionalNodeLiveness) Optional(issueNos ...int) (optionalNodeLivenessI, bool) {
	v, ok := nl.w.Optional()
	if !ok {
		return nil, false
	}
	return v.(optionalNodeLivenessI), true
}
