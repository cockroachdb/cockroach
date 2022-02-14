// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"github.com/cockroachdb/cockroach/pkg/sql/faketreeeval"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildstmt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

var _ scbuildstmt.TreeContextBuilder = buildCtx{}

// SemaCtx implements the scbuildstmt.TreeContextBuilder interface.
func (b buildCtx) SemaCtx() *tree.SemaContext {
	semaCtx := tree.MakeSemaContext()
	semaCtx.Annotations = nil
	semaCtx.SearchPath = b.SessionData().SearchPath
	semaCtx.IntervalStyleEnabled = b.SessionData().IntervalStyleEnabled
	semaCtx.DateStyleEnabled = b.SessionData().DateStyleEnabled
	semaCtx.TypeResolver = b.CatalogReader()
	semaCtx.TableNameResolver = b.CatalogReader()
	semaCtx.DateStyle = b.SessionData().GetDateStyle()
	semaCtx.IntervalStyle = b.SessionData().GetIntervalStyle()
	return &semaCtx
}

// EvalCtx implements the scbuildstmt.TreeContextBuilder interface.
func (b buildCtx) EvalCtx() *tree.EvalContext {
	return &tree.EvalContext{
		ClusterID:          b.ClusterID(),
		SessionDataStack:   sessiondata.NewStack(b.SessionData()),
		Context:            b.Context,
		Planner:            &faketreeeval.DummyEvalPlanner{},
		PrivilegedAccessor: &faketreeeval.DummyPrivilegedAccessor{},
		SessionAccessor:    &faketreeeval.DummySessionAccessor{},
		ClientNoticeSender: &faketreeeval.DummyClientNoticeSender{},
		Sequence:           &faketreeeval.DummySequenceOperators{},
		Tenant:             &faketreeeval.DummyTenantOperator{},
		Regions:            &faketreeeval.DummyRegionOperator{},
		Settings:           b.ClusterSettings(),
		Codec:              b.Codec(),
	}
}
