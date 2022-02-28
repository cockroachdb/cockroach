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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/faketreeeval"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildstmt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

var _ scbuildstmt.TreeContextBuilder = buildCtx{}

// SemaCtx implements the scbuildstmt.TreeContextBuilder interface.
func (b buildCtx) SemaCtx() *tree.SemaContext {
	return newSemaCtx(b.Dependencies)
}

func newSemaCtx(d Dependencies) *tree.SemaContext {
	semaCtx := tree.MakeSemaContext()
	semaCtx.Annotations = nil
	semaCtx.SearchPath = d.SessionData().SearchPath
	semaCtx.IntervalStyleEnabled = d.SessionData().IntervalStyleEnabled
	semaCtx.DateStyleEnabled = d.SessionData().DateStyleEnabled
	semaCtx.TypeResolver = d.CatalogReader()
	semaCtx.TableNameResolver = d.CatalogReader()
	semaCtx.DateStyle = d.SessionData().GetDateStyle()
	semaCtx.IntervalStyle = d.SessionData().GetIntervalStyle()
	return &semaCtx
}

// EvalCtx implements the scbuildstmt.TreeContextBuilder interface.
func (b buildCtx) EvalCtx() *tree.EvalContext {
	return newEvalCtx(b.Context, b.Dependencies)
}

func newEvalCtx(ctx context.Context, d Dependencies) *tree.EvalContext {
	return &tree.EvalContext{
		ClusterID:          d.ClusterID(),
		SessionDataStack:   sessiondata.NewStack(d.SessionData()),
		Context:            ctx,
		Planner:            &faketreeeval.DummyEvalPlanner{},
		PrivilegedAccessor: &faketreeeval.DummyPrivilegedAccessor{},
		SessionAccessor:    &faketreeeval.DummySessionAccessor{},
		ClientNoticeSender: &faketreeeval.DummyClientNoticeSender{},
		Sequence:           &faketreeeval.DummySequenceOperators{},
		Tenant:             &faketreeeval.DummyTenantOperator{},
		Regions:            &faketreeeval.DummyRegionOperator{},
		Settings:           d.ClusterSettings(),
		Codec:              d.Codec(),
	}
}
