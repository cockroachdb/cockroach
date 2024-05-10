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
	crypto_rand "crypto/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/faketreeeval"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildstmt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/ulid"
)

var _ scbuildstmt.TreeContextBuilder = buildCtx{}

// SemaCtx implements the scbuildstmt.TreeContextBuilder interface.
func (b buildCtx) SemaCtx() *tree.SemaContext {
	return newSemaCtx(b.Dependencies)
}

func newSemaCtx(d Dependencies) *tree.SemaContext {
	semaCtx := tree.MakeSemaContext(d.CatalogReader())
	semaCtx.Annotations = nil
	semaCtx.SearchPath = &d.SessionData().SearchPath
	semaCtx.DateStyle = d.SessionData().GetDateStyle()
	semaCtx.IntervalStyle = d.SessionData().GetIntervalStyle()
	semaCtx.UnsupportedTypeChecker = eval.NewUnsupportedTypeChecker(d.ClusterSettings().Version)
	return &semaCtx
}

// EvalCtx implements the scbuildstmt.TreeContextBuilder interface.
func (b buildCtx) EvalCtx() *eval.Context {
	return newEvalCtx(b.Context, b.Dependencies)
}

func newEvalCtx(ctx context.Context, d Dependencies) *eval.Context {
	evalCtx := &eval.Context{
		ClusterID:            d.ClusterID(),
		SessionDataStack:     sessiondata.NewStack(d.SessionData()),
		Planner:              &faketreeeval.DummyEvalPlanner{},
		StreamManagerFactory: &faketreeeval.DummyStreamManagerFactory{},
		PrivilegedAccessor:   &faketreeeval.DummyPrivilegedAccessor{},
		SessionAccessor:      &faketreeeval.DummySessionAccessor{},
		ClientNoticeSender:   d.ClientNoticeSender(),
		Sequence:             &faketreeeval.DummySequenceOperators{},
		Tenant:               &faketreeeval.DummyTenantOperator{},
		Regions:              &faketreeeval.DummyRegionOperator{},
		Settings:             d.ClusterSettings(),
		Codec:                d.Codec(),
		DescIDGenerator:      d.DescIDGenerator(),
		ULIDEntropy:          ulid.Monotonic(crypto_rand.Reader, 0),
	}
	evalCtx.SetDeprecatedContext(ctx)
	return evalCtx
}
