// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
)

type referenceProvider struct {
	tableReferences planDependencies
	typeReferences  typeDependencies
}

func (r *referenceProvider) ForEachTableReference(
	f func(tblID descpb.ID, refs []descpb.TableDescriptor_Reference) error,
) error {
	allRefs := make([]planDependencyInfo, len(r.tableReferences))
	cnt := 0
	for _, info := range r.tableReferences {
		allRefs[cnt] = info
		cnt++
	}
	sort.Slice(allRefs, func(i, j int) bool {
		return allRefs[i].desc.GetID() < allRefs[j].desc.GetID()
	})
	for _, info := range allRefs {
		if err := f(info.desc.GetID(), info.deps); err != nil {
			return err
		}
	}
	return nil
}

func (r *referenceProvider) ForEachTypeReference(f func(typeID descpb.ID) error) error {
	var allRefs catalog.DescriptorIDSet
	for id := range r.typeReferences {
		allRefs.Add(id)
	}
	for _, typeID := range allRefs.Ordered() {
		if err := f(typeID); err != nil {
			return err
		}
	}
	return nil
}

type referenceProviderFactory struct {
	p *planner
}

func (f *referenceProviderFactory) NewReferenceProvider(
	ctx context.Context, stmt tree.Statement,
) (scbuild.ReferenceProvider, error) {
	ctlg := &optCatalog{}
	ctlg.init(f.p)
	var optFactory norm.Factory
	optFactory.Init(ctx, f.p.EvalContext(), ctlg)
	optBld := optbuilder.New(ctx, f.p.SemaCtx(), f.p.EvalContext(), ctlg, &optFactory, stmt)
	if err := optBld.Build(); err != nil {
		return nil, err
	}
	createFnExpr := optFactory.Memo().RootExpr().(*memo.CreateFunctionExpr)
	tableReferences, typeReferences, err := toPlanDependencies(createFnExpr.Deps, createFnExpr.TypeDeps)
	if err != nil {
		return nil, err
	}
	return &referenceProvider{
		tableReferences: tableReferences,
		typeReferences:  typeReferences,
	}, nil
}

// NewReferenceProviderFactory returns a new ReferenceProviderFactory.
func NewReferenceProviderFactory(p *planner) scbuild.ReferenceProviderFactory {
	return &referenceProviderFactory{p: p}
}

// NewReferenceProviderFactoryForTest returns a new ReferenceProviderFactory
// only for test. A cleanup function is returned as well, which should be called
// after test is done.
func NewReferenceProviderFactoryForTest(
	opName string, txn *kv.Txn, user username.SQLUsername, execCfg *ExecutorConfig, curDB string,
) (scbuild.ReferenceProviderFactory, func()) {
	ip, cleanup := newInternalPlanner(opName, txn, user, &MemoryMetrics{}, execCfg, sessiondatapb.SessionData{})
	ip.SessionData().Database = "defaultdb"
	return &referenceProviderFactory{p: ip}, cleanup
}
