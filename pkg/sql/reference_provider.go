// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type tableDescReferences []descpb.TableDescriptor_Reference

type referenceProvider struct {
	tableReferences     map[descpb.ID]tableDescReferences
	viewReferences      map[descpb.ID]tableDescReferences
	referencedFunctions catalog.DescriptorIDSet
	referencedSequences catalog.DescriptorIDSet
	referencedTypes     catalog.DescriptorIDSet
	allRelationIDs      catalog.DescriptorIDSet
}

func newReferenceProvider() *referenceProvider {
	return &referenceProvider{
		tableReferences: make(map[descpb.ID]tableDescReferences),
		viewReferences:  make(map[descpb.ID]tableDescReferences),
	}
}

// ReferencedRelationIDs implements scbuildstmt.ReferenceProvider
func (r *referenceProvider) ReferencedRelationIDs() catalog.DescriptorIDSet {
	return r.allRelationIDs
}

// ForEachTableReference implements scbuildstmt.ReferenceProvider
func (r *referenceProvider) ForEachTableReference(
	f func(tblID descpb.ID, idxID descpb.IndexID, colIDs descpb.ColumnIDs) error,
) error {
	var tblIDs catalog.DescriptorIDSet
	for id := range r.tableReferences {
		tblIDs.Add(id)
	}
	for _, id := range tblIDs.Ordered() {
		for _, ref := range r.tableReferences[id] {
			if err := f(id, ref.IndexID, ref.ColumnIDs); err != nil {
				return err
			}
		}
	}
	return nil
}

// ForEachFunctionReference implements scbuildstmt.ReferenceProvider.
func (r *referenceProvider) ForEachFunctionReference(f func(functionID descpb.ID) error) error {
	for _, functionID := range r.referencedFunctions.Ordered() {
		if err := f(functionID); err != nil {
			return err
		}
	}
	return nil
}

// ForEachViewReference implements scbuildstmt.ReferenceProvider
func (r *referenceProvider) ForEachViewReference(
	f func(viewID descpb.ID, colIDs descpb.ColumnIDs) error,
) error {
	var viewIDs catalog.DescriptorIDSet
	for id := range r.viewReferences {
		viewIDs.Add(id)
	}
	for _, id := range viewIDs.Ordered() {
		for _, ref := range r.viewReferences[id] {
			if err := f(id, ref.ColumnIDs); err != nil {
				return err
			}
		}
	}
	return nil
}

// ReferencedSequences implements scbuildstmt.ReferenceProvider
func (r *referenceProvider) ReferencedSequences() catalog.DescriptorIDSet {
	return r.referencedSequences
}

// ReferencedTypes implements scbuildstmt.ReferenceProvider
func (r *referenceProvider) ReferencedTypes() catalog.DescriptorIDSet {
	return r.referencedTypes
}

// ReferencedRoutines implements scbuildstmt.ReferenceProvider
func (r *referenceProvider) ReferencedRoutines() catalog.DescriptorIDSet {
	return r.referencedFunctions
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
	var (
		err      error
		planDeps planDependencies
		typeDeps typeDependencies
		funcDeps functionDependencies
	)
	switch t := optFactory.Memo().RootExpr().(type) {
	case *memo.CreateFunctionExpr:
		planDeps, typeDeps, funcDeps, err = toPlanDependencies(t.Deps, t.TypeDeps, t.FuncDeps)
	case *memo.CreateTriggerExpr:
		planDeps, typeDeps, funcDeps, err = toPlanDependencies(t.Deps, t.TypeDeps, t.FuncDeps)
	default:
		return nil, errors.AssertionFailedf("unexpected root expression: %s", t.(memo.RelExpr).Op())
	}
	if err != nil {
		return nil, err
	}

	ret := newReferenceProvider()

	for descID, refs := range planDeps {
		ret.allRelationIDs.Add(descID)
		if refs.desc.IsView() {
			ret.viewReferences[descID] = append(ret.viewReferences[descID], refs.deps...)
		} else if refs.desc.IsSequence() {
			ret.referencedSequences.Add(descID)
		} else {
			ret.tableReferences[descID] = append(ret.tableReferences[descID], refs.deps...)
		}
	}

	for typeID := range typeDeps {
		desc, err := f.p.descCollection.ByIDWithoutLeased(f.p.txn).WithoutNonPublic().Get().Desc(ctx, typeID)
		if err != nil {
			return nil, err
		}
		if desc.DescriptorType() == catalog.Table {
			ret.allRelationIDs.Add(typeID)
			ret.tableReferences[desc.GetID()] = append(ret.tableReferences[desc.GetID()], descpb.TableDescriptor_Reference{})
		} else {
			ret.referencedTypes.Add(typeID)
		}
	}

	for functionID := range funcDeps {
		ret.referencedFunctions.Add(functionID)
	}
	return ret, nil
}

// NewReferenceProviderFactory returns a new ReferenceProviderFactory.
func NewReferenceProviderFactory(p *planner) scbuild.ReferenceProviderFactory {
	return &referenceProviderFactory{p: p}
}

// NewReferenceProviderFactoryForTest returns a new ReferenceProviderFactory
// only for test. A cleanup function is returned as well, which should be called
// after test is done.
func NewReferenceProviderFactoryForTest(
	ctx context.Context,
	opName redact.SafeString,
	txn *kv.Txn,
	user username.SQLUsername,
	execCfg *ExecutorConfig,
	curDB string,
) (scbuild.ReferenceProviderFactory, func()) {
	sd := NewInternalSessionData(ctx, execCfg.Settings, opName)
	sd.Database = curDB
	ip, cleanup := newInternalPlanner(opName, txn, user, &MemoryMetrics{}, execCfg, sd)
	return &referenceProviderFactory{p: ip}, cleanup
}
