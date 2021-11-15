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

	scbuildimpl2 "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildimpl"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/scbuildctx"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// Build constructs a new state from an initial state and a statement.
//
// The function takes an AST for a DDL statement and constructs targets
// which represent schema changes to be performed. These targets are enqueued
// via AddNode method calls on scbuild.StateBuilder.
func Build(
	ctx context.Context, dependencies scbuildctx.Dependencies, initial scpb.State, n tree.Statement,
) (scpb.State, error) {
	builder := scbuildctx.NewBuilder(dependencies, initial, n)
	if err := ProcessStatement(ctx, buildCtx{BuilderState: builder.BuilderState()}, n); err != nil {
		return scpb.State{}, err
	}
	return builder.Build(), nil
}

// ProcessStatement is its own public function to facilitate testing.
func ProcessStatement(ctx context.Context, bCtx scbuildctx.BuildCtx, n tree.Statement) (err error) {
	defer func() {
		if recErr := recover(); recErr != nil {
			if errObj, ok := recErr.(error); ok {
				err = errObj
			} else {
				err = errors.Errorf("unexpected error encountered while building schema change plan %s", recErr)
			}
		}
	}()
	switch n := n.(type) {
	case *tree.AlterTable:
		scbuildimpl2.AlterTable(ctx, bCtx, n)
	case *tree.CreateIndex:
		scbuildimpl2.CreateIndex(ctx, bCtx, n)
	case *tree.DropDatabase:
		scbuildimpl2.DropDatabase(ctx, bCtx, n)
	case *tree.DropSchema:
		scbuildimpl2.DropSchema(ctx, bCtx, n)
	case *tree.DropSequence:
		scbuildimpl2.DropSequence(ctx, bCtx, n)
	case *tree.DropTable:
		scbuildimpl2.DropTable(ctx, bCtx, n)
	case *tree.DropType:
		scbuildimpl2.DropType(ctx, bCtx, n)
	case *tree.DropView:
		scbuildimpl2.DropView(ctx, bCtx, n)
	default:
		panic(scbuildctx.NotImplementedError(n))
	}
	return err
}

// buildCtx is the backing struct for the BuildCtx interface.
type buildCtx struct {
	scbuildctx.BuilderState
}

var _ scbuildctx.BuildCtx = buildCtx{}

// BuildCtxWithNewSourceElementID implements the BuildCtx interface.
func (b buildCtx) BuildCtxWithNewSourceElementID() scbuildctx.BuildCtx {
	return buildCtx{BuilderState: b.BuilderStateWithNewSourceElementID()}
}

// TestingBuildCtx is the backing struct for the BuildCtx interface used
// in unit tests.
type TestingBuildCtx struct {
	scbuildctx.BuilderState
	scbuildctx.TreeContextBuilder
	scbuildctx.PrivilegeChecker
	scbuildctx.DescriptorReader
	scbuildctx.NameResolver
	scbuildctx.NodeEnqueuerAndChecker
	scbuildctx.TableElementIDGenerator
}

var _ scbuildctx.BuildCtx = TestingBuildCtx{}

// BuildCtxWithNewSourceElementID implements the BuildCtx interface.
func (b TestingBuildCtx) BuildCtxWithNewSourceElementID() scbuildctx.BuildCtx {
	// No-op because we're not going to unit-test eventlog behavior.
	return b
}

// NewTestingBuildImplCtx creates a new TestingBuildCtx populated with default
// interface implementations.
func NewTestingBuildImplCtx(b scbuildctx.BuilderState) TestingBuildCtx {
	return TestingBuildCtx{
		BuilderState:            b,
		TreeContextBuilder:      buildCtx{BuilderState: b},
		PrivilegeChecker:        buildCtx{BuilderState: b},
		DescriptorReader:        buildCtx{BuilderState: b},
		NameResolver:            buildCtx{BuilderState: b},
		NodeEnqueuerAndChecker:  buildCtx{BuilderState: b},
		TableElementIDGenerator: buildCtx{BuilderState: b},
	}
}

// Squelch linter.
// TODO(postamar): write unit tests which use this.
var _ = NewTestingBuildImplCtx

// TestingNodeAccumulator is the backing struct for the NodeAccumulator
// interface to be used in unit tests.
// TODO(postamar): write unit tests that use this.
type TestingNodeAccumulator struct {
	Nodes []*scpb.Node
}

var _ scbuildctx.NodeAccumulator = TestingNodeAccumulator{}

// AddNode implements the NodeAccumulator interface.
func (acc TestingNodeAccumulator) AddNode(dir scpb.Target_Direction, elem scpb.Element) {
	node := &scpb.Node{
		Target: scpb.NewTarget(dir, elem, nil /* metadata */),
	}
	switch dir {
	case scpb.Target_ADD:
		node.Status = scpb.Status_ABSENT
	case scpb.Target_DROP:
		node.Status = scpb.Status_PUBLIC
	}
	acc.Nodes = append(acc.Nodes, node)
}

// ForEachNode implements the NodeAccumulator interface.
func (acc TestingNodeAccumulator) ForEachNode(
	fn func(status scpb.Status, dir scpb.Target_Direction, elem scpb.Element),
) {
	for _, node := range acc.Nodes {
		fn(node.Status, node.Direction, node.Element())
	}
}

// TestingBuilderState is the backing struct for the BuilderState interface
// to be used in unit tests.
// TODO(postamar): write unit tests that use this.
type TestingBuilderState struct {
	scbuildctx.Dependencies
	scbuildctx.NodeAccumulator
}

var _ scbuildctx.BuilderState = TestingBuilderState{}

// IncrementSubWorkID implements the BuilderState interface.
func (b TestingBuilderState) IncrementSubWorkID() {
	// No-op because we're not going to unit-test eventlog behavior.
}

// BuilderStateWithNewSourceElementID implements the BuilderState interface.
func (b TestingBuilderState) BuilderStateWithNewSourceElementID() scbuildctx.BuilderState {
	// No-op because we're not going to unit-test eventlog behavior.
	return b
}
