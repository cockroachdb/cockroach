// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildctx

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// NodeAccumulator exposes operations on a builder's accumulated nodes.
type NodeAccumulator interface {

	// AddNode adds a node into the NodeAccumulator.
	AddNode(dir scpb.Target_Direction, elem scpb.Element)

	// ForEachNode iterates over the accumulated notes in the NodeAccumulator.
	ForEachNode(fn func(status scpb.Status, dir scpb.Target_Direction, elem scpb.Element))
}

// BuilderState encapsulates the state of the planned schema changes, hiding
// its internal state to anything that ends up using it and only allowing
// state changes via the provided methods.
type BuilderState interface {
	Dependencies
	NodeAccumulator

	// IncrementSubWorkID increments the current subwork ID used for tracking
	// when a statement does operations on multiple objects or in multiple
	// commands.
	IncrementSubWorkID()

	// BuilderStateWithNewSourceElementID returns a BuilderState with an
	// incremented source element ID, which will be inherited by any element added
	// using the returned BuilderState.
	BuilderStateWithNewSourceElementID() BuilderState
}

// Builder is used by scbuild.Build.
type Builder interface {
	BuilderState() BuilderState
	Build() scpb.State
}

// builderState is the backing struct for BuilderState and Builder interfaces.
type builderState struct {
	Dependencies

	// output contains the schema change targets that have been planned so far.
	output *scpb.State

	// statementMetaData is used to associate each element in the output to the
	// statement which resulted in it being added there.
	statementMetaData scpb.TargetMetadata

	// sourceElementID tracks the parent elements responsible
	// for any new elements added. This is used for detailed
	// tracking during cascade operations.
	sourceElementID *scpb.SourceElementID
}

var _ NodeAccumulator = (*builderState)(nil)
var _ BuilderState = (*builderState)(nil)
var _ Builder = (*builderState)(nil)

// NewBuilder constructs a Builder.
func NewBuilder(d Dependencies, initial scpb.State, n tree.Statement) Builder {
	o := initial.Clone()
	o.Statements = append(o.Statements, &scpb.Statement{
		Statement: n.String(),
	})
	o.Authorization = scpb.Authorization{
		AppName:  d.SessionData().ApplicationName,
		Username: d.SessionData().SessionUser().Normalized(),
	}
	sourceElementID := new(scpb.SourceElementID)
	*sourceElementID = 1
	b := &builderState{
		Dependencies:    d,
		output:          &o,
		sourceElementID: sourceElementID,
		statementMetaData: scpb.TargetMetadata{
			StatementID:     uint32(len(o.Statements) - 1),
			SubWorkID:       1,
			SourceElementID: *sourceElementID,
		},
	}
	return b
}

// IncrementSubWorkID implements the BuilderState interface.
func (b *builderState) IncrementSubWorkID() {
	b.statementMetaData.SubWorkID++
}

// AddNode implements the NodeAccumulator interface.
func (b *builderState) AddNode(dir scpb.Target_Direction, elem scpb.Element) {
	for _, node := range b.output.Nodes {
		if screl.EqualElements(node.Element(), elem) {
			panic(errors.AssertionFailedf("element already present in builder state: %s", elem))
		}
	}
	b.output.Nodes = append(b.output.Nodes, &scpb.Node{
		Target: scpb.NewTarget(dir, elem, &b.statementMetaData),
		Status: nodeStatusFromDirection(dir),
	})
}

func nodeStatusFromDirection(dir scpb.Target_Direction) scpb.Status {
	switch dir {
	case scpb.Target_ADD:
		return scpb.Status_ABSENT
	case scpb.Target_DROP:
		return scpb.Status_PUBLIC
	default:
		panic(errors.AssertionFailedf("unknown direction %s", dir))
	}
}

// BuilderStateWithNewSourceElementID implements the BuilderState interface.
func (b *builderState) BuilderStateWithNewSourceElementID() BuilderState {
	*b.sourceElementID++
	return &builderState{
		Dependencies:    b.Dependencies,
		output:          b.output,
		sourceElementID: b.sourceElementID,
		statementMetaData: scpb.TargetMetadata{
			StatementID:     b.statementMetaData.StatementID,
			SubWorkID:       b.statementMetaData.SubWorkID,
			SourceElementID: *b.sourceElementID,
		},
	}
}

// ForEachNode implements the NodeAccumulator interface.
func (b *builderState) ForEachNode(
	fn func(status scpb.Status, dir scpb.Target_Direction, elem scpb.Element),
) {
	for _, node := range b.output.Nodes {
		fn(node.Status, node.Direction, node.Element())
	}
}

// BuilderState implements the Builder interface.
func (b *builderState) BuilderState() BuilderState {
	return b
}

// Build implements the Builder interface.
func (b *builderState) Build() scpb.State {
	// We deliberately deep-copy the output to avoid exposing references to the
	// internal state.
	return b.output.Clone()
}
