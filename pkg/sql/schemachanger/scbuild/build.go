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

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildstmt"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// Build constructs a new state from an initial state and a statement.
//
// The function takes an AST for a DDL statement and constructs targets
// which represent schema changes to be performed.
func Build(
	ctx context.Context, dependencies Dependencies, initial scpb.CurrentState, n tree.Statement,
) (_ scpb.CurrentState, err error) {
	initial = initial.DeepCopy()
	bs := newBuilderState(initial)
	els := newEventLogState(dependencies, initial, n)
	// TODO(fqazi): The optimizer can end up already modifying the statement above
	// to fully resolve names. We need to take this into account for CTAS/CREATE
	// VIEW statements.
	an, err := newAstAnnotator(n)
	if err != nil {
		return scpb.CurrentState{}, err
	}
	b := buildCtx{
		Context:       ctx,
		Dependencies:  dependencies,
		BuilderState:  bs,
		EventLogState: els,
		TreeAnnotator: an,
	}
	defer func() {
		if recErr := recover(); recErr != nil {
			if errObj, ok := recErr.(error); ok {
				err = errObj
			} else {
				err = errors.Errorf("unexpected error encountered while building schema change plan %s", recErr)
			}
		}
	}()
	scbuildstmt.Process(b, an.GetStatement())
	an.ValidateAnnotations()
	els.statements[len(els.statements)-1].RedactedStatement =
		string(els.astFormatter.FormatAstAsRedactableString(an.GetStatement(), &an.annotation))
	ts := scpb.TargetState{
		Targets:       make([]scpb.Target, len(bs.output)),
		Statements:    els.statements,
		Authorization: els.authorization,
	}
	current := make([]scpb.Status, len(bs.output))
	for i, e := range bs.output {
		ts.Targets[i] = scpb.MakeTarget(e.targetStatus, e.element, &e.metadata)
		current[i] = e.currentStatus
	}
	return scpb.CurrentState{TargetState: ts, Current: current}, nil
}

// Export dependency interfaces.
// These are defined in the scbuildstmts package instead of scbuild to avoid
// circular import dependencies.
type (
	// Dependencies contains all the dependencies required by the builder.
	Dependencies = scbuildstmt.Dependencies

	// CatalogReader contains all catalog operations required by the builder.
	CatalogReader = scbuildstmt.CatalogReader

	// AuthorizationAccessor contains all privilege checking operations required
	// by the builder.
	AuthorizationAccessor = scbuildstmt.AuthorizationAccessor

	// AstFormatter contains operations for formatting out AST nodes into
	// SQL statement text.
	AstFormatter = scbuildstmt.AstFormatter
)

type elementState struct {
	element                     scpb.Element
	targetStatus, currentStatus scpb.Status
	metadata                    scpb.TargetMetadata
}

// builderState is the backing struct for scbuildstmt.BuilderState interface.
type builderState struct {
	// output contains the schema change targets that have been planned so far.
	output []elementState
}

// newBuilderState constructs a builderState.
func newBuilderState(initial scpb.CurrentState) *builderState {
	bs := builderState{output: make([]elementState, len(initial.Current))}
	for i, t := range initial.TargetState.Targets {
		bs.output[i] = elementState{
			element:       t.Element(),
			targetStatus:  t.TargetStatus,
			currentStatus: initial.Current[i],
			metadata:      t.Metadata,
		}
	}
	return &bs
}

// eventLogState is the backing struct for scbuildstmt.EventLogState interface.
type eventLogState struct {

	// statements contains the statements in the schema changer state.
	statements []scpb.Statement

	// authorization contains application and user names for the current session.
	authorization scpb.Authorization

	// statementMetaData is used to associate each element in the output to the
	// statement which resulted in it being added there.
	statementMetaData scpb.TargetMetadata

	// sourceElementID tracks the parent elements responsible
	// for any new elements added. This is used for detailed
	// tracking during cascade operations.
	sourceElementID *scpb.SourceElementID

	// astFormatter used to format AST elements as redactable strings.
	astFormatter AstFormatter
}

// newEventLogState constructs an eventLogState.
func newEventLogState(
	d scbuildstmt.Dependencies, initial scpb.CurrentState, n tree.Statement,
) *eventLogState {
	stmts := initial.Statements
	els := eventLogState{
		statements: append(stmts, scpb.Statement{
			Statement: n.String(),
		}),
		authorization: scpb.Authorization{
			AppName:  d.SessionData().ApplicationName,
			UserName: d.SessionData().SessionUser().Normalized(),
		},
		sourceElementID: new(scpb.SourceElementID),
		statementMetaData: scpb.TargetMetadata{
			StatementID:     uint32(len(stmts)),
			SubWorkID:       1,
			SourceElementID: 1,
		},
		astFormatter: d.AstFormatter(),
	}
	*els.sourceElementID = 1
	return &els
}

// buildCtx is the backing struct for the scbuildstmt.BuildCtx interface.
// It deliberately embeds the scbuildstmt.BuilderState interface instead of
// the builderState backing struct to avoid leaking the latter's internal state.
type buildCtx struct {
	context.Context
	scbuildstmt.Dependencies
	scbuildstmt.BuilderState
	scbuildstmt.EventLogState
	scbuildstmt.TreeAnnotator
}

var _ scbuildstmt.BuildCtx = buildCtx{}

// WithNewSourceElementID implements the scbuildstmt.BuildCtx interface.
func (b buildCtx) WithNewSourceElementID() scbuildstmt.BuildCtx {
	return buildCtx{
		Context:       b.Context,
		Dependencies:  b.Dependencies,
		BuilderState:  b.BuilderState,
		TreeAnnotator: b.TreeAnnotator,
		EventLogState: b.EventLogStateWithNewSourceElementID(),
	}
}
