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

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildstmts"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// Build constructs a new state from an initial state and a statement.
//
// The function takes an AST for a DDL statement and constructs targets
// which represent schema changes to be performed.
func Build(
	ctx context.Context, dependencies Dependencies, initial scpb.State, n tree.Statement,
) (scpb.State, error) {
	bs := newBuilderState(dependencies, initial, n)
	{
		bCtx := buildCtx{
			Dependencies: dependencies,
			BuilderState: &bs,
		}
		if err := ProcessStatement(ctx, bCtx, n); err != nil {
			return scpb.State{}, err
		}
	}
	return *bs.output, nil
}

// Export dependency interfaces.
// These are defined in the scbuildstmts package instead of scbuild to avoid
// circular import dependencies.
type (
	// Dependencies contains all the dependencies required by the builder.
	Dependencies = scbuildstmts.Dependencies

	// CatalogReader contains all catalog operations required by the builder.
	CatalogReader = scbuildstmts.CatalogReader

	// AuthorizationAccessor contains all privilege checking operations required
	// by the builder.
	AuthorizationAccessor = scbuildstmts.AuthorizationAccessor
)

// ProcessStatement is its own public function to facilitate testing.
func ProcessStatement(
	ctx context.Context, bCtx scbuildstmts.BuildCtx, n tree.Statement,
) (err error) {
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
		scbuildstmts.AlterTable(ctx, bCtx, n)
	case *tree.CreateIndex:
		scbuildstmts.CreateIndex(ctx, bCtx, n)
	case *tree.DropDatabase:
		scbuildstmts.DropDatabase(ctx, bCtx, n)
	case *tree.DropSchema:
		scbuildstmts.DropSchema(ctx, bCtx, n)
	case *tree.DropSequence:
		scbuildstmts.DropSequence(ctx, bCtx, n)
	case *tree.DropTable:
		scbuildstmts.DropTable(ctx, bCtx, n)
	case *tree.DropType:
		scbuildstmts.DropType(ctx, bCtx, n)
	case *tree.DropView:
		scbuildstmts.DropView(ctx, bCtx, n)
	default:
		panic(scerrors.NotImplementedError(n))
	}
	return err
}

// builderState is the backing struct for scbuildstmts.BuilderState interface.
type builderState struct {
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

// newBuilderState constructs a builderState.
func newBuilderState(
	d scbuildstmts.Dependencies, initial scpb.State, n tree.Statement,
) builderState {
	s := initial.Clone()
	s.Statements = append(s.Statements, &scpb.Statement{
		Statement: n.String(),
	})
	s.Authorization = scpb.Authorization{
		AppName:  d.SessionData().ApplicationName,
		Username: d.SessionData().SessionUser().Normalized(),
	}
	bs := builderState{
		output:          &s,
		sourceElementID: new(scpb.SourceElementID),
		statementMetaData: scpb.TargetMetadata{
			StatementID:     uint32(len(s.Statements) - 1),
			SubWorkID:       1,
			SourceElementID: 1,
		},
	}
	*bs.sourceElementID = 1
	return bs
}

// buildCtx is the backing struct for the scbuildstmts.BuildCtx interface.
// It deliberately embeds the scbuildstmts.BuilderState interface instead of
// the builderState backing struct to avoid leaking the latter's internal state.
type buildCtx struct {
	scbuildstmts.Dependencies
	scbuildstmts.BuilderState
}

var _ scbuildstmts.BuildCtx = buildCtx{}

// WithNewSourceElementID implements the scbuildstmts.BuildCtx interface.
func (b buildCtx) WithNewSourceElementID() scbuildstmts.BuildCtx {
	return buildCtx{
		Dependencies: b.Dependencies,
		BuilderState: b.BuilderStateWithNewSourceElementID(),
	}
}
