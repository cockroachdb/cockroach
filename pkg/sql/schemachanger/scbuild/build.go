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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildstmt"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
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
	bs := newBuilderState(ctx, dependencies, initial)
	els := newEventLogState(dependencies, initial, n)
	// TODO(fqazi): The optimizer can end up already modifying the statement above
	// to fully resolve names. We need to take this into account for CTAS/CREATE
	// VIEW statements.
	an, err := newAstAnnotator(n)
	if err != nil {
		return scpb.CurrentState{}, err
	}
	b := buildCtx{
		Context:              ctx,
		Dependencies:         dependencies,
		BuilderState:         bs,
		EventLogState:        els,
		TreeAnnotator:        an,
		SchemaFeatureChecker: dependencies.FeatureChecker(),
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
		Targets:       make([]scpb.Target, 0, len(bs.output)),
		Statements:    els.statements,
		Authorization: els.authorization,
	}
	current := make([]scpb.Status, 0, len(bs.output))
	for _, e := range bs.output {
		if e.metadata.Size() == 0 {
			// Exclude targets which weren't explicitly set.
			// Explicity-set targets have non-zero values in the target metadata.
			continue
		}
		ts.Targets = append(ts.Targets, scpb.MakeTarget(e.target, e.element, &e.metadata))
		current = append(current, e.current)
	}
	return scpb.CurrentState{TargetState: ts, Current: current}, nil
}

// Export dependency interfaces.
// These are defined in the scbuildstmts package instead of scbuild to avoid
// circular import dependencies.
type (
	// FeatureChecker contains operations for checking if a schema change
	// feature is allowed by the database administrator.
	FeatureChecker = scbuildstmt.SchemaFeatureChecker
)

type elementState struct {
	element  scpb.Element
	current  scpb.Status
	target   scpb.TargetStatus
	metadata scpb.TargetMetadata
}

// builderState is the backing struct for scbuildstmt.BuilderState interface.
type builderState struct {
	// Dependencies
	ctx             context.Context
	clusterSettings *cluster.Settings
	evalCtx         *tree.EvalContext
	semaCtx         *tree.SemaContext
	cr              CatalogReader
	auth            AuthorizationAccessor
	createPartCCL   CreatePartitioningCCLCallback
	hasAdmin        bool

	// output contains the schema change targets that have been planned so far.
	output []elementState

	descCache   map[catid.DescID]*cachedDesc
	tempSchemas map[catid.DescID]catalog.SchemaDescriptor
}

type cachedDesc struct {
	desc         catalog.Descriptor
	prefix       tree.ObjectNamePrefix
	backrefs     catalog.DescriptorIDSet
	ers          *elementResultSet
	privileges   map[privilege.Kind]error
	hasOwnership bool
}

// newBuilderState constructs a builderState.
func newBuilderState(ctx context.Context, d Dependencies, initial scpb.CurrentState) *builderState {
	bs := builderState{
		ctx:             ctx,
		clusterSettings: d.ClusterSettings(),
		evalCtx:         newEvalCtx(ctx, d),
		semaCtx:         newSemaCtx(d),
		cr:              d.CatalogReader(),
		auth:            d.AuthorizationAccessor(),
		createPartCCL:   d.IndexPartitioningCCLCallback(),
		output:          make([]elementState, 0, len(initial.Current)),
		descCache:       make(map[catid.DescID]*cachedDesc),
		tempSchemas:     make(map[catid.DescID]catalog.SchemaDescriptor),
	}
	var err error
	bs.hasAdmin, err = bs.auth.HasAdminRole(ctx)
	if err != nil {
		panic(err)
	}
	for _, t := range initial.TargetState.Targets {
		bs.ensureDescriptor(screl.GetDescID(t.Element()))
	}
	for i, t := range initial.TargetState.Targets {
		bs.Ensure(initial.Current[i], scpb.AsTargetStatus(t.TargetStatus), t.Element(), t.Metadata)
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
func newEventLogState(d Dependencies, initial scpb.CurrentState, n tree.Statement) *eventLogState {
	stmts := initial.Statements
	els := eventLogState{
		statements: append(stmts, scpb.Statement{
			Statement:    n.String(),
			StatementTag: n.StatementTag(),
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
	Dependencies
	scbuildstmt.BuilderState
	scbuildstmt.EventLogState
	scbuildstmt.TreeAnnotator
	scbuildstmt.SchemaFeatureChecker
}

var _ scbuildstmt.BuildCtx = buildCtx{}

// Add implements the scbuildstmt.BuildCtx interface.
func (b buildCtx) Add(element scpb.Element) {
	b.Ensure(scpb.Status_UNKNOWN, scpb.ToPublic, element, b.TargetMetadata())
}

// Drop implements the scbuildstmt.BuildCtx interface.
func (b buildCtx) Drop(element scpb.Element) {
	b.Ensure(scpb.Status_UNKNOWN, scpb.ToAbsent, element, b.TargetMetadata())
}

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
