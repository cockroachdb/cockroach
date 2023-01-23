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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildstmt"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdecomp"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/redact"
)

// Build constructs a new state from an initial state and a statement.
//
// The function takes an AST for a DDL statement and constructs targets
// which represent schema changes to be performed.
func Build(
	ctx context.Context, dependencies Dependencies, initial scpb.CurrentState, n tree.Statement,
) (_ scpb.CurrentState, err error) {
	defer scerrors.StartEventf(
		ctx,
		"building declarative schema change targets for %s",
		redact.Safe(n.StatementTag()),
	).HandlePanicAndLogError(ctx, &err)
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
	scbuildstmt.Process(b, an.GetStatement())
	an.ValidateAnnotations()
	els.statements[len(els.statements)-1].RedactedStatement = string(
		dependencies.AstFormatter().FormatAstAsRedactableString(an.GetStatement(), &an.annotation))
	ts := scpb.TargetState{
		Targets:       make([]scpb.Target, 0, len(bs.output)),
		Statements:    els.statements,
		Authorization: els.authorization,
	}
	current := make([]scpb.Status, 0, len(bs.output))
	version := dependencies.ClusterSettings().Version.ActiveVersion(ctx)
	withLogEvent := make([]scpb.Target, 0, len(bs.output))
	for _, e := range bs.output {
		if e.metadata.Size() == 0 {
			// Exclude targets which weren't explicitly set.
			// Explicitly-set targets have non-zero values in the target metadata.
			continue
		}
		// Exclude targets which are not yet usable in the currently active
		// cluster version.
		if !version.IsActive(screl.MinVersion(e.element)) {
			continue
		}
		t := scpb.MakeTarget(e.target, e.element, &e.metadata)
		ts.Targets = append(ts.Targets, t)
		current = append(current, e.current)
		if e.withLogEvent {
			withLogEvent = append(withLogEvent, t)
		}
	}
	// Ensure that no concurrent schema change are on going on any targets.
	descSet := screl.AllTargetDescIDs(ts)
	descSet.ForEach(func(id descpb.ID) {
		bs.ensureDescriptor(id)
		desc := bs.descCache[id].desc
		if desc.HasConcurrentSchemaChanges() {
			panic(scerrors.ConcurrentSchemaChangeError(desc))
		}
	})
	// Write to event log and return.
	logEvents(b, ts, withLogEvent)
	return scpb.CurrentState{TargetState: ts, Current: current}, nil
}

// CheckIfSupported returns if a statement is fully supported by the declarative
// schema changer.
func CheckIfSupported(statement tree.Statement) bool {
	return scbuildstmt.CheckIfStmtIsSupported(statement, sessiondatapb.UseNewSchemaChangerOn)
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
	element      scpb.Element
	current      scpb.Status
	target       scpb.TargetStatus
	metadata     scpb.TargetMetadata
	withLogEvent bool
}

// builderState is the backing struct for scbuildstmt.BuilderState interface.
type builderState struct {
	// Dependencies
	ctx              context.Context
	clusterSettings  *cluster.Settings
	evalCtx          *eval.Context
	semaCtx          *tree.SemaContext
	cr               CatalogReader
	tr               TableReader
	auth             AuthorizationAccessor
	commentGetter    scdecomp.CommentGetter
	zoneConfigReader scdecomp.ZoneConfigGetter
	createPartCCL    CreatePartitioningCCLCallback
	hasAdmin         bool

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

	// elementIndexMap maps from the string serialization of the element
	// to the index of the element in the builder state. Note that this
	// works as a key because the string is derived from the same set of
	// attributes used to define equality. If we were to change that, we'd
	// need to derive a new cache key.
	//
	// This map ends up being very important to make sure that Ensure does
	// not become O(N) where N is the number of elements in the descriptor.
	elementIndexMap map[string]int
}

// newBuilderState constructs a builderState.
func newBuilderState(ctx context.Context, d Dependencies, initial scpb.CurrentState) *builderState {
	bs := builderState{
		ctx:              ctx,
		clusterSettings:  d.ClusterSettings(),
		evalCtx:          newEvalCtx(ctx, d),
		semaCtx:          newSemaCtx(d),
		cr:               d.CatalogReader(),
		tr:               d.TableReader(),
		auth:             d.AuthorizationAccessor(),
		createPartCCL:    d.IndexPartitioningCCLCallback(),
		output:           make([]elementState, 0, len(initial.Current)),
		descCache:        make(map[catid.DescID]*cachedDesc),
		tempSchemas:      make(map[catid.DescID]catalog.SchemaDescriptor),
		commentGetter:    d.DescriptorCommentGetter(),
		zoneConfigReader: d.ZoneConfigGetter(),
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

func (b buildCtx) AddTransient(element scpb.Element) {
	b.Ensure(scpb.Status_UNKNOWN, scpb.Transient, element, b.TargetMetadata())
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
