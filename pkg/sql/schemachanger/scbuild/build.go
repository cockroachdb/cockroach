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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
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

// Build constructs a new state from an incumbent state and a statement.
//
// The function takes an AST for a DDL statement and constructs targets
// which represent schema changes to be performed. The incumbent state
// is the schema changer state for the statement transaction, which reached
// its present value from preceding calls to Build which were followed by the
// execution of their corresponding statement phase stages. In other words,
// the incumbent state encodes the schema change such as it has been defined
// prior to this call, along with any in-transaction side effects.
func Build(
	ctx context.Context, dependencies Dependencies, incumbent scpb.CurrentState, n tree.Statement,
) (_ scpb.CurrentState, err error) {
	defer scerrors.StartEventf(
		ctx,
		"building declarative schema change targets for %s",
		redact.Safe(n.StatementTag()),
	).HandlePanicAndLogError(ctx, &err)
	incumbent = incumbent.DeepCopy()
	bs := newBuilderState(ctx, dependencies, incumbent)
	els := newEventLogState(dependencies, incumbent, n)
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
	currentStatementID := uint32(len(els.statements) - 1)
	els.statements[currentStatementID].RedactedStatement = string(
		dependencies.AstFormatter().FormatAstAsRedactableString(an.GetStatement(), &an.annotation))
	ts := scpb.TargetState{
		Targets:       make([]scpb.Target, 0, len(bs.output)),
		Statements:    els.statements,
		Authorization: els.authorization,
	}
	initial := make([]scpb.Status, 0, len(bs.output))
	current := make([]scpb.Status, 0, len(bs.output))
	version := dependencies.ClusterSettings().Version.ActiveVersion(ctx)
	withLogEvent := make([]scpb.Target, 0, len(bs.output))
	for _, e := range bs.output {
		if e.metadata.Size() == 0 {
			// Exclude targets which weren't explicitly set.
			// Explicitly-set targets have non-zero values in the target metadata.
			continue
		}
		if !version.IsActive(screl.MinVersion(e.element)) {
			// Exclude targets which are not yet usable in the currently active
			// cluster version.
			continue
		}
		maxVersion := screl.MaxVersion(e.element)
		if maxVersion != nil && version.IsActive(*maxVersion) {
			continue
		}
		t := scpb.MakeTarget(e.target, e.element, &e.metadata)
		ts.Targets = append(ts.Targets, t)
		initial = append(initial, e.initial)
		current = append(current, e.current)
		if e.withLogEvent {
			withLogEvent = append(withLogEvent, t)
		}
	}
	// Ensure that no concurrent schema change are on going on any targets.
	descSet := screl.AllTargetDescIDs(ts)
	descSet.ForEach(func(id descpb.ID) {
		bs.ensureDescriptor(id)
		cached := bs.descCache[id]
		// If a descriptor is being created, we don't need to worry about concurrent
		// schema changes.
		if !cached.isBeingCreated() && cached.desc.HasConcurrentSchemaChanges() {
			panic(scerrors.ConcurrentSchemaChangeError(cached.desc))
		}
	})
	// Write to event log and return.
	logEvents(b, ts, withLogEvent)
	return scpb.CurrentState{
		TargetState: ts,
		Initial:     initial,
		Current:     current,
	}, nil
}

// CheckIfSupported returns if a statement is fully supported by the declarative
// schema changer.
func CheckIfSupported(version clusterversion.ClusterVersion, statement tree.Statement) bool {
	return scbuildstmt.CheckIfStmtIsSupported(version, statement, sessiondatapb.UseNewSchemaChangerOn)
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
	// element is the element which identifies this structure.
	element scpb.Element
	// current is the current status of the element;
	// initial is the status of the element at the beginning of the transaction.
	initial, current scpb.Status
	// target indicates the status to be fulfilled by the element.
	target scpb.TargetStatus
	// metadata contains the target metadata to store in the resulting
	// scpb.TargetState produced by the current call to scbuild.Build.
	metadata scpb.TargetMetadata
	// withLogEvent is true iff an event should be written to the event log
	// based on this element.
	withLogEvent bool
}

// builderState is the backing struct for scbuildstmt.BuilderState interface.
type builderState struct {
	// Dependencies
	ctx                      context.Context
	clusterSettings          *cluster.Settings
	evalCtx                  *eval.Context
	semaCtx                  *tree.SemaContext
	cr                       CatalogReader
	tr                       TableReader
	auth                     AuthorizationAccessor
	commentGetter            scdecomp.CommentGetter
	zoneConfigReader         scdecomp.ZoneConfigGetter
	referenceProviderFactory ReferenceProviderFactory
	createPartCCL            CreatePartitioningCCLCallback
	hasAdmin                 bool

	// output contains the schema change targets that have been planned so far.
	output []elementState

	descCache      map[catid.DescID]*cachedDesc
	tempSchemas    map[catid.DescID]catalog.SchemaDescriptor
	newDescriptors catalog.DescriptorIDSet
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

func (c *cachedDesc) isBeingCreated() bool {
	return c.desc == nil
}

// newBuilderState constructs a builderState.
func newBuilderState(
	ctx context.Context, d Dependencies, incumbent scpb.CurrentState,
) *builderState {
	bs := builderState{
		ctx:                      ctx,
		clusterSettings:          d.ClusterSettings(),
		evalCtx:                  newEvalCtx(ctx, d),
		semaCtx:                  newSemaCtx(d),
		cr:                       d.CatalogReader(),
		tr:                       d.TableReader(),
		auth:                     d.AuthorizationAccessor(),
		createPartCCL:            d.IndexPartitioningCCLCallback(),
		output:                   make([]elementState, 0, len(incumbent.Current)),
		descCache:                make(map[catid.DescID]*cachedDesc),
		tempSchemas:              make(map[catid.DescID]catalog.SchemaDescriptor),
		commentGetter:            d.DescriptorCommentGetter(),
		zoneConfigReader:         d.ZoneConfigGetter(),
		referenceProviderFactory: d.ReferenceProviderFactory(),
	}
	var err error
	bs.hasAdmin, err = bs.auth.HasAdminRole(ctx)
	if err != nil {
		panic(err)
	}
	for _, t := range incumbent.TargetState.Targets {
		bs.ensureDescriptor(screl.GetDescID(t.Element()))
	}
	for i, t := range incumbent.TargetState.Targets {
		src := elementState{
			element:  t.Element(),
			initial:  incumbent.Initial[i],
			current:  incumbent.Current[i],
			target:   scpb.AsTargetStatus(t.TargetStatus),
			metadata: t.Metadata,
		}
		if dst := bs.getExistingElementState(src.element); dst != nil {
			*dst = src
		} else {
			bs.addNewElementState(src)
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
}

// newEventLogState constructs an eventLogState.
func newEventLogState(
	d Dependencies, incumbent scpb.CurrentState, n tree.Statement,
) *eventLogState {
	stmts := incumbent.Statements
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
	b.Ensure(element, scpb.ToPublic, b.TargetMetadata())
}

func (b buildCtx) AddTransient(element scpb.Element) {
	b.Ensure(element, scpb.Transient, b.TargetMetadata())
}

// Drop implements the scbuildstmt.BuildCtx interface.
func (b buildCtx) Drop(element scpb.Element) {
	b.Ensure(element, scpb.ToAbsent, b.TargetMetadata())
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
