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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
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
	"github.com/cockroachdb/cockroach/pkg/util/mon"
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
//
// `memAcc` is an injected memory account to track allocation of objects that
// long-live this function (i.e. objects that will not be destroyed after this
// function returns).
func Build(
	ctx context.Context,
	dependencies Dependencies,
	incumbent scpb.CurrentState,
	n tree.Statement,
	memAcc *mon.BoundAccount,
) (_ scpb.CurrentState, err error) {
	defer scerrors.StartEventf(
		ctx,
		"building declarative schema change targets for %s",
		redact.Safe(n.StatementTag()),
	).HandlePanicAndLogError(ctx, &err)

	// localMemAcc tracks memory allocations for local objects.
	var localMemAcc *mon.BoundAccount
	defer func() {
		localMemAcc.Clear(ctx)
	}()
	if monitor := memAcc.Monitor(); monitor != nil {
		acc := monitor.MakeBoundAccount()
		localMemAcc = &acc
	}
	bs := newBuilderState(ctx, dependencies, incumbent, localMemAcc)
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

	// Generate redacted statement.
	{
		an.ValidateAnnotations()
		currentStatementID := uint32(len(els.statements) - 1)
		els.statements[currentStatementID].RedactedStatement = string(
			dependencies.AstFormatter().FormatAstAsRedactableString(an.GetStatement(), &an.annotation))
	}

	// Generate returned state.
	ret, loggedTargets := makeState(dependencies.ClusterSettings().Version.ActiveVersion(ctx), bs)
	ret.Statements = els.statements
	ret.Authorization = els.authorization

	// Update memory accounting.
	if err := memAcc.Grow(ctx, ret.ByteSize()); err != nil {
		panic(err)
	}

	// Write to event log and return.
	logEvents(b, ret.TargetState, loggedTargets)
	return ret, nil
}

// makeState populates the declarative schema changer state returned by Build
// with the targets and the statuses present in the builderState.
func makeState(
	version clusterversion.ClusterVersion, bs *builderState,
) (s scpb.CurrentState, loggedTargets []scpb.Target) {
	s = scpb.CurrentState{
		TargetState: scpb.TargetState{
			Targets: make([]scpb.Target, 0, len(bs.output)),
		},
		Initial: make([]scpb.Status, 0, len(bs.output)),
		Current: make([]scpb.Status, 0, len(bs.output)),
	}
	loggedTargets = make([]scpb.Target, 0, len(bs.output))
	for _, e := range bs.output {
		if e.metadata.Size() == 0 {
			// Exclude targets which weren't explicitly set.
			// Explicitly-set targets have non-zero values in the target metadata.
			continue
		}
		if !version.IsActive(screl.MinElementVersion(e.element)) {
			// Exclude targets which are not yet usable in the currently active
			// cluster version.
			continue
		}
		if maxVersion, exists := screl.MaxElementVersion(e.element); exists && version.IsActive(maxVersion) {
			// Exclude the target which are no longer allowed at the active
			// max version.
			continue
		}
		t := scpb.MakeTarget(e.target, e.element, &e.metadata)
		s.Targets = append(s.Targets, t)
		s.Initial = append(s.Initial, e.initial)
		s.Current = append(s.Current, e.current)
		if e.withLogEvent {
			loggedTargets = append(loggedTargets, t)
		}
	}
	return s, loggedTargets
}

// IsFullySupportedWithFalsePositive returns if a statement is fully supported
// by the declarative schema changer.
// It is possible of false positive but never false negative.
func IsFullySupportedWithFalsePositive(
	statement tree.Statement, version clusterversion.ClusterVersion,
) bool {
	return scbuildstmt.IsFullySupportedWithFalsePositive(statement, version, sessiondatapb.UseNewSchemaChangerOn)
}

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

// byteSize returns an estimated memory usage of `es` in bytes.
func (es *elementState) byteSize() int64 {
	// scpb.Element + 3 * scpb.Status + scpb.TargetMetadata + bool
	return int64(es.element.Size()) + 3*int64(unsafe.Sizeof(scpb.Status(0))) +
		int64(es.metadata.Size()) + int64(unsafe.Sizeof(false))
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

	// localMemAcc is an account to track memory allocation short-lived inside
	// scbuild.Build function.
	// For memory allocation long-lived scbuild.Build function, see the
	// memory account in the build dependency.
	localMemAcc *mon.BoundAccount
}

type cachedDesc struct {
	desc             catalog.Descriptor
	prefix           tree.ObjectNamePrefix
	backrefs         catalog.DescriptorIDSet
	ers              *elementResultSet
	privileges       map[privilege.Kind]error
	hasOwnership     bool
	backrefsResolved bool

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
func newBuilderState(
	ctx context.Context, d Dependencies, incumbent scpb.CurrentState, localMemAcc *mon.BoundAccount,
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
		localMemAcc:              localMemAcc,
	}
	var err error
	bs.hasAdmin, err = bs.auth.HasAdminRole(ctx)
	if err != nil {
		panic(err)
	}
	for _, t := range incumbent.TargetState.Targets {
		bs.ensureDescriptors(t.Element())
	}
	for i, t := range incumbent.TargetState.Targets {
		bs.upsertElementState(elementState{
			element:  t.Element(),
			initial:  incumbent.Initial[i],
			current:  incumbent.Current[i],
			target:   scpb.AsTargetStatus(t.TargetStatus),
			metadata: t.Metadata,
		})
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
