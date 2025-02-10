// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuild

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildstmt"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var _ scbuildstmt.EventLogState = (*eventLogState)(nil)

// TargetMetadata implements the scbuildstmt.EventLogState interface.
func (e *eventLogState) TargetMetadata() scpb.TargetMetadata {
	return e.statementMetaData
}

// IncrementSubWorkID implements the scbuildstmt.EventLogState interface.
func (e *eventLogState) IncrementSubWorkID() {
	e.statementMetaData.SubWorkID++
}

// EventLogStateWithNewSourceElementID implements the scbuildstmt.EventLogState
// interface.
func (e *eventLogState) EventLogStateWithNewSourceElementID() scbuildstmt.EventLogState {
	*e.sourceElementID++
	return &eventLogState{
		statements:      e.statements,
		authorization:   e.authorization,
		sourceElementID: e.sourceElementID,
		statementMetaData: scpb.TargetMetadata{
			StatementID:     e.statementMetaData.StatementID,
			SubWorkID:       e.statementMetaData.SubWorkID,
			SourceElementID: *e.sourceElementID,
		},
	}
}

// makeEventLogCallback makes a callback that will generate event log
// entries based on the builder targets (elements and target state).
func makeEventLogCallback(
	b buildCtx, ts scpb.TargetState, loggedTargets []loggedTarget,
) LogSchemaChangerEventsFn {
	if len(loggedTargets) == 0 {
		return stubLogSchemaChangerEventsFn
	}
	var swallowedError error
	defer scerrors.StartEventf(
		b.Context,
		0, /* level */
		"event logging for declarative schema change targets built for %s",
		redact.Safe(ts.Statements[loggedTargets[0].target.Metadata.StatementID].StatementTag),
	).HandlePanicAndLogError(b.Context, &swallowedError)
	detailSlice := make([]eventpb.CommonSQLEventDetails, 0, len(loggedTargets))
	payLoadsSlice := make([]logpb.EventPayload, 0, len(loggedTargets))
	for _, lt := range loggedTargets {
		descID := screl.GetDescID(lt.target.Element())
		stmtID := lt.target.Metadata.StatementID
		details := eventpb.CommonSQLEventDetails{
			Statement:       ts.Statements[stmtID].RedactedStatement,
			Tag:             ts.Statements[stmtID].StatementTag,
			User:            ts.Authorization.UserName,
			DescriptorID:    uint32(descID),
			ApplicationName: ts.Authorization.AppName,
		}
		pb := payloadBuilder{
			Target:         lt.target,
			relatedTargets: make([]scpb.Target, 0, len(ts.Targets)),
			maybePayload:   lt.maybeInfo,
		}
		for _, t := range ts.Targets {
			if t.Metadata.StatementID != stmtID || t.Metadata.SubWorkID != lt.target.Metadata.SubWorkID {
				continue
			}
			pb.relatedTargets = append(pb.relatedTargets, t)
		}
		pl := pb.build(b)
		if pl == nil {
			continue
		}
		detailSlice = append(detailSlice, details)
		payLoadsSlice = append(payLoadsSlice, pl)
	}
	// Return a function for logging schema change events.
	return func(ctx context.Context) error {
		for i := range detailSlice {
			if err := b.EventLogger().LogEvent(ctx, detailSlice[i], payLoadsSlice[i]); err != nil {
				return err
			}
		}
		return nil
	}
}

type payloadBuilder struct {
	relatedTargets []scpb.Target
	scpb.Target
	maybePayload logpb.EventPayload
}

func namespace(b buildCtx, id descpb.ID) (ns *scpb.Namespace) {
	scpb.ForEachNamespace(
		b.QueryByID(id),
		func(_ scpb.Status, target scpb.TargetStatus, e *scpb.Namespace) {
			if ns == nil || target != scpb.ToAbsent {
				ns = e
			}
		},
	)
	if ns == nil {
		panic(errors.AssertionFailedf("missing Namespace element for descriptor #%d", id))
	}
	return ns
}

func fullyQualifiedName(b buildCtx, e scpb.Element) string {
	return fullyQualifiedNameFromID(b, screl.GetDescID(e))
}

func fullyQualifiedNameFromID(b buildCtx, id descpb.ID) string {
	ns := namespace(b, id)
	if ns.DatabaseID == descpb.InvalidID {
		return ns.Name
	}
	nsDatabase := namespace(b, ns.DatabaseID)
	if ns.SchemaID == descpb.InvalidID {
		p := tree.ObjectNamePrefix{
			CatalogName:     tree.Name(nsDatabase.Name),
			SchemaName:      tree.Name(ns.Name),
			ExplicitCatalog: true,
			ExplicitSchema:  true,
		}
		return p.String()
	}
	nsSchema := namespace(b, ns.SchemaID)
	return tree.NewTableNameWithSchema(
		tree.Name(nsDatabase.Name), tree.Name(nsSchema.Name), tree.Name(ns.Name),
	).FQString()
}

// functionName returns the fully qualified function name of the function
// descriptor that `e` belongs to.
// `e` must therefore have a DescID attr and is a function-related element.
func functionName(b buildCtx, e scpb.Element) string {
	// Retrieve the function's name.
	descID := screl.GetDescID(e)
	var fnNameElem *scpb.FunctionName
	scpb.ForEachFunctionName(b.QueryByID(descID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.FunctionName,
	) {
		if e.FunctionID == descID {
			fnNameElem = e
		}
	})
	if fnNameElem == nil {
		panic(errors.AssertionFailedf("cannot find RoutineName element for function with ID %v", descID))
	}
	// Retrieve parent schema and database name.
	var schemaID catid.DescID
	scpb.ForEachSchemaChild(b.QueryByID(descID), func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.SchemaChild,
	) {
		if e.ChildObjectID == descID {
			schemaID = e.SchemaID
		}
	})
	schemaNamespaceElem := namespace(b, schemaID)
	databaseNamespaceElem := namespace(b, schemaNamespaceElem.DatabaseID)
	fnName := tree.MakeQualifiedRoutineName(databaseNamespaceElem.Name, schemaNamespaceElem.Name, fnNameElem.Name)
	return fnName.FQString()
}

// triggerName returns the name of the trigger that element `e` belongs to.
// `e` must therefore have a DescID and TriggerID attr and is a trigger-related
// element.
func triggerName(b buildCtx, e scpb.Element) string {
	descID := screl.GetDescID(e)
	triggerID, err := screl.Schema.GetAttribute(screl.TriggerID, e)
	if err != nil {
		panic(err)
	}
	var triggerNameElem *scpb.TriggerName
	scpb.ForEachTriggerName(
		b.QueryByID(descID),
		func(_ scpb.Status, target scpb.TargetStatus, e *scpb.TriggerName) {
			if e.TriggerID == triggerID && (triggerNameElem == nil || target != scpb.ToAbsent) {
				triggerNameElem = e
			}
		},
	)
	if triggerNameElem == nil {
		panic(errors.AssertionFailedf("missing TriggerName element for table #%d and trigger ID #%s",
			descID, triggerID))
	}
	return triggerNameElem.Name
}

// policyName returns the name of the policy that element `e` belongs to.
// `e` must therefore have a DescID and PolicyID attr and is a policy-related
// element.
func policyName(b buildCtx, e scpb.Element) string {
	descID := screl.GetDescID(e)
	policyID, err := screl.Schema.GetAttribute(screl.PolicyID, e)
	if err != nil {
		panic(err)
	}
	pn := b.QueryByID(descID).FilterPolicyName().FilterElement(func(e *scpb.PolicyName) bool {
		return e.PolicyID == policyID
	}).MustGetOneElement()
	return pn.Name
}

// ownerName finds the owner of the descriptor that element `e` belongs to.
// `e` must therefore have a DescID attr.
func ownerName(b buildCtx, e scpb.Element) string {
	descID := screl.GetDescID(e)
	var ownerElem *scpb.Owner
	scpb.ForEachOwner(
		b.QueryByID(descID),
		func(_ scpb.Status, target scpb.TargetStatus, e *scpb.Owner) {
			if ownerElem == nil || target != scpb.ToAbsent {
				ownerElem = e
			}
		},
	)
	if ownerElem == nil {
		panic(errors.AssertionFailedf("missing Owner element for descriptor #%d", descID))
	}
	return ownerElem.Owner
}

func indexName(b buildCtx, e scpb.Element) string {
	tableID := screl.GetDescID(e)
	indexID, err := screl.Schema.GetAttribute(screl.IndexID, e)
	if err != nil {
		panic(err)
	}
	var name *scpb.IndexName
	scpb.ForEachIndexName(
		b.QueryByID(tableID),
		func(_ scpb.Status, target scpb.TargetStatus, e *scpb.IndexName) {
			if e.IndexID == indexID && (name == nil || target != scpb.ToAbsent) {
				name = e
			}
		},
	)
	if name == nil {
		panic(errors.AssertionFailedf("missing IndexName element for table #%d and index ID #%s",
			tableID, indexID))
	}
	return name.Name
}

func columnName(b buildCtx, e scpb.Element) string {
	tableID := screl.GetDescID(e)
	columnID, err := screl.Schema.GetAttribute(screl.ColumnID, e)
	if err != nil {
		panic(err)
	}
	var name *scpb.ColumnName
	scpb.ForEachColumnName(
		b.QueryByID(tableID),
		func(_ scpb.Status, target scpb.TargetStatus, e *scpb.ColumnName) {
			if e.ColumnID == columnID && (name == nil || target != scpb.ToAbsent) {
				name = e
			}
		},
	)
	if name == nil {
		panic(errors.AssertionFailedf("missing ColumnName element for table #%d and column ID #%s",
			tableID, columnID))
	}
	return name.Name
}

func constraintName(b buildCtx, e scpb.Element) string {
	tableID := screl.GetDescID(e)
	constraintID, err := screl.Schema.GetAttribute(screl.ConstraintID, e)
	if err != nil {
		panic(err)
	}
	var name *scpb.ConstraintWithoutIndexName
	scpb.ForEachConstraintWithoutIndexName(
		b.QueryByID(tableID),
		func(_ scpb.Status, target scpb.TargetStatus, e *scpb.ConstraintWithoutIndexName) {
			if e.ConstraintID == constraintID && (name == nil || target != scpb.ToAbsent) {
				name = e
			}
		},
	)
	if name == nil {
		return indexName(b, e)
	}
	return name.Name
}

func (pb payloadBuilder) cascadeDroppedViews(b buildCtx) (ret []string) {
	descID := screl.GetDescID(pb.Element())
	for _, t := range pb.relatedTargets {
		if t.TargetStatus == scpb.Status_PUBLIC {
			continue
		}
		v, ok := t.Element().(*scpb.View)
		if !ok || v.ViewID == descID {
			continue
		}
		ret = append(ret, fullyQualifiedName(b, v))
	}
	return ret
}

func (pb payloadBuilder) droppedSchemaObjects(b buildCtx) (ret []string) {
	databaseID := screl.GetDescID(pb.Element())
	for _, t := range pb.relatedTargets {
		if t.TargetStatus == scpb.Status_PUBLIC {
			continue
		}
		ns, ok := t.Element().(*scpb.Namespace)
		if !ok || ns.DatabaseID != databaseID || ns.SchemaID == descpb.InvalidID {
			continue
		}
		ret = append(ret, fullyQualifiedName(b, ns))
	}
	return ret
}

func (pb payloadBuilder) build(b buildCtx) logpb.EventPayload {
	const mutationID = 1
	switch e := pb.Element().(type) {
	case *scpb.Database:
		if pb.TargetStatus == scpb.Status_PUBLIC {
			return &eventpb.CreateDatabase{
				DatabaseName: fullyQualifiedName(b, e),
			}
		} else {
			return &eventpb.DropDatabase{
				DatabaseName:         fullyQualifiedName(b, e),
				DroppedSchemaObjects: pb.droppedSchemaObjects(b),
			}
		}
	case *scpb.Schema:
		if pb.TargetStatus == scpb.Status_PUBLIC {
			return &eventpb.CreateSchema{
				SchemaName: fullyQualifiedName(b, e),
				Owner:      ownerName(b, e),
			}
		} else {
			return &eventpb.DropSchema{
				SchemaName: fullyQualifiedName(b, e),
			}
		}
	case *scpb.Table:
		if pb.TargetStatus == scpb.Status_PUBLIC {
			return nil
		} else {
			return &eventpb.DropTable{
				TableName:           fullyQualifiedName(b, e),
				CascadeDroppedViews: pb.cascadeDroppedViews(b),
			}
		}
	case *scpb.View:
		if pb.TargetStatus == scpb.Status_PUBLIC {
			return nil
		} else {
			return &eventpb.DropView{
				ViewName:            fullyQualifiedName(b, e),
				CascadeDroppedViews: pb.cascadeDroppedViews(b),
			}
		}
	case *scpb.Sequence:
		if pb.TargetStatus == scpb.Status_PUBLIC {
			return &eventpb.CreateSequence{
				SequenceName: fullyQualifiedName(b, e),
			}
		} else {
			return &eventpb.DropSequence{
				SequenceName: fullyQualifiedName(b, e),
			}
		}
	case *scpb.EnumType:
		if pb.TargetStatus == scpb.Status_PUBLIC {
			return nil
		} else {
			return &eventpb.DropType{
				TypeName: fullyQualifiedName(b, e),
			}
		}
	case *scpb.CompositeType:
		if pb.TargetStatus == scpb.Status_PUBLIC {
			return nil
		} else {
			return &eventpb.DropType{
				TypeName: fullyQualifiedName(b, e),
			}
		}
	case *scpb.SecondaryIndex:
		if pb.TargetStatus == scpb.Status_PUBLIC {
			return &eventpb.CreateIndex{
				TableName:  fullyQualifiedName(b, e),
				IndexName:  indexName(b, e),
				MutationID: mutationID,
			}
		} else {
			return &eventpb.DropIndex{
				TableName:           fullyQualifiedName(b, e),
				IndexName:           indexName(b, e),
				MutationID:          mutationID,
				CascadeDroppedViews: pb.cascadeDroppedViews(b),
			}
		}
	case *scpb.DatabaseComment:
		return &eventpb.CommentOnDatabase{
			DatabaseName: fullyQualifiedName(b, e),
			Comment:      e.Comment,
			NullComment:  pb.TargetStatus != scpb.Status_PUBLIC,
		}
	case *scpb.SchemaComment:
		return &eventpb.CommentOnSchema{
			SchemaName:  fullyQualifiedName(b, e),
			Comment:     e.Comment,
			NullComment: pb.TargetStatus != scpb.Status_PUBLIC,
		}
	case *scpb.TableComment:
		return &eventpb.CommentOnTable{
			TableName:   fullyQualifiedName(b, e),
			Comment:     e.Comment,
			NullComment: pb.TargetStatus != scpb.Status_PUBLIC,
		}
	case *scpb.TypeComment:
		return &eventpb.CommentOnType{
			TypeName:    fullyQualifiedName(b, e),
			Comment:     e.Comment,
			NullComment: pb.TargetStatus != scpb.Status_PUBLIC,
		}
	case *scpb.ColumnComment:
		return &eventpb.CommentOnColumn{
			TableName:   fullyQualifiedName(b, e),
			ColumnName:  columnName(b, e),
			Comment:     e.Comment,
			NullComment: pb.TargetStatus != scpb.Status_PUBLIC,
		}
	case *scpb.IndexComment:
		return &eventpb.CommentOnIndex{
			TableName:   fullyQualifiedName(b, e),
			IndexName:   indexName(b, e),
			Comment:     e.Comment,
			NullComment: pb.TargetStatus != scpb.Status_PUBLIC,
		}
	case *scpb.ConstraintComment:
		return &eventpb.CommentOnConstraint{
			TableName:      fullyQualifiedName(b, e),
			ConstraintName: constraintName(b, e),
			Comment:        e.Comment,
			NullComment:    pb.TargetStatus != scpb.Status_PUBLIC,
		}
	case *scpb.Function:
		if pb.TargetStatus == scpb.Status_PUBLIC {
			return &eventpb.CreateFunction{
				FunctionName: functionName(b, e),
				IsReplace:    false, // TODO (xiang): refine this once we support replacing a function.
			}
		} else {
			return &eventpb.DropFunction{
				FunctionName: functionName(b, e),
			}
		}
	case *scpb.DatabaseZoneConfig, *scpb.TableZoneConfig, *scpb.IndexZoneConfig,
		*scpb.PartitionZoneConfig, *scpb.NamedRangeZoneConfig:
		if pb.TargetStatus == scpb.Status_PUBLIC {
			var zcDetails eventpb.CommonZoneConfigDetails
			var oldConfig string
			if pb.maybePayload != nil {
				if payload, ok := pb.maybePayload.(*eventpb.SetZoneConfig); ok {
					zcDetails = eventpb.CommonZoneConfigDetails{
						Target:  payload.Target,
						Options: payload.Options,
					}
					oldConfig = payload.ResolvedOldConfig
				}
			}
			return &eventpb.SetZoneConfig{
				CommonZoneConfigDetails: zcDetails,
				ResolvedOldConfig:       oldConfig,
			}
		} else {
			var zcDetails eventpb.CommonZoneConfigDetails
			if pb.maybePayload != nil {
				if payload, ok := pb.maybePayload.(*eventpb.RemoveZoneConfig); ok {
					zcDetails = eventpb.CommonZoneConfigDetails{
						Target:  payload.Target,
						Options: payload.Options,
					}
				}
			}
			return &eventpb.RemoveZoneConfig{
				CommonZoneConfigDetails: zcDetails,
			}
		}
	case *scpb.Trigger:
		if pb.TargetStatus == scpb.Status_PUBLIC {
			return &eventpb.CreateTrigger{
				TableName:   fullyQualifiedNameFromID(b, e.TableID),
				TriggerName: triggerName(b, e),
			}
		} else {
			return &eventpb.DropTrigger{
				TableName:   fullyQualifiedNameFromID(b, e.TableID),
				TriggerName: triggerName(b, e),
			}
		}
	case *scpb.Policy:
		if pb.TargetStatus == scpb.Status_PUBLIC {
			return &eventpb.CreatePolicy{
				TableName:  fullyQualifiedNameFromID(b, e.TableID),
				PolicyName: policyName(b, e),
			}
		} else {
			return &eventpb.DropPolicy{
				TableName:  fullyQualifiedNameFromID(b, e.TableID),
				PolicyName: policyName(b, e),
			}
		}
	}
	if _, _, tbl := scpb.FindTable(b.QueryByID(screl.GetDescID(pb.Element()))); tbl != nil {
		return &eventpb.AlterTable{
			TableName:           fullyQualifiedName(b, pb.Element()),
			MutationID:          mutationID,
			CascadeDroppedViews: pb.cascadeDroppedViews(b),
		}
	}
	return nil
}
