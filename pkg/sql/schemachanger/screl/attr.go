// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package screl

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// Attr are attributes used to identify, order, and relate nodes,
// targets, and elements to each other using the rel library.
type Attr int

// MustQuery constructs a query using this package's schema. Intending to be
// called during init, this function panics if query construction fails.
func MustQuery(clauses ...rel.Clause) *rel.Query {
	q, err := rel.NewQuery(Schema, clauses...)
	if err != nil {
		panic(err)
	}
	return q
}

var elementProtoElementTypes = func() (selectors []reflect.Type) {
	oneOfProtos := scpb.GetElementOneOfProtos()
	selectors = make([]reflect.Type, 0, len(oneOfProtos))
	for _, proto := range oneOfProtos {
		selectors = append(selectors, reflect.TypeOf(proto))
	}
	return selectors
}()

var _ rel.Attr = Attr(0)

//go:generate stringer -type=Attr -trimprefix=Attr
const (
	_ Attr = iota // reserve 0 for rel.Type
	// DescID is the descriptor ID to which this element belongs.
	DescID
	// IndexID is the index ID to which this element corresponds.
	IndexID
	// ColumnFamilyID is the ID of the column family for this element.
	ColumnFamilyID
	// ColumnID is the ID of the column for this element.
	ColumnID
	// ConstraintID is the ID of a constraint
	ConstraintID
	// Name is the name of the element.
	Name
	// ReferencedDescID is the descriptor ID to which this element refers.
	ReferencedDescID
	// Comment is the comment metadata on descriptors.
	Comment
	// TemporaryIndexID is the index ID of the temporary index being populated
	// during this index's backfill.
	TemporaryIndexID
	// SourceIndexID is the index ID of the source index for a newly created
	// index.
	SourceIndexID
	// RecreateSourceIndexID is the index ID that we are replacing with
	// this new index.
	RecreateSourceIndexID
	// SeqNum is a number given to different elements of the same conceptual
	// object. It is used so we can differentiate those elements in the graph.
	//
	// E.g. Updating a zone config of some database `db` translates to the
	// dropping of the existing zone config (if any) element and adding of another
	// zone config element. Those two DatabaseZoneConfig nodes in the graph will
	// have different SeqNum attribute.
	SeqNum
	// TriggerID is the ID of a trigger.
	TriggerID

	// TargetStatus is the target status of an element.
	TargetStatus
	// CurrentStatus is the current status of an element.
	CurrentStatus
	// Element references an element.
	Element
	// Target is the reference from a node to a target.
	Target

	// ReferencedTypeIDs corresponds to a slice of type descriptor IDs referenced
	// by an element.
	ReferencedTypeIDs
	// ReferencedSequenceIDs corresponds to a slice of sequence descriptor IDs
	// referenced by an element.
	ReferencedSequenceIDs
	// ReferencedFunctionIDs corresponds to a slice of function descriptor IDs
	// referenced by an element.
	ReferencedFunctionIDs
	// ReferencedColumnIDs corresponds to a slice of column IDs referenced by an
	// element.
	ReferencedColumnIDs

	// Expr corresponds to the string representation of a SQL expression for an element.
	Expr
	// TypeName corresponds to the SQL string name of the type.
	TypeName
	// PartitionName corresponds to the name of a partition.
	PartitionName
	// Usage is an attribute for column compute expression to identify why it's
	// being added.
	Usage

	// AttrMax is the largest possible Attr value.
	// Note: add any new enum values before TargetStatus, leave these at the end.
	AttrMax = iota - 1
)

var t = reflect.TypeOf

var elementSchemaOptions = []rel.SchemaOption{
	// We need this `Element` attribute to be of type `protoulti.Message`
	// interface and better have it as the first in the schema option list. This
	// is because the schema needs to know a type of each attribute, and it
	// creates a mapping between attribute and the type. If you're trying to add a
	// same attribute of different type, it panics. In the context of schema
	// changer, a target's element can be of any type listed in
	// `scpb.ElementProto`, which means that we want this `Element` attribute to
	// be mapped to a more general interface type which is implemented by the
	// types listed in `scpb.ElementProto`, so that a Target can be represented
	// correctly in the schema. This is legit because golang reflection considers
	// concrete type underneath an interface value, so we won't have a problem
	// evaluating field values within a concrete Element struct.
	rel.AttrType(Element, t((*protoutil.Message)(nil)).Elem()),
	// Top-level elements.
	rel.EntityMapping(t((*scpb.Database)(nil)),
		rel.EntityAttr(DescID, "DatabaseID"),
	),
	rel.EntityMapping(t((*scpb.Schema)(nil)),
		rel.EntityAttr(DescID, "SchemaID"),
	),
	rel.EntityMapping(t((*scpb.AliasType)(nil)),
		rel.EntityAttr(DescID, "TypeID"),
		rel.EntityAttr(ReferencedTypeIDs, "ClosedTypeIDs"),
	),
	rel.EntityMapping(t((*scpb.EnumType)(nil)),
		rel.EntityAttr(DescID, "TypeID"),
	),
	rel.EntityMapping(t((*scpb.EnumTypeValue)(nil)),
		rel.EntityAttr(DescID, "TypeID"),
		rel.EntityAttr(Name, "LogicalRepresentation"),
	),
	rel.EntityMapping(t((*scpb.CompositeType)(nil)),
		rel.EntityAttr(DescID, "TypeID"),
	),
	rel.EntityMapping(t((*scpb.CompositeTypeAttrName)(nil)),
		rel.EntityAttr(DescID, "CompositeTypeID"),
		rel.EntityAttr(Name, "Name"),
	),
	rel.EntityMapping(t((*scpb.CompositeTypeAttrType)(nil)),
		rel.EntityAttr(DescID, "CompositeTypeID"),
		rel.EntityAttr(ReferencedTypeIDs, "ClosedTypeIDs"),
	),
	rel.EntityMapping(t((*scpb.View)(nil)),
		rel.EntityAttr(DescID, "ViewID"),
	),
	rel.EntityMapping(t((*scpb.Sequence)(nil)),
		rel.EntityAttr(DescID, "SequenceID"),
	),
	rel.EntityMapping(t((*scpb.Table)(nil)),
		rel.EntityAttr(DescID, "TableID"),
	),
	// Relation elements.
	rel.EntityMapping(t((*scpb.ColumnFamily)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnFamilyID, "FamilyID"),
		rel.EntityAttr(Name, "Name"),
	),
	rel.EntityMapping(t((*scpb.Column)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
	),
	rel.EntityMapping(t((*scpb.PrimaryIndex)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(IndexID, "IndexID"),
		rel.EntityAttr(ConstraintID, "ConstraintID"),
		rel.EntityAttr(TemporaryIndexID, "TemporaryIndexID"),
		rel.EntityAttr(SourceIndexID, "SourceIndexID"),
	),
	rel.EntityMapping(t((*scpb.SecondaryIndex)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(IndexID, "IndexID"),
		rel.EntityAttr(ConstraintID, "ConstraintID"),
		rel.EntityAttr(TemporaryIndexID, "TemporaryIndexID"),
		rel.EntityAttr(SourceIndexID, "SourceIndexID"),
		rel.EntityAttr(RecreateSourceIndexID, "RecreateSourceIndexID"),
	),
	rel.EntityMapping(t((*scpb.TemporaryIndex)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(IndexID, "IndexID"),
		rel.EntityAttr(ConstraintID, "ConstraintID"),
		rel.EntityAttr(SourceIndexID, "SourceIndexID"),
	),
	rel.EntityMapping(t((*scpb.UniqueWithoutIndexConstraint)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ConstraintID, "ConstraintID"),
		rel.EntityAttr(IndexID, "IndexIDForValidation"),
		rel.EntityAttr(ReferencedColumnIDs, "ColumnIDs"),
	),
	rel.EntityMapping(t((*scpb.UniqueWithoutIndexConstraintUnvalidated)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ConstraintID, "ConstraintID"),
		rel.EntityAttr(ReferencedColumnIDs, "ColumnIDs"),
	),
	rel.EntityMapping(t((*scpb.CheckConstraint)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ConstraintID, "ConstraintID"),
		rel.EntityAttr(ReferencedSequenceIDs, "UsesSequenceIDs"),
		rel.EntityAttr(ReferencedTypeIDs, "UsesTypeIDs"),
		rel.EntityAttr(IndexID, "IndexIDForValidation"),
		rel.EntityAttr(ReferencedColumnIDs, "ReferencedColumnIDs"),
	),
	rel.EntityMapping(t((*scpb.CheckConstraintUnvalidated)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ConstraintID, "ConstraintID"),
		rel.EntityAttr(ReferencedSequenceIDs, "UsesSequenceIDs"),
		rel.EntityAttr(ReferencedTypeIDs, "UsesTypeIDs"),
		rel.EntityAttr(ReferencedColumnIDs, "ReferencedColumnIDs"),
	),
	rel.EntityMapping(t((*scpb.ForeignKeyConstraint)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ReferencedDescID, "ReferencedTableID"),
		rel.EntityAttr(ConstraintID, "ConstraintID"),
		rel.EntityAttr(IndexID, "IndexIDForValidation"),
		rel.EntityAttr(ReferencedColumnIDs, "ReferencedColumnIDs"),
	),
	rel.EntityMapping(t((*scpb.ForeignKeyConstraintUnvalidated)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ReferencedDescID, "ReferencedTableID"),
		rel.EntityAttr(ConstraintID, "ConstraintID"),
		rel.EntityAttr(ReferencedColumnIDs, "ReferencedColumnIDs"),
	),
	rel.EntityMapping(t((*scpb.RowLevelTTL)(nil)),
		rel.EntityAttr(DescID, "TableID"),
	),
	rel.EntityMapping(t((*scpb.Trigger)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(TriggerID, "TriggerID"),
	),
	// Multi-region elements.
	rel.EntityMapping(t((*scpb.TableLocalityGlobal)(nil)),
		rel.EntityAttr(DescID, "TableID"),
	),
	rel.EntityMapping(t((*scpb.TableLocalityPrimaryRegion)(nil)),
		rel.EntityAttr(DescID, "TableID"),
	),
	rel.EntityMapping(t((*scpb.TableLocalitySecondaryRegion)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ReferencedDescID, "RegionEnumTypeID"),
	),
	rel.EntityMapping(t((*scpb.TableLocalityRegionalByRow)(nil)),
		rel.EntityAttr(DescID, "TableID"),
	),
	// Column elements.
	rel.EntityMapping(t((*scpb.ColumnName)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
		rel.EntityAttr(Name, "Name"),
	),
	rel.EntityMapping(t((*scpb.ColumnType)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnFamilyID, "FamilyID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
		rel.EntityAttr(ReferencedTypeIDs, "ClosedTypeIDs"),
		rel.EntityAttr(TypeName, "TypeName"),
	),
	rel.EntityMapping(t((*scpb.SequenceOption)(nil)),
		rel.EntityAttr(DescID, "SequenceID"),
		rel.EntityAttr(Name, "Key"),
	),
	rel.EntityMapping(t((*scpb.SequenceOwner)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
		rel.EntityAttr(ReferencedDescID, "SequenceID"),
	),
	rel.EntityMapping(t((*scpb.ColumnDefaultExpression)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
		rel.EntityAttr(Expr, "Expr"),
		rel.EntityAttr(ReferencedSequenceIDs, "UsesSequenceIDs"),
		rel.EntityAttr(ReferencedTypeIDs, "UsesTypeIDs"),
		rel.EntityAttr(ReferencedFunctionIDs, "UsesFunctionIDs"),
	),
	rel.EntityMapping(t((*scpb.ColumnOnUpdateExpression)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
		rel.EntityAttr(ReferencedSequenceIDs, "UsesSequenceIDs"),
		rel.EntityAttr(ReferencedTypeIDs, "UsesTypeIDs"),
	),
	rel.EntityMapping(t((*scpb.ColumnComputeExpression)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
		rel.EntityAttr(ReferencedSequenceIDs, "UsesSequenceIDs"),
		rel.EntityAttr(ReferencedTypeIDs, "UsesTypeIDs"),
		rel.EntityAttr(Usage, "Usage"),
	),
	rel.EntityMapping(t((*scpb.ColumnNotNull)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
		rel.EntityAttr(IndexID, "IndexIDForValidation"),
	),
	// Index elements.
	rel.EntityMapping(t((*scpb.IndexName)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(IndexID, "IndexID"),
		rel.EntityAttr(Name, "Name"),
	),
	rel.EntityMapping(t((*scpb.IndexPartitioning)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(IndexID, "IndexID"),
	),
	rel.EntityMapping(t((*scpb.SecondaryIndexPartial)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(IndexID, "IndexID"),
	),
	// Constraint elements.
	rel.EntityMapping(t((*scpb.ConstraintWithoutIndexName)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ConstraintID, "ConstraintID"),
		rel.EntityAttr(Name, "Name"),
	),
	// Trigger elements.
	rel.EntityMapping(t((*scpb.TriggerName)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(TriggerID, "TriggerID"),
	),
	rel.EntityMapping(t((*scpb.TriggerEnabled)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(TriggerID, "TriggerID"),
	),
	rel.EntityMapping(t((*scpb.TriggerTiming)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(TriggerID, "TriggerID"),
	),
	rel.EntityMapping(t((*scpb.TriggerEvents)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(TriggerID, "TriggerID"),
	),
	rel.EntityMapping(t((*scpb.TriggerTransition)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(TriggerID, "TriggerID"),
	),
	rel.EntityMapping(t((*scpb.TriggerWhen)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(TriggerID, "TriggerID"),
	),
	rel.EntityMapping(t((*scpb.TriggerFunctionCall)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(TriggerID, "TriggerID"),
	),
	rel.EntityMapping(t((*scpb.TriggerDeps)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(TriggerID, "TriggerID"),
	),
	// Common elements.
	rel.EntityMapping(t((*scpb.Namespace)(nil)),
		rel.EntityAttr(DescID, "DescriptorID"),
		rel.EntityAttr(ReferencedDescID, "DatabaseID"),
		rel.EntityAttr(Name, "Name"),
	),
	rel.EntityMapping(t((*scpb.Owner)(nil)),
		rel.EntityAttr(DescID, "DescriptorID"),
	),
	rel.EntityMapping(t((*scpb.UserPrivileges)(nil)),
		rel.EntityAttr(DescID, "DescriptorID"),
		rel.EntityAttr(Name, "UserName"),
	),
	// Database elements.
	rel.EntityMapping(t((*scpb.DatabaseRegionConfig)(nil)),
		rel.EntityAttr(DescID, "DatabaseID"),
		rel.EntityAttr(ReferencedDescID, "RegionEnumTypeID"),
	),
	rel.EntityMapping(t((*scpb.DatabaseRoleSetting)(nil)),
		rel.EntityAttr(DescID, "DatabaseID"),
		rel.EntityAttr(Name, "RoleName"),
	),
	// Parent elements.
	rel.EntityMapping(t((*scpb.SchemaParent)(nil)),
		rel.EntityAttr(DescID, "SchemaID"),
		rel.EntityAttr(ReferencedDescID, "ParentDatabaseID"),
	),
	rel.EntityMapping(t((*scpb.SchemaChild)(nil)),
		rel.EntityAttr(DescID, "ChildObjectID"),
		rel.EntityAttr(ReferencedDescID, "SchemaID"),
	),
	// Comment elements.
	rel.EntityMapping(t((*scpb.TableComment)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(Comment, "Comment"),
	),
	rel.EntityMapping(t((*scpb.TypeComment)(nil)),
		rel.EntityAttr(DescID, "TypeID"),
		rel.EntityAttr(Comment, "Comment"),
	),
	rel.EntityMapping(t((*scpb.DatabaseComment)(nil)),
		rel.EntityAttr(DescID, "DatabaseID"),
		rel.EntityAttr(Comment, "Comment"),
	),
	rel.EntityMapping(t((*scpb.SchemaComment)(nil)),
		rel.EntityAttr(DescID, "SchemaID"),
		rel.EntityAttr(Comment, "Comment"),
	),
	rel.EntityMapping(t((*scpb.ColumnComment)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
		rel.EntityAttr(Comment, "Comment"),
	),
	rel.EntityMapping(t((*scpb.IndexComment)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(IndexID, "IndexID"),
		rel.EntityAttr(Comment, "Comment"),
	),
	rel.EntityMapping(t((*scpb.ConstraintComment)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ConstraintID, "ConstraintID"),
		rel.EntityAttr(Comment, "Comment"),
	),
	rel.EntityMapping(t((*scpb.IndexColumn)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(IndexID, "IndexID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
	),
	rel.EntityMapping(t((*scpb.DatabaseZoneConfig)(nil)),
		rel.EntityAttr(DescID, "DatabaseID"),
		rel.EntityAttr(SeqNum, "SeqNum"),
	),
	rel.EntityMapping(t((*scpb.TableZoneConfig)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(SeqNum, "SeqNum"),
	),
	rel.EntityMapping(t((*scpb.IndexZoneConfig)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(IndexID, "IndexID"),
		rel.EntityAttr(SeqNum, "SeqNum"),
	),
	rel.EntityMapping(t((*scpb.PartitionZoneConfig)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(IndexID, "IndexID"),
		rel.EntityAttr(PartitionName, "PartitionName"),
		rel.EntityAttr(SeqNum, "SeqNum"),
	),
	rel.EntityMapping(t((*scpb.DatabaseData)(nil)),
		rel.EntityAttr(DescID, "DatabaseID"),
	),
	rel.EntityMapping(t((*scpb.TableData)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ReferencedDescID, "DatabaseID"),
	),
	rel.EntityMapping(t((*scpb.IndexData)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(IndexID, "IndexID"),
	),
	rel.EntityMapping(t((*scpb.TablePartitioning)(nil)),
		rel.EntityAttr(DescID, "TableID"),
	),
	rel.EntityMapping(t((*scpb.TableSchemaLocked)(nil)),
		rel.EntityAttr(DescID, "TableID"),
	),
	rel.EntityMapping(t((*scpb.LDRJobIDs)(nil)),
		rel.EntityAttr(DescID, "TableID"),
	),
	rel.EntityMapping(t((*scpb.Function)(nil)),
		rel.EntityAttr(DescID, "FunctionID"),
	),
	rel.EntityMapping(t((*scpb.FunctionName)(nil)),
		rel.EntityAttr(DescID, "FunctionID"),
	),
	rel.EntityMapping(t((*scpb.FunctionVolatility)(nil)),
		rel.EntityAttr(DescID, "FunctionID"),
	),
	rel.EntityMapping(t((*scpb.FunctionLeakProof)(nil)),
		rel.EntityAttr(DescID, "FunctionID"),
	),
	rel.EntityMapping(t((*scpb.FunctionNullInputBehavior)(nil)),
		rel.EntityAttr(DescID, "FunctionID"),
	),
	rel.EntityMapping(t((*scpb.FunctionBody)(nil)),
		rel.EntityAttr(DescID, "FunctionID"),
	),
	rel.EntityMapping(t((*scpb.FunctionSecurity)(nil)),
		rel.EntityAttr(DescID, "FunctionID"),
	),
}

// Schema is the schema exported by this package covering the elements of scpb.
var Schema = rel.MustSchema("screl", append(
	elementSchemaOptions,
	rel.EntityMapping(t((*Node)(nil)),
		rel.EntityAttr(CurrentStatus, "CurrentStatus"),
		rel.EntityAttr(Target, "Target"),
	),
	rel.EntityMapping(t((*scpb.Target)(nil)),
		rel.EntityAttr(TargetStatus, "TargetStatus"),
		rel.EntityAttrOneOf(Element, "ElementOneOf", elementProtoElementTypes...),
	),
)...)

var (
	// JoinTarget generates a clause that joins the target
	// to the corresponding element.
	JoinTarget = Schema.Def2(
		"joinTarget", "element", "target", func(
			element, target rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				target.Type((*scpb.Target)(nil)),
				target.AttrEqVar(Element, element),
				element.AttrEqVar(DescID, rel.Blank),
			}
		})

	// JoinTargetNode generates a clause that joins the target and node vars
	// to the corresponding element.
	JoinTargetNode = Schema.Def3(
		"joinTargetNode", "element", "target", "node", func(
			element, target, node rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				JoinTarget(element, target),
				node.Type((*Node)(nil)),
				node.AttrEqVar(Target, target),
			}
		})
)
