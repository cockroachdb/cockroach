// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

var elementProtoElementSelectors = func() (selectors []string) {
	elementProtoType := reflect.TypeOf((*scpb.ElementProto)(nil)).Elem()
	selectors = make([]string, elementProtoType.NumField())
	for i := 0; i < elementProtoType.NumField(); i++ {
		selectors[i] = elementProtoType.Field(i).Name
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

	// TargetStatus is the target status of an element.
	TargetStatus
	// CurrentStatus is the current status of an element.
	CurrentStatus
	// Element references an element.
	Element
	// Target is the reference from a node to a target.
	Target
	// AttrMax is the largest possible Attr value.
	// Note: add any new enum values before TargetStatus, leave these at the end.
	AttrMax = iota - 1
)

var t = reflect.TypeOf

var elementSchemaOptions = []rel.SchemaOption{
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
	),
	rel.EntityMapping(t((*scpb.EnumType)(nil)),
		rel.EntityAttr(DescID, "TypeID"),
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
	),
	rel.EntityMapping(t((*scpb.SecondaryIndex)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(IndexID, "IndexID"),
	),
	rel.EntityMapping(t((*scpb.TemporaryIndex)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(IndexID, "IndexID"),
	),
	rel.EntityMapping(t((*scpb.UniqueWithoutIndexConstraint)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ConstraintID, "ConstraintID"),
	),
	rel.EntityMapping(t((*scpb.CheckConstraint)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ConstraintID, "ConstraintID"),
	),
	rel.EntityMapping(t((*scpb.ForeignKeyConstraint)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ReferencedDescID, "ReferencedTableID"),
		rel.EntityAttr(ConstraintID, "ConstraintID"),
	),
	rel.EntityMapping(t((*scpb.RowLevelTTL)(nil)),
		rel.EntityAttr(DescID, "TableID"),
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
	),
	rel.EntityMapping(t((*scpb.SequenceOwner)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
		rel.EntityAttr(ReferencedDescID, "SequenceID"),
	),
	rel.EntityMapping(t((*scpb.ColumnDefaultExpression)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
	),
	rel.EntityMapping(t((*scpb.ColumnOnUpdateExpression)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
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
	rel.EntityMapping(t((*scpb.ConstraintName)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ConstraintID, "ConstraintID"),
		rel.EntityAttr(Name, "Name"),
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
	rel.EntityMapping(t((*scpb.ObjectParent)(nil)),
		rel.EntityAttr(DescID, "ObjectID"),
		rel.EntityAttr(ReferencedDescID, "ParentSchemaID"),
	),
	// Comment elements.
	rel.EntityMapping(t((*scpb.TableComment)(nil)),
		rel.EntityAttr(DescID, "TableID"),
	),
	rel.EntityMapping(t((*scpb.DatabaseComment)(nil)),
		rel.EntityAttr(DescID, "DatabaseID"),
	),
	rel.EntityMapping(t((*scpb.SchemaComment)(nil)),
		rel.EntityAttr(DescID, "SchemaID"),
	),
	rel.EntityMapping(t((*scpb.ColumnComment)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
	),
	rel.EntityMapping(t((*scpb.IndexComment)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(IndexID, "IndexID"),
	),
	rel.EntityMapping(t((*scpb.ConstraintComment)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ConstraintID, "ConstraintID"),
	),
}

// Schema is the schema exported by this package covering the elements of scpb.
var Schema = rel.MustSchema("screl", append(
	elementSchemaOptions,
	rel.AttrType(Element, t((*protoutil.Message)(nil)).Elem()),
	rel.EntityMapping(t((*Node)(nil)),
		rel.EntityAttr(CurrentStatus, "CurrentStatus"),
		rel.EntityAttr(Target, "Target"),
	),
	rel.EntityMapping(t((*scpb.Target)(nil)),
		rel.EntityAttr(TargetStatus, "TargetStatus"),
		rel.EntityAttr(Element, elementProtoElementSelectors...),
	),
)...)

// JoinTarget generates a clause that joins the target
// to the corresponding element.
func JoinTarget(element, target rel.Var) rel.Clause {
	return rel.And(
		target.Type((*scpb.Target)(nil)),
		target.AttrEqVar(Element, element),
	)
}

// JoinTargetNode generates a clause that joins the target and node vars
// to the corresponding element.
func JoinTargetNode(element, target, node rel.Var) rel.Clause {
	return rel.And(
		JoinTarget(element, target),
		node.Type((*Node)(nil)),
		node.AttrEqVar(Target, target),
	)
}
