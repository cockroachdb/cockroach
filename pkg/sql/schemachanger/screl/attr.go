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
	// ReferencedDescID is the descriptor ID to which this element refers.
	ReferencedDescID
	//ColumnID is the column ID to which this element corresponds.
	ColumnID
	// Name is the name of the element.
	Name
	// IndexID is the index ID to which this element corresponds.
	IndexID
	// TargetStatus is the target status of an element.
	TargetStatus
	// CurrentStatus is the current status of an element.
	CurrentStatus
	// Element references an element.
	Element
	// Target is the reference from a node to a target.
	Target
	// Username is the username of the element
	Username
	// ConstraintType is the ID of a constraint
	ConstraintType
	// ConstraintOrdinal is the ordinal of the constraints
	ConstraintOrdinal
	// RoleName is the role name of an element
	RoleName
)

var t = reflect.TypeOf

// Schema is the schema exported by this package covering the elements of scpb.
var Schema = rel.MustSchema("screl",
	rel.AttrType(Element, t((*protoutil.Message)(nil)).Elem()),
	rel.EntityMapping(t((*Node)(nil)),
		rel.EntityAttr(CurrentStatus, "CurrentStatus"),
		rel.EntityAttr(Target, "Target"),
	),
	rel.EntityMapping(t((*scpb.Target)(nil)),
		rel.EntityAttr(TargetStatus, "TargetStatus"),
		rel.EntityAttr(Element, elementProtoElementSelectors...),
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
	rel.EntityMapping(t((*scpb.SequenceDependency)(nil)),
		rel.EntityAttr(DescID, "SequenceID"),
		rel.EntityAttr(ReferencedDescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
	),
	rel.EntityMapping(t((*scpb.UniqueConstraint)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(IndexID, "IndexID"),
		rel.EntityAttr(ConstraintType, "ConstraintType"),
		rel.EntityAttr(ConstraintOrdinal, "ConstraintOrdinal"),
	),
	rel.EntityMapping(t((*scpb.CheckConstraint)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(Name, "Name"),
		rel.EntityAttr(ConstraintType, "ConstraintType"),
		rel.EntityAttr(ConstraintOrdinal, "ConstraintOrdinal"),
	),
	rel.EntityMapping(t((*scpb.Sequence)(nil)),
		rel.EntityAttr(DescID, "SequenceID"),
	),
	rel.EntityMapping(t((*scpb.DefaultExpression)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
	),
	rel.EntityMapping(t((*scpb.View)(nil)),
		rel.EntityAttr(DescID, "TableID"),
	),
	rel.EntityMapping(t((*scpb.DefaultExprTypeReference)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
		rel.EntityAttr(ReferencedDescID, "TypeID"),
	),
	rel.EntityMapping(t((*scpb.ComputedExprTypeReference)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
		rel.EntityAttr(ReferencedDescID, "TypeID"),
	),
	rel.EntityMapping(t((*scpb.OnUpdateExprTypeReference)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
		rel.EntityAttr(ReferencedDescID, "TypeID"),
	),
	rel.EntityMapping(t((*scpb.ColumnTypeReference)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
		rel.EntityAttr(ReferencedDescID, "TypeID"),
	),
	rel.EntityMapping(t((*scpb.CheckConstraintTypeReference)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ConstraintOrdinal, "ConstraintOrdinal"),
		rel.EntityAttr(ReferencedDescID, "TypeID"),
	),
	rel.EntityMapping(t((*scpb.ViewDependsOnType)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ReferencedDescID, "TypeID"),
	),
	rel.EntityMapping(t((*scpb.Table)(nil)),
		rel.EntityAttr(DescID, "TableID"),
	),
	rel.EntityMapping(t((*scpb.ForeignKeyBackReference)(nil)),
		rel.EntityAttr(DescID, "OriginID"),
		rel.EntityAttr(ReferencedDescID, "ReferenceID"),
		rel.EntityAttr(Name, "Name"),
	),
	rel.EntityMapping(t((*scpb.ForeignKey)(nil)),
		rel.EntityAttr(DescID, "OriginID"),
		rel.EntityAttr(ReferencedDescID, "ReferenceID"),
		rel.EntityAttr(Name, "Name"),
	),
	rel.EntityMapping(t((*scpb.RelationDependedOnBy)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ReferencedDescID, "DependedOnBy"),
	),
	rel.EntityMapping(t((*scpb.SequenceOwnedBy)(nil)),
		rel.EntityAttr(DescID, "SequenceID"),
		rel.EntityAttr(ReferencedDescID, "OwnerTableID"),
	),
	rel.EntityMapping(t((*scpb.Type)(nil)),
		rel.EntityAttr(DescID, "TypeID"),
	),
	rel.EntityMapping(t((*scpb.Schema)(nil)),
		rel.EntityAttr(DescID, "SchemaID"),
	),
	rel.EntityMapping(t((*scpb.Database)(nil)),
		rel.EntityAttr(DescID, "DatabaseID"),
	),
	rel.EntityMapping(t((*scpb.Partitioning)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(IndexID, "IndexID"),
	),
	rel.EntityMapping(t((*scpb.Namespace)(nil)),
		rel.EntityAttr(DescID, "DescriptorID"),
		rel.EntityAttr(Name, "Name"),
	),
	rel.EntityMapping(t((*scpb.Owner)(nil)),
		rel.EntityAttr(DescID, "DescriptorID"),
	),
	rel.EntityMapping(t((*scpb.UserPrivileges)(nil)),
		rel.EntityAttr(DescID, "DescriptorID"),
		rel.EntityAttr(Username, "Username"),
	),
	rel.EntityMapping(t((*scpb.ColumnName)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(ColumnID, "ColumnID"),
		rel.EntityAttr(Name, "Name"),
	),
	rel.EntityMapping(t((*scpb.Locality)(nil)),
		rel.EntityAttr(DescID, "DescriptorID"),
	),
	rel.EntityMapping(t((*scpb.IndexName)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(IndexID, "IndexID"),
		rel.EntityAttr(Name, "Name"),
	),
	rel.EntityMapping(t((*scpb.ConstraintName)(nil)),
		rel.EntityAttr(DescID, "TableID"),
		rel.EntityAttr(Name, "Name"),
		rel.EntityAttr(ConstraintType, "ConstraintType"),
		rel.EntityAttr(ConstraintOrdinal, "ConstraintOrdinal"),
	),
	rel.EntityMapping(t((*scpb.DatabaseSchemaEntry)(nil)),
		rel.EntityAttr(DescID, "DatabaseID"),
		rel.EntityAttr(ReferencedDescID, "SchemaID"),
	),
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
		rel.EntityAttr(Name, "ConstraintName"),
		rel.EntityAttr(ConstraintType, "ConstraintType"),
	),
	rel.EntityMapping(t((*scpb.DatabaseRoleSetting)(nil)),
		rel.EntityAttr(DescID, "DatabaseID"),
		rel.EntityAttr(RoleName, "RoleName"),
	),
)

// JoinTargetNode generates a clause that joins the target and node vars
// to the corresponding element.
func JoinTargetNode(element, target, node rel.Var) rel.Clause {
	return rel.And(
		target.Type((*scpb.Target)(nil)),
		target.AttrEqVar(Element, element),
		node.Type((*Node)(nil)),
		node.AttrEqVar(Target, target),
	)
}
