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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// columnDescToElement converts an individual column descriptor
// into the equivalent column element.
func (b *buildContext) columnDescToElement(
	table catalog.TableDescriptor,
	column descpb.ColumnDescriptor,
	familyName *string,
	familyID *descpb.FamilyID,
) *scpb.Column {
	nilToEmptyString := func(val *string) string {
		if val == nil {
			return ""
		}
		return *val
	}
	// Find the column family entries, when they
	// aren't populated
	if familyID == nil &&
		!column.Virtual {
		columnFamilies := table.GetFamilies()
		familyIdx := -1
		for idx, family := range columnFamilies {
			for _, col := range family.ColumnIDs {
				if col == column.ID {
					familyIdx = idx
					break
				}
			}
			if familyIdx != -1 {
				break
			}
		}
		familyID = &columnFamilies[familyIdx].ID
		familyName = &columnFamilies[familyIdx].Name
	} else if column.Virtual {
		localID := descpb.FamilyID(0)
		localName := ""
		familyID = &localID
		familyName = &localName
	}

	return &scpb.Column{
		TableID:                           table.GetID(),
		ColumnID:                          column.ID,
		Type:                              column.Type,
		FamilyName:                        *familyName,
		FamilyID:                          *familyID,
		Nullable:                          column.Nullable,
		DefaultExpr:                       nilToEmptyString(column.DefaultExpr),
		OnUpdateExpr:                      nilToEmptyString(column.OnUpdateExpr),
		Hidden:                            column.Hidden,
		Inaccessible:                      column.Inaccessible,
		GeneratedAsIdentitySequenceOption: nilToEmptyString(column.GeneratedAsIdentitySequenceOption),
		GeneratedAsIdentityType:           column.GeneratedAsIdentityType,
		UsesSequenceIds:                   column.UsesSequenceIds,
		ComputerExpr:                      nilToEmptyString(column.ComputeExpr),
		PgAttributeNum:                    column.GetPGAttributeNum(),
		SystemColumnKind:                  column.SystemColumnKind,
		Virtual:                           column.Virtual,
	}
}

type exprType int

const (
	exprTypeDefault  exprType = 0
	exprTypeOnUpdate exprType = 1
	exprTypeComputed exprType = 2
)

// decomposeExprToTypeRef converts and inserts type references from
// an expression.
func (b *buildContext) decomposeExprToElements(
	exprString string,
	exprType exprType,
	table catalog.TableDescriptor,
	column catalog.Column,
	dir scpb.Target_Direction,
) {
	// Empty expressions don't have any type references.
	if exprString == "" {
		return
	}
	// Get all available type references and create nodes
	// for dropping these type references.
	visitor := &tree.TypeCollectorVisitor{
		OIDs: make(map[oid.Oid]struct{}),
	}
	// Deal with any default expressions with types.
	expr, err := parser.ParseExpr(exprString)
	if err != nil {
		panic(err)
	}
	tree.WalkExpr(visitor, expr)
	for oid := range visitor.OIDs {
		typeID, err := typedesc.UserDefinedTypeOIDToID(oid)
		if err != nil {
			panic(err)
		}
		var typeRef scpb.Element

		switch exprType {
		case exprTypeDefault:
			typeRef = &scpb.DefaultExprTypeReference{
				TableID:  table.GetID(),
				ColumnID: column.GetID(),
				TypeID:   typeID,
			}
		case exprTypeComputed:
			typeRef = &scpb.ComputedExprTypeReference{
				TableID:  table.GetID(),
				ColumnID: column.GetID(),
				TypeID:   typeID,
			}
		case exprTypeOnUpdate:
			typeRef = &scpb.OnUpdateExprTypeReference{
				TableID:  table.GetID(),
				ColumnID: column.GetID(),
				TypeID:   typeID,
			}
		}
		b.addIfDuplicateDoesNotExistForDir(dir, typeRef)
	}
}

// decomposeDefaultExprToElements converts and inserts default
// expression elements into the graph.
func (b *buildContext) decomposeDefaultExprToElements(
	table catalog.TableDescriptor, column catalog.Column, dir scpb.Target_Direction,
) {
	if !column.HasDefault() || column.ColumnDesc().HasNullDefault() {
		return
	}
	defaultExpr := column.GetDefaultExpr()
	sequenceIDs := make([]descpb.ID, 0, column.NumUsesSequences())
	for idx := 0; idx < column.NumUsesSequences(); idx++ {
		sequenceIDs = append(sequenceIDs, column.GetUsesSequenceID(idx))
	}
	expressionElem := scpb.DefaultExpression{
		TableID:         table.GetID(),
		ColumnID:        column.GetID(),
		DefaultExpr:     defaultExpr,
		UsesSequenceIDs: sequenceIDs,
	}
	b.addIfDuplicateDoesNotExistForDir(dir, &expressionElem)
	// Decompose any elements required for expressions.
	b.decomposeExprToElements(defaultExpr,
		exprTypeDefault,
		table,
		column,
		dir)
}

// decomposeDescToElements converts generic parts
// of a descriptor into an elements in the graph.
func (b *buildContext) decomposeDescToElements(
	_ context.Context, tbl catalog.Descriptor, dir scpb.Target_Direction,
) {
	// Decompose all security settings
	privileges := tbl.GetPrivileges()
	ownerElem := scpb.Owner{
		DescriptorID: tbl.GetID(),
		Owner:        privileges.Owner().Normalized(),
	}
	b.addIfDuplicateDoesNotExistForDir(dir, &ownerElem)

	for _, user := range privileges.Users {
		userElem := scpb.UserPrivileges{
			DescriptorID: tbl.GetID(),
			Username:     user.User().Normalized(),
			Privileges:   user.Privileges,
		}
		b.addIfDuplicateDoesNotExistForDir(dir, &userElem)
	}
}

func (b *buildContext) decomposeColumnIntoElements(
	_ context.Context, tbl catalog.TableDescriptor, column catalog.Column, dir scpb.Target_Direction,
) {
	if column.IsHidden() {
		return
	}
	b.addIfDuplicateDoesNotExistForDir(dir,
		&scpb.ColumnName{
			TableID:  tbl.GetID(),
			ColumnID: column.GetID(),
			Name:     column.GetName(),
		},
	)
	b.addIfDuplicateDoesNotExistForDir(dir, b.columnDescToElement(tbl, column.ColumnDescDeepCopy(), nil, nil))
	// Convert any default expressions.
	b.decomposeDefaultExprToElements(tbl, column, dir)
	// Deal with computed and on update expressions
	b.decomposeExprToElements(column.GetComputeExpr(),
		exprTypeComputed,
		tbl,
		column,
		dir)
	b.decomposeExprToElements(column.GetOnUpdateExpr(),
		exprTypeOnUpdate,
		tbl,
		column,
		dir)
	// If there was a sequence owner dependency clean that up next.
	if column.NumOwnsSequences() > 0 {
		// Drop the depends on within the sequence side.
		for seqOrd := 0; seqOrd < column.NumOwnsSequences(); seqOrd++ {
			seqID := column.GetOwnsSequenceID(seqOrd)
			// Remove dependencies to this sequences.
			sequenceOwnedBy := &scpb.SequenceOwnedBy{SequenceID: seqID,
				OwnerTableID: tbl.GetID()}
			b.addIfDuplicateDoesNotExistForDir(dir, sequenceOwnedBy)
		}
	}
	// If there was a sequence dependency track those.
	if column.NumUsesSequences() > 0 {
		// Drop the depends on within the sequence side.
		for seqOrd := 0; seqOrd < column.NumUsesSequences(); seqOrd++ {
			seqID := column.GetUsesSequenceID(seqOrd)
			// Remove dependencies to this sequences.
			relationDep := &scpb.RelationDependedOnBy{TableID: seqID,
				DependedOnBy: tbl.GetID(),
				ColumnID:     column.GetID()}
			b.addIfDuplicateDoesNotExistForDir(dir, relationDep)
		}
	}
}

// decomposeViewDescToElements converts view specific
// parts of a table descriptor into elements for the graph.
func (b *buildContext) decomposeViewDescToElements(
	_ context.Context, view catalog.TableDescriptor, dir scpb.Target_Direction,
) {
	for _, typeRef := range view.GetDependsOnTypes() {
		typeDep := &scpb.ViewDependsOnType{
			TableID: view.GetID(),
			TypeID:  typeRef,
		}
		b.addIfDuplicateDoesNotExistForDir(dir, typeDep)

	}
}

// decomposeSequenceDescToElements converts sequence specific
// parts of a table descriptor into elements for the graph.
func (b *buildContext) decomposeSequenceDescToElements(
	_ context.Context, seq catalog.TableDescriptor, dir scpb.Target_Direction,
) {
	if seq.GetSequenceOpts().SequenceOwner.OwnerTableID != descpb.InvalidID {
		sequenceOwnedBy := &scpb.SequenceOwnedBy{
			SequenceID:   seq.GetID(),
			OwnerTableID: seq.GetSequenceOpts().SequenceOwner.OwnerTableID}
		b.addIfDuplicateDoesNotExistForDir(dir, sequenceOwnedBy)
	}
}

// decomposeTableDescToElements converts a table/view/sequence into
// parts of a table descriptor into elements for the graph.
func (b *buildContext) decomposeTableDescToElements(
	ctx context.Context, tbl catalog.TableDescriptor, dir scpb.Target_Direction,
) {
	duplicateObjectsAllowed := dir == scpb.Target_DROP
	var objectElem scpb.Element
	switch {
	case tbl.IsTable():
		objectElem = &scpb.Table{
			TableID: tbl.GetID(),
		}
	case tbl.IsView():
		objectElem = &scpb.View{
			TableID: tbl.GetID(),
		}
	case tbl.IsSequence():
		objectElem = &scpb.Sequence{
			SequenceID: tbl.GetID(),
		}
	default:
		panic(errors.Newf("unable to convert tbl descriptor that is not a tbl/view/sequence"))
	}
	// If the node is already added then skip
	// adding it again.
	if exists, _ := b.checkIfNodeExists(dir, objectElem); exists && duplicateObjectsAllowed {
		return
	}
	b.addNode(dir, objectElem)
	nameElem := scpb.Namespace{
		Name:         tbl.GetName(),
		DatabaseID:   tbl.GetParentSchemaID(),
		SchemaID:     tbl.GetParentID(),
		DescriptorID: tbl.GetID(),
	}
	b.addIfDuplicateDoesNotExistForDir(dir, &nameElem)
	// Convert common fields for descriptors into elements.
	b.decomposeDescToElements(ctx, tbl, dir)
	switch {
	case tbl.IsTable():
		// Decompose columns into elements.
		for _, column := range tbl.AllColumns() {
			b.decomposeColumnIntoElements(ctx, tbl, column, dir)
		}
		// Decompose indexes into elements.
		for _, index := range tbl.AllIndexes() {
			if index.Primary() {
				primaryIndex, indexName := primaryIndexElemFromDescriptor(index.IndexDesc(), tbl)
				b.addIfDuplicateDoesNotExistForDir(dir, primaryIndex)
				b.addIfDuplicateDoesNotExistForDir(dir, indexName)

			} else {
				secondaryIndex, indexName := secondaryIndexElemFromDescriptor(index.IndexDesc(), tbl)
				b.addIfDuplicateDoesNotExistForDir(dir, secondaryIndex)
				b.addIfDuplicateDoesNotExistForDir(dir, indexName)
			}
		}
	case tbl.IsSequence():
		b.decomposeSequenceDescToElements(ctx, tbl, dir)
	case tbl.IsView():
		b.decomposeViewDescToElements(ctx, tbl, dir)

	}
	// Go through outbound/inbound foreign keys
	err := tbl.ForeachOutboundFK(func(fk *descpb.ForeignKeyConstraint) error {
		outBoundFk := scpb.ForeignKey{
			OriginID:         fk.OriginTableID,
			OriginColumns:    fk.OriginColumnIDs,
			ReferenceColumns: fk.ReferencedColumnIDs,
			ReferenceID:      fk.ReferencedTableID,
			OnUpdate:         fk.OnUpdate,
			OnDelete:         fk.OnDelete,
			Name:             fk.Name,
		}
		b.addIfDuplicateDoesNotExistForDir(dir, &outBoundFk)
		return nil
	})
	if err != nil {
		panic(err)
	}
	err = tbl.ForeachInboundFK(func(fk *descpb.ForeignKeyConstraint) error {
		inBoundFk := scpb.ForeignKeyBackReference{
			OriginID:         fk.ReferencedTableID,
			OriginColumns:    fk.ReferencedColumnIDs,
			ReferenceID:      fk.OriginTableID,
			ReferenceColumns: fk.OriginColumnIDs,
			OnUpdate:         fk.OnUpdate,
			OnDelete:         fk.OnDelete,
			Name:             fk.Name,
		}
		b.addIfDuplicateDoesNotExistForDir(dir, &inBoundFk)
		return nil
	})
	if err != nil {
		panic(err)
	}
	// Add any constraints without indexes first.
	for idx, constraint := range tbl.AllActiveAndInactiveUniqueWithoutIndexConstraints() {
		constraintID := &scpb.ConstraintID{
			Type:    scpb.ConstraintID_UniqueWithoutIndex,
			Ordinal: uint32(idx),
		}
		constraintName := &scpb.ConstraintName{
			TableID:      tbl.GetID(),
			ConstraintID: constraintID,
			Name:         constraint.Name,
		}
		uniqueWithoutConstraint := &scpb.UniqueConstraint{
			ConstraintID: constraintID,
			TableID:      tbl.GetID(),
			IndexID:      0, // Invalid ID
			ColumnIDs:    constraint.ColumnIDs,
		}
		b.addIfDuplicateDoesNotExistForDir(dir, uniqueWithoutConstraint)
		b.addIfDuplicateDoesNotExistForDir(dir, constraintName)
	}
	// Add any check constraints next.
	for idx, constraint := range tbl.AllActiveAndInactiveChecks() {
		constraintID := &scpb.ConstraintID{
			Type:    scpb.ConstraintID_CheckConstraint,
			Ordinal: uint32(idx),
		}
		constraintName := &scpb.ConstraintName{
			TableID:      tbl.GetID(),
			ConstraintID: constraintID,
			Name:         constraint.Name,
		}
		checkConstraint := &scpb.CheckConstraint{
			TableID:   tbl.GetID(),
			Name:      constraint.Name,
			Validated: constraint.Validity == descpb.ConstraintValidity_Validated,
			ColumnIDs: constraint.ColumnIDs,
		}
		b.addIfDuplicateDoesNotExistForDir(dir, checkConstraint)
		b.addIfDuplicateDoesNotExistForDir(dir, constraintName)
	}
	// Add locality information.
	b.addIfDuplicateDoesNotExistForDir(dir, &scpb.Locality{
		DescriptorID: tbl.GetID(),
		Locality:     tbl.GetLocalityConfig(),
	})
	// Inject any dependencies into the plan.
	for _, dep := range tbl.GetDependsOn() {
		dependsOn := &scpb.RelationDependedOnBy{
			DependedOnBy: tbl.GetID(),
			TableID:      dep,
		}
		b.addIfDuplicateDoesNotExistForDir(scpb.Target_DROP, dependsOn)
	}
	for _, depBy := range tbl.GetDependedOnBy() {
		dependedOnBy := &scpb.RelationDependedOnBy{
			DependedOnBy: depBy.ID,
			TableID:      tbl.GetID(),
		}
		b.addIfDuplicateDoesNotExistForDir(scpb.Target_DROP, dependedOnBy)
	}
	//TODO (fqazi) Computed Expressions / Update expressions can be moved out
	// of column (similar to the UML).
	//TODO (fqazi) Type references will later on need better handling.
}
