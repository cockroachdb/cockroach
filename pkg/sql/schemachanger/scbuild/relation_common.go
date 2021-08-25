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
	if familyID == nil {
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
	}
}

// decomposeTableTypeBackRefDeps converts and inserts
// type back references into the graph.
func (b *buildContext) decomposeTableTypeBackRefDeps(
	ctx context.Context, tableDesc catalog.TableDescriptor, dir scpb.Target_Direction,
) {
	_, dbDesc, err := b.Descs.GetImmutableDatabaseByID(ctx, b.EvalCtx.Txn,
		tableDesc.GetParentID(), tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		panic(err)
	}
	typeIDs, _, err := tableDesc.GetAllReferencedTypeIDs(dbDesc, func(id descpb.ID) (catalog.TypeDescriptor, error) {
		mutDesc, err := b.Descs.GetMutableTypeByID(ctx, b.EvalCtx.Txn, id, tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			return nil, err
		}
		return mutDesc, nil
	})
	if err != nil {
		panic(err)
	}
	// Drop all references to this table/view/sequence
	for _, typeID := range typeIDs {
		typeRef := &scpb.TypeReference{
			TypeID: typeID,
			DescID: tableDesc.GetID(),
		}
		b.addIfDuplicateDoesNotExistForDir(dir, typeRef)
		if exists, _ := b.checkIfNodeExists(scpb.Target_DROP, typeRef); !exists {
			b.addNode(scpb.Target_DROP,
				typeRef)
		}
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
	// Get all available type references and create nodes
	// for dropping these type references.
	visitor := &tree.TypeCollectorVisitor{
		OIDs: make(map[oid.Oid]struct{}),
	}
	// Deal with any default expressions with types.
	expr, err := parser.ParseExpr(defaultExpr)
	if err != nil {
		panic(err)
	}
	tree.WalkExpr(visitor, expr)
	for oid := range visitor.OIDs {
		typeID, err := typedesc.UserDefinedTypeOIDToID(oid)
		if err != nil {
			panic(err)
		}
		typeRef := &scpb.TypeReference{
			TypeID: typeID,
			DescID: table.GetID(),
		}
		b.addIfDuplicateDoesNotExistForDir(dir, typeRef)
	}
}

// decomposeDescToElements converts generic parts
// of a descriptor into an elements in the graph.
func (b *buildContext) decomposeDescToElements(
	_ context.Context, table catalog.Descriptor, dir scpb.Target_Direction,
) {
	// Decompose all security settings
	privileges := table.GetPrivileges()
	ownerElem := scpb.Owner{
		DescriptorID: table.GetID(),
		Owner:        privileges.Owner().Normalized(),
	}
	b.addIfDuplicateDoesNotExistForDir(dir, &ownerElem)

	for _, user := range privileges.Users {
		userElem := scpb.UserPrivileges{
			DescriptorID: table.GetID(),
			Username:     user.User().Normalized(),
			Privileges:   user.Privileges,
		}
		b.addIfDuplicateDoesNotExistForDir(dir, &userElem)
	}
}

func (b *buildContext) decomposeColumnIntoElements(
	_ context.Context,
	table catalog.TableDescriptor,
	column catalog.Column,
	dir scpb.Target_Direction,
) {
	if column.IsHidden() {
		return
	}
	b.addIfDuplicateDoesNotExistForDir(dir,
		&scpb.ColumnName{
			TableID:  table.GetID(),
			ColumnID: column.GetID(),
			Name:     column.GetName(),
		},
	)
	b.addIfDuplicateDoesNotExistForDir(dir, b.columnDescToElement(table, *column.ColumnDesc(), nil, nil))
	// Convert any default expressions.
	b.decomposeDefaultExprToElements(table, column, dir)
	// If there was a sequence owner dependency clean that up next.
	if column.NumOwnsSequences() > 0 {
		// Drop the depends on within the sequence side.
		for seqOrd := 0; seqOrd < column.NumOwnsSequences(); seqOrd++ {
			seqID := column.GetOwnsSequenceID(seqOrd)
			// Remove dependencies to this sequences.
			sequenceOwnedBy := &scpb.SequenceOwnedBy{SequenceID: seqID,
				OwnerTableID: table.GetID()}
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
				DependedOnBy: table.GetID(),
				XColumnID:    &scpb.RelationDependedOnBy_ColumnID{ColumnID: column.GetID()}}
			b.addIfDuplicateDoesNotExistForDir(dir, relationDep)
		}
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
	ctx context.Context, table catalog.TableDescriptor, dir scpb.Target_Direction,
) {
	duplicateObjectsAllowed := dir == scpb.Target_DROP ||
		dir == scpb.Target_STATIC
	var objectElem scpb.Element
	switch {
	case table.IsTable():
		objectElem = &scpb.Table{
			TableID: table.GetID(),
		}
	case table.IsView():
		objectElem = &scpb.View{
			TableID: table.GetID(),
		}
	case table.IsSequence():
		objectElem = &scpb.Sequence{
			SequenceID: table.GetID(),
		}
	default:
		panic(errors.Newf("unable to convert table descriptor that is not a table/view/sequence"))
	}
	// If the node is already added then skip
	// adding it again.
	if exists, _ := b.checkIfNodeExists(dir, objectElem); exists && duplicateObjectsAllowed {
		return
	}
	b.addNode(dir, objectElem)
	nameElem := scpb.Namespace{
		Name:         table.GetName(),
		DatabaseID:   table.GetParentSchemaID(),
		SchemaID:     table.GetParentID(),
		DescriptorID: table.GetID(),
	}
	b.addIfDuplicateDoesNotExistForDir(dir, &nameElem)
	// Convert common fields for descriptors into elements.
	b.decomposeDescToElements(ctx, table, dir)
	switch {
	case table.IsTable():
		// Decompose columns into elements.
		for _, column := range table.AllColumns() {
			b.decomposeColumnIntoElements(ctx, table, column, dir)
		}
		// Decompose indexes into elements.
		for _, index := range table.AllIndexes() {
			if index.Primary() {
				primaryIndex := scpb.PrimaryIndex{
					TableID:             table.GetID(),
					Index:               *index.IndexDesc(),
					OtherPrimaryIndexID: 0,
				}
				b.addIfDuplicateDoesNotExistForDir(dir, &primaryIndex)
			} else {
				secondaryIndex := scpb.SecondaryIndex{
					TableID:      table.GetID(),
					Index:        *index.IndexDesc(),
					PrimaryIndex: table.GetPrimaryIndexID(),
				}
				b.addIfDuplicateDoesNotExistForDir(dir, &secondaryIndex)
			}
		}
	case table.IsSequence():
		b.decomposeSequenceDescToElements(ctx, table, dir)

	}
	// Go through outbound/inbound foreign keys
	err := table.ForeachOutboundFK(func(fk *descpb.ForeignKeyConstraint) error {
		outBoundFk := scpb.OutboundForeignKey{
			OriginID:         fk.OriginTableID,
			OriginColumns:    fk.OriginColumnIDs,
			ReferenceColumns: fk.ReferencedColumnIDs,
			ReferenceID:      fk.ReferencedTableID,
			Name:             fk.Name,
		}
		b.addIfDuplicateDoesNotExistForDir(dir, &outBoundFk)
		return nil
	})
	if err != nil {
		panic(err)
	}
	err = table.ForeachInboundFK(func(fk *descpb.ForeignKeyConstraint) error {
		inBoundFk := scpb.InboundForeignKey{
			OriginID:         fk.ReferencedTableID,
			OriginColumns:    fk.ReferencedColumnIDs,
			ReferenceID:      fk.OriginTableID,
			ReferenceColumns: fk.OriginColumnIDs,
			Name:             fk.Name,
		}
		b.addIfDuplicateDoesNotExistForDir(dir, &inBoundFk)
		return nil
	})
	if err != nil {
		panic(err)
	}
	// Add any constraints next.
	constraintMap, err := table.GetConstraintInfo()
	if err != nil {
		panic(err)
	}
	for _, constraintInfo := range constraintMap {
		if constraintInfo.Kind == descpb.ConstraintTypeCheck {
			constraintElem := scpb.CheckConstraint{
				TableID:   table.GetID(),
				Name:      constraintInfo.Details,
				Expr:      constraintInfo.CheckConstraint.Expr,
				Validated: constraintInfo.CheckConstraint.Validity == descpb.ConstraintValidity_Validated,
				ColumnIDs: constraintInfo.CheckConstraint.ColumnIDs,
			}
			b.addIfDuplicateDoesNotExistForDir(dir, &constraintElem)
		} else if constraintInfo.Kind == descpb.ConstraintTypeUnique {
			columnIDs := make([]descpb.ColumnID, 0, len(constraintInfo.Columns))
			for _, colName := range constraintInfo.Columns {
				colID, err := table.FindColumnWithName(tree.Name(colName))
				if err != nil {
					panic(err)
				}
				columnIDs = append(columnIDs, colID.GetID())
			}
			uniqueElem := scpb.UniqueConstraint{
				TableID:   table.GetID(),
				IndexID:   constraintInfo.Index.ID,
				ColumnIDs: columnIDs,
			}
			b.addIfDuplicateDoesNotExistForDir(dir, &uniqueElem)
		} else if constraintInfo.Kind == descpb.ConstraintTypePK ||
			constraintInfo.Kind == descpb.ConstraintTypeFK {
			// For the purpose of converting to elements, we can
			// completely ignore these types. Equivalent information
			// exists in the FK / PK elements.
		} else {
			panic(errors.Newf("unhandled constraint type: %v", constraintInfo.Kind))
		}
	}
	// Add locality information.
	b.addIfDuplicateDoesNotExistForDir(dir, &scpb.Locality{
		DescriptorID: table.GetID(),
		Locality:     table.GetLocalityConfig(),
	})
	// Inject any dependencies into the plan.
	for _, dep := range table.GetDependsOn() {
		dependsOn := &scpb.RelationDependedOnBy{
			DependedOnBy: table.GetID(),
			TableID:      dep,
		}
		b.addIfDuplicateDoesNotExistForDir(scpb.Target_DROP, dependsOn)
	}
	for _, depBy := range table.GetDependedOnBy() {
		dependedOnBy := &scpb.RelationDependedOnBy{
			DependedOnBy: depBy.ID,
			TableID:      table.GetID(),
		}
		b.addIfDuplicateDoesNotExistForDir(scpb.Target_DROP, dependedOnBy)
	}
	// Decompose type backrefs next
	b.decomposeTableTypeBackRefDeps(ctx, table, dir)
}
