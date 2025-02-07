// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tabledesc

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	plpgsqlparser "github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/semenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// ValidateTxnCommit performs pre-transaction-commit checks.
func (desc *wrapper) ValidateTxnCommit(
	vea catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
	// Check that primary key exists.
	if !desc.HasPrimaryKey() {
		vea.Report(unimplemented.NewWithIssue(48026,
			"primary key dropped without subsequent addition of new primary key in same transaction"))
	}
	// Check that the mutation ID values are appropriately set when a declarative
	// schema change is underway.
	if n := len(desc.Mutations); n > 0 && desc.GetDeclarativeSchemaChangerState() != nil {
		lastMutationID := desc.Mutations[n-1].MutationID
		if lastMutationID != desc.NextMutationID {
			vea.Report(errors.AssertionFailedf(
				"expected next mutation ID to be %d in table undergoing declarative schema change, found %d instead",
				lastMutationID, desc.NextMutationID))
		}
	}
}

// GetReferencedDescIDs returns the IDs of all descriptors referenced by
// this descriptor, including itself.
func (desc *wrapper) GetReferencedDescIDs(
	catalog.ValidationLevel,
) (catalog.DescriptorIDSet, error) {
	ids := catalog.MakeDescriptorIDSet(desc.GetID(), desc.GetParentID())
	// TODO(richardjcai): Remove logic for keys.PublicSchemaID in 22.2.
	if desc.GetParentSchemaID() != keys.PublicSchemaID {
		ids.Add(desc.GetParentSchemaID())
	}
	// Collect referenced table IDs in foreign keys.
	for _, fk := range desc.OutboundForeignKeys() {
		ids.Add(fk.GetReferencedTableID())
	}
	for _, fk := range desc.InboundForeignKeys() {
		ids.Add(fk.GetOriginTableID())
	}
	// Collect user defined type Oids and sequence references in columns.
	for _, col := range desc.DeletableColumns() {
		typedesc.GetTypeDescriptorClosure(col.GetType()).ForEach(ids.Add)
		for i := 0; i < col.NumUsesSequences(); i++ {
			ids.Add(col.GetUsesSequenceID(i))
		}
	}
	fnIDs, err := desc.GetAllReferencedFunctionIDs()
	if err != nil {
		return catalog.DescriptorIDSet{}, err
	}
	fnIDs.ForEach(ids.Add)
	// Collect user defined type IDs in expressions.
	// All serialized expressions within a table descriptor are serialized
	// with type annotations as IDs, so this visitor will collect them all.
	visitor := &tree.TypeCollectorVisitor{OIDs: make(map[oid.Oid]struct{})}
	_ = ForEachExprStringInTableDesc(desc, func(expr *string, typ catalog.DescExprType) error {
		if typ != catalog.SQLExpr {
			// Skip trigger function bodies - they are handled below.
			return nil
		}
		if parsedExpr, err := parser.ParseExpr(*expr); err == nil {
			// ignore errors
			tree.WalkExpr(visitor, parsedExpr)
		}
		return nil
	})
	// Add collected Oids to return set.
	for oid := range visitor.OIDs {
		if !types.IsOIDUserDefinedType(oid) {
			continue
		}
		ids.Add(typedesc.UserDefinedTypeOIDToID(oid))
	}
	// Add view dependencies.
	for _, id := range desc.GetDependsOn() {
		ids.Add(id)
	}
	for _, id := range desc.GetDependsOnTypes() {
		ids.Add(id)
	}
	for _, id := range desc.GetDependsOnFunctions() {
		ids.Add(id)
	}
	for _, ref := range desc.GetDependedOnBy() {
		ids.Add(ref.ID)
	}
	// Add trigger dependencies. NOTE: routine references are included above in
	// the call to GetAllReferencedFunctionIDs().
	for _, t := range desc.Triggers {
		for _, id := range t.DependsOn {
			ids.Add(id)
		}
		for _, id := range t.DependsOnTypes {
			ids.Add(id)
		}
	}
	// Add policy dependencies.
	for _, p := range desc.Policies {
		for _, id := range p.DependsOnTypes {
			ids.Add(id)
		}
		for _, id := range p.DependsOnRelations {
			ids.Add(id)
		}
		for _, id := range p.DependsOnFunctions {
			ids.Add(id)
		}
	}
	return ids, nil
}

// ValidateForwardReferences implements the catalog.Descriptor interface.
func (desc *wrapper) ValidateForwardReferences(
	vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {
	// Check that parent DB exists.
	dbDesc, err := vdg.GetDatabaseDescriptor(desc.GetParentID())
	if err != nil {
		vea.Report(err)
	} else if dbDesc.Dropped() {
		vea.Report(errors.AssertionFailedf("parent database %q (%d) is dropped",
			dbDesc.GetName(), dbDesc.GetID()))
	}

	// Check that parent schema exists.
	// TODO(richardjcai): Remove logic for keys.PublicSchemaID in 22.2.
	if desc.GetParentSchemaID() != keys.PublicSchemaID && !desc.IsTemporary() {
		schemaDesc, err := vdg.GetSchemaDescriptor(desc.GetParentSchemaID())
		if err != nil {
			vea.Report(err)
		}
		if schemaDesc != nil && dbDesc != nil && schemaDesc.GetParentID() != dbDesc.GetID() {
			vea.Report(errors.AssertionFailedf("parent schema %d is in different database %d",
				desc.GetParentSchemaID(), schemaDesc.GetParentID()))
		}
		if schemaDesc != nil && schemaDesc.Dropped() {
			vea.Report(errors.AssertionFailedf("parent schema %q (%d) is dropped",
				schemaDesc.GetName(), schemaDesc.GetID()))
		}
	}

	if dbDesc != nil {
		// Validate the all types present in the descriptor exist.
		typeIDs, _, err := desc.GetAllReferencedTypeIDs(dbDesc, vdg.GetTypeDescriptor)
		if err != nil {
			vea.Report(err)
		} else {
			for _, id := range typeIDs {
				_, err := vdg.GetTypeDescriptor(id)
				vea.Report(err)
			}
		}

		// Validate table locality.
		if err := multiregion.ValidateTableLocalityConfig(desc, dbDesc, vdg); err != nil {
			vea.Report(errors.Wrap(err, "invalid locality config"))
			return
		}
	}

	// For views, check dependent relations.
	if desc.IsView() {
		for _, id := range desc.DependsOnTypes {
			vea.Report(desc.validateOutboundTypeRef(id, vdg))
		}
		for _, id := range desc.DependsOnFunctions {
			vea.Report(desc.validateOutboundFuncRef(id, vdg))
		}
	}

	// Check all functions referenced by constraint exists.
	for _, cst := range desc.Checks {
		fnIDs, err := desc.GetAllReferencedFunctionIDsInConstraint(cst.ConstraintID)
		if err != nil {
			vea.Report(errors.Wrap(err, "invalid referenced functions IDs in constraint"))
		}
		for _, fnID := range fnIDs.Ordered() {
			vea.Report(desc.validateOutboundFuncRef(fnID, vdg))
		}
	}

	// check all functions referenced by columns exists.
	for _, col := range desc.Columns {
		for _, fnID := range col.UsesFunctionIds {
			vea.Report(desc.validateOutboundFuncRef(fnID, vdg))
		}
	}

	// Check enforced outbound foreign keys.
	for _, fk := range desc.EnforcedOutboundForeignKeys() {
		vea.Report(desc.validateOutboundFK(fk.ForeignKeyDesc(), vdg))
	}

	// Check partitioning is correctly set.
	// We only check these for active indexes, as inactive indexes may be in the
	// process of being backfilled without PartitionAllBy.
	// This check cannot be performed in ValidateSelf due to a conflict with
	// AllocateIDs.
	if desc.PartitionAllBy {
		for _, indexI := range desc.ActiveIndexes() {
			if !desc.matchingPartitionbyAll(indexI) {
				vea.Report(errors.AssertionFailedf(
					"table has PARTITION ALL BY defined, but index %s does not have matching PARTITION BY",
					indexI.GetName(),
				))
			}
		}
	}

	// Check that relations, types, and routines referenced by triggers exist.
	for i := range desc.Triggers {
		trigger := &desc.Triggers[i]
		for _, id := range trigger.DependsOn {
			vea.Report(catalog.ValidateOutboundTableRef(id, vdg))
		}
		for _, id := range trigger.DependsOnTypes {
			vea.Report(catalog.ValidateOutboundTypeRef(id, vdg))
		}
		for _, id := range trigger.DependsOnRoutines {
			vea.Report(catalog.ValidateOutboundFunctionRef(id, vdg))
		}
	}

	for i := range desc.Policies {
		policy := &desc.Policies[i]
		for _, id := range policy.DependsOnTypes {
			vea.Report(catalog.ValidateOutboundTypeRef(id, vdg))
		}
		for _, id := range policy.DependsOnRelations {
			vea.Report(catalog.ValidateOutboundTableRef(id, vdg))
		}
		for _, id := range policy.DependsOnFunctions {
			vea.Report(catalog.ValidateOutboundFunctionRef(id, vdg))
		}
	}
}

// ValidateBackReferences implements the catalog.Descriptor interface.
func (desc *wrapper) ValidateBackReferences(
	vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {
	// Check that all expression strings can be parsed.
	// NOTE: This could be performed in ValidateSelf, but we want to avoid that
	// since parsing all the expressions is a relatively expensive thing to do.
	_ = ForEachExprStringInTableDesc(desc, func(expr *string, typ catalog.DescExprType) (err error) {
		switch typ {
		case catalog.SQLExpr:
			_, err = parser.ParseExpr(*expr)
		case catalog.SQLStmt:
			_, err = parser.Parse(*expr)
		case catalog.PLpgSQLStmt:
			_, err = plpgsqlparser.Parse(*expr)
		}
		vea.Report(err)
		return nil
	})

	// Check that outbound foreign keys have matching back-references.
	for i := range desc.OutboundFKs {
		vea.Report(desc.validateOutboundFKBackReference(&desc.OutboundFKs[i], vdg))
	}

	// Check foreign key back-references.
	for i := range desc.InboundFKs {
		vea.Report(desc.validateInboundFK(&desc.InboundFKs[i], vdg))
	}

	// Check all functions referenced by constraint exists.
	for _, cst := range desc.Checks {
		fnIDs, err := desc.GetAllReferencedFunctionIDsInConstraint(cst.ConstraintID)
		if err != nil {
			vea.Report(errors.Wrap(err, "invalid referenced functions IDs in constraint"))
		}
		for _, fnID := range fnIDs.Ordered() {
			fn, err := vdg.GetFunctionDescriptor(fnID)
			if err != nil {
				vea.Report(err)
				continue
			}
			vea.Report(desc.validateOutboundFuncRefBackReferenceForConstraint(fn, cst.ConstraintID))
		}
	}

	// Check back-references in functions referenced by columns.
	for _, col := range desc.Columns {
		for _, fnID := range col.UsesFunctionIds {
			fn, err := vdg.GetFunctionDescriptor(fnID)
			if err != nil {
				vea.Report(err)
				continue
			}
			vea.Report(desc.validateOutboundFuncRefBackReferenceForColumn(fn, col.ID))
		}
	}

	// For views, check dependent relations.
	if desc.IsView() {
		for _, id := range desc.DependsOnTypes {
			typ, _ := vdg.GetTypeDescriptor(id)
			vea.Report(desc.validateOutboundTypeRefBackReference(typ))
		}
		for _, id := range desc.DependsOnFunctions {
			fn, _ := vdg.GetFunctionDescriptor(id)
			vea.Report(desc.validateOutboundFuncRefBackReference(fn))
		}
	}

	for _, id := range desc.DependsOn {
		ref, _ := vdg.GetTableDescriptor(id)
		if ref == nil {
			// Don't follow up on backward references for invalid or irrelevant
			// forward references.
			continue
		}
		vea.Report(catalog.ValidateOutboundTableRefBackReference(desc.GetID(), ref))
	}

	// Check relation back-references to relations and functions.
	for _, by := range desc.DependedOnBy {
		depDesc, err := vdg.GetDescriptor(by.ID)
		if err != nil {
			vea.Report(errors.NewAssertionErrorWithWrappedErrf(err, "invalid depended-on-by relation back reference"))
			continue
		}
		switch depDesc.DescriptorType() {
		case catalog.Table:
			// If this is a table, it may be referenced by a view, otherwise if this
			// is a sequence, then it may be also be referenced by a table.
			vea.Report(desc.validateInboundTableRef(by, vdg))
		case catalog.Function:
			// This relation may be referenced by a function.
			vea.Report(desc.validateInboundFunctionRef(by, vdg))
		default:
			vea.Report(errors.AssertionFailedf("table is depended on by unexpected %s %s (%d)",
				depDesc.DescriptorType(), depDesc.GetName(), depDesc.GetID()))
		}
	}

	// Check back-references in policies
	for _, p := range desc.Policies {
		for _, fnID := range p.DependsOnFunctions {
			fn, err := vdg.GetFunctionDescriptor(fnID)
			if err != nil {
				vea.Report(err)
				continue
			}
			vea.Report(desc.validateOutboundFuncRefBackReference(fn))
		}
		for _, id := range p.DependsOnTypes {
			typ, err := vdg.GetTypeDescriptor(id)
			if err != nil {
				vea.Report(err)
				continue
			}
			vea.Report(desc.validateOutboundTypeRefBackReference(typ))
		}
		for _, id := range p.DependsOnRelations {
			ref, _ := vdg.GetTableDescriptor(id)
			if ref == nil {
				continue
			}
			vea.Report(catalog.ValidateOutboundTableRefBackReference(desc.GetID(), ref))
		}
	}
}

func (desc *wrapper) validateOutboundTypeRef(id descpb.ID, vdg catalog.ValidationDescGetter) error {
	typ, err := vdg.GetTypeDescriptor(id)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "invalid depends-on type reference")
	}
	if typ.Dropped() {
		return errors.AssertionFailedf("depends-on type %q (%d) is dropped",
			typ.GetName(), typ.GetID())
	}
	return nil
}

func (desc *wrapper) validateOutboundTypeRefBackReference(ref catalog.TypeDescriptor) error {
	// TODO(postamar): maintain back-references in type, and validate these.
	return nil
}

func (desc *wrapper) validateOutboundFuncRef(id descpb.ID, vdg catalog.ValidationDescGetter) error {
	_, err := vdg.GetFunctionDescriptor(id)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "invalid depends-on function back reference")
	}
	return nil
}

func (desc *wrapper) validateOutboundFuncRefBackReference(ref catalog.FunctionDescriptor) error {
	for _, dep := range ref.GetDependedOnBy() {
		if dep.ID == desc.GetID() {
			return nil
		}
	}
	return errors.AssertionFailedf("depends-on function %q (%d) has no corresponding depended-on-by back reference",
		ref.GetName(), ref.GetID())
}

func (desc *wrapper) validateOutboundFuncRefBackReferenceForConstraint(
	ref catalog.FunctionDescriptor, cstID descpb.ConstraintID,
) error {
	for _, dep := range ref.GetDependedOnBy() {
		if dep.ID != desc.GetID() {
			continue
		}
		for _, id := range dep.ConstraintIDs {
			if id == cstID {
				return nil
			}
		}
	}
	return errors.AssertionFailedf("depends-on function %q (%d) has no corresponding depended-on-by back reference",
		ref.GetName(), ref.GetID())
}

func (desc *wrapper) validateOutboundFuncRefBackReferenceForColumn(
	ref catalog.FunctionDescriptor, colID descpb.ColumnID,
) error {
	for _, dep := range ref.GetDependedOnBy() {
		if dep.ID != desc.GetID() {
			continue
		}
		for _, id := range dep.ColumnIDs {
			if id == colID {
				return nil
			}
		}
	}
	return errors.AssertionFailedf("depends-on function %q (%d) has no corresponding depended-on-by back reference",
		ref.GetName(), ref.GetID())
}

func (desc *wrapper) validateInboundFunctionRef(
	by descpb.TableDescriptor_Reference, vdg catalog.ValidationDescGetter,
) error {
	backRefFunc, err := vdg.GetFunctionDescriptor(by.ID)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "invalid depended-on-by function back reference")
	}
	if backRefFunc.Dropped() {
		return errors.AssertionFailedf("depended-on-by function %q (%d) is dropped",
			backRefFunc.GetName(), backRefFunc.GetID())
	}

	for _, id := range backRefFunc.GetDependsOn() {
		if id == desc.GetID() {
			return nil
		}
	}

	return errors.AssertionFailedf("depended-on-by function %q (%d) has no corresponding depends-on forward reference",
		backRefFunc.GetName(), by.ID)
}

func (desc *wrapper) validateInboundTableRef(
	by descpb.TableDescriptor_Reference, vdg catalog.ValidationDescGetter,
) error {
	backReferencedTable, err := vdg.GetTableDescriptor(by.ID)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "invalid depended-on-by relation back reference")
	}
	if backReferencedTable.Dropped() {
		return errors.AssertionFailedf("depended-on-by relation %q (%d) is dropped",
			backReferencedTable.GetName(), backReferencedTable.GetID())
	}
	if desc.IsSequence() {
		// The ColumnIDs field takes a different meaning when the validated
		// descriptor is for a sequence. In this case, they refer to the columns
		// in the referenced descriptor instead.
		for _, colID := range by.ColumnIDs {
			// Skip this check if the column ID is zero. This can happen due to
			// bugs in 20.2.
			//
			// TODO(ajwerner): Make sure that a migration in 22.2 fixes this issue.
			if colID == 0 {
				continue
			}
			col := catalog.FindColumnByID(backReferencedTable, colID)
			if col == nil {
				return errors.AssertionFailedf("depended-on-by relation %q (%d) does not have a column with ID %d",
					backReferencedTable.GetName(), by.ID, colID)
			}
			var found bool
			for i := 0; i < col.NumUsesSequences(); i++ {
				if col.GetUsesSequenceID(i) == desc.GetID() {
					found = true
					break
				}
			}
			if found {
				continue
			}
			return errors.AssertionFailedf(
				"depended-on-by relation %q (%d) has no reference to this sequence in column %q (%d)",
				backReferencedTable.GetName(), by.ID, col.GetName(), col.GetID())
		}
	}

	// View back-references need corresponding forward reference.
	if !backReferencedTable.IsView() {
		return nil
	}
	for _, id := range backReferencedTable.TableDesc().DependsOn {
		if id == desc.GetID() {
			return nil
		}
	}
	return errors.AssertionFailedf("depended-on-by view %q (%d) has no corresponding depends-on forward reference",
		backReferencedTable.GetName(), by.ID)
}

// validateFK asserts that references to desc from inbound and outbound FKs are
// valid.
func (desc *wrapper) validateSelfFKs() error {
	colsByID := catalog.ColumnsByIDs(desc)

	// Validate all FKs where desc is the origin table.
	// We'll validate the referenced table elsewhere.
	for _, wrapper := range desc.OutboundForeignKeys() {
		fk := wrapper.ForeignKeyDesc()

		// OriginTableID should be equal to desc's ID because desc is the origin.
		if fk.OriginTableID != desc.GetID() {
			return errors.AssertionFailedf(
				"invalid outbound foreign key %q: origin table ID should be %d. got %d",
				fk.Name,
				desc.GetID(),
				fk.OriginTableID,
			)
		}

		if len(fk.OriginColumnIDs) == 0 {
			return errors.AssertionFailedf("invalid outbound foreign key %q: no origin columns", fk.Name)
		}

		if len(fk.OriginColumnIDs) != len(fk.ReferencedColumnIDs) {
			return errors.AssertionFailedf("invalid outbound foreign key %q: mismatched number of referenced and origin columns", fk.Name)
		}

		for _, colID := range fk.OriginColumnIDs {
			if _, ok := colsByID[colID]; !ok {
				return errors.AssertionFailedf(
					"invalid outbound foreign key %q from table %q (%d): missing origin column=%d",
					fk.Name,
					desc.GetName(),
					desc.GetID(),
					colID,
				)
			}
		}
	}

	// Validate all FKs where desc is the referenced table.
	// We'll validate the origin table elsewhere.
	for _, wrapper := range desc.InboundForeignKeys() {
		fk := wrapper.ForeignKeyDesc()

		// ReferencedTableID should be equal to desc's ID because desc is the
		// referenced table.
		if fk.ReferencedTableID != desc.GetID() {
			return errors.AssertionFailedf(
				"invalid inbound foreign key %q: referenced table ID should be %d. got %d",
				fk.Name,
				desc.GetID(),
				fk.ReferencedTableID,
			)
		}

		if len(fk.ReferencedColumnIDs) == 0 {
			return errors.AssertionFailedf("invalid inbound foreign key %q: no referenced columns", fk.Name)
		}

		if len(fk.ReferencedColumnIDs) != len(fk.OriginColumnIDs) {
			return errors.AssertionFailedf("invalid inbound foreign key %q: mismatched number of referenced and origin columns", fk.Name)
		}

		for _, colID := range fk.ReferencedColumnIDs {
			if _, ok := colsByID[colID]; !ok {
				return errors.AssertionFailedf(
					"invalid inbound foreign key %q to table %q (%d): missing referenced column=%d",
					fk.Name,
					desc.GetName(),
					desc.GetID(),
					colID,
				)
			}
		}
	}

	return nil
}

func (desc *wrapper) validateOutboundFK(
	fk *descpb.ForeignKeyConstraint, vdg catalog.ValidationDescGetter,
) error {
	referencedTable, err := vdg.GetTableDescriptor(fk.ReferencedTableID)
	if err != nil {
		return errors.Wrapf(err,
			"invalid foreign key: missing table=%d", fk.ReferencedTableID)
	}
	if referencedTable.Dropped() {
		return errors.AssertionFailedf("referenced table %q (%d) is dropped",
			referencedTable.GetName(), referencedTable.GetID())
	}

	return nil
}

func (desc *wrapper) validateOutboundFKBackReference(
	fk *descpb.ForeignKeyConstraint, vdg catalog.ValidationDescGetter,
) error {
	referencedTable, _ := vdg.GetTableDescriptor(fk.ReferencedTableID)
	if referencedTable == nil || referencedTable.Dropped() {
		// Don't follow up on backward references for invalid or irrelevant forward
		// references.
		return nil
	}

	colsByID := catalog.ColumnsByIDs(referencedTable)
	for _, colID := range fk.ReferencedColumnIDs {
		if _, ok := colsByID[colID]; !ok {
			return errors.AssertionFailedf(
				"invalid outbound foreign key backreference from table %q (%d): missing referenced column=%d",
				referencedTable.GetName(),
				referencedTable.GetID(),
				colID,
			)
		}
	}

	for _, backref := range referencedTable.InboundForeignKeys() {
		if backref.GetOriginTableID() == desc.ID && backref.GetName() == fk.Name {
			return nil
		}
	}

	return errors.AssertionFailedf("missing fk back reference %q to %q from %q",
		fk.Name, desc.Name, referencedTable.GetName())
}

func (desc *wrapper) validateInboundFK(
	backref *descpb.ForeignKeyConstraint, vdg catalog.ValidationDescGetter,
) error {
	originTable, err := vdg.GetTableDescriptor(backref.OriginTableID)
	if err != nil {
		return errors.Wrapf(err,
			"invalid foreign key backreference: missing table=%d", backref.OriginTableID)
	}

	if originTable.Dropped() {
		return errors.AssertionFailedf("origin table %q (%d) is dropped",
			originTable.GetName(), originTable.GetID())
	}

	colsByID := catalog.ColumnsByIDs(originTable)
	for _, colID := range backref.OriginColumnIDs {
		if _, ok := colsByID[colID]; !ok {
			return errors.AssertionFailedf(
				"invalid foreign key backreference from table %q (%d): missing origin column=%d",
				originTable.GetName(),
				originTable.GetID(),
				colID,
			)
		}
	}

	for _, fk := range originTable.OutboundForeignKeys() {
		if fk.GetReferencedTableID() == desc.ID && fk.GetName() == backref.Name {
			return nil
		}
	}

	return errors.AssertionFailedf("missing fk forward reference %q to %q from %q",
		backref.Name, desc.Name, originTable.GetName())
}

func (desc *wrapper) matchingPartitionbyAll(indexI catalog.Index) bool {
	primaryIndexPartitioning := desc.PrimaryIndex.KeyColumnIDs[:desc.PrimaryIndex.Partitioning.NumColumns]
	indexPartitioning := indexI.IndexDesc().KeyColumnIDs[:indexI.PartitioningColumnCount()]
	if len(primaryIndexPartitioning) != len(indexPartitioning) {
		return false
	}
	for i, id := range primaryIndexPartitioning {
		if id != indexPartitioning[i] {
			return false
		}
	}
	return true
}

func validateMutation(m *descpb.DescriptorMutation) error {
	unSetEnums := m.State == descpb.DescriptorMutation_UNKNOWN || m.Direction == descpb.DescriptorMutation_NONE
	switch desc := m.Descriptor_.(type) {
	case *descpb.DescriptorMutation_Column:
		col := desc.Column
		if unSetEnums {
			return errors.AssertionFailedf(
				"mutation in state %s, direction %s, col %q, id %v",
				errors.Safe(m.State), errors.Safe(m.Direction), col.Name, errors.Safe(col.ID))
		}
	case *descpb.DescriptorMutation_Index:
		if unSetEnums {
			idx := desc.Index
			return errors.AssertionFailedf(
				"mutation in state %s, direction %s, index %s, id %v",
				errors.Safe(m.State), errors.Safe(m.Direction), idx.Name, errors.Safe(idx.ID))
		}
	case *descpb.DescriptorMutation_Constraint:
		if unSetEnums {
			return errors.AssertionFailedf(
				"mutation in state %s, direction %s, constraint %v",
				errors.Safe(m.State), errors.Safe(m.Direction), desc.Constraint.Name)
		}
	case *descpb.DescriptorMutation_PrimaryKeySwap:
		if m.Direction == descpb.DescriptorMutation_NONE {
			return errors.AssertionFailedf(
				"primary key swap mutation in state %s, direction %s", errors.Safe(m.State), errors.Safe(m.Direction))
		}
	case *descpb.DescriptorMutation_ComputedColumnSwap:
		if m.Direction == descpb.DescriptorMutation_NONE {
			return errors.AssertionFailedf(
				"computed column swap mutation in state %s, direction %s", errors.Safe(m.State), errors.Safe(m.Direction))
		}
	case *descpb.DescriptorMutation_MaterializedViewRefresh:
		if m.Direction == descpb.DescriptorMutation_NONE {
			return errors.AssertionFailedf(
				"materialized view refresh mutation in state %s, direction %s", errors.Safe(m.State), errors.Safe(m.Direction))
		}
	case *descpb.DescriptorMutation_ModifyRowLevelTTL:
		if m.Direction == descpb.DescriptorMutation_NONE {
			return errors.AssertionFailedf(
				"modify row level TTL mutation in state %s, direction %s", errors.Safe(m.State), errors.Safe(m.Direction))
		}
	default:
		return errors.AssertionFailedf(
			"mutation in state %s, direction %s, and no column/index descriptor",
			errors.Safe(m.State), errors.Safe(m.Direction))
	}

	switch m.State {
	case descpb.DescriptorMutation_BACKFILLING:
		if _, ok := m.Descriptor_.(*descpb.DescriptorMutation_Index); !ok {
			return errors.AssertionFailedf("non-index mutation in state %s", errors.Safe(m.State))
		}
	case descpb.DescriptorMutation_MERGING:
		if _, ok := m.Descriptor_.(*descpb.DescriptorMutation_Index); !ok {
			return errors.AssertionFailedf("non-index mutation in state %s", errors.Safe(m.State))
		}
	}

	return nil
}

// ValidateSelf validates that the table descriptor is well formed. Checks
// include validating the table, column and index names, verifying that column
// names and index names are unique and verifying that column IDs and index IDs
// are consistent. Use Validate to validate that cross-table references are
// correct.
// If version is supplied, the descriptor is checked for version incompatibilities.
func (desc *wrapper) ValidateSelf(vea catalog.ValidationErrorAccumulator) {
	// Validate local properties of the descriptor.
	vea.Report(catalog.ValidateName(desc))
	if desc.GetID() == descpb.InvalidID {
		vea.Report(errors.AssertionFailedf("invalid table ID %d", desc.GetID()))
	}
	if desc.GetParentSchemaID() == descpb.InvalidID {
		vea.Report(errors.AssertionFailedf("invalid parent schema ID %d", desc.GetParentSchemaID()))
	}

	// ParentID is the ID of the database holding this table.
	// It is often < ID, except when a table gets moved across databases.
	if desc.GetParentID() == descpb.InvalidID && !desc.IsVirtualTable() {
		vea.Report(errors.AssertionFailedf("invalid parent ID %d", desc.GetParentID()))
	}

	// Check that CreateAsOfTime is set if the table relies on it and is in the
	// process of being added.
	if desc.Adding() && desc.IsAs() && desc.GetCreateAsOfTime().IsEmpty() && desc.GetVersion() > 1 {
		vea.Report(errors.AssertionFailedf("table is in the ADD state and was created with " +
			"CREATE TABLE ... AS but does not have a CreateAsOfTime set"))
	}

	// VirtualTables have their privileges stored in system.privileges which
	// is validated outside of the descriptor.
	if !desc.IsVirtualTable() {
		// Validate the privilege descriptor.
		if desc.Privileges == nil {
			vea.Report(errors.AssertionFailedf("privileges not set"))
		}
		vea.Report(catprivilege.Validate(*desc.Privileges, desc, desc.GetObjectType()))
	}

	// Validate that the depended-on-by references are well-formed.
	for _, ref := range desc.DependedOnBy {
		if ref.ID == descpb.InvalidID {
			vea.Report(errors.AssertionFailedf(
				"invalid relation ID %d in depended-on-by references",
				ref.ID))
		}
		if len(ref.ColumnIDs) > catalog.MakeTableColSet(ref.ColumnIDs...).Len() {
			vea.Report(errors.AssertionFailedf("duplicate column IDs found in depended-on-by references: %v",
				ref.ColumnIDs))
		}
	}

	if !desc.IsView() {
		if len(desc.DependsOn) > 0 {
			vea.Report(errors.AssertionFailedf(
				"has depends-on references despite not being a view"))
		}
		if len(desc.DependsOnTypes) > 0 {
			vea.Report(errors.AssertionFailedf(
				"has depends-on-types references despite not being a view"))
		}
	}

	desc.validateAutoStatsSettings(vea)

	if desc.IsSequence() {
		return
	}

	// Validate the depended-on-by references to this table's columns and indexes
	// for non-sequence relations.
	// The ColumnIDs field takes a different meaning when the validated
	// descriptor is for a sequence, in that case they refer to the columns of the
	// referenced relation. This case is handled during cross-reference
	// validation.
	for _, ref := range desc.DependedOnBy {
		if ref.IndexID != 0 {
			if catalog.FindIndexByID(desc, ref.IndexID) == nil {
				vea.Report(errors.AssertionFailedf(
					"index ID %d found in depended-on-by references, no such index in this relation",
					ref.IndexID))
			}
		}
		for _, colID := range ref.ColumnIDs {
			if catalog.FindColumnByID(desc, colID) == nil {
				vea.Report(errors.AssertionFailedf(
					"column ID %d found in depended-on-by references, no such column in this relation",
					colID))
			}
		}
	}

	if len(desc.Columns) == 0 {
		vea.Report(ErrMissingColumns)
		return
	}

	if err := desc.validateColumns(); err != nil {
		vea.Report(err)
		return
	}

	if err := desc.validateSelfFKs(); err != nil {
		vea.Report(err)
		return
	}

	if err := desc.validateTriggers(); err != nil {
		vea.Report(err)
		return
	}

	if desc.IsVirtualTable() {
		return
	}

	// We maintain forward compatibility, so if you see this error message with a
	// version older that what this client supports, then there's a
	// maybeFillInDescriptor missing from some codepath.
	if desc.GetFormatVersion() < descpb.InterleavedFormatVersion {
		vea.Report(errors.AssertionFailedf(
			"table is encoded using using version %d, but this client only supports version %d",
			desc.GetFormatVersion(), descpb.InterleavedFormatVersion))
		return
	}

	// Validate mutations and exit early if any of these are deeply corrupted.
	{
		var mutationIDs intsets.Fast
		mutationsHaveErrs := false
		for _, m := range desc.Mutations {
			mutationIDs.Add(int(m.MutationID))
			if err := validateMutation(&m); err != nil {
				vea.Report(err)
				mutationsHaveErrs = true
			}
		}
		// Check that job IDs are uniquely mapped to mutation IDs in the
		// MutationJobs slice.
		jobIDs := make(map[descpb.MutationID]catpb.JobID)
		for _, mj := range desc.MutationJobs {
			if !mutationIDs.Contains(int(mj.MutationID)) {
				vea.Report(errors.AssertionFailedf("unknown mutation ID %d associated with job ID %d",
					mj.MutationID, mj.JobID))
				mutationsHaveErrs = true
				continue
			}
			if jobID, found := jobIDs[mj.MutationID]; found {
				vea.Report(errors.AssertionFailedf("two job IDs %d and %d mapped to the same mutation ID %d",
					jobID, mj.JobID, mj.MutationID))
				mutationsHaveErrs = true
				continue
			}
			jobIDs[mj.MutationID] = mj.JobID
		}
		if mutationsHaveErrs {
			return
		}
	}

	// Build a mapping of column descriptors by ID. The columns have already been
	// validated at this point, so we know that the mapping is 1-to-1.
	columnsByID := make(map[descpb.ColumnID]catalog.Column, len(desc.Columns))
	for _, col := range desc.DeletableColumns() {
		columnsByID[col.GetID()] = col
	}

	// TODO(dt): Validate each column only appears at-most-once in any FKs.

	// Only validate column families, constraints, and indexes if this is
	// actually a table, not if it's just a view.
	if desc.IsPhysicalTable() {
		desc.validateConstraintNamesAndIDs(vea)
		newErrs := []error{
			desc.validateColumnFamilies(columnsByID),
			desc.validateCheckConstraints(columnsByID),
			desc.validateUniqueWithoutIndexConstraints(columnsByID),
			desc.validateTableIndexes(columnsByID, vea.IsActive),
			desc.validatePartitioning(),
			desc.validatePolicies(),
		}
		hasErrs := false
		for _, err := range newErrs {
			if err != nil {
				vea.Report(err)
				hasErrs = true
			}
		}
		if hasErrs {
			return
		}
	}

	// Ensure that mutations cannot be queued if a primary key change, TTL change
	// or an alter column type schema change has either been started in
	// this transaction, or is currently in progress.
	var alterPKMutation descpb.MutationID
	var alterColumnTypeMutation descpb.MutationID
	var modifyTTLMutation descpb.MutationID
	var foundAlterPK bool
	var foundAlterColumnType bool
	var foundModifyTTL bool

	for _, m := range desc.Mutations {
		// If we have seen an alter primary key mutation, then
		// m we are considering right now is invalid.
		if foundAlterPK {
			if alterPKMutation == m.MutationID {
				vea.Report(unimplemented.NewWithIssue(
					45615,
					"cannot perform other schema changes in the same transaction as a primary key change"),
				)
			} else {
				vea.Report(unimplemented.NewWithIssue(
					45615,
					"cannot perform a schema change operation while a primary key change is in progress"),
				)
			}
			return
		}
		if foundAlterColumnType {
			if alterColumnTypeMutation == m.MutationID {
				vea.Report(unimplemented.NewWithIssue(
					47137,
					"cannot perform other schema changes in the same transaction as an ALTER COLUMN TYPE schema change",
				))
			} else {
				vea.Report(unimplemented.NewWithIssue(
					47137,
					"cannot perform a schema change operation while an ALTER COLUMN TYPE schema change is in progress",
				))
			}
			return
		}
		if foundModifyTTL {
			if modifyTTLMutation == m.MutationID {
				vea.Report(pgerror.Newf(
					pgcode.FeatureNotSupported,
					"cannot perform other schema changes in the same transaction as a TTL mutation",
				))
			} else {
				vea.Report(pgerror.Newf(
					pgcode.FeatureNotSupported,
					"cannot perform a schema change operation while a TTL change is in progress",
				))
			}
			return
		}
		if m.GetPrimaryKeySwap() != nil {
			foundAlterPK = true
			alterPKMutation = m.MutationID
		}
		if m.GetComputedColumnSwap() != nil {
			foundAlterColumnType = true
			alterColumnTypeMutation = m.MutationID
		}
		if m.GetModifyRowLevelTTL() != nil {
			foundModifyTTL = true
			modifyTTLMutation = m.MutationID
		}
	}

	// Validate that the presence of MutationJobs (from the old schema changer)
	// and the presence of a DeclarativeSchemaChangeJobID are mutually exclusive. (Note
	// the jobs themselves can be running simultaneously, since a resumer can
	// still be running after the schema change is complete from the point of view
	// of the descriptor, in both the new and old schema change jobs.)
	if dscs := desc.DeclarativeSchemaChangerState; dscs != nil && len(desc.MutationJobs) > 0 {
		vea.Report(errors.AssertionFailedf(
			"invalid concurrent declarative schema change job %d and legacy schema change jobs %v",
			dscs.JobID, desc.MutationJobs))
	}

	vea.Report(ValidateRowLevelTTL(desc.GetRowLevelTTL()))
	// The remaining validation is called separately from ValidateRowLevelTTL
	// because it can only be called on an initialized table descriptor.
	// ValidateRowLevelTTL is also used before the table descriptor is fully
	// initialized to validate the storage parameters.
	vea.Report(ValidateTTLExpirationExpr(desc))
	vea.Report(ValidateTTLExpirationColumn(desc))

	// Validate that there are no column with both a foreign key ON UPDATE and an
	// ON UPDATE expression. This check is made to ensure that we know which ON
	// UPDATE action to perform when a FK UPDATE happens.
	ValidateOnUpdate(desc, vea.Report)
}

// ValidateNotVisibleIndex returns a notice when dropping the given index may
// behave differently than marking the index invisible. NotVisible indexes may
// still be used to police unique or foreign key constraint check behind the
// scene. Hence, dropping the index might behave different from marking the
// index invisible. There are three cases where this might happen:
// Case 1: If the index is unique,
// - Sub case 1: if the given tableDes is a parent table and this index could be
// useful for FK check on the parent table.
// - Sub case 2: otherwise, a unique index may only be useful for unique
// constraint check. These first two cases can be covered by just checking
// whether the index is unique.
// Case 2: if the given tableDesc is a child table and this index could be
// helpful for FK check in the child table. Note that we can only decide if an
// index is currently useful for FK check on a child table. It is possible that
// the user adds FK constraint later and this invisible index becomes useful. No
// notices would be given at that point.
func ValidateNotVisibleIndex(
	index catalog.Index, tableDesc catalog.TableDescriptor,
) pgnotice.Notice {
	notices := pgnotice.Newf("queries may still use not visible indexes to enforce unique and foreign key constraints")
	if index.IsUnique() {
		// Case 1: The given index is a unique index.
		return notices
	}

	if index.IsPartial() || index.NumKeyColumns() == 0 {
		return nil
	}
	firstKeyColID := index.GetKeyColumnID(0)
	for _, fk := range tableDesc.OutboundForeignKeys() {
		for i, n := 0, fk.NumOriginColumns(); i < n; i++ {
			if fk.GetOriginColumnID(i) == firstKeyColID {
				// Case 2: The given index is an index on a child table that may be useful
				// for FK check.
				return notices
			}
		}
	}
	return nil
}

// ValidateOnUpdate returns an error if there is a column with both a foreign
// key constraint and an ON UPDATE expression, nil otherwise.
func ValidateOnUpdate(desc catalog.TableDescriptor, errReportFn func(err error)) {
	var onUpdateCols catalog.TableColSet
	for _, col := range desc.AllColumns() {
		if col.HasOnUpdate() {
			onUpdateCols.Add(col.GetID())
		}
	}

	for _, fk := range desc.OutboundForeignKeys() {
		if fk.OnUpdate() == semenumpb.ForeignKeyAction_NO_ACTION ||
			fk.OnUpdate() == semenumpb.ForeignKeyAction_RESTRICT {
			continue
		}
		for i, n := 0, fk.NumOriginColumns(); i < n; i++ {
			fkCol := fk.GetOriginColumnID(i)
			if onUpdateCols.Contains(fkCol) {
				col, err := catalog.MustFindColumnByID(desc, fkCol)
				if err != nil {
					errReportFn(err)
				} else {
					errReportFn(pgerror.Newf(pgcode.InvalidTableDefinition,
						"cannot specify both ON UPDATE expression and a foreign key"+
							" ON UPDATE action for column %q",
						col.ColName(),
					))
				}
			}
		}
	}
}

func (desc *wrapper) validateConstraintNamesAndIDs(vea catalog.ValidationErrorAccumulator) {
	if !desc.IsTable() {
		return
	}
	constraints := desc.AllConstraints()
	names := make(map[string]descpb.ConstraintID, len(constraints))
	idToName := make(map[descpb.ConstraintID]string, len(constraints))
	for _, c := range constraints {
		if c.AsCheck() != nil && c.AsCheck().IsNotNullColumnConstraint() {
			// Relax validation for NOT NULL constraint when disguised as CHECK
			// constraint, because they don't have a constraintID.
			continue
		}
		if c.GetConstraintID() == 0 {
			vea.Report(errors.AssertionFailedf(
				"constraint ID was missing for constraint %q",
				c.GetName()))
		} else if c.GetConstraintID() >= desc.NextConstraintID {
			vea.Report(errors.AssertionFailedf(
				"constraint %q has ID %d not less than NextConstraintID value %d for table",
				c.GetName(), c.GetConstraintID(), desc.NextConstraintID))
		}
		if c.GetName() == "" {
			vea.Report(pgerror.Newf(pgcode.Syntax, "empty constraint name"))
		}
		if !c.Dropped() {
			if otherID, found := names[c.GetName()]; found && c.GetConstraintID() != otherID {
				vea.Report(pgerror.Newf(pgcode.DuplicateObject,
					"duplicate constraint name: %q", c.GetName()))
			}
			names[c.GetName()] = c.GetConstraintID()
		}
		if other, found := idToName[c.GetConstraintID()]; found {
			vea.Report(pgerror.Newf(pgcode.DuplicateObject,
				"constraint ID %d in constraint %q already in use by %q",
				c.GetConstraintID(), c.GetName(), other))
		}
		idToName[c.GetConstraintID()] = c.GetName()
	}

}

func (desc *wrapper) validateColumns() error {
	columnIDs := make(map[descpb.ColumnID]*descpb.ColumnDescriptor, len(desc.Columns))
	columnNames := make(map[string]descpb.ColumnID, len(desc.Columns))
	for _, column := range desc.DeletableColumns() {
		if len(column.GetName()) == 0 {
			return pgerror.Newf(pgcode.Syntax, "empty column name")
		}
		if column.GetID() == 0 {
			return errors.AssertionFailedf("invalid column ID %d", errors.Safe(column.GetID()))
		}

		if column.GetID() >= desc.NextColumnID {
			return errors.AssertionFailedf("column %q invalid ID (%d) >= next column ID (%d)",
				column.GetName(), errors.Safe(column.GetID()), errors.Safe(desc.NextColumnID))
		}

		if other, ok := columnIDs[column.GetID()]; ok {
			return errors.Newf("column %q duplicate ID of column %q: %d",
				column.GetName(), other.Name, column.GetID())
		}
		columnIDs[column.GetID()] = column.ColumnDesc()

		if column.IsComputed() {
			// Verify that the computed column expression is valid.
			expr, err := parser.ParseExpr(column.GetComputeExpr())
			if err != nil {
				return err
			}
			valid, err := schemaexpr.HasValidColumnReferences(desc, expr)
			if err != nil {
				return err
			}
			if !valid {
				return errors.Newf("computed column %q refers to unknown columns in expression: %s",
					column.GetName(), column.GetComputeExpr())
			}
		} else if column.IsVirtual() {
			return errors.Newf("virtual column %q is not computed", column.GetName())
		}

		if column.IsComputed() {
			if column.HasDefault() {
				return pgerror.Newf(pgcode.Syntax,
					"computed column %q cannot also have a DEFAULT expression",
					column.GetName(),
				)
			}
			if column.HasOnUpdate() {
				return pgerror.Newf(pgcode.InvalidTableDefinition,
					"computed column %q cannot also have an ON UPDATE expression",
					column.GetName(),
				)
			}
		}

		if column.IsHidden() && column.IsInaccessible() {
			return errors.Newf("column %q cannot be hidden and inaccessible", column.GetName())
		}

		if column.IsComputed() && column.IsGeneratedAsIdentity() {
			return errors.Newf("both generated identity and computed expression specified for column %q", column.GetName())
		}

		// If the column is public and it's generated as identity, then
		// the column has to have an enforced NOT NULL constraint. In a mutation
		// stage, the column is only accessible for non-user facing writes/deletes and its
		// fine to not enforce the null constraint for identity columns until column
		// moves to public.
		if column.Public() && column.IsGeneratedAsIdentity() {
			// A column's NOT NULL constraint is enforced when either the column
			// descriptor is NOT NULL, or there is an enforced, functionally equivalent
			// CHECK constraint, (col_name IS NOT NULL), in `desc`, which can happen
			// during schema changes.
			if column.IsNullable() {
				found := false
				for _, ck := range desc.CheckConstraints() {
					if ck.IsNotNullColumnConstraint() &&
						ck.GetReferencedColumnID(0) == column.GetID() {
						found = true
					}
				}
				if !found {
					return errors.Newf("conflicting NULL/NOT NULL declarations for column %q", column.GetName())
				}
			}

			// For generated as identity columns ensure that the column uses sequences
			// if it is backed by a sequence.
			if column.NumOwnsSequences() == 1 && column.NumUsesSequences() != 1 {
				return errors.Newf("column %q is GENERATED BY IDENTITY without sequence references", column.GetName())
			}
		}

		if column.HasOnUpdate() && column.IsGeneratedAsIdentity() {
			return errors.Newf("both generated identity and on update expression specified for column %q", column.GetName())
		}

		// The following checks on names only apply to non-dropped columns.
		if column.Dropped() {
			continue
		}

		if _, columnNameExists := columnNames[column.GetName()]; columnNameExists {
			for i := range desc.Columns {
				if desc.Columns[i].Name == column.GetName() {
					return pgerror.Newf(pgcode.DuplicateColumn,
						"duplicate column name: %q", column.GetName())
				}
			}
			return pgerror.Newf(pgcode.DuplicateColumn,
				"duplicate: column %q in the middle of being added, not yet public", column.GetName())
		}
		if colinfo.IsSystemColumnName(column.GetName()) {
			return pgerror.Newf(pgcode.DuplicateColumn,
				"column name %q conflicts with a system column name", column.GetName())
		}
		columnNames[column.GetName()] = column.GetID()
	}
	return nil
}

func (desc *wrapper) validateColumnFamilies(columnsByID map[descpb.ColumnID]catalog.Column) error {
	if len(desc.Families) < 1 {
		return errors.Newf("at least 1 column family must be specified")
	}
	if desc.Families[0].ID != descpb.FamilyID(0) {
		return errors.Newf("the 0th family must have ID 0")
	}

	familyNames := map[string]struct{}{}
	familyIDs := map[descpb.FamilyID]string{}
	colIDToFamilyID := map[descpb.ColumnID]descpb.FamilyID{}
	for i := range desc.Families {
		family := &desc.Families[i]
		if len(family.Name) == 0 {
			return pgerror.Newf(pgcode.Syntax, "empty family name")
		}

		if i != 0 {
			prevFam := desc.Families[i-1]
			if family.ID < prevFam.ID {
				return errors.Newf(
					"family %s at index %d has id %d less than family %s at index %d with id %d",
					family.Name, i, family.ID, prevFam.Name, i-1, prevFam.ID)
			}
		}

		if _, ok := familyNames[family.Name]; ok {
			return errors.Newf("duplicate family name: %q", family.Name)
		}
		familyNames[family.Name] = struct{}{}

		if other, ok := familyIDs[family.ID]; ok {
			return errors.Newf("family %q duplicate ID of family %q: %d",
				family.Name, other, family.ID)
		}
		familyIDs[family.ID] = family.Name

		if family.ID >= desc.NextFamilyID {
			return errors.Newf("family %q invalid family ID (%d) > next family ID (%d)",
				family.Name, family.ID, desc.NextFamilyID)
		}

		if len(family.ColumnIDs) != len(family.ColumnNames) {
			return errors.Newf("mismatched column ID size (%d) and name size (%d)",
				len(family.ColumnIDs), len(family.ColumnNames))
		}

		for i, colID := range family.ColumnIDs {
			colName := family.ColumnNames[i]
			col, ok := columnsByID[colID]
			if !ok {
				return errors.Newf("family %q contains column reference %q with unknown ID %d", family.Name, colName, colID)
			}
			if col.GetName() != colName {
				return errors.Newf("family %q column %d should have name %q, but found name %q",
					family.Name, colID, col.GetName(), colName)
			}
			if col.IsVirtual() {
				return errors.Newf("virtual computed column %q cannot be part of a family", col.GetName())
			}
		}

		for _, colID := range family.ColumnIDs {
			if famID, ok := colIDToFamilyID[colID]; ok {
				return errors.Newf("column %d is in both family %d and %d", colID, famID, family.ID)
			}
			colIDToFamilyID[colID] = family.ID
		}
	}
	for colID, col := range columnsByID {
		if !col.IsVirtual() {
			if _, ok := colIDToFamilyID[colID]; !ok {
				return errors.Newf("column %q is not in any column family", col.GetName())
			}
		}
	}
	return nil
}

// validateTriggers validates that triggers are well-formed.
func (desc *wrapper) validateTriggers() error {
	var triggerIDs intsets.Fast
	triggerNames := map[string]struct{}{}
	for i := range desc.Triggers {
		trigger := &desc.Triggers[i]

		// Validate that the trigger's ID is valid.
		if trigger.ID >= desc.NextTriggerID {
			return errors.Newf(
				"trigger %q has ID %d not less than NextTrigger value %d for table",
				trigger.Name, trigger.ID, desc.NextTriggerID)
		}
		if triggerIDs.Contains(int(trigger.ID)) {
			return errors.Newf("duplicate trigger ID: %d", trigger.ID)
		}
		triggerIDs.Add(int(trigger.ID))

		// Verify that the trigger's name is valid.
		if len(trigger.Name) == 0 {
			return pgerror.Newf(pgcode.Syntax, "empty trigger name")
		}
		if _, ok := triggerNames[trigger.Name]; ok {
			return errors.Newf("duplicate trigger name: %q", trigger.Name)
		}
		triggerNames[trigger.Name] = struct{}{}

		// Verify that columns referenced by the trigger events are valid.
		for _, ev := range trigger.Events {
			if len(ev.ColumnNames) > 0 {
				for _, colName := range ev.ColumnNames {
					if catalog.FindColumnByTreeName(desc, tree.Name(colName)) == nil {
						return errors.Newf("trigger %q contains unknown column \"%s\"", trigger.Name, colName)
					}
				}
			}
		}

		// Verify that the WHEN expression and function body statements are valid.
		if trigger.WhenExpr != "" {
			_, err := parser.ParseExpr(trigger.WhenExpr)
			if err != nil {
				return err
			}
		}
		_, err := plpgsqlparser.Parse(trigger.FuncBody)
		if err != nil {
			return err
		}

		// Verify that the trigger function ID is valid.
		if trigger.FuncID == descpb.InvalidID {
			return errors.Newf("invalid function id %d in trigger %q", trigger.FuncID, trigger.Name)
		}
		routineIDs := catalog.MakeDescriptorIDSet(trigger.DependsOnRoutines...)
		if !routineIDs.Contains(trigger.FuncID) {
			return errors.Newf("expected function id %d to be in depends-on-routines for trigger %q",
				trigger.FuncID, trigger.Name)
		}

		// Verify that the trigger's references are valid. Note that the existence
		// and status of the referenced objects are checked in
		// ValidateForwardReferences for the table.
		var seenIDs catalog.DescriptorIDSet
		for idx, depID := range trigger.DependsOn {
			if depID == descpb.InvalidID {
				return errors.Newf("invalid relation id %d in depends-on references #%d", depID, idx)
			}
			if seenIDs.Contains(depID) {
				return errors.Newf("relation id %d in depends-on references #%d is duplicated", depID, idx)
			}
			seenIDs.Add(depID)
		}
		for idx, typeID := range trigger.DependsOnTypes {
			if typeID == descpb.InvalidID {
				return errors.Newf("invalid type id %d in depends-on-types references #%d", typeID, idx)
			}
			if seenIDs.Contains(typeID) {
				return errors.Newf("relation id %d in depends-on-type references #%d is duplicated", typeID, idx)
			}
			seenIDs.Add(typeID)
		}
		for idx, routineID := range trigger.DependsOnRoutines {
			if routineID == descpb.InvalidID {
				return errors.Newf("invalid routine id %d in depends-on-routine references #%d", routineID, idx)
			}
			if seenIDs.Contains(routineID) {
				return errors.Newf("relation id %d in depends-on-routine references #%d is duplicated", routineID, idx)
			}
			seenIDs.Add(routineID)
		}
	}
	return nil
}

// validateCheckConstraints validates that check constraints are well formed.
// Checks include validating the column IDs and verifying that check expressions
// do not reference non-existent columns.
func (desc *wrapper) validateCheckConstraints(
	columnsByID map[descpb.ColumnID]catalog.Column,
) error {
	for _, chk := range desc.CheckConstraints() {
		// Verify that the check's column IDs are valid.
		for i, n := 0, chk.NumReferencedColumns(); i < n; i++ {
			colID := chk.GetReferencedColumnID(i)
			_, ok := columnsByID[colID]
			if !ok {
				return errors.Newf("check constraint %q contains unknown column \"%d\"", chk.GetName(), colID)
			}
		}

		// Verify that the check's expression is valid.
		expr, err := parser.ParseExpr(chk.GetExpr())
		if err != nil {
			return err
		}
		valid, err := schemaexpr.HasValidColumnReferences(desc, expr)
		if err != nil {
			return err
		}
		if !valid {
			return errors.Newf("check constraint %q refers to unknown columns in expression: %s",
				chk.GetName(), chk.GetExpr())
		}
	}
	return nil
}

// validateUniqueWithoutIndexConstraints validates that unique without index
// constraints are well formed. Checks include validating the column IDs and
// column names.
func (desc *wrapper) validateUniqueWithoutIndexConstraints(
	columnsByID map[descpb.ColumnID]catalog.Column,
) error {
	for _, c := range desc.UniqueConstraintsWithoutIndex() {
		if len(c.GetName()) == 0 {
			return pgerror.Newf(pgcode.Syntax, "empty unique without index constraint name")
		}

		// Verify that the table ID is valid.
		if c.ParentTableID() != desc.ID {
			return errors.Newf(
				"TableID mismatch for unique without index constraint %q: \"%d\" doesn't match descriptor: \"%d\"",
				c.GetName(), c.ParentTableID(), desc.ID,
			)
		}

		// Verify that the constraint's column IDs are valid and unique.
		var seen intsets.Fast
		for i, n := 0, c.NumKeyColumns(); i < n; i++ {
			colID := c.GetKeyColumnID(i)
			_, ok := columnsByID[colID]
			if !ok {
				return errors.Newf(
					"unique without index constraint %q contains unknown column \"%d\"", c.GetName(), colID,
				)
			}
			if seen.Contains(int(colID)) {
				return errors.Newf(
					"unique without index constraint %q contains duplicate column \"%d\"", c.GetName(), colID,
				)
			}
			seen.Add(int(colID))
		}

		if c.IsPartial() {
			expr, err := parser.ParseExpr(c.GetPredicate())
			if err != nil {
				return err
			}
			valid, err := schemaexpr.HasValidColumnReferences(desc, expr)
			if err != nil {
				return err
			}
			if !valid {
				return errors.Newf(
					"partial unique without index constraint %q refers to unknown columns in predicate: %s",
					c.GetName(),
					c.GetPredicate(),
				)
			}
		}
	}

	return nil
}

// validateTableIndexes validates that indexes are well formed. Checks include
// validating the columns involved in the index, verifying the index names and
// IDs are unique, and the family of the primary key is 0. This does not check
// if indexes are unique (i.e. same set of columns, direction, and uniqueness)
// as there are practical uses for them.
func (desc *wrapper) validateTableIndexes(
	columnsByID map[descpb.ColumnID]catalog.Column, isActive func(version clusterversion.Key) bool,
) error {
	if len(desc.PrimaryIndex.KeyColumnIDs) == 0 {
		return ErrMissingPrimaryKey
	}

	indexNames := map[string]struct{}{}
	indexIDs := map[descpb.IndexID]string{}
	for _, idx := range desc.NonDropIndexes() {
		if len(idx.GetName()) == 0 {
			return pgerror.Newf(pgcode.Syntax, "empty index name")
		}
		if idx.GetID() == 0 {
			return errors.Newf("invalid index ID %d", idx.GetID())
		}

		if idx.IndexDesc().ForeignKey.IsSet() || len(idx.IndexDesc().ReferencedBy) > 0 {
			return errors.AssertionFailedf("index %q contains deprecated foreign key representation", idx.GetName())
		}

		if len(idx.IndexDesc().Interleave.Ancestors) > 0 || len(idx.IndexDesc().InterleavedBy) > 0 {
			return errors.Newf("index is interleaved")
		}

		if (idx.GetInvisibility() == 0.0 && idx.IsNotVisible()) || (idx.GetInvisibility() != 0.0 && !idx.IsNotVisible()) {
			return errors.Newf("invisibility is incompatible with value for not_visible")
		}

		if _, indexNameExists := indexNames[idx.GetName()]; indexNameExists {
			for i := range desc.Indexes {
				if desc.Indexes[i].Name == idx.GetName() {
					// This error should be caught in MakeIndexDescriptor or NewTableDesc.
					return errors.HandleAsAssertionFailure(errors.Newf("duplicate index name: %q", idx.GetName()))
				}
			}
			// This error should be caught in MakeIndexDescriptor.
			return errors.HandleAsAssertionFailure(errors.Newf(
				"duplicate: index %q in the middle of being added, not yet public", idx.GetName()))
		}
		indexNames[idx.GetName()] = struct{}{}

		if other, ok := indexIDs[idx.GetID()]; ok {
			return errors.Newf("index %q duplicate ID of index %q: %d",
				idx.GetName(), other, idx.GetID())
		}
		indexIDs[idx.GetID()] = idx.GetName()

		if idx.GetID() >= desc.NextIndexID {
			return errors.Newf("index %q invalid index ID (%d) > next index ID (%d)",
				idx.GetName(), idx.GetID(), desc.NextIndexID)
		}

		if len(idx.IndexDesc().KeyColumnIDs) != len(idx.IndexDesc().KeyColumnNames) {
			return errors.Newf("mismatched column IDs (%d) and names (%d)",
				len(idx.IndexDesc().KeyColumnIDs), len(idx.IndexDesc().KeyColumnNames))
		}
		if len(idx.IndexDesc().KeyColumnIDs) != len(idx.IndexDesc().KeyColumnDirections) {
			return errors.Newf("mismatched column IDs (%d) and directions (%d)",
				len(idx.IndexDesc().KeyColumnIDs), len(idx.IndexDesc().KeyColumnDirections))
		}
		// In the old STORING encoding, stored columns are in ExtraColumnIDs;
		// tolerate a longer list of column names.
		if len(idx.IndexDesc().StoreColumnIDs) > len(idx.IndexDesc().StoreColumnNames) {
			return errors.Newf("mismatched STORING column IDs (%d) and names (%d)",
				len(idx.IndexDesc().StoreColumnIDs), len(idx.IndexDesc().StoreColumnNames))
		}

		if len(idx.IndexDesc().KeyColumnIDs) == 0 {
			return errors.Newf("index %q must contain at least 1 column", idx.GetName())
		}

		var validateIndexDup catalog.TableColSet
		for i, colID := range idx.IndexDesc().KeyColumnIDs {
			inIndexColName := idx.IndexDesc().KeyColumnNames[i]
			col, exists := columnsByID[colID]
			if !exists {
				return errors.Newf("index %q contains key column %q with unknown ID %d", idx.GetName(), inIndexColName, colID)
			}
			if col.GetName() != inIndexColName {
				return errors.Newf("index %q key column ID %d should have name %q, but found name %q",
					idx.GetName(), colID, col.ColName(), inIndexColName)
			}
			if col.Dropped() && idx.GetEncodingType() != catenumpb.PrimaryIndexEncoding {
				return errors.Newf("secondary index %q contains dropped key column %q", idx.GetName(), col.ColName())
			}
			if validateIndexDup.Contains(colID) {
				if col.IsExpressionIndexColumn() {
					return pgerror.Newf(pgcode.FeatureNotSupported,
						"index %q contains duplicate expression %q",
						idx.GetName(), col.GetComputeExpr(),
					)
				}
				return pgerror.Newf(pgcode.FeatureNotSupported,
					"index %q contains duplicate column %q",
					idx.GetName(), col.ColName(),
				)
			}
			validateIndexDup.Add(colID)
		}
		for _, colID := range idx.IndexDesc().KeySuffixColumnIDs {
			col, exists := columnsByID[colID]
			if !exists {
				return errors.Newf("index %q key suffix column ID %d is invalid",
					idx.GetName(), colID)
			}
			if idx.GetEncodingType() == catenumpb.PrimaryIndexEncoding {
				return errors.Newf("primary-encoded index %q unexpectedly contains key suffix columns, for instance %q",
					idx.GetName(), col.ColName())
			}
			if col.Dropped() {
				return errors.Newf("secondary index %q contains dropped key suffix column %q", idx.GetName(), col.ColName())
			}
		}
		for i, colID := range idx.IndexDesc().StoreColumnIDs {
			inIndexColName := idx.IndexDesc().StoreColumnNames[i]
			col, exists := columnsByID[colID]
			if !exists {
				return errors.Newf("index %q contains stored column %q with unknown ID %d", idx.GetName(), inIndexColName, colID)
			}
			if col.GetName() != inIndexColName {
				return errors.Newf("index %q stored column ID %d should have name %q, but found name %q",
					idx.GetName(), colID, col.ColName(), inIndexColName)
			}
			if col.Dropped() && idx.GetEncodingType() != catenumpb.PrimaryIndexEncoding {
				return errors.Newf("secondary index %q contains dropped stored column %q", idx.GetName(), col.ColName())
			}
			// Ensure any active index does not store a primary key column (added and gated in V24.1).
			if !idx.IsMutation() && catalog.MakeTableColSet(desc.PrimaryIndex.KeyColumnIDs...).Contains(colID) {
				return sqlerrors.NewColumnAlreadyExistsInIndexError(idx.GetName(), col.GetName())
			}
		}
		if idx.IsSharded() {
			if err := desc.ensureShardedIndexNotComputed(idx.IndexDesc()); err != nil {
				return err
			}
			if catalog.FindColumnByName(desc, idx.GetSharded().Name) == nil {
				return errors.Newf("index %q refers to non-existent shard column %q",
					idx.GetName(), idx.GetSharded().Name)
			}
		}
		if idx.IsPartial() {
			expr, err := parser.ParseExpr(idx.GetPredicate())
			if err != nil {
				return err
			}
			valid, err := schemaexpr.HasValidColumnReferences(desc, expr)
			if err != nil {
				return err
			}
			if !valid {
				return errors.Newf("partial index %q refers to unknown columns in predicate: %s",
					idx.GetName(), idx.GetPredicate())
			}
		}

		if !idx.IsMutation() {
			if idx.IndexDesc().UseDeletePreservingEncoding {
				return errors.Newf("public index %q is using the delete preserving encoding", idx.GetName())
			}
		}

		// Ensure that indexes do not STORE virtual columns as suffix columns unless
		// they are primary key columns or future primary key columns (when `ALTER
		// PRIMARY KEY` is executed and a primary key mutation exists).
		curPKColIDs := catalog.MakeTableColSet(desc.PrimaryIndex.KeyColumnIDs...)
		newPKColIDs := catalog.MakeTableColSet()
		for _, mut := range desc.Mutations {
			if mut.GetPrimaryKeySwap() != nil {
				newPKIdxID := mut.GetPrimaryKeySwap().NewPrimaryIndexId
				newPK, err := catalog.MustFindIndexByID(desc, newPKIdxID)
				if err != nil {
					return err
				}
				newPKColIDs.UnionWith(newPK.CollectKeyColumnIDs())
			}
		}
		if newPKColIDs.Empty() {
			// Sadly, if the `ALTER PRIMARY KEY USING HASH` is from declarative schema changer,
			// we won't find the `PrimaryKeySwap` mutation. In that case, we will attempt to
			// find a mutation of adding a primary index and allow its key columns to be used
			// as SUFFIX columns in other indexes, even if they are virtual.
			for _, mut := range desc.Mutations {
				if pidx := mut.GetIndex(); pidx != nil &&
					pidx.EncodingType == catenumpb.PrimaryIndexEncoding &&
					mut.Direction == descpb.DescriptorMutation_ADD &&
					!mut.Rollback {
					newPKColIDs.UnionWith(catalog.MakeTableColSet(pidx.KeyColumnIDs...))
				}
			}
		}
		for _, colID := range idx.IndexDesc().KeySuffixColumnIDs {
			// At this point the ID -> column mapping is known to be valid.
			col := columnsByID[colID]
			if !col.IsVirtual() {
				continue
			}

			// When newPKColIDs is empty, it means there's no `ALTER PRIMARY KEY` in
			// progress.
			if newPKColIDs.Len() == 0 && curPKColIDs.Contains(colID) {
				continue
			}

			// When newPKColIDs is not empty, it means there is an in-progress `ALTER
			// PRIMARY KEY`. We don't allow queueing schema changes when there's a
			// primary key mutation, so it's safe to make the assumption that `Adding`
			// indexes are associated with the new primary key because they are
			// rewritten and `Non-adding` indexes should only contain virtual column
			// from old primary key.
			isOldPKCol := !idx.Adding() && curPKColIDs.Contains(colID)
			isNewPKCol := idx.Adding() && newPKColIDs.Contains(colID)
			if newPKColIDs.Len() > 0 && (isOldPKCol || isNewPKCol) {
				continue
			}

			return errors.Newf("index %q cannot store virtual column %q", idx.GetName(), col.GetName())
		}

		// Ensure that indexes do not STORE virtual columns.
		for i, colID := range idx.IndexDesc().StoreColumnIDs {
			if col := columnsByID[colID]; col != nil && col.IsVirtual() {
				return errors.Newf("index %q cannot store virtual column %q",
					idx.GetName(), idx.IndexDesc().StoreColumnNames[i])
			}
		}
		if idx.Primary() {
			if idx.GetVersion() < descpb.PrimaryIndexWithStoredColumnsVersion {
				return errors.AssertionFailedf("primary index %q has invalid version %d, expected at least %d",
					idx.GetName(), idx.GetVersion(), descpb.PrimaryIndexWithStoredColumnsVersion)
			}
			if idx.IndexDesc().EncodingType != catenumpb.PrimaryIndexEncoding {
				return errors.AssertionFailedf("primary index %q has invalid encoding type %d in proto, expected %d",
					idx.GetName(), idx.IndexDesc().EncodingType, catenumpb.PrimaryIndexEncoding)
			}
			if idx.GetInvisibility() != 0.0 {
				return errors.Newf("primary index %q cannot be not visible", idx.GetName())
			}

			// Check that each non-virtual column is in the key or store columns of
			// the primary index.
			keyCols := idx.CollectKeyColumnIDs()
			storeCols := idx.CollectPrimaryStoredColumnIDs()
			for _, col := range desc.PublicColumns() {
				if col.IsVirtual() {
					if storeCols.Contains(col.GetID()) {
						return errors.Newf(
							"primary index %q store columns cannot contain virtual column ID %d",
							idx.GetName(), col.GetID(),
						)
					}
					// No need to check anything else for virtual columns.
					continue
				}
				if !keyCols.Contains(col.GetID()) && !storeCols.Contains(col.GetID()) {
					return errors.Newf(
						"primary index %q must contain column ID %d in either key or store columns",
						idx.GetName(), col.GetID(),
					)
				}
			}
		}
		if !idx.Primary() && len(desc.Mutations) == 0 && desc.DeclarativeSchemaChangerState == nil {
			if idx.IndexDesc().EncodingType != catenumpb.SecondaryIndexEncoding {
				return errors.AssertionFailedf("secondary index %q has invalid encoding type %d in proto, expected %d",
					idx.GetName(), idx.IndexDesc().EncodingType, catenumpb.SecondaryIndexEncoding)
			}
		}

		// Ensure that an index column ID shows up at most once in `keyColumnIDs`,
		// `keySuffixColumnIDs`, and `storeColumnIDs`.
		if idx.GetVersion() < descpb.StrictIndexColumnIDGuaranteesVersion {
			continue
		}
		slices := []struct {
			name  string
			slice []descpb.ColumnID
		}{
			{"KeyColumnIDs", idx.IndexDesc().KeyColumnIDs},
			{"KeySuffixColumnIDs", idx.IndexDesc().KeySuffixColumnIDs},
			{"StoreColumnIDs", idx.IndexDesc().StoreColumnIDs},
		}
		allIDs := catalog.MakeTableColSet()
		sets := map[string]catalog.TableColSet{}
		for _, s := range slices {
			set := catalog.MakeTableColSet(s.slice...)
			sets[s.name] = set
			if set.Len() == 0 {
				continue
			}
			if set.Ordered()[0] <= 0 {
				return errors.AssertionFailedf("index %q contains invalid column ID value %d in %s",
					idx.GetName(), set.Ordered()[0], s.name)
			}
			if set.Len() < len(s.slice) {
				return errors.AssertionFailedf("index %q has duplicates in %s: %v",
					idx.GetName(), s.name, s.slice)
			}
			allIDs.UnionWith(set)
		}
		foundIn := make([]string, 0, len(sets))
		for _, colID := range allIDs.Ordered() {
			foundIn = foundIn[:0]
			for _, s := range slices {
				set := sets[s.name]
				if set.Contains(colID) {
					foundIn = append(foundIn, s.name)
				}
			}
			if len(foundIn) > 1 {
				return errors.AssertionFailedf("index %q has column ID %d present in: %v",
					idx.GetName(), colID, foundIn)
			}
		}
	}
	return nil
}

// ensureShardedIndexNotComputed ensures that the sharded index is not based on a computed
// column. This is because the sharded index is based on a hidden computed shard column
// under the hood and we don't support transitively computed columns (computed column A
// based on another computed column B).
func (desc *wrapper) ensureShardedIndexNotComputed(index *descpb.IndexDescriptor) error {
	for _, colName := range index.Sharded.ColumnNames {
		col, err := catalog.MustFindColumnByName(desc, colName)
		if err != nil {
			return err
		}
		if col.IsComputed() {
			return pgerror.Newf(pgcode.InvalidTableDefinition,
				"cannot create a sharded index on a computed column")
		}
	}
	return nil
}

// validatePartitioningDescriptor validates that a PartitioningDescriptor, which
// may represent a subpartition, is well-formed. Checks include validating the
// index-level uniqueness of all partition names, validating that the encoded
// tuples match the corresponding column types, and that range partitions are
// stored sorted by upper bound. colOffset is non-zero for subpartitions and
// indicates how many index columns to skip over.
func (desc *wrapper) validatePartitioningDescriptor(
	a *tree.DatumAlloc,
	idx catalog.Index,
	part catalog.Partitioning,
	colOffset int,
	partitionNames map[string]string,
) error {
	if part.NumImplicitColumns() > part.NumColumns() {
		return errors.Newf(
			"cannot have implicit partitioning columns (%d) > partitioning columns (%d)",
			part.NumImplicitColumns(),
			part.NumColumns(),
		)
	}
	if part.NumColumns() == 0 {
		return nil
	}

	// We don't need real prefixes in the DecodePartitionTuple calls because we're
	// only using it to look for collisions and the prefix would be the same for
	// all of them. Faking them out with DNull allows us to make O(list partition)
	// calls to DecodePartitionTuple instead of O(list partition entry).
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	if part.NumLists() == 0 && part.NumRanges() == 0 {
		return pgerror.Newf(pgcode.InvalidObjectDefinition, "at least one of LIST or RANGE partitioning must be used")
	}
	if part.NumLists() > 0 && part.NumRanges() > 0 {
		return errors.Newf("only one LIST or RANGE partitioning may used")
	}

	// Do not validate partitions which use unhydrated user-defined types.
	// This should only happen at read time and descriptors should not become
	// invalid at read time, only at write time.
	{
		for i := colOffset; i < colOffset+part.NumColumns(); i++ {
			// The partitioning descriptor may be invalid and refer to columns
			// not stored in the index. In that case, skip this check as the
			// validation will fail later.
			if i >= idx.NumKeyColumns() {
				continue
			}
			col, err := catalog.MustFindColumnByID(desc, idx.GetKeyColumnID(i))
			if err != nil {
				return err
			}
			if col.GetType().UserDefined() && !col.GetType().IsHydrated() {
				return nil
			}
		}
	}

	checkName := func(name string) error {
		if len(name) == 0 {
			return errors.Newf("PARTITION name must be non-empty")
		}
		if indexName, exists := partitionNames[name]; exists {
			if indexName == idx.GetName() {
				return pgerror.Newf(pgcode.InvalidObjectDefinition, "PARTITION %s: name must be unique (used twice in index %q)",
					name, indexName)
			}
		}
		partitionNames[name] = idx.GetName()
		return nil
	}

	// Use the system-tenant SQL codec when validating the keys in the partition
	// descriptor. We just want to know how the partitions relate to one another,
	// so it's fine to ignore the tenant ID prefix.
	codec := keys.SystemSQLCodec

	if part.NumLists() > 0 {
		listValues := make(map[string]struct{}, part.NumLists())
		err := part.ForEachList(func(name string, values [][]byte, subPartitioning catalog.Partitioning) error {
			if err := checkName(name); err != nil {
				return err
			}

			if len(values) == 0 {
				return errors.Newf("PARTITION %s: must contain values", name)
			}
			// NB: key encoding is used to check uniqueness because it has
			// to match the behavior of the value when indexed.
			for _, valueEncBuf := range values {
				tuple, keyPrefix, err := rowenc.DecodePartitionTuple(
					a, codec, desc, idx, part, valueEncBuf, fakePrefixDatums)
				if err != nil {
					return errors.Wrapf(err, "PARTITION %s", name)
				}
				if _, exists := listValues[string(keyPrefix)]; exists {
					return errors.Newf("%s cannot be present in more than one partition", tuple)
				}
				listValues[string(keyPrefix)] = struct{}{}
			}

			newColOffset := colOffset + part.NumColumns()
			return desc.validatePartitioningDescriptor(
				a, idx, subPartitioning, newColOffset, partitionNames,
			)
		})
		if err != nil {
			return err
		}
	}

	if part.NumRanges() > 0 {
		tree := interval.NewTree(interval.ExclusiveOverlapper)
		err := part.ForEachRange(func(name string, from, to []byte) error {
			if err := checkName(name); err != nil {
				return err
			}

			// NB: key encoding is used to check uniqueness because it has to match
			// the behavior of the value when indexed.
			fromDatums, fromKey, err := rowenc.DecodePartitionTuple(
				a, codec, desc, idx, part, from, fakePrefixDatums)
			if err != nil {
				return errors.Wrapf(err, "PARTITION %s", name)
			}
			toDatums, toKey, err := rowenc.DecodePartitionTuple(
				a, codec, desc, idx, part, to, fakePrefixDatums)
			if err != nil {
				return errors.Wrapf(err, "PARTITION %s", name)
			}
			pi := partitionInterval{name, fromKey, toKey}
			if overlaps := tree.Get(pi.Range()); len(overlaps) > 0 {
				return errors.Newf("partitions %s and %s overlap",
					overlaps[0].(partitionInterval).name, name)
			}
			if err := tree.Insert(pi, false /* fast */); errors.Is(err, interval.ErrEmptyRange) {
				return errors.Newf("PARTITION %s: empty range: lower bound %s is equal to upper bound %s",
					name, fromDatums, toDatums)
			} else if errors.Is(err, interval.ErrInvertedRange) {
				return errors.Newf("PARTITION %s: empty range: lower bound %s is greater than upper bound %s",
					name, fromDatums, toDatums)
			} else if err != nil {
				return errors.Wrapf(err, "PARTITION %s", name)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

type partitionInterval struct {
	name  string
	start roachpb.Key
	end   roachpb.Key
}

var _ interval.Interface = partitionInterval{}

// ID is part of `interval.Interface` but unused in validatePartitioningDescriptor.
func (ps partitionInterval) ID() uintptr { return 0 }

// Range is part of `interval.Interface`.
func (ps partitionInterval) Range() interval.Range {
	return interval.Range{Start: []byte(ps.start), End: []byte(ps.end)}
}

// validatePartitioning validates that any PartitioningDescriptors contained in
// table indexes are well-formed. See validatePartitioningDesc for details.
func (desc *wrapper) validatePartitioning() error {
	partitionNames := make(map[string]string)

	a := &tree.DatumAlloc{}
	return catalog.ForEachNonDropIndex(desc, func(idx catalog.Index) error {
		return desc.validatePartitioningDescriptor(
			a, idx, idx.GetPartitioning(), 0 /* colOffset */, partitionNames,
		)
	})
}

func (desc *wrapper) validatePolicies() error {
	if !desc.IsTable() {
		return nil
	}
	policies := desc.GetPolicies()
	names := make(map[string]descpb.PolicyID, len(policies))
	idToName := make(map[descpb.PolicyID]string, len(policies))
	for _, p := range policies {
		if err := desc.validatePolicy(&p); err != nil {
			return err
		}

		// Perform validation across all policies defined for the table.
		if otherID, found := names[p.Name]; found && p.ID != otherID {
			return pgerror.Newf(pgcode.DuplicateObject,
				"duplicate policy name: %q", p.Name)
		}
		names[p.Name] = p.ID
		if other, found := idToName[p.ID]; found {
			return pgerror.Newf(pgcode.DuplicateObject,
				"policy ID %d in policy %q already in use by %q",
				p.ID, p.Name, other)
		}
		idToName[p.ID] = p.Name
	}
	return nil
}

// validatePolicy will validate a single policy in isolation from other policies in the table.
func (desc *wrapper) validatePolicy(p *descpb.PolicyDescriptor) error {
	if p.ID == 0 {
		return errors.AssertionFailedf(
			"policy ID was missing for policy %q",
			p.Name)
	} else if p.ID >= desc.NextPolicyID {
		return errors.AssertionFailedf(
			"policy %q has ID %d, which is not less than the NextPolicyID value %d for the table",
			p.Name, p.ID, desc.NextPolicyID)
	}
	if p.Name == "" {
		return pgerror.Newf(pgcode.Syntax, "empty policy name")
	}
	if _, ok := catpb.PolicyType_name[int32(p.Type)]; !ok || p.Type == catpb.PolicyType_POLICYTYPE_UNUSED {
		return errors.AssertionFailedf(
			"policy %q has an unknown policy type %v", p.Name, p.Type)
	}
	if _, ok := catpb.PolicyCommand_name[int32(p.Command)]; !ok || p.Command == catpb.PolicyCommand_POLICYCOMMAND_UNUSED {
		return errors.AssertionFailedf(
			"policy %q has an unknown policy command %v", p.Name, p.Command)
	}
	if err := desc.validatePolicyRoles(p); err != nil {
		return err
	}
	if err := desc.validatePolicyExprs(p); err != nil {
		return err
	}
	return nil
}

// validatePolicyRoles will validate the roles that are in one policy.
func (desc *wrapper) validatePolicyRoles(p *descpb.PolicyDescriptor) error {
	if len(p.RoleNames) == 0 {
		return errors.AssertionFailedf(
			"policy %q has no roles defined", p.Name)
	}
	rolesInUse := make(map[string]struct{}, len(p.RoleNames))
	for i, roleName := range p.RoleNames {
		if _, found := rolesInUse[roleName]; found {
			return errors.AssertionFailedf(
				"policy %q contains duplicate role name %q", p.Name, roleName)
		}
		rolesInUse[roleName] = struct{}{}
		// The public role, if included, must always be the first entry in the
		// role names slice.
		if roleName == username.PublicRole && i > 0 {
			return errors.AssertionFailedf(
				"the public role must be the first role defined in policy %q", p.Name)
		}
	}
	return nil
}

// validatePolicyExprs will validate the expressions within the policy.
func (desc *wrapper) validatePolicyExprs(p *descpb.PolicyDescriptor) error {
	if p.WithCheckExpr != "" {
		_, err := parser.ParseExpr(p.WithCheckExpr)
		if err != nil {
			return errors.Wrapf(err, "WITH CHECK expression %q is invalid", p.WithCheckExpr)
		}
	}
	if p.UsingExpr != "" {
		_, err := parser.ParseExpr(p.UsingExpr)
		if err != nil {
			return errors.Wrapf(err, "USING expression %q is invalid", p.UsingExpr)
		}
	}

	// Ensure the validity of policy back-references. The existence and status of
	// referenced objects are verified during the execution of
	// ValidateForwardReferences and ValidateBackwardReferences for the table.
	var seenIDs catalog.DescriptorIDSet
	for _, id := range []struct {
		idName string
		ids    []descpb.ID
	}{
		{"type", p.DependsOnTypes},
		{"functions", p.DependsOnFunctions},
		{"relations", p.DependsOnRelations},
	} {
		for idx, depID := range id.ids {
			if depID == descpb.InvalidID {
				return errors.Newf("invalid %s id %d in depends-on references #%d", id.idName, id, idx)
			}
			if seenIDs.Contains(depID) {
				return errors.Newf("%s id %d in depends-on references #%d is duplicated", id.idName, depID, idx)
			}
			seenIDs.Add(depID)
		}
	}
	return nil
}

// validateAutoStatsSettings validates that any new settings in
// catpb.AutoStatsSettings hold a valid value.
func (desc *wrapper) validateAutoStatsSettings(vea catalog.ValidationErrorAccumulator) {
	if desc.AutoStatsSettings == nil {
		return
	}
	desc.validateAutoStatsEnabled(vea, catpb.AutoStatsEnabledTableSettingName, desc.AutoStatsSettings.Enabled)
	desc.validateAutoStatsEnabled(vea, catpb.AutoPartialStatsEnabledTableSettingName, desc.AutoStatsSettings.PartialEnabled)

	desc.validateMinStaleRows(vea, catpb.AutoStatsMinStaleTableSettingName, desc.AutoStatsSettings.MinStaleRows)
	desc.validateMinStaleRows(vea, catpb.AutoPartialStatsMinStaleTableSettingName, desc.AutoStatsSettings.PartialMinStaleRows)

	desc.validateFractionStaleRows(vea, catpb.AutoStatsFractionStaleTableSettingName, desc.AutoStatsSettings.FractionStaleRows)
	desc.validateFractionStaleRows(vea, catpb.AutoPartialStatsFractionStaleTableSettingName, desc.AutoStatsSettings.PartialFractionStaleRows)
}

func (desc *wrapper) verifyProperTableForStatsSetting(
	vea catalog.ValidationErrorAccumulator, settingName string,
) {
	if desc.IsVirtualTable() {
		vea.Report(errors.Newf("Setting %s may not be set on virtual table", settingName))
	}
	if !desc.IsTable() {
		vea.Report(errors.Newf("Setting %s may not be set on a view or sequence", settingName))
	}
}

func (desc *wrapper) validateAutoStatsEnabled(
	vea catalog.ValidationErrorAccumulator, settingName string, value *bool,
) {
	if value != nil {
		desc.verifyProperTableForStatsSetting(vea, settingName)
	}
}

func (desc *wrapper) validateMinStaleRows(
	vea catalog.ValidationErrorAccumulator, settingName string, value *int64,
) {
	if value != nil {
		desc.verifyProperTableForStatsSetting(vea, settingName)
		if *value < 0 {
			vea.Report(errors.Newf("invalid integer value for %s: cannot be set to a negative value: %d", settingName, *value))
		}
	}
}

func (desc *wrapper) validateFractionStaleRows(
	vea catalog.ValidationErrorAccumulator, settingName string, value *float64,
) {
	if value != nil {
		desc.verifyProperTableForStatsSetting(vea, settingName)
		if *value < 0 {
			vea.Report(errors.Newf("invalid float value for %s: cannot set to a negative value: %f", settingName, *value))
		}
	}
}
