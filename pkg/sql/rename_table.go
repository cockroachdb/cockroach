// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/descmetadata"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type renameTableNode struct {
	zeroInputPlanNode
	n            *tree.RenameTable
	oldTn, newTn *tree.TableName
	tableDesc    *tabledesc.Mutable
}

// RenameTable renames the table, view or sequence.
// Privileges: DROP on source table/view/sequence, CREATE on destination database.
//
//	Notes: postgres requires the table owner.
//	       mysql requires ALTER, DROP on the original table, and CREATE, INSERT
//	       on the new table (and does not copy privileges over).
func (p *planner) RenameTable(ctx context.Context, n *tree.RenameTable) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"RENAME TABLE/VIEW/SEQUENCE",
	); err != nil {
		return nil, err
	}

	oldTn := n.Name.ToTableName()
	newTn := n.NewName.ToTableName()
	toRequire := tree.ResolveRequireTableOrViewDesc
	if n.IsView {
		toRequire = tree.ResolveRequireViewDesc
	} else if n.IsSequence {
		toRequire = tree.ResolveRequireSequenceDesc
	}

	_, tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &oldTn, !n.IfExists, toRequire)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		// Noop.
		return newZeroNode(nil /* columns */), nil
	}

	if err := checkViewMatchesMaterialized(tableDesc, n.IsView, n.IsMaterialized); err != nil {
		return nil, err
	}

	if tableDesc.State != descpb.DescriptorState_PUBLIC {
		return nil, sqlerrors.NewUndefinedRelationError(&oldTn)
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.DROP); err != nil {
		return nil, err
	}

	// Check if any objects depend on this table/view/sequence via its name.
	// If so, then we disallow renaming, otherwise we allow it.
	for _, dependent := range tableDesc.DependedOnBy {
		if !dependent.ByID {
			return nil, p.dependentError(
				ctx, string(tableDesc.DescriptorType()), oldTn.String(),
				tableDesc.ParentID, dependent.ID, "rename",
			)
		}
	}

	// Disallow schema changes if this table's schema is locked.
	if err := checkSchemaChangeIsAllowed(tableDesc, n); err != nil {
		return nil, err
	}

	return &renameTableNode{n: n, oldTn: &oldTn, newTn: &newTn, tableDesc: tableDesc}, nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because RENAME DATABASE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *renameTableNode) ReadingOwnWrites() {}

func (n *renameTableNode) startExec(params runParams) error {
	p := params.p
	ctx := params.ctx
	tableDesc := n.tableDesc
	oldTn := n.oldTn
	oldNameKey := descpb.NameInfo{
		ParentID:       tableDesc.GetParentID(),
		ParentSchemaID: tableDesc.GetParentSchemaID(),
		Name:           tableDesc.GetName(),
	}

	var targetDbDesc catalog.DatabaseDescriptor
	var targetSchemaDesc catalog.SchemaDescriptor
	// If the target new name has no qualifications, then assume that the table
	// is intended to be renamed into the same database and schema.
	newTn := n.newTn
	if !newTn.ExplicitSchema && !newTn.ExplicitCatalog {
		newTn.ObjectNamePrefix = oldTn.ObjectNamePrefix
		var err error
		targetDbDesc, err = p.Descriptors().ByNameWithLeased(p.txn).Get().Database(ctx, string(oldTn.CatalogName))
		if err != nil {
			return err
		}
		targetSchemaDesc, err = p.Descriptors().ByName(p.txn).Get().Schema(ctx, targetDbDesc, oldTn.Schema())
		if err != nil {
			return err
		}
	} else {
		// Otherwise, resolve the new qualified name of the table. We are in the
		// process of deprecating qualified rename targets, so issue a notice.
		// TODO (rohany): Convert this to take in an unqualified name after 20.2
		//  is released (#51445).
		params.p.BufferClientNotice(
			ctx,
			errors.WithHintf(
				pgnotice.Newf("renaming tables with a qualification is deprecated"),
				"use ALTER TABLE %s RENAME TO %s instead",
				n.n.Name.String(),
				newTn.Table(),
			),
		)

		newUn := newTn.ToUnresolvedObjectName()
		var prefix tree.ObjectNamePrefix
		var err error
		targetDbDesc, targetSchemaDesc, prefix, err = p.ResolveTargetObject(ctx, newUn)
		if err != nil {
			return err
		}
		newTn.ObjectNamePrefix = prefix
	}

	if err := p.CheckPrivilege(ctx, targetDbDesc, privilege.CREATE); err != nil {
		return err
	}

	// Disable renaming objects between schemas of the same database.
	if newTn.Catalog() == oldTn.Catalog() && newTn.Schema() != oldTn.Schema() {
		return errors.WithHint(
			pgerror.Newf(pgcode.InvalidName, "cannot change schema of table with RENAME"),
			"use ALTER TABLE ... SET SCHEMA instead",
		)
	}

	// Ensure tables cannot be moved cross-database.
	if oldTn.Catalog() != newTn.Catalog() {
		// The public schema is expected to always be present in the database for 22.2+.
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"cannot change database of table using alter table rename to")
	}

	// Special checks for tables, view and sequences to determine if cross
	// DB references would occur.
	if oldTn.Catalog() != newTn.Catalog() {
		err := n.checkForCrossDbReferences(ctx, p, targetDbDesc)
		if err != nil {
			return err
		}
	}

	// oldTn and newTn are already normalized, so we can compare directly here.
	if oldTn.Catalog() == newTn.Catalog() &&
		oldTn.Schema() == newTn.Schema() &&
		oldTn.Table() == newTn.Table() {
		// Noop.
		return nil
	}

	err := descs.CheckObjectNameCollision(
		params.ctx,
		p.Descriptors(),
		params.p.txn,
		targetDbDesc.GetID(),
		targetSchemaDesc.GetID(),
		newTn,
	)
	if err != nil {
		return err
	}

	// The parent schema ID is never modified here because changing the schema of
	// a table within the same database is disallowed, and changing the database
	// of a table is only allowed if both the source and target schemas are the
	// public schema.
	tableDesc.SetName(newTn.Table())
	tableDesc.ParentID = targetDbDesc.GetID()

	if err := validateDescriptor(ctx, p, tableDesc); err != nil {
		return err
	}

	// Populate namespace update batch.
	b := p.txn.NewBatch()
	if err := p.renameNamespaceEntry(ctx, b, oldNameKey, tableDesc); err != nil {
		return err
	}

	// Write the updated table descriptor.
	if err := p.writeSchemaChange(
		ctx, tableDesc, descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Run the namespace update batch.
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	metadataUpdater := descmetadata.NewMetadataUpdater(
		ctx,
		p.InternalSQLTxn(),
		p.Descriptors(),
		&p.ExecCfg().Settings.SV,
		p.SessionData(),
	)

	// If this table has row level ttl enabled, update the schedule_name of all
	// row-level-ttl jobs with the name table name. If row level TTL is not
	// enabled, UpdateTTLScheduleName will no-op for us.
	if err := metadataUpdater.UpdateTTLScheduleLabel(ctx, tableDesc); err != nil {
		return err
	}

	// Log Rename Table event. This is an auditable log event and is recorded
	// in the same transaction as the table descriptor update.
	return p.logEvent(ctx,
		tableDesc.ID,
		&eventpb.RenameTable{
			TableName:    oldTn.FQString(),
			NewTableName: newTn.FQString(),
		})
}

func (n *renameTableNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameTableNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameTableNode) Close(context.Context)        {}

// TODO(a-robinson): Support renaming objects depended on by views once we have
// a better encoding for view queries (#10083).
func (p *planner) dependentError(
	ctx context.Context, typeName string, objName string, parentID descpb.ID, id descpb.ID, op string,
) error {
	desc, err := p.Descriptors().ByIDWithLeased(p.txn).WithoutNonPublic().Get().Desc(ctx, id)
	if err != nil {
		return err
	}
	switch desc.DescriptorType() {
	case catalog.Table:
		return p.dependentViewError(ctx, typeName, objName, parentID, desc.(catalog.TableDescriptor), op)
	case catalog.Function:
		return p.dependentFunctionError(typeName, objName, desc.(catalog.FunctionDescriptor), op)
	default:
		return errors.AssertionFailedf(
			"unexpected dependent %s %s on %s %s",
			desc.DescriptorType(), desc.GetName(), typeName, objName,
		)
	}
}

func (p *planner) dependentFunctionError(
	typeName, objName string, fnDesc catalog.FunctionDescriptor, op string,
) error {
	return sqlerrors.NewDependentBlocksOpError(op, typeName, objName, "function", fnDesc.GetName())
}

func (p *planner) dependentViewError(
	ctx context.Context,
	typeName, objName string,
	parentID descpb.ID,
	viewDesc catalog.TableDescriptor,
	op string,
) error {
	viewName := viewDesc.GetName()
	if viewDesc.GetParentID() != parentID {
		viewFQName, err := p.getQualifiedTableName(ctx, viewDesc)
		if err != nil {
			log.Warningf(ctx, "unable to retrieve name of view %d: %v", viewDesc.GetID(), err)
			return sqlerrors.NewDependentObjectErrorf(
				"cannot %s %s %q because a view depends on it",
				op, typeName, objName)
		}
		viewName = viewFQName.FQString()
	}
	return sqlerrors.NewDependentBlocksOpError(op, typeName, objName, "view", viewName)
}

// checkForCrossDbReferences validates if any cross DB references
// will exist after any rename operation.
func (n *renameTableNode) checkForCrossDbReferences(
	ctx context.Context, p *planner, targetDbDesc catalog.DatabaseDescriptor,
) error {
	tableDesc := n.tableDesc

	// Checks inbound / outbound foreign key references for cross DB references.
	// The refTableID flag determines if the reference or origin field are checked.
	checkFkForCrossDbDep := func(fk catalog.ForeignKeyConstraint, refTableID bool) error {
		if allowCrossDatabaseFKs.Get(&p.execCfg.Settings.SV) {
			return nil
		}
		tableID := fk.GetReferencedTableID()
		if !refTableID {
			tableID = fk.GetOriginTableID()
		}

		referencedTable, err := p.Descriptors().ByIDWithoutLeased(p.txn).WithoutNonPublic().Get().Table(ctx, tableID)
		if err != nil {
			return err
		}
		// No cross DB reference
		if referencedTable.GetParentID() == targetDbDesc.GetID() {
			return nil
		}

		return errors.WithHint(
			pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"a foreign key constraint %q will exist between databases after rename "+
					"(see the '%s' cluster setting)",
				fk.GetName(),
				allowCrossDatabaseFKsSetting),
			crossDBReferenceDeprecationHint(),
		)
	}
	// Validates if a given dependency on a relation will
	// lead to a cross DB reference, and an appropriate
	// error is generated.
	type crossDBDepType int
	const owner, reference crossDBDepType = 0, 1
	checkDepForCrossDbRef := func(depID descpb.ID, depType crossDBDepType) error {
		dependentObject, err := p.Descriptors().ByIDWithoutLeased(p.txn).WithoutNonPublic().Get().Table(ctx, depID)
		if err != nil {
			return err
		}
		// No cross DB reference detected
		if dependentObject.GetParentID() == targetDbDesc.GetID() {
			return nil
		}
		// Based in the primary object.
		switch {
		case tableDesc.IsTable():
			// Based on the dependent objects type, since
			// for tables the type of the dependent object will
			// determine the message.
			switch {
			case dependentObject.IsView():
				if !allowCrossDatabaseViews.Get(&p.execCfg.Settings.SV) {
					return errors.WithHint(
						pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
							"a view %q reference to this table will refer to another databases after rename "+
								"(see the '%s' cluster setting)",
							dependentObject.GetName(),
							allowCrossDatabaseViewsSetting),
						crossDBReferenceDeprecationHint(),
					)
				}
			case dependentObject.IsSequence() && depType == owner:
				if !allowCrossDatabaseSeqOwner.Get(&p.execCfg.Settings.SV) {
					return errors.WithHint(
						pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
							"a sequence %q will be OWNED BY a table in a different database after rename "+
								"(see the '%s' cluster setting)",
							dependentObject.GetName(),
							allowCrossDatabaseSeqOwnerSetting),
						crossDBReferenceDeprecationHint(),
					)
				}
			case dependentObject.IsSequence() && depType == reference:
				if !allowCrossDatabaseSeqReferences.Get(&p.execCfg.Settings.SV) {
					return errors.WithHint(
						pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
							"a sequence %q will be referenced by a table in a different database after rename "+
								"(see the '%s' cluster setting)",
							dependentObject.GetName(),
							allowCrossDatabaseSeqOwnerSetting),
						crossDBReferenceDeprecationHint(),
					)
				}
			}
		case tableDesc.IsView():
			if !allowCrossDatabaseViews.Get(&p.execCfg.Settings.SV) {
				// For view's dependent objects can only be
				// relations.
				return errors.WithHint(
					pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"this view will reference a table %q in another databases after rename "+
							"(see the '%s' cluster setting)",
						dependentObject.GetName(),
						allowCrossDatabaseViewsSetting),
					crossDBReferenceDeprecationHint(),
				)
			}
		case tableDesc.IsSequence() && depType == reference:
			if !allowCrossDatabaseSeqReferences.Get(&p.execCfg.Settings.SV) {
				// For sequences dependent references can only be
				// a relations.
				return errors.WithHint(
					pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"this sequence will be referenced by a table %q in a different database after rename "+
							"(see the '%s' cluster setting)",
						dependentObject.GetName(),
						allowCrossDatabaseSeqReferencesSetting),
					crossDBReferenceDeprecationHint(),
				)
			}
		case tableDesc.IsSequence() && depType == owner:
			if !allowCrossDatabaseSeqOwner.Get(&p.execCfg.Settings.SV) {
				// For sequences dependent owners can only be
				// a relations.
				return errors.WithHint(
					pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"this sequence will be OWNED BY a table %q in a different database after rename "+
							"(see the '%s' cluster setting)",
						dependentObject.GetName(),
						allowCrossDatabaseSeqReferencesSetting),
					crossDBReferenceDeprecationHint(),
				)
			}
		}
		return nil
	}

	checkTypeDepForCrossDbRef := func(depID descpb.ID) error {
		if allowCrossDatabaseViews.Get(&p.execCfg.Settings.SV) {
			return nil
		}
		dependentObject, err := p.Descriptors().ByIDWithoutLeased(p.txn).WithoutNonPublic().Get().Type(ctx, depID)
		if err != nil {
			return err
		}
		// No cross DB reference detected
		if dependentObject.GetParentID() == targetDbDesc.GetID() {
			return nil
		}
		return errors.WithHint(
			pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"this view will reference a type %q in another databases after rename "+
					"(see the '%s' cluster setting)",
				dependentObject.GetName(),
				allowCrossDatabaseViewsSetting),
			crossDBReferenceDeprecationHint(),
		)
	}

	// For tables check if any outbound or inbound foreign key references would
	// be impacted.
	if tableDesc.IsTable() {
		for _, fk := range tableDesc.OutboundForeignKeys() {
			if err := checkFkForCrossDbDep(fk, false); err != nil {
				return err
			}
		}
		for _, fk := range tableDesc.InboundForeignKeys() {
			if err := checkFkForCrossDbDep(fk, false); err != nil {
				return err
			}
		}
		// If cross database sequence owners are not allowed, then
		// check if any column owns a sequence.
		for _, columnDesc := range tableDesc.Columns {
			for _, ownsSequenceID := range columnDesc.OwnsSequenceIds {
				if err := checkDepForCrossDbRef(ownsSequenceID, owner); err != nil {
					return err
				}
			}
			for _, seqID := range columnDesc.UsesSequenceIds {
				if err := checkDepForCrossDbRef(seqID, reference); err != nil {
					return err
				}
			}
		}
		// Check if any views depend on this table, while
		// DependsOnBy contains sequences these are only
		// once that are in use.
		if !allowCrossDatabaseViews.Get(&p.execCfg.Settings.SV) {
			err := tableDesc.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
				return checkDepForCrossDbRef(dep.ID, reference)
			})
			if err != nil {
				return err
			}
		}
	} else if tableDesc.IsView() {
		// For views check if we depend on tables in a different database.
		dependsOn := tableDesc.GetDependsOn()
		for _, dependency := range dependsOn {
			if err := checkDepForCrossDbRef(dependency, reference); err != nil {
				return err
			}
		}
		// Check if we depend on types in a different database.
		dependsOnTypes := tableDesc.GetDependsOnTypes()
		for _, dependency := range dependsOnTypes {
			if err := checkTypeDepForCrossDbRef(dependency); err != nil {
				return err
			}
		}
	} else if tableDesc.IsSequence() {
		// Check if the sequence is owned by a different database.
		sequenceOpts := tableDesc.GetSequenceOpts()
		if sequenceOpts.SequenceOwner.OwnerTableID != descpb.InvalidID {
			err := checkDepForCrossDbRef(sequenceOpts.SequenceOwner.OwnerTableID, owner)
			if err != nil {
				return err
			}
		}
		// Check if a table in a different database depends on this
		// sequence.
		for _, sequenceReferences := range tableDesc.GetDependedOnBy() {
			err := checkDepForCrossDbRef(sequenceReferences.ID, reference)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
