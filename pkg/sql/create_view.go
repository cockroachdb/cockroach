// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

// createViewNode represents a CREATE VIEW statement.
type createViewNode struct {
	// viewName is the fully qualified name of the new view.
	viewName *tree.TableName
	// viewQuery contains the view definition, with all table names fully
	// qualified.
	viewQuery    string
	ifNotExists  bool
	replace      bool
	persistence  tree.Persistence
	materialized bool
	dbDesc       *dbdesc.Immutable
	columns      colinfo.ResultColumns

	// planDeps tracks which tables and views the view being created
	// depends on. This is collected during the construction of
	// the view query's logical plan.
	planDeps planDependencies
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because CREATE VIEW performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *createViewNode) ReadingOwnWrites() {}

func (n *createViewNode) startExec(params runParams) error {
	tableType := tree.GetTableType(
		false /* isSequence */, true /* isView */, n.materialized,
	)
	if n.replace {
		telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter(fmt.Sprintf("or_replace_%s", tableType)))
	} else {
		telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter(tableType))
	}

	viewName := n.viewName.Object()
	persistence := n.persistence
	log.VEventf(params.ctx, 2, "dependencies for view %s:\n%s", viewName, n.planDeps.String())

	// Check that the view does not contain references to other databases.
	if !allowCrossDatabaseViews.Get(&params.p.execCfg.Settings.SV) {
		for _, dep := range n.planDeps {
			if dbID := dep.desc.ParentID; dbID != n.dbDesc.ID && dbID != keys.SystemDatabaseID {
				return pgerror.Newf(pgcode.FeatureNotSupported,
					"the view cannot refer to other databases; (see the '%s' cluster setting)",
					allowCrossDatabaseViewsSetting,
				)
			}
		}
	}

	// First check the backrefs and see if any of them are temporary.
	// If so, promote this view to temporary.
	backRefMutables := make(map[descpb.ID]*tabledesc.Mutable, len(n.planDeps))
	for id, updated := range n.planDeps {
		backRefMutable := params.p.Descriptors().GetUncommittedTableByID(id)
		if backRefMutable == nil {
			backRefMutable = tabledesc.NewExistingMutable(*updated.desc.TableDesc())
		}
		if !persistence.IsTemporary() && backRefMutable.Temporary {
			// This notice is sent from pg, let's imitate.
			params.p.BufferClientNotice(
				params.ctx,
				pgnotice.Newf(`view "%s" will be a temporary view`, viewName),
			)
			persistence = tree.PersistenceTemporary
		}
		backRefMutables[id] = backRefMutable
	}

	var replacingDesc *tabledesc.Mutable

	tKey, schemaID, err := getTableCreateParams(params, n.dbDesc.GetID(), persistence, n.viewName)
	if err != nil {
		switch {
		case !sqlerrors.IsRelationAlreadyExistsError(err):
			return err
		case n.ifNotExists:
			return nil
		case n.replace:
			// If we are replacing an existing view see if what we are
			// replacing is actually a view.
			id, err := catalogkv.GetDescriptorID(params.ctx, params.p.txn, params.ExecCfg().Codec, tKey)
			if err != nil {
				return err
			}
			desc, err := params.p.Descriptors().GetMutableTableVersionByID(params.ctx, id, params.p.txn)
			if err != nil {
				return err
			}
			if err := params.p.CheckPrivilege(params.ctx, desc, privilege.DROP); err != nil {
				return err
			}
			if !desc.IsView() {
				return pgerror.Newf(pgcode.WrongObjectType, `%q is not a view`, viewName)
			}
			replacingDesc = desc
		default:
			return err
		}
	}

	if n.persistence.IsTemporary() {
		telemetry.Inc(sqltelemetry.CreateTempViewCounter)
	}

	privs := CreateInheritedPrivilegesFromDBDesc(n.dbDesc, params.SessionData().User())

	var newDesc *tabledesc.Mutable

	// If replacingDesc != nil, we found an existing view while resolving
	// the name for our view. So instead of creating a new view, replace
	// the existing one.
	if replacingDesc != nil {
		newDesc, err = params.p.replaceViewDesc(params.ctx, n, replacingDesc, backRefMutables)
		if err != nil {
			return err
		}
	} else {
		// If we aren't replacing anything, make a new table descriptor.
		id, err := catalogkv.GenerateUniqueDescID(params.ctx, params.p.ExecCfg().DB, params.p.ExecCfg().Codec)
		if err != nil {
			return err
		}
		// creationTime is initialized to a zero value and populated at read time.
		// See the comment in desc.MaybeIncrementVersion.
		//
		// TODO(ajwerner): remove the timestamp from MakeViewTableDesc, it's
		// currently relied on in import and restore code and tests.
		var creationTime hlc.Timestamp
		desc, err := makeViewTableDesc(
			params.ctx,
			viewName,
			n.viewQuery,
			n.dbDesc.GetID(),
			schemaID,
			id,
			n.columns,
			creationTime,
			privs,
			&params.p.semaCtx,
			params.p.EvalContext(),
			n.persistence,
		)
		if err != nil {
			return err
		}

		if n.materialized {
			// Ensure all nodes are the correct version.
			if !params.ExecCfg().Settings.Version.IsActive(params.ctx, clusterversion.MaterializedViews) {
				return pgerror.New(pgcode.FeatureNotSupported,
					"all nodes are not the correct version to use materialized views")
			}
			// If the view is materialized, set up some more state on the view descriptor.
			// In particular,
			// * mark the descriptor as a materialized view
			// * mark the state as adding and remember the AsOf time to perform
			//   the view query
			// * use AllocateIDs to give the view descriptor a primary key
			desc.IsMaterializedView = true
			desc.State = descpb.DescriptorState_ADD
			desc.CreateAsOfTime = params.p.Txn().ReadTimestamp()
			if err := desc.AllocateIDs(params.ctx); err != nil {
				return err
			}
		}

		// Collect all the tables/views this view depends on.
		for backrefID := range n.planDeps {
			desc.DependsOn = append(desc.DependsOn, backrefID)
		}

		// TODO (lucy): I think this needs a NodeFormatter implementation. For now,
		// do some basic string formatting (not accurate in the general case).
		if err = params.p.createDescriptorWithID(
			params.ctx, tKey.Key(params.ExecCfg().Codec), id, &desc, params.EvalContext().Settings,
			fmt.Sprintf("CREATE VIEW %q AS %q", n.viewName, n.viewQuery),
		); err != nil {
			return err
		}
		newDesc = &desc
	}

	// Persist the back-references in all referenced table descriptors.
	for id, updated := range n.planDeps {
		backRefMutable := backRefMutables[id]
		// In case that we are replacing a view that already depends on
		// this table, remove all existing references so that we don't leave
		// any out of date references. Then, add the new references.
		backRefMutable.DependedOnBy = removeMatchingReferences(
			backRefMutable.DependedOnBy,
			newDesc.ID,
		)
		for _, dep := range updated.deps {
			// The logical plan constructor merely registered the dependencies.
			// It did not populate the "ID" field of TableDescriptor_Reference,
			// because the ID of the newly created view descriptor was not
			// yet known.
			// We need to do it here.
			dep.ID = newDesc.ID
			backRefMutable.DependedOnBy = append(backRefMutable.DependedOnBy, dep)
		}
		if err := params.p.writeSchemaChange(
			params.ctx,
			backRefMutable,
			descpb.InvalidMutationID,
			fmt.Sprintf("updating view reference %q in table %s(%d)", n.viewName,
				updated.desc.Name, updated.desc.ID,
			),
		); err != nil {
			return err
		}
	}

	// Install back references to types used by this view.
	if err := params.p.addBackRefsFromAllTypesInTable(params.ctx, newDesc); err != nil {
		return err
	}

	dg := catalogkv.NewOneLevelUncachedDescGetter(params.p.txn, params.ExecCfg().Codec)
	if err := newDesc.Validate(params.ctx, dg); err != nil {
		return err
	}

	// Log Create View event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	return params.p.logEvent(params.ctx,
		newDesc.ID,
		&eventpb.CreateView{
			ViewName:  n.viewName.FQString(),
			ViewQuery: n.viewQuery,
		})
}

func (*createViewNode) Next(runParams) (bool, error) { return false, nil }
func (*createViewNode) Values() tree.Datums          { return tree.Datums{} }
func (n *createViewNode) Close(ctx context.Context)  {}

// makeViewTableDesc returns the table descriptor for a new view.
//
// It creates the descriptor directly in the PUBLIC state rather than
// the ADDING state because back-references are added to the view's
// dependencies in the same transaction that the view is created and it
// doesn't matter if reads/writes use a cached descriptor that doesn't
// include the back-references.
func makeViewTableDesc(
	ctx context.Context,
	viewName string,
	viewQuery string,
	parentID descpb.ID,
	schemaID descpb.ID,
	id descpb.ID,
	resultColumns []colinfo.ResultColumn,
	creationTime hlc.Timestamp,
	privileges *descpb.PrivilegeDescriptor,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	persistence tree.Persistence,
) (tabledesc.Mutable, error) {
	desc := tabledesc.InitTableDescriptor(
		id,
		parentID,
		schemaID,
		viewName,
		creationTime,
		privileges,
		persistence,
	)
	desc.ViewQuery = viewQuery
	if err := addResultColumns(ctx, semaCtx, evalCtx, &desc, resultColumns); err != nil {
		return tabledesc.Mutable{}, err
	}

	return desc, nil
}

// replaceViewDesc modifies and returns the input view descriptor changed
// to hold the new view represented by n. Note that back references from
// tables that the new view depends on still need to be added. This function
// will additionally drop backreferences from tables the old view depended
// on that the new view no longer depends on.
func (p *planner) replaceViewDesc(
	ctx context.Context,
	n *createViewNode,
	toReplace *tabledesc.Mutable,
	backRefMutables map[descpb.ID]*tabledesc.Mutable,
) (*tabledesc.Mutable, error) {
	// Set the query to the new query.
	toReplace.ViewQuery = n.viewQuery
	// Reset the columns to add the new result columns onto.
	toReplace.Columns = make([]descpb.ColumnDescriptor, 0, len(n.columns))
	toReplace.NextColumnID = 0
	if err := addResultColumns(ctx, &p.semaCtx, p.EvalContext(), toReplace, n.columns); err != nil {
		return nil, err
	}

	// Compare toReplace against its ClusterVersion to verify if
	// its new set of columns is valid for a replacement view.
	if err := verifyReplacingViewColumns(
		toReplace.ClusterVersion.Columns,
		toReplace.Columns,
	); err != nil {
		return nil, err
	}

	// Remove the back reference from all tables that the view depended on.
	for _, id := range toReplace.DependsOn {
		desc, ok := backRefMutables[id]
		if !ok {
			var err error
			desc, err = p.Descriptors().GetMutableTableVersionByID(ctx, id, p.txn)
			if err != nil {
				return nil, err
			}
			backRefMutables[id] = desc
		}

		// If n.planDeps doesn't contain id, then the new view definition doesn't
		// reference this table anymore, so we can remove all existing references.
		if _, ok := n.planDeps[id]; !ok {
			desc.DependedOnBy = removeMatchingReferences(desc.DependedOnBy, toReplace.ID)
			if err := p.writeSchemaChange(
				ctx,
				desc,
				descpb.InvalidMutationID,
				fmt.Sprintf("removing view reference for %q from %s(%d)", n.viewName,
					desc.Name, desc.ID,
				),
			); err != nil {
				return nil, err
			}
		}
	}

	// Since the view query has been replaced, the dependencies that this
	// table descriptor had are gone.
	toReplace.DependsOn = make([]descpb.ID, 0, len(n.planDeps))
	for backrefID := range n.planDeps {
		toReplace.DependsOn = append(toReplace.DependsOn, backrefID)
	}

	// Since we are replacing an existing view here, we need to write the new
	// descriptor into place.
	if err := p.writeSchemaChange(ctx, toReplace, descpb.InvalidMutationID,
		fmt.Sprintf("CREATE OR REPLACE VIEW %q AS %q", n.viewName, n.viewQuery),
	); err != nil {
		return nil, err
	}
	return toReplace, nil
}

// addResultColumns adds the resultColumns as actual column
// descriptors onto desc.
func addResultColumns(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	desc *tabledesc.Mutable,
	resultColumns colinfo.ResultColumns,
) error {
	for _, colRes := range resultColumns {
		columnTableDef := tree.ColumnTableDef{Name: tree.Name(colRes.Name), Type: colRes.Typ}
		// Nullability constraints do not need to exist on the view, since they are
		// already enforced on the source data.
		columnTableDef.Nullable.Nullability = tree.SilentNull
		// The new types in the CREATE VIEW column specs never use
		// SERIAL so we need not process SERIAL types here.
		col, _, _, err := tabledesc.MakeColumnDefDescs(ctx, &columnTableDef, semaCtx, evalCtx)
		if err != nil {
			return err
		}
		desc.AddColumn(col)
	}
	if err := desc.AllocateIDs(ctx); err != nil {
		return err
	}
	return nil
}

// verifyReplacingViewColumns ensures that the new set of view columns must
// have at least the same prefix of columns as the old view. We attempt to
// match the postgres error message in each of the error cases below.
func verifyReplacingViewColumns(oldColumns, newColumns []descpb.ColumnDescriptor) error {
	if len(newColumns) < len(oldColumns) {
		return pgerror.Newf(pgcode.InvalidTableDefinition, "cannot drop columns from view")
	}
	for i := range oldColumns {
		oldCol, newCol := &oldColumns[i], &newColumns[i]
		if oldCol.Name != newCol.Name {
			return pgerror.Newf(
				pgcode.InvalidTableDefinition,
				`cannot change name of view column %q to %q`,
				oldCol.Name,
				newCol.Name,
			)
		}
		if !newCol.Type.Identical(oldCol.Type) {
			return pgerror.Newf(
				pgcode.InvalidTableDefinition,
				`cannot change type of view column %q from %s to %s`,
				oldCol.Name,
				oldCol.Type.String(),
				newCol.Type.String(),
			)
		}
		if newCol.Hidden != oldCol.Hidden {
			return pgerror.Newf(
				pgcode.InvalidTableDefinition,
				`cannot change visibility of view column %q`,
				oldCol.Name,
			)
		}
		if newCol.Nullable != oldCol.Nullable {
			return pgerror.Newf(
				pgcode.InvalidTableDefinition,
				`cannot change nullability of view column %q`,
				oldCol.Name,
			)
		}
	}
	return nil
}

func overrideColumnNames(cols colinfo.ResultColumns, newNames tree.NameList) colinfo.ResultColumns {
	res := append(colinfo.ResultColumns(nil), cols...)
	for i := range res {
		res[i].Name = string(newNames[i])
	}
	return res
}
