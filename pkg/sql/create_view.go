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

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// createViewNode represents a CREATE VIEW statement.
type createViewNode struct {
	viewName tree.Name
	// viewQuery contains the view definition, with all table names fully
	// qualified.
	viewQuery   string
	ifNotExists bool
	replace     bool
	temporary   bool
	dbDesc      *sqlbase.DatabaseDescriptor
	columns     sqlbase.ResultColumns

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
	telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter("view"))

	viewName := string(n.viewName)
	isTemporary := n.temporary
	log.VEventf(params.ctx, 2, "dependencies for view %s:\n%s", viewName, n.planDeps.String())

	// First check the backrefs and see if any of them are temporary.
	// If so, promote this view to temporary.
	backRefMutables := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor, len(n.planDeps))
	for id, updated := range n.planDeps {
		backRefMutable := params.p.Tables().getUncommittedTableByID(id).MutableTableDescriptor
		if backRefMutable == nil {
			backRefMutable = sqlbase.NewMutableExistingTableDescriptor(*updated.desc.TableDesc())
		}
		if !isTemporary && backRefMutable.Temporary {
			// This notice is sent from pg, let's imitate.
			params.p.SendClientNotice(params.ctx,
				pgerror.Noticef(`view "%s" will be a temporary view`, viewName),
			)
			isTemporary = true
		}
		backRefMutables[id] = backRefMutable
	}

	var replacingDesc *sqlbase.MutableTableDescriptor

	tKey, schemaID, err := getTableCreateParams(params, n.dbDesc.ID, isTemporary, viewName)
	if err != nil {
		switch {
		case !sqlbase.IsRelationAlreadyExistsError(err):
			return err
		case n.ifNotExists:
			return nil
		case n.replace:
			// If we are replacing an existing view see if what we are
			// replacing is actually a view.
			id, err := getDescriptorID(params.ctx, params.p.txn, tKey)
			if err != nil {
				return err
			}
			desc, err := params.p.Tables().getMutableTableVersionByID(params.ctx, id, params.p.txn)
			if err != nil {
				return err
			}
			if !desc.IsView() {
				return pgerror.Newf(pgcode.WrongObjectType, `"%s" is not a view`, viewName)
			}
			replacingDesc = desc
		default:
			return err
		}
	}

	schemaName := tree.PublicSchemaName
	if isTemporary {
		telemetry.Inc(sqltelemetry.CreateTempViewCounter)
		schemaName = tree.Name(params.p.TemporarySchemaName())
	}

	// Inherit permissions from the database descriptor.
	privs := n.dbDesc.GetPrivileges()

	var newDesc *sqlbase.MutableTableDescriptor

	if replacingDesc != nil {
		// Set the query to the new query.
		replacingDesc.ViewQuery = n.viewQuery
		// Reset the columns to add the new result columns onto.
		replacingDesc.Columns = make([]sqlbase.ColumnDescriptor, 0, len(n.columns))
		replacingDesc.NextColumnID = 0
		if err := addResultColumns(&params.p.semaCtx, replacingDesc, n.columns); err != nil {
			return err
		}

		// Compare replacingDesc against its ClusterVersion to verify if
		// its new set of columns is valid for a replacement view.
		if err := verifyReplacingViewColumns(
			replacingDesc.ClusterVersion.Columns,
			replacingDesc.Columns,
		); err != nil {
			return err
		}

		// Remove the back reference from all tables that the view depended on.
		for _, id := range replacingDesc.DependsOn {
			desc, ok := backRefMutables[id]
			if !ok {
				var err error
				desc, err = params.p.Tables().getMutableTableVersionByID(params.ctx, id, params.p.txn)
				if err != nil {
					return err
				}
				backRefMutables[id] = desc
			}

			// Remove the back reference.
			desc.DependedOnBy = removeMatchingReferences(desc.DependedOnBy, replacingDesc.ID)
			if err := params.p.writeSchemaChange(
				params.ctx, desc, sqlbase.InvalidMutationID, "updating view reference",
			); err != nil {
				return err
			}
		}

		// Since the view query has been replaced, the dependencies that this
		// table descriptor had are gone.
		replacingDesc.DependsOn = make([]sqlbase.ID, 0, len(n.planDeps))
		for backrefID := range n.planDeps {
			replacingDesc.DependsOn = append(replacingDesc.DependsOn, backrefID)
		}

		// Since we are replacing an existing view here, we need to write the new
		// descriptor into place.
		if err := params.p.writeSchemaChange(params.ctx, replacingDesc, sqlbase.InvalidMutationID,
			fmt.Sprintf("CREATE OR REPLACE VIEW %q AS %q", n.viewName, n.viewQuery),
		); err != nil {
			return err
		}
		newDesc = replacingDesc
	} else {
		// If we aren't replacing anything, make a new table descriptor.
		id, err := GenerateUniqueDescID(params.ctx, params.extendedEvalCtx.ExecCfg.DB)
		if err != nil {
			return err
		}
		desc, err := makeViewTableDesc(
			viewName,
			n.viewQuery,
			n.dbDesc.ID,
			schemaID,
			id,
			n.columns,
			params.creationTimeForNewTableDescriptor(),
			privs,
			&params.p.semaCtx,
			isTemporary,
		)
		if err != nil {
			return err
		}

		// Collect all the tables/views this view depends on.
		for backrefID := range n.planDeps {
			desc.DependsOn = append(desc.DependsOn, backrefID)
		}

		// TODO (lucy): I think this needs a NodeFormatter implementation. For now,
		// do some basic string formatting (not accurate in the general case).
		if err = params.p.createDescriptorWithID(
			params.ctx, tKey.Key(), id, &desc, params.EvalContext().Settings,
			fmt.Sprintf("CREATE VIEW %q AS %q", n.viewName, n.viewQuery),
		); err != nil {
			return err
		}
		newDesc = &desc
	}

	// Persist the back-references in all referenced table descriptors.
	for id, updated := range n.planDeps {
		backRefMutable := backRefMutables[id]
		for _, dep := range updated.deps {
			// The logical plan constructor merely registered the dependencies.
			// It did not populate the "ID" field of TableDescriptor_Reference,
			// because the ID of the newly created view descriptor was not
			// yet known.
			// We need to do it here.
			dep.ID = newDesc.ID
			backRefMutable.DependedOnBy = append(backRefMutable.DependedOnBy, dep)
		}
		// TODO (lucy): Have more consistent/informative names for dependent jobs.
		if err := params.p.writeSchemaChange(
			params.ctx, backRefMutable, sqlbase.InvalidMutationID, "updating view reference",
		); err != nil {
			return err
		}
	}

	if err := newDesc.Validate(params.ctx, params.p.txn); err != nil {
		return err
	}

	// Log Create View event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	tn := tree.MakeTableNameWithSchema(tree.Name(n.dbDesc.Name), schemaName, n.viewName)
	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogCreateView,
		int32(newDesc.ID),
		int32(params.extendedEvalCtx.NodeID),
		struct {
			ViewName  string
			ViewQuery string
			User      string
		}{
			ViewName:  tn.FQString(),
			ViewQuery: n.viewQuery,
			User:      params.SessionData().User,
		},
	)
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
	viewName string,
	viewQuery string,
	parentID sqlbase.ID,
	schemaID sqlbase.ID,
	id sqlbase.ID,
	resultColumns []sqlbase.ResultColumn,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	semaCtx *tree.SemaContext,
	temporary bool,
) (sqlbase.MutableTableDescriptor, error) {
	desc := InitTableDescriptor(
		id,
		parentID,
		schemaID,
		viewName,
		creationTime,
		privileges,
		temporary,
	)
	desc.ViewQuery = viewQuery
	if err := addResultColumns(semaCtx, &desc, resultColumns); err != nil {
		return sqlbase.MutableTableDescriptor{}, err
	}
	return desc, nil
}

func addResultColumns(
	semaCtx *tree.SemaContext,
	desc *sqlbase.MutableTableDescriptor,
	resultColumns sqlbase.ResultColumns,
) error {
	for _, colRes := range resultColumns {
		columnTableDef := tree.ColumnTableDef{Name: tree.Name(colRes.Name), Type: colRes.Typ}
		// The new types in the CREATE VIEW column specs never use
		// SERIAL so we need not process SERIAL types here.
		col, _, _, err := sqlbase.MakeColumnDefDescs(&columnTableDef, semaCtx)
		if err != nil {
			return err
		}
		desc.AddColumn(col)
	}
	if err := desc.AllocateIDs(); err != nil {
		return err
	}
	return nil
}

// verifyReplacingViewColumns ensures that the new set of view columns must
// have at least the same prefix of columns as the old view. We attempt to
// match the postgres error message in each of the error cases below.
func verifyReplacingViewColumns(oldColumns, newColumns []sqlbase.ColumnDescriptor) error {
	if len(newColumns) < len(oldColumns) {
		return pgerror.Newf(pgcode.InvalidTableDefinition, "cannot drop columns from view")
	}
	for i := range oldColumns {
		oldCol, newCol := &oldColumns[i], &newColumns[i]
		if oldCol.Name != newCol.Name {
			return pgerror.Newf(
				pgcode.InvalidTableDefinition,
				`cannot change name of view column "%s" to "%s"`,
				oldCol.Name,
				newCol.Name,
			)
		}
		if !newCol.Type.Equal(oldCol.Type) {
			return pgerror.Newf(
				pgcode.InvalidTableDefinition,
				`cannot change type of view column "%s" from %s to %s`,
				oldCol.Name,
				oldCol.Type.String(),
				newCol.Type.String(),
			)
		}
	}
	return nil
}

func overrideColumnNames(cols sqlbase.ResultColumns, newNames tree.NameList) sqlbase.ResultColumns {
	res := append(sqlbase.ResultColumns(nil), cols...)
	for i := range res {
		res[i].Name = string(newNames[i])
	}
	return res
}
