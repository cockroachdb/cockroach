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

	tKey, schemaID, err := getTableCreateParams(params, n.dbDesc.ID, isTemporary, viewName)
	if err != nil {
		if sqlbase.IsRelationAlreadyExistsError(err) && n.ifNotExists {
			return nil
		}
		return err
	}

	schemaName := tree.PublicSchemaName
	if isTemporary {
		telemetry.Inc(sqltelemetry.CreateTempViewCounter)
		schemaName = tree.Name(params.p.TemporarySchemaName())
	}

	id, err := GenerateUniqueDescID(params.ctx, params.extendedEvalCtx.ExecCfg.DB)
	if err != nil {
		return err
	}

	// Inherit permissions from the database descriptor.
	privs := n.dbDesc.GetPrivileges()

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

	// Persist the back-references in all referenced table descriptors.
	for id, updated := range n.planDeps {
		backRefMutable := backRefMutables[id]
		for _, dep := range updated.deps {
			// The logical plan constructor merely registered the dependencies.
			// It did not populate the "ID" field of TableDescriptor_Reference,
			// because the ID of the newly created view descriptor was not
			// yet known.
			// We need to do it here.
			dep.ID = desc.ID
			backRefMutable.DependedOnBy = append(backRefMutable.DependedOnBy, dep)
		}
		// TODO (lucy): Have more consistent/informative names for dependent jobs.
		if err := params.p.writeSchemaChange(
			params.ctx, backRefMutable, sqlbase.InvalidMutationID, "updating view reference",
		); err != nil {
			return err
		}
	}

	if err := desc.Validate(params.ctx, params.p.txn); err != nil {
		return err
	}

	// Log Create View event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	tn := tree.MakeTableNameWithSchema(tree.Name(n.dbDesc.Name), schemaName, n.viewName)
	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogCreateView,
		int32(desc.ID),
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
	for _, colRes := range resultColumns {
		columnTableDef := tree.ColumnTableDef{Name: tree.Name(colRes.Name), Type: colRes.Typ}
		// The new types in the CREATE VIEW column specs never use
		// SERIAL so we need not process SERIAL types here.
		col, _, _, err := sqlbase.MakeColumnDefDescs(&columnTableDef, semaCtx)
		if err != nil {
			return desc, err
		}
		desc.AddColumn(col)
	}
	if err := desc.AllocateIDs(); err != nil {
		return sqlbase.MutableTableDescriptor{}, err
	}
	return desc, nil
}

func overrideColumnNames(cols sqlbase.ResultColumns, newNames tree.NameList) sqlbase.ResultColumns {
	res := append(sqlbase.ResultColumns(nil), cols...)
	for i := range res {
		res[i].Name = string(newNames[i])
	}
	return res
}
