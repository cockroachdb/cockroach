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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// createViewNode represents a CREATE VIEW statement.
type createViewNode struct {
	viewName tree.Name
	// viewQuery contains the view definition, with all table names fully
	// qualified.
	viewQuery string
	temporary bool
	dbDesc    *sqlbase.DatabaseDescriptor
	columns   sqlbase.ResultColumns

	// planDeps tracks which tables and views the view being created
	// depends on. This is collected during the construction of
	// the view query's logical plan.
	planDeps planDependencies
}

func (n *createViewNode) startExec(params runParams) error {
	// TODO(arul): Allow temporary views once temp tables work for regular tables.
	if n.temporary {
		return unimplemented.NewWithIssuef(5807,
			"temporary views are unsupported")
	}

	viewName := string(n.viewName)
	log.VEventf(params.ctx, 2, "dependencies for view %s:\n%s", viewName, n.planDeps.String())

	tKey := sqlbase.NewTableKey(n.dbDesc.ID, viewName)
	key := tKey.Key()
	if exists, err := descExists(params.ctx, params.p.txn, key); err == nil && exists {
		// TODO(a-robinson): Support CREATE OR REPLACE commands.
		return sqlbase.NewRelationAlreadyExistsError(tKey.Name())
	} else if err != nil {
		return err
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
		id,
		n.columns,
		params.creationTimeForNewTableDescriptor(),
		privs,
		&params.p.semaCtx,
	)
	if err != nil {
		return err
	}

	// Collect all the tables/views this view depends on.
	for backrefID := range n.planDeps {
		desc.DependsOn = append(desc.DependsOn, backrefID)
	}

	if err = params.p.createDescriptorWithID(
		params.ctx, key, id, &desc, params.EvalContext().Settings); err != nil {
		return err
	}

	// Persist the back-references in all referenced table descriptors.
	for _, updated := range n.planDeps {
		backrefID := updated.desc.ID
		backRefMutable := params.p.Tables().getUncommittedTableByID(backrefID).MutableTableDescriptor
		if backRefMutable == nil {
			backRefMutable = sqlbase.NewMutableExistingTableDescriptor(*updated.desc.TableDesc())
		}
		for _, dep := range updated.deps {
			// The logical plan constructor merely registered the dependencies.
			// It did not populate the "ID" field of TableDescriptor_Reference,
			// because the ID of the newly created view descriptor was not
			// yet known.
			// We need to do it here.
			dep.ID = desc.ID
			backRefMutable.DependedOnBy = append(backRefMutable.DependedOnBy, dep)
		}
		if err := params.p.writeSchemaChange(params.ctx, backRefMutable, sqlbase.InvalidMutationID); err != nil {
			return err
		}
	}

	if err := desc.Validate(params.ctx, params.p.txn); err != nil {
		return err
	}

	// Log Create View event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	tn := tree.MakeTableName(tree.Name(n.dbDesc.Name), n.viewName)
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
	id sqlbase.ID,
	resultColumns []sqlbase.ResultColumn,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	semaCtx *tree.SemaContext,
) (sqlbase.MutableTableDescriptor, error) {
	desc := InitTableDescriptor(id, parentID, viewName, creationTime, privileges, false /* temporary */)
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
