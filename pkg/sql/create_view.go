// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// createViewNode represents a CREATE VIEW statement.
type createViewNode struct {
	n             *tree.CreateView
	dbDesc        *sqlbase.DatabaseDescriptor
	sourceColumns sqlbase.ResultColumns
	// planDeps tracks which tables and views the view being created
	// depends on. This is collected during the construction of
	// the view query's logical plan.
	planDeps planDependencies
}

// CreateView creates a view.
// Privileges: CREATE on database plus SELECT on all the selected columns.
//   notes: postgres requires CREATE on database plus SELECT on all the
//						selected columns.
//          mysql requires CREATE VIEW plus SELECT on all the selected columns.
func (p *planner) CreateView(ctx context.Context, n *tree.CreateView) (planNode, error) {
	name, err := n.Name.Normalize()
	if err != nil {
		return nil, err
	}

	var dbDesc *DatabaseDescriptor
	p.runWithOptions(resolveFlags{skipCache: true, allowAdding: true}, func() {
		dbDesc, err = ResolveTargetObject(ctx, p, name)
	})
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, dbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	// Ensure that all the table names are properly qualified.  The
	// traversal will update the NormalizableTableNames in-place, so the
	// changes are persisted in n.AsSource. We use tree.FormatNode
	// merely as a traversal method; its output buffer is discarded
	// immediately after the traversal because it is not needed further.
	var fmtErr error
	{
		f := tree.NewFmtCtxWithBuf(tree.FmtParsable)
		f.WithReformatTableNames(
			func(_ *tree.FmtCtx, t *tree.NormalizableTableName) {
				tn, err := p.QualifyWithDatabase(ctx, t)
				if err != nil {
					log.Warningf(ctx, "failed to qualify table name %q with database name: %v",
						tree.ErrString(t), err)
					fmtErr = err
					return
				}
				// Persist the database prefix expansion.
				tn.ExplicitSchema = true
				tn.ExplicitCatalog = true
			},
		)
		f.FormatNode(n.AsSource)
		f.Close() // We don't need the string.
	}

	if fmtErr != nil {
		return nil, fmtErr
	}

	planDeps, sourceColumns, err := p.analyzeViewQuery(ctx, n.AsSource)
	if err != nil {
		return nil, err
	}

	numColNames := len(n.ColumnNames)
	numColumns := len(sourceColumns)
	if numColNames != 0 && numColNames != numColumns {
		return nil, sqlbase.NewSyntaxError(fmt.Sprintf(
			"CREATE VIEW specifies %d column name%s, but data source has %d column%s",
			numColNames, util.Pluralize(int64(numColNames)),
			numColumns, util.Pluralize(int64(numColumns))))
	}

	log.VEventf(ctx, 2, "collected view dependencies:\n%s", planDeps.String())

	return &createViewNode{
		n:             n,
		dbDesc:        dbDesc,
		sourceColumns: sourceColumns,
		planDeps:      planDeps,
	}, nil
}

func (n *createViewNode) startExec(params runParams) error {
	viewName := n.n.Name.TableName().Table()
	tKey := tableKey{parentID: n.dbDesc.ID, name: viewName}
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

	desc, err := n.makeViewTableDesc(
		params,
		viewName,
		n.n.ColumnNames,
		n.dbDesc.ID,
		id,
		n.sourceColumns,
		privs,
	)
	if err != nil {
		return err
	}

	if err = desc.ValidateTable(params.EvalContext().Settings); err != nil {
		return err
	}

	// Collect all the tables/views this view depends on.
	for backrefID := range n.planDeps {
		desc.DependsOn = append(desc.DependsOn, backrefID)
	}

	if err = params.p.createDescriptorWithID(params.ctx, key, id, &desc); err != nil {
		return err
	}

	// Persist the back-references in all referenced table descriptors.
	for _, updated := range n.planDeps {
		backrefDesc := *updated.desc
		for _, dep := range updated.deps {
			// The logical plan constructor merely registered the dependencies.
			// It did not populate the "ID" field of TableDescriptor_Reference,
			// because the ID of the newly created view descriptor was not
			// yet known.
			// We need to do it here.
			dep.ID = desc.ID
			backrefDesc.DependedOnBy = append(backrefDesc.DependedOnBy, dep)
		}
		if err := params.p.saveNonmutationAndNotify(params.ctx, &backrefDesc); err != nil {
			return err
		}
	}

	if desc.Adding() {
		params.p.notifySchemaChange(&desc, sqlbase.InvalidMutationID)
	}
	if err := desc.Validate(params.ctx, params.p.txn, params.EvalContext().Settings); err != nil {
		return err
	}

	// Log Create View event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogCreateView,
		int32(desc.ID),
		int32(params.extendedEvalCtx.NodeID),
		struct {
			ViewName  string
			Statement string
			User      string
		}{n.n.Name.TableName().FQString(), n.n.String(), params.SessionData().User},
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
func (n *createViewNode) makeViewTableDesc(
	params runParams,
	viewName string,
	columnNames tree.NameList,
	parentID sqlbase.ID,
	id sqlbase.ID,
	resultColumns []sqlbase.ResultColumn,
	privileges *sqlbase.PrivilegeDescriptor,
) (sqlbase.TableDescriptor, error) {
	desc := initTableDescriptor(id, parentID, viewName,
		params.p.txn.CommitTimestamp(), privileges)
	desc.ViewQuery = tree.AsStringWithFlags(n.n.AsSource, tree.FmtParsable)
	for i, colRes := range resultColumns {
		colType, err := coltypes.DatumTypeToColumnType(colRes.Typ)
		if err != nil {
			return desc, err
		}
		columnTableDef := tree.ColumnTableDef{Name: tree.Name(colRes.Name), Type: colType}
		if len(columnNames) > i {
			columnTableDef.Name = columnNames[i]
		}
		col, _, _, err := sqlbase.MakeColumnDefDescs(
			&columnTableDef, &params.p.semaCtx, params.EvalContext())
		if err != nil {
			return desc, err
		}
		desc.AddColumn(*col)
	}
	// AllocateIDs mutates its receiver. `return desc, desc.AllocateIDs()`
	// happens to work in gc, but does not work in gccgo.
	//
	// See https://github.com/golang/go/issues/23188.
	err := desc.AllocateIDs()
	return desc, err
}
