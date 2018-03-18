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
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
)

type createTableNode struct {
	n          *tree.CreateTable
	dbDesc     *sqlbase.DatabaseDescriptor
	sourcePlan planNode

	run createTableRun
}

// CreateTable creates a table.
// Privileges: CREATE on database.
//   Notes: postgres/mysql require CREATE on database.
func (p *planner) CreateTable(ctx context.Context, n *tree.CreateTable) (planNode, error) {
	tn, err := n.Table.Normalize()
	if err != nil {
		return nil, err
	}

	var dbDesc *DatabaseDescriptor
	p.runWithOptions(resolveFlags{skipCache: true, allowAdding: true}, func() {
		dbDesc, err = ResolveTargetObject(ctx, p, tn)
	})
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, dbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	HoistConstraints(n)
	for _, def := range n.Defs {
		switch t := def.(type) {
		case *tree.ForeignKeyConstraintTableDef:
			// Just check the target name has the right structure. We'll do
			// name resolution later, after the descriptor has been
			// allocated.
			_, err := t.Table.Normalize()
			if err != nil {
				return nil, err
			}
		}
	}

	var sourcePlan planNode
	if n.As() {
		// The sourcePlan is needed to determine the set of columns to use
		// to populate the new table descriptor in Start() below.
		sourcePlan, err = p.Select(ctx, n.AsSource, []types.T{})
		if err != nil {
			return nil, err
		}

		numColNames := len(n.AsColumnNames)
		numColumns := len(planColumns(sourcePlan))
		if numColNames != 0 && numColNames != numColumns {
			sourcePlan.Close(ctx)
			return nil, sqlbase.NewSyntaxError(fmt.Sprintf(
				"CREATE TABLE specifies %d column name%s, but data source has %d column%s",
				numColNames, util.Pluralize(int64(numColNames)),
				numColumns, util.Pluralize(int64(numColumns))))
		}
	}

	return &createTableNode{n: n, dbDesc: dbDesc, sourcePlan: sourcePlan}, nil
}

// createTableRun contains the run-time state of createTableNode
// during local execution.
type createTableRun struct {
	autoCommit   autoCommitOpt
	rowsAffected int
}

func (n *createTableNode) startExec(params runParams) error {
	tKey := tableKey{parentID: n.dbDesc.ID, name: n.n.Table.TableName().Table()}
	key := tKey.Key()
	if exists, err := descExists(params.ctx, params.p.txn, key); err == nil && exists {
		if n.n.IfNotExists {
			return nil
		}
		return sqlbase.NewRelationAlreadyExistsError(tKey.Name())
	} else if err != nil {
		return err
	}

	id, err := GenerateUniqueDescID(params.ctx, params.extendedEvalCtx.ExecCfg.DB)
	if err != nil {
		return err
	}

	// If a new system table is being created (which should only be doable by
	// an internal user account), make sure it gets the correct privileges.
	privs := n.dbDesc.GetPrivileges()
	if n.dbDesc.ID == keys.SystemDatabaseID {
		privs = sqlbase.NewDefaultPrivilegeDescriptor()
	}

	var desc sqlbase.TableDescriptor
	var affected map[sqlbase.ID]*sqlbase.TableDescriptor
	creationTime := params.p.txn.CommitTimestamp()
	if n.n.As() {
		desc, err = makeTableDescIfAs(
			n.n, n.dbDesc.ID, id, creationTime, planColumns(n.sourcePlan),
			privs, &params.p.semaCtx, params.EvalContext())
	} else {
		affected = make(map[sqlbase.ID]*sqlbase.TableDescriptor)
		desc, err = params.p.makeTableDesc(params.ctx, n.n, n.dbDesc.ID, id, creationTime, privs, affected)
	}
	if err != nil {
		return err
	}

	// We need to validate again after adding the FKs.
	// Only validate the table because backreferences aren't created yet.
	// Everything is validated below.
	if err := desc.ValidateTable(params.EvalContext().Settings); err != nil {
		return err
	}

	if err := params.p.createDescriptorWithID(params.ctx, key, id, &desc); err != nil {
		return err
	}

	for _, updated := range affected {
		if err := params.p.saveNonmutationAndNotify(params.ctx, updated); err != nil {
			return err
		}
	}
	if desc.Adding() {
		params.p.notifySchemaChange(&desc, sqlbase.InvalidMutationID)
	}

	for _, index := range desc.AllNonDropIndexes() {
		if len(index.Interleave.Ancestors) > 0 {
			if err := params.p.finalizeInterleave(params.ctx, &desc, index); err != nil {
				return err
			}
		}
	}

	if err := desc.Validate(params.ctx, params.p.txn, params.EvalContext().Settings); err != nil {
		return err
	}

	// Log Create Table event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	if err := MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogCreateTable,
		int32(desc.ID),
		int32(params.extendedEvalCtx.NodeID),
		struct {
			TableName string
			Statement string
			User      string
		}{n.n.Table.TableName().FQString(), n.n.String(), params.SessionData().User},
	); err != nil {
		return err
	}

	if n.n.As() {
		// This is a very simplified version of the INSERT logic: no CHECK
		// expressions, no FK checks, no arbitrary insertion order, no
		// RETURNING, etc.

		// Instantiate a row inserter and table writer. It has a 1-1
		// mapping to the definitions in the descriptor.
		ri, err := sqlbase.MakeRowInserter(
			params.p.txn, &desc, nil, desc.Columns, sqlbase.SkipFKs, &params.p.alloc)
		if err != nil {
			return err
		}
		ti := tableInserterPool.Get().(*tableInserter)
		*ti = tableInserter{ri: ri}
		tw := tableWriter(ti)
		defer func() {
			tw.close(params.ctx)
			*ti = tableInserter{}
			tableInserterPool.Put(ti)
		}()
		if err := tw.init(params.p.txn, params.p.EvalContext()); err != nil {
			return err
		}

		// Prepare the buffer for row values. At this point, one more
		// column has been added by ensurePrimaryKey() to the list of
		// columns in sourcePlan.
		rowBuffer := make(tree.Datums, len(desc.Columns))
		pkColIdx := len(desc.Columns) - 1

		// Prepare the rowID expression.
		defExprSQL := *desc.Columns[pkColIdx].DefaultExpr
		defExpr, err := parser.ParseExpr(defExprSQL)
		if err != nil {
			return err
		}
		defTypedExpr, err := params.p.analyzeExpr(
			params.ctx,
			defExpr,
			nil, /*sources*/
			tree.IndexedVarHelper{},
			types.Any,
			false, /*requireType*/
			"CREATE TABLE AS")
		if err != nil {
			return err
		}

		for {
			if err := params.p.cancelChecker.Check(); err != nil {
				return err
			}
			if next, err := n.sourcePlan.Next(params); !next {
				if err != nil {
					return err
				}
				_, err := tw.finalize(
					params.ctx, n.run.autoCommit, params.extendedEvalCtx.Tracing.KVTracingEnabled())
				if err != nil {
					return err
				}
				break
			}

			// Populate the buffer and generate the PK value.
			copy(rowBuffer, n.sourcePlan.Values())
			rowBuffer[pkColIdx], err = defTypedExpr.Eval(params.p.EvalContext())
			if err != nil {
				return err
			}

			_, err := tw.row(params.ctx, rowBuffer, params.extendedEvalCtx.Tracing.KVTracingEnabled())
			if err != nil {
				return err
			}
			n.run.rowsAffected++
		}
	}
	return nil
}

// enableAutoCommit is part of the autoCommitNode interface.
func (n *createTableNode) enableAutoCommit() {
	n.run.autoCommit = autoCommitEnabled
}

func (*createTableNode) Next(runParams) (bool, error) { return false, nil }
func (*createTableNode) Values() tree.Datums          { return tree.Datums{} }

func (n *createTableNode) Close(ctx context.Context) {
	if n.sourcePlan != nil {
		n.sourcePlan.Close(ctx)
		n.sourcePlan = nil
	}
}

func (n *createTableNode) FastPathResults() (int, bool) {
	if n.n.As() {
		return n.run.rowsAffected, true
	}
	return 0, false
}

// HoistConstraints finds column constraints defined inline with their columns
// and makes them table-level constraints, stored in n.Defs. For example, the
// foreign key constraint in
//
//     CREATE TABLE foo (a INT REFERENCES bar(a))
//
// gets pulled into a top-level constraint like:
//
//     CREATE TABLE foo (a INT, FOREIGN KEY (a) REFERENCES bar(a))
//
// Similarly, the CHECK constraint in
//
//    CREATE TABLE foo (a INT CHECK (a < 1), b INT)
//
// gets pulled into a top-level constraint like:
//
//    CREATE TABLE foo (a INT, b INT, CHECK (a < 1))
//
// Note some SQL databases require that a constraint attached to a column to
// refer only to the column it is attached to. We follow Postgres' behavior,
// however, in omitting this restriction by blindly hoisting all column
// constraints. For example, the following table definition is accepted in
// CockroachDB and Postgres, but not necessarily other SQL databases:
//
//    CREATE TABLE foo (a INT CHECK (a < b), b INT)
//
func HoistConstraints(n *tree.CreateTable) {
	for _, d := range n.Defs {
		if col, ok := d.(*tree.ColumnTableDef); ok {
			for _, checkExpr := range col.CheckExprs {
				n.Defs = append(n.Defs,
					&tree.CheckConstraintTableDef{
						Expr: checkExpr.Expr,
						Name: checkExpr.ConstraintName,
					},
				)
			}
			col.CheckExprs = nil
			if col.HasFKConstraint() {
				var targetCol tree.NameList
				if col.References.Col != "" {
					targetCol = append(targetCol, col.References.Col)
				}
				n.Defs = append(n.Defs, &tree.ForeignKeyConstraintTableDef{
					Table:    col.References.Table,
					FromCols: tree.NameList{col.Name},
					ToCols:   targetCol,
					Name:     col.References.ConstraintName,
					Actions:  col.References.Actions,
				})
				col.References.Table = tree.NormalizableTableName{}
			}
		}
	}
}

type indexMatch bool

const (
	matchExact  indexMatch = true
	matchPrefix indexMatch = false
)

// Referenced cols must be unique, thus referenced indexes must match exactly.
// Referencing cols have no uniqueness requirement and thus may match a strict
// prefix of an index.
func matchesIndex(
	cols []sqlbase.ColumnDescriptor, idx sqlbase.IndexDescriptor, exact indexMatch,
) bool {
	if len(cols) > len(idx.ColumnIDs) || (exact && len(cols) != len(idx.ColumnIDs)) {
		return false
	}

	for i := range cols {
		if cols[i].ID != idx.ColumnIDs[i] {
			return false
		}
	}
	return true
}

// resolveFK on the planner calls resolveFK() on the current txn.
//
// The caller must make sure the planner is configured to look up
// descriptors without caching. See the comment on resolveFK().
func (p *planner) resolveFK(
	ctx context.Context,
	tbl *sqlbase.TableDescriptor,
	d *tree.ForeignKeyConstraintTableDef,
	backrefs map[sqlbase.ID]*sqlbase.TableDescriptor,
	mode sqlbase.ConstraintValidity,
) error {
	return resolveFK(ctx, p.txn, p, tbl, d, backrefs, mode)
}

// resolveFK looks up the tables and columns mentioned in a `REFERENCES`
// constraint and adds metadata representing that constraint to the descriptor.
// It may, in doing so, add to or alter descriptors in the passed in `backrefs`
// map of other tables that need to be updated when this table is created.
// Constraints that are not known to hold for existing data are created
// "unvalidated", but when table is empty (e.g. during creation), no existing
// data imples no existing violations, and thus the constraint can be created
// without the unvalidated flag.
//
// The caller should pass an instance of fkSelfResolver as
// SchemaResolver, so that FK references can find the newly created
// table for self-references.
//
// The caller must also ensure that the SchemaResolver is configured to
// bypass caching and enable visibility of just-added descriptors.
// If there are any FKs, the descriptor of the depended-on table must
// be looked up uncached, and we'll allow FK dependencies on tables
// that were just added.
func resolveFK(
	ctx context.Context,
	txn *client.Txn,
	sc SchemaResolver,
	tbl *sqlbase.TableDescriptor,
	d *tree.ForeignKeyConstraintTableDef,
	backrefs map[sqlbase.ID]*sqlbase.TableDescriptor,
	mode sqlbase.ConstraintValidity,
) error {
	targetTable := d.Table.TableName()

	target, err := ResolveExistingObject(ctx, sc, targetTable, true /*required*/, requireTableDesc)
	if err != nil {
		return err
	}
	if target.ID == tbl.ID {
		// When adding a self-ref FK to an _existing_ table, we want to make sure
		// we edit the same copy.
		target = tbl
	} else {
		// Since this FK is referencing another table, this table must be created in
		// a non-public "ADD" state and made public only after all leases on the
		// other table are updated to include the backref.
		if mode == sqlbase.ConstraintValidity_Validated {
			tbl.State = sqlbase.TableDescriptor_ADD
			if err := tbl.SetUpVersion(); err != nil {
				return err
			}
		}

		// If we resolve the same table more than once, we only want to edit a
		// single instance of it, so replace target with previously resolved table.
		if prev, ok := backrefs[target.ID]; ok {
			target = prev
		} else {
			backrefs[target.ID] = target
		}
	}

	srcCols, err := tbl.FindActiveColumnsByNames(d.FromCols)
	if err != nil {
		return err
	}

	targetColNames := d.ToCols
	// If no columns are specified, attempt to default to PK.
	if len(targetColNames) == 0 {
		targetColNames = make(tree.NameList, len(target.PrimaryIndex.ColumnNames))
		for i, n := range target.PrimaryIndex.ColumnNames {
			targetColNames[i] = tree.Name(n)
		}
	}

	targetCols, err := target.FindActiveColumnsByNames(targetColNames)
	if err != nil {
		return err
	}

	if len(targetCols) != len(srcCols) {
		return fmt.Errorf("%d columns must reference exactly %d columns in referenced table (found %d)",
			len(srcCols), len(srcCols), len(targetCols))
	}

	for i := range srcCols {
		if s, t := srcCols[i], targetCols[i]; s.Type.SemanticType != t.Type.SemanticType {
			return fmt.Errorf("type of %q (%s) does not match foreign key %q.%q (%s)",
				s.Name, s.Type.SemanticType, target.Name, t.Name, t.Type.SemanticType)
		}
	}

	constraintName := string(d.Name)
	if constraintName == "" {
		constraintName = fmt.Sprintf("fk_%s_ref_%s", string(d.FromCols[0]), target.Name)
	}

	// We can't keep a reference to the index in the slice and at the same time
	// add a new index to that slice without losing the reference. Instead, keep
	// the index's index into target's list of indexes. If it is a primary index,
	// targetIdxIndex is set to -1. Also store the targetIndex's ID so we
	// don't have to do the lookup twice.
	targetIdxIndex := -1
	var targetIdxID sqlbase.IndexID
	if matchesIndex(targetCols, target.PrimaryIndex, matchExact) {
		targetIdxID = target.PrimaryIndex.ID
	} else {
		found := false
		// Find the index corresponding to the referenced column.
		for i, idx := range target.Indexes {
			if idx.Unique && matchesIndex(targetCols, idx, matchExact) {
				targetIdxIndex = i
				targetIdxID = idx.ID
				found = true
				break
			}
		}
		if !found {
			return pgerror.NewErrorf(
				pgerror.CodeInvalidForeignKeyError,
				"there is no unique constraint matching given keys for referenced table %s",
				targetTable.String(),
			)
		}
	}

	// Don't add a SET NULL action on an index that has any column that is NOT
	// NULL.
	if d.Actions.Delete == tree.SetNull || d.Actions.Update == tree.SetNull {
		for _, sourceColumn := range srcCols {
			if !sourceColumn.Nullable {
				// TODO(whomever): this ought to use a database cache.
				database, err := sqlbase.GetDatabaseDescFromID(ctx, txn, tbl.ParentID)
				if err != nil {
					return err
				}
				return pgerror.NewErrorf(pgerror.CodeInvalidForeignKeyError,
					"cannot add a SET NULL cascading action on column %q which has a NOT NULL constraint",
					tree.ErrString(tree.NewUnresolvedName(
						database.Name, tree.PublicSchema, tbl.Name, sourceColumn.Name)))
			}
		}
	}

	// Don't add a SET DEFAULT action on an index that has any column that does
	// not have a DEFAULT expression.
	if d.Actions.Delete == tree.SetDefault || d.Actions.Update == tree.SetDefault {
		for _, sourceColumn := range srcCols {
			if sourceColumn.DefaultExpr == nil {
				// TODO(whomever): this ought to use a database cache.
				database, err := sqlbase.GetDatabaseDescFromID(ctx, txn, tbl.ParentID)
				if err != nil {
					return err
				}
				return pgerror.NewErrorf(pgerror.CodeInvalidForeignKeyError,
					"cannot add a SET DEFAULT cascading action on column %q which has no DEFAULT expression",
					tree.ErrString(tree.NewUnresolvedName(
						database.Name, tree.PublicSchema, tbl.Name, sourceColumn.Name)))
			}
		}
	}

	ref := sqlbase.ForeignKeyReference{
		Table:           target.ID,
		Index:           targetIdxID,
		Name:            constraintName,
		SharedPrefixLen: int32(len(srcCols)),
		OnDelete:        sqlbase.ForeignKeyReferenceActionValue[d.Actions.Delete],
		OnUpdate:        sqlbase.ForeignKeyReferenceActionValue[d.Actions.Update],
	}

	if mode == sqlbase.ConstraintValidity_Unvalidated {
		ref.Validity = sqlbase.ConstraintValidity_Unvalidated
	}
	backref := sqlbase.ForeignKeyReference{Table: tbl.ID}

	if matchesIndex(srcCols, tbl.PrimaryIndex, matchPrefix) {
		if tbl.PrimaryIndex.ForeignKey.IsSet() {
			return pgerror.NewErrorf(pgerror.CodeInvalidForeignKeyError,
				"columns cannot be used by multiple foreign key constraints")
		}
		tbl.PrimaryIndex.ForeignKey = ref
		backref.Index = tbl.PrimaryIndex.ID
	} else {
		found := false
		for i := range tbl.Indexes {
			if matchesIndex(srcCols, tbl.Indexes[i], matchPrefix) {
				if tbl.Indexes[i].ForeignKey.IsSet() {
					return pgerror.NewErrorf(pgerror.CodeInvalidForeignKeyError,
						"columns cannot be used by multiple foreign key constraints")
				}
				tbl.Indexes[i].ForeignKey = ref
				backref.Index = tbl.Indexes[i].ID
				found = true
				break
			}
		}
		if !found {
			// Avoid unexpected index builds from ALTER TABLE ADD CONSTRAINT.
			if mode == sqlbase.ConstraintValidity_Unvalidated {
				return pgerror.NewErrorf(pgerror.CodeInvalidForeignKeyError,
					"foreign key requires an existing index on columns %s", colNames(srcCols))
			}
			added, err := addIndexForFK(tbl, srcCols, constraintName, ref)
			if err != nil {
				return err
			}
			backref.Index = added
		}
	}
	if targetIdxIndex > -1 {
		target.Indexes[targetIdxIndex].ReferencedBy = append(target.Indexes[targetIdxIndex].ReferencedBy, backref)
	} else {
		target.PrimaryIndex.ReferencedBy = append(target.PrimaryIndex.ReferencedBy, backref)
	}
	return nil
}

// Adds an index to a table descriptor (that is in the process of being created)
// that will support using `srcCols` as the referencing (src) side of an FK.
func addIndexForFK(
	tbl *sqlbase.TableDescriptor,
	srcCols []sqlbase.ColumnDescriptor,
	constraintName string,
	ref sqlbase.ForeignKeyReference,
) (sqlbase.IndexID, error) {
	// No existing index for the referencing columns found, so we add one.
	idx := sqlbase.IndexDescriptor{
		Name:             fmt.Sprintf("%s_auto_index_%s", tbl.Name, constraintName),
		ColumnNames:      make([]string, len(srcCols)),
		ColumnDirections: make([]sqlbase.IndexDescriptor_Direction, len(srcCols)),
		ForeignKey:       ref,
	}
	for i, c := range srcCols {
		idx.ColumnDirections[i] = sqlbase.IndexDescriptor_ASC
		idx.ColumnNames[i] = c.Name
	}
	if err := tbl.AddIndex(idx, false); err != nil {
		return 0, err
	}
	if err := tbl.AllocateIDs(); err != nil {
		return 0, err
	}

	added := tbl.Indexes[len(tbl.Indexes)-1]

	// Since we just added the index, we can assume it is the last one rather than
	// searching all the indexes again. That said, we sanity check that it matches
	// in case a refactor ever violates that assumption.
	if !matchesIndex(srcCols, added, matchPrefix) {
		panic("no matching index and auto-generated index failed to match")
	}

	return added.ID, nil
}

// colNames converts a []colDesc to a human-readable string for use in error messages.
func colNames(cols []sqlbase.ColumnDescriptor) string {
	var s bytes.Buffer
	s.WriteString(`("`)
	for i, c := range cols {
		if i != 0 {
			s.WriteString(`", "`)
		}
		s.WriteString(c.Name)
	}
	s.WriteString(`")`)
	return s.String()
}

func (p *planner) saveNonmutationAndNotify(ctx context.Context, td *sqlbase.TableDescriptor) error {
	if err := td.SetUpVersion(); err != nil {
		return err
	}
	if err := td.ValidateTable(p.EvalContext().Settings); err != nil {
		return err
	}
	if err := p.writeTableDesc(ctx, td); err != nil {
		return err
	}
	p.notifySchemaChange(td, sqlbase.InvalidMutationID)
	return nil
}

func (p *planner) addInterleave(
	ctx context.Context,
	desc *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	interleave *tree.InterleaveDef,
) error {
	return addInterleave(ctx, p.txn, p, desc, index, interleave)
}

// addInterleave marks an index as one that is interleaved in some parent data
// according to the given definition.
func addInterleave(
	ctx context.Context,
	txn *client.Txn,
	vt SchemaResolver,
	desc *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	interleave *tree.InterleaveDef,
) error {
	if interleave.DropBehavior != tree.DropDefault {
		return pgerror.UnimplementedWithIssueErrorf(
			7854, "unsupported shorthand %s", interleave.DropBehavior)
	}

	tn, err := interleave.Parent.Normalize()
	if err != nil {
		return err
	}

	parentTable, err := ResolveExistingObject(ctx, vt, tn, true /*required*/, requireTableDesc)
	if err != nil {
		return err
	}
	parentIndex := parentTable.PrimaryIndex

	// typeOfIndex is used to give more informative error messages.
	var typeOfIndex string
	if index.ID == desc.PrimaryIndex.ID {
		typeOfIndex = "primary key"
	} else {
		typeOfIndex = "index"
	}

	if len(interleave.Fields) != len(parentIndex.ColumnIDs) {
		return pgerror.NewErrorf(
			pgerror.CodeInvalidSchemaDefinitionError,
			"declared interleaved columns (%s) must match the parent's primary index (%s)",
			&interleave.Fields,
			strings.Join(parentIndex.ColumnNames, ", "),
		)
	}
	if len(interleave.Fields) > len(index.ColumnIDs) {
		return pgerror.NewErrorf(
			pgerror.CodeInvalidSchemaDefinitionError,
			"declared interleaved columns (%s) must be a prefix of the %s columns being interleaved (%s)",
			&interleave.Fields,
			typeOfIndex,
			strings.Join(index.ColumnNames, ", "),
		)
	}

	for i, targetColID := range parentIndex.ColumnIDs {
		targetCol, err := parentTable.FindColumnByID(targetColID)
		if err != nil {
			return err
		}
		col, err := desc.FindColumnByID(index.ColumnIDs[i])
		if err != nil {
			return err
		}
		if string(interleave.Fields[i]) != col.Name {
			return pgerror.NewErrorf(
				pgerror.CodeInvalidSchemaDefinitionError,
				"declared interleaved columns (%s) must refer to a prefix of the %s column names being interleaved (%s)",
				&interleave.Fields,
				typeOfIndex,
				strings.Join(index.ColumnNames, ", "),
			)
		}
		if !col.Type.Equal(targetCol.Type) || index.ColumnDirections[i] != parentIndex.ColumnDirections[i] {
			return pgerror.NewErrorf(
				pgerror.CodeInvalidSchemaDefinitionError,
				"declared interleaved columns (%s) must match type and sort direction of the parent's primary index (%s)",
				&interleave.Fields,
				strings.Join(parentIndex.ColumnNames, ", "),
			)
		}
	}

	ancestorPrefix := append(
		[]sqlbase.InterleaveDescriptor_Ancestor(nil), parentIndex.Interleave.Ancestors...)
	intl := sqlbase.InterleaveDescriptor_Ancestor{
		TableID:         parentTable.ID,
		IndexID:         parentIndex.ID,
		SharedPrefixLen: uint32(len(parentIndex.ColumnIDs)),
	}
	for _, ancestor := range ancestorPrefix {
		intl.SharedPrefixLen -= ancestor.SharedPrefixLen
	}
	index.Interleave = sqlbase.InterleaveDescriptor{Ancestors: append(ancestorPrefix, intl)}

	desc.State = sqlbase.TableDescriptor_ADD
	return nil
}

// finalizeInterleave creates backreferences from an interleaving parent to the
// child data being interleaved.
func (p *planner) finalizeInterleave(
	ctx context.Context, desc *sqlbase.TableDescriptor, index sqlbase.IndexDescriptor,
) error {
	// TODO(dan): This is similar to finalizeFKs. Consolidate them
	if len(index.Interleave.Ancestors) == 0 {
		return nil
	}
	// Only the last ancestor needs the backreference.
	ancestor := index.Interleave.Ancestors[len(index.Interleave.Ancestors)-1]
	var ancestorTable *sqlbase.TableDescriptor
	if ancestor.TableID == desc.ID {
		ancestorTable = desc
	} else {
		var err error
		ancestorTable, err = sqlbase.GetTableDescFromID(ctx, p.txn, ancestor.TableID)
		if err != nil {
			return err
		}
	}
	ancestorIndex, err := ancestorTable.FindIndexByID(ancestor.IndexID)
	if err != nil {
		return err
	}
	ancestorIndex.InterleavedBy = append(ancestorIndex.InterleavedBy,
		sqlbase.ForeignKeyReference{Table: desc.ID, Index: index.ID})

	if err := p.saveNonmutationAndNotify(ctx, ancestorTable); err != nil {
		return err
	}

	if desc.State == sqlbase.TableDescriptor_ADD {
		desc.State = sqlbase.TableDescriptor_PUBLIC

		if err := p.saveNonmutationAndNotify(ctx, desc); err != nil {
			return err
		}
	}

	return nil
}

// CreatePartitioning constructs the partitioning descriptor for an index that
// is partitioned into ranges, each addressable by zone configs.
func CreatePartitioning(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *tree.EvalContext,
	tableDesc *sqlbase.TableDescriptor,
	indexDesc *sqlbase.IndexDescriptor,
	partBy *tree.PartitionBy,
) (sqlbase.PartitioningDescriptor, error) {
	if partBy == nil {
		// No CCL necessary if we're looking at PARTITION BY NOTHING.
		return sqlbase.PartitioningDescriptor{}, nil
	}
	return CreatePartitioningCCL(ctx, st, evalCtx, tableDesc, indexDesc, partBy)
}

// CreatePartitioningCCL is the public hook point for the CCL-licensed
// partitioning creation code.
var CreatePartitioningCCL = func(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *tree.EvalContext,
	tableDesc *sqlbase.TableDescriptor,
	indexDesc *sqlbase.IndexDescriptor,
	partBy *tree.PartitionBy,
) (sqlbase.PartitioningDescriptor, error) {
	return sqlbase.PartitioningDescriptor{}, sqlbase.NewCCLRequiredError(errors.New(
		"creating or manipulating partitions requires a CCL binary"))
}

func initTableDescriptor(
	id, parentID sqlbase.ID,
	name string,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
) sqlbase.TableDescriptor {
	return sqlbase.TableDescriptor{
		ID:               id,
		Name:             name,
		ParentID:         parentID,
		FormatVersion:    sqlbase.InterleavedFormatVersion,
		Version:          1,
		ModificationTime: creationTime,
		Privileges:       privileges,
	}
}

// makeTableDescIfAs is the MakeTableDesc method for when we have a table
// that is created with the CREATE AS format.
func makeTableDescIfAs(
	p *tree.CreateTable,
	parentID, id sqlbase.ID,
	creationTime hlc.Timestamp,
	resultColumns []sqlbase.ResultColumn,
	privileges *sqlbase.PrivilegeDescriptor,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
) (desc sqlbase.TableDescriptor, err error) {
	tableName, err := p.Table.Normalize()
	if err != nil {
		return desc, err
	}
	desc = initTableDescriptor(id, parentID, tableName.Table(), creationTime, privileges)
	for i, colRes := range resultColumns {
		colType, err := coltypes.DatumTypeToColumnType(colRes.Typ)
		if err != nil {
			return desc, err
		}
		columnTableDef := tree.ColumnTableDef{Name: tree.Name(colRes.Name), Type: colType}
		columnTableDef.Nullable.Nullability = tree.SilentNull
		if len(p.AsColumnNames) > i {
			columnTableDef.Name = p.AsColumnNames[i]
		}

		col, _, _, err := sqlbase.MakeColumnDefDescs(&columnTableDef, semaCtx, evalCtx)
		if err != nil {
			return desc, err
		}
		desc.AddColumn(*col)
	}

	// AllocateIDs mutates its receiver. `return desc, desc.AllocateIDs()`
	// happens to work in gc, but does not work in gccgo.
	//
	// See https://github.com/golang/go/issues/23188.
	err = desc.AllocateIDs()
	return desc, err
}

func validateComputedColumnHasNoImpureFunctions(e tree.TypedExpr, colName tree.Name) error {
	if fns := tree.ImpureFunctions(e); len(fns) != 0 {
		var errMsg bytes.Buffer
		errMsg.WriteString(
			fmt.Sprintf(
				"computed column %q contains impure functions: ",
				tree.ErrString(&colName),
			),
		)
		for i, fn := range fns {
			if i != 0 {
				errMsg.WriteString(", ")
			}
			errMsg.WriteString(fn.String())
		}
		return pgerror.NewError(pgerror.CodeInvalidTableDefinitionError, errMsg.String())
	}
	return nil
}

func dequalifyColumnRefs(
	ctx context.Context, sources sqlbase.MultiSourceInfo, expr tree.Expr,
) (tree.Expr, error) {
	resolver := sqlbase.ColumnResolver{Sources: sources}
	return tree.SimpleVisit(
		expr,
		func(expr tree.Expr) (err error, recurse bool, newExpr tree.Expr) {
			if vBase, ok := expr.(tree.VarName); ok {
				v, err := vBase.NormalizeVarName()
				if err != nil {
					return err, false, nil
				}
				if c, ok := v.(*tree.ColumnItem); ok {
					_, err := c.Resolve(ctx, &resolver)
					if err != nil {
						return err, false, nil
					}
					srcIdx := resolver.ResolverState.SrcIdx
					colIdx := resolver.ResolverState.ColIdx
					col := sources[srcIdx].SourceColumns[colIdx]
					return nil, false, &tree.ColumnItem{ColumnName: tree.Name(col.Name)}
				}
			}
			return nil, true, expr
		},
	)
}

// MakeTableDesc creates a table descriptor from a CreateTable statement.
//
// txn and vt can be nil if the table to be created does not contain references
// to other tables (e.g. foreign keys or interleaving). This is useful at
// bootstrap when creating descriptors for virtual tables.
//
// evalCtx can be nil if the table to be created has no default expression for
// any of the columns and no partitioning expression.
//
// semaCtx can be nil if the table to be created has no default expression on
// any of the columns and no check constraints.
//
// The caller must also ensure that the SchemaResolver is configured
// to bypass caching and enable visibility of just-added descriptors.
// This is used to resolve sequence and FK dependencies. Also see the
// comment at the start of the global scope resolveFK().
func MakeTableDesc(
	ctx context.Context,
	txn *client.Txn,
	vt SchemaResolver,
	st *cluster.Settings,
	n *tree.CreateTable,
	parentID, id sqlbase.ID,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	affected map[sqlbase.ID]*sqlbase.TableDescriptor,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
) (sqlbase.TableDescriptor, error) {
	tableName, err := n.Table.Normalize()
	if err != nil {
		return sqlbase.TableDescriptor{}, err
	}
	desc := initTableDescriptor(id, parentID, tableName.Table(), creationTime, privileges)

	for _, def := range n.Defs {
		if d, ok := def.(*tree.ColumnTableDef); ok {
			if !desc.IsVirtualTable() {
				if _, ok := d.Type.(*coltypes.TVector); ok {
					return desc, pgerror.NewErrorf(
						pgerror.CodeFeatureNotSupportedError,
						"VECTOR column types are unsupported",
					)
				}
			}
			col, idx, expr, err := sqlbase.MakeColumnDefDescs(d, semaCtx, evalCtx)
			if err != nil {
				return desc, err
			}

			if d.HasDefaultExpr() {
				changedSeqDescs, err := maybeAddSequenceDependencies(vt, &desc, col, expr, evalCtx)
				if err != nil {
					return desc, err
				}
				for _, changedSeqDesc := range changedSeqDescs {
					affected[changedSeqDesc.ID] = changedSeqDesc
				}
			}

			desc.AddColumn(*col)
			if idx != nil {
				if err := desc.AddIndex(*idx, d.PrimaryKey); err != nil {
					return desc, err
				}
			}
			if d.HasColumnFamily() {
				// Pass true for `create` and `ifNotExists` because when we're creating
				// a table, we always want to create the specified family if it doesn't
				// exist.
				err := desc.AddColumnToFamilyMaybeCreate(col.Name, string(d.Family.Name), true, true)
				if err != nil {
					return desc, err
				}
			}
		}
	}

	// Now that we've constructed our columns, we pop into any of our computed
	// columns so that we can dequalify any column references.
	sourceInfo := sqlbase.NewSourceInfoForSingleTable(
		*tableName, sqlbase.ResultColumnsFromColDescs(desc.Columns),
	)
	sources := sqlbase.MultiSourceInfo{sourceInfo}

	for i, col := range desc.Columns {
		if col.IsComputed() {
			expr, err := parser.ParseExpr(*col.ComputeExpr)
			if err != nil {
				return desc, err
			}

			expr, err = dequalifyColumnRefs(ctx, sources, expr)
			if err != nil {
				return desc, err
			}
			serialized := tree.Serialize(expr)
			desc.Columns[i].ComputeExpr = &serialized
		}
	}

	var primaryIndexColumnSet map[string]struct{}
	for _, def := range n.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			// pass, handled above.

		case *tree.IndexTableDef:
			idx := sqlbase.IndexDescriptor{
				Name:             string(d.Name),
				StoreColumnNames: d.Storing.ToStrings(),
			}
			if d.Inverted {
				idx.Type = sqlbase.IndexDescriptor_INVERTED
			}
			if err := idx.FillColumns(d.Columns); err != nil {
				return desc, err
			}
			if d.PartitionBy != nil {
				partitioning, err := CreatePartitioning(ctx, st, evalCtx, &desc, &idx, d.PartitionBy)
				if err != nil {
					return desc, err
				}
				idx.Partitioning = partitioning
			}
			if err := desc.AddIndex(idx, false); err != nil {
				return desc, err
			}
			if d.Interleave != nil {
				return desc, pgerror.UnimplementedWithIssueError(9148, "use CREATE INDEX to make interleaved indexes")
			}
		case *tree.UniqueConstraintTableDef:
			idx := sqlbase.IndexDescriptor{
				Name:             string(d.Name),
				Unique:           true,
				StoreColumnNames: d.Storing.ToStrings(),
			}
			if err := idx.FillColumns(d.Columns); err != nil {
				return desc, err
			}
			if d.PartitionBy != nil {
				partitioning, err := CreatePartitioning(ctx, st, evalCtx, &desc, &idx, d.PartitionBy)
				if err != nil {
					return desc, err
				}
				idx.Partitioning = partitioning
			}
			if err := desc.AddIndex(idx, d.PrimaryKey); err != nil {
				return desc, err
			}
			if d.PrimaryKey {
				primaryIndexColumnSet = make(map[string]struct{})
				for _, c := range d.Columns {
					primaryIndexColumnSet[string(c.Column)] = struct{}{}
				}
			}
			if d.Interleave != nil {
				return desc, pgerror.UnimplementedWithIssueError(9148, "use CREATE INDEX to make interleaved indexes")
			}
		case *tree.CheckConstraintTableDef, *tree.ForeignKeyConstraintTableDef, *tree.FamilyTableDef:
			// pass, handled below.

		default:
			return desc, errors.Errorf("unsupported table def: %T", def)
		}
	}

	if primaryIndexColumnSet != nil {
		// Primary index columns are not nullable.
		for i := range desc.Columns {
			if _, ok := primaryIndexColumnSet[desc.Columns[i].Name]; ok {
				desc.Columns[i].Nullable = false
			}
		}
	}

	// Now that all columns are in place, add any explicit families (this is done
	// here, rather than in the constraint pass below since we want to pick up
	// explicit allocations before AllocateIDs adds implicit ones).
	for _, def := range n.Defs {
		if d, ok := def.(*tree.FamilyTableDef); ok {
			fam := sqlbase.ColumnFamilyDescriptor{
				Name:        string(d.Name),
				ColumnNames: d.Columns.ToStrings(),
			}
			desc.AddFamily(fam)
		}
	}

	if err := desc.AllocateIDs(); err != nil {
		return desc, err
	}

	if n.Interleave != nil {
		if err := addInterleave(ctx, txn, vt, &desc, &desc.PrimaryIndex, n.Interleave); err != nil {
			return desc, err
		}
	}

	if n.PartitionBy != nil {
		partitioning, err := CreatePartitioning(
			ctx, st, evalCtx, &desc, &desc.PrimaryIndex, n.PartitionBy)
		if err != nil {
			return desc, err
		}
		desc.PrimaryIndex.Partitioning = partitioning
	}

	// With all structural elements in place and IDs allocated, we can resolve the
	// constraints and qualifications.
	// FKs are resolved after the descriptor is otherwise complete and IDs have
	// been allocated since the FKs will reference those IDs. Resolution also
	// accumulates updates to other tables (adding backreferences) in the passed
	// map -- anything in that map should be saved when the table is created.
	//

	// We use a fkSelfResolver so that name resolution can find the newly created
	// table.
	fkResolver := &fkSelfResolver{
		SchemaResolver: vt,
		newTableDesc:   &desc,
		newTableName:   n.Table.TableName(),
	}

	generatedNames := map[string]struct{}{}
	for _, def := range n.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			// Now that we have all the other columns set up, we can validate any
			// computed columns.
			if d.IsComputed() {
				if err := validateComputedColumn(desc, n, d, semaCtx, evalCtx); err != nil {
					return desc, err
				}
			}

		case *tree.IndexTableDef, *tree.UniqueConstraintTableDef, *tree.FamilyTableDef:
			// Pass, handled above.

		case *tree.CheckConstraintTableDef:
			ck, err := makeCheckConstraint(ctx, desc, d, generatedNames, semaCtx, evalCtx, *tableName)
			if err != nil {
				return desc, err
			}
			desc.Checks = append(desc.Checks, ck)

		case *tree.ForeignKeyConstraintTableDef:
			for _, col := range d.FromCols {
				col, _, err := desc.FindColumnByName(col)
				if err != nil {
					return desc, err
				}
				if err := col.CheckCanBeFKRef(); err != nil {
					return desc, err
				}
			}
			if err := resolveFK(ctx, txn, fkResolver, &desc, d, affected, sqlbase.ConstraintValidity_Validated); err != nil {
				return desc, err
			}
		default:
			return desc, errors.Errorf("unsupported table def: %T", def)
		}
	}

	// Multiple FKs from the same column would potentially result in ambiguous or
	// unexpected behavior with conflicting CASCADE/RESTRICT/etc behaviors.
	colsInFKs := make(map[sqlbase.ColumnID]struct{})
	for _, idx := range desc.Indexes {
		if idx.ForeignKey.IsSet() {
			numCols := len(idx.ColumnIDs)
			if idx.ForeignKey.SharedPrefixLen > 0 {
				numCols = int(idx.ForeignKey.SharedPrefixLen)
			}
			for i := 0; i < numCols; i++ {
				if _, ok := colsInFKs[idx.ColumnIDs[i]]; ok {
					return desc, fmt.Errorf(
						"column %q cannot be used by multiple foreign key constraints", idx.ColumnNames[i])
				}
				colsInFKs[idx.ColumnIDs[i]] = struct{}{}
			}
		}
	}

	// AllocateIDs mutates its receiver. `return desc, desc.AllocateIDs()`
	// happens to work in gc, but does not work in gccgo.
	//
	// See https://github.com/golang/go/issues/23188.
	err = desc.AllocateIDs()
	return desc, err
}

// makeTableDesc creates a table descriptor from a CreateTable statement.
func (p *planner) makeTableDesc(
	ctx context.Context,
	n *tree.CreateTable,
	parentID, id sqlbase.ID,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	affected map[sqlbase.ID]*sqlbase.TableDescriptor,
) (ret sqlbase.TableDescriptor, err error) {
	// We need to run MakeTableDesc with caching disabled, because
	// it needs to pull in descriptors from FK depended-on tables
	// and interleaved parents using their current state in KV.
	// See the comment at the start of MakeTableDesc() and resolveFK().
	p.runWithOptions(resolveFlags{allowAdding: true, skipCache: true}, func() {
		ret, err = MakeTableDesc(
			ctx,
			p.txn,
			p,
			p.ExecCfg().Settings,
			n,
			parentID,
			id,
			creationTime,
			privileges,
			affected,
			&p.semaCtx,
			p.EvalContext(),
		)
	})
	return ret, err
}

// dummyColumnItem is used in makeCheckConstraint to construct an expression
// that can be both type-checked and examined for variable expressions.
type dummyColumnItem struct {
	typ types.T
	// name is only used for error-reporting.
	name tree.Name
}

// String implements the Stringer interface.
func (d *dummyColumnItem) String() string {
	return tree.AsString(d)
}

// Format implements the NodeFormatter interface.
func (d *dummyColumnItem) Format(ctx *tree.FmtCtx) {
	d.name.Format(ctx)
}

// Walk implements the Expr interface.
func (d *dummyColumnItem) Walk(_ tree.Visitor) tree.Expr {
	return d
}

// TypeCheck implements the Expr interface.
func (d *dummyColumnItem) TypeCheck(_ *tree.SemaContext, desired types.T) (tree.TypedExpr, error) {
	return d, nil
}

// Eval implements the TypedExpr interface.
func (*dummyColumnItem) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic("dummyColumnItem.Eval() is undefined")
}

// ResolvedType implements the TypedExpr interface.
func (d *dummyColumnItem) ResolvedType() types.T {
	return d.typ
}

func generateNameForCheckConstraint(
	desc sqlbase.TableDescriptor, expr tree.Expr, inuseNames map[string]struct{},
) (string, error) {
	var nameBuf bytes.Buffer
	nameBuf.WriteString("check")

	if err := iterColDescriptorsInExpr(desc, expr, func(c sqlbase.ColumnDescriptor) error {
		nameBuf.WriteByte('_')
		nameBuf.WriteString(c.Name)
		return nil
	}); err != nil {
		return "", err
	}
	name := nameBuf.String()

	// If generated name isn't unique, attempt to add a number to the end to
	// get a unique name.
	if _, ok := inuseNames[name]; ok {
		i := 1
		for {
			appended := fmt.Sprintf("%s%d", name, i)
			if _, ok := inuseNames[appended]; !ok {
				name = appended
				break
			}
			i++
		}
	}
	if inuseNames != nil {
		inuseNames[name] = struct{}{}
	}

	return name, nil
}

func iterColDescriptorsInExpr(
	desc sqlbase.TableDescriptor, rootExpr tree.Expr, f func(sqlbase.ColumnDescriptor) error,
) error {
	_, err := tree.SimpleVisit(rootExpr, func(expr tree.Expr) (err error, recurse bool, newExpr tree.Expr) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
			// Not a VarName, don't do anything to this node.
			return nil, true, expr
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return err, false, nil
		}

		c, ok := v.(*tree.ColumnItem)
		if !ok {
			return nil, true, expr
		}

		col, err := desc.FindActiveColumnByName(string(c.ColumnName))
		if err != nil {
			return pgerror.NewErrorf(pgerror.CodeInvalidTableDefinitionError,
				"column %q not found, referenced in %q",
				c.ColumnName, rootExpr), false, nil
		}

		if err := f(col); err != nil {
			return err, false, nil
		}
		return nil, false, expr
	})

	return err
}

// validateComputedColumn checks that a computed column satisfies a number of
// validity constraints, for instance, that it typechecks.
func validateComputedColumn(
	desc sqlbase.TableDescriptor,
	t *tree.CreateTable,
	d *tree.ColumnTableDef,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
) error {
	dependencies := make(map[string]struct{})
	// First, check that no column in the expression is a computed column.
	if err := iterColDescriptorsInExpr(desc, d.Computed.Expr, func(c sqlbase.ColumnDescriptor) error {
		if c.IsComputed() {
			return pgerror.NewError(pgerror.CodeInvalidTableDefinitionError,
				"computed columns cannot reference other computed columns")
		}
		dependencies[c.Name] = struct{}{}

		return nil
	}); err != nil {
		return err
	}

	var tCtx transform.ExprTransformContext
	if err := tCtx.AssertNoAggregationOrWindowing(
		d.Computed.Expr, "computed column expressions", semaCtx.SearchPath,
	); err != nil {
		return err
	}

	// TODO(justin,bram): allow depending on columns like this. We disallow it
	// for now because cascading changes must hook into the computed column
	// update path.
	for _, def := range t.Defs {
		switch c := def.(type) {
		case *tree.ColumnTableDef:
			if _, ok := dependencies[string(c.Name)]; !ok {
				// We don't depend on this column.
				continue
			}
			for _, action := range []tree.ReferenceAction{
				c.References.Actions.Update,
				c.References.Actions.Delete,
			} {
				switch action {
				case tree.Cascade, tree.SetNull, tree.SetDefault:
					return pgerror.NewError(pgerror.CodeInvalidTableDefinitionError,
						"computed columns cannot reference non-restricted FK columns")
				}
			}
		}
	}

	// Replace column references with typed dummies to allow typechecking.
	replacedExpr, _, err := replaceVars(desc, d.Computed.Expr)
	if err != nil {
		return err
	}

	typedExpr, err := sqlbase.SanitizeVarFreeExpr(
		replacedExpr, coltypes.CastTargetToDatumType(d.Type), "computed column", semaCtx, evalCtx,
	)
	if err != nil {
		return err
	}

	if d.HasDefaultExpr() {
		return pgerror.NewError(
			pgerror.CodeInvalidTableDefinitionError,
			"computed columns cannot have default values",
		)
	}

	return validateComputedColumnHasNoImpureFunctions(typedExpr, d.Name)
}

// replaceVars replaces the occurrences of column names in an expression with
// dummies containing their type, so that they may be typechecked. It returns
// this new expression tree alongside a set containing the ColumnID of each
// column seen in the expression.
func replaceVars(
	desc sqlbase.TableDescriptor, expr tree.Expr,
) (tree.Expr, map[sqlbase.ColumnID]struct{}, error) {
	colIDs := make(map[sqlbase.ColumnID]struct{})
	newExpr, err := tree.SimpleVisit(expr, func(expr tree.Expr) (err error, recurse bool, newExpr tree.Expr) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
			// Not a VarName, don't do anything to this node.
			return nil, true, expr
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return err, false, nil
		}

		c, ok := v.(*tree.ColumnItem)
		if !ok {
			return nil, true, expr
		}

		col, err := desc.FindActiveColumnByName(string(c.ColumnName))
		if err != nil {
			return fmt.Errorf("column %q not found for constraint %q",
				c.ColumnName, expr.String()), false, nil
		}
		colIDs[col.ID] = struct{}{}
		// Convert to a dummy node of the correct type.
		return nil, false, &dummyColumnItem{typ: col.Type.ToDatumType(), name: c.ColumnName}
	})
	return newExpr, colIDs, err
}

func makeCheckConstraint(
	ctx context.Context,
	desc sqlbase.TableDescriptor,
	d *tree.CheckConstraintTableDef,
	inuseNames map[string]struct{},
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	tableName tree.TableName,
) (*sqlbase.TableDescriptor_CheckConstraint, error) {
	name := string(d.Name)

	if name == "" {
		var err error
		name, err = generateNameForCheckConstraint(desc, d.Expr, inuseNames)
		if err != nil {
			return nil, err
		}
	}

	expr, colIDsUsed, err := replaceVars(desc, d.Expr)
	if err != nil {
		return nil, err
	}

	var t transform.ExprTransformContext
	if err := t.AssertNoAggregationOrWindowing(expr, "CHECK expressions", semaCtx.SearchPath); err != nil {
		return nil, err
	}

	if _, err := sqlbase.SanitizeVarFreeExpr(
		expr, types.Bool, "CHECK", semaCtx, evalCtx,
	); err != nil {
		return nil, err
	}

	colIDs := make([]sqlbase.ColumnID, 0, len(colIDsUsed))
	for colID := range colIDsUsed {
		colIDs = append(colIDs, colID)
	}
	sort.Sort(sqlbase.ColumnIDs(colIDs))

	sourceInfo := sqlbase.NewSourceInfoForSingleTable(
		tableName, sqlbase.ResultColumnsFromColDescs(desc.Columns),
	)
	sources := sqlbase.MultiSourceInfo{sourceInfo}

	expr, err = dequalifyColumnRefs(ctx, sources, d.Expr)
	if err != nil {
		return nil, err
	}

	return &sqlbase.TableDescriptor_CheckConstraint{
		Expr:      tree.Serialize(expr),
		Name:      name,
		ColumnIDs: colIDs,
	}, nil
}
