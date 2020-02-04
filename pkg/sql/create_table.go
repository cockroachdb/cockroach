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
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/schema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

type createTableNode struct {
	n          *tree.CreateTable
	dbDesc     *sqlbase.DatabaseDescriptor
	sourcePlan planNode

	run createTableRun
}

// createTableRun contains the run-time state of createTableNode
// during local execution.
type createTableRun struct {
	autoCommit autoCommitOpt

	// synthRowID indicates whether an input column needs to be synthesized to
	// provide the default value for the hidden rowid column. The optimizer's plan
	// already includes this column if a user specified PK does not exist (so
	// synthRowID is false), whereas the heuristic planner's plan does not in this
	// case (so synthRowID is true).
	synthRowID bool

	// fromHeuristicPlanner indicates whether the planning was performed by the
	// heuristic planner instead of the optimizer. This is used to determine
	// whether or not a row_id was synthesized as part of the planning stage, if a
	// user defined PK is not specified.
	fromHeuristicPlanner bool
}

// storageParamType indicates the required type of a storage parameter.
type storageParamType int

// storageParamType values
const (
	storageParamBool storageParamType = iota
	storageParamInt
	storageParamFloat
	storageParamUnimplemented
)

var storageParamExpectedTypes = map[string]storageParamType{
	`fillfactor`:                                  storageParamInt,
	`toast_tuple_target`:                          storageParamUnimplemented,
	`parallel_workers`:                            storageParamUnimplemented,
	`autovacuum_enabled`:                          storageParamUnimplemented,
	`toast.autovacuum_enabled`:                    storageParamUnimplemented,
	`autovacuum_vacuum_threshold`:                 storageParamUnimplemented,
	`toast.autovacuum_vacuum_threshold`:           storageParamUnimplemented,
	`autovacuum_vacuum_scale_factor`:              storageParamUnimplemented,
	`toast.autovacuum_vacuum_scale_factor`:        storageParamUnimplemented,
	`autovacuum_analyze_threshold`:                storageParamUnimplemented,
	`autovacuum_analyze_scale_factor`:             storageParamUnimplemented,
	`autovacuum_vacuum_cost_delay`:                storageParamUnimplemented,
	`toast.autovacuum_vacuum_cost_delay`:          storageParamUnimplemented,
	`autovacuum_vacuum_cost_limit`:                storageParamUnimplemented,
	`autovacuum_freeze_min_age`:                   storageParamUnimplemented,
	`toast.autovacuum_freeze_min_age`:             storageParamUnimplemented,
	`autovacuum_freeze_max_age`:                   storageParamUnimplemented,
	`toast.autovacuum_freeze_max_age`:             storageParamUnimplemented,
	`autovacuum_freeze_table_age`:                 storageParamUnimplemented,
	`toast.autovacuum_freeze_table_age`:           storageParamUnimplemented,
	`autovacuum_multixact_freeze_min_age`:         storageParamUnimplemented,
	`toast.autovacuum_multixact_freeze_min_age`:   storageParamUnimplemented,
	`autovacuum_multixact_freeze_max_age`:         storageParamUnimplemented,
	`toast.autovacuum_multixact_freeze_max_age`:   storageParamUnimplemented,
	`autovacuum_multixact_freeze_table_age`:       storageParamUnimplemented,
	`toast.autovacuum_multixact_freeze_table_age`: storageParamUnimplemented,
	`log_autovacuum_min_duration`:                 storageParamUnimplemented,
	`toast.log_autovacuum_min_duration`:           storageParamUnimplemented,
	`user_catalog_table`:                          storageParamUnimplemented,
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because CREATE TABLE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *createTableNode) ReadingOwnWrites() {}

// getTableCreateParams returns the table key needed for the new table,
// as well as the schema id.
func getTableCreateParams(
	params runParams, dbID sqlbase.ID, isTemporary bool, tableName string,
) (sqlbase.DescriptorKey, sqlbase.ID, error) {
	// By default, all tables are created in the `public` schema.
	schemaID := sqlbase.ID(keys.PublicSchemaID)
	tKey := sqlbase.MakePublicTableNameKey(params.ctx,
		params.ExecCfg().Settings, dbID, tableName)
	if isTemporary {
		if !params.SessionData().TempTablesEnabled {
			return nil, 0, unimplemented.NewWithIssuef(5807,
				"temporary tables are unsupported")
		}

		tempSchemaName := params.p.TemporarySchemaName()
		sKey := sqlbase.NewSchemaKey(dbID, tempSchemaName)
		var err error
		schemaID, err = getDescriptorID(params.ctx, params.p.txn, sKey)
		if err != nil {
			return nil, 0, err
		} else if schemaID == sqlbase.InvalidID {
			// The temporary schema has not been created yet.
			if schemaID, err = createTempSchema(params, sKey); err != nil {
				return nil, 0, err
			}
		}

		tKey = sqlbase.NewTableKey(dbID, schemaID, tableName)
	}

	exists, _, err := sqlbase.LookupObjectID(params.ctx, params.p.txn, dbID, schemaID, tableName)
	if err == nil && exists {
		return nil, 0, sqlbase.NewRelationAlreadyExistsError(tableName)
	} else if err != nil {
		return nil, 0, err
	}
	return tKey, schemaID, nil
}

func (n *createTableNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeCreate("table"))
	isTemporary := n.n.Temporary

	tKey, schemaID, err := getTableCreateParams(params, n.dbDesc.ID, isTemporary, n.n.Table.Table())
	if err != nil {
		if sqlbase.IsRelationAlreadyExistsError(err) && n.n.IfNotExists {
			return nil
		}
		return err
	}

	if isTemporary {
		telemetry.Inc(sqltelemetry.CreateTempTableCounter)
	}

	// Guard against creating non-partitioned indexes on a partitioned table,
	// which is undesirable in most cases.
	if params.SessionData().SafeUpdates && n.n.PartitionBy != nil {
		for _, def := range n.n.Defs {
			if d, ok := def.(*tree.IndexTableDef); ok {
				if d.PartitionBy == nil {
					return pgerror.DangerousStatementf("non-partitioned index on partitioned table")
				}
			}
		}
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

	var asCols sqlbase.ResultColumns
	var desc sqlbase.MutableTableDescriptor
	var affected map[sqlbase.ID]*sqlbase.MutableTableDescriptor
	creationTime := params.creationTimeForNewTableDescriptor()
	if n.n.As() {
		asCols = planColumns(n.sourcePlan)
		if !n.run.fromHeuristicPlanner && !n.n.AsHasUserSpecifiedPrimaryKey() {
			// rowID column is already present in the input as the last column if it
			// was planned by the optimizer and the user did not specify a PRIMARY
			// KEY. So ignore it for the purpose of creating column metadata (because
			// makeTableDescIfAs does it automatically).
			asCols = asCols[:len(asCols)-1]
		}

		desc, err = makeTableDescIfAs(params,
			n.n, n.dbDesc.ID, schemaID, id, creationTime, asCols, privs, params.p.EvalContext(), isTemporary)
		if err != nil {
			return err
		}

		// If we have an implicit txn we want to run CTAS async, and consequently
		// ensure it gets queued as a SchemaChange.
		if params.p.ExtendedEvalContext().TxnImplicit {
			desc.State = sqlbase.TableDescriptor_ADD
		}
	} else {
		affected = make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor)
		desc, err = makeTableDesc(params, n.n, n.dbDesc.ID, schemaID, id, creationTime, privs, affected, isTemporary)
		if err != nil {
			return err
		}

		if desc.Adding() {
			// if this table and all its references are created in the same
			// transaction it can be made PUBLIC.
			refs, err := desc.FindAllReferences()
			if err != nil {
				return err
			}
			var foundExternalReference bool
			for id := range refs {
				if t := params.p.Tables().getUncommittedTableByID(id).MutableTableDescriptor; t == nil || !t.IsNewTable() {
					foundExternalReference = true
					break
				}
			}
			if !foundExternalReference {
				desc.State = sqlbase.TableDescriptor_PUBLIC
			}
		}
	}

	// Descriptor written to store here.
	if err := params.p.createDescriptorWithID(
		params.ctx, tKey.Key(), id, &desc, params.EvalContext().Settings); err != nil {
		return err
	}

	for _, updated := range affected {
		if err := params.p.writeSchemaChange(params.ctx, updated, sqlbase.InvalidMutationID); err != nil {
			return err
		}
	}

	for _, index := range desc.AllNonDropIndexes() {
		if len(index.Interleave.Ancestors) > 0 {
			if err := params.p.finalizeInterleave(params.ctx, &desc, index); err != nil {
				return err
			}
		}
	}

	if err := desc.Validate(params.ctx, params.p.txn); err != nil {
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
		}{n.n.Table.FQString(), n.n.String(), params.SessionData().User},
	); err != nil {
		return err
	}

	// If we are in an explicit txn or the source has placeholders, we execute the
	// CTAS query synchronously.
	if n.n.As() && !params.p.ExtendedEvalContext().TxnImplicit {
		err = func() error {
			// The data fill portion of CREATE AS must operate on a read snapshot,
			// so that it doesn't end up observing its own writes.
			prevMode := params.p.Txn().ConfigureStepping(params.ctx, client.SteppingEnabled)
			defer func() { _ = params.p.Txn().ConfigureStepping(params.ctx, prevMode) }()

			// This is a very simplified version of the INSERT logic: no CHECK
			// expressions, no FK checks, no arbitrary insertion order, no
			// RETURNING, etc.

			// Instantiate a row inserter and table writer. It has a 1-1
			// mapping to the definitions in the descriptor.
			ri, err := row.MakeInserter(
				params.ctx,
				params.p.txn,
				sqlbase.NewImmutableTableDescriptor(*desc.TableDesc()),
				desc.Columns,
				row.SkipFKs,
				nil, /* fkTables */
				&params.p.alloc)
			if err != nil {
				return err
			}
			ti := tableInserterPool.Get().(*tableInserter)
			*ti = tableInserter{ri: ri}
			tw := tableWriter(ti)
			if n.run.autoCommit == autoCommitEnabled {
				tw.enableAutoCommit()
			}
			defer func() {
				tw.close(params.ctx)
				*ti = tableInserter{}
				tableInserterPool.Put(ti)
			}()
			if err := tw.init(params.ctx, params.p.txn, params.p.EvalContext()); err != nil {
				return err
			}

			// Prepare the buffer for row values. At this point, one more column has
			// been added by ensurePrimaryKey() to the list of columns in sourcePlan, if
			// a PRIMARY KEY is not specified by the user.
			rowBuffer := make(tree.Datums, len(desc.Columns))
			pkColIdx := len(desc.Columns) - 1

			// The optimizer includes the rowID expression as part of the input
			// expression. But the heuristic planner does not do this, so construct
			// a rowID expression to be evaluated separately.
			var defTypedExpr tree.TypedExpr
			if n.run.synthRowID {
				// Prepare the rowID expression.
				defExprSQL := *desc.Columns[pkColIdx].DefaultExpr
				defExpr, err := parser.ParseExpr(defExprSQL)
				if err != nil {
					return err
				}
				defTypedExpr, err = params.p.analyzeExpr(
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
						params.ctx, params.extendedEvalCtx.Tracing.KVTracingEnabled())
					if err != nil {
						return err
					}
					break
				}

				// Populate the buffer and generate the PK value.
				copy(rowBuffer, n.sourcePlan.Values())
				if n.run.synthRowID {
					rowBuffer[pkColIdx], err = defTypedExpr.Eval(params.p.EvalContext())
					if err != nil {
						return err
					}
				}

				if err := tw.row(params.ctx, rowBuffer, params.extendedEvalCtx.Tracing.KVTracingEnabled()); err != nil {
					return err
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}

	// The CREATE STATISTICS run for an async CTAS query is initiated by the
	// SchemaChanger.
	if n.n.As() && params.p.autoCommit {
		return nil
	}

	// Initiate a run of CREATE STATISTICS. We use a large number
	// for rowsAffected because we want to make sure that stats always get
	// created/refreshed here.
	params.ExecCfg().StatsRefresher.NotifyMutation(desc.ID, math.MaxInt32 /* rowsAffected */)
	return nil
}

func (*createTableNode) Next(runParams) (bool, error) { return false, nil }
func (*createTableNode) Values() tree.Datums          { return tree.Datums{} }

func (n *createTableNode) Close(ctx context.Context) {
	if n.sourcePlan != nil {
		n.sourcePlan.Close(ctx)
		n.sourcePlan = nil
	}
}

// resolveFK on the planner calls resolveFK() on the current txn.
//
// The caller must make sure the planner is configured to look up
// descriptors without caching. See the comment on resolveFK().
func (p *planner) resolveFK(
	ctx context.Context,
	tbl *sqlbase.MutableTableDescriptor,
	d *tree.ForeignKeyConstraintTableDef,
	backrefs map[sqlbase.ID]*sqlbase.MutableTableDescriptor,
	ts FKTableState,
	validationBehavior tree.ValidationBehavior,
) error {
	return ResolveFK(ctx, p.txn, p, tbl, d, backrefs, ts, validationBehavior, p.ExecCfg().Settings)
}

func qualifyFKColErrorWithDB(
	ctx context.Context, txn *client.Txn, tbl *sqlbase.TableDescriptor, col string,
) string {
	if txn == nil {
		return tree.ErrString(tree.NewUnresolvedName(tbl.Name, col))
	}

	// TODO(solon): this ought to use a database cache.
	db, err := sqlbase.GetDatabaseDescFromID(ctx, txn, tbl.ParentID)
	if err != nil {
		return tree.ErrString(tree.NewUnresolvedName(tbl.Name, col))
	}
	schema, err := schema.ResolveNameByID(ctx, txn, db.ID, tbl.GetParentSchemaID())
	if err != nil {
		return tree.ErrString(tree.NewUnresolvedName(tbl.Name, col))
	}
	return tree.ErrString(tree.NewUnresolvedName(db.Name, schema, tbl.Name, col))
}

// FKTableState is the state of the referencing table resolveFK() is called on.
type FKTableState int

const (
	// NewTable represents a new table, where the FK constraint is specified in the
	// CREATE TABLE
	NewTable FKTableState = iota
	// EmptyTable represents an existing table that is empty
	EmptyTable
	// NonEmptyTable represents an existing non-empty table
	NonEmptyTable
)

// MaybeUpgradeDependentOldForeignKeyVersionTables upgrades the on-disk foreign key descriptor
// version of all table descriptors that have foreign key relationships with desc. This is intended
// to catch upgrade 19.1 version table descriptors that haven't been upgraded yet before an operation
// like drop index which could cause them to lose FK information in the old representation.
func (p *planner) MaybeUpgradeDependentOldForeignKeyVersionTables(
	ctx context.Context, desc *sqlbase.MutableTableDescriptor,
) error {
	// In order to avoid having old version foreign key descriptors that depend on this
	// index lose information when this index is dropped, ensure that they get updated.
	maybeUpgradeFKRepresentation := func(id sqlbase.ID) error {
		// Read the referenced table and see if the foreign key representation has changed. If it has, write
		// the upgraded descriptor back to disk.
		tbl, didUpgrade, err := sqlbase.GetTableDescFromIDWithFKsChanged(ctx, p.txn, id)
		if err != nil {
			return err
		}
		if didUpgrade {
			err := p.writeSchemaChange(ctx, sqlbase.NewMutableExistingTableDescriptor(*tbl), sqlbase.InvalidMutationID)
			if err != nil {
				return err
			}
		}
		return nil
	}
	for i := range desc.OutboundFKs {
		if err := maybeUpgradeFKRepresentation(desc.OutboundFKs[i].ReferencedTableID); err != nil {
			return err
		}
	}
	for i := range desc.InboundFKs {
		if err := maybeUpgradeFKRepresentation(desc.InboundFKs[i].OriginTableID); err != nil {
			return err
		}
	}
	return nil
}

// ResolveFK looks up the tables and columns mentioned in a `REFERENCES`
// constraint and adds metadata representing that constraint to the descriptor.
// It may, in doing so, add to or alter descriptors in the passed in `backrefs`
// map of other tables that need to be updated when this table is created.
// Constraints that are not known to hold for existing data are created
// "unvalidated", but when table is empty (e.g. during creation), no existing
// data implies no existing violations, and thus the constraint can be created
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
//
// The passed Txn is used to lookup databases to qualify names in error messages
// but if nil, will result in unqualified names in those errors.
//
// The passed validationBehavior is used to determine whether or not preexisting
// entries in the table need to be validated against the foreign key being added.
// This only applies for existing tables, not new tables.
func ResolveFK(
	ctx context.Context,
	txn *client.Txn,
	sc SchemaResolver,
	tbl *sqlbase.MutableTableDescriptor,
	d *tree.ForeignKeyConstraintTableDef,
	backrefs map[sqlbase.ID]*sqlbase.MutableTableDescriptor,
	ts FKTableState,
	validationBehavior tree.ValidationBehavior,
	settings *cluster.Settings,
) error {
	originColumnIDs := make(sqlbase.ColumnIDs, len(d.FromCols))
	for i, col := range d.FromCols {
		col, _, err := tbl.FindColumnByName(col)
		if err != nil {
			return err
		}
		if err := col.CheckCanBeFKRef(); err != nil {
			return err
		}
		originColumnIDs[i] = col.ID
	}

	target, err := ResolveMutableExistingObject(ctx, sc, &d.Table, true /*required*/, ResolveRequireTableDesc)
	if err != nil {
		return err
	}
	if tbl.Temporary != target.Temporary {
		tablePersistenceType := "permanent"
		if tbl.Temporary {
			tablePersistenceType = "temporary"
		}
		return pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"constraints on %s tables may reference only %s tables",
			tablePersistenceType,
			tablePersistenceType,
		)
	}
	if target.ID == tbl.ID {
		// When adding a self-ref FK to an _existing_ table, we want to make sure
		// we edit the same copy.
		target = tbl
	} else {
		// Since this FK is referencing another table, this table must be created in
		// a non-public "ADD" state and made public only after all leases on the
		// other table are updated to include the backref, if it does not already
		// exist.
		if ts == NewTable {
			tbl.State = sqlbase.TableDescriptor_ADD
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
		return pgerror.Newf(pgcode.Syntax,
			"%d columns must reference exactly %d columns in referenced table (found %d)",
			len(srcCols), len(srcCols), len(targetCols))
	}

	for i := range srcCols {
		if s, t := srcCols[i], targetCols[i]; !s.Type.Equivalent(&t.Type) {
			return pgerror.Newf(pgcode.DatatypeMismatch,
				"type of %q (%s) does not match foreign key %q.%q (%s)",
				s.Name, s.Type.String(), target.Name, t.Name, t.Type.String())
		}
	}

	// Verify we are not writing a constraint over the same name.
	// This check is done in Verify(), but we must do it earlier
	// or else we can hit other checks that break things with
	// undesired error codes, e.g. #42858.
	// It may be removable after #37255 is complete.
	constraintInfo, err := tbl.GetConstraintInfo(ctx, nil)
	if err != nil {
		return err
	}
	constraintName := string(d.Name)
	if constraintName == "" {
		constraintName = generateUniqueConstraintName(
			fmt.Sprintf("fk_%s_ref_%s", string(d.FromCols[0]), target.Name),
			func(p string) bool {
				_, ok := constraintInfo[p]
				return ok
			},
		)
	} else {
		if _, ok := constraintInfo[constraintName]; ok {
			return pgerror.Newf(pgcode.DuplicateObject, "duplicate constraint name: %q", constraintName)
		}
	}

	targetColIDs := make(sqlbase.ColumnIDs, len(targetCols))
	for i := range targetCols {
		targetColIDs[i] = targetCols[i].ID
	}

	// Don't add a SET NULL action on an index that has any column that is NOT
	// NULL.
	if d.Actions.Delete == tree.SetNull || d.Actions.Update == tree.SetNull {
		for _, sourceColumn := range srcCols {
			if !sourceColumn.Nullable {
				col := qualifyFKColErrorWithDB(ctx, txn, tbl.TableDesc(), sourceColumn.Name)
				return pgerror.Newf(pgcode.InvalidForeignKey,
					"cannot add a SET NULL cascading action on column %q which has a NOT NULL constraint", col,
				)
			}
		}
	}

	// Don't add a SET DEFAULT action on an index that has any column that has
	// a DEFAULT expression of NULL and a NOT NULL constraint.
	if d.Actions.Delete == tree.SetDefault || d.Actions.Update == tree.SetDefault {
		for _, sourceColumn := range srcCols {
			// Having a default expression of NULL, and a constraint of NOT NULL is a
			// contradiction and should never be allowed.
			if sourceColumn.DefaultExpr == nil && !sourceColumn.Nullable {
				col := qualifyFKColErrorWithDB(ctx, txn, tbl.TableDesc(), sourceColumn.Name)
				return pgerror.Newf(pgcode.InvalidForeignKey,
					"cannot add a SET DEFAULT cascading action on column %q which has a "+
						"NOT NULL constraint and a NULL default expression", col,
				)
			}
		}
	}

	var legacyOriginIndexID sqlbase.IndexID
	// Search for an index on the origin table that matches. If one doesn't exist,
	// we create one automatically if the table to alter is new or empty.
	originIdx, err := sqlbase.FindFKOriginIndex(tbl.TableDesc(), originColumnIDs)
	if err == nil {
		// If there was no error, we found a suitable index.
		legacyOriginIndexID = originIdx.ID
	} else {
		// No existing suitable index was found.
		if ts == NonEmptyTable {
			var colNames bytes.Buffer
			colNames.WriteString(`("`)
			for i, id := range originColumnIDs {
				if i != 0 {
					colNames.WriteString(`", "`)
				}
				col, err := tbl.TableDesc().FindColumnByID(id)
				if err != nil {
					return err
				}
				colNames.WriteString(col.Name)
			}
			colNames.WriteString(`")`)
			return pgerror.Newf(pgcode.ForeignKeyViolation,
				"foreign key requires an existing index on columns %s", colNames.String())
		}
		id, err := addIndexForFK(tbl, srcCols, constraintName, ts)
		if err != nil {
			return err
		}
		legacyOriginIndexID = id
	}

	referencedIdx, err := sqlbase.FindFKReferencedIndex(target.TableDesc(), targetColIDs)
	if err != nil {
		return err
	}
	legacyReferencedIndexID := referencedIdx.ID

	var validity sqlbase.ConstraintValidity
	if ts != NewTable {
		if validationBehavior == tree.ValidationSkip {
			validity = sqlbase.ConstraintValidity_Unvalidated
		} else {
			validity = sqlbase.ConstraintValidity_Validating
		}
	}

	ref := sqlbase.ForeignKeyConstraint{
		OriginTableID:         tbl.ID,
		OriginColumnIDs:       originColumnIDs,
		ReferencedColumnIDs:   targetColIDs,
		ReferencedTableID:     target.ID,
		Name:                  constraintName,
		Validity:              validity,
		OnDelete:              sqlbase.ForeignKeyReferenceActionValue[d.Actions.Delete],
		OnUpdate:              sqlbase.ForeignKeyReferenceActionValue[d.Actions.Update],
		Match:                 sqlbase.CompositeKeyMatchMethodValue[d.Match],
		LegacyOriginIndex:     legacyOriginIndexID,
		LegacyReferencedIndex: legacyReferencedIndexID,
	}

	if ts == NewTable {
		tbl.OutboundFKs = append(tbl.OutboundFKs, ref)
		target.InboundFKs = append(target.InboundFKs, ref)
	} else {
		tbl.AddForeignKeyMutation(&ref, sqlbase.DescriptorMutation_ADD)
	}

	return nil
}

// generateUniqueConstraintName attempts to generate a unique constraint name
// with the given prefix.
// It will first try prefix by itself, then it will subsequently try
// adding numeric digits at the end, starting from 1.
func generateUniqueConstraintName(prefix string, nameExistsFunc func(name string) bool) string {
	name := prefix
	for i := 1; nameExistsFunc(name); i++ {
		name = fmt.Sprintf("%s_%d", prefix, i)
	}
	return name
}

// Adds an index to a table descriptor (that is in the process of being created)
// that will support using `srcCols` as the referencing (src) side of an FK.
func addIndexForFK(
	tbl *sqlbase.MutableTableDescriptor,
	srcCols []sqlbase.ColumnDescriptor,
	constraintName string,
	ts FKTableState,
) (sqlbase.IndexID, error) {
	// No existing index for the referencing columns found, so we add one.
	idx := sqlbase.IndexDescriptor{
		Name:             fmt.Sprintf("%s_auto_index_%s", tbl.Name, constraintName),
		ColumnNames:      make([]string, len(srcCols)),
		ColumnDirections: make([]sqlbase.IndexDescriptor_Direction, len(srcCols)),
	}
	for i, c := range srcCols {
		idx.ColumnDirections[i] = sqlbase.IndexDescriptor_ASC
		idx.ColumnNames[i] = c.Name
	}

	if ts == NewTable {
		if err := tbl.AddIndex(idx, false); err != nil {
			return 0, err
		}
		if err := tbl.AllocateIDs(); err != nil {
			return 0, err
		}
		added := tbl.Indexes[len(tbl.Indexes)-1]
		return added.ID, nil
	}

	// TODO (lucy): In the EmptyTable case, we add an index mutation, making this
	// the only case where a foreign key is added to an index being added.
	// Allowing FKs to be added to other indexes/columns also being added should
	// be a generalization of this special case.
	if err := tbl.AddIndexMutation(&idx, sqlbase.DescriptorMutation_ADD); err != nil {
		return 0, err
	}
	if err := tbl.AllocateIDs(); err != nil {
		return 0, err
	}
	id := tbl.Mutations[len(tbl.Mutations)-1].GetIndex().ID
	return id, nil
}

func (p *planner) addInterleave(
	ctx context.Context,
	desc *sqlbase.MutableTableDescriptor,
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
	desc *sqlbase.MutableTableDescriptor,
	index *sqlbase.IndexDescriptor,
	interleave *tree.InterleaveDef,
) error {
	if interleave.DropBehavior != tree.DropDefault {
		return unimplemented.NewWithIssuef(
			7854, "unsupported shorthand %s", interleave.DropBehavior)
	}

	parentTable, err := ResolveExistingObject(
		ctx, vt, &interleave.Parent, tree.ObjectLookupFlagsWithRequired(), ResolveRequireTableDesc,
	)
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
		return pgerror.Newf(
			pgcode.InvalidSchemaDefinition,
			"declared interleaved columns (%s) must match the parent's primary index (%s)",
			&interleave.Fields,
			strings.Join(parentIndex.ColumnNames, ", "),
		)
	}
	if len(interleave.Fields) > len(index.ColumnIDs) {
		return pgerror.Newf(
			pgcode.InvalidSchemaDefinition,
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
			return pgerror.Newf(
				pgcode.InvalidSchemaDefinition,
				"declared interleaved columns (%s) must refer to a prefix of the %s column names being interleaved (%s)",
				&interleave.Fields,
				typeOfIndex,
				strings.Join(index.ColumnNames, ", "),
			)
		}
		if !col.Type.Identical(&targetCol.Type) || index.ColumnDirections[i] != parentIndex.ColumnDirections[i] {
			return pgerror.Newf(
				pgcode.InvalidSchemaDefinition,
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
	ctx context.Context, desc *sqlbase.MutableTableDescriptor, index *sqlbase.IndexDescriptor,
) error {
	// TODO(dan): This is similar to finalizeFKs. Consolidate them
	if len(index.Interleave.Ancestors) == 0 {
		return nil
	}
	// Only the last ancestor needs the backreference.
	ancestor := index.Interleave.Ancestors[len(index.Interleave.Ancestors)-1]
	var ancestorTable *sqlbase.MutableTableDescriptor
	if ancestor.TableID == desc.ID {
		ancestorTable = desc
	} else {
		var err error
		ancestorTable, err = p.Tables().getMutableTableVersionByID(ctx, ancestor.TableID, p.txn)
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

	if err := p.writeSchemaChange(ctx, ancestorTable, sqlbase.InvalidMutationID); err != nil {
		return err
	}

	if desc.State == sqlbase.TableDescriptor_ADD {
		desc.State = sqlbase.TableDescriptor_PUBLIC

		if err := p.writeSchemaChange(ctx, desc, sqlbase.InvalidMutationID); err != nil {
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
	tableDesc *sqlbase.MutableTableDescriptor,
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
	tableDesc *sqlbase.MutableTableDescriptor,
	indexDesc *sqlbase.IndexDescriptor,
	partBy *tree.PartitionBy,
) (sqlbase.PartitioningDescriptor, error) {
	return sqlbase.PartitioningDescriptor{}, sqlbase.NewCCLRequiredError(errors.New(
		"creating or manipulating partitions requires a CCL binary"))
}

// InitTableDescriptor returns a blank TableDescriptor.
func InitTableDescriptor(
	id, parentID, parentSchemaID sqlbase.ID,
	name string,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	temporary bool,
) sqlbase.MutableTableDescriptor {
	return *sqlbase.NewMutableCreatedTableDescriptor(sqlbase.TableDescriptor{
		ID:                      id,
		Name:                    name,
		ParentID:                parentID,
		UnexposedParentSchemaID: parentSchemaID,
		FormatVersion:           sqlbase.InterleavedFormatVersion,
		Version:                 1,
		ModificationTime:        creationTime,
		Privileges:              privileges,
		CreateAsOfTime:          creationTime,
		Temporary:               temporary,
	})
}

func getFinalSourceQuery(source *tree.Select, evalCtx *tree.EvalContext) string {
	// Ensure that all the table names pretty-print as fully qualified, so we
	// store that in the table descriptor.
	//
	// The traversal will update the TableNames in-place, so the changes are
	// persisted in n.n.AsSource. We exploit the fact that planning step above
	// has populated any missing db/schema details in the table names in-place.
	// We use tree.FormatNode merely as a traversal method; its output buffer is
	// discarded immediately after the traversal because it is not needed
	// further.
	f := tree.NewFmtCtx(tree.FmtParsable)
	f.SetReformatTableNames(
		func(_ *tree.FmtCtx, tn *tree.TableName) {
			// Persist the database prefix expansion.
			if tn.SchemaName != "" {
				// All CTE or table aliases have no schema
				// information. Those do not turn into explicit.
				tn.ExplicitSchema = true
				tn.ExplicitCatalog = true
			}
		},
	)
	f.FormatNode(source)
	f.Close()

	// Substitute placeholders with their values.
	ctx := tree.NewFmtCtx(tree.FmtParsable)
	ctx.SetPlaceholderFormat(func(ctx *tree.FmtCtx, placeholder *tree.Placeholder) {
		d, err := placeholder.Eval(evalCtx)
		if err != nil {
			panic(fmt.Sprintf("failed to serialize placeholder: %s", err))
		}
		d.Format(ctx)
	})
	ctx.FormatNode(source)

	return ctx.CloseAndGetString()
}

// makeTableDescIfAs is the MakeTableDesc method for when we have a table
// that is created with the CREATE AS format.
func makeTableDescIfAs(
	params runParams,
	p *tree.CreateTable,
	parentID, parentSchemaID, id sqlbase.ID,
	creationTime hlc.Timestamp,
	resultColumns []sqlbase.ResultColumn,
	privileges *sqlbase.PrivilegeDescriptor,
	evalContext *tree.EvalContext,
	temporary bool,
) (desc sqlbase.MutableTableDescriptor, err error) {
	colResIndex := 0
	// TableDefs for a CREATE TABLE ... AS AST node comprise of a ColumnTableDef
	// for each column, and a ConstraintTableDef for any constraints on those
	// columns.
	for _, defs := range p.Defs {
		var d *tree.ColumnTableDef
		var ok bool
		if d, ok = defs.(*tree.ColumnTableDef); ok {
			d.Type = resultColumns[colResIndex].Typ
			colResIndex++
		}
	}

	// If there are no TableDefs defined by the parser, then we construct a
	// ColumnTableDef for each column using resultColumns.
	if len(p.Defs) == 0 {
		for _, colRes := range resultColumns {
			var d *tree.ColumnTableDef
			var ok bool
			var tableDef tree.TableDef = &tree.ColumnTableDef{Name: tree.Name(colRes.Name), Type: colRes.Typ}
			if d, ok = tableDef.(*tree.ColumnTableDef); !ok {
				return desc, errors.Errorf("failed to cast type to ColumnTableDef\n")
			}
			d.Nullable.Nullability = tree.SilentNull
			p.Defs = append(p.Defs, tableDef)
		}
	}

	desc, err = makeTableDesc(
		params,
		p,
		parentID, parentSchemaID, id,
		creationTime,
		privileges,
		nil, /* affected */
		temporary,
	)
	desc.CreateQuery = getFinalSourceQuery(p.AsSource, evalContext)
	return desc, err
}

func dequalifyColumnRefs(
	ctx context.Context, source *sqlbase.DataSourceInfo, expr tree.Expr,
) (tree.Expr, error) {
	resolver := sqlbase.ColumnResolver{Source: source}
	return tree.SimpleVisit(
		expr,
		func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
			if vBase, ok := expr.(tree.VarName); ok {
				v, err := vBase.NormalizeVarName()
				if err != nil {
					return false, nil, err
				}
				if c, ok := v.(*tree.ColumnItem); ok {
					_, err := c.Resolve(ctx, &resolver)
					if err != nil {
						return false, nil, err
					}
					colIdx := resolver.ResolverState.ColIdx
					col := source.SourceColumns[colIdx]
					return false, &tree.ColumnItem{ColumnName: tree.Name(col.Name)}, nil
				}
			}
			return true, expr, err
		},
	)
}

// MakeTableDesc creates a table descriptor from a CreateTable statement.
//
// txn and vt can be nil if the table to be created does not contain references
// to other tables (e.g. foreign keys or interleaving). This is useful at
// bootstrap when creating descriptors for virtual tables.
//
// parentID refers to the databaseID under which the descriptor is being
// created,and parentSchemaID refers to the schemaID of the schema under which
// the descriptor is being created.
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
//
// If the table definition *may* use the SERIAL type, the caller is
// also responsible for processing serial types using
// processSerialInColumnDef() on every column definition, and creating
// the necessary sequences in KV before calling MakeTableDesc().
func MakeTableDesc(
	ctx context.Context,
	txn *client.Txn,
	vt SchemaResolver,
	st *cluster.Settings,
	n *tree.CreateTable,
	parentID, parentSchemaID, id sqlbase.ID,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	affected map[sqlbase.ID]*sqlbase.MutableTableDescriptor,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	temporary bool,
) (sqlbase.MutableTableDescriptor, error) {
	// Used to delay establishing Column/Sequence dependency until ColumnIDs have
	// been populated.
	columnDefaultExprs := make([]tree.TypedExpr, len(n.Defs))

	desc := InitTableDescriptor(
		id, parentID, parentSchemaID, n.Table.Table(), creationTime, privileges, temporary,
	)

	if err := checkStorageParameters(semaCtx, n.StorageParams, storageParamExpectedTypes); err != nil {
		return desc, err
	}

	// If all nodes in the cluster know how to handle secondary indexes with column families,
	// write the new version into new index descriptors.
	indexEncodingVersion := sqlbase.BaseIndexFormatVersion
	// We can't use cluster.Version.IsActive because this method is sometimes called
	// before the version has been initialized, leading to a panic. There are also
	// cases where this function is called in tests where st is nil.
	if st != nil {
		if version := cluster.Version.ActiveVersionOrEmpty(ctx, st); version != (cluster.ClusterVersion{}) &&
			version.IsActive(cluster.VersionSecondaryIndexColumnFamilies) {
			indexEncodingVersion = sqlbase.SecondaryIndexFamilyFormatVersion
		}
	}

	for i, def := range n.Defs {
		if d, ok := def.(*tree.ColumnTableDef); ok {
			if !desc.IsVirtualTable() {
				switch d.Type.Oid() {
				case oid.T_int2vector, oid.T_oidvector:
					return desc, pgerror.Newf(
						pgcode.FeatureNotSupported,
						"VECTOR column types are unsupported",
					)
				}
			}
			col, idx, expr, err := sqlbase.MakeColumnDefDescs(d, semaCtx)
			if err != nil {
				return desc, err
			}

			desc.AddColumn(col)
			if d.HasDefaultExpr() {
				// This resolution must be delayed until ColumnIDs have been populated.
				columnDefaultExprs[i] = expr
			} else {
				columnDefaultExprs[i] = nil
			}

			if idx != nil {
				idx.Version = indexEncodingVersion
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
		n.Table, sqlbase.ResultColumnsFromColDescs(desc.Columns),
	)

	for i := range desc.Columns {
		col := &desc.Columns[i]
		if col.IsComputed() {
			expr, err := parser.ParseExpr(*col.ComputeExpr)
			if err != nil {
				return desc, err
			}

			expr, err = dequalifyColumnRefs(ctx, sourceInfo, expr)
			if err != nil {
				return desc, err
			}
			serialized := tree.Serialize(expr)
			col.ComputeExpr = &serialized
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
				Version:          indexEncodingVersion,
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
				return desc, unimplemented.NewWithIssue(9148, "use CREATE INDEX to make interleaved indexes")
			}
		case *tree.UniqueConstraintTableDef:
			idx := sqlbase.IndexDescriptor{
				Name:             string(d.Name),
				Unique:           true,
				StoreColumnNames: d.Storing.ToStrings(),
				Version:          indexEncodingVersion,
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
				return desc, unimplemented.NewWithIssue(9148, "use CREATE INDEX to make interleaved indexes")
			}
		case *tree.CheckConstraintTableDef, *tree.ForeignKeyConstraintTableDef, *tree.FamilyTableDef:
			// pass, handled below.

		default:
			return desc, errors.Errorf("unsupported table def: %T", def)
		}
	}

	// If explicit primary keys are required, error out since a primary key was not supplied.
	if len(desc.PrimaryIndex.ColumnNames) == 0 && desc.IsPhysicalTable() && evalCtx != nil &&
		evalCtx.SessionData != nil && evalCtx.SessionData.RequireExplicitPrimaryKeys {
		return desc, errors.Errorf(
			"no primary key specified for table %s (require_explicit_primary_keys = true)", desc.Name)
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

	// If any nodes are not at version VersionPrimaryKeyColumnsOutOfFamilyZero, then return an error
	// if a primary key column is not in column family 0.
	if st != nil {
		if version := cluster.Version.ActiveVersionOrEmpty(ctx, st); version != (cluster.ClusterVersion{}) &&
			!version.IsActive(cluster.VersionPrimaryKeyColumnsOutOfFamilyZero) {
			var colsInFamZero util.FastIntSet
			for _, colID := range desc.Families[0].ColumnIDs {
				colsInFamZero.Add(int(colID))
			}
			for _, colID := range desc.PrimaryIndex.ColumnIDs {
				if !colsInFamZero.Contains(int(colID)) {
					return desc, errors.Errorf("primary key column %d is not in column family 0", colID)
				}
			}
		}
	}

	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		// Increment the counter if this index could be storing data across multiple column families.
		if len(idx.StoreColumnNames) > 1 && len(desc.Families) > 1 {
			telemetry.Inc(sqltelemetry.SecondaryIndexColumnFamiliesCounter)
		}
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

	// Once all the IDs have been allocated, we can add the Sequence dependencies
	// as maybeAddSequenceDependencies requires ColumnIDs to be correct.
	// Elements in n.Defs are not necessarily column definitions, so use a separate
	// counter to map ColumnDefs to columns.
	colIdx := 0
	for i := range n.Defs {
		if _, ok := n.Defs[i].(*tree.ColumnTableDef); ok {
			if expr := columnDefaultExprs[i]; expr != nil {
				changedSeqDescs, err := maybeAddSequenceDependencies(ctx, vt, &desc, &desc.Columns[colIdx], expr, affected)
				if err != nil {
					return desc, err
				}
				for _, changedSeqDesc := range changedSeqDescs {
					affected[changedSeqDesc.ID] = changedSeqDesc
				}
			}
			colIdx++
		}
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
		newTableDesc:   desc.TableDesc(),
		newTableName:   &n.Table,
	}

	generatedNames := map[string]struct{}{}
	for _, def := range n.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			// Check after all ResolveFK calls.

		case *tree.IndexTableDef, *tree.UniqueConstraintTableDef, *tree.FamilyTableDef:
			// Pass, handled above.

		case *tree.CheckConstraintTableDef:
			ck, err := MakeCheckConstraint(ctx, &desc, d, generatedNames, semaCtx, n.Table)
			if err != nil {
				return desc, err
			}
			desc.Checks = append(desc.Checks, ck)

		case *tree.ForeignKeyConstraintTableDef:
			if err := ResolveFK(ctx, txn, fkResolver, &desc, d, affected, NewTable, tree.ValidationDefault, st); err != nil {
				return desc, err
			}

		default:
			return desc, errors.Errorf("unsupported table def: %T", def)
		}
	}

	// Now that we have all the other columns set up, we can validate
	// any computed columns.
	for _, def := range n.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			if d.IsComputed() {
				if err := validateComputedColumn(&desc, d, semaCtx); err != nil {
					return desc, err
				}
			}
		}
	}

	// AllocateIDs mutates its receiver. `return desc, desc.AllocateIDs()`
	// happens to work in gc, but does not work in gccgo.
	//
	// See https://github.com/golang/go/issues/23188.
	err := desc.AllocateIDs()
	return desc, err
}

func checkStorageParameters(
	semaCtx *tree.SemaContext, params tree.StorageParams, expectedTypes map[string]storageParamType,
) error {
	for _, sp := range params {
		k := string(sp.Key)
		validate, ok := expectedTypes[k]
		if !ok {
			return errors.Errorf("invalid storage parameter %q", k)
		}
		if sp.Value == nil {
			return errors.Errorf("storage parameter %q requires a value", k)
		}
		var expectedType *types.T
		if validate == storageParamBool {
			expectedType = types.Bool
		} else if validate == storageParamInt {
			expectedType = types.Int
		} else if validate == storageParamFloat {
			expectedType = types.Float
		} else {
			return unimplemented.NewWithIssuef(43299, "storage parameter %q", k)
		}

		_, err := tree.TypeCheckAndRequire(sp.Value, semaCtx, expectedType, k)
		if err != nil {
			return err
		}
	}
	return nil
}

// makeTableDesc creates a table descriptor from a CreateTable statement.
func makeTableDesc(
	params runParams,
	n *tree.CreateTable,
	parentID, parentSchemaID, id sqlbase.ID,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	affected map[sqlbase.ID]*sqlbase.MutableTableDescriptor,
	temporary bool,
) (ret sqlbase.MutableTableDescriptor, err error) {
	// Process any SERIAL columns to remove the SERIAL type,
	// as required by MakeTableDesc.
	createStmt := n
	ensureCopy := func() {
		if createStmt == n {
			newCreateStmt := *n
			n.Defs = append(tree.TableDefs(nil), n.Defs...)
			createStmt = &newCreateStmt
		}
	}
	for i, def := range n.Defs {
		d, ok := def.(*tree.ColumnTableDef)
		if !ok {
			continue
		}
		telemetry.Inc(sqltelemetry.SchemaNewTypeCounter(d.Type.TelemetryName()))
		newDef, seqDbDesc, seqName, seqOpts, err := params.p.processSerialInColumnDef(params.ctx, d, &n.Table)
		if err != nil {
			return ret, err
		}
		if seqName != nil {
			if err := doCreateSequence(params, n.String(), seqDbDesc, seqName, seqOpts); err != nil {
				return ret, err
			}
		}
		if d != newDef {
			ensureCopy()
			n.Defs[i] = newDef
		}
	}

	// We need to run MakeTableDesc with caching disabled, because
	// it needs to pull in descriptors from FK depended-on tables
	// and interleaved parents using their current state in KV.
	// See the comment at the start of MakeTableDesc() and resolveFK().
	params.p.runWithOptions(resolveFlags{skipCache: true}, func() {
		ret, err = MakeTableDesc(
			params.ctx,
			params.p.txn,
			params.p,
			params.p.ExecCfg().Settings,
			n,
			parentID,
			parentSchemaID,
			id,
			creationTime,
			privileges,
			affected,
			&params.p.semaCtx,
			params.EvalContext(),
			temporary,
		)
	})
	return ret, err
}

// dummyColumnItem is used in MakeCheckConstraint to construct an expression
// that can be both type-checked and examined for variable expressions.
type dummyColumnItem struct {
	typ *types.T
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
func (d *dummyColumnItem) TypeCheck(_ *tree.SemaContext, desired *types.T) (tree.TypedExpr, error) {
	return d, nil
}

// Eval implements the TypedExpr interface.
func (*dummyColumnItem) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic("dummyColumnItem.Eval() is undefined")
}

// ResolvedType implements the TypedExpr interface.
func (d *dummyColumnItem) ResolvedType() *types.T {
	return d.typ
}

func generateNameForCheckConstraint(
	desc *sqlbase.MutableTableDescriptor, expr tree.Expr, inuseNames map[string]struct{},
) (string, error) {
	var nameBuf bytes.Buffer
	nameBuf.WriteString("check")

	if err := iterColDescriptorsInExpr(desc, expr, func(c *sqlbase.ColumnDescriptor) error {
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
	desc *sqlbase.MutableTableDescriptor, rootExpr tree.Expr, f func(*sqlbase.ColumnDescriptor) error,
) error {
	_, err := tree.SimpleVisit(rootExpr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
			// Not a VarName, don't do anything to this node.
			return true, expr, nil
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return false, nil, err
		}

		c, ok := v.(*tree.ColumnItem)
		if !ok {
			return true, expr, nil
		}

		col, dropped, err := desc.FindColumnByName(c.ColumnName)
		if err != nil || dropped {
			return false, nil, pgerror.Newf(pgcode.InvalidTableDefinition,
				"column %q not found, referenced in %q",
				c.ColumnName, rootExpr)
		}

		if err := f(col); err != nil {
			return false, nil, err
		}
		return false, expr, err
	})

	return err
}

// validateComputedColumn checks that a computed column satisfies a number of
// validity constraints, for instance, that it typechecks.
func validateComputedColumn(
	desc *sqlbase.MutableTableDescriptor, d *tree.ColumnTableDef, semaCtx *tree.SemaContext,
) error {
	if d.HasDefaultExpr() {
		return pgerror.New(
			pgcode.InvalidTableDefinition,
			"computed columns cannot have default values",
		)
	}

	dependencies := make(map[sqlbase.ColumnID]struct{})
	// First, check that no column in the expression is a computed column.
	if err := iterColDescriptorsInExpr(desc, d.Computed.Expr, func(c *sqlbase.ColumnDescriptor) error {
		if c.IsComputed() {
			return pgerror.New(pgcode.InvalidTableDefinition,
				"computed columns cannot reference other computed columns")
		}
		dependencies[c.ID] = struct{}{}

		return nil
	}); err != nil {
		return err
	}

	// TODO(justin,bram): allow depending on columns like this. We disallow it
	// for now because cascading changes must hook into the computed column
	// update path.
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		for _, id := range fk.OriginColumnIDs {
			if _, ok := dependencies[id]; !ok {
				// We don't depend on this column.
				continue
			}
			for _, action := range []sqlbase.ForeignKeyReference_Action{
				fk.OnDelete,
				fk.OnUpdate,
			} {
				switch action {
				case sqlbase.ForeignKeyReference_CASCADE,
					sqlbase.ForeignKeyReference_SET_NULL,
					sqlbase.ForeignKeyReference_SET_DEFAULT:
					return pgerror.New(pgcode.InvalidTableDefinition,
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

	if _, err := sqlbase.SanitizeVarFreeExpr(
		replacedExpr, d.Type, "computed column", semaCtx, false, /* allowImpure */
	); err != nil {
		return err
	}

	return nil
}

// replaceVars replaces the occurrences of column names in an expression with
// dummies containing their type, so that they may be typechecked. It returns
// this new expression tree alongside a set containing the ColumnID of each
// column seen in the expression.
func replaceVars(
	desc *sqlbase.MutableTableDescriptor, expr tree.Expr,
) (tree.Expr, map[sqlbase.ColumnID]struct{}, error) {
	colIDs := make(map[sqlbase.ColumnID]struct{})
	newExpr, err := tree.SimpleVisit(expr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
			// Not a VarName, don't do anything to this node.
			return true, expr, nil
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return false, nil, err
		}

		c, ok := v.(*tree.ColumnItem)
		if !ok {
			return true, expr, nil
		}

		col, dropped, err := desc.FindColumnByName(c.ColumnName)
		if err != nil || dropped {
			return false, nil, fmt.Errorf("column %q not found for constraint %q",
				c.ColumnName, expr.String())
		}
		colIDs[col.ID] = struct{}{}
		// Convert to a dummy node of the correct type.
		return false, &dummyColumnItem{typ: &col.Type, name: c.ColumnName}, nil
	})
	return newExpr, colIDs, err
}

// MakeCheckConstraint makes a descriptor representation of a check from a def.
func MakeCheckConstraint(
	ctx context.Context,
	desc *sqlbase.MutableTableDescriptor,
	d *tree.CheckConstraintTableDef,
	inuseNames map[string]struct{},
	semaCtx *tree.SemaContext,
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

	if _, err := sqlbase.SanitizeVarFreeExpr(
		expr, types.Bool, "CHECK", semaCtx, true, /* allowImpure */
	); err != nil {
		return nil, err
	}

	colIDs := make([]sqlbase.ColumnID, 0, len(colIDsUsed))
	for colID := range colIDsUsed {
		colIDs = append(colIDs, colID)
	}
	sort.Sort(sqlbase.ColumnIDs(colIDs))

	sourceInfo := sqlbase.NewSourceInfoForSingleTable(
		tableName, sqlbase.ResultColumnsFromColDescs(desc.TableDesc().AllNonDropColumns()),
	)

	expr, err = dequalifyColumnRefs(ctx, sourceInfo, d.Expr)
	if err != nil {
		return nil, err
	}

	return &sqlbase.TableDescriptor_CheckConstraint{
		Expr:      tree.Serialize(expr),
		Name:      name,
		ColumnIDs: colIDs,
	}, nil
}
