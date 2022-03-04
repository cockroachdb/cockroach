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
	"go/constant"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/lib/pq/oid"
)

type createTableNode struct {
	n          *tree.CreateTable
	dbDesc     catalog.DatabaseDescriptor
	sourcePlan planNode
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because CREATE TABLE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *createTableNode) ReadingOwnWrites() {}

// getNonTemporarySchemaForCreate returns the schema in which to create an object.
// Note that it does not handle the temporary schema -- if the requested schema
// is temporary, the caller needs to use (*planner).getOrCreateTemporarySchema.
func (p *planner) getNonTemporarySchemaForCreate(
	ctx context.Context, db catalog.DatabaseDescriptor, scName string,
) (catalog.SchemaDescriptor, error) {
	res, err := p.Descriptors().GetMutableSchemaByName(
		ctx, p.txn, db, scName, tree.SchemaLookupFlags{
			Required:       true,
			RequireMutable: true,
		})
	if err != nil {
		return nil, err
	}
	switch res.SchemaKind() {
	case catalog.SchemaPublic, catalog.SchemaUserDefined:
		return res, nil
	case catalog.SchemaVirtual:
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege, "schema cannot be modified: %q", scName)
	default:
		return nil, errors.AssertionFailedf(
			"invalid schema kind for getNonTemporarySchemaForCreate: %d", res.SchemaKind())
	}
}

// getSchemaForCreateTable returns the table key needed for the new table,
// as well as the schema id. It returns valid data in the case that
// the desired object exists.
func getSchemaForCreateTable(
	params runParams,
	db catalog.DatabaseDescriptor,
	persistence tree.Persistence,
	tableName *tree.TableName,
	kind tree.RequiredTableKind,
	ifNotExists bool,
) (schema catalog.SchemaDescriptor, err error) {
	// Check we are not creating a table which conflicts with an alias available
	// as a built-in type in CockroachDB but an extension type on the public
	// schema for PostgreSQL.
	if tableName.Schema() == tree.PublicSchema {
		if _, ok := types.PublicSchemaAliases[tableName.Object()]; ok {
			return nil, sqlerrors.NewTypeAlreadyExistsError(tableName.String())
		}
	}

	if persistence.IsTemporary() {
		if !params.SessionData().TempTablesEnabled {
			return nil, errors.WithTelemetry(
				pgerror.WithCandidateCode(
					errors.WithHint(
						errors.WithIssueLink(
							errors.Newf("temporary tables are only supported experimentally"),
							errors.IssueLink{IssueURL: build.MakeIssueURL(46260)},
						),
						"You can enable temporary tables by running `SET experimental_enable_temp_tables = 'on'`.",
					),
					pgcode.FeatureNotSupported,
				),
				"sql.schema.temp_tables_disabled",
			)
		}

		// If the table is temporary, get the temporary schema ID.
		var err error
		schema, err = params.p.getOrCreateTemporarySchema(params.ctx, db)
		if err != nil {
			return nil, err
		}
	} else {
		// Otherwise, find the ID of the schema to create the table within.
		var err error
		schema, err = params.p.getNonTemporarySchemaForCreate(params.ctx, db, tableName.Schema())
		if err != nil {
			return nil, err
		}
		if schema.SchemaKind() == catalog.SchemaUserDefined {
			sqltelemetry.IncrementUserDefinedSchemaCounter(sqltelemetry.UserDefinedSchemaUsedByObject)
		}
	}

	if persistence.IsUnlogged() {
		telemetry.Inc(sqltelemetry.CreateUnloggedTableCounter)
		params.p.BufferClientNotice(
			params.ctx,
			pgnotice.Newf("UNLOGGED TABLE will behave as a regular table in CockroachDB"),
		)
	}

	// Check permissions on the schema.
	if err := params.p.canCreateOnSchema(
		params.ctx, schema.GetID(), db.GetID(), params.p.User(), skipCheckPublicSchema); err != nil {
		return nil, err
	}

	desc, err := params.p.Descriptors().Direct().GetDescriptorCollidingWithObject(
		params.ctx,
		params.p.txn,
		db.GetID(),
		schema.GetID(),
		tableName.Table(),
	)
	if err != nil {
		return nil, err
	}
	if desc != nil {
		// Ensure that the descriptor that does exist has the appropriate type.
		{
			mismatchedType := true
			if tableDescriptor, ok := desc.(catalog.TableDescriptor); ok {
				mismatchedType = false
				switch kind {
				case tree.ResolveRequireTableDesc:
					mismatchedType = !tableDescriptor.IsTable()
				case tree.ResolveRequireViewDesc:
					mismatchedType = !tableDescriptor.IsView()
				case tree.ResolveRequireSequenceDesc:
					mismatchedType = !tableDescriptor.IsSequence()
				}
				// If kind any is passed then there will never be a mismatch
				// and we can return an exists error.
			}
			// Only complain about mismatched types for
			// if not exists clauses.
			if mismatchedType && ifNotExists {
				return nil, pgerror.Newf(pgcode.WrongObjectType,
					"%q is not a %s",
					tableName.Table(),
					kind)
			}
		}

		// Check if the object already exists in a dropped state
		if desc.Dropped() {
			return nil, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"%s %q is being dropped, try again later",
				kind,
				tableName.Table())
		}

		// Still return data in this case.
		return schema, sqlerrors.MakeObjectAlreadyExistsError(desc.DescriptorProto(), tableName.FQString())
	}

	return schema, nil
}
func hasPrimaryKeySerialType(params runParams, colDef *tree.ColumnTableDef) (bool, error) {
	if colDef.IsSerial || colDef.GeneratedIdentity.IsGeneratedAsIdentity {
		return true, nil
	}

	if funcExpr, ok := colDef.DefaultExpr.Expr.(*tree.FuncExpr); ok {
		var name string

		switch t := funcExpr.Func.FunctionReference.(type) {
		case *tree.FunctionDefinition:
			name = t.Name
		case *tree.UnresolvedName:
			fn, err := t.ResolveFunction(params.SessionData().SearchPath)
			if err != nil {
				return false, err
			}
			name = fn.Name
		}

		if name == "nextval" {
			return true, nil
		}
	}

	return false, nil
}

func (n *createTableNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter("table"))

	colsWithPrimaryKeyConstraint := make(map[tree.Name]bool)

	for _, def := range n.n.Defs {
		switch v := def.(type) {
		case *tree.UniqueConstraintTableDef:
			if v.PrimaryKey {
				for _, indexEle := range v.IndexTableDef.Columns {
					colsWithPrimaryKeyConstraint[indexEle.Column] = true
				}
			}

		case *tree.ColumnTableDef:
			if v.PrimaryKey.IsPrimaryKey {
				colsWithPrimaryKeyConstraint[v.Name] = true
			}
		}
	}

	for _, def := range n.n.Defs {
		switch v := def.(type) {
		case *tree.ColumnTableDef:
			if _, ok := colsWithPrimaryKeyConstraint[v.Name]; ok {
				primaryKeySerial, err := hasPrimaryKeySerialType(params, v)
				if err != nil {
					return err
				}

				if primaryKeySerial {
					params.p.BufferClientNotice(
						params.ctx,
						pgnotice.Newf("using sequential values in a primary key does not perform as well as using random UUIDs. See %s", docs.URL("serial.html")),
					)
					break
				}
			}
		}
	}

	schema, err := getSchemaForCreateTable(params, n.dbDesc, n.n.Persistence, &n.n.Table,
		tree.ResolveRequireTableDesc, n.n.IfNotExists)
	if err != nil {
		if sqlerrors.IsRelationAlreadyExistsError(err) && n.n.IfNotExists {
			params.p.BufferClientNotice(
				params.ctx,
				pgnotice.Newf("relation %q already exists, skipping", n.n.Table.Table()),
			)
			return nil
		}
		return err
	}
	if n.n.Persistence.IsTemporary() {
		telemetry.Inc(sqltelemetry.CreateTempTableCounter)

		// TODO(#46556): support ON COMMIT DROP and DELETE ROWS on TEMPORARY TABLE.
		// If we do this, the n.n.OnCommit variable should probably be stored on the
		// table descriptor.
		// Note UNSET / PRESERVE ROWS behave the same way so we do not need to do that for now.
		switch n.n.OnCommit {
		case tree.CreateTableOnCommitUnset, tree.CreateTableOnCommitPreserveRows:
		default:
			return errors.AssertionFailedf("ON COMMIT value %d is unrecognized", n.n.OnCommit)
		}
	} else if n.n.OnCommit != tree.CreateTableOnCommitUnset {
		return pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"ON COMMIT can only be used on temporary tables",
		)
	}

	// Warn against creating non-partitioned indexes on a partitioned table,
	// which is undesirable in most cases.
	// Avoid the warning if we have PARTITION ALL BY as all indexes will implicitly
	// have relevant partitioning columns prepended at the front.
	if n.n.PartitionByTable.ContainsPartitions() {
		for _, def := range n.n.Defs {
			if d, ok := def.(*tree.IndexTableDef); ok {
				if d.PartitionByIndex == nil && !n.n.PartitionByTable.All {
					params.p.BufferClientNotice(
						params.ctx,
						errors.WithHint(
							pgnotice.Newf("creating non-partitioned index on partitioned table may not be performant"),
							"Consider modifying the index such that it is also partitioned.",
						),
					)
				}
			}
		}
	}

	id, err := descidgen.GenerateUniqueDescID(params.ctx, params.p.ExecCfg().DB, params.p.ExecCfg().Codec)
	if err != nil {
		return err
	}

	var desc *tabledesc.Mutable
	var affected map[descpb.ID]*tabledesc.Mutable
	// creationTime is initialized to a zero value and populated at read time.
	// See the comment in desc.MaybeIncrementVersion.
	//
	// TODO(ajwerner): remove the timestamp from newTableDesc and its friends,
	// it's	currently relied on in import and restore code and tests.
	var creationTime hlc.Timestamp
	privs := catprivilege.CreatePrivilegesFromDefaultPrivileges(
		n.dbDesc.GetDefaultPrivilegeDescriptor(),
		schema.GetDefaultPrivilegeDescriptor(),
		n.dbDesc.GetID(),
		params.SessionData().User(),
		tree.Tables,
		n.dbDesc.GetPrivileges(),
	)
	if n.n.As() {
		asCols := planColumns(n.sourcePlan)
		if !n.n.AsHasUserSpecifiedPrimaryKey() {
			// rowID column is already present in the input as the last column
			// if the user did not specify a PRIMARY KEY. So ignore it for the
			// purpose of creating column metadata (because newTableDescIfAs
			// does it automatically).
			asCols = asCols[:len(asCols)-1]
		}
		desc, err = newTableDescIfAs(
			params, n.n, n.dbDesc, schema, id, creationTime, asCols, privs, params.p.EvalContext(),
		)
		if err != nil {
			return err
		}

		// If we have an implicit txn we want to run CTAS async, and consequently
		// ensure it gets queued as a SchemaChange.
		if params.p.ExtendedEvalContext().TxnImplicit {
			desc.State = descpb.DescriptorState_ADD
		}
	} else {
		affected = make(map[descpb.ID]*tabledesc.Mutable)
		desc, err = newTableDesc(params, n.n, n.dbDesc, schema, id, creationTime, privs, affected)
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
				if t, err := params.p.Descriptors().GetUncommittedMutableTableByID(id); err != nil {
					return err
				} else if t == nil || !t.IsNew() {
					foundExternalReference = true
					break
				}
			}
			if !foundExternalReference {
				desc.State = descpb.DescriptorState_PUBLIC
			}
		}
	}

	// Descriptor written to store here.
	if err := params.p.createDescriptorWithID(
		params.ctx,
		catalogkeys.MakeObjectNameKey(params.ExecCfg().Codec, n.dbDesc.GetID(), schema.GetID(), n.n.Table.Table()),
		id,
		desc,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	for _, updated := range affected {
		if err := params.p.writeSchemaChange(
			params.ctx, updated, descpb.InvalidMutationID,
			fmt.Sprintf("updating referenced FK table %s(%d) for table %s(%d)",
				updated.Name, updated.ID, desc.Name, desc.ID,
			),
		); err != nil {
			return err
		}
	}

	// Install back references to types used by this table.
	if err := params.p.addBackRefsFromAllTypesInTable(params.ctx, desc); err != nil {
		return err
	}

	if err := validateDescriptor(params.ctx, params.p, desc); err != nil {
		return err
	}

	if desc.LocalityConfig != nil {
		_, dbDesc, err := params.p.Descriptors().GetImmutableDatabaseByID(
			params.ctx,
			params.p.txn,
			desc.ParentID,
			tree.DatabaseLookupFlags{Required: true},
		)
		if err != nil {
			return errors.Wrap(err, "error resolving database for multi-region")
		}

		regionConfig, err := SynthesizeRegionConfig(params.ctx, params.p.txn, dbDesc.GetID(), params.p.Descriptors())
		if err != nil {
			return err
		}

		if err := ApplyZoneConfigForMultiRegionTable(
			params.ctx,
			params.p.txn,
			params.p.ExecCfg(),
			regionConfig,
			desc,
			ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes,
		); err != nil {
			return err
		}
		// Save the reference on the multi-region enum if there is a dependency with
		// the descriptor.
		if desc.GetMultiRegionEnumDependencyIfExists() {
			regionEnumID, err := dbDesc.MultiRegionEnumID()
			if err != nil {
				return err
			}
			typeDesc, err := params.p.Descriptors().GetMutableTypeVersionByID(
				params.ctx,
				params.p.txn,
				regionEnumID,
			)
			if err != nil {
				return errors.Wrap(err, "error resolving multi-region enum")
			}
			typeDesc.AddReferencingDescriptorID(desc.ID)
			err = params.p.writeTypeSchemaChange(
				params.ctx, typeDesc, "add REGIONAL BY TABLE back reference")
			if err != nil {
				return errors.Wrap(err, "error adding backreference to multi-region enum")
			}
		}
	}

	// Log Create Table event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	if err := params.p.logEvent(params.ctx,
		desc.ID,
		&eventpb.CreateTable{
			TableName: n.n.Table.FQString(),
		}); err != nil {
		return err
	}

	// If we are in an explicit txn or the source has placeholders, we execute the
	// CTAS query synchronously.
	if n.n.As() && !params.p.ExtendedEvalContext().TxnImplicit {
		err = func() error {
			// The data fill portion of CREATE AS must operate on a read snapshot,
			// so that it doesn't end up observing its own writes.
			prevMode := params.p.Txn().ConfigureStepping(params.ctx, kv.SteppingEnabled)
			defer func() { _ = params.p.Txn().ConfigureStepping(params.ctx, prevMode) }()

			// This is a very simplified version of the INSERT logic: no CHECK
			// expressions, no FK checks, no arbitrary insertion order, no
			// RETURNING, etc.

			// Instantiate a row inserter and table writer. It has a 1-1
			// mapping to the definitions in the descriptor.
			internal := params.p.SessionData().Internal
			ri, err := row.MakeInserter(
				params.ctx,
				params.p.txn,
				params.ExecCfg().Codec,
				desc.ImmutableCopy().(catalog.TableDescriptor),
				desc.PublicColumns(),
				params.p.alloc,
				&params.ExecCfg().Settings.SV,
				internal,
				params.ExecCfg().GetRowMetrics(internal),
			)
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
			if err := tw.init(params.ctx, params.p.txn, params.p.EvalContext(), &params.p.EvalContext().Settings.SV); err != nil {
				return err
			}

			// Prepare the buffer for row values. At this point, one more column has
			// been added by ensurePrimaryKey() to the list of columns in sourcePlan, if
			// a PRIMARY KEY is not specified by the user.
			rowBuffer := make(tree.Datums, len(desc.Columns))

			for {
				if err := params.p.cancelChecker.Check(); err != nil {
					return err
				}
				if next, err := n.sourcePlan.Next(params); !next {
					if err != nil {
						return err
					}
					if err := tw.finalize(params.ctx); err != nil {
						return err
					}
					break
				}

				// Populate the buffer.
				copy(rowBuffer, n.sourcePlan.Values())

				// CREATE TABLE AS does not copy indexes from the input table.
				// An empty row.PartialIndexUpdateHelper is used here because
				// there are no indexes, partial or otherwise, to update.
				var pm row.PartialIndexUpdateHelper
				if err := tw.row(params.ctx, rowBuffer, pm, params.extendedEvalCtx.Tracing.KVTracingEnabled()); err != nil {
					return err
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}

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

func qualifyFKColErrorWithDB(
	ctx context.Context,
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
	tbl catalog.TableDescriptor,
	col string,
) string {
	return tree.ErrString(tree.NewUnresolvedName(
		db.GetName(),
		sc.GetName(),
		tbl.GetName(),
		col,
	))
}

// TableState is the state of the referencing table ResolveFK() or
// ResolveUniqueWithoutIndexConstraint() is called on.
type TableState int

const (
	// NewTable represents a new table, where the constraint is specified in the
	// CREATE TABLE
	NewTable TableState = iota
	// EmptyTable represents an existing table that is empty
	EmptyTable
	// NonEmptyTable represents an existing non-empty table
	NonEmptyTable
)

// addUniqueWithoutIndexColumnTableDef runs various checks on the given
// ColumnTableDef before adding it as a UNIQUE WITHOUT INDEX constraint to the
// given table descriptor.
func addUniqueWithoutIndexColumnTableDef(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	sessionData *sessiondata.SessionData,
	d *tree.ColumnTableDef,
	desc *tabledesc.Mutable,
	ts TableState,
	validationBehavior tree.ValidationBehavior,
) error {
	if !sessionData.EnableUniqueWithoutIndexConstraints {
		return pgerror.New(pgcode.FeatureNotSupported,
			"unique constraints without an index are not yet supported",
		)
	}
	// Add a unique constraint.
	if err := ResolveUniqueWithoutIndexConstraint(
		ctx,
		desc,
		string(d.Unique.ConstraintName),
		[]string{string(d.Name)},
		"", /* predicate */
		ts,
		validationBehavior,
	); err != nil {
		return err
	}
	return nil
}

// addUniqueWithoutIndexTableDef runs various checks on the given
// UniqueConstraintTableDef before adding it as a UNIQUE WITHOUT INDEX
// constraint to the given table descriptor.
func addUniqueWithoutIndexTableDef(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	sessionData *sessiondata.SessionData,
	d *tree.UniqueConstraintTableDef,
	desc *tabledesc.Mutable,
	tn tree.TableName,
	ts TableState,
	validationBehavior tree.ValidationBehavior,
	semaCtx *tree.SemaContext,
) error {
	if !sessionData.EnableUniqueWithoutIndexConstraints {
		return pgerror.New(pgcode.FeatureNotSupported,
			"unique constraints without an index are not yet supported",
		)
	}
	if len(d.Storing) > 0 {
		return pgerror.New(pgcode.FeatureNotSupported,
			"unique constraints without an index cannot store columns",
		)
	}
	if d.PartitionByIndex.ContainsPartitions() {
		return pgerror.New(pgcode.FeatureNotSupported,
			"partitioned unique constraints without an index are not supported",
		)
	}

	// If there is a predicate, validate it.
	var predicate string
	if d.Predicate != nil {
		var err error
		predicate, err = schemaexpr.ValidateUniqueWithoutIndexPredicate(
			ctx, tn, desc, d.Predicate, semaCtx,
		)
		if err != nil {
			return err
		}
	}

	// Add a unique constraint.
	colNames := make([]string, len(d.Columns))
	for i := range colNames {
		colNames[i] = string(d.Columns[i].Column)
	}
	if err := ResolveUniqueWithoutIndexConstraint(
		ctx, desc, string(d.Name), colNames, predicate, ts, validationBehavior,
	); err != nil {
		return err
	}
	return nil
}

// ResolveUniqueWithoutIndexConstraint looks up the columns mentioned in a
// UNIQUE WITHOUT INDEX constraint and adds metadata representing that
// constraint to the descriptor.
//
// The passed validationBehavior is used to determine whether or not preexisting
// entries in the table need to be validated against the unique constraint being
// added. This only applies for existing tables, not new tables.
func ResolveUniqueWithoutIndexConstraint(
	ctx context.Context,
	tbl *tabledesc.Mutable,
	constraintName string,
	colNames []string,
	predicate string,
	ts TableState,
	validationBehavior tree.ValidationBehavior,
) error {
	var colSet catalog.TableColSet
	cols := make([]catalog.Column, len(colNames))
	for i, name := range colNames {
		col, err := tbl.FindActiveOrNewColumnByName(tree.Name(name))
		if err != nil {
			return err
		}
		// Ensure that the columns don't have duplicates.
		if colSet.Contains(col.GetID()) {
			return pgerror.Newf(pgcode.DuplicateColumn,
				"column %q appears twice in unique constraint", col.GetName())
		}
		colSet.Add(col.GetID())
		cols[i] = col
	}

	// Verify we are not writing a constraint over the same name.
	constraintInfo, err := tbl.GetConstraintInfo()
	if err != nil {
		return err
	}
	if constraintName == "" {
		constraintName = tabledesc.GenerateUniqueName(
			fmt.Sprintf("unique_%s", strings.Join(colNames, "_")),
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

	columnIDs := make(descpb.ColumnIDs, len(cols))
	for i, col := range cols {
		columnIDs[i] = col.GetID()
	}

	validity := descpb.ConstraintValidity_Validated
	if ts != NewTable {
		if validationBehavior == tree.ValidationSkip {
			validity = descpb.ConstraintValidity_Unvalidated
		} else {
			validity = descpb.ConstraintValidity_Validating
		}
	}

	uc := descpb.UniqueWithoutIndexConstraint{
		Name:         constraintName,
		TableID:      tbl.ID,
		ColumnIDs:    columnIDs,
		Predicate:    predicate,
		Validity:     validity,
		ConstraintID: tbl.NextConstraintID,
	}
	tbl.NextConstraintID++
	if ts == NewTable {
		tbl.UniqueWithoutIndexConstraints = append(tbl.UniqueWithoutIndexConstraints, uc)
	} else {
		tbl.AddUniqueWithoutIndexMutation(&uc, descpb.DescriptorMutation_ADD)
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
	txn *kv.Txn,
	sc resolver.SchemaResolver,
	parentDB catalog.DatabaseDescriptor,
	parentSchema catalog.SchemaDescriptor,
	tbl *tabledesc.Mutable,
	d *tree.ForeignKeyConstraintTableDef,
	backrefs map[descpb.ID]*tabledesc.Mutable,
	ts TableState,
	validationBehavior tree.ValidationBehavior,
	evalCtx *tree.EvalContext,
) error {
	var originColSet catalog.TableColSet
	originCols := make([]catalog.Column, len(d.FromCols))
	for i, fromCol := range d.FromCols {
		col, err := tbl.FindActiveOrNewColumnByName(fromCol)
		if err != nil {
			return err
		}
		if err := col.CheckCanBeOutboundFKRef(); err != nil {
			return err
		}
		// Ensure that the origin columns don't have duplicates.
		if originColSet.Contains(col.GetID()) {
			return pgerror.Newf(pgcode.InvalidForeignKey,
				"foreign key contains duplicate column %q", col.GetName())
		}
		originColSet.Add(col.GetID())
		originCols[i] = col
	}

	_, target, err := resolver.ResolveMutableExistingTableObject(ctx, sc, &d.Table, true /*required*/, tree.ResolveRequireTableDesc)
	if err != nil {
		return err
	}
	if target.ParentID != tbl.ParentID {
		if !allowCrossDatabaseFKs.Get(&evalCtx.Settings.SV) {
			return errors.WithHintf(
				pgerror.Newf(pgcode.InvalidForeignKey,
					"foreign references between databases are not allowed (see the '%s' cluster setting)",
					allowCrossDatabaseFKsSetting),
				crossDBReferenceDeprecationHint(),
			)
		}
	}
	if tbl.Temporary != target.Temporary {
		persistenceType := "permanent"
		if tbl.Temporary {
			persistenceType = "temporary"
		}
		return pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"constraints on %s tables may reference only %s tables",
			persistenceType,
			persistenceType,
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
			tbl.State = descpb.DescriptorState_ADD
		}

		// If we resolve the same table more than once, we only want to edit a
		// single instance of it, so replace target with previously resolved table.
		if prev, ok := backrefs[target.ID]; ok {
			target = prev
		} else {
			backrefs[target.ID] = target
		}
	}

	referencedColNames := d.ToCols
	// If no columns are specified, attempt to default to PK, ignoring implicit columns.
	if len(referencedColNames) == 0 {
		numImplicitCols := target.GetPrimaryIndex().GetPartitioning().NumImplicitColumns()
		referencedColNames = make(
			tree.NameList,
			0,
			target.GetPrimaryIndex().NumKeyColumns()-numImplicitCols,
		)
		for i := numImplicitCols; i < target.GetPrimaryIndex().NumKeyColumns(); i++ {
			referencedColNames = append(
				referencedColNames,
				tree.Name(target.GetPrimaryIndex().GetKeyColumnName(i)),
			)
		}
	}

	referencedCols, err := tabledesc.FindPublicColumnsWithNames(target, referencedColNames)
	if err != nil {
		return err
	}

	for i := range referencedCols {
		if err := referencedCols[i].CheckCanBeInboundFKRef(); err != nil {
			return err
		}
	}

	if len(referencedCols) != len(originCols) {
		return pgerror.Newf(pgcode.Syntax,
			"%d columns must reference exactly %d columns in referenced table (found %d)",
			len(originCols), len(originCols), len(referencedCols))
	}

	for i := range originCols {
		if s, t := originCols[i], referencedCols[i]; !s.GetType().Equivalent(t.GetType()) {
			return pgerror.Newf(pgcode.DatatypeMismatch,
				"type of %q (%s) does not match foreign key %q.%q (%s)",
				s.GetName(), s.GetType().String(), target.Name, t.GetName(), t.GetType().String())
		}
		// Send a notice to client if origin col type is not identical to the
		// referenced col.
		if s, t := originCols[i], referencedCols[i]; !s.GetType().Identical(t.GetType()) {
			notice := pgnotice.Newf(
				"type of foreign key column %q (%s) is not identical to referenced column %q.%q (%s)",
				s.ColName(), s.GetType().String(), target.Name, t.GetName(), t.GetType().String())
			evalCtx.ClientNoticeSender.BufferClientNotice(ctx, notice)
		}
	}

	// Verify we are not writing a constraint over the same name.
	// This check is done in Verify(), but we must do it earlier
	// or else we can hit other checks that break things with
	// undesired error codes, e.g. #42858.
	// It may be removable after #37255 is complete.
	constraintInfo, err := tbl.GetConstraintInfo()
	if err != nil {
		return err
	}
	constraintName := string(d.Name)
	if constraintName == "" {
		constraintName = tabledesc.GenerateUniqueName(
			tabledesc.ForeignKeyConstraintName(tbl.GetName(), d.FromCols.ToStrings()),
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

	originColumnIDs := make(descpb.ColumnIDs, len(originCols))
	for i, col := range originCols {
		originColumnIDs[i] = col.GetID()
	}

	targetColIDs := make(descpb.ColumnIDs, len(referencedCols))
	for i := range referencedCols {
		targetColIDs[i] = referencedCols[i].GetID()
	}

	// Don't add a SET NULL action on an index that has any column that is NOT
	// NULL.
	if d.Actions.Delete == tree.SetNull || d.Actions.Update == tree.SetNull {
		for _, originColumn := range originCols {
			if !originColumn.IsNullable() {
				col := qualifyFKColErrorWithDB(ctx, parentDB, parentSchema, tbl, originColumn.GetName())
				return pgerror.Newf(pgcode.InvalidForeignKey,
					"cannot add a SET NULL cascading action on column %q which has a NOT NULL constraint", col,
				)
			}
		}
	}

	// Don't add a SET DEFAULT action on an index that has any column that has
	// a DEFAULT expression of NULL and a NOT NULL constraint.
	if d.Actions.Delete == tree.SetDefault || d.Actions.Update == tree.SetDefault {
		for _, originColumn := range originCols {
			// Having a default expression of NULL, and a constraint of NOT NULL is a
			// contradiction and should never be allowed.
			if !originColumn.HasDefault() && !originColumn.IsNullable() {
				col := qualifyFKColErrorWithDB(ctx, parentDB, parentSchema, tbl, originColumn.GetName())
				return pgerror.Newf(pgcode.InvalidForeignKey,
					"cannot add a SET DEFAULT cascading action on column %q which has a "+
						"NOT NULL constraint and a NULL default expression", col,
				)
			}
		}
	}

	// Ensure that there is a unique constraint on the referenced side to use.
	_, err = tabledesc.FindFKReferencedUniqueConstraint(target, targetColIDs)
	if err != nil {
		return err
	}

	var validity descpb.ConstraintValidity
	if ts != NewTable {
		if validationBehavior == tree.ValidationSkip {
			validity = descpb.ConstraintValidity_Unvalidated
		} else {
			validity = descpb.ConstraintValidity_Validating
		}
	}

	ref := descpb.ForeignKeyConstraint{
		OriginTableID:       tbl.ID,
		OriginColumnIDs:     originColumnIDs,
		ReferencedColumnIDs: targetColIDs,
		ReferencedTableID:   target.ID,
		Name:                constraintName,
		Validity:            validity,
		OnDelete:            descpb.ForeignKeyReferenceActionValue[d.Actions.Delete],
		OnUpdate:            descpb.ForeignKeyReferenceActionValue[d.Actions.Update],
		Match:               descpb.CompositeKeyMatchMethodValue[d.Match],
		ConstraintID:        tbl.NextConstraintID,
	}
	tbl.NextConstraintID++
	if ts == NewTable {
		tbl.OutboundFKs = append(tbl.OutboundFKs, ref)
		target.InboundFKs = append(target.InboundFKs, ref)
	} else {
		tbl.AddForeignKeyMutation(&ref, descpb.DescriptorMutation_ADD)
	}

	return nil
}

// CreatePartitioning returns a set of implicit columns and a new partitioning
// descriptor to build an index with partitioning fields populated to align with
// the tree.PartitionBy clause.
func CreatePartitioning(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *tree.EvalContext,
	tableDesc catalog.TableDescriptor,
	indexDesc descpb.IndexDescriptor,
	partBy *tree.PartitionBy,
	allowedNewColumnNames []tree.Name,
	allowImplicitPartitioning bool,
) (newImplicitCols []catalog.Column, newPartitioning catpb.PartitioningDescriptor, err error) {
	if partBy == nil {
		if indexDesc.Partitioning.NumImplicitColumns > 0 {
			return nil, newPartitioning, unimplemented.Newf(
				"ALTER ... PARTITION BY NOTHING",
				"cannot alter to PARTITION BY NOTHING if the object has implicit column partitioning",
			)
		}
		// No CCL necessary if we're looking at PARTITION BY NOTHING - we can
		// set the partitioning to nothing.
		return nil, newPartitioning, nil
	}
	return CreatePartitioningCCL(
		ctx,
		st,
		evalCtx,
		tableDesc.FindColumnWithName,
		int(indexDesc.Partitioning.NumImplicitColumns),
		indexDesc.KeyColumnNames,
		partBy,
		allowedNewColumnNames,
		allowImplicitPartitioning,
	)
}

// CreatePartitioningCCL is the public hook point for the CCL-licensed
// partitioning creation code.
var CreatePartitioningCCL = func(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *tree.EvalContext,
	columnLookupFn func(tree.Name) (catalog.Column, error),
	oldNumImplicitColumns int,
	oldKeyColumnNames []string,
	partBy *tree.PartitionBy,
	allowedNewColumnNames []tree.Name,
	allowImplicitPartitioning bool,
) (newImplicitCols []catalog.Column, newPartitioning catpb.PartitioningDescriptor, err error) {
	return nil, catpb.PartitioningDescriptor{}, sqlerrors.NewCCLRequiredError(errors.New(
		"creating or manipulating partitions requires a CCL binary"))
}

func getFinalSourceQuery(
	params runParams, source *tree.Select, evalCtx *tree.EvalContext,
) (string, error) {
	// Ensure that all the table names pretty-print as fully qualified, so we
	// store that in the table descriptor.
	//
	// The traversal will update the TableNames in-place, so the changes are
	// persisted in n.n.AsSource. We exploit the fact that planning step above
	// has populated any missing db/schema details in the table names in-place.
	// We use tree.FormatNode merely as a traversal method; its output buffer is
	// discarded immediately after the traversal because it is not needed
	// further.
	f := evalCtx.FmtCtx(
		tree.FmtSerializable,
		tree.FmtReformatTableNames(
			func(_ *tree.FmtCtx, tn *tree.TableName) {
				// Persist the database prefix expansion.
				if tn.SchemaName != "" {
					// All CTE or table aliases have no schema
					// information. Those do not turn into explicit.
					tn.ExplicitSchema = true
					tn.ExplicitCatalog = true
				}
			}),
	)
	f.FormatNode(source)
	f.Close()

	// Substitute placeholders with their values.
	ctx := evalCtx.FmtCtx(
		tree.FmtSerializable,
		tree.FmtPlaceholderFormat(func(ctx *tree.FmtCtx, placeholder *tree.Placeholder) {
			d, err := placeholder.Eval(evalCtx)
			if err != nil {
				panic(errors.NewAssertionErrorWithWrappedErrf(err, "failed to serialize placeholder"))
			}
			d.Format(ctx)
		}),
	)
	ctx.FormatNode(source)

	// Use IDs instead of sequence names because name resolution depends on
	// session data, and the internal executor has different session data.
	sequenceReplacedQuery, err := replaceSeqNamesWithIDs(params.ctx, params.p, ctx.CloseAndGetString())
	if err != nil {
		return "", err
	}
	return sequenceReplacedQuery, nil
}

// newTableDescIfAs is the NewTableDesc method for when we have a table
// that is created with the CREATE AS format.
func newTableDescIfAs(
	params runParams,
	p *tree.CreateTable,
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
	id descpb.ID,
	creationTime hlc.Timestamp,
	resultColumns []colinfo.ResultColumn,
	privileges *catpb.PrivilegeDescriptor,
	evalContext *tree.EvalContext,
) (desc *tabledesc.Mutable, err error) {
	if err := validateUniqueConstraintParamsForCreateTableAs(p); err != nil {
		return nil, err
	}

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
			var tableDef tree.TableDef = &tree.ColumnTableDef{
				Name:   tree.Name(colRes.Name),
				Type:   colRes.Typ,
				Hidden: colRes.Hidden,
			}
			if d, ok = tableDef.(*tree.ColumnTableDef); !ok {
				return nil, errors.Errorf("failed to cast type to ColumnTableDef\n")
			}
			d.Nullable.Nullability = tree.SilentNull
			p.Defs = append(p.Defs, tableDef)
		}
	}

	desc, err = newTableDesc(
		params,
		p,
		db, sc, id,
		creationTime,
		privileges,
		nil, /* affected */
	)
	if err != nil {
		return nil, err
	}
	createQuery, err := getFinalSourceQuery(params, p.AsSource, evalContext)
	if err != nil {
		return nil, err
	}
	desc.CreateQuery = createQuery
	return desc, nil
}

type newTableDescOptions struct {
	bypassLocalityOnNonMultiRegionDatabaseCheck bool
}

// NewTableDescOption is an option on NewTableDesc.
type NewTableDescOption func(o *newTableDescOptions)

// NewTableDescOptionBypassLocalityOnNonMultiRegionDatabaseCheck will allow
// LOCALITY on non multi-region tables.
func NewTableDescOptionBypassLocalityOnNonMultiRegionDatabaseCheck() NewTableDescOption {
	return func(o *newTableDescOptions) {
		o.bypassLocalityOnNonMultiRegionDatabaseCheck = true
	}
}

// NewTableDesc creates a table descriptor from a CreateTable statement.
//
// txn and vt can be nil if the table to be created does not contain references
// to other tables (e.g. foreign keys). This is useful at bootstrap when
// creating descriptors for virtual tables.
//
// parentID refers to the databaseID under which the descriptor is being
// created and parentSchemaID refers to the schemaID of the schema under which
// the descriptor is being created.
//
// evalCtx can be nil if the table to be created has no default expression for
// any of the columns and no partitioning expression.
//
// semaCtx can be nil if the table to be created has no default expression on
// any of the columns and no check constraints.
//
// regionConfig indicates if the table is being created in a multi-region db.
// A non-nil regionConfig represents the region configuration of a multi-region
// db. A nil regionConfig means current db is not multi-regional.
//
// The caller must also ensure that the SchemaResolver is configured
// to bypass caching and enable visibility of just-added descriptors.
// This is used to resolve sequence and FK dependencies. Also see the
// comment at the start of ResolveFK().
//
// If the table definition *may* use the SERIAL type, the caller is
// also responsible for processing serial types using
// processSerialLikeInColumnDef() on every column definition, and creating
// the necessary sequences in KV before calling NewTableDesc().
func NewTableDesc(
	ctx context.Context,
	txn *kv.Txn,
	vt resolver.SchemaResolver,
	st *cluster.Settings,
	n *tree.CreateTable,
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
	id descpb.ID,
	regionConfig *multiregion.RegionConfig,
	creationTime hlc.Timestamp,
	privileges *catpb.PrivilegeDescriptor,
	affected map[descpb.ID]*tabledesc.Mutable,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	sessionData *sessiondata.SessionData,
	persistence tree.Persistence,
	inOpts ...NewTableDescOption,
) (*tabledesc.Mutable, error) {
	// Used to delay establishing Column/Sequence dependency until ColumnIDs have
	// been populated.
	cdd := make([]*tabledesc.ColumnDefDescs, len(n.Defs))

	var opts newTableDescOptions
	for _, o := range inOpts {
		o(&opts)
	}

	var dbID descpb.ID
	if db != nil {
		dbID = db.GetID()
	}
	desc := tabledesc.InitTableDescriptor(
		id, dbID, sc.GetID(), n.Table.Table(), creationTime, privileges, persistence,
	)

	if err := paramparse.SetStorageParameters(
		ctx,
		semaCtx,
		evalCtx,
		n.StorageParams,
		paramparse.NewTableStorageParamObserver(&desc),
	); err != nil {
		return nil, err
	}

	indexEncodingVersion := descpb.StrictIndexColumnIDGuaranteesVersion
	isRegionalByRow := n.Locality != nil && n.Locality.LocalityLevel == tree.LocalityLevelRow

	var partitionAllBy *tree.PartitionBy
	primaryIndexColumnSet := make(map[string]struct{})

	if n.Locality != nil && regionConfig == nil &&
		!opts.bypassLocalityOnNonMultiRegionDatabaseCheck {
		return nil, errors.WithHint(pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"cannot set LOCALITY on a table in a database that is not multi-region enabled",
		),
			"database must first be multi-region enabled using ALTER DATABASE ... SET PRIMARY REGION <region>",
		)
	}

	// PARTITION BY and PARTITION ALL BY are not supported in multi-regional table.
	if n.Locality != nil || regionConfig != nil {
		// Check PARTITION BY is not set on any column, index or table definition.
		if n.PartitionByTable.ContainsPartitioningClause() {
			return nil, pgerror.New(
				pgcode.FeatureNotSupported,
				"multi-region tables containing PARTITION BY are not supported",
			)
		}
		for _, def := range n.Defs {
			switch d := def.(type) {
			case *tree.IndexTableDef:
				if d.PartitionByIndex.ContainsPartitioningClause() {
					return nil, pgerror.New(
						pgcode.FeatureNotSupported,
						"multi-region tables with an INDEX containing PARTITION BY are not supported",
					)
				}
			case *tree.UniqueConstraintTableDef:
				if d.PartitionByIndex.ContainsPartitioningClause() {
					return nil, pgerror.New(
						pgcode.FeatureNotSupported,
						"multi-region tables with an UNIQUE constraint containing PARTITION BY are not supported",
					)
				}
			}
		}
	}

	// Add implied columns under REGIONAL BY ROW.
	if isRegionalByRow {
		regionalByRowCol := tree.RegionalByRowRegionDefaultColName
		if n.Locality.RegionalByRowColumn != "" {
			regionalByRowCol = n.Locality.RegionalByRowColumn
		}

		// Check PARTITION BY is not set on anything partitionable, and also check
		// for the existence of the column to partition by.
		regionalByRowColExists := false
		for _, def := range n.Defs {
			switch d := def.(type) {
			case *tree.ColumnTableDef:
				if d.Name == regionalByRowCol {
					regionalByRowColExists = true
					t, err := tree.ResolveType(ctx, d.Type, vt)
					if err != nil {
						return nil, errors.Wrap(err, "error resolving REGIONAL BY ROW column type")
					}
					if t.Oid() != typedesc.TypeIDToOID(regionConfig.RegionEnumID()) {
						err = pgerror.Newf(
							pgcode.InvalidTableDefinition,
							"cannot use column %s which has type %s in REGIONAL BY ROW",
							d.Name,
							t.SQLString(),
						)
						if t, terr := vt.ResolveTypeByOID(
							ctx,
							typedesc.TypeIDToOID(regionConfig.RegionEnumID()),
						); terr == nil {
							if n.Locality.RegionalByRowColumn != tree.RegionalByRowRegionNotSpecifiedName {
								// In this case, someone used REGIONAL BY ROW AS <col> where
								// col has a non crdb_internal_region type.
								err = errors.WithDetailf(
									err,
									"REGIONAL BY ROW AS must reference a column of type %s",
									t.Name(),
								)
							} else {
								// In this case, someone used REGIONAL BY ROW but also specified
								// a crdb_region column that does not have a crdb_internal_region type.
								err = errors.WithDetailf(
									err,
									"Column %s must be of type %s",
									t.Name(),
									tree.RegionEnum,
								)
							}
						}
						return nil, err
					}
					break
				}
			}
		}

		if !regionalByRowColExists {
			if n.Locality.RegionalByRowColumn != tree.RegionalByRowRegionNotSpecifiedName {
				return nil, pgerror.Newf(
					pgcode.UndefinedColumn,
					"column %s in REGIONAL BY ROW AS does not exist",
					regionalByRowCol.String(),
				)
			}
			oid := typedesc.TypeIDToOID(regionConfig.RegionEnumID())
			n.Defs = append(
				n.Defs,
				regionalByRowDefaultColDef(
					oid,
					regionalByRowGatewayRegionDefaultExpr(oid),
					maybeRegionalByRowOnUpdateExpr(evalCtx, oid),
				),
			)
			cdd = append(cdd, nil)
		}

		// Construct the partitioning for the PARTITION ALL BY.
		desc.PartitionAllBy = true
		partitionAllBy = partitionByForRegionalByRow(
			*regionConfig,
			regionalByRowCol,
		)
		// Leading region column of REGIONAL BY ROW is part of the primary
		// index column set.
		primaryIndexColumnSet[string(regionalByRowCol)] = struct{}{}
	}

	// Create the TTL column if one does not already exist.
	if ttl := desc.GetRowLevelTTL(); ttl != nil {
		if err := checkTTLEnabledForCluster(ctx, st); err != nil {
			return nil, err
		}
		hasRowLevelTTLColumn := false
		for _, def := range n.Defs {
			switch def := def.(type) {
			case *tree.ColumnTableDef:
				if def.Name == colinfo.TTLDefaultExpirationColumnName {
					// If we find the column, make sure it has the expected type.
					if def.Type.SQLString() != types.TimestampTZ.SQLString() {
						return nil, pgerror.Newf(
							pgcode.InvalidTableDefinition,
							`table %s has TTL defined, but column %s is not a %s`,
							def.Name,
							colinfo.TTLDefaultExpirationColumnName,
							types.TimestampTZ.SQLString(),
						)
					}
					hasRowLevelTTLColumn = true
					break
				}
			}
		}
		if !hasRowLevelTTLColumn {
			col, err := rowLevelTTLAutomaticColumnDef(ttl)
			if err != nil {
				return nil, err
			}
			n.Defs = append(n.Defs, col)
			cdd = append(cdd, nil)
		}
	}

	if n.PartitionByTable.ContainsPartitioningClause() {
		// Table PARTITION BY columns are always part of the primary index
		// column set.
		if n.PartitionByTable.PartitionBy != nil {
			for _, field := range n.PartitionByTable.PartitionBy.Fields {
				primaryIndexColumnSet[string(field)] = struct{}{}
			}
		}
		if n.PartitionByTable.All {
			if !evalCtx.SessionData().ImplicitColumnPartitioningEnabled {
				return nil, errors.WithHint(
					pgerror.New(
						pgcode.FeatureNotSupported,
						"PARTITION ALL BY LIST/RANGE is currently experimental",
					),
					"to enable, use SET experimental_enable_implicit_column_partitioning = true",
				)
			}
			desc.PartitionAllBy = true
			partitionAllBy = n.PartitionByTable.PartitionBy
		}
	}

	allowImplicitPartitioning := (sessionData != nil && sessionData.ImplicitColumnPartitioningEnabled) ||
		(n.Locality != nil && n.Locality.LocalityLevel == tree.LocalityLevelRow)

	// We defer index creation of implicit indexes in column definitions
	// until after all columns have been initialized, in case there is
	// an implicit index that will depend on a column that has not yet
	// been initialized.
	type implicitColumnDefIdx struct {
		idx *descpb.IndexDescriptor
		def *tree.ColumnTableDef
	}
	var implicitColumnDefIdxs []implicitColumnDefIdx

	for i, def := range n.Defs {
		if d, ok := def.(*tree.ColumnTableDef); ok {
			if d.IsComputed() {
				d.Computed.Expr = schemaexpr.MaybeRewriteComputedColumn(d.Computed.Expr, evalCtx.SessionData())
			}
			// NewTableDesc is called sometimes with a nil SemaCtx (for example
			// during bootstrapping). In order to not panic, pass a nil TypeResolver
			// when attempting to resolve the columns type.
			defType, err := tree.ResolveType(ctx, d.Type, semaCtx.GetTypeResolver())
			if err != nil {
				return nil, err
			}
			if !desc.IsVirtualTable() {
				switch defType.Oid() {
				case oid.T_int2vector, oid.T_oidvector:
					return nil, pgerror.Newf(
						pgcode.FeatureNotSupported,
						"VECTOR column types are unsupported",
					)
				}
			}
			if d.PrimaryKey.Sharded {
				if n.PartitionByTable.ContainsPartitions() && !n.PartitionByTable.All {
					return nil, pgerror.New(pgcode.FeatureNotSupported, "hash sharded indexes cannot be explicitly partitioned")
				}
				buckets, err := tabledesc.EvalShardBucketCount(ctx, semaCtx, evalCtx, d.PrimaryKey.ShardBuckets, d.PrimaryKey.StorageParams)
				if err != nil {
					return nil, err
				}
				shardCol, err := maybeCreateAndAddShardCol(int(buckets), &desc,
					[]string{string(d.Name)}, true, /* isNewTable */
				)
				if err != nil {
					return nil, err
				}
				checkConstraint, err := makeShardCheckConstraintDef(int(buckets), shardCol)
				if err != nil {
					return nil, err
				}
				// Add the shard's check constraint to the list of TableDefs to treat it
				// like it's been "hoisted" like the explicitly added check constraints.
				// It'll then be added to this table's resulting table descriptor below in
				// the constraint pass.
				n.Defs = append(n.Defs, checkConstraint)
				cdd = append(cdd, nil)
			}
			if d.IsVirtual() && d.HasColumnFamily() {
				return nil, pgerror.Newf(pgcode.Syntax, "virtual columns cannot have family specifications")
			}

			cdd[i], err = tabledesc.MakeColumnDefDescs(ctx, d, semaCtx, evalCtx)
			if err != nil {
				return nil, err
			}
			col := cdd[i].ColumnDescriptor
			idx := cdd[i].PrimaryKeyOrUniqueIndexDescriptor

			// Do not include virtual tables in these statistics.
			if !descpb.IsVirtualTable(id) {
				incTelemetryForNewColumn(d, col)
			}

			desc.AddColumn(col)

			if idx != nil {
				idx.Version = indexEncodingVersion
				implicitColumnDefIdxs = append(implicitColumnDefIdxs, implicitColumnDefIdx{idx: idx, def: d})
			}

			if d.HasColumnFamily() {
				// Pass true for `create` and `ifNotExists` because when we're creating
				// a table, we always want to create the specified family if it doesn't
				// exist.
				err := desc.AddColumnToFamilyMaybeCreate(col.Name, string(d.Family.Name), true, true)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	for _, implicitColumnDefIdx := range implicitColumnDefIdxs {
		if implicitColumnDefIdx.def.PrimaryKey.IsPrimaryKey {
			if err := desc.AddPrimaryIndex(*implicitColumnDefIdx.idx); err != nil {
				return nil, err
			}
		} else {
			// If it is a non-primary index that is implicitly created, ensure
			// partitioning for PARTITION ALL BY.
			if desc.PartitionAllBy {
				var err error
				newImplicitCols, newPartitioning, err := CreatePartitioning(
					ctx,
					st,
					evalCtx,
					&desc,
					*implicitColumnDefIdx.idx,
					partitionAllBy,
					nil, /* allowedNewColumnNames */
					allowImplicitPartitioning,
				)
				if err != nil {
					return nil, err
				}
				tabledesc.UpdateIndexPartitioning(implicitColumnDefIdx.idx, false /* isIndexPrimary */, newImplicitCols, newPartitioning)
			}

			if err := desc.AddSecondaryIndex(*implicitColumnDefIdx.idx); err != nil {
				return nil, err
			}
		}
	}

	// Now that we've constructed our columns, we pop into any of our computed
	// columns so that we can dequalify any column references.
	sourceInfo := colinfo.NewSourceInfoForSingleTable(
		n.Table, colinfo.ResultColumnsFromColumns(desc.GetID(), desc.PublicColumns()),
	)

	for i := range desc.Columns {
		col := &desc.Columns[i]
		if col.IsComputed() {
			expr, err := parser.ParseExpr(*col.ComputeExpr)
			if err != nil {
				return nil, err
			}

			deqExpr, err := schemaexpr.DequalifyColumnRefs(ctx, sourceInfo, expr)
			if err != nil {
				return nil, err
			}
			col.ComputeExpr = &deqExpr
		}
	}

	setupShardedIndexForNewTable := func(
		d tree.IndexTableDef, idx *descpb.IndexDescriptor,
	) (columns tree.IndexElemList, _ error) {
		if d.PartitionByIndex.ContainsPartitions() {
			return nil, pgerror.New(pgcode.FeatureNotSupported, "hash sharded indexes cannot be explicitly partitioned")
		}
		if desc.IsPartitionAllBy() && anyColumnIsPartitioningField(d.Columns, partitionAllBy) {
			return nil, pgerror.New(
				pgcode.FeatureNotSupported,
				`hash sharded indexes cannot include implicit partitioning columns from "PARTITION ALL BY" or "LOCALITY REGIONAL BY ROW"`,
			)
		}
		shardCol, newColumns, err := setupShardedIndex(
			ctx,
			evalCtx,
			semaCtx,
			d.Columns,
			d.Sharded.ShardBuckets,
			&desc,
			idx,
			d.StorageParams,
			true /* isNewTable */)
		if err != nil {
			return nil, err
		}

		buckets, err := tabledesc.EvalShardBucketCount(ctx, semaCtx, evalCtx, d.Sharded.ShardBuckets, d.StorageParams)
		if err != nil {
			return nil, err
		}
		checkConstraint, err := makeShardCheckConstraintDef(int(buckets), shardCol)
		if err != nil {
			return nil, err
		}

		// If there is an equivalent check constraint from the CREATE TABLE (should
		// be rare since we hide the constraint of shard column), we don't create a
		// duplicate one.
		ckBuilder := schemaexpr.MakeCheckConstraintBuilder(ctx, n.Table, &desc, semaCtx)
		checkConstraintDesc, err := ckBuilder.Build(checkConstraint)
		if err != nil {
			return nil, err
		}
		for _, def := range n.Defs {
			if inputCheckConstraint, ok := def.(*tree.CheckConstraintTableDef); ok {
				inputCheckConstraintDesc, err := ckBuilder.Build(inputCheckConstraint)
				if err != nil {
					return nil, err
				}
				if checkConstraintDesc.Expr == inputCheckConstraintDesc.Expr {
					return newColumns, nil
				}
			}
		}

		n.Defs = append(n.Defs, checkConstraint)
		cdd = append(cdd, nil)

		return newColumns, nil
	}

	// Now that we have all the other columns set up, we can validate
	// any computed columns.
	for _, def := range n.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			if d.IsComputed() {
				serializedExpr, _, err := schemaexpr.ValidateComputedColumnExpression(
					ctx, &desc, d, &n.Table, "computed column", semaCtx,
				)
				if err != nil {
					return nil, err
				}
				col, err := desc.FindColumnWithName(d.Name)
				if err != nil {
					return nil, err
				}
				col.ColumnDesc().ComputeExpr = &serializedExpr
			}
		}
	}

	for _, def := range n.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef, *tree.LikeTableDef:
			// pass, handled above.

		case *tree.IndexTableDef:
			// If the index is named, ensure that the name is unique. Unnamed
			// indexes will be given a unique auto-generated name later on when
			// AllocateIDs is called.
			if d.Name != "" {
				if idx, _ := desc.FindIndexWithName(d.Name.String()); idx != nil {
					return nil, pgerror.Newf(pgcode.DuplicateRelation, "duplicate index name: %q", d.Name)
				}
			}
			if err := validateColumnsAreAccessible(&desc, d.Columns); err != nil {
				return nil, err
			}
			if err := replaceExpressionElemsWithVirtualCols(
				ctx,
				&desc,
				&n.Table,
				d.Columns,
				d.Inverted,
				true, /* isNewTable */
				semaCtx,
			); err != nil {
				return nil, err
			}
			idx := descpb.IndexDescriptor{
				Name:             string(d.Name),
				StoreColumnNames: d.Storing.ToStrings(),
				Version:          indexEncodingVersion,
			}
			if d.Inverted {
				idx.Type = descpb.IndexDescriptor_INVERTED
			}
			columns := d.Columns
			if d.Sharded != nil {
				var err error
				columns, err = setupShardedIndexForNewTable(*d, &idx)
				if err != nil {
					return nil, err
				}
			}
			if err := idx.FillColumns(columns); err != nil {
				return nil, err
			}
			if d.Inverted {
				column, err := desc.FindColumnWithName(tree.Name(idx.InvertedColumnName()))
				if err != nil {
					return nil, err
				}
				switch column.GetType().Family() {
				case types.GeometryFamily:
					config, err := geoindex.GeometryIndexConfigForSRID(column.GetType().GeoSRIDOrZero())
					if err != nil {
						return nil, err
					}
					idx.GeoConfig = *config
				case types.GeographyFamily:
					idx.GeoConfig = *geoindex.DefaultGeographyIndexConfig()
				}
			}

			var idxPartitionBy *tree.PartitionBy
			if desc.PartitionAllBy && d.PartitionByIndex.ContainsPartitions() {
				return nil, pgerror.New(
					pgcode.FeatureNotSupported,
					"cannot define PARTITION BY on an index if the table is implicitly partitioned with PARTITION ALL BY or LOCALITY REGIONAL BY ROW definition",
				)
			}
			if desc.PartitionAllBy {
				idxPartitionBy = partitionAllBy
			} else if d.PartitionByIndex.ContainsPartitions() {
				idxPartitionBy = d.PartitionByIndex.PartitionBy
			}
			if idxPartitionBy != nil {
				var err error
				newImplicitCols, newPartitioning, err := CreatePartitioning(
					ctx,
					st,
					evalCtx,
					&desc,
					idx,
					idxPartitionBy,
					nil, /* allowedNewColumnNames */
					allowImplicitPartitioning,
				)
				if err != nil {
					return nil, err
				}
				tabledesc.UpdateIndexPartitioning(&idx, false /* isIndexPrimary */, newImplicitCols, newPartitioning)
			}

			if d.Predicate != nil {
				expr, err := schemaexpr.ValidatePartialIndexPredicate(
					ctx, &desc, d.Predicate, &n.Table, semaCtx,
				)
				if err != nil {
					return nil, err
				}
				idx.Predicate = expr
			}
			if err := paramparse.SetStorageParameters(
				ctx,
				semaCtx,
				evalCtx,
				d.StorageParams,
				&paramparse.IndexStorageParamObserver{IndexDesc: &idx},
			); err != nil {
				return nil, err
			}

			if err := desc.AddSecondaryIndex(idx); err != nil {
				return nil, err
			}
		case *tree.UniqueConstraintTableDef:
			if d.WithoutIndex {
				// We will add the unique constraint below.
				break
			}
			// If the index is named, ensure that the name is unique. Unnamed
			// indexes will be given a unique auto-generated name later on when
			// AllocateIDs is called.
			if d.Name != "" {
				if idx, _ := desc.FindIndexWithName(d.Name.String()); idx != nil {
					return nil, pgerror.Newf(pgcode.DuplicateRelation, "duplicate index name: %q", d.Name)
				}
			}
			if err := validateColumnsAreAccessible(&desc, d.Columns); err != nil {
				return nil, err
			}
			if err := replaceExpressionElemsWithVirtualCols(
				ctx,
				&desc,
				&n.Table,
				d.Columns,
				false, /* isInverted */
				true,  /* isNewTable */
				semaCtx,
			); err != nil {
				return nil, err
			}
			idx := descpb.IndexDescriptor{
				Name:             string(d.Name),
				Unique:           true,
				StoreColumnNames: d.Storing.ToStrings(),
				Version:          indexEncodingVersion,
			}
			columns := d.Columns
			if d.Sharded != nil {
				if d.PrimaryKey && n.PartitionByTable.ContainsPartitions() && !n.PartitionByTable.All {
					return nil, pgerror.New(
						pgcode.FeatureNotSupported,
						"hash sharded indexes cannot be explicitly partitioned",
					)
				}
				var err error
				columns, err = setupShardedIndexForNewTable(d.IndexTableDef, &idx)
				if err != nil {
					return nil, err
				}
			}
			if err := idx.FillColumns(columns); err != nil {
				return nil, err
			}
			// Specifying a partitioning on a PRIMARY KEY constraint should be disallowed by the
			// syntax, but do a sanity check.
			if d.PrimaryKey && d.PartitionByIndex.ContainsPartitioningClause() {
				return nil, errors.AssertionFailedf(
					"PRIMARY KEY partitioning should be defined at table level",
				)
			}
			// We should only do partitioning of non-primary indexes at this point -
			// the PRIMARY KEY CreatePartitioning is done at the of CreateTable, so
			// avoid the duplicate work.
			if !d.PrimaryKey {
				if desc.PartitionAllBy && d.PartitionByIndex.ContainsPartitions() {
					return nil, pgerror.New(
						pgcode.FeatureNotSupported,
						"cannot define PARTITION BY on an unique constraint if the table is implicitly partitioned with PARTITION ALL BY or LOCALITY REGIONAL BY ROW definition",
					)
				}
				var idxPartitionBy *tree.PartitionBy
				if desc.PartitionAllBy {
					idxPartitionBy = partitionAllBy
				} else if d.PartitionByIndex.ContainsPartitions() {
					idxPartitionBy = d.PartitionByIndex.PartitionBy
				}

				if idxPartitionBy != nil {
					var err error
					newImplicitCols, newPartitioning, err := CreatePartitioning(
						ctx,
						st,
						evalCtx,
						&desc,
						idx,
						idxPartitionBy,
						nil, /* allowedNewColumnNames */
						allowImplicitPartitioning,
					)
					if err != nil {
						return nil, err
					}
					tabledesc.UpdateIndexPartitioning(&idx, false /* isIndexPrimary */, newImplicitCols, newPartitioning)
				}
			}
			if d.Predicate != nil {
				expr, err := schemaexpr.ValidatePartialIndexPredicate(
					ctx, &desc, d.Predicate, &n.Table, semaCtx,
				)
				if err != nil {
					return nil, err
				}
				idx.Predicate = expr
			}
			if d.PrimaryKey {
				if err := desc.AddPrimaryIndex(idx); err != nil {
					return nil, err
				}
				for _, c := range columns {
					primaryIndexColumnSet[string(c.Column)] = struct{}{}
				}
			} else {
				if err := desc.AddSecondaryIndex(idx); err != nil {
					return nil, err
				}
			}
		case *tree.CheckConstraintTableDef, *tree.ForeignKeyConstraintTableDef, *tree.FamilyTableDef:
			// pass, handled below.

		default:
			return nil, errors.Errorf("unsupported table def: %T", def)
		}
	}

	// If explicit primary keys are required, error out since a primary key was not supplied.
	if desc.GetPrimaryIndex().NumKeyColumns() == 0 && desc.IsPhysicalTable() && evalCtx != nil &&
		evalCtx.SessionData() != nil && evalCtx.SessionData().RequireExplicitPrimaryKeys {
		return nil, errors.Errorf(
			"no primary key specified for table %s (require_explicit_primary_keys = true)", desc.Name)
	}

	for i := range desc.Columns {
		if _, ok := primaryIndexColumnSet[desc.Columns[i].Name]; ok {
			desc.Columns[i].Nullable = false
		}
	}

	// Now that all columns are in place, add any explicit families (this is done
	// here, rather than in the constraint pass below since we want to pick up
	// explicit allocations before AllocateIDs adds implicit ones).
	columnsInExplicitFamilies := map[string]bool{}
	for _, def := range n.Defs {
		if d, ok := def.(*tree.FamilyTableDef); ok {
			fam := descpb.ColumnFamilyDescriptor{
				Name:        string(d.Name),
				ColumnNames: d.Columns.ToStrings(),
			}
			for _, c := range fam.ColumnNames {
				columnsInExplicitFamilies[c] = true
			}
			desc.AddFamily(fam)
		}
	}
	version := st.Version.ActiveVersionOrEmpty(ctx)
	if err := desc.AllocateIDs(ctx, version); err != nil {
		return nil, err
	}

	for _, idx := range desc.PublicNonPrimaryIndexes() {
		// Increment the counter if this index could be storing data across multiple column families.
		if idx.NumSecondaryStoredColumns() > 1 && len(desc.Families) > 1 {
			telemetry.Inc(sqltelemetry.SecondaryIndexColumnFamiliesCounter)
		}
	}

	if n.PartitionByTable.ContainsPartitions() || desc.PartitionAllBy {
		partitionBy := partitionAllBy
		if partitionBy == nil {
			partitionBy = n.PartitionByTable.PartitionBy
		}
		// At this point, we could have PARTITION ALL BY NOTHING, so check it is != nil.
		if partitionBy != nil {
			newPrimaryIndex := desc.GetPrimaryIndex().IndexDescDeepCopy()
			newImplicitCols, newPartitioning, err := CreatePartitioning(
				ctx,
				st,
				evalCtx,
				&desc,
				newPrimaryIndex,
				partitionBy,
				nil, /* allowedNewColumnNames */
				allowImplicitPartitioning,
			)
			if err != nil {
				return nil, err
			}
			isIndexAltered := tabledesc.UpdateIndexPartitioning(&newPrimaryIndex, true /* isIndexPrimary */, newImplicitCols, newPartitioning)
			if isIndexAltered {
				// During CreatePartitioning, implicitly partitioned columns may be
				// created. AllocateIDs which allocates column IDs to each index
				// needs to be called before CreatePartitioning as CreatePartitioning
				// requires IDs to be allocated.
				//
				// As such, do a post check for implicitly partitioned columns, and
				// if they are detected, ensure each index contains the implicitly
				// partitioned column.
				if numImplicitCols := newPrimaryIndex.Partitioning.NumImplicitColumns; numImplicitCols > 0 {
					for _, idx := range desc.PublicNonPrimaryIndexes() {
						if idx.GetEncodingType() != descpb.SecondaryIndexEncoding {
							continue
						}
						colIDs := idx.CollectKeyColumnIDs()
						colIDs.UnionWith(idx.CollectSecondaryStoredColumnIDs())
						colIDs.UnionWith(idx.CollectKeySuffixColumnIDs())
						missingExtraColumnIDs := make([]descpb.ColumnID, 0, numImplicitCols)
						for _, implicitPrimaryColID := range newPrimaryIndex.KeyColumnIDs[:numImplicitCols] {
							if !colIDs.Contains(implicitPrimaryColID) {
								missingExtraColumnIDs = append(missingExtraColumnIDs, implicitPrimaryColID)
							}
						}
						if len(missingExtraColumnIDs) == 0 {
							continue
						}
						newIdxDesc := idx.IndexDescDeepCopy()
						newIdxDesc.KeySuffixColumnIDs = append(newIdxDesc.KeySuffixColumnIDs, missingExtraColumnIDs...)
						desc.SetPublicNonPrimaryIndex(idx.Ordinal(), newIdxDesc)
					}
				}
				desc.SetPrimaryIndex(newPrimaryIndex)
			}
		}
	}

	// Once all the IDs have been allocated, we can add the Sequence dependencies
	// as maybeAddSequenceDependencies requires ColumnIDs to be correct.
	// Elements in n.Defs are not necessarily column definitions, so use a separate
	// counter to map ColumnDefs to columns.
	colIdx := 0
	for i := range n.Defs {
		if _, ok := n.Defs[i].(*tree.ColumnTableDef); ok {
			if cdd[i] != nil {
				if err := cdd[i].ForEachTypedExpr(func(expr tree.TypedExpr) error {
					changedSeqDescs, err := maybeAddSequenceDependencies(
						ctx, st, vt, &desc, &desc.Columns[colIdx], expr, affected)
					if err != nil {
						return err
					}
					for _, changedSeqDesc := range changedSeqDescs {
						affected[changedSeqDesc.ID] = changedSeqDesc
					}
					return nil
				}); err != nil {
					return nil, err
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
		prefix: catalog.ResolvedObjectPrefix{
			Database: db,
			Schema:   sc,
		},
		newTableDesc: &desc,
		newTableName: &n.Table,
	}

	ckBuilder := schemaexpr.MakeCheckConstraintBuilder(ctx, n.Table, &desc, semaCtx)
	for _, def := range n.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			if d.Unique.WithoutIndex {
				if err := addUniqueWithoutIndexColumnTableDef(
					ctx, evalCtx, sessionData, d, &desc, NewTable, tree.ValidationDefault,
				); err != nil {
					return nil, err
				}
			}

		case *tree.UniqueConstraintTableDef:
			if d.WithoutIndex {
				if err := addUniqueWithoutIndexTableDef(
					ctx, evalCtx, sessionData, d, &desc, n.Table, NewTable, tree.ValidationDefault, semaCtx,
				); err != nil {
					return nil, err
				}
			}

		case *tree.IndexTableDef, *tree.FamilyTableDef, *tree.LikeTableDef:
			// Pass, handled above.

		case *tree.CheckConstraintTableDef:
			ck, err := ckBuilder.Build(d)
			if err != nil {
				return nil, err
			}
			desc.Checks = append(desc.Checks, ck)

		case *tree.ForeignKeyConstraintTableDef:
			if err := ResolveFK(
				ctx, txn, fkResolver, db, sc, &desc, d, affected, NewTable,
				tree.ValidationDefault, evalCtx,
			); err != nil {
				return nil, err
			}

		default:
			return nil, errors.Errorf("unsupported table def: %T", def)
		}
	}

	// We validate the table descriptor, checking for ON UPDATE expressions that
	// conflict with FK ON UPDATE actions. We perform this validation after
	// constructing the table descriptor so that we can check all foreign key
	// constraints in on place as opposed to traversing the input and finding all
	// inline/explicit foreign key constraints.
	var onUpdateErr error
	tabledesc.ValidateOnUpdate(&desc, func(err error) {
		onUpdateErr = err
	})
	if onUpdateErr != nil {
		return nil, onUpdateErr
	}

	// AllocateIDs mutates its receiver. `return desc, desc.AllocateIDs()`
	// happens to work in gc, but does not work in gccgo.
	//
	// See https://github.com/golang/go/issues/23188.
	if err := desc.AllocateIDs(ctx, version); err != nil {
		return nil, err
	}

	// Note that due to historical reasons, the automatic creation of the primary
	// index occurs in AllocateIDs. That call does not have access to the current
	// timestamp to set the created_at timestamp.
	if desc.IsPhysicalTable() && !catalog.IsSystemDescriptor(&desc) {
		ts := evalCtx.GetTxnTimestamp(time.Microsecond).UnixNano()
		_ = catalog.ForEachNonDropIndex(&desc, func(idx catalog.Index) error {
			idx.IndexDesc().CreatedAtNanos = ts
			return nil
		})
	}

	// Record the types of indexes that the table has.
	if err := catalog.ForEachNonDropIndex(&desc, func(idx catalog.Index) error {
		if idx.IsSharded() {
			telemetry.Inc(sqltelemetry.HashShardedIndexCounter)
		}
		if idx.GetType() == descpb.IndexDescriptor_INVERTED {
			telemetry.Inc(sqltelemetry.InvertedIndexCounter)
			geoConfig := idx.GetGeoConfig()
			if !geoindex.IsEmptyConfig(&geoConfig) {
				if geoindex.IsGeographyConfig(&geoConfig) {
					telemetry.Inc(sqltelemetry.GeographyInvertedIndexCounter)
				} else if geoindex.IsGeometryConfig(&geoConfig) {
					telemetry.Inc(sqltelemetry.GeometryInvertedIndexCounter)
				}
			}
			if idx.IsPartial() {
				telemetry.Inc(sqltelemetry.PartialInvertedIndexCounter)
			}
			if idx.NumKeyColumns() > 1 {
				telemetry.Inc(sqltelemetry.MultiColumnInvertedIndexCounter)
			}
			if idx.GetPartitioning().NumColumns() != 0 {
				telemetry.Inc(sqltelemetry.PartitionedInvertedIndexCounter)
			}
		}
		if idx.IsPartial() {
			telemetry.Inc(sqltelemetry.PartialIndexCounter)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if regionConfig != nil || n.Locality != nil {
		localityTelemetryName := "unspecified"
		if n.Locality != nil {
			localityTelemetryName = n.Locality.TelemetryName()
		}
		telemetry.Inc(sqltelemetry.CreateTableLocalityCounter(localityTelemetryName))
		if n.Locality == nil {
			// The absence of a locality on the AST node indicates that the table must
			// be homed in the primary region.
			desc.SetTableLocalityRegionalByTable(tree.PrimaryRegionNotSpecifiedName)
		} else if n.Locality.LocalityLevel == tree.LocalityLevelTable {
			desc.SetTableLocalityRegionalByTable(n.Locality.TableRegion)
		} else if n.Locality.LocalityLevel == tree.LocalityLevelGlobal {
			desc.SetTableLocalityGlobal()
		} else if n.Locality.LocalityLevel == tree.LocalityLevelRow {
			desc.SetTableLocalityRegionalByRow(n.Locality.RegionalByRowColumn)
		} else {
			return nil, errors.Newf("unknown locality level: %v", n.Locality.LocalityLevel)
		}
	}

	return &desc, nil
}

// newTableDesc creates a table descriptor from a CreateTable statement.
func newTableDesc(
	params runParams,
	n *tree.CreateTable,
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
	id descpb.ID,
	creationTime hlc.Timestamp,
	privileges *catpb.PrivilegeDescriptor,
	affected map[descpb.ID]*tabledesc.Mutable,
) (ret *tabledesc.Mutable, err error) {
	if err := validateUniqueConstraintParamsForCreateTable(n); err != nil {
		return nil, err
	}

	newDefs, err := replaceLikeTableOpts(n, params)
	if err != nil {
		return nil, err
	}

	if newDefs != nil {
		// If we found any LIKE table defs, we actually modified the list of
		// defs during iteration, so we re-assign the resultant list back to
		// n.Defs.
		n.Defs = newDefs
	}

	// Process any SERIAL columns to remove the SERIAL type, as required by
	// NewTableDesc.
	colNameToOwnedSeq, err := createSequencesForSerialColumns(
		params.ctx, params.p, params.SessionData(), db, sc, n,
	)
	if err != nil {
		return nil, err
	}

	var regionConfig *multiregion.RegionConfig
	if db.IsMultiRegion() {
		conf, err := SynthesizeRegionConfig(params.ctx, params.p.txn, db.GetID(), params.p.Descriptors())
		if err != nil {
			return nil, err
		}
		regionConfig = &conf
	}

	// We need to run NewTableDesc with caching disabled, because it needs to pull
	// in descriptors from FK depended-on tables using their current state in KV.
	// See the comment at the start of NewTableDesc() and ResolveFK().
	params.p.runWithOptions(resolveFlags{skipCache: true, contextDatabaseID: db.GetID()}, func() {
		ret, err = NewTableDesc(
			params.ctx,
			params.p.txn,
			params.p,
			params.p.ExecCfg().Settings,
			n,
			db,
			sc,
			id,
			regionConfig,
			creationTime,
			privileges,
			affected,
			&params.p.semaCtx,
			params.EvalContext(),
			params.SessionData(),
			n.Persistence,
		)
	})
	if err != nil {
		return nil, err
	}

	// We need to ensure sequence ownerships so that column owned sequences are
	// correctly dropped when a column/table is dropped.
	for colName, seqDesc := range colNameToOwnedSeq {
		// When a table is first created, `affected` includes all the newly created
		// sequenced. So `affectedSeqDesc` should be always non-nil.
		affectedSeqDesc := affected[seqDesc.ID]
		if err := setSequenceOwner(affectedSeqDesc, colName, ret); err != nil {
			return nil, err
		}
	}

	// Row level TTL tables require a scheduled job to be created as well.
	if ttl := ret.RowLevelTTL; ttl != nil {
		j, err := CreateRowLevelTTLScheduledJob(
			params.ctx,
			params.ExecCfg(),
			params.p.txn,
			params.p.User(),
			ret.GetID(),
			ttl,
		)
		if err != nil {
			return nil, err
		}
		ttl.ScheduleID = j.ScheduleID()
	}
	return ret, nil
}

// defaultTTLScheduleCron is the default cron duration for row-level TTL.
// defaultTTLScheduleCron cannot be a cluster setting as this would involve
// changing all existing schedules to match the new setting.
const defaultTTLScheduleCron = "@hourly"

// newRowLevelTTLScheduledJob returns a *jobs.ScheduledJob for row level TTL
// for a given table.
func newRowLevelTTLScheduledJob(
	env scheduledjobs.JobSchedulerEnv,
	owner security.SQLUsername,
	tblID descpb.ID,
	ttl *catpb.RowLevelTTL,
) (*jobs.ScheduledJob, error) {
	sj := jobs.NewScheduledJob(env)
	sj.SetScheduleLabel(fmt.Sprintf("row-level-ttl-%d", tblID))
	sj.SetOwner(owner)
	sj.SetScheduleDetails(jobspb.ScheduleDetails{
		Wait: jobspb.ScheduleDetails_WAIT,
		// If a job fails, try again at the allocated cron time.
		OnError: jobspb.ScheduleDetails_RETRY_SCHED,
	})

	if err := sj.SetSchedule(rowLevelTTLSchedule(ttl)); err != nil {
		return nil, err
	}
	args := &catpb.ScheduledRowLevelTTLArgs{
		TableID: tblID,
	}
	any, err := pbtypes.MarshalAny(args)
	if err != nil {
		return nil, err
	}
	sj.SetExecutionDetails(
		tree.ScheduledRowLevelTTLExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: any},
	)
	return sj, nil
}

func rowLevelTTLSchedule(ttl *catpb.RowLevelTTL) string {
	if override := ttl.DeletionCron; override != "" {
		return override
	}
	return defaultTTLScheduleCron
}

func checkTTLEnabledForCluster(ctx context.Context, st *cluster.Settings) error {
	if !st.Version.IsActive(ctx, clusterversion.RowLevelTTL) {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"row level TTL is only available once the cluster is fully upgraded",
		)
	}
	return nil
}

// CreateRowLevelTTLScheduledJob creates a new row-level TTL schedule.
func CreateRowLevelTTLScheduledJob(
	ctx context.Context,
	execCfg *ExecutorConfig,
	txn *kv.Txn,
	owner security.SQLUsername,
	tblID descpb.ID,
	ttl *catpb.RowLevelTTL,
) (*jobs.ScheduledJob, error) {
	telemetry.Inc(sqltelemetry.RowLevelTTLCreated)
	env := JobSchedulerEnv(execCfg)
	j, err := newRowLevelTTLScheduledJob(env, owner, tblID, ttl)
	if err != nil {
		return nil, err
	}
	if err := j.Create(ctx, execCfg.InternalExecutor, txn); err != nil {
		return nil, err
	}
	return j, nil
}

func rowLevelTTLAutomaticColumnDef(ttl *catpb.RowLevelTTL) (*tree.ColumnTableDef, error) {
	def := &tree.ColumnTableDef{
		Name:   colinfo.TTLDefaultExpirationColumnName,
		Type:   types.TimestampTZ,
		Hidden: true,
	}
	intervalExpr, err := parser.ParseExpr(string(ttl.DurationExpr))
	if err != nil {
		return nil, errors.Wrapf(err, "unexpected expression for TTL duration")
	}
	def.DefaultExpr.Expr = rowLevelTTLAutomaticColumnExpr(intervalExpr)
	def.OnUpdateExpr.Expr = rowLevelTTLAutomaticColumnExpr(intervalExpr)
	return def, nil
}

func rowLevelTTLAutomaticColumnExpr(intervalExpr tree.Expr) tree.Expr {
	return &tree.BinaryExpr{
		Operator: treebin.MakeBinaryOperator(treebin.Plus),
		Left:     &tree.FuncExpr{Func: tree.WrapFunction("current_timestamp")},
		Right:    intervalExpr,
	}
}

// replaceLikeTableOps processes the TableDefs in the input CreateTableNode,
// searching for LikeTableDefs. If any are found, each LikeTableDef will be
// replaced in the output tree.TableDefs (which will be a copy of the input
// node's TableDefs) by an equivalent set of TableDefs pulled from the
// LikeTableDef's target table.
// If no LikeTableDefs are found, the output tree.TableDefs will be nil.
func replaceLikeTableOpts(n *tree.CreateTable, params runParams) (tree.TableDefs, error) {
	var newDefs tree.TableDefs
	for i, def := range n.Defs {
		d, ok := def.(*tree.LikeTableDef)
		if !ok {
			if newDefs != nil {
				newDefs = append(newDefs, def)
			}
			continue
		}
		// We're definitely going to be editing n.Defs now, so make a copy of it.
		if newDefs == nil {
			newDefs = make(tree.TableDefs, 0, len(n.Defs))
			newDefs = append(newDefs, n.Defs[:i]...)
		}
		_, td, err := params.p.ResolveMutableTableDescriptor(params.ctx, &d.Name, true, tree.ResolveRequireTableDesc)
		if err != nil {
			return nil, err
		}
		opts := tree.LikeTableOpt(0)
		// Process ons / offs.
		for _, opt := range d.Options {
			if opt.Excluded {
				opts &^= opt.Opt
			} else {
				opts |= opt.Opt
			}
		}

		// Copy defaults of implicitly created columns if they are needed by indexes.
		// This is required to ensure the newly created table still works as expected
		// as these columns are required for certain features to work when used
		// as an index.
		shouldCopyColumnDefaultSet := make(map[string]struct{})
		if opts.Has(tree.LikeTableOptIndexes) {
			for _, idx := range td.NonDropIndexes() {
				// Copy the rowid default if it was created implicitly by not specifying
				// PRIMARY KEY.
				if idx.Primary() && td.IsPrimaryIndexDefaultRowID() {
					for i := 0; i < idx.NumKeyColumns(); i++ {
						shouldCopyColumnDefaultSet[idx.GetKeyColumnName(i)] = struct{}{}
					}
				}
				// Copy any implicitly created columns (e.g. hash-sharded indexes,
				// REGIONAL BY ROW).
				for i := 0; i < idx.ExplicitColumnStartIdx(); i++ {
					for i := 0; i < idx.NumKeyColumns(); i++ {
						shouldCopyColumnDefaultSet[idx.GetKeyColumnName(i)] = struct{}{}
					}
				}
			}
		}

		defs := make(tree.TableDefs, 0)
		// Add all columns. Columns are always added.
		for i := range td.Columns {
			c := &td.Columns[i]
			if c.Inaccessible {
				// Inaccessible columns automatically get added by
				// the system; we don't need to add them ourselves here.
				continue
			}
			def := tree.ColumnTableDef{
				Name:   tree.Name(c.Name),
				Type:   c.Type,
				Hidden: c.Hidden,
			}
			if c.Nullable {
				def.Nullable.Nullability = tree.Null
			} else {
				def.Nullable.Nullability = tree.NotNull
			}
			if c.DefaultExpr != nil {
				_, shouldCopyColumnDefault := shouldCopyColumnDefaultSet[c.Name]
				if opts.Has(tree.LikeTableOptDefaults) || shouldCopyColumnDefault {
					def.DefaultExpr.Expr, err = parser.ParseExpr(*c.DefaultExpr)
					if err != nil {
						return nil, err
					}
				}
			}
			if c.ComputeExpr != nil {
				if opts.Has(tree.LikeTableOptGenerated) {
					def.Computed.Computed = true
					def.Computed.Virtual = c.Virtual
					def.Computed.Expr, err = parser.ParseExpr(*c.ComputeExpr)
					if err != nil {
						return nil, err
					}
				}
			}
			if c.OnUpdateExpr != nil {
				if opts.Has(tree.LikeTableOptDefaults) {
					def.OnUpdateExpr.Expr, err = parser.ParseExpr(*c.OnUpdateExpr)
					if err != nil {
						return nil, err
					}
				}
			}
			defs = append(defs, &def)
		}
		if opts.Has(tree.LikeTableOptConstraints) {
			for _, c := range td.Checks {
				def := tree.CheckConstraintTableDef{
					Name:   tree.Name(c.Name),
					Hidden: c.Hidden,
				}
				def.Expr, err = parser.ParseExpr(c.Expr)
				if err != nil {
					return nil, err
				}
				defs = append(defs, &def)
			}
			for _, c := range td.UniqueWithoutIndexConstraints {
				def := tree.UniqueConstraintTableDef{
					IndexTableDef: tree.IndexTableDef{
						Name:    tree.Name(c.Name),
						Columns: make(tree.IndexElemList, 0, len(c.ColumnIDs)),
					},
					WithoutIndex: true,
				}
				colNames, err := td.NamesForColumnIDs(c.ColumnIDs)
				if err != nil {
					return nil, err
				}
				for i := range colNames {
					def.Columns = append(def.Columns, tree.IndexElem{Column: tree.Name(colNames[i])})
				}
				defs = append(defs, &def)
				if c.IsPartial() {
					def.Predicate, err = parser.ParseExpr(c.Predicate)
					if err != nil {
						return nil, err
					}
				}
			}
		}
		if opts.Has(tree.LikeTableOptIndexes) {
			for _, idx := range td.NonDropIndexes() {
				indexDef := tree.IndexTableDef{
					Name:     tree.Name(idx.GetName()),
					Inverted: idx.GetType() == descpb.IndexDescriptor_INVERTED,
					Storing:  make(tree.NameList, 0, idx.NumSecondaryStoredColumns()),
					Columns:  make(tree.IndexElemList, 0, idx.NumKeyColumns()),
				}
				numColumns := idx.NumKeyColumns()
				if idx.IsSharded() {
					indexDef.Sharded = &tree.ShardedIndexDef{
						ShardBuckets: tree.NewDInt(tree.DInt(idx.GetSharded().ShardBuckets)),
					}
					numColumns = len(idx.GetSharded().ColumnNames)
				}
				for j := 0; j < numColumns; j++ {
					name := idx.GetKeyColumnName(j)
					if idx.IsSharded() {
						name = idx.GetSharded().ColumnNames[j]
					}
					elem := tree.IndexElem{
						Column:    tree.Name(name),
						Direction: tree.Ascending,
					}
					col, err := td.FindColumnWithID(idx.GetKeyColumnID(j))
					if err != nil {
						return nil, err
					}
					if col.IsExpressionIndexColumn() {
						elem.Column = ""
						elem.Expr, err = parser.ParseExpr(col.GetComputeExpr())
						if err != nil {
							return nil, err
						}
					}
					if idx.GetKeyColumnDirection(j) == descpb.IndexDescriptor_DESC {
						elem.Direction = tree.Descending
					}
					indexDef.Columns = append(indexDef.Columns, elem)
				}
				for j := 0; j < idx.NumSecondaryStoredColumns(); j++ {
					indexDef.Storing = append(indexDef.Storing, tree.Name(idx.GetStoredColumnName(j)))
				}
				var def tree.TableDef = &indexDef
				if idx.IsUnique() {
					def = &tree.UniqueConstraintTableDef{
						IndexTableDef: indexDef,
						PrimaryKey:    idx.Primary(),
					}
				}
				if idx.IsPartial() {
					indexDef.Predicate, err = parser.ParseExpr(idx.GetPredicate())
					if err != nil {
						return nil, err
					}
				}
				defs = append(defs, def)
			}
		}
		newDefs = append(newDefs, defs...)
	}
	return newDefs, nil
}

// makeShardColumnDesc returns a new column descriptor for a hidden computed shard column
// based on all the `colNames` and the bucket count. It delegates to one of
// makeHashShardComputeExpr.
func makeShardColumnDesc(colNames []string, buckets int) (*descpb.ColumnDescriptor, error) {
	col := &descpb.ColumnDescriptor{
		Hidden:   true,
		Nullable: false,
		Type:     types.Int4,
		Virtual:  true,
	}
	col.Name = tabledesc.GetShardColumnName(colNames, int32(buckets))
	col.ComputeExpr = schemaexpr.MakeHashShardComputeExpr(colNames, buckets)
	return col, nil
}

func makeShardCheckConstraintDef(
	buckets int, shardCol catalog.Column,
) (*tree.CheckConstraintTableDef, error) {
	values := &tree.Tuple{}
	for i := 0; i < buckets; i++ {
		const negative = false
		values.Exprs = append(values.Exprs, tree.NewNumVal(
			constant.MakeInt64(int64(i)),
			strconv.Itoa(i),
			negative))
	}
	return &tree.CheckConstraintTableDef{
		Expr: &tree.ComparisonExpr{
			Operator: treecmp.MakeComparisonOperator(treecmp.In),
			Left: &tree.ColumnItem{
				ColumnName: tree.Name(shardCol.GetName()),
			},
			Right: values,
		},
		Hidden: true,
	}, nil
}

// incTelemetryForNewColumn increments relevant telemetry every time a new column
// is added to a table.
func incTelemetryForNewColumn(def *tree.ColumnTableDef, desc *descpb.ColumnDescriptor) {
	switch desc.Type.Family() {
	case types.EnumFamily:
		sqltelemetry.IncrementEnumCounter(sqltelemetry.EnumInTable)
	default:
		telemetry.Inc(sqltelemetry.SchemaNewTypeCounter(desc.Type.TelemetryName()))
	}
	if desc.IsComputed() {
		if desc.Virtual {
			telemetry.Inc(sqltelemetry.SchemaNewColumnTypeQualificationCounter("virtual"))
		} else {
			telemetry.Inc(sqltelemetry.SchemaNewColumnTypeQualificationCounter("computed"))
		}
	}
	if desc.HasDefault() {
		telemetry.Inc(sqltelemetry.SchemaNewColumnTypeQualificationCounter("default_expr"))
	}
	if def.Unique.IsUnique {
		if def.Unique.WithoutIndex {
			telemetry.Inc(sqltelemetry.SchemaNewColumnTypeQualificationCounter("unique_without_index"))
		} else {
			telemetry.Inc(sqltelemetry.SchemaNewColumnTypeQualificationCounter("unique"))
		}
	}
	if desc.HasOnUpdate() {
		telemetry.Inc(sqltelemetry.SchemaNewColumnTypeQualificationCounter("on_update"))
	}
}

func regionalByRowRegionDefaultExpr(oid oid.Oid, region tree.Name) tree.Expr {
	return &tree.CastExpr{
		Expr:       tree.NewDString(string(region)),
		Type:       &tree.OIDTypeReference{OID: oid},
		SyntaxMode: tree.CastShort,
	}
}

func regionalByRowGatewayRegionDefaultExpr(oid oid.Oid) tree.Expr {
	return &tree.CastExpr{
		Expr: &tree.FuncExpr{
			Func: tree.WrapFunction(builtins.DefaultToDatabasePrimaryRegionBuiltinName),
			Exprs: []tree.Expr{
				&tree.FuncExpr{
					Func: tree.WrapFunction(builtins.GatewayRegionBuiltinName),
				},
			},
		},
		Type:       &tree.OIDTypeReference{OID: oid},
		SyntaxMode: tree.CastShort,
	}
}

// maybeRegionalByRowOnUpdateExpr returns a gateway region default statement if
// the auto rehoming session setting is enabled, nil otherwise.
func maybeRegionalByRowOnUpdateExpr(evalCtx *tree.EvalContext, enumOid oid.Oid) tree.Expr {
	if evalCtx.SessionData().AutoRehomingEnabled {
		return &tree.CastExpr{
			Expr: &tree.FuncExpr{
				Func: tree.WrapFunction(builtins.RehomeRowBuiltinName),
			},
			Type:       &tree.OIDTypeReference{OID: enumOid},
			SyntaxMode: tree.CastShort,
		}
	}
	return nil
}

func regionalByRowDefaultColDef(
	oid oid.Oid, defaultExpr tree.Expr, onUpdateExpr tree.Expr,
) *tree.ColumnTableDef {
	c := &tree.ColumnTableDef{
		Name:   tree.RegionalByRowRegionDefaultColName,
		Type:   &tree.OIDTypeReference{OID: oid},
		Hidden: true,
	}
	c.Nullable.Nullability = tree.NotNull
	c.DefaultExpr.Expr = defaultExpr
	c.OnUpdateExpr.Expr = onUpdateExpr

	return c
}

// setSequenceOwner adds sequence id to the sequence id list owned by a column
// and set ownership values of sequence options.
func setSequenceOwner(
	seqDesc *tabledesc.Mutable, colName tree.Name, table *tabledesc.Mutable,
) error {
	if !seqDesc.IsSequence() {
		return errors.Errorf("%s is not a sequence", seqDesc.Name)
	}

	col, err := table.FindColumnWithName(colName)
	if err != nil {
		return err
	}
	found := false
	for _, seqID := range col.ColumnDesc().OwnsSequenceIds {
		if seqID == seqDesc.ID {
			found = true
			break
		}
	}
	if !found {
		col.ColumnDesc().OwnsSequenceIds = append(col.ColumnDesc().OwnsSequenceIds, seqDesc.ID)
	}
	seqDesc.SequenceOpts.SequenceOwner.OwnerTableID = table.ID
	seqDesc.SequenceOpts.SequenceOwner.OwnerColumnID = col.GetID()

	return nil
}

// validateUniqueConstraintParamsForCreateTable validate storage params of
// unique constraints passed in through `CREATE TABLE` statement.
func validateUniqueConstraintParamsForCreateTable(n *tree.CreateTable) error {
	for _, def := range n.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			if err := paramparse.ValidateUniqueConstraintParams(
				d.PrimaryKey.StorageParams,
				paramparse.UniqueConstraintParamContext{
					IsPrimaryKey: true,
					IsSharded:    d.PrimaryKey.Sharded,
				}); err != nil {
				return err
			}
		case *tree.UniqueConstraintTableDef:
			if err := paramparse.ValidateUniqueConstraintParams(
				d.IndexTableDef.StorageParams,
				paramparse.UniqueConstraintParamContext{
					IsPrimaryKey: d.PrimaryKey,
					IsSharded:    d.Sharded != nil,
				},
			); err != nil {
				return err
			}
		case *tree.IndexTableDef:
			if d.Sharded == nil && d.StorageParams.GetVal(`bucket_count`) != nil {
				return pgerror.New(
					pgcode.InvalidParameterValue,
					`"bucket_count" storage param should only be set with "USING HASH" for hash sharded index`,
				)
			}
		}
	}
	return nil
}

// validateUniqueConstraintParamsForCreateTableAs validate storage params of
// unique constraints passed in through `CREATE TABLE...AS...` statement.
func validateUniqueConstraintParamsForCreateTableAs(n *tree.CreateTable) error {
	// TODO (issue 75896): enable storage parameters of primary key.
	const errMsg = `storage parameters are not supported on primary key for CREATE TABLE...AS... statement`
	for _, def := range n.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			if len(d.PrimaryKey.StorageParams) > 0 {
				return pgerror.New(pgcode.FeatureNotSupported, errMsg)
			}
		case *tree.UniqueConstraintTableDef:
			if d.PrimaryKey && len(d.StorageParams) > 0 {
				return pgerror.New(pgcode.FeatureNotSupported, errMsg)
			}
		}
	}
	return nil
}
