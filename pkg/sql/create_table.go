// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/storageparam"
	"github.com/cockroachdb/cockroach/pkg/sql/storageparam/indexstorageparam"
	"github.com/cockroachdb/cockroach/pkg/sql/storageparam/tablestorageparam"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/lib/pq/oid"
)

type createTableNode struct {
	n      *tree.CreateTable
	dbDesc catalog.DatabaseDescriptor
	input  planNode
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
	sc, err := p.Descriptors().ByName(p.txn).Get().Schema(ctx, db, scName)
	if err != nil {
		return nil, err
	}
	switch sc.SchemaKind() {
	case catalog.SchemaPublic:
		return sc, nil
	case catalog.SchemaUserDefined:
		sc, err = p.Descriptors().ByIDWithoutLeased(p.Txn()).Get().Schema(ctx, sc.GetID())
		if err != nil {
			return nil, err
		}
		// Exit early with an error if the schema is undergoing any legacy
		// or declarative schema change.
		if sc.HasConcurrentSchemaChanges() {
			return nil, scerrors.ConcurrentSchemaChangeError(sc)
		}
		return sc, nil
	case catalog.SchemaVirtual:
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege, "schema cannot be modified: %q", scName)
	default:
		return nil, errors.AssertionFailedf(
			"invalid schema kind for getNonTemporarySchemaForCreate: %d", sc.SchemaKind())
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
	if tableName.Schema() == catconstants.PublicSchemaName {
		if _, ok := types.PublicSchemaAliases[tableName.Object()]; ok {
			return nil, sqlerrors.NewTypeAlreadyExistsError(tableName.Object())
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
					pgcode.ExperimentalFeature,
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

	desc, err := descs.GetDescriptorCollidingWithObjectName(
		params.ctx,
		params.p.Descriptors(),
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
		searchPath := params.p.CurrentSearchPath()
		fd, err := funcExpr.Func.Resolve(params.ctx, &searchPath, params.p.semaCtx.FunctionResolver)
		if err != nil {
			return false, err
		}
		if fd.Name == "nextval" {
			return true, nil
		}
	}

	return false, nil
}

func (n *createTableNode) startExec(params runParams) error {
	// Check if the parent object is a replicated PCR descriptor, which will block
	// schema changes.
	if n.dbDesc.GetReplicatedPCRVersion() != 0 {
		return pgerror.Newf(pgcode.ReadOnlySQLTransaction, "schema changes are not allowed on a reader catalog")
	}

	telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter("table"))

	colsWithPrimaryKeyConstraint := make(map[tree.Name]bool)

	// Copy column definition slice, since we will modify it below. This
	// ensures, that  nothing bad happens on a transaction retry errors, for
	// example we implicitly add columns for REGIONAL BY ROW.
	// Note: This is only a shallow copy and meant to deal with addition /
	// deletion into the slice.
	defsCopy := make(tree.TableDefs, 0, len(n.n.Defs))
	defsCopy = append(defsCopy, n.n.Defs...)
	defer func(originalDefs tree.TableDefs) { n.n.Defs = originalDefs }(n.n.Defs)
	n.n.Defs = defsCopy

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

	id, err := params.extendedEvalCtx.DescIDGenerator.
		GenerateUniqueDescID(params.ctx)
	if err != nil {
		return err
	}

	var desc *tabledesc.Mutable
	var affected map[descpb.ID]*tabledesc.Mutable
	// creationTime is usually initialized to a zero value and populated at read
	// time. See the comment in desc.MaybeIncrementVersion. However, for CREATE
	// TABLE AS, we need to set the creation time to the specified timestamp.
	var creationTime hlc.Timestamp
	if asOf := params.p.extendedEvalCtx.AsOfSystemTime; asOf != nil {
		if asOf.ForBackfill {
			creationTime = asOf.Timestamp
		}
	}
	privs, err := catprivilege.CreatePrivilegesFromDefaultPrivileges(
		n.dbDesc.GetDefaultPrivilegeDescriptor(),
		schema.GetDefaultPrivilegeDescriptor(),
		n.dbDesc.GetID(),
		params.SessionData().User(),
		privilege.Tables,
	)
	if err != nil {
		return err
	}
	if n.n.As() {
		params.p.BufferClientNotice(
			params.ctx,
			pgnotice.Newf("CREATE TABLE ... AS does not copy over "+
				"indexes, default expressions, or constraints; the new table "+
				"has a hidden rowid primary key column"),
		)

		asCols := planColumns(n.input)
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

		// If we have a single statement txn we want to run CTAS async, and
		// consequently ensure it gets queued as a SchemaChange.
		if params.extendedEvalCtx.TxnIsSingleStmt {
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
			// TODO(chengxiong): do we need to do something here? Like. add logic to find all references.
			refs, err := desc.FindAllReferences()
			if err != nil {
				return err
			}
			var foundExternalReference bool
			for id := range refs {
				if _, t, err := params.p.Descriptors().GetUncommittedMutableTableByID(id); err != nil {
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

	// Replace all UDF names with OIDs in check constraints and update back
	// references in functions used.
	for _, ck := range desc.CheckConstraints() {
		if err := params.p.updateFunctionReferencesForCheck(params.ctx, desc, ck.CheckDesc()); err != nil {
			return err
		}
	}

	// Update cross-references between functions and columns.
	for i := range desc.Columns {
		if err := params.p.maybeUpdateFunctionReferencesForColumn(params.ctx, desc, &desc.Columns[i]); err != nil {
			return err
		}
	}

	// Descriptor written to store here.
	if err := params.p.createDescriptor(
		params.ctx,
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
		dbDesc, err := params.p.Descriptors().ByIDWithoutLeased(params.p.txn).WithoutNonPublic().Get().Database(params.ctx, desc.ParentID)
		if err != nil {
			return errors.Wrap(err, "error resolving database for multi-region")
		}

		regionConfig, err := SynthesizeRegionConfig(params.ctx, params.p.txn, dbDesc.GetID(), params.p.Descriptors())
		if err != nil {
			return err
		}

		if err := ApplyZoneConfigForMultiRegionTable(
			params.ctx,
			params.p.InternalSQLTxn(),
			params.p.ExecCfg(),
			params.p.extendedEvalCtx.Tracing.KVTracingEnabled(),
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
			typeDesc, err := params.p.Descriptors().MutableByID(params.p.txn).Type(params.ctx, regionEnumID)
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

	// If we are in a multi-statement txn or the source has placeholders, we
	// execute the CTAS query synchronously.
	if n.n.As() && !params.extendedEvalCtx.TxnIsSingleStmt {
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
				nil, /* uniqueWithTombstoneIndexes */
				desc.PublicColumns(),
				&tree.DatumAlloc{},
				&params.ExecCfg().Settings.SV,
				internal,
				params.ExecCfg().GetRowMetrics(internal),
			)
			if err != nil {
				return err
			}
			ti := tableInserterPool.Get().(*tableInserter)
			*ti = tableInserter{ri: ri}
			defer func() {
				ti.close(params.ctx)
				*ti = tableInserter{}
				tableInserterPool.Put(ti)
			}()
			if err := ti.init(params.ctx, params.p.txn, params.p.EvalContext()); err != nil {
				return err
			}

			// Prepare the buffer for row values. At this point, one more column has
			// been added by ensurePrimaryKey() to the list of columns in input, if
			// a PRIMARY KEY is not specified by the user.
			rowBuffer := make(tree.Datums, len(desc.Columns))

			for {
				if err := params.p.cancelChecker.Check(); err != nil {
					return err
				}
				if next, err := n.input.Next(params); !next {
					if err != nil {
						return err
					}
					if err := ti.finalize(params.ctx); err != nil {
						return err
					}
					break
				}

				// Periodically flush out the batches, so that we don't issue gigantic
				// raft commands.
				if ti.currentBatchSize >= ti.maxBatchSize ||
					ti.b.ApproximateMutationBytes() >= ti.maxBatchByteSize {
					if err := ti.flushAndStartNewBatch(params.ctx); err != nil {
						return err
					}
				}

				// Populate the buffer.
				copy(rowBuffer, n.input.Values())

				// CREATE TABLE AS does not copy indexes from the input table.
				// An empty row.PartialIndexUpdateHelper is used here because
				// there are no indexes, partial or otherwise, to update.
				var pm row.PartialIndexUpdateHelper
				if err := ti.row(params.ctx, rowBuffer, pm, params.extendedEvalCtx.Tracing.KVTracingEnabled()); err != nil {
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
	if n.input != nil {
		n.input.Close(ctx)
		n.input = nil
	}
}

func (n *createTableNode) InputCount() int {
	if n.n.As() {
		return 1
	}
	return 0
}

func (n *createTableNode) Input(i int) (planNode, error) {
	if i == 0 && n.n.As() {
		return n.input, nil
	}
	return nil, errors.AssertionFailedf("input index %d is out of range", i)
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
	evalCtx *eval.Context,
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
	evalCtx *eval.Context,
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
	if d.Invisibility.Value != 0.0 {
		// Theoretically, this should never happen because this is not supported by
		// the parser. This is just a safe check.
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"creating a unique constraint using UNIQUE WITH NOT VISIBLE INDEX is not supported",
		)
	}

	// If there is a predicate, validate it.
	var predicate string
	if d.Predicate != nil {
		var err error
		predicate, err = schemaexpr.ValidateUniqueWithoutIndexPredicate(
			ctx, tn, desc, d.Predicate, semaCtx, evalCtx.Settings.Version.ActiveVersionOrEmpty(ctx),
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
	if constraintName == "" {
		constraintName = tabledesc.GenerateUniqueName(
			fmt.Sprintf("unique_%s", strings.Join(colNames, "_")),
			func(p string) bool {
				return catalog.FindConstraintByName(tbl, p) != nil
			},
		)
	} else {
		if c := catalog.FindConstraintByName(tbl, constraintName); c != nil {
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
	evalCtx *eval.Context,
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
			return errors.WithHint(
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
		numImplicitCols := target.GetPrimaryIndex().ImplicitPartitioningColumnCount()
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

	referencedCols, err := catalog.MustFindPublicColumnsByNameList(target, referencedColNames)
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
				s.ColName(), s.GetType().SQLString(), target.Name, t.GetName(), t.GetType().SQLString())
			evalCtx.ClientNoticeSender.BufferClientNotice(ctx, notice)
		}
	}

	// Verify we are not writing a constraint over the same name.
	// This check is done in Verify(), but we must do it earlier
	// or else we can hit other checks that break things with
	// undesired error codes, e.g. #42858.
	// It may be removable after #37255 is complete.
	constraintName := string(d.Name)
	if constraintName == "" {
		constraintName = tabledesc.GenerateUniqueName(
			tabledesc.ForeignKeyConstraintName(tbl.GetName(), d.FromCols.ToStrings()),
			func(p string) bool {
				return catalog.FindConstraintByName(tbl, p) != nil
			},
		)
	} else {
		if c := catalog.FindConstraintByName(tbl, constraintName); c != nil {
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

	// We disallow any ON UPDATE and ON DELETE action that will modify the fk
	// column of a computed key. The key value is computed and cannot change.
	if d.Actions.HasDisallowedActionForComputedFKCol() {
		for _, originColumn := range originCols {
			if originColumn.IsComputed() {
				return sqlerrors.NewInvalidActionOnComputedFKColumnError(d.Actions.HasUpdateAction())
			}
		}
	}

	var validity descpb.ConstraintValidity
	if ts != NewTable {
		if validationBehavior == tree.ValidationSkip {
			validity = descpb.ConstraintValidity_Unvalidated
		} else {
			validity = descpb.ConstraintValidity_Validating
		}
	}

	// Adding a foreign key dependency on a table with row-level TTL enabled can
	// cause a slowdown in the TTL deletion job as the number of rows to be updated per
	// deletion can go up. In such a case, flag a notice to the user advising them to
	// update the ttl_delete_batch_size to avoid generating TTL deletion jobs with a high
	// cardinality of rows being deleted.
	// See https://github.com/cockroachdb/cockroach/issues/125103 for more details.
	if target.HasRowLevelTTL() {
		// Use foreign key actions to determine upstream impact and flag a notice if the
		// actions for delete involve cascading deletes.
		if d.Actions.Delete != tree.NoAction && d.Actions.Delete != tree.Restrict {
			evalCtx.ClientNoticeSender.BufferClientNotice(
				ctx,
				pgnotice.Newf("Table %s has row level TTL enabled. This will make TTL deletion jobs"+
					" more expensive as dependent rows will need to be updated as well. To improve performance"+
					" of the TTL job, consider reducing the value of ttl_delete_batch_size.",
					target.GetName()))
		}
	}

	ref := descpb.ForeignKeyConstraint{
		OriginTableID:       tbl.ID,
		OriginColumnIDs:     originColumnIDs,
		ReferencedColumnIDs: targetColIDs,
		ReferencedTableID:   target.ID,
		Name:                constraintName,
		Validity:            validity,
		OnDelete:            tree.ForeignKeyReferenceActionValue[d.Actions.Delete],
		OnUpdate:            tree.ForeignKeyReferenceActionValue[d.Actions.Update],
		Match:               tree.CompositeKeyMatchMethodValue[d.Match],
		ConstraintID:        tbl.NextConstraintID,
	}
	tbl.NextConstraintID++
	if ts == NewTable {
		tbl.OutboundFKs = append(tbl.OutboundFKs, ref)
		target.InboundFKs = append(target.InboundFKs, ref)
	} else {
		tbl.AddForeignKeyMutation(&ref, descpb.DescriptorMutation_ADD)
	}

	c, err := catalog.MustFindConstraintByID(tbl, ref.ConstraintID)
	if err != nil {
		return errors.HandleAsAssertionFailure(err)
	}
	// Ensure that there is a unique constraint on the referenced side to use.
	_, err = catalog.FindFKReferencedUniqueConstraint(target, c.(catalog.ForeignKeyConstraint))
	return err
}

// CreatePartitioning returns a set of implicit columns and a new partitioning
// descriptor to build an index with partitioning fields populated to align with
// the tree.PartitionBy clause.
func CreatePartitioning(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *eval.Context,
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
		func(name tree.Name) (catalog.Column, error) {
			return catalog.MustFindColumnByTreeName(tableDesc, name)
		},
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
	evalCtx *eval.Context,
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
	params runParams, source *tree.Select, evalCtx *eval.Context,
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
			d, err := eval.Expr(params.ctx, evalCtx, placeholder)
			if err != nil {
				panic(errors.NewAssertionErrorWithWrappedErrf(err, "failed to serialize placeholder"))
			}
			d.Format(ctx)
		}),
	)
	ctx.FormatNode(source)

	// Use IDs instead of sequence names because name resolution depends on
	// session data, and the internal executor has different session data.
	sequenceReplacedQuery, err := replaceSeqNamesWithIDs(params.ctx, params.p, ctx.CloseAndGetString(), false /* multiStmt */)
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
	evalContext *eval.Context,
) (desc *tabledesc.Mutable, err error) {
	if err := validateUniqueConstraintParamsForCreateTableAs(p); err != nil {
		return nil, err
	}

	// If there are no TableDefs defined by the parser, then we construct a
	// ColumnTableDef for each column using resultColumns.
	if len(p.Defs) == 0 {
		for _, colRes := range resultColumns {
			var d *tree.ColumnTableDef
			var ok bool
			var tableDef tree.TableDef = &tree.ColumnTableDef{
				Name:       tree.Name(colRes.Name),
				Type:       colRes.Typ,
				IsCreateAs: true,
				Hidden:     colRes.Hidden,
			}
			if d, ok = tableDef.(*tree.ColumnTableDef); !ok {
				return nil, errors.Errorf("failed to cast type to ColumnTableDef\n")
			}
			d.Nullable.Nullability = tree.SilentNull
			p.Defs = append(p.Defs, tableDef)
		}
	} else {
		colResIndex := 0
		// TableDefs for a CREATE TABLE ... AS AST node comprise of a ColumnTableDef
		// for each column, and a ConstraintTableDef for any constraints on those
		// columns.
		for _, defs := range p.Defs {
			var d *tree.ColumnTableDef
			var ok bool
			if d, ok = defs.(*tree.ColumnTableDef); ok {
				d.Type = resultColumns[colResIndex].Typ
				d.IsCreateAs = true
				colResIndex++
			}
		}
	}

	// Check if there is any reference to a user defined type that belongs to
	// another database which is not allowed.
	for _, def := range p.Defs {
		if d, ok := def.(*tree.ColumnTableDef); ok {
			// In CTAS, ColumnTableDef are generated from resultColumns which are
			// resolved already. So we may cast it to *types.T directly without
			// resolving it again.
			typ := d.Type.(*types.T)
			if typ.UserDefined() {
				tn, typDesc, err := params.p.GetTypeDescriptor(params.ctx, typedesc.UserDefinedTypeOIDToID(typ.Oid()))
				if err != nil {
					return nil, err
				}
				if typDesc.GetParentID() != db.GetID() {
					return nil, pgerror.Newf(
						pgcode.FeatureNotSupported, "cross database type references are not supported: %s", tn.String())
				}
			}
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
	evalCtx *eval.Context,
	sessionData *sessiondata.SessionData,
	persistence tree.Persistence,
	colToSequenceRefs map[tree.Name]*tabledesc.Mutable,
	inOpts ...NewTableDescOption,
) (*tabledesc.Mutable, error) {

	version := st.Version.ActiveVersionOrEmpty(ctx)
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

	setter := tablestorageparam.NewSetter(&desc)
	if err := storageparam.Set(
		ctx,
		semaCtx,
		evalCtx,
		n.StorageParams,
		setter,
	); err != nil {
		return nil, err
	}
	setter.TableDesc.RowLevelTTL = setter.UpdatedRowLevelTTL

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
					if t.Oid() != catid.TypeIDToOID(regionConfig.RegionEnumID()) {
						err = pgerror.Newf(
							pgcode.InvalidTableDefinition,
							"cannot use column %s which has type %s in REGIONAL BY ROW",
							d.Name,
							t.SQLString(),
						)
						if t, terr := vt.ResolveTypeByOID(
							ctx,
							catid.TypeIDToOID(regionConfig.RegionEnumID()),
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
			oid := catid.TypeIDToOID(regionConfig.RegionEnumID())
			n.Defs = append(
				n.Defs,
				multiregion.RegionalByRowDefaultColDef(
					oid,
					multiregion.RegionalByRowGatewayRegionDefaultExpr(oid),
					multiregion.MaybeRegionalByRowOnUpdateExpr(evalCtx, oid),
				),
			)
			cdd = append(cdd, nil)
		}

		// Construct the partitioning for the PARTITION ALL BY.
		desc.PartitionAllBy = true
		partitionAllBy = multiregion.PartitionByForRegionalByRow(
			*regionConfig,
			regionalByRowCol,
		)
		// Leading region column of REGIONAL BY ROW is part of the primary
		// index column set.
		primaryIndexColumnSet[string(regionalByRowCol)] = struct{}{}
	}

	// Create the TTL automatic column (crdb_internal_expiration) if one does not already exist.
	if desc.HasRowLevelTTL() {
		ttl := desc.GetRowLevelTTL()
		if ttl.HasDurationExpr() {
			hasRowLevelTTLColumn := false
			for _, def := range n.Defs {
				switch def := def.(type) {
				case *tree.ColumnTableDef:
					if def.Name == catpb.TTLDefaultExpirationColumnName {
						// If we find the column, make sure it has the expected type.
						if def.Type.SQLString() != types.TimestampTZ.SQLString() {
							return nil, pgerror.Newf(
								pgcode.InvalidTableDefinition,
								`table %s has TTL defined, but column %s is not a %s`,
								def.Name,
								catpb.TTLDefaultExpirationColumnName,
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
						pgcode.ExperimentalFeature,
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
				primaryIndexColumnSet[shardCol.GetName()] = struct{}{}
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

			cdd[i], err = tabledesc.MakeColumnDefDescs(ctx, d, semaCtx, evalCtx, tree.ColumnDefaultExprInNewTable)
			if err != nil {
				return nil, err
			}
			col := cdd[i].ColumnDescriptor
			idx := cdd[i].PrimaryKeyOrUniqueIndexDescriptor

			// If necessary add any sequence references for this column, which is
			// only needed for SERIAL / IDENTITY columns on create.
			if colToSequenceRefs != nil {
				if seqDesc := colToSequenceRefs[d.Name]; seqDesc != nil {
					col.UsesSequenceIds = append(col.UsesSequenceIds, seqDesc.GetID())
				}
			}

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
			primaryIndexColumnSet[string(implicitColumnDefIdx.def.Name)] = struct{}{}
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
		checkConstraintDesc, err := ckBuilder.Build(checkConstraint, version)
		if err != nil {
			return nil, err
		}
		for _, def := range n.Defs {
			if inputCheckConstraint, ok := def.(*tree.CheckConstraintTableDef); ok {
				inputCheckConstraintDesc, err := ckBuilder.Build(inputCheckConstraint, version)
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

	// Copies the index elements, and returns a closure to restore them back,
	// so that any mutation to the AST is undone once this statement completes.
	copyIndexElemListAndRestore := func(existingList *tree.IndexElemList) func() {
		newList := make(tree.IndexElemList, len(*existingList))
		copy(newList, *existingList)
		restoreList := *existingList
		*existingList = newList
		return func() {
			*existingList = restoreList
		}
	}
	// Now that we have all the other columns set up, we can validate
	// any computed columns.
	for _, def := range n.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			if d.IsComputed() {
				serializedExpr, _, err := schemaexpr.ValidateComputedColumnExpression(
					ctx, &desc, d, &n.Table, tree.ComputedColumnExprContext(d.IsVirtual()), semaCtx, version,
				)
				if err != nil {
					return nil, err
				}
				col, err := catalog.MustFindColumnByTreeName(&desc, d.Name)
				if err != nil {
					return nil, err
				}
				col.ColumnDesc().ComputeExpr = &serializedExpr
			}

			// Validate storage parameters for
			// CREATE TABLE ... (x INT PRIMARY KEY USING HASH WITH (...));
			if d.PrimaryKey.IsPrimaryKey {
				if err := storageparam.Set(
					ctx,
					semaCtx,
					evalCtx,
					d.PrimaryKey.StorageParams,
					&indexstorageparam.Setter{
						IndexDesc: &descpb.IndexDescriptor{},
					}); err != nil {
					return nil, err
				}
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
				if idx := catalog.FindIndexByName(&desc, d.Name.String()); idx != nil {
					return nil, pgerror.Newf(pgcode.DuplicateRelation, "duplicate index name: %q", d.Name)
				}
			}
			if err := validateColumnsAreAccessible(&desc, d.Columns); err != nil {
				return nil, err
			}
			// We are going to modify the AST to replace any index expressions with
			// virtual columns. If the txn ends up retrying, then this change is not
			// syntactically valid, since the virtual column is only added in the descriptor
			// and not in the AST.
			//nolint:deferloop
			defer copyIndexElemListAndRestore(&d.Columns)()
			if err := replaceExpressionElemsWithVirtualCols(
				ctx,
				&desc,
				&n.Table,
				d.Columns,
				d.Type,
				true, /* isNewTable */
				semaCtx,
				version,
			); err != nil {
				return nil, err
			}
			if err := checkIndexColumns(&desc, d.Columns, d.Storing, d.Type, version); err != nil {
				return nil, err
			}
			idx := descpb.IndexDescriptor{
				Name:             string(d.Name),
				StoreColumnNames: d.Storing.ToStrings(),
				Version:          indexEncodingVersion,
				NotVisible:       d.Invisibility.Value != 0.0,
				Invisibility:     d.Invisibility.Value,
				Type:             d.Type,
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
			if d.Type == idxtype.INVERTED {
				column, err := catalog.MustFindColumnByName(&desc, idx.InvertedColumnName())
				if err != nil {
					return nil, err
				}
				if err := populateInvertedIndexDescriptor(
					ctx, evalCtx.Settings, column, &idx, columns[len(columns)-1]); err != nil {
					return nil, err
				}
			}
			if d.Type == idxtype.VECTOR {
				column, err := catalog.MustFindColumnByName(&desc, idx.VectorColumnName())
				if err != nil {
					return nil, err
				}
				idx.VecConfig.Dims = column.GetType().Width()
				idx.VecConfig.Seed = evalCtx.GetRNG().Int63()
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
					ctx, &desc, d.Predicate, &n.Table, semaCtx, version,
				)
				if err != nil {
					return nil, err
				}
				idx.Predicate = expr
			}
			if err := storageparam.Set(
				ctx,
				semaCtx,
				evalCtx,
				d.StorageParams,
				&indexstorageparam.Setter{IndexDesc: &idx},
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
				if idx := catalog.FindIndexByName(&desc, d.Name.String()); idx != nil {
					return nil, pgerror.Newf(pgcode.DuplicateRelation, "duplicate index name: %q", d.Name)
				}
			}
			if err := validateColumnsAreAccessible(&desc, d.Columns); err != nil {
				return nil, err
			}
			// We are going to modify the AST to replace any index expressions with
			// virtual columns. If the txn ends up retrying, then this change is not
			// syntactically valid, since the virtual descriptor is only added in the descriptor
			// and not in the AST.
			//nolint:deferloop
			defer copyIndexElemListAndRestore(&d.Columns)()
			if err := replaceExpressionElemsWithVirtualCols(
				ctx,
				&desc,
				&n.Table,
				d.Columns,
				d.Type,
				true, /* isNewTable */
				semaCtx,
				version,
			); err != nil {
				return nil, err
			}
			if err := checkIndexColumns(&desc, d.Columns, d.Storing, d.Type, version); err != nil {
				return nil, err
			}
			idx := descpb.IndexDescriptor{
				Name:             string(d.Name),
				Unique:           true,
				StoreColumnNames: d.Storing.ToStrings(),
				Version:          indexEncodingVersion,
				NotVisible:       d.Invisibility.Value != 0.0,
				Invisibility:     d.Invisibility.Value,
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
					ctx, &desc, d.Predicate, &n.Table, semaCtx, version,
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

			// Validate storage parameters for
			// CREATE TABLE ... (x INT, PRIMARY KEY (x) USING HASH WITH (...));
			if err := storageparam.Set(
				ctx,
				semaCtx,
				evalCtx,
				d.StorageParams,
				&indexstorageparam.Setter{IndexDesc: &idx},
			); err != nil {
				return nil, err
			}
		case *tree.CheckConstraintTableDef, *tree.ForeignKeyConstraintTableDef, *tree.FamilyTableDef:
			// pass, handled below.

		default:
			return nil, errors.Errorf("unsupported table def: %T", def)
		}
	}

	for i := range desc.Columns {
		if _, ok := primaryIndexColumnSet[desc.Columns[i].Name]; ok {
			desc.Columns[i].Nullable = false
		}
	}

	// Now that all columns are in place, add any explicit families (this is done
	// here, rather than in the constraint pass below since we want to pick up
	// explicit allocations before AllocateIDs adds implicit ones).
	for _, def := range n.Defs {
		if d, ok := def.(*tree.FamilyTableDef); ok {
			desc.AddFamily(descpb.ColumnFamilyDescriptor{
				Name:        string(d.Name),
				ColumnNames: d.Columns.ToStrings(),
			})
		}
	}
	if err := desc.AllocateIDs(ctx, version); err != nil {
		return nil, err
	}

	// If explicit primary keys are required, error out if a primary key was not
	// supplied.
	if desc.IsPhysicalTable() &&
		evalCtx != nil && evalCtx.SessionData() != nil &&
		evalCtx.SessionData().RequireExplicitPrimaryKeys &&
		desc.IsPrimaryIndexDefaultRowID() {
		return nil, errors.Errorf(
			"no primary key specified for table %s (require_explicit_primary_keys = true)", desc.Name)
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
						if idx.GetEncodingType() != catenumpb.SecondaryIndexEncoding {
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
				if err := cdd[i].ForEachTypedExpr(func(expr tree.TypedExpr, colExprKind tabledesc.ColExprKind) error {
					changedSeqDescs, err := maybeAddSequenceDependencies(
						ctx, st, vt, &desc, &desc.Columns[colIdx], expr, affected, colExprKind)
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
			ck, err := ckBuilder.Build(d, version)
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
		if idx.GetType() == idxtype.INVERTED {
			telemetry.Inc(sqltelemetry.InvertedIndexCounter)
			geoConfig := idx.GetGeoConfig()
			if !geoConfig.IsEmpty() {
				if geoConfig.IsGeography() {
					telemetry.Inc(sqltelemetry.GeographyInvertedIndexCounter)
				} else if geoConfig.IsGeometry() {
					telemetry.Inc(sqltelemetry.GeometryInvertedIndexCounter)
				}
			}
			if idx.InvertedColumnKind() == catpb.InvertedIndexColumnKind_TRIGRAM {
				telemetry.Inc(sqltelemetry.TrigramInvertedIndexCounter)
			}
			if idx.IsPartial() {
				telemetry.Inc(sqltelemetry.PartialInvertedIndexCounter)
			}
			if idx.NumKeyColumns() > 1 {
				telemetry.Inc(sqltelemetry.MultiColumnInvertedIndexCounter)
			}
			if idx.PartitioningColumnCount() != 0 {
				telemetry.Inc(sqltelemetry.PartitionedInvertedIndexCounter)
			}
		}
		if idx.GetType() == idxtype.VECTOR {
			telemetry.Inc(sqltelemetry.VectorIndexCounter)
			if idx.IsPartial() {
				telemetry.Inc(sqltelemetry.PartialVectorIndexCounter)
			}
			if idx.NumKeyColumns() > 1 {
				telemetry.Inc(sqltelemetry.MultiColumnVectorIndexCounter)
			}
			if idx.PartitioningColumnCount() != 0 {
				telemetry.Inc(sqltelemetry.PartitionedVectorIndexCounter)
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
			localityTelemetryName = multiregion.TelemetryNameForLocality(n.Locality)
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
			params.p.SemaCtx(),
			params.EvalContext(),
			params.SessionData(),
			n.Persistence,
			colNameToOwnedSeq,
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
	if ret.HasRowLevelTTL() {
		ttl := ret.GetRowLevelTTL()
		if err := schemaexpr.ValidateTTLExpirationExpression(
			params.ctx, ret, params.p.SemaCtx(), &n.Table, ttl, params.ExecCfg().Settings.Version.ActiveVersionOrEmpty(params.ctx),
		); err != nil {
			return nil, err
		}

		params.p.Txn()
		j, err := CreateRowLevelTTLScheduledJob(
			params.ctx,
			params.ExecCfg().JobsKnobs(),
			jobs.ScheduledJobTxn(params.p.InternalSQLTxn()),
			params.p.User(),
			ret,
			params.p.extendedEvalCtx.ClusterID,
			params.p.execCfg.Settings.Version.ActiveVersion(params.ctx),
		)
		if err != nil {
			return nil, err
		}
		ttl.ScheduleID = j.ScheduleID()
	}
	return ret, nil
}

// newRowLevelTTLScheduledJob returns a *jobs.ScheduledJob for row level TTL
// for a given table. newRowLevelTTLScheduledJob assumes that
// tblDesc.RowLevelTTL is not nil.
func newRowLevelTTLScheduledJob(
	env scheduledjobs.JobSchedulerEnv,
	owner username.SQLUsername,
	tblDesc *tabledesc.Mutable,
	clusterID uuid.UUID,
	clusterVersion clusterversion.ClusterVersion,
) (*jobs.ScheduledJob, error) {
	sj := jobs.NewScheduledJob(env)
	sj.SetScheduleLabel(ttlbase.BuildScheduleLabel(tblDesc))
	sj.SetOwner(owner)
	sj.SetScheduleDetails(jobspb.ScheduleDetails{
		Wait: jobspb.ScheduleDetails_SKIP,
		// If a job fails, try again at the allocated cron time.
		OnError:                jobspb.ScheduleDetails_RETRY_SCHED,
		ClusterID:              clusterID,
		CreationClusterVersion: clusterVersion,
	})

	if err := sj.SetScheduleAndNextRun(tblDesc.RowLevelTTL.DeletionCronOrDefault()); err != nil {
		return nil, err
	}
	args := &catpb.ScheduledRowLevelTTLArgs{
		TableID: tblDesc.GetID(),
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

// CreateRowLevelTTLScheduledJob creates a new row-level TTL schedule.
func CreateRowLevelTTLScheduledJob(
	ctx context.Context,
	knobs *jobs.TestingKnobs,
	s jobs.ScheduledJobStorage,
	owner username.SQLUsername,
	tblDesc *tabledesc.Mutable,
	clusterID uuid.UUID,
	version clusterversion.ClusterVersion,
) (*jobs.ScheduledJob, error) {
	if !tblDesc.HasRowLevelTTL() {
		return nil, errors.AssertionFailedf("CreateRowLevelTTLScheduledJob called with no .RowLevelTTL: %#v", tblDesc)
	}

	telemetry.Inc(sqltelemetry.RowLevelTTLCreated)
	env := JobSchedulerEnv(knobs)
	j, err := newRowLevelTTLScheduledJob(env, owner, tblDesc, clusterID, version)
	if err != nil {
		return nil, err
	}
	if err := s.Create(ctx, j); err != nil {
		return nil, err
	}
	return j, nil
}

func rowLevelTTLAutomaticColumnDef(ttl *catpb.RowLevelTTL) (*tree.ColumnTableDef, error) {
	def := &tree.ColumnTableDef{
		Name:   catpb.TTLDefaultExpirationColumnName,
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
		// TODO(#82672): We shouldn't need this. This is only still required for
		// the REGIONAL BY ROW column.
		shouldCopyColumnDefaultSet := make(map[string]struct{})
		if opts.Has(tree.LikeTableOptIndexes) {
			for _, idx := range td.NonDropIndexes() {
				for i := 0; i < idx.ExplicitColumnStartIdx(); i++ {
					for i := 0; i < idx.NumKeyColumns(); i++ {
						shouldCopyColumnDefaultSet[idx.GetKeyColumnName(i)] = struct{}{}
					}
				}
			}
		}

		defs := make(tree.TableDefs, 0)
		// Add user-defined columns.
		for i := range td.Columns {
			c := &td.Columns[i]
			implicit, err := isImplicitlyCreatedBySystem(td, c)
			if err != nil {
				return nil, err
			}
			if implicit {
				// Don't add system-created implicit columns.
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
					Name:                  tree.Name(c.Name),
					FromHashShardedColumn: c.FromHashShardedColumn,
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
				colNames, err := catalog.ColumnNamesForIDs(td, c.ColumnIDs)
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
				if idx.Primary() && td.IsPrimaryIndexDefaultRowID() {
					// We won't copy over the default rowid primary index; instead
					// we'll just generate a new one.
					continue
				}
				indexDef := tree.IndexTableDef{
					Name:         tree.Name(idx.GetName()),
					Type:         idx.GetType(),
					Storing:      make(tree.NameList, 0, idx.NumSecondaryStoredColumns()),
					Columns:      make(tree.IndexElemList, 0, idx.NumKeyColumns()),
					Invisibility: tree.IndexInvisibility{Value: idx.GetInvisibility()},
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
					col, err := catalog.MustFindColumnByID(td, idx.GetKeyColumnID(j))
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
					if idx.GetKeyColumnDirection(j) == catenumpb.IndexColumn_DESC {
						elem.Direction = tree.Descending
					}
					indexDef.Columns = append(indexDef.Columns, elem)
				}
				// The last column of an inverted or vector index cannot have an
				// explicit direction, because it does not have a linear ordering.
				if !indexDef.Type.HasLinearOrdering() {
					indexDef.Columns[len(indexDef.Columns)-1].Direction = tree.DefaultDirection
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
		Type:     types.Int,
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
		FromHashShardedColumn: true,
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

// setSequenceOwner adds sequence id to the sequence id list owned by a column
// and set ownership values of sequence options.
func setSequenceOwner(
	seqDesc *tabledesc.Mutable, colName tree.Name, table *tabledesc.Mutable,
) error {
	if !seqDesc.IsSequence() {
		return errors.Errorf("%s is not a sequence", seqDesc.Name)
	}

	col, err := catalog.MustFindColumnByTreeName(table, colName)
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

// Checks if the column was automatically added by the system (e.g. for a rowid
// primary key or hash sharded index).
func isImplicitlyCreatedBySystem(td *tabledesc.Mutable, c *descpb.ColumnDescriptor) (bool, error) {
	// TODO(#82672): add check for REGIONAL BY ROW column
	if td.IsPrimaryIndexDefaultRowID() && c.ID == td.GetPrimaryIndex().GetKeyColumnID(0) {
		return true, nil
	}
	col, err := catalog.MustFindColumnByID(td, c.ID)
	if err != nil {
		return false, err
	}
	if td.IsShardColumn(col) {
		return true, nil
	}
	if c.Inaccessible {
		return true, nil
	}
	return false, nil
}
