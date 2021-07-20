// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

const (
	// We need to choose arbitrary database and table IDs. These aren't important,
	// but they do match what would happen when creating a new database and
	// table on an empty cluster.
	defaultCSVParentID descpb.ID = keys.MinNonPredefinedUserDescID
	defaultCSVTableID  descpb.ID = defaultCSVParentID + 1
)

func readCreateTableFromStore(
	ctx context.Context,
	filename string,
	externalStorageFromURI cloud.ExternalStorageFromURIFactory,
	user security.SQLUsername,
) (*tree.CreateTable, error) {
	store, err := externalStorageFromURI(ctx, filename, user)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	reader, err := store.ReadFile(ctx, "")
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	tableDefStr, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	stmts, err := parser.Parse(string(tableDefStr))
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, errors.Errorf("expected 1 create table statement, found %d", len(stmts))
	}
	create, ok := stmts[0].AST.(*tree.CreateTable)
	if !ok {
		return nil, errors.New("expected CREATE TABLE statement in table file")
	}
	return create, nil
}

type fkHandler struct {
	allowed  bool
	skip     bool
	resolver fkResolver
}

// NoFKs is used by formats that do not support FKs.
var NoFKs = fkHandler{resolver: fkResolver{
	tableNameToDesc: make(map[string]*tabledesc.Mutable),
}}

// MakeSimpleTableDescriptor creates a Mutable from a CreateTable parse
// node without the full machinery. Many parts of the syntax are unsupported
// (see the implementation and TestMakeSimpleTableDescriptorErrors for details),
// but this is enough for our csv IMPORT and for some unit tests.
//
// Any occurrence of SERIAL in the column definitions is handled using
// the CockroachDB legacy behavior, i.e. INT NOT NULL DEFAULT
// unique_rowid().
func MakeSimpleTableDescriptor(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	st *cluster.Settings,
	create *tree.CreateTable,
	parentID, parentSchemaID, tableID descpb.ID,
	fks fkHandler,
	walltime int64,
) (*tabledesc.Mutable, error) {
	create.HoistConstraints()
	if create.IfNotExists {
		return nil, unimplemented.NewWithIssueDetailf(42846, "import.if-no-exists", "unsupported IF NOT EXISTS")
	}
	if create.Interleave != nil {
		return nil, unimplemented.NewWithIssueDetailf(42846, "import.interleave", "interleaved not supported")
	}
	if create.AsSource != nil {
		return nil, unimplemented.NewWithIssueDetailf(42846, "import.create-as", "CREATE AS not supported")
	}

	filteredDefs := create.Defs[:0]
	for i := range create.Defs {
		switch def := create.Defs[i].(type) {
		case *tree.CheckConstraintTableDef,
			*tree.FamilyTableDef,
			*tree.IndexTableDef,
			*tree.UniqueConstraintTableDef:
			// ignore
		case *tree.ColumnTableDef:
			if err := sql.SimplifySerialInColumnDefWithRowID(ctx, def, &create.Table); err != nil {
				return nil, err
			}

		case *tree.ForeignKeyConstraintTableDef:
			if !fks.allowed {
				return nil, unimplemented.NewWithIssueDetailf(42846, "import.fk",
					"this IMPORT format does not support foreign keys")
			}
			if fks.skip {
				continue
			}
			// Strip the schema/db prefix.
			def.Table = tree.MakeUnqualifiedTableName(def.Table.ObjectName)

		default:
			return nil, unimplemented.Newf(fmt.Sprintf("import.%T", def), "unsupported table definition: %s", tree.AsString(def))
		}
		// only append this def after we make it past the error checks and continues
		filteredDefs = append(filteredDefs, create.Defs[i])
	}
	create.Defs = filteredDefs

	evalCtx := tree.EvalContext{
		Context:     ctx,
		Sequence:    &importSequenceOperators{},
		SessionData: &sessiondata.SessionData{},
	}
	affected := make(map[descpb.ID]*tabledesc.Mutable)

	tableDesc, err := sql.NewTableDesc(
		ctx,
		nil, /* txn */
		&fks.resolver,
		st,
		create,
		parentID,
		parentSchemaID,
		tableID,
		nil, /* regionConfig */
		hlc.Timestamp{WallTime: walltime},
		descpb.NewDefaultPrivilegeDescriptor(security.AdminRoleName()),
		affected,
		semaCtx,
		&evalCtx,
		&sessiondata.SessionData{}, /* sessionData */
		tree.PersistencePermanent,
		// We need to bypass the LOCALITY on non multi-region check here because
		// we cannot access the database region config at import level.
		// There is code that only allows REGIONAL BY TABLE tables to be imported,
		// which will safely execute even if the locality check is bypassed.
		sql.NewTableDescOptionBypassLocalityOnNonMultiRegionDatabaseCheck(),
	)
	if err != nil {
		return nil, err
	}
	if err := fixDescriptorFKState(tableDesc); err != nil {
		return nil, err
	}

	return tableDesc, nil
}

// fixDescriptorFKState repairs validity and table states set during descriptor
// creation. sql.NewTableDesc and ResolveFK set the table to the ADD state
// and mark references an validated. This function sets the table to PUBLIC
// and the FKs to unvalidated.
func fixDescriptorFKState(tableDesc *tabledesc.Mutable) error {
	tableDesc.SetPublic()
	for i := range tableDesc.OutboundFKs {
		tableDesc.OutboundFKs[i].Validity = descpb.ConstraintValidity_Unvalidated
	}
	return nil
}

var (
	errSequenceOperators = errors.New("sequence operations unsupported")
	errSchemaResolver    = errors.New("schema resolver unsupported")
)

// Implements the tree.SequenceOperators interface.
type importSequenceOperators struct {
}

// GetSerialSequenceNameFromColumn is part of the tree.SequenceOperators interface.
func (so *importSequenceOperators) GetSerialSequenceNameFromColumn(
	ctx context.Context, tn *tree.TableName, columnName tree.Name,
) (*tree.TableName, error) {
	return nil, errors.WithStack(errSequenceOperators)
}

// CurrentDatabaseRegionConfig is part of the tree.EvalDatabase interface.
func (so *importSequenceOperators) CurrentDatabaseRegionConfig(
	_ context.Context,
) (tree.DatabaseRegionConfig, error) {
	return nil, errors.WithStack(errSequenceOperators)
}

// ValidateAllMultiRegionZoneConfigsInCurrentDatabase is part of the tree.EvalDatabase interface.
func (so *importSequenceOperators) ValidateAllMultiRegionZoneConfigsInCurrentDatabase(
	_ context.Context,
) error {
	return errors.WithStack(errSequenceOperators)
}

// Implements the tree.EvalDatabase interface.
func (so *importSequenceOperators) ParseQualifiedTableName(sql string) (*tree.TableName, error) {
	name, err := parser.ParseTableName(sql)
	if err != nil {
		return nil, err
	}
	tn := name.ToTableName()
	return &tn, nil
}

// Implements the tree.EvalDatabase interface.
func (so *importSequenceOperators) ResolveTableName(
	ctx context.Context, tn *tree.TableName,
) (tree.ID, error) {
	return 0, errSequenceOperators
}

// Implements the tree.EvalDatabase interface.
func (so *importSequenceOperators) LookupSchema(
	ctx context.Context, dbName, scName string,
) (bool, tree.SchemaMeta, error) {
	return false, nil, errSequenceOperators
}

// IsTableVisible is part of the tree.EvalDatabase interface.
func (so *importSequenceOperators) IsTableVisible(
	ctx context.Context, curDB string, searchPath sessiondata.SearchPath, tableID oid.Oid,
) (bool, bool, error) {
	return false, false, errors.WithStack(errSequenceOperators)
}

// IsTypeVisible is part of the tree.EvalDatabase interface.
func (so *importSequenceOperators) IsTypeVisible(
	ctx context.Context, curDB string, searchPath sessiondata.SearchPath, typeID oid.Oid,
) (bool, bool, error) {
	return false, false, errors.WithStack(errSequenceOperators)
}

// Implements the tree.SequenceOperators interface.
func (so *importSequenceOperators) IncrementSequence(
	ctx context.Context, seqName *tree.TableName,
) (int64, error) {
	return 0, errSequenceOperators
}

// Implements the tree.SequenceOperators interface.
func (so *importSequenceOperators) IncrementSequenceByID(
	ctx context.Context, seqID int64,
) (int64, error) {
	return 0, errSequenceOperators
}

// Implements the tree.SequenceOperators interface.
func (so *importSequenceOperators) GetLatestValueInSessionForSequence(
	ctx context.Context, seqName *tree.TableName,
) (int64, error) {
	return 0, errSequenceOperators
}

// Implements the tree.SequenceOperators interface.
func (so *importSequenceOperators) GetLatestValueInSessionForSequenceByID(
	ctx context.Context, seqID int64,
) (int64, error) {
	return 0, errSequenceOperators
}

// Implements the tree.SequenceOperators interface.
func (so *importSequenceOperators) SetSequenceValue(
	ctx context.Context, seqName *tree.TableName, newVal int64, isCalled bool,
) error {
	return errSequenceOperators
}

// Implements the tree.SequenceOperators interface.
func (so *importSequenceOperators) SetSequenceValueByID(
	ctx context.Context, seqID int64, newVal int64, isCalled bool,
) error {
	return errSequenceOperators
}

type fkResolver struct {
	tableNameToDesc map[string]*tabledesc.Mutable
	format          roachpb.IOFileFormat
}

var _ resolver.SchemaResolver = &fkResolver{}

// Implements the sql.SchemaResolver interface.
func (r *fkResolver) Txn() *kv.Txn {
	return nil
}

// Implements the sql.SchemaResolver interface.
func (r *fkResolver) Accessor() catalog.Accessor {
	return nil
}

// Implements the sql.SchemaResolver interface.
func (r *fkResolver) CurrentDatabase() string {
	return ""
}

// Implements the sql.SchemaResolver interface.
func (r *fkResolver) CurrentSearchPath() sessiondata.SearchPath {
	return sessiondata.SearchPath{}
}

// Implements the sql.SchemaResolver interface.
func (r *fkResolver) CommonLookupFlags(required bool) tree.CommonLookupFlags {
	return tree.CommonLookupFlags{}
}

// Implements the sql.SchemaResolver interface.
func (r *fkResolver) ObjectLookupFlags(required bool, requireMutable bool) tree.ObjectLookupFlags {
	return tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{Required: required, RequireMutable: requireMutable},
	}
}

// Implements the tree.ObjectNameExistingResolver interface.
func (r *fkResolver) LookupObject(
	_ context.Context, _ tree.ObjectLookupFlags, catalogName, scName, obName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	// PGDUMP supports non-public schemas so respect the schema name.
	var lookupName string
	if r.format.Format == roachpb.IOFileFormat_PgDump {
		if scName == "" || catalogName == "" {
			return false, nil, errors.Errorf("expected catalog and schema name to be set when resolving"+
				" table %q in PGDUMP", obName)
		}
		lookupName = fmt.Sprintf("%s.%s", scName, obName)
	} else {
		if scName != "" {
			lookupName = strings.TrimPrefix(obName, scName+".")
		}
	}
	tbl, ok := r.tableNameToDesc[lookupName]
	if ok {
		return true, tbl, nil
	}
	names := make([]string, 0, len(r.tableNameToDesc))
	for k := range r.tableNameToDesc {
		names = append(names, k)
	}
	suggestions := strings.Join(names, ",")
	return false, nil, errors.Errorf("referenced table %q not found in tables being imported (%s)",
		lookupName, suggestions)
}

// Implements the tree.ObjectNameTargetResolver interface.
func (r fkResolver) LookupSchema(
	ctx context.Context, dbName, scName string,
) (found bool, scMeta tree.SchemaMeta, err error) {
	return false, nil, errSchemaResolver
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) LookupTableByID(
	ctx context.Context, id descpb.ID,
) (catalog.TableDescriptor, error) {
	return nil, errSchemaResolver
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) ResolveTypeByOID(ctx context.Context, oid oid.Oid) (*types.T, error) {
	return nil, errSchemaResolver
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	return nil, errSchemaResolver
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) GetQualifiedTableNameByID(
	ctx context.Context, id int64, requiredType tree.RequiredTableKind,
) (*tree.TableName, error) {
	return nil, errSchemaResolver
}
