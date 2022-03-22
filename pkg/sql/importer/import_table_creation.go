// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importer

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/faketreeeval"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

type fkHandler struct {
	allowed  bool
	skip     bool
	resolver fkResolver
}

// NoFKs is used by formats that do not support FKs.
var NoFKs = fkHandler{resolver: fkResolver{
	tableNameToDesc: make(map[string]*tabledesc.Mutable),
}}

// MakeTestingSimpleTableDescriptor is like MakeSimpleTableDescriptor but it
// uses parentID and parentSchemaID instead of descriptors.
func MakeTestingSimpleTableDescriptor(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	st *cluster.Settings,
	create *tree.CreateTable,
	parentID, parentSchemaID, tableID descpb.ID,
	fks fkHandler,
	walltime int64,
) (*tabledesc.Mutable, error) {
	db := dbdesc.NewInitial(parentID, "foo", security.RootUserName())
	var sc catalog.SchemaDescriptor
	if !st.Version.IsActive(ctx, clusterversion.PublicSchemasWithDescriptors) && parentSchemaID == keys.PublicSchemaIDForBackup {
		// If we're not on version PublicSchemasWithDescriptors, continue to
		// use a synthetic public schema, the migration when we update to
		// PublicSchemasWithDescriptors will handle creating the explicit public
		// schema.
		sc = schemadesc.GetPublicSchema()
	} else {
		sc = schemadesc.NewBuilder(&descpb.SchemaDescriptor{
			Name:     "foo",
			ID:       parentSchemaID,
			Version:  1,
			ParentID: parentID,
			Privileges: catpb.NewPrivilegeDescriptor(
				security.PublicRoleName(),
				privilege.SchemaPrivileges,
				privilege.List{},
				security.RootUserName(),
			),
		}).BuildCreatedMutableSchema()
	}
	return MakeSimpleTableDescriptor(ctx, semaCtx, st, create, db, sc, tableID, fks, walltime)
}

func makeSemaCtxWithoutTypeResolver(semaCtx *tree.SemaContext) *tree.SemaContext {
	semaCtxCopy := *semaCtx
	semaCtxCopy.TypeResolver = nil
	return &semaCtxCopy
}

// MakeSimpleTableDescriptor creates a tabledesc.Mutable from a CreateTable
// parse node without the full machinery. Many parts of the syntax are
// unsupported (see the implementation and TestMakeSimpleTableDescriptorErrors
// for details), but this is enough for our csv IMPORT and for some unit tests.
//
// Any occurrence of SERIAL in the column definitions is handled using
// the CockroachDB legacy behavior, i.e. INT NOT NULL DEFAULT
// unique_rowid().
func MakeSimpleTableDescriptor(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	st *cluster.Settings,
	create *tree.CreateTable,
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
	tableID descpb.ID,
	fks fkHandler,
	walltime int64,
) (*tabledesc.Mutable, error) {
	create.HoistConstraints()
	if create.IfNotExists {
		return nil, unimplemented.NewWithIssueDetailf(42846, "import.if-no-exists", "unsupported IF NOT EXISTS")
	}
	if create.AsSource != nil {
		return nil, unimplemented.NewWithIssueDetailf(42846, "import.create-as", "CREATE AS not supported")
	}

	filteredDefs := create.Defs[:0]
	for i := range create.Defs {
		switch def := create.Defs[i].(type) {
		case *tree.CheckConstraintTableDef,
			*tree.FamilyTableDef,
			*tree.UniqueConstraintTableDef:
			// ignore
		case *tree.IndexTableDef:
			for i := range def.Columns {
				if def.Columns[i].Expr != nil {
					return nil, unimplemented.NewWithIssueDetail(56002, "import.expression-index",
						"to import into a table with expression indexes, use IMPORT INTO")
				}
			}

		case *tree.ColumnTableDef:
			if def.IsComputed() && def.IsVirtual() {
				return nil, unimplemented.NewWithIssueDetail(56002, "import.computed",
					"to import into a table with virtual computed columns, use IMPORT INTO")
			}

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
		Context:            ctx,
		Sequence:           &importSequenceOperators{},
		Regions:            makeImportRegionOperator(""),
		SessionDataStack:   sessiondata.NewStack(&sessiondata.SessionData{}),
		ClientNoticeSender: &faketreeeval.DummyClientNoticeSender{},
		TxnTimestamp:       timeutil.Unix(0, walltime),
		Settings:           st,
	}
	affected := make(map[descpb.ID]*tabledesc.Mutable)

	tableDesc, err := sql.NewTableDesc(
		ctx,
		nil, /* txn */
		&fks.resolver,
		st,
		create,
		db,
		sc,
		tableID,
		nil, /* regionConfig */
		hlc.Timestamp{WallTime: walltime},
		catpb.NewBasePrivilegeDescriptor(security.AdminRoleName()),
		affected,
		semaCtx,
		&evalCtx,
		evalCtx.SessionData(), /* sessionData */
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
	errRegionOperator    = errors.New("region operations unsupported")
	errSchemaResolver    = errors.New("schema resolver unsupported")
)

// Implements the tree.RegionOperator interface.
type importRegionOperator struct {
	primaryRegion catpb.RegionName
}

func makeImportRegionOperator(primaryRegion catpb.RegionName) *importRegionOperator {
	return &importRegionOperator{primaryRegion: primaryRegion}
}

// importDatabaseRegionConfig is a stripped down version of
// multiregion.RegionConfig that is used by import.
type importDatabaseRegionConfig struct {
	primaryRegion catpb.RegionName
}

// IsValidRegionNameString implements the tree.DatabaseRegionConfig interface.
func (i importDatabaseRegionConfig) IsValidRegionNameString(_ string) bool {
	// Unimplemented.
	return false
}

// PrimaryRegionString implements the tree.DatabaseRegionConfig interface.
func (i importDatabaseRegionConfig) PrimaryRegionString() string {
	return string(i.primaryRegion)
}

var _ tree.DatabaseRegionConfig = &importDatabaseRegionConfig{}

// CurrentDatabaseRegionConfig is part of the tree.EvalDatabase interface.
func (so *importRegionOperator) CurrentDatabaseRegionConfig(
	_ context.Context,
) (tree.DatabaseRegionConfig, error) {
	return importDatabaseRegionConfig{primaryRegion: so.primaryRegion}, nil
}

// ValidateAllMultiRegionZoneConfigsInCurrentDatabase is part of the tree.EvalDatabase interface.
func (so *importRegionOperator) ValidateAllMultiRegionZoneConfigsInCurrentDatabase(
	_ context.Context,
) error {
	return errors.WithStack(errRegionOperator)
}

// ResetMultiRegionZoneConfigsForTable is part of the tree.EvalDatabase
// interface.
func (so *importRegionOperator) ResetMultiRegionZoneConfigsForTable(
	_ context.Context, _ int64,
) error {
	return errors.WithStack(errRegionOperator)
}

// ResetMultiRegionZoneConfigsForDatabase is part of the tree.EvalDatabase
// interface.
func (so *importRegionOperator) ResetMultiRegionZoneConfigsForDatabase(
	_ context.Context, _ int64,
) error {
	return errors.WithStack(errRegionOperator)
}

// Implements the tree.SequenceOperators interface.
type importSequenceOperators struct{}

// GetSerialSequenceNameFromColumn is part of the tree.SequenceOperators interface.
func (so *importSequenceOperators) GetSerialSequenceNameFromColumn(
	ctx context.Context, tn *tree.TableName, columnName tree.Name,
) (*tree.TableName, error) {
	return nil, errors.WithStack(errSequenceOperators)
}

// ParseQualifiedTableName implements the tree.EvalDatabase interface.
func (so *importSequenceOperators) ParseQualifiedTableName(sql string) (*tree.TableName, error) {
	name, err := parser.ParseTableName(sql)
	if err != nil {
		return nil, err
	}
	tn := name.ToTableName()
	return &tn, nil
}

// ResolveTableName implements the tree.EvalDatabase interface.
func (so *importSequenceOperators) ResolveTableName(
	ctx context.Context, tn *tree.TableName,
) (tree.ID, error) {
	return 0, errSequenceOperators
}

// SchemaExists implements the tree.EvalDatabase interface.
func (so *importSequenceOperators) SchemaExists(
	ctx context.Context, dbName, scName string,
) (bool, error) {
	return false, errSequenceOperators
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

// HasAnyPrivilege is part of the tree.EvalDatabase interface.
func (so *importSequenceOperators) HasAnyPrivilege(
	ctx context.Context,
	specifier tree.HasPrivilegeSpecifier,
	user security.SQLUsername,
	privs []privilege.Privilege,
) (tree.HasAnyPrivilegeResult, error) {
	return tree.HasNoPrivilege, errors.WithStack(errSequenceOperators)
}

// IncrementSequenceByID implements the tree.SequenceOperators interface.
func (so *importSequenceOperators) IncrementSequenceByID(
	ctx context.Context, seqID int64,
) (int64, error) {
	return 0, errSequenceOperators
}

// GetLatestValueInSessionForSequenceByID implements the tree.SequenceOperators interface.
func (so *importSequenceOperators) GetLatestValueInSessionForSequenceByID(
	ctx context.Context, seqID int64,
) (int64, error) {
	return 0, errSequenceOperators
}

// SetSequenceValueByID implements the tree.SequenceOperators interface.
func (so *importSequenceOperators) SetSequenceValueByID(
	ctx context.Context, seqID uint32, newVal int64, isCalled bool,
) error {
	return errSequenceOperators
}

type fkResolver struct {
	tableNameToDesc map[string]*tabledesc.Mutable
	format          roachpb.IOFileFormat
}

var _ resolver.SchemaResolver = &fkResolver{}

// Accessor implements the resolver.SchemaResolver interface.
func (r *fkResolver) Accessor() catalog.Accessor {
	return nil
}

// CurrentDatabase implements the resolver.SchemaResolver interface.
func (r *fkResolver) CurrentDatabase() string {
	return ""
}

// CurrentSearchPath implements the resolver.SchemaResolver interface.
func (r *fkResolver) CurrentSearchPath() sessiondata.SearchPath {
	return sessiondata.SearchPath{}
}

// CommonLookupFlags implements the resolver.SchemaResolver interface.
func (r *fkResolver) CommonLookupFlags(required bool) tree.CommonLookupFlags {
	return tree.CommonLookupFlags{}
}

// LookupObject implements the tree.ObjectNameExistingResolver interface.
func (r *fkResolver) LookupObject(
	ctx context.Context, flags tree.ObjectLookupFlags, dbName, scName, obName string,
) (found bool, prefix catalog.ResolvedObjectPrefix, objMeta catalog.Descriptor, err error) {
	// PGDUMP supports non-public schemas so respect the schema name.
	var lookupName string
	if r.format.Format == roachpb.IOFileFormat_PgDump {
		if scName == "" || dbName == "" {
			return false, prefix, nil, errors.Errorf("expected catalog and schema name to be set when resolving"+
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
		return true, prefix, tbl, nil
	}
	names := make([]string, 0, len(r.tableNameToDesc))
	for k := range r.tableNameToDesc {
		names = append(names, k)
	}
	suggestions := strings.Join(names, ",")
	return false, prefix, nil, errors.Errorf("referenced table %q not found in tables being imported (%s)",
		lookupName, suggestions)
}

// LookupSchema implements the resolver.ObjectNameTargetResolver interface.
func (r fkResolver) LookupSchema(
	ctx context.Context, dbName, scName string,
) (found bool, scMeta catalog.ResolvedObjectPrefix, err error) {
	return false, scMeta, errSchemaResolver
}

// ResolveTypeByOID implements the resolver.SchemaResolver interface.
func (r fkResolver) ResolveTypeByOID(ctx context.Context, oid oid.Oid) (*types.T, error) {
	return nil, errSchemaResolver
}

// ResolveType implements the resolver.SchemaResolver interface.
func (r fkResolver) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	return nil, errSchemaResolver
}

// GetQualifiedTableNameByID implements the resolver.SchemaResolver interface.
func (r fkResolver) GetQualifiedTableNameByID(
	ctx context.Context, id int64, requiredType tree.RequiredTableKind,
) (*tree.TableName, error) {
	return nil, errSchemaResolver
}
