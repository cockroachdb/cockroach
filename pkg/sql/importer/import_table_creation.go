// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/faketreeeval"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// MakeTestingSimpleTableDescriptor creates a tabledesc.Mutable from a
// CreateTable parse node without the full machinery. Many parts of the syntax
// are unsupported (see the implementation and
// TestMakeSimpleTableDescriptorErrors for details), but this is enough for some
// unit tests.
//
// Any occurrence of SERIAL in the column definitions is handled using
// the CockroachDB legacy behavior, i.e. INT NOT NULL DEFAULT
// unique_rowid().
//
// TODO(yuzefovich): move this out of importer package into some test utils
// package.
func MakeTestingSimpleTableDescriptor(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	st *cluster.Settings,
	create *tree.CreateTable,
	parentID, parentSchemaID, tableID descpb.ID,
	walltime int64,
) (*tabledesc.Mutable, error) {
	db := dbdesc.NewInitial(parentID, "foo", username.RootUserName())
	sc := schemadesc.NewBuilder(&descpb.SchemaDescriptor{
		Name:     "foo",
		ID:       parentSchemaID,
		Version:  1,
		ParentID: parentID,
		Privileges: catpb.NewPrivilegeDescriptor(
			username.PublicRoleName(),
			privilege.SchemaPrivileges,
			privilege.List{},
			username.RootUserName(),
		),
	}).BuildCreatedMutableSchema()
	create.HoistConstraints()
	if create.IfNotExists {
		return nil, errors.Newf("IF NOT EXISTS is not supported")
	}
	if create.AsSource != nil {
		return nil, errors.Newf("CREATE AS is not supported")
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
					return nil, errors.Newf("expression indexes are not supported")
				}
			}
		case *tree.ColumnTableDef:
			if def.IsComputed() && def.IsVirtual() {
				return nil, errors.Newf("virtual computed columns are not supported")
			}

			if err := sql.SimplifySerialInColumnDefWithRowID(ctx, def, &create.Table); err != nil {
				return nil, err
			}
		case *tree.ForeignKeyConstraintTableDef:
			return nil, errors.Newf("foreign keys are not supported")
		default:
			return nil, errors.Newf("unsupported table definition: %s", tree.AsString(def))
		}
		// only append this def after we make it past the error checks and continues
		filteredDefs = append(filteredDefs, create.Defs[i])
	}
	create.Defs = filteredDefs

	evalCtx := eval.Context{
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
		nil, /* vt */
		st,
		create,
		db,
		sc,
		tableID,
		nil, /* regionConfig */
		hlc.Timestamp{WallTime: walltime},
		catpb.NewBasePrivilegeDescriptor(username.AdminRoleName()),
		affected,
		semaCtx,
		&evalCtx,
		evalCtx.SessionData(), /* sessionData */
		tree.PersistencePermanent,
		// Sequences are unsupported here.
		nil, /* colToSequenceRefs */
		// We need to bypass the LOCALITY on non multi-region check here because
		// we cannot access the database region config at import level.
		// There is code that only allows REGIONAL BY TABLE tables to be imported,
		// which will safely execute even if the locality check is bypassed.
		sql.NewTableDescOptionBypassLocalityOnNonMultiRegionDatabaseCheck(),
	)
	if err != nil {
		return nil, err
	}
	tableDesc.SetPublic()

	return tableDesc, nil
}

var (
	errSequenceOperators = errors.New("sequence operations unsupported")
	errRegionOperator    = errors.New("region operations unsupported")
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

var _ eval.DatabaseRegionConfig = &importDatabaseRegionConfig{}

// CurrentDatabaseRegionConfig is part of the eval.RegionOperator interface.
func (so *importRegionOperator) CurrentDatabaseRegionConfig(
	_ context.Context,
) (eval.DatabaseRegionConfig, error) {
	return importDatabaseRegionConfig{primaryRegion: so.primaryRegion}, nil
}

// ValidateAllMultiRegionZoneConfigsInCurrentDatabase is part of the eval.RegionOperator interface.
func (so *importRegionOperator) ValidateAllMultiRegionZoneConfigsInCurrentDatabase(
	_ context.Context,
) error {
	return errors.WithStack(errRegionOperator)
}

// ResetMultiRegionZoneConfigsForTable is part of the eval.RegionOperator
// interface.
func (so *importRegionOperator) ResetMultiRegionZoneConfigsForTable(
	_ context.Context, _ int64, _ bool,
) error {
	return errors.WithStack(errRegionOperator)
}

// ResetMultiRegionZoneConfigsForDatabase is part of the eval.RegionOperator
// interface.
func (so *importRegionOperator) ResetMultiRegionZoneConfigsForDatabase(
	_ context.Context, _ int64,
) error {
	return errors.WithStack(errRegionOperator)
}

// Implements the eval.SequenceOperators interface.
type importSequenceOperators struct{}

// GetSerialSequenceNameFromColumn is part of the eval.SequenceOperators interface.
func (so *importSequenceOperators) GetSerialSequenceNameFromColumn(
	ctx context.Context, tn *tree.TableName, columnName tree.Name,
) (*tree.TableName, error) {
	return nil, errors.WithStack(errSequenceOperators)
}

// ResolveTableName implements the eval.DatabaseCatalog interface.
func (so *importSequenceOperators) ResolveTableName(
	ctx context.Context, tn *tree.TableName,
) (tree.ID, error) {
	return 0, errSequenceOperators
}

// SchemaExists implements the eval.DatabaseCatalog interface.
func (so *importSequenceOperators) SchemaExists(
	ctx context.Context, dbName, scName string,
) (bool, error) {
	return false, errSequenceOperators
}

// HasAnyPrivilegeForSpecifier is part of the eval.DatabaseCatalog interface.
func (so *importSequenceOperators) HasAnyPrivilegeForSpecifier(
	ctx context.Context,
	specifier eval.HasPrivilegeSpecifier,
	user username.SQLUsername,
	privs []privilege.Privilege,
) (eval.HasAnyPrivilegeResult, error) {
	return eval.HasNoPrivilege, errors.WithStack(errSequenceOperators)
}

// IncrementSequenceByID implements the eval.SequenceOperators interface.
func (so *importSequenceOperators) IncrementSequenceByID(
	ctx context.Context, seqID int64,
) (int64, error) {
	return 0, errSequenceOperators
}

// GetLatestValueInSessionForSequenceByID implements the eval.SequenceOperators interface.
func (so *importSequenceOperators) GetLatestValueInSessionForSequenceByID(
	ctx context.Context, seqID int64,
) (int64, error) {
	return 0, errSequenceOperators
}

// GetLastSequenceValueByID implements the eval.SequenceOperators interface.
func (so *importSequenceOperators) GetLastSequenceValueByID(
	ctx context.Context, seqID uint32,
) (int64, bool, error) {
	return 0, false, errSequenceOperators
}

// SetSequenceValueByID implements the eval.SequenceOperators interface.
func (so *importSequenceOperators) SetSequenceValueByID(
	ctx context.Context, seqID uint32, newVal int64, isCalled bool,
) error {
	return errSequenceOperators
}
