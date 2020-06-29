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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

const (
	// We need to choose arbitrary database and table IDs. These aren't important,
	// but they do match what would happen when creating a new database and
	// table on an empty cluster.
	defaultCSVParentID sqlbase.ID = keys.MinNonPredefinedUserDescID
	defaultCSVTableID  sqlbase.ID = defaultCSVParentID + 1
)

func readCreateTableFromStore(
	ctx context.Context,
	filename string,
	externalStorageFromURI cloud.ExternalStorageFromURIFactory,
	user string,
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
var NoFKs = fkHandler{resolver: make(fkResolver)}

// MakeSimpleTableDescriptor creates a MutableTableDescriptor from a CreateTable parse
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
	parentID, tableID sqlbase.ID,
	fks fkHandler,
	walltime int64,
) (*sqlbase.MutableTableDescriptor, error) {
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
			if def.Computed.Expr != nil {
				return nil, unimplemented.NewWithIssueDetailf(42846, "import.computed",
					"computed columns not supported: %s", tree.AsString(def))
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
		Context:  ctx,
		Sequence: &importSequenceOperators{},
	}
	affected := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor)

	tableDesc, err := sql.MakeTableDesc(
		ctx,
		nil, /* txn */
		fks.resolver,
		st,
		create,
		parentID,
		keys.PublicSchemaID,
		tableID,
		hlc.Timestamp{WallTime: walltime},
		sqlbase.NewDefaultPrivilegeDescriptor(),
		affected,
		semaCtx,
		&evalCtx,
		&sessiondata.SessionData{}, /* sessionData */
		false,                      /* temporary */
	)
	if err != nil {
		return nil, err
	}
	if err := fixDescriptorFKState(tableDesc.TableDesc()); err != nil {
		return nil, err
	}

	return &tableDesc, nil
}

// fixDescriptorFKState repairs validity and table states set during descriptor
// creation. sql.MakeTableDesc and ResolveFK set the table to the ADD state
// and mark references an validated. This function sets the table to PUBLIC
// and the FKs to unvalidated.
func fixDescriptorFKState(tableDesc *sqlbase.TableDescriptor) error {
	tableDesc.State = sqlbase.TableDescriptor_PUBLIC
	for i := range tableDesc.OutboundFKs {
		tableDesc.OutboundFKs[i].Validity = sqlbase.ConstraintValidity_Unvalidated
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

// Implements the tree.SequenceOperators interface.
func (so *importSequenceOperators) IncrementSequence(
	ctx context.Context, seqName *tree.TableName,
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
func (so *importSequenceOperators) SetSequenceValue(
	ctx context.Context, seqName *tree.TableName, newVal int64, isCalled bool,
) error {
	return errSequenceOperators
}

type fkResolver map[string]*sqlbase.MutableTableDescriptor

var _ resolver.SchemaResolver = fkResolver{}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) Txn() *kv.Txn {
	return nil
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) LogicalSchemaAccessor() catalog.Accessor {
	return nil
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) CurrentDatabase() string {
	return ""
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) CurrentSearchPath() sessiondata.SearchPath {
	return sessiondata.SearchPath{}
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) CommonLookupFlags(required bool) tree.CommonLookupFlags {
	return tree.CommonLookupFlags{}
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) ObjectLookupFlags(required bool, requireMutable bool) tree.ObjectLookupFlags {
	return tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{Required: required},
		RequireMutable:    requireMutable,
	}
}

// Implements the tree.ObjectNameExistingResolver interface.
func (r fkResolver) LookupObject(
	ctx context.Context, lookupFlags tree.ObjectLookupFlags, dbName, scName, obName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	if scName != "" {
		obName = strings.TrimPrefix(obName, scName+".")
	}
	tbl, ok := r[obName]
	if ok {
		return true, tbl, nil
	}
	names := make([]string, 0, len(r))
	for k := range r {
		names = append(names, k)
	}
	suggestions := strings.Join(names, ",")
	return false, nil, errors.Errorf("referenced table %q not found in tables being imported (%s)", obName, suggestions)
}

// Implements the tree.ObjectNameTargetResolver interface.
func (r fkResolver) LookupSchema(
	ctx context.Context, dbName, scName string,
) (found bool, scMeta tree.SchemaMeta, err error) {
	return false, nil, errSchemaResolver
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) LookupTableByID(
	ctx context.Context, id sqlbase.ID,
) (catalog.TableEntry, error) {
	return catalog.TableEntry{}, errSchemaResolver
}
