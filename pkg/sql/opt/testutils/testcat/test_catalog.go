// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testcat

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

const (
	// testDB is the default current database for testing purposes.
	testDB = "t"
)

// Catalog implements the cat.Catalog interface for testing purposes.
type Catalog struct {
	testSchema       Schema
	counter          int
	dependencyDigest int64
	enumTypes        map[string]*types.T

	udfs           map[string]*tree.ResolvedFunctionDefinition
	revokedUDFOids intsets.Fast

	users       map[username.SQLUsername]roleMembership
	currentUser username.SQLUsername
}

type roleMembership struct {
	isMemberOfAdminRole bool
}

type dataSource interface {
	cat.DataSource
	fqName() cat.DataSourceName
}

var _ cat.Catalog = &Catalog{}

// New creates a new empty instance of the test catalog.
func New() *Catalog {
	users := make(map[username.SQLUsername]roleMembership)
	users[username.RootUserName()] = roleMembership{isMemberOfAdminRole: true}

	return &Catalog{
		testSchema: Schema{
			SchemaID: 1,
			SchemaName: cat.SchemaName{
				CatalogName:     testDB,
				SchemaName:      catconstants.PublicSchemaName,
				ExplicitSchema:  true,
				ExplicitCatalog: true,
			},
			dataSources: make(map[string]dataSource),
		},
		users:       users,
		currentUser: username.RootUserName(),
	}
}

func (tc *Catalog) LookupDatabaseName(
	_ context.Context, _ cat.Flags, name string,
) (tree.Name, error) {
	if name != testDB {
		return "", sqlerrors.NewUndefinedDatabaseError(name)
	}
	return tree.Name(name), nil
}

// ResolveSchema is part of the cat.Catalog interface.
func (tc *Catalog) ResolveSchema(
	_ context.Context, _ cat.Flags, name *cat.SchemaName,
) (cat.Schema, cat.SchemaName, error) {
	// This is a simplified version of tree.TableName.ResolveTarget() from
	// sql/tree/name_resolution.go.
	toResolve := *name
	if name.ExplicitSchema {
		if name.ExplicitCatalog {
			// Already 2 parts: nothing to do.
			return tc.resolveSchema(&toResolve)
		}

		// Only one part specified; assume it's a schema name and determine
		// whether the current database has that schema.
		toResolve.CatalogName = testDB
		if sch, resName, err := tc.resolveSchema(&toResolve); err == nil {
			return sch, resName, nil
		}

		// No luck so far. Compatibility with CockroachDB v1.1: use D.public
		// instead.
		toResolve.CatalogName = name.SchemaName
		toResolve.SchemaName = catconstants.PublicSchemaName
		toResolve.ExplicitCatalog = true
		return tc.resolveSchema(&toResolve)
	}

	// Neither schema or catalog was specified, so use t.public.
	toResolve.CatalogName = tree.Name(testDB)
	toResolve.SchemaName = catconstants.PublicSchemaName
	return tc.resolveSchema(&toResolve)
}

// GetAllSchemaNamesForDB is part of the cat.Catalog interface.
func (tc *Catalog) GetAllSchemaNamesForDB(
	ctx context.Context, dbName string,
) ([]cat.SchemaName, error) {
	var schemaNames []cat.SchemaName
	var scName cat.SchemaName
	scName.SchemaName = catconstants.PublicSchemaName
	scName.ExplicitSchema = true
	scName.CatalogName = tree.Name(dbName)
	scName.ExplicitCatalog = true
	schemaNames = append(schemaNames, scName)

	return schemaNames, nil
}

// ResolveDataSource is part of the cat.Catalog interface.
func (tc *Catalog) ResolveDataSource(
	_ context.Context, _ cat.Flags, name *cat.DataSourceName,
) (cat.DataSource, cat.DataSourceName, error) {
	// This is a simplified version of tree.TableName.ResolveExisting() from
	// sql/tree/name_resolution.go.
	var ds cat.DataSource
	var err error
	toResolve := *name
	if name.ExplicitSchema && name.ExplicitCatalog {
		// Already 3 parts.
		ds, err = tc.resolveDataSource(&toResolve)
		if err == nil {
			return ds, toResolve, nil
		}
	} else if name.ExplicitSchema {
		// Two parts: Try to use the current database, and be satisfied if it's
		// sufficient to find the object.
		toResolve.CatalogName = testDB
		if tab, err := tc.resolveDataSource(&toResolve); err == nil {
			return tab, toResolve, nil
		}

		// No luck so far. Compatibility with CockroachDB v1.1: try D.public.T
		// instead.
		toResolve.CatalogName = name.SchemaName
		toResolve.SchemaName = catconstants.PublicSchemaName
		toResolve.ExplicitCatalog = true
		ds, err = tc.resolveDataSource(&toResolve)
		if err == nil {
			return ds, toResolve, nil
		}
	} else {
		// This is a naked data source name. Use the current database.
		toResolve.CatalogName = tree.Name(testDB)
		toResolve.SchemaName = catconstants.PublicSchemaName
		ds, err = tc.resolveDataSource(&toResolve)
		if err == nil {
			return ds, toResolve, nil
		}
	}

	// If we didn't find the table in the catalog, try to lazily resolve it as
	// a virtual table.
	if table, ok := resolveVTable(name); ok {
		// We rely on the check in CreateTable against this table's schema to infer
		// that this is a virtual table.
		return tc.CreateTable(table), *name, nil
	}
	if view, ok := resolveVirtualView(name); ok {
		// We rely on the check in CreateView against this table's schema to infer
		// that this is a virtual table.
		return tc.CreateView(view), *name, nil
	}

	// If this didn't end up being a virtual table, then return the original
	// error returned by resolveDataSource.
	return nil, cat.DataSourceName{}, err
}

// ResolveDataSourceByID is part of the cat.Catalog interface.
func (tc *Catalog) ResolveDataSourceByID(
	ctx context.Context, flags cat.Flags, id cat.StableID,
) (_ cat.DataSource, isAdding bool, _ error) {
	for _, ds := range tc.testSchema.dataSources {
		if ds.ID() == id {
			return ds, false, nil
		}
	}
	return nil, false, pgerror.Newf(pgcode.UndefinedTable,
		"relation [%d] does not exist", id)
}

// ResolveIndex is part of the cat.Catalog interface.
func (tc *Catalog) ResolveIndex(
	ctx context.Context, flags cat.Flags, name *tree.TableIndexName,
) (cat.Index, cat.DataSourceName, error) {
	if name.Table.Object() != "" {
		ds, dsName, err := tc.ResolveDataSource(ctx, flags, &name.Table)
		if err != nil {
			return nil, cat.DataSourceName{}, err
		}
		table := ds.(cat.Table)
		if name.Index == "" {
			// Return primary index.
			return table.Index(0), dsName, nil
		}
		for i := 0; i < table.IndexCount(); i++ {
			if idx := table.Index(i); idx.Name() == tree.Name(name.Index) {
				return idx, dsName, nil
			}
		}

		return nil, cat.DataSourceName{}, pgerror.Newf(
			pgcode.UndefinedObject, "index %q does not exist", name.Index,
		)
	}

	schema, _, err := tc.ResolveSchema(ctx, flags, &name.Table.ObjectNamePrefix)
	if err != nil {
		return nil, cat.DataSourceName{}, err
	}
	dsNames, _, err := schema.GetDataSourceNames(ctx)
	if err != nil {
		return nil, cat.DataSourceName{}, err
	}
	for i := range dsNames {
		ds, tn, err := tc.ResolveDataSource(ctx, flags, &dsNames[i])
		if err != nil {
			return nil, cat.DataSourceName{}, err
		}
		table := ds.(cat.Table)
		for i := 0; i < table.IndexCount(); i++ {
			if idx := table.Index(i); idx.Name() == tree.Name(name.Index) {
				return idx, tn, nil
			}
		}
	}
	return nil, cat.DataSourceName{}, pgerror.Newf(
		pgcode.UndefinedObject, "index %q does not exist", name.Index,
	)
}

// CheckPrivilege is part of the cat.Catalog interface.
func (tc *Catalog) CheckPrivilege(
	ctx context.Context, o cat.Object, user username.SQLUsername, priv privilege.Kind,
) error {
	return tc.CheckAnyPrivilege(ctx, o)
}

// CheckAnyPrivilege is part of the cat.Catalog interface.
func (tc *Catalog) CheckAnyPrivilege(ctx context.Context, o cat.Object) error {
	switch t := o.(type) {
	case *Schema:
		if t.Revoked {
			return pgerror.Newf(pgcode.InsufficientPrivilege, "user does not have privilege to access %v", t.SchemaName)
		}
	case *Table:
		if t.Revoked {
			return pgerror.Newf(pgcode.InsufficientPrivilege, "user does not have privilege to access %v", t.TabName)
		}
	case *View:
		if t.Revoked {
			return pgerror.Newf(pgcode.InsufficientPrivilege, "user does not have privilege to access %v", t.ViewName)
		}
	case *Sequence:
		if t.Revoked {
			return pgerror.Newf(pgcode.InsufficientPrivilege, "user does not have privilege to access %v", t.SeqName)
		}
	case *syntheticprivilege.GlobalPrivilege:

	default:
		panic("invalid Object")
	}
	return nil
}

// CheckExecutionPrivilege is part of the cat.Catalog interface.
func (tc *Catalog) CheckExecutionPrivilege(
	ctx context.Context, oid oid.Oid, user username.SQLUsername,
) error {
	if tc.revokedUDFOids.Contains(int(oid)) {
		return pgerror.Newf(pgcode.InsufficientPrivilege, "user does not have privilege to execute function with OID %d", oid)
	}
	return nil
}

// HasAdminRole is part of the cat.Catalog interface.
func (tc *Catalog) HasAdminRole(ctx context.Context) (bool, error) {
	roleMembership, found := tc.users[tc.currentUser]
	if !found {
		return false, errors.AssertionFailedf("user %q not found", tc.currentUser)
	}
	return roleMembership.isMemberOfAdminRole, nil
}

// HasRoleOption is part of the cat.Catalog interface.
func (tc *Catalog) HasRoleOption(ctx context.Context, roleOption roleoption.Option) (bool, error) {
	return true, nil
}

// FullyQualifiedName is part of the cat.Catalog interface.
func (tc *Catalog) FullyQualifiedName(
	ctx context.Context, ds cat.DataSource,
) (cat.DataSourceName, error) {
	return ds.(dataSource).fqName(), nil
}

// CheckRoleExists is part of the cat.Catalog interface.
func (tc *Catalog) CheckRoleExists(ctx context.Context, role username.SQLUsername) error {
	return nil
}

// Optimizer is part of the cat.Catalog interface.
func (tc *Catalog) Optimizer() interface{} {
	return nil
}

// GetCurrentUser is part of the cat.Catalog interface.
func (tc *Catalog) GetCurrentUser() username.SQLUsername {
	return tc.currentUser
}

// GetRoutineOwner is part of the cat.Catalog interface.
func (tc *Catalog) GetRoutineOwner(
	ctx context.Context, routineOid oid.Oid,
) (username.SQLUsername, error) {
	return tc.GetCurrentUser(), nil
}

func (tc *Catalog) resolveSchema(toResolve *cat.SchemaName) (cat.Schema, cat.SchemaName, error) {
	if string(toResolve.CatalogName) != testDB {
		return nil, cat.SchemaName{}, pgerror.Newf(pgcode.InvalidSchemaName,
			"target database or schema does not exist")
	}

	if string(toResolve.SchemaName) != catconstants.PublicSchemaName {
		return nil, cat.SchemaName{}, pgerror.Newf(pgcode.InvalidName,
			"schema cannot be modified: %q", tree.ErrString(toResolve))
	}

	return &tc.testSchema, *toResolve, nil
}

// resolveDataSource checks if `toResolve` exists among the data sources in this
// Catalog. If it does, returns the corresponding data source. Otherwise, it
// returns an error.
func (tc *Catalog) resolveDataSource(toResolve *cat.DataSourceName) (cat.DataSource, error) {
	if table, ok := tc.testSchema.dataSources[toResolve.FQString()]; ok {
		return table, nil
	}
	return nil, pgerror.Newf(pgcode.UndefinedTable,
		"no data source matches prefix: %q", tree.ErrString(toResolve))
}

// Schema returns the singleton test schema.
func (tc *Catalog) Schema() *Schema {
	return &tc.testSchema
}

// Table returns the test table that was previously added with the given name.
func (tc *Catalog) Table(name *tree.TableName) *Table {
	ds, _, err := tc.ResolveDataSource(context.Background(), cat.Flags{}, name)
	if err != nil {
		panic(err)
	}
	if tab, ok := ds.(*Table); ok {
		return tab
	}
	panic(pgerror.Newf(pgcode.WrongObjectType,
		"\"%q\" is not a table", tree.ErrString(name)))
}

// LookupTable returns the test table that was previously added with the given
// name but returns an error if the name does not exist instead of panicking.
func (tc *Catalog) LookupTable(name *tree.TableName) (*Table, error) {
	ds, _, err := tc.ResolveDataSource(context.Background(), cat.Flags{}, name)
	if err != nil {
		return nil, err
	}
	if tab, ok := ds.(*Table); ok {
		return tab, nil
	}
	return nil, pgerror.Newf(pgcode.WrongObjectType,
		"\"%q\" is not a table", tree.ErrString(name))
}

// Tables returns a list of all tables added to the test catalog.
func (tc *Catalog) Tables() []*Table {
	tables := make([]*Table, 0, len(tc.testSchema.dataSources))
	for _, ds := range tc.testSchema.dataSources {
		if tab, ok := ds.(*Table); ok {
			tables = append(tables, tab)
		}
	}
	return tables
}

// AddTable adds the given test table to the catalog.
func (tc *Catalog) AddTable(tab *Table) {
	fq := tab.TabName.FQString()
	if _, ok := tc.testSchema.dataSources[fq]; ok {
		panic(pgerror.Newf(pgcode.DuplicateObject,
			"table %q already exists", tree.ErrString(&tab.TabName)))
	}
	tc.testSchema.dataSources[fq] = tab
}

// View returns the test view that was previously added with the given name.
func (tc *Catalog) View(name *cat.DataSourceName) *View {
	ds, _, err := tc.ResolveDataSource(context.Background(), cat.Flags{}, name)
	if err != nil {
		panic(err)
	}
	if vw, ok := ds.(*View); ok {
		return vw
	}
	panic(pgerror.Newf(pgcode.WrongObjectType,
		"\"%q\" is not a view", tree.ErrString(name)))
}

// AddView adds the given test view to the catalog.
func (tc *Catalog) AddView(view *View) {
	fq := view.ViewName.FQString()
	if _, ok := tc.testSchema.dataSources[fq]; ok {
		panic(pgerror.Newf(pgcode.DuplicateObject,
			"view %q already exists", tree.ErrString(&view.ViewName)))
	}
	tc.testSchema.dataSources[fq] = view
}

// AddSequence adds the given test sequence to the catalog.
func (tc *Catalog) AddSequence(seq *Sequence) {
	fq := seq.SeqName.FQString()
	if _, ok := tc.testSchema.dataSources[fq]; ok {
		panic(pgerror.Newf(pgcode.DuplicateObject,
			"sequence %q already exists", tree.ErrString(&seq.SeqName)))
	}
	tc.testSchema.dataSources[fq] = seq
}

// GetDependencyDigest always assume that the generations are changing
// on us.
func (tc *Catalog) GetDependencyDigest() cat.DependencyDigest {
	tc.dependencyDigest++
	return cat.DependencyDigest{
		LeaseGeneration: tc.dependencyDigest,
	}
}

// ExecuteMultipleDDL parses the given semicolon-separated DDL SQL statements
// and applies each of them to the test catalog.
func (tc *Catalog) ExecuteMultipleDDL(sql string) error {
	stmts, err := parser.Parse(sql)
	if err != nil {
		return err
	}

	for _, stmt := range stmts {
		_, err := tc.ExecuteDDL(stmt.SQL)
		if err != nil {
			return err
		}
	}

	return nil
}

// ExecuteDDL parses the given DDL SQL statement and creates objects in the test
// catalog. This is used to test without spinning up a cluster.
func (tc *Catalog) ExecuteDDL(sql string) (string, error) {
	return tc.ExecuteDDLWithIndexVersion(sql, descpb.LatestIndexDescriptorVersion)
}

// ExecuteDDLWithIndexVersion parses the given DDL SQL statement and creates
// objects in the test catalog. This is used to test without spinning up a
// cluster.
//
// Any indexes created with CREATE INDEX will have the provided index
// descriptor version (the version is ignored for indexes created as part of
// a CREATE TABLE statement).
func (tc *Catalog) ExecuteDDLWithIndexVersion(
	sql string, indexVersion descpb.IndexDescriptorVersion,
) (string, error) {
	stmt, err := parser.ParseOne(sql)
	if err != nil {
		return "", err
	}
	return tc.executeDDLStmtWithIndexVersion(stmt, indexVersion)
}

// ExecuteDDLStmtWithIndexVersion statement and creates objects in the test
// catalog from the given statement. This is used to test without spinning up a
// cluster.
func (tc *Catalog) ExecuteDDLStmt(stmt statements.Statement[tree.Statement]) (string, error) {
	return tc.executeDDLStmtWithIndexVersion(stmt, descpb.LatestIndexDescriptorVersion)
}

// executeDDLStmtWithIndexVersion statement and creates objects in the test
// catalog from the given statement.
func (tc *Catalog) executeDDLStmtWithIndexVersion(
	stmt statements.Statement[tree.Statement], indexVersion descpb.IndexDescriptorVersion,
) (string, error) {

	switch stmt := stmt.AST.(type) {
	case *tree.CreateTable:
		tc.CreateTable(stmt)
		return "", nil

	case *tree.CreateIndex:
		tc.CreateIndex(stmt, indexVersion)
		return "", nil

	case *tree.CreateView:
		tc.CreateView(stmt)
		return "", nil

	case *tree.AlterTable:
		tc.AlterTable(stmt)
		return "", nil

	case *tree.DropTable:
		tc.DropTable(stmt)
		return "", nil

	case *tree.DropIndex:
		tc.DropIndex(stmt)
		return "", nil

	case *tree.CreateSequence:
		tc.CreateSequence(stmt)
		return "", nil

	case *tree.CreateType:
		tc.CreateType(stmt)
		return "", nil

	case *tree.CreateRoutine:
		tc.CreateRoutine(stmt)
		return "", nil

	case *tree.SetZoneConfig:
		tc.SetZoneConfig(stmt)
		return "", nil

	case *tree.ShowCreate:
		tn := stmt.Name.ToTableName()
		ds, _, err := tc.ResolveDataSource(context.Background(), cat.Flags{}, &tn)
		if err != nil {
			return "", err
		}
		return ds.(fmt.Stringer).String(), nil

	case *tree.ShowCreateRoutine:
		fn := tree.MakeUnresolvedFunctionName(stmt.Name.FunctionReference.(*tree.UnresolvedName))
		def, err := tc.ResolveFunction(context.Background(), fn, tree.EmptySearchPath)
		if err != nil {
			return "", err
		}
		return formatFunction(def), nil

	case *tree.CreateTrigger:
		tc.CreateTrigger(stmt)
		return "", nil

	case *tree.DropTrigger:
		tc.DropTrigger(stmt)
		return "", nil

	case *tree.CreatePolicy:
		tc.CreatePolicy(stmt)
		return "", nil

	case *tree.DropPolicy:
		tc.DropPolicy(stmt)
		return "", nil

	case *tree.SetVar:
		tc.SetVar(stmt)
		return "", nil

	case *tree.CreateRole:
		tc.CreateRole(stmt)
		return "", nil

	default:
		return "", errors.AssertionFailedf("unsupported statement: %v", stmt)
	}
}

// nextStableID returns a new unique StableID for a data source.
func (tc *Catalog) nextStableID() cat.StableID {
	tc.counter++

	// 53 is a magic number derived from how CRDB internally stores tables. The
	// first user table is 53. Use this to have the test catalog look more
	// consistent with the real catalog.
	return cat.StableID(53 + tc.counter - 1)
}

// qualifyTableName updates the given table name to include catalog and schema
// if not already included.
func (tc *Catalog) qualifyTableName(name *tree.TableName) {
	hadExplicitSchema := name.ExplicitSchema
	hadExplicitCatalog := name.ExplicitCatalog
	name.ExplicitSchema = true
	name.ExplicitCatalog = true

	if hadExplicitSchema {
		if hadExplicitCatalog {
			// Already 3 parts: nothing to do.
			return
		}

		if name.SchemaName == catconstants.PublicSchemaName {
			// Use the current database.
			name.CatalogName = testDB
			return
		}

		// Compatibility with CockroachDB v1.1: use D.public.T.
		name.CatalogName = name.SchemaName
		name.SchemaName = catconstants.PublicSchemaName
		return
	}

	// Use the current database.
	name.CatalogName = testDB
	name.SchemaName = catconstants.PublicSchemaName
}

// Schema implements the cat.Schema interface for testing purposes.
type Schema struct {
	SchemaID   cat.StableID
	SchemaName cat.SchemaName

	// If Revoked is true, then the user has had privileges on the schema revoked.
	Revoked bool

	dataSources map[string]dataSource
}

var _ cat.Schema = &Schema{}

// ID is part of the cat.Object interface.
func (s *Schema) ID() cat.StableID {
	return s.SchemaID
}

// PostgresDescriptorID is part of the cat.Object interface.
func (s *Schema) PostgresDescriptorID() catid.DescID {
	return catid.DescID(s.SchemaID)
}

// Equals is part of the cat.Object interface.
func (s *Schema) Equals(other cat.Object) bool {
	otherSchema, ok := other.(*Schema)
	return ok && s.SchemaID == otherSchema.SchemaID
}

// Name is part of the cat.Schema interface.
func (s *Schema) Name() *cat.SchemaName {
	return &s.SchemaName
}

// GetDataSourceNames is part of the cat.Schema interface.
func (s *Schema) GetDataSourceNames(ctx context.Context) ([]cat.DataSourceName, descpb.IDs, error) {
	var keys []string
	for k := range s.dataSources {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	names := make([]cat.DataSourceName, 0, len(keys))
	IDs := make(descpb.IDs, 0, len(keys))
	for _, k := range keys {
		names = append(names, s.dataSources[k].fqName())
		IDs = append(IDs, descpb.ID(s.dataSources[k].ID()))
	}
	return names, IDs, nil
}

// View implements the cat.View interface for testing purposes.
type View struct {
	ViewID      cat.StableID
	ViewVersion int
	ViewName    cat.DataSourceName
	QueryText   string
	ColumnNames tree.NameList
	Triggers    []Trigger

	// If Revoked is true, then the user has had privileges on the view revoked.
	Revoked bool
}

var _ cat.View = &View{}

func (tv *View) String() string {
	tp := treeprinter.New()
	cat.FormatView(tv, tp)
	return tp.String()
}

// ID is part of the cat.DataSource interface.
func (tv *View) ID() cat.StableID {
	return tv.ViewID
}

// PostgresDescriptorID is part of the cat.Object interface.
func (tv *View) PostgresDescriptorID() catid.DescID {
	return catid.DescID(tv.ViewID)
}

// Equals is part of the cat.Object interface.
func (tv *View) Equals(other cat.Object) bool {
	otherView, ok := other.(*View)
	if !ok {
		return false
	}
	return tv.ViewID == otherView.ViewID && tv.ViewVersion == otherView.ViewVersion
}

// Name is part of the cat.DataSource interface.
func (tv *View) Name() tree.Name {
	return tv.ViewName.ObjectName
}

// fqName is part of the dataSource interface.
func (tv *View) fqName() cat.DataSourceName {
	return tv.ViewName
}

// IsSystemView is part of the cat.View interface.
func (tv *View) IsSystemView() bool {
	return false
}

// Query is part of the cat.View interface.
func (tv *View) Query() string {
	return tv.QueryText
}

// ColumnNameCount is part of the cat.View interface.
func (tv *View) ColumnNameCount() int {
	return len(tv.ColumnNames)
}

// ColumnName is part of the cat.View interface.
func (tv *View) ColumnName(i int) tree.Name {
	return tv.ColumnNames[i]
}

// CollectTypes is part of the cat.DataSource interface.
func (tv *View) CollectTypes(ord int) (descpb.IDs, error) {
	return nil, nil
}

// TriggerCount is a part of the cat.View interface.
func (tv *View) TriggerCount() int {
	return len(tv.Triggers)
}

// Trigger is a part of the cat.View interface.
func (tv *View) Trigger(i int) cat.Trigger {
	return &tv.Triggers[i]
}

// Table implements the cat.Table interface for testing purposes.
type Table struct {
	TabID      cat.StableID
	DatabaseID descpb.ID
	TabVersion int
	TabName    tree.TableName
	Columns    []cat.Column
	Indexes    []*Index
	Stats      TableStats
	Checks     []cat.CheckConstraint
	Families   []*Family
	Triggers   []Trigger
	IsVirtual  bool
	IsSystem   bool
	Catalog    *Catalog

	// If Revoked is true, then the user has had privileges on the table revoked.
	Revoked bool

	writeOnlyIdxCount  int
	deleteOnlyIdxCount int

	outboundFKs []ForeignKeyConstraint
	inboundFKs  []ForeignKeyConstraint

	uniqueConstraints []UniqueConstraint

	// partitionBy is the partitioning clause that corresponds to the primary
	// index. Used to initialize the partitioning for the primary index.
	partitionBy *tree.PartitionBy

	multiRegion bool

	implicitRBRIndexElem *tree.IndexElem

	homeRegion string

	rlsEnabled bool
	policies   map[tree.PolicyType][]cat.Policy
}

var _ cat.Table = &Table{}

func (tt *Table) String() string {
	tp := treeprinter.New()
	cat.FormatTable(context.Background(), tt.Catalog, tt, tp, false /* redactableValues */)
	return tp.String()
}

// SetMultiRegion can make a table in the test catalog appear to be a
// multiregion table, in that it can cause cat.Table.IsMultiregion() to return
// true after SetMultiRegion(true) has been called.
func (tt *Table) SetMultiRegion(val bool) {
	tt.multiRegion = val
}

// ID is part of the cat.DataSource interface.
func (tt *Table) ID() cat.StableID {
	return tt.TabID
}

// PostgresDescriptorID is part of the cat.Object interface.
func (tt *Table) PostgresDescriptorID() catid.DescID {
	return catid.DescID(tt.TabID)
}

// Equals is part of the cat.Object interface.
func (tt *Table) Equals(other cat.Object) bool {
	otherTable, ok := other.(*Table)
	if !ok {
		return false
	}
	return tt.TabID == otherTable.TabID && tt.TabVersion == otherTable.TabVersion
}

// Name is part of the cat.DataSource interface.
func (tt *Table) Name() tree.Name {
	return tt.TabName.ObjectName
}

// fqName is part of the dataSource interface.
func (tt *Table) fqName() cat.DataSourceName {
	return tt.TabName
}

// IsVirtualTable is part of the cat.Table interface.
func (tt *Table) IsVirtualTable() bool {
	return tt.IsVirtual
}

// IsSystemTable is part of the cat.Table interface.
func (tt *Table) IsSystemTable() bool {
	return tt.IsSystem
}

// IsMaterializedView is part of the cat.Table interface.
func (tt *Table) IsMaterializedView() bool {
	return false
}

// ColumnCount is part of the cat.Table interface.
func (tt *Table) ColumnCount() int {
	return len(tt.Columns)
}

// Column is part of the cat.Table interface.
func (tt *Table) Column(i int) *cat.Column {
	return &tt.Columns[i]
}

// IndexCount is part of the cat.Table interface.
func (tt *Table) IndexCount() int {
	return len(tt.Indexes) - tt.writeOnlyIdxCount - tt.deleteOnlyIdxCount
}

// WritableIndexCount is part of the cat.Table interface.
func (tt *Table) WritableIndexCount() int {
	return len(tt.Indexes) - tt.deleteOnlyIdxCount
}

// DeletableIndexCount is part of the cat.Table interface.
func (tt *Table) DeletableIndexCount() int {
	return len(tt.Indexes)
}

// Index is part of the cat.Table interface.
func (tt *Table) Index(i cat.IndexOrdinal) cat.Index {
	return tt.Indexes[i]
}

// StatisticCount is part of the cat.Table interface.
func (tt *Table) StatisticCount() int {
	return len(tt.Stats)
}

// Statistic is part of the cat.Table interface.
func (tt *Table) Statistic(i int) cat.TableStatistic {
	return tt.Stats[i]
}

// CheckCount is part of the cat.Table interface.
func (tt *Table) CheckCount() int {
	return len(tt.Checks)
}

// Check is part of the cat.Table interface.
func (tt *Table) Check(i int) cat.CheckConstraint {
	return tt.Checks[i]
}

// FamilyCount is part of the cat.Table interface.
func (tt *Table) FamilyCount() int {
	return len(tt.Families)
}

// Family is part of the cat.Table interface.
func (tt *Table) Family(i int) cat.Family {
	return tt.Families[i]
}

// OutboundForeignKeyCount is part of the cat.Table interface.
func (tt *Table) OutboundForeignKeyCount() int {
	return len(tt.outboundFKs)
}

// OutboundForeignKey is part of the cat.Table interface.
func (tt *Table) OutboundForeignKey(i int) cat.ForeignKeyConstraint {
	return &tt.outboundFKs[i]
}

// InboundForeignKeyCount is part of the cat.Table interface.
func (tt *Table) InboundForeignKeyCount() int {
	return len(tt.inboundFKs)
}

// InboundForeignKey is part of the cat.Table interface.
func (tt *Table) InboundForeignKey(i int) cat.ForeignKeyConstraint {
	return &tt.inboundFKs[i]
}

// UniqueCount is part of the cat.Table interface.
func (tt *Table) UniqueCount() int {
	return len(tt.uniqueConstraints)
}

// Unique is part of the cat.Table interface.
func (tt *Table) Unique(i cat.UniqueOrdinal) cat.UniqueConstraint {
	return &tt.uniqueConstraints[i]
}

// Zone is part of the cat.Table interface.
func (tt *Table) Zone() cat.Zone {
	zone := zonepb.DefaultZoneConfig()
	return cat.AsZone(&zone)
}

// IsPartitionAllBy is part of the cat.Table interface.
func (tt *Table) IsPartitionAllBy() bool {
	return tt.implicitRBRIndexElem != nil
}

// HomeRegion is part of the cat.Table interface.
func (tt *Table) HomeRegion() (region string, ok bool) {
	if tt.homeRegion == "" {
		return "", false
	}
	return tt.homeRegion, true
}

// IsGlobalTable is part of the cat.Table interface.
func (tt *Table) IsGlobalTable() bool {
	return false
}

// IsRegionalByRow is part of the cat.Table interface.
func (tt *Table) IsRegionalByRow() bool {
	return tt.implicitRBRIndexElem != nil
}

// IsMultiregion is part of the cat.Table interface.
func (tt *Table) IsMultiregion() bool {
	return tt.multiRegion
}

// HomeRegionColName is part of the cat.Table interface.
func (tt *Table) HomeRegionColName() (colName string, ok bool) {
	if !tt.IsRegionalByRow() {
		return "", false
	}
	return string(tree.RegionalByRowRegionDefaultColName), true
}

// GetDatabaseID is part of the cat.Table interface.
func (tt *Table) GetDatabaseID() descpb.ID {
	return tt.DatabaseID
}

// IsHypothetical is part of the cat.Table interface.
func (tt *Table) IsHypothetical() bool {
	return false
}

// FindOrdinal returns the ordinal of the column with the given name.
func (tt *Table) FindOrdinal(name string) int {
	for i, col := range tt.Columns {
		if col.ColName() == tree.Name(name) {
			return i
		}
	}
	panic(pgerror.Newf(pgcode.UndefinedColumn,
		"cannot find column %q in table %q",
		tree.ErrString((*tree.Name)(&name)),
		tree.ErrString(&tt.TabName),
	))
}

// CollectTypes is part of the cat.DataSource interface.
func (tt *Table) CollectTypes(ord int) (descpb.IDs, error) {
	visitor := &tree.TypeCollectorVisitor{
		OIDs: make(map[oid.Oid]struct{}),
	}
	addOIDsInExpr := func(exprStr string) error {
		expr, err := parser.ParseExpr(exprStr)
		if err != nil {
			return err
		}
		tree.WalkExpr(visitor, expr)
		return nil
	}

	// Collect UDTs in default expression, ON UPDATE expression, computed column
	// and the column type itself.
	col := tt.Columns[ord]
	if col.HasDefault() {
		if err := addOIDsInExpr(col.DefaultExprStr()); err != nil {
			return nil, err
		}
	}
	if col.HasOnUpdate() {
		if err := addOIDsInExpr(col.OnUpdateExprStr()); err != nil {
			return nil, err
		}
	}
	if col.IsComputed() {
		if err := addOIDsInExpr(col.ComputedExprStr()); err != nil {
			return nil, err
		}
	}
	if col.DatumType() != nil && col.DatumType().UserDefined() {
		visitor.OIDs[col.DatumType().Oid()] = struct{}{}
	}

	ids := make(descpb.IDs, 0, len(visitor.OIDs))
	for collectedOid := range visitor.OIDs {
		ids = append(ids, typedesc.UserDefinedTypeOIDToID(collectedOid))
	}
	return ids, nil
}

// IsRefreshViewRequired is a part of the cat.Table interface.
func (tt *Table) IsRefreshViewRequired() bool {
	return false
}

// TriggerCount is a part of the cat.Table interface.
func (tt *Table) TriggerCount() int {
	return len(tt.Triggers)
}

// Trigger is a part of the cat.Table interface.
func (tt *Table) Trigger(i int) cat.Trigger {
	return &tt.Triggers[i]
}

// IsRowLevelSecurityEnabled is part of the cat.Table interface.
func (tt *Table) IsRowLevelSecurityEnabled() bool { return tt.rlsEnabled }

// PolicyCount is part of the cat.Table interface
func (tt *Table) PolicyCount(polType tree.PolicyType) int {
	pols := tt.policies[polType]
	return len(pols)
}

// Policy is part of the cat.Table interface
func (tt *Table) Policy(policyType tree.PolicyType, index int) cat.Policy {
	policies, exists := tt.policies[policyType]
	if !exists || index >= len(policies) {
		panic(errors.AssertionFailedf("policy of type %v at index %d not found", policyType, index))
	}
	return policies[index]
}

// findPolicyByName will lookup the policy by its name. It returns it's policy
// type and index within that policy type slice so that callers can do removal
// if needed.
func (tt *Table) findPolicyByName(policyName tree.Name) (cat.Policy, tree.PolicyType, int) {
	for _, pt := range []tree.PolicyType{tree.PolicyTypePermissive, tree.PolicyTypeRestrictive} {
		for i := range tt.PolicyCount(pt) {
			p := tt.Policy(pt, i)
			if p.Name() == policyName {
				return p, pt, i
			}
		}
	}
	return nil, tree.PolicyTypePermissive, -1
}

// Index implements the cat.Index interface for testing purposes.
type Index struct {
	IdxName string

	// ExplicitColCount is the number of columns that are explicitly specified in
	// the index definition.
	ExplicitColCount int

	// KeyCount is the number of columns that make up the unique key for the
	// index. See the cat.Index.KeyColumnCount for more details.
	KeyCount int

	// LaxKeyCount is the number of columns that make up a lax key for the
	// index, which allows duplicate rows when at least one of the values is
	// NULL. See the cat.Index.LaxKeyColumnCount for more details.
	LaxKeyCount int

	// Unique is true if this index is declared as UNIQUE in the schema.
	Unique bool

	// Inverted is true when this index is an inverted index.
	Inverted bool

	// Vector is true when this index is a vector index.
	Vector bool

	// Invisibility specifies the invisibility of an index and can be any float64
	// between [0.0, 1.0]. An index with invisibility 0.0 means that the index is
	// visible. An index with invisibility 1.0 means that the index is fully not
	// visible.
	Invisibility float64

	Columns []cat.IndexColumn

	// IdxZone is the zone associated with the index. This may be inherited from
	// the parent table, database, or even the default zone.
	IdxZone cat.Zone

	// Ordinal is the ordinal of this index in the table.
	ordinal int

	// table is a back reference to the table this index is on.
	table *Table

	// partitions stores zone information and datums for PARTITION BY LIST
	// partitions.
	partitions []Partition

	// predicate is the partial index predicate expression, if it exists.
	predicate string

	// invertedOrd is the ordinal of the Inverted column, if the index is
	// an inverted index.
	invertedOrd int

	// vectorOrd is the ordinal of the vector column, if the index is a vector
	// index.
	vectorOrd int

	// geoConfig is the geospatial index configuration, if this is a geospatial
	// inverted index.
	geoConfig geopb.Config

	// version is the index descriptor version of the index.
	version descpb.IndexDescriptorVersion

	// numImplicitPartitioningColumns is the number of implicit partitioning
	// columns defined in this index.
	numImplicitPartitioningColumns int
}

// ID is part of the cat.Index interface.
func (ti *Index) ID() cat.StableID {
	return 1 + cat.StableID(ti.ordinal)
}

// Name is part of the cat.Index interface.
func (ti *Index) Name() tree.Name {
	return tree.Name(ti.IdxName)
}

// Table is part of the cat.Index interface.
func (ti *Index) Table() cat.Table {
	return ti.table
}

// Ordinal is part of the cat.Index interface.
func (ti *Index) Ordinal() cat.IndexOrdinal {
	return ti.ordinal
}

// IsUnique is part of the cat.Index interface.
func (ti *Index) IsUnique() bool {
	return ti.Unique
}

// IsInverted is part of the cat.Index interface.
func (ti *Index) IsInverted() bool {
	return ti.Inverted
}

// IsVector is part of the cat.Index interface.
func (ti *Index) IsVector() bool {
	return ti.Vector
}

// GetInvisibility is part of the cat.Index interface.
func (ti *Index) GetInvisibility() float64 {
	return ti.Invisibility
}

// ColumnCount is part of the cat.Index interface.
func (ti *Index) ColumnCount() int {
	return len(ti.Columns)
}

// ExplicitColumnCount is part of the cat.Index interface.
func (ti *Index) ExplicitColumnCount() int {
	return ti.ExplicitColCount
}

// KeyColumnCount is part of the cat.Index interface.
func (ti *Index) KeyColumnCount() int {
	return ti.KeyCount
}

// LaxKeyColumnCount is part of the cat.Index interface.
func (ti *Index) LaxKeyColumnCount() int {
	return ti.LaxKeyCount
}

// PrefixColumnCount is part of the cat.Index interface.
func (ti *Index) PrefixColumnCount() int {
	if ti.IsInverted() {
		return ti.invertedOrd
	}
	if ti.IsVector() {
		return ti.vectorOrd
	}
	panic("only supported for inverted and vector indexes")
}

// Column is part of the cat.Index interface.
func (ti *Index) Column(i int) cat.IndexColumn {
	return ti.Columns[i]
}

// InvertedColumn is part of the cat.Index interface.
func (ti *Index) InvertedColumn() cat.IndexColumn {
	if !ti.IsInverted() {
		panic("non-inverted indexes do not have inverted columns")
	}
	return ti.Column(ti.invertedOrd)
}

// VectorColumn is part of the cat.Index interface.
func (ti *Index) VectorColumn() cat.IndexColumn {
	if !ti.IsVector() {
		panic("non-vector indexes do not have indexed vector columns")
	}
	return ti.Column(ti.vectorOrd)
}

// Zone is part of the cat.Index interface.
func (ti *Index) Zone() cat.Zone {
	return ti.IdxZone
}

// Span is part of the cat.Index interface.
func (ti *Index) Span() roachpb.Span {
	panic("not implemented")
}

// Predicate is part of the cat.Index interface. It returns the predicate
// expression and true if the index is a partial index. If the index is not
// partial, the empty string and false is returned.
func (ti *Index) Predicate() (string, bool) {
	return ti.predicate, ti.predicate != ""
}

// ImplicitColumnCount is part of the cat.Index interface.
func (ti *Index) ImplicitColumnCount() int {
	return ti.numImplicitPartitioningColumns
}

// ImplicitPartitioningColumnCount is part of the cat.Index interface.
func (ti *Index) ImplicitPartitioningColumnCount() int {
	return ti.numImplicitPartitioningColumns
}

// GeoConfig is part of the cat.Index interface.
func (ti *Index) GeoConfig() geopb.Config {
	return ti.geoConfig
}

// Version is part of the cat.Index interface.
func (ti *Index) Version() descpb.IndexDescriptorVersion {
	return ti.version
}

// PartitionCount is part of the cat.Index interface.
func (ti *Index) PartitionCount() int {
	return len(ti.partitions)
}

// Partition is part of the cat.Index interface.
func (ti *Index) Partition(i int) cat.Partition {
	return &ti.partitions[i]
}

// SetPartitions manually sets the partitions.
func (ti *Index) SetPartitions(partitions []Partition) {
	ti.partitions = partitions
}

// Partition implements the cat.Partition interface for testing purposes.
type Partition struct {
	name   string
	zone   cat.Zone
	datums []tree.Datums
}

var _ cat.Partition = &Partition{}

// Name is part of the cat.Partition interface.
func (p *Partition) Name() string {
	return p.name
}

// Zone is part of the cat.Partition interface.
func (p *Partition) Zone() cat.Zone {
	return p.zone
}

// PartitionByListPrefixes is part of the cat.Partition interface.
func (p *Partition) PartitionByListPrefixes() []tree.Datums {
	return p.datums
}

// SetDatums manually sets the partitioning values.
func (p *Partition) SetDatums(datums []tree.Datums) {
	p.datums = datums
}

// CheckConstraint implements cat.CheckConstraint. See that interface
// for more information on the fields.
type CheckConstraint struct {
	constraint     string
	validated      bool
	columnOrdinals []int
}

var _ cat.CheckConstraint = &CheckConstraint{}

// Constraint is part of the cat.CheckConstraint interface.
func (c *CheckConstraint) Constraint() string {
	return c.constraint
}

// Validated is part of the cat.CheckConstraint interface.
func (c *CheckConstraint) Validated() bool {
	return c.validated
}

// ColumnCount is part of the cat.CheckConstraint interface.
func (c *CheckConstraint) ColumnCount() int {
	return len(c.columnOrdinals)
}

// ColumnOrdinal is part of the cat.CheckConstraint interface.
func (c *CheckConstraint) ColumnOrdinal(i int) int {
	return c.columnOrdinals[i]
}

// TableStat implements the cat.TableStatistic interface for testing purposes.
type TableStat struct {
	js            stats.JSONStatistic
	tt            *Table
	evalCtx       *eval.Context
	histogram     []cat.HistogramBucket
	histogramType *types.T
	tc            *Catalog
}

var _ cat.TableStatistic = &TableStat{}

// CreatedAt is part of the cat.TableStatistic interface.
func (ts *TableStat) CreatedAt() time.Time {
	d, _, err := tree.ParseDTimestamp(nil, ts.js.CreatedAt, time.Microsecond)
	if err != nil {
		panic(err)
	}
	return d.Time
}

// ColumnCount is part of the cat.TableStatistic interface.
func (ts *TableStat) ColumnCount() int {
	return len(ts.js.Columns)
}

// ColumnOrdinal is part of the cat.TableStatistic interface.
func (ts *TableStat) ColumnOrdinal(i int) int {
	return ts.tt.FindOrdinal(ts.js.Columns[i])
}

// RowCount is part of the cat.TableStatistic interface.
func (ts *TableStat) RowCount() uint64 {
	return ts.js.RowCount
}

// DistinctCount is part of the cat.TableStatistic interface.
func (ts *TableStat) DistinctCount() uint64 {
	return ts.js.DistinctCount
}

// NullCount is part of the cat.TableStatistic interface.
func (ts *TableStat) NullCount() uint64 {
	return ts.js.NullCount
}

// AvgSize is part of the cat.TableStatistic interface.
func (ts *TableStat) AvgSize() uint64 {
	return ts.js.AvgSize
}

// Histogram is part of the cat.TableStatistic interface.
func (ts *TableStat) Histogram() []cat.HistogramBucket {
	if ts.histogram != nil {
		return ts.histogram
	}
	evalCtx := ts.evalCtx
	if evalCtx == nil {
		evalCtxVal := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
		evalCtx = &evalCtxVal
	}
	if ts.js.HistogramColumnType == "" || ts.js.HistogramBuckets == nil {
		return nil
	}
	colTypeRef, err := parser.GetTypeFromValidSQLSyntax(ts.js.HistogramColumnType)
	if err != nil {
		panic(err)
	}
	colType, err := tree.ResolveType(context.Background(), colTypeRef, ts.tc)
	if err != nil {
		return nil
	}

	var offset int
	if ts.js.NullCount > 0 {
		// A bucket for NULL is not persisted, but we create a fake one to
		// make histograms easier to work with. The length of histogram
		// is therefore 1 greater than the length of ts.js.HistogramBuckets.
		// NOTE: This matches the logic in the TableStatisticsCache.
		ts.histogram = make([]cat.HistogramBucket, len(ts.js.HistogramBuckets)+1)
		ts.histogram[0] = cat.HistogramBucket{
			NumEq:         float64(ts.js.NullCount),
			NumRange:      0,
			DistinctRange: 0,
			UpperBound:    tree.DNull,
		}
		offset = 1
	} else {
		ts.histogram = make([]cat.HistogramBucket, len(ts.js.HistogramBuckets))
		offset = 0
	}

	for i := offset; i < len(ts.histogram); i++ {
		bucket := &ts.js.HistogramBuckets[i-offset]
		datum, err := rowenc.ParseDatumStringAs(context.Background(), colType, bucket.UpperBound, evalCtx, nil /* semaCtx */)
		if err != nil {
			panic(err)
		}
		ts.histogram[i] = cat.HistogramBucket{
			NumEq:         float64(bucket.NumEq),
			NumRange:      float64(bucket.NumRange),
			DistinctRange: bucket.DistinctRange,
			UpperBound:    datum,
		}
	}
	return ts.histogram
}

// HistogramType is part of the cat.TableStatistic interface.
func (ts *TableStat) HistogramType() *types.T {
	if ts.histogramType != nil {
		return ts.histogramType
	}
	colTypeRef, err := parser.GetTypeFromValidSQLSyntax(ts.js.HistogramColumnType)
	if err != nil {
		panic(err)
	}
	ts.histogramType = tree.MustBeStaticallyKnownType(colTypeRef)
	return ts.histogramType
}

// IsPartial is part of the cat.TableStatistic interface.
func (ts *TableStat) IsPartial() bool {
	return ts.js.IsPartial()
}

// IsMerged is part of the cat.TableStatistic interface.
func (ts *TableStat) IsMerged() bool {
	return ts.js.IsMerged()
}

// IsForecast is part of the cat.TableStatistic interface.
func (ts *TableStat) IsForecast() bool {
	return ts.js.IsForecast()
}

// IsAuto is part of the cat.TableStatistic interface.
func (ts *TableStat) IsAuto() bool {
	return ts.js.IsAuto()
}

// TableStats is a slice of TableStat pointers.
type TableStats []*TableStat

// Len is part of the Sorter interface.
func (ts TableStats) Len() int { return len(ts) }

// Less is part of the Sorter interface.
func (ts TableStats) Less(i, j int) bool {
	// Sort with most recent first.
	return ts[i].CreatedAt().Unix() > ts[j].CreatedAt().Unix()
}

// Swap is part of the Sorter interface.
func (ts TableStats) Swap(i, j int) {
	ts[i], ts[j] = ts[j], ts[i]
}

// ForeignKeyConstraint implements cat.ForeignKeyConstraint. See that interface
// for more information on the fields.
type ForeignKeyConstraint struct {
	name              string
	originTableID     cat.StableID
	referencedTableID cat.StableID

	originColumnOrdinals     []int
	referencedColumnOrdinals []int

	validated    bool
	matchMethod  tree.CompositeKeyMatchMethod
	deleteAction tree.ReferenceAction
	updateAction tree.ReferenceAction
}

var _ cat.ForeignKeyConstraint = &ForeignKeyConstraint{}

// Name is part of the cat.ForeignKeyConstraint interface.
func (fk *ForeignKeyConstraint) Name() string {
	return fk.name
}

// OriginTableID is part of the cat.ForeignKeyConstraint interface.
func (fk *ForeignKeyConstraint) OriginTableID() cat.StableID {
	return fk.originTableID
}

// ReferencedTableID is part of the cat.ForeignKeyConstraint interface.
func (fk *ForeignKeyConstraint) ReferencedTableID() cat.StableID {
	return fk.referencedTableID
}

// ColumnCount is part of the cat.ForeignKeyConstraint interface.
func (fk *ForeignKeyConstraint) ColumnCount() int {
	return len(fk.originColumnOrdinals)
}

// OriginColumnOrdinal is part of the cat.ForeignKeyConstraint interface.
func (fk *ForeignKeyConstraint) OriginColumnOrdinal(originTable cat.Table, i int) int {
	if originTable.ID() != fk.originTableID {
		panic(errors.AssertionFailedf(
			"invalid table %d passed to OriginColumnOrdinal (expected %d)",
			originTable.ID(), fk.originTableID,
		))
	}

	return fk.originColumnOrdinals[i]
}

// ReferencedColumnOrdinal is part of the cat.ForeignKeyConstraint interface.
func (fk *ForeignKeyConstraint) ReferencedColumnOrdinal(referencedTable cat.Table, i int) int {
	if referencedTable.ID() != fk.referencedTableID {
		panic(errors.AssertionFailedf(
			"invalid table %d passed to ReferencedColumnOrdinal (expected %d)",
			referencedTable.ID(), fk.referencedTableID,
		))
	}
	return fk.referencedColumnOrdinals[i]
}

// Validated is part of the cat.ForeignKeyConstraint interface.
func (fk *ForeignKeyConstraint) Validated() bool {
	return fk.validated
}

// MatchMethod is part of the cat.ForeignKeyConstraint interface.
func (fk *ForeignKeyConstraint) MatchMethod() tree.CompositeKeyMatchMethod {
	return fk.matchMethod
}

// DeleteReferenceAction is part of the cat.ForeignKeyConstraint interface.
func (fk *ForeignKeyConstraint) DeleteReferenceAction() tree.ReferenceAction {
	return fk.deleteAction
}

// UpdateReferenceAction is part of the cat.ForeignKeyConstraint interface.
func (fk *ForeignKeyConstraint) UpdateReferenceAction() tree.ReferenceAction {
	return fk.updateAction
}

// UniqueConstraint implements cat.UniqueConstraint. See that interface
// for more information on the fields.
type UniqueConstraint struct {
	name                  string
	tabID                 cat.StableID
	columnOrdinals        []int
	predicate             string
	withoutIndex          bool
	canUseTombstones      bool
	tombstoneIndexOrdinal cat.IndexOrdinal
	validated             bool
}

var _ cat.UniqueConstraint = &UniqueConstraint{}

// Name is part of the cat.UniqueConstraint interface.
func (u *UniqueConstraint) Name() string {
	return u.name
}

// TableID is part of the cat.UniqueConstraint interface.
func (u *UniqueConstraint) TableID() cat.StableID {
	return u.tabID
}

// ColumnCount is part of the cat.UniqueConstraint interface.
func (u *UniqueConstraint) ColumnCount() int {
	return len(u.columnOrdinals)
}

// ColumnOrdinal is part of the cat.UniqueConstraint interface.
func (u *UniqueConstraint) ColumnOrdinal(tab cat.Table, i int) int {
	if tab.ID() != u.tabID {
		panic(errors.AssertionFailedf(
			"invalid table %d passed to ColumnOrdinal (expected %d)",
			tab.ID(), u.tabID,
		))
	}
	return u.columnOrdinals[i]
}

// Predicate is part of the cat.UniqueConstraint interface.
func (u *UniqueConstraint) Predicate() (string, bool) {
	return u.predicate, u.predicate != ""
}

// WithoutIndex is part of the cat.UniqueConstraint interface.
func (u *UniqueConstraint) WithoutIndex() bool {
	return u.withoutIndex
}

// TombstoneIndexOrdinal is part of the cat.UniqueConstraint interface
func (u *UniqueConstraint) TombstoneIndexOrdinal() (ordinal cat.IndexOrdinal, ok bool) {
	ok = u.canUseTombstones
	if ok {
		ordinal = u.tombstoneIndexOrdinal
	} else {
		ordinal = -1
	}
	return ordinal, ok
}

// Validated is part of the cat.UniqueConstraint interface.
func (u *UniqueConstraint) Validated() bool {
	return u.validated
}

// UniquenessGuaranteedByAnotherIndex is part of the cat.UniqueConstraint
// interface.
func (u *UniqueConstraint) UniquenessGuaranteedByAnotherIndex() bool {
	return false
}

// Sequence implements the cat.Sequence interface for testing purposes.
type Sequence struct {
	SeqID      cat.StableID
	SeqVersion int
	SeqName    tree.TableName
	Catalog    cat.Catalog

	// If Revoked is true, then the user has had privileges on the sequence revoked.
	Revoked bool
}

var _ cat.Sequence = &Sequence{}

// ID is part of the cat.DataSource interface.
func (ts *Sequence) ID() cat.StableID {
	return ts.SeqID
}

// PostgresDescriptorID is part of the cat.Object interface.
func (ts *Sequence) PostgresDescriptorID() catid.DescID {
	return catid.DescID(ts.SeqID)
}

// Equals is part of the cat.Object interface.
func (ts *Sequence) Equals(other cat.Object) bool {
	otherSequence, ok := other.(*Sequence)
	if !ok {
		return false
	}
	return ts.SeqID == otherSequence.SeqID && ts.SeqVersion == otherSequence.SeqVersion
}

// Name is part of the cat.DataSource interface.
func (ts *Sequence) Name() tree.Name {
	return ts.SeqName.ObjectName
}

// fqName is part of the dataSource interface.
func (ts *Sequence) fqName() cat.DataSourceName {
	return ts.SeqName
}

// SequenceMarker is part of the cat.Sequence interface.
func (ts *Sequence) SequenceMarker() {}

func (ts *Sequence) String() string {
	tp := treeprinter.New()
	cat.FormatSequence(ts.Catalog, ts, tp)
	return tp.String()
}

// CollectTypes is part of the cat.DataSource interface.
func (ts *Sequence) CollectTypes(ord int) (descpb.IDs, error) {
	return nil, nil
}

// Family implements the cat.Family interface for testing purposes.
type Family struct {
	FamName string

	// Ordinal is the ordinal of this family in the table.
	Ordinal int

	Columns []cat.FamilyColumn

	// table is a back reference to the table this index is on.
	table *Table
}

// ID is part of the cat.Family interface.
func (tf *Family) ID() cat.StableID {
	return 1 + cat.StableID(tf.Ordinal)
}

// Name is part of the cat.Family interface.
func (tf *Family) Name() tree.Name {
	return tree.Name(tf.FamName)
}

// Table is part of the cat.Family interface.
func (tf *Family) Table() cat.Table {
	return tf.table
}

// ColumnCount is part of the cat.Family interface.
func (tf *Family) ColumnCount() int {
	return len(tf.Columns)
}

// Column is part of the cat.Family interface.
func (tf *Family) Column(i int) cat.FamilyColumn {
	return tf.Columns[i]
}

// Trigger implements the cat.Trigger interface for testing purposes.
type Trigger struct {
	TriggerName               tree.Name
	TriggerActionTime         tree.TriggerActionTime
	TriggerEvents             []*tree.TriggerEvent
	TriggerTableID            cat.StableID
	TriggerNewTransitionAlias tree.Name
	TriggerOldTransitionAlias tree.Name
	TriggerForEachRow         bool
	TriggerWhenExpr           string
	TriggerFuncID             cat.StableID
	TriggerFuncArgs           tree.Datums
	TriggerFuncBody           string
	TriggerEnabled            bool
}

var _ cat.Trigger = &Trigger{}

func (t *Trigger) Name() tree.Name {
	return t.TriggerName
}

func (t *Trigger) ActionTime() tree.TriggerActionTime {
	return t.TriggerActionTime
}

func (t *Trigger) EventCount() int {
	return len(t.TriggerEvents)
}

func (t *Trigger) Event(i int) tree.TriggerEvent {
	return *t.TriggerEvents[i]
}

func (t *Trigger) TableID() cat.StableID {
	return t.TriggerTableID
}

func (t *Trigger) NewTransitionAlias() tree.Name {
	return t.TriggerNewTransitionAlias
}

func (t *Trigger) OldTransitionAlias() tree.Name {
	return t.TriggerOldTransitionAlias
}

func (t *Trigger) ForEachRow() bool {
	return t.TriggerForEachRow
}

func (t *Trigger) WhenExpr() string {
	return t.TriggerWhenExpr
}

func (t *Trigger) FuncID() cat.StableID {
	return t.TriggerFuncID
}

func (t *Trigger) FuncArgs() tree.Datums {
	return t.TriggerFuncArgs
}

func (t *Trigger) FuncBody() string {
	return t.TriggerFuncBody
}

func (t *Trigger) Enabled() bool {
	return t.TriggerEnabled
}

// Policy implements the cat.Policy interface
type Policy struct {
	name          tree.Name
	usingExpr     string
	withCheckExpr string
	command       catpb.PolicyCommand
	roles         map[string]struct{}
}

// Name implements the cat.Policy interface
func (p *Policy) Name() tree.Name {
	return p.name
}

// GetUsingExpr implements the cat.Policy interface
func (p *Policy) GetUsingExpr() string {
	return p.usingExpr
}

// GetWithCheckExpr implements the cat.Policy interface
func (p *Policy) GetWithCheckExpr() string {
	return p.withCheckExpr
}

// GetPolicyCommand implements the cat.Policy interface
func (p *Policy) GetPolicyCommand() catpb.PolicyCommand { return p.command }

// AppliesToRole implements the cat.Policy interface
func (p *Policy) AppliesToRole(user username.SQLUsername) bool {
	// If no roles are specified, assume the policy applies to all users (public role).
	if p.roles == nil {
		return true
	}
	_, found := p.roles[user.Normalized()]
	return found
}
