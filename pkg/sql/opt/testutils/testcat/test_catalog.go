// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testcat

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
	testSchema Schema
	counter    int
	enumTypes  map[string]*types.T
}

type dataSource interface {
	cat.DataSource
	fqName() cat.DataSourceName
}

var _ cat.Catalog = &Catalog{}

// New creates a new empty instance of the test catalog.
func New() *Catalog {
	return &Catalog{
		testSchema: Schema{
			SchemaID: 1,
			SchemaName: cat.SchemaName{
				CatalogName:     testDB,
				SchemaName:      tree.PublicSchemaName,
				ExplicitSchema:  true,
				ExplicitCatalog: true,
			},
			dataSources: make(map[string]dataSource),
		},
	}
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
		toResolve.SchemaName = tree.PublicSchemaName
		toResolve.ExplicitCatalog = true
		return tc.resolveSchema(&toResolve)
	}

	// Neither schema or catalog was specified, so use t.public.
	toResolve.CatalogName = tree.Name(testDB)
	toResolve.SchemaName = tree.PublicSchemaName
	return tc.resolveSchema(&toResolve)
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
		toResolve.SchemaName = tree.PublicSchemaName
		toResolve.ExplicitCatalog = true
		ds, err = tc.resolveDataSource(&toResolve)
		if err == nil {
			return ds, toResolve, nil
		}
	} else {
		// This is a naked data source name. Use the current database.
		toResolve.CatalogName = tree.Name(testDB)
		toResolve.SchemaName = tree.PublicSchemaName
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

// CheckPrivilege is part of the cat.Catalog interface.
func (tc *Catalog) CheckPrivilege(ctx context.Context, o cat.Object, priv privilege.Kind) error {
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
	default:
		panic("invalid Object")
	}
	return nil
}

// HasAdminRole is part of the cat.Catalog interface.
func (tc *Catalog) HasAdminRole(ctx context.Context) (bool, error) {
	return true, nil
}

// RequireAdminRole is part of the cat.Catalog interface.
func (tc *Catalog) RequireAdminRole(ctx context.Context, action string) error {
	return nil
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

// RoleExists is part of the cat.Catalog interface.
func (tc *Catalog) RoleExists(ctx context.Context, role security.SQLUsername) (bool, error) {
	return true, nil
}

func (tc *Catalog) resolveSchema(toResolve *cat.SchemaName) (cat.Schema, cat.SchemaName, error) {
	if string(toResolve.CatalogName) != testDB {
		return nil, cat.SchemaName{}, pgerror.Newf(pgcode.InvalidSchemaName,
			"target database or schema does not exist")
	}

	if string(toResolve.SchemaName) != tree.PublicSchema {
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
	ds, _, err := tc.ResolveDataSource(context.TODO(), cat.Flags{}, name)
	if err != nil {
		panic(err)
	}
	if tab, ok := ds.(*Table); ok {
		return tab
	}
	panic(pgerror.Newf(pgcode.WrongObjectType,
		"\"%q\" is not a table", tree.ErrString(name)))
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
	ds, _, err := tc.ResolveDataSource(context.TODO(), cat.Flags{}, name)
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
	return tc.ExecuteDDLWithIndexVersion(sql, descpb.PrimaryIndexWithStoredColumnsVersion)
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

		if name.SchemaName == tree.PublicSchemaName {
			// Use the current database.
			name.CatalogName = testDB
			return
		}

		// Compatibility with CockroachDB v1.1: use D.public.T.
		name.CatalogName = name.SchemaName
		name.SchemaName = tree.PublicSchemaName
		return
	}

	// Use the current database.
	name.CatalogName = testDB
	name.SchemaName = tree.PublicSchemaName
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
func (s *Schema) PostgresDescriptorID() cat.StableID {
	return s.SchemaID
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
func (tv *View) PostgresDescriptorID() cat.StableID {
	return tv.ViewID
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

// Table implements the cat.Table interface for testing purposes.
type Table struct {
	TabID      cat.StableID
	TabVersion int
	TabName    tree.TableName
	Columns    []cat.Column
	Indexes    []*Index
	Stats      TableStats
	Checks     []cat.CheckConstraint
	Families   []*Family
	IsVirtual  bool
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
}

var _ cat.Table = &Table{}

func (tt *Table) String() string {
	tp := treeprinter.New()
	cat.FormatTable(tt.Catalog, tt, tp)
	return tp.String()
}

// ID is part of the cat.DataSource interface.
func (tt *Table) ID() cat.StableID {
	return tt.TabID
}

// PostgresDescriptorID is part of the cat.Object interface.
func (tt *Table) PostgresDescriptorID() cat.StableID {
	return tt.TabID
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
	return &zone
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
		id, err := typedesc.UserDefinedTypeOIDToID(collectedOid)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
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

	Columns []cat.IndexColumn

	// IdxZone is the zone associated with the index. This may be inherited from
	// the parent table, database, or even the default zone.
	IdxZone *zonepb.ZoneConfig

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

	// geoConfig is the geospatial index configuration, if this is a geospatial
	// inverted index. Otherwise geoConfig is nil.
	geoConfig *geoindex.Config

	// version is the index descriptor version of the index.
	version descpb.IndexDescriptorVersion
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
func (ti *Index) Ordinal() int {
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

// NonInvertedPrefixColumnCount is part of the cat.Index interface.
func (ti *Index) NonInvertedPrefixColumnCount() int {
	if !ti.IsInverted() {
		panic("not supported for non-inverted indexes")
	}
	return ti.invertedOrd
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
	return 0
}

// GeoConfig is part of the cat.Index interface.
func (ti *Index) GeoConfig() *geoindex.Config {
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
	zone   *zonepb.ZoneConfig
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

// TableStat implements the cat.TableStatistic interface for testing purposes.
type TableStat struct {
	js stats.JSONStatistic
	tt *Table
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
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	if ts.js.HistogramColumnType == "" || ts.js.HistogramBuckets == nil {
		return nil
	}
	colTypeRef, err := parser.GetTypeFromValidSQLSyntax(ts.js.HistogramColumnType)
	if err != nil {
		panic(err)
	}
	colType := tree.MustBeStaticallyKnownType(colTypeRef)

	var histogram []cat.HistogramBucket
	var offset int
	if ts.js.NullCount > 0 {
		// A bucket for NULL is not persisted, but we create a fake one to
		// make histograms easier to work with. The length of histogram
		// is therefore 1 greater than the length of ts.js.HistogramBuckets.
		// NOTE: This matches the logic in the TableStatisticsCache.
		histogram = make([]cat.HistogramBucket, len(ts.js.HistogramBuckets)+1)
		histogram[0] = cat.HistogramBucket{
			NumEq:         float64(ts.js.NullCount),
			NumRange:      0,
			DistinctRange: 0,
			UpperBound:    tree.DNull,
		}
		offset = 1
	} else {
		histogram = make([]cat.HistogramBucket, len(ts.js.HistogramBuckets))
		offset = 0
	}

	for i := offset; i < len(histogram); i++ {
		bucket := &ts.js.HistogramBuckets[i-offset]
		datum, err := rowenc.ParseDatumStringAs(colType, bucket.UpperBound, &evalCtx)
		if err != nil {
			panic(err)
		}
		histogram[i] = cat.HistogramBucket{
			NumEq:         float64(bucket.NumEq),
			NumRange:      float64(bucket.NumRange),
			DistinctRange: bucket.DistinctRange,
			UpperBound:    datum,
		}
	}
	return histogram
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
	name           string
	tabID          cat.StableID
	columnOrdinals []int
	predicate      string
	withoutIndex   bool
	validated      bool
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
func (ts *Sequence) PostgresDescriptorID() cat.StableID {
	return ts.SeqID
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
