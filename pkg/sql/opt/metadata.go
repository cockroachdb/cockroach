// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opt

import (
	"context"
	"fmt"
	"math/bits"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// SchemaID uniquely identifies the usage of a schema within the scope of a
// query. SchemaID 0 is reserved to mean "unknown schema". Internally, the
// SchemaID consists of an index into the Metadata.schemas slice.
//
// See the comment for Metadata for more details on identifiers.
type SchemaID int32

// privilegeBitmap stores a union of zero or more privileges. Each privilege
// that is present in the bitmap is represented by a bit that is shifted by
// 1 << privilege.Kind, so that multiple privileges can be stored.
type privilegeBitmap uint32

// Metadata assigns unique ids to the columns, tables, and other metadata used
// for global identification within the scope of a particular query. These ids
// tend to be small integers that can be efficiently stored and manipulated.
//
// Within a query, every unique column and every projection should be assigned a
// unique column id. Additionally, every separate reference to a table in the
// query should get a new set of output column ids.
//
// For example, consider the query:
//
//   SELECT x FROM a WHERE y > 0
//
// There are 2 columns in the above query: x and y. During name resolution, the
// above query becomes:
//
//   SELECT [0] FROM a WHERE [1] > 0
//   -- [0] -> x
//   -- [1] -> y
//
// An operator is allowed to reuse some or all of the column ids of an input if:
//
// 1. For every output row, there exists at least one input row having identical
//    values for those columns.
// 2. OR if no such input row exists, there is at least one output row having
//    NULL values for all those columns (e.g. when outer join NULL-extends).
//
// For example, is it safe for a Select to use its input's column ids because it
// only filters rows. Likewise, pass-through column ids of a Project can be
// reused.
//
// For an example where columns cannot be reused, consider the query:
//
//   SELECT * FROM a AS l JOIN a AS r ON (l.x = r.y)
//
// In this query, `l.x` is not equivalent to `r.x` and `l.y` is not equivalent
// to `r.y`. Therefore, we need to give these columns different ids.
type Metadata struct {
	// schemas stores each schema used in the query, indexed by SchemaID.
	schemas []cat.Schema

	// cols stores information about each metadata column, indexed by
	// ColumnID.index().
	cols []ColumnMeta

	// tables stores information about each metadata table, indexed by
	// TableID.index().
	tables []TableMeta

	// sequences stores information about each metadata sequence, indexed by SequenceID.
	sequences []cat.Sequence

	// userDefinedTypes contains all user defined types present in expressions
	// in this query.
	// TODO (rohany): This only contains user defined types present in the query
	//  because the installation of type metadata in tables doesn't go through
	//  the type resolver that the optimizer hijacks. However, we could update
	//  this map when adding a table via metadata.AddTable.
	userDefinedTypes      map[oid.Oid]struct{}
	userDefinedTypesSlice []*types.T

	// deps stores information about all data source objects depended on by the
	// query, as well as the privileges required to access them. The objects are
	// deduplicated: any name/object pair shows up at most once.
	// Note: the same data source object can appear multiple times if different
	// names were used. For example, in the query `SELECT * from t, db.t` the two
	// tables might resolve to the same object now but to different objects later;
	// we want to verify the resolution of both names.
	deps []mdDep

	// views stores the list of referenced views. This information is only
	// needed for EXPLAIN (opt, env).
	views []cat.View

	// currUniqueID is the highest UniqueID that has been assigned.
	currUniqueID UniqueID

	// withBindings store bindings for relational expressions inside With or
	// mutation operators, used to determine the logical properties of WithScan.
	withBindings map[WithID]Expr

	// NOTE! When adding fields here, update Init (if reusing allocated
	// data structures is desired), CopyFrom and TestMetadata.
}

type mdDep struct {
	ds cat.DataSource

	name MDDepName

	// privileges is the union of all required privileges.
	privileges privilegeBitmap
}

// MDDepName stores either the unresolved DataSourceName or the StableID from
// the query that was used to resolve a data source.
type MDDepName struct {
	// byID is non-zero if and only if the data source was looked up using the
	// StableID.
	byID cat.StableID

	// byName is non-zero if and only if the data source was looked up using a
	// name.
	byName cat.DataSourceName
}

func (n *MDDepName) equals(other *MDDepName) bool {
	return n.byID == other.byID && n.byName.Equals(&other.byName)
}

// Init prepares the metadata for use (or reuse).
func (md *Metadata) Init() {
	// Clear the metadata objects to release memory (this clearing pattern is
	// optimized by Go).
	schemas := md.schemas
	for i := range schemas {
		schemas[i] = nil
	}

	cols := md.cols
	for i := range cols {
		cols[i] = ColumnMeta{}
	}

	tables := md.tables
	for i := range tables {
		tables[i] = TableMeta{}
	}

	sequences := md.sequences
	for i := range sequences {
		sequences[i] = nil
	}

	deps := md.deps
	for i := range deps {
		deps[i] = mdDep{}
	}

	views := md.views
	for i := range views {
		views[i] = nil
	}

	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*md = Metadata{}
	md.schemas = schemas[:0]
	md.cols = cols[:0]
	md.tables = tables[:0]
	md.sequences = sequences[:0]
	md.deps = deps[:0]
	md.views = views[:0]
}

// CopyFrom initializes the metadata with a copy of the provided metadata.
// This metadata can then be modified independent of the copied metadata.
//
// Table annotations are not transferred over; all annotations are unset on
// the copy.
//
// copyScalarFn must be a function that returns a copy of the given scalar
// expression.
func (md *Metadata) CopyFrom(from *Metadata, copyScalarFn func(Expr) Expr) {
	if len(md.schemas) != 0 || len(md.cols) != 0 || len(md.tables) != 0 ||
		len(md.sequences) != 0 || len(md.deps) != 0 || len(md.views) != 0 ||
		len(md.userDefinedTypes) != 0 || len(md.userDefinedTypesSlice) != 0 {
		panic(errors.AssertionFailedf("CopyFrom requires empty destination"))
	}
	md.schemas = append(md.schemas, from.schemas...)
	md.cols = append(md.cols, from.cols...)

	if len(from.userDefinedTypesSlice) > 0 {
		if md.userDefinedTypes == nil {
			md.userDefinedTypes = make(map[oid.Oid]struct{}, len(from.userDefinedTypesSlice))
		}
		for i := range from.userDefinedTypesSlice {
			typ := from.userDefinedTypesSlice[i]
			md.userDefinedTypes[typ.Oid()] = struct{}{}
			md.userDefinedTypesSlice = append(md.userDefinedTypesSlice, typ)
		}
	}

	if cap(md.tables) >= len(from.tables) {
		md.tables = md.tables[:len(from.tables)]
	} else {
		md.tables = make([]TableMeta, len(from.tables))
	}
	for i := range from.tables {
		// Note: annotations inside TableMeta are not retained.
		md.tables[i].copyFrom(&from.tables[i], copyScalarFn)
	}

	md.sequences = append(md.sequences, from.sequences...)
	md.deps = append(md.deps, from.deps...)
	md.views = append(md.views, from.views...)
	md.currUniqueID = from.currUniqueID

	// We cannot copy the bound expressions; they must be rebuilt in the new memo.
	md.withBindings = nil
}

// DepByName is used with AddDependency when the data source was looked up using a
// data source name.
func DepByName(name *cat.DataSourceName) MDDepName {
	return MDDepName{byName: *name}
}

// DepByID is used with AddDependency when the data source was looked up by ID.
func DepByID(id cat.StableID) MDDepName {
	return MDDepName{byID: id}
}

// AddDependency tracks one of the catalog data sources on which the query
// depends, as well as the privilege required to access that data source. If
// the Memo using this metadata is cached, then a call to CheckDependencies can
// detect if the name resolves to a different data source now, or if changes to
// schema or permissions on the data source has invalidated the cached metadata.
func (md *Metadata) AddDependency(name MDDepName, ds cat.DataSource, priv privilege.Kind) {
	// Search for the same name / object pair.
	for i := range md.deps {
		if md.deps[i].ds == ds && md.deps[i].name.equals(&name) {
			md.deps[i].privileges |= (1 << priv)
			return
		}
	}
	md.deps = append(md.deps, mdDep{
		ds:         ds,
		name:       name,
		privileges: (1 << priv),
	})
}

// CheckDependencies resolves (again) each data source on which this metadata
// depends, in order to check that all data source names resolve to the same
// objects, and that the user still has sufficient privileges to access the
// objects. If the dependencies are no longer up-to-date, then CheckDependencies
// returns false.
//
// This function cannot swallow errors and return only a boolean, as it may
// perform KV operations on behalf of the transaction associated with the
// provided catalog, and those errors are required to be propagated.
func (md *Metadata) CheckDependencies(
	ctx context.Context, catalog cat.Catalog,
) (upToDate bool, err error) {
	for i := range md.deps {
		name := &md.deps[i].name
		var toCheck cat.DataSource
		var err error
		if name.byID != 0 {
			toCheck, _, err = catalog.ResolveDataSourceByID(ctx, cat.Flags{}, name.byID)
		} else {
			// Resolve data source object.
			toCheck, _, err = catalog.ResolveDataSource(ctx, cat.Flags{}, &name.byName)
		}
		if err != nil {
			return false, err
		}

		// Ensure that it's the same object, and there were no schema or table
		// statistics changes.
		if !toCheck.Equals(md.deps[i].ds) {
			return false, nil
		}

		for privs := md.deps[i].privileges; privs != 0; {
			// Strip off each privilege bit and make call to CheckPrivilege for it.
			// Note that priv == 0 can occur when a dependency was added with
			// privilege.Kind = 0 (e.g. for a table within a view, where the table
			// privileges do not need to be checked). Ignore the "zero privilege".
			priv := privilege.Kind(bits.TrailingZeros32(uint32(privs)))
			if priv != 0 {
				if err := catalog.CheckPrivilege(ctx, toCheck, priv); err != nil {
					return false, err
				}
			}

			// Set the just-handled privilege bit to zero and look for next.
			privs &= ^(1 << priv)
		}
	}
	// Check that all of the user defined types present have not changed.
	for _, typ := range md.AllUserDefinedTypes() {
		toCheck, err := catalog.ResolveTypeByOID(ctx, typ.Oid())
		if err != nil {
			// Handle when the type no longer exists.
			if pgerror.GetPGCode(err) == pgcode.UndefinedObject {
				return false, nil
			}
			return false, err
		}
		if typ.TypeMeta.Version != toCheck.TypeMeta.Version {
			return false, nil
		}
	}
	return true, nil
}

// AddSchema indexes a new reference to a schema used by the query.
func (md *Metadata) AddSchema(sch cat.Schema) SchemaID {
	md.schemas = append(md.schemas, sch)
	return SchemaID(len(md.schemas))
}

// Schema looks up the metadata for the schema associated with the given schema
// id.
func (md *Metadata) Schema(schID SchemaID) cat.Schema {
	return md.schemas[schID-1]
}

// AddUserDefinedType adds a user defined type to the metadata for this query.
func (md *Metadata) AddUserDefinedType(typ *types.T) {
	if !typ.UserDefined() {
		return
	}
	if md.userDefinedTypes == nil {
		md.userDefinedTypes = make(map[oid.Oid]struct{})
	}
	if _, ok := md.userDefinedTypes[typ.Oid()]; !ok {
		md.userDefinedTypes[typ.Oid()] = struct{}{}
		md.userDefinedTypesSlice = append(md.userDefinedTypesSlice, typ)
	}
}

// AllUserDefinedTypes returns all user defined types contained in this query.
func (md *Metadata) AllUserDefinedTypes() []*types.T {
	return md.userDefinedTypesSlice
}

// AddTable indexes a new reference to a table within the query. Separate
// references to the same table are assigned different table ids (e.g.  in a
// self-join query). All columns are added to the metadata. If mutation columns
// are present, they are added after active columns.
//
// The ExplicitCatalog/ExplicitSchema fields of the table's alias are honored so
// that its original formatting is preserved for error messages,
// pretty-printing, etc.
func (md *Metadata) AddTable(tab cat.Table, alias *tree.TableName) TableID {
	tabID := makeTableID(len(md.tables), ColumnID(len(md.cols)+1))
	if md.tables == nil {
		md.tables = make([]TableMeta, 0, 4)
	}
	md.tables = append(md.tables, TableMeta{MetaID: tabID, Table: tab, Alias: *alias})

	colCount := tab.ColumnCount()
	if md.cols == nil {
		md.cols = make([]ColumnMeta, 0, colCount)
	}

	for i := 0; i < colCount; i++ {
		col := tab.Column(i)
		colID := md.AddColumn(string(col.ColName()), col.DatumType())
		md.ColumnMeta(colID).Table = tabID
	}

	return tabID
}

// DuplicateTable creates a new reference to the table with the given ID. All
// columns are added to the metadata with new column IDs. If mutation columns
// are present, they are added after active columns. The ID of the new table
// reference is returned. This function panics if a table with the given ID does
// not exists in the metadata.
//
// remapColumnIDs must be a function that remaps the column IDs within a
// ScalarExpr to new column IDs. It takes as arguments a ScalarExpr and a
// mapping of old column IDs to new column IDs, and returns a new ScalarExpr.
// This function is used when duplicating Constraints, ComputedCols, and
// partialIndexPredicates. DuplicateTable requires this callback function,
// rather than performing the remapping itself, because remapping column IDs
// requires constructing new expressions with norm.Factory. The norm package
// depends on opt, and cannot be imported here.
//
// The ExplicitCatalog/ExplicitSchema fields of the table's alias are honored so
// that its original formatting is preserved for error messages,
// pretty-printing, etc.
func (md *Metadata) DuplicateTable(
	tabID TableID, remapColumnIDs func(e ScalarExpr, colMap ColMap) ScalarExpr,
) TableID {
	if md.tables == nil || tabID.index() >= len(md.tables) {
		panic(errors.AssertionFailedf("table with ID %d does not exist", tabID))
	}

	tabMeta := md.TableMeta(tabID)
	tab := tabMeta.Table
	newTabID := makeTableID(len(md.tables), ColumnID(len(md.cols)+1))

	// Generate new column IDs for each column in the table, and keep track of
	// a mapping from the original TableMeta's column IDs to the new ones.
	var colMap ColMap
	for i, n := 0, tab.ColumnCount(); i < n; i++ {
		col := tab.Column(i)
		oldColID := tabID.ColumnID(i)
		newColID := md.AddColumn(string(col.ColName()), col.DatumType())
		md.ColumnMeta(newColID).Table = newTabID
		colMap.Set(int(oldColID), int(newColID))
	}

	// Create new constraints by remapping the column IDs to the new TableMeta's
	// column IDs.
	var constraints ScalarExpr
	if tabMeta.Constraints != nil {
		constraints = remapColumnIDs(tabMeta.Constraints, colMap)
	}

	// Create new computed column expressions by remapping the column IDs in
	// each ScalarExpr.
	var computedCols map[ColumnID]ScalarExpr
	if len(tabMeta.ComputedCols) > 0 {
		computedCols = make(map[ColumnID]ScalarExpr, len(tabMeta.ComputedCols))
		for colID, e := range tabMeta.ComputedCols {
			newColID, ok := colMap.Get(int(colID))
			if !ok {
				panic(errors.AssertionFailedf("column with ID %d does not exist in map", colID))
			}
			computedCols[ColumnID(newColID)] = remapColumnIDs(e, colMap)
		}
	}

	// Create new partial index predicate expressions by remapping the column
	// IDs in each ScalarExpr.
	var partialIndexPredicates map[cat.IndexOrdinal]ScalarExpr
	if len(tabMeta.partialIndexPredicates) > 0 {
		partialIndexPredicates = make(map[cat.IndexOrdinal]ScalarExpr, len(tabMeta.partialIndexPredicates))
		for idxOrd, e := range tabMeta.partialIndexPredicates {
			partialIndexPredicates[idxOrd] = remapColumnIDs(e, colMap)
		}
	}

	md.tables = append(md.tables, TableMeta{
		MetaID:                 newTabID,
		Table:                  tabMeta.Table,
		Alias:                  tabMeta.Alias,
		IgnoreForeignKeys:      tabMeta.IgnoreForeignKeys,
		Constraints:            constraints,
		ComputedCols:           computedCols,
		partialIndexPredicates: partialIndexPredicates,
	})

	return newTabID
}

// TableMeta looks up the metadata for the table associated with the given table
// id. The same table can be added multiple times to the query metadata and
// associated with multiple table ids.
func (md *Metadata) TableMeta(tabID TableID) *TableMeta {
	return &md.tables[tabID.index()]
}

// Table looks up the catalog table associated with the given metadata id. The
// same table can be associated with multiple metadata ids.
func (md *Metadata) Table(tabID TableID) cat.Table {
	return md.TableMeta(tabID).Table
}

// AllTables returns the metadata for all tables. The result must not be
// modified.
func (md *Metadata) AllTables() []TableMeta {
	return md.tables
}

// AddColumn assigns a new unique id to a column within the query and records
// its alias and type. If the alias is empty, a "column<ID>" alias is created.
func (md *Metadata) AddColumn(alias string, typ *types.T) ColumnID {
	if alias == "" {
		alias = fmt.Sprintf("column%d", len(md.cols)+1)
	}
	colID := ColumnID(len(md.cols) + 1)
	md.cols = append(md.cols, ColumnMeta{MetaID: colID, Alias: alias, Type: typ})
	return colID
}

// NumColumns returns the count of columns tracked by this Metadata instance.
func (md *Metadata) NumColumns() int {
	return len(md.cols)
}

// ColumnMeta looks up the metadata for the column associated with the given
// column id. The same column can be added multiple times to the query metadata
// and associated with multiple column ids.
func (md *Metadata) ColumnMeta(colID ColumnID) *ColumnMeta {
	return &md.cols[colID.index()]
}

// QualifiedAlias returns the column alias, possibly qualified with the table,
// schema, or database name:
//
//   1. If fullyQualify is true, then the returned alias is prefixed by the
//      original, fully qualified name of the table: tab.Name().FQString().
//
//   2. If there's another column in the metadata with the same column alias but
//      a different table name, then prefix the column alias with the table
//      name: "tabName.columnAlias".
//
func (md *Metadata) QualifiedAlias(colID ColumnID, fullyQualify bool, catalog cat.Catalog) string {
	cm := md.ColumnMeta(colID)
	if cm.Table == 0 {
		// Column doesn't belong to a table, so no need to qualify it further.
		return cm.Alias
	}

	// If a fully qualified alias has not been requested, then only qualify it if
	// it would otherwise be ambiguous.
	var tabAlias tree.TableName
	qualify := fullyQualify
	if !fullyQualify {
		for i := range md.cols {
			if i == int(cm.MetaID-1) {
				continue
			}

			// If there are two columns with same alias, then column is ambiguous.
			cm2 := &md.cols[i]
			if cm2.Alias == cm.Alias {
				tabAlias = md.TableMeta(cm.Table).Alias
				if cm2.Table == 0 {
					qualify = true
				} else {
					// Only qualify if the qualified names are actually different.
					tabAlias2 := md.TableMeta(cm2.Table).Alias
					if tabAlias.String() != tabAlias2.String() {
						qualify = true
					}
				}
			}
		}
	}

	// If the column name should not even be partly qualified, then no more to do.
	if !qualify {
		return cm.Alias
	}

	var sb strings.Builder
	if fullyQualify {
		tn, err := catalog.FullyQualifiedName(context.TODO(), md.TableMeta(cm.Table).Table)
		if err != nil {
			panic(err)
		}
		sb.WriteString(tn.FQString())
	} else {
		sb.WriteString(tabAlias.String())
	}
	sb.WriteRune('.')
	sb.WriteString(cm.Alias)
	return sb.String()
}

// SequenceID uniquely identifies the usage of a sequence within the scope of a
// query. SequenceID 0 is reserved to mean "unknown sequence".
type SequenceID uint64

// index returns the index of the sequence in Metadata.sequences. It's biased by 1, so
// that SequenceID 0 can be be reserved to mean "unknown sequence".
func (s SequenceID) index() int {
	return int(s - 1)
}

// makeSequenceID constructs a new SequenceID from its component parts.
func makeSequenceID(index int) SequenceID {
	// Bias the sequence index by 1.
	return SequenceID(index + 1)
}

// AddSequence adds the sequence to the metadata, returning a SequenceID that
// can be used to retrieve it.
func (md *Metadata) AddSequence(seq cat.Sequence) SequenceID {
	seqID := makeSequenceID(len(md.sequences))
	if md.sequences == nil {
		md.sequences = make([]cat.Sequence, 0, 4)
	}
	md.sequences = append(md.sequences, seq)

	return seqID
}

// Sequence looks up the catalog sequence associated with the given metadata id. The
// same sequence can be associated with multiple metadata ids.
func (md *Metadata) Sequence(seqID SequenceID) cat.Sequence {
	return md.sequences[seqID.index()]
}

// UniqueID should be used to disambiguate multiple uses of an expression
// within the scope of a query. For example, a UniqueID field should be
// added to an expression type if two instances of that type might otherwise
// be indistinguishable based on the values of their other fields.
//
// See the comment for Metadata for more details on identifiers.
type UniqueID uint64

// NextUniqueID returns a fresh UniqueID which is guaranteed to never have been
// previously allocated in this memo.
func (md *Metadata) NextUniqueID() UniqueID {
	md.currUniqueID++
	return md.currUniqueID
}

// AddView adds a new reference to a view used by the query.
func (md *Metadata) AddView(v cat.View) {
	md.views = append(md.views, v)
}

// AllViews returns the metadata for all views. The result must not be
// modified.
func (md *Metadata) AllViews() []cat.View {
	return md.views
}

// AllDataSourceNames returns the fully qualified names of all datasources
// referenced by the metadata.
func (md *Metadata) AllDataSourceNames(
	fullyQualifiedName func(ds cat.DataSource) (cat.DataSourceName, error),
) (tables, sequences, views []tree.TableName, _ error) {
	// Catalog objects can show up multiple times in our lists, so deduplicate
	// them.
	seen := make(map[tree.TableName]struct{})

	getNames := func(count int, get func(int) cat.DataSource) ([]tree.TableName, error) {
		result := make([]tree.TableName, 0, count)
		for i := 0; i < count; i++ {
			ds := get(i)
			tn, err := fullyQualifiedName(ds)
			if err != nil {
				return nil, err
			}
			if _, ok := seen[tn]; !ok {
				seen[tn] = struct{}{}
				result = append(result, tn)
			}
		}
		return result, nil
	}
	var err error
	tables, err = getNames(len(md.tables), func(i int) cat.DataSource {
		return md.tables[i].Table
	})
	if err != nil {
		return nil, nil, nil, err
	}
	sequences, err = getNames(len(md.sequences), func(i int) cat.DataSource {
		return md.sequences[i]
	})
	if err != nil {
		return nil, nil, nil, err
	}
	views, err = getNames(len(md.views), func(i int) cat.DataSource {
		return md.views[i]
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return tables, sequences, views, nil
}

// WithID uniquely identifies a With expression within the scope of a query.
// WithID=0 is reserved to mean "unknown expression".
// See the comment for Metadata for more details on identifiers.
type WithID uint64

// AddWithBinding associates a WithID to its bound expression.
func (md *Metadata) AddWithBinding(id WithID, expr Expr) {
	if md.withBindings == nil {
		md.withBindings = make(map[WithID]Expr)
	}
	md.withBindings[id] = expr
}

// WithBinding returns the bound expression for the given WithID.
// Panics with an assertion error if there is none.
func (md *Metadata) WithBinding(id WithID) Expr {
	res, ok := md.withBindings[id]
	if !ok {
		panic(errors.AssertionFailedf("no binding for WithID %d", id))
	}
	return res
}
