// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package opt

import (
	"context"
	"fmt"
	"math/bits"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
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
// within the scope of a particular query. Because it is specific to one query,
// the ids tend to be small integers that can be efficiently stored and
// manipulated.
//
// Within a query, every unique column and every projection (that is more than
// just a pass through of a column) is assigned a unique column id.
// Additionally, every separate reference to a table in the query gets a new
// set of output column ids. Consider the query:
//
//   SELECT * FROM a AS l JOIN a AS r ON (l.x = r.y)
//
// In this query, `l.x` is not equivalent to `r.x` and `l.y` is not equivalent
// to `r.y`. In order to achieve this, we need to give these columns different
// ids.
//
// In all cases, the column ids are global to the query. For example, consider
// the query:
//
//   SELECT x FROM a WHERE y > 0
//
// There are 2 columns in the above query: x and y. During name resolution, the
// above query becomes:
//
//   SELECT [0] FROM a WHERE [1] > 0
//   -- [0] -> x
//   -- [1] -> y
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

	// values is the highest id for a Values clause that has been assigned.
	values ValuesID

	// deps stores information about all catalog objects depended on by the query,
	// as well as the privileges required to access those objects. The objects are
	// deduplicated: any name/object pair shows up at most once.
	// Note: the same object can appear multiple times if different names were
	// used. For example, in the query `SELECT * from t, db.t` the two tables
	// might resolve to the same object now but to different objects later; we
	// want to verify the resolution of both names.
	deps []mdDep

	// views stores the list of referenced views. This information is only
	// needed for EXPLAIN (opt, env).
	views []cat.View

	// NOTE! When adding fields here, update CopyFrom.
}

type mdDep struct {
	object cat.Object

	// name is the unresolved name from the query that was used to resolve the
	// object. It stores either a cat.DataSourceName, or cat.SchemaName in its
	// TablePrefix portion, depending on the type of the object.
	name tree.TableName

	// privileges is the union of all required privileges.
	privileges privilegeBitmap
}

func (d *mdDep) dsName() *cat.DataSourceName {
	return &d.name
}

func (d *mdDep) schemaName() *cat.SchemaName {
	return &d.name.TableNamePrefix
}

// Init prepares the metadata for use (or reuse).
func (md *Metadata) Init() {
	// Clear the metadata objects to release memory (this clearing pattern is
	// optimized by Go).
	for i := range md.schemas {
		md.schemas[i] = nil
	}
	for i := range md.cols {
		md.cols[i] = ColumnMeta{}
	}
	for i := range md.tables {
		md.tables[i] = TableMeta{}
	}
	md.schemas = md.schemas[:0]
	md.cols = md.cols[:0]
	md.tables = md.tables[:0]
	md.views = md.views[:0]
	md.deps = md.deps[:0]
}

// CopyFrom initializes the metadata with a copy of the provided metadata.
// This metadata can then be modified independent of the copied metadata.
//
// Table annotations are not transferred over; all annotations are unset on
// the copy.
func (md *Metadata) CopyFrom(from *Metadata) {
	if len(md.schemas) != 0 || len(md.cols) != 0 || len(md.tables) != 0 ||
		len(md.sequences) != 0 || len(md.deps) != 0 || len(md.views) != 0 {
		panic(errors.AssertionFailedf("CopyFrom requires empty destination"))
	}
	md.schemas = append(md.schemas, from.schemas...)
	md.cols = append(md.cols, from.cols...)
	md.tables = append(md.tables, from.tables...)
	md.views = append(md.views, from.views...)

	// Clear table annotations. These objects can be mutable and can't be safely
	// shared between different metadata instances.
	for i := range md.tables {
		md.tables[i].clearAnnotations()
	}

	md.sequences = append(md.sequences, from.sequences...)
	md.deps = append(md.deps, from.deps...)
}

// AddDataSourceDependency tracks one of the catalog data sources on which the
// query depends, as well as the privilege required to access that data source.
// If the Memo using this metadata is cached, then a call to CheckDependencies
// can detect if the name resolves to a different data source now, or if changes
// to schema or permissions on the data source has invalidated the cached metadata.
func (md *Metadata) AddDataSourceDependency(
	name *cat.DataSourceName, ds cat.DataSource, priv privilege.Kind,
) {
	// Search for the same name / object pair.
	for i := range md.deps {
		if md.deps[i].object == ds && md.deps[i].dsName().Equals(name) {
			md.deps[i].privileges |= (1 << priv)
			return
		}
	}
	md.deps = append(md.deps, mdDep{
		object:     ds,
		name:       *name,
		privileges: (1 << priv),
	})
}

// AddSchemaDependency tracks one of the catalog schemas on which the query depends,
// as well as the privilege required to access that schema. If the Memo using
// this metadata is cached, then a call to CheckDependencies can detect if
// the name resolves to a different schema, or if the permissions on the schema
// are no longer sufficient.
func (md *Metadata) AddSchemaDependency(name *cat.SchemaName, s cat.Schema, priv privilege.Kind) {
	// Search for the same name / object pair.
	for i := range md.deps {
		if md.deps[i].object == s && md.deps[i].schemaName().Equals(name) {
			md.deps[i].privileges |= (1 << priv)
			return
		}
	}
	d := mdDep{object: s, privileges: (1 << priv)}
	d.name.TableNamePrefix = *name
	md.deps = append(md.deps, d)
}

// CheckDependencies resolves each data source and schema on which this metadata
// depends, in order to check that the fully qualified object names still
// resolve to the same version of the same objects, and that the user still has
// sufficient privileges to access the objects. If the dependencies are no
// longer up-to-date, then CheckDependencies returns false.
//
// This function cannot swallow errors and return only a boolean, as it may
// perform KV operations on behalf of the transaction associated with the
// provided catalog, and those errors are required to be propagated.
func (md *Metadata) CheckDependencies(
	ctx context.Context, catalog cat.Catalog,
) (upToDate bool, err error) {
	for i := range md.deps {
		obj := md.deps[i].object
		var toCheck cat.Object
		switch obj.(type) {
		case cat.DataSource:
			name := *md.deps[i].dsName()
			// Resolve data source object.
			new, _, err := catalog.ResolveDataSource(ctx, cat.Flags{}, &name)
			if err != nil {
				return false, err
			}
			toCheck = new

		case cat.Schema:
			name := *md.deps[i].schemaName()
			// Resolve schema object.
			new, _, err := catalog.ResolveSchema(ctx, cat.Flags{}, &name)
			if err != nil {
				return false, err
			}
			toCheck = new

		default:
			return false, errors.AssertionFailedf("unknown dependency type: %v", obj)
		}

		// Ensure that it's the same object, and there were no schema or table
		// statistics changes.
		if !toCheck.Equals(obj) {
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

// AddTable is a helper that calls AddTableWithAlias with tab.Name() as the
// table's alias.
func (md *Metadata) AddTable(tab cat.Table) TableID {
	return md.AddTableWithAlias(tab, tab.Name())
}

// AddTableWithAlias indexes a new reference to a table within the query.
// Separate references to the same table are assigned different table ids (e.g.
// in a self-join query). All columns are added to the metadata. If mutation
// columns are present, they are added after active columns. The table's alias
// is passed separately so that its original formatting is preserved for error
// messages, pretty-printing, etc.
func (md *Metadata) AddTableWithAlias(tab cat.Table, alias *tree.TableName) TableID {
	tabID := makeTableID(len(md.tables), ColumnID(len(md.cols)+1))
	if md.tables == nil {
		md.tables = make([]TableMeta, 0, 4)
	}
	md.tables = append(md.tables, TableMeta{MetaID: tabID, Table: tab, Alias: *alias})

	colCount := tab.DeletableColumnCount()
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

// TableByStableID looks up the catalog table associated with the given
// StableID (unique across all tables and stable across queries).
func (md *Metadata) TableByStableID(id cat.StableID) cat.Table {
	for _, mdTab := range md.tables {
		if mdTab.Table.ID() == id {
			return mdTab.Table
		}
	}
	return nil
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
func (md *Metadata) QualifiedAlias(colID ColumnID, fullyQualify bool) string {
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
		s := md.TableMeta(cm.Table).Table.Name().FQString()
		sb.WriteString(s)
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

// AllSequences returns the metadata for all sequences. The result must not be
// modified.
func (md *Metadata) AllSequences() []cat.Sequence {
	return md.sequences
}

// ValuesID uniquely identifies the usage of a values clause within the scope of a
// query.
//
// See the comment for Metadata for more details on identifiers.
type ValuesID uint64

// NextValuesID returns a fresh ValuesID which is guaranteed to never have been
// allocated prior in this memo.
func (md *Metadata) NextValuesID() ValuesID {
	md.values++
	return md.values
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
