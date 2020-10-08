// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colinfo

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// To understand DataSourceInfo below it is crucial to understand the
// meaning of a "data source" and its relationship to names/IndexedVars.
//
// A data source is an object that can deliver rows of column data,
// where each row is implemented in CockroachDB as an array of values.
// The defining property of a data source is that the columns in its
// result row arrays are always 0-indexed.
//
// From the language perspective, data sources are defined indirectly by:
// - the FROM clause in a SELECT statement;
// - JOIN clauses within the FROM clause;
// - the clause that follows INSERT INTO colName(Cols...);
// - the clause that follows UPSERT ....;
// - the invisible data source defined by the original table row during
//   UPSERT, if it exists.
//
// Most expressions (tree.Expr trees) in CockroachDB refer to a
// single data source. A notable exception is UPSERT, where expressions
// can refer to two sources: one for the values being inserted, one for
// the original row data in the table for the conflicting (already
// existing) rows.
//
// Meanwhile, IndexedVars in CockroachDB provide the interface between
// symbolic names in expressions (e.g. "f.x", called VarNames,
// or names) and data sources. During evaluation, an IndexedVar must
// resolve to a column value. For a given name there are thus two
// subsequent questions that must be answered:
//
// - which data source is the name referring to? (when there is more than 1 source)
// - which 0-indexed column in that data source is the name referring to?
//
// The IndexedVar must distinguish data sources because the same column index
// may refer to different columns in different data sources. For
// example in an UPSERT statement the IndexedVar for "excluded.x" could refer
// to column 0 in the (already existing) table row, whereas "src.x" could
// refer to column 0 in the valueNode that provides values to insert.
//
// Within this context, the infrastructure for data sources and IndexedVars
// is implemented as follows:
//
// - DataSourceInfo provides column metadata for exactly one data source;
// - the index in IndexedVars points to one of the columns in the DataSourceInfo.
// - IndexedVarResolver (select_name_resolution.go) is tasked with
//   linking back IndexedVars with their data source and column index.
//
// This being said, there is a misunderstanding one should be careful
// to avoid: *there is no direct relationship between data sources and
// table names* in SQL. In other words:
//
// - the same table name can be present in two or more data sources; for example
//   with:
//        INSERT INTO excluded VALUES (42) ON CONFLICT (x) DO UPDATE ...
//   the name "excluded" can refer either to the data source for VALUES(42)
//   or the implicit data source corresponding to the rows in the original table
//   that conflict with the new values.
//
//   When this happens, a name of the form "excluded.x" must be
//   resolved by considering all the data sources; if there is more
//   than one data source providing the table name "excluded" (as in
//   this case), the query is rejected with an ambiguity error.
//
// - a single data source may provide values for multiple table names; for
//   example with:
//         SELECT * FROM (f CROSS JOIN g) WHERE f.x = g.x
//   there is a single data source corresponding to the results of the
//   CROSS JOIN, providing a single 0-indexed array of values on each
//   result row.
//
//   (multiple table names for a single data source happen in JOINed sources
//   and JOINed sources only. Note that a FROM clause with a comma-separated
//   list of sources is a CROSS JOIN in disguise.)
//
//   When this happens, names of the form "f.x" in either WHERE,
//   SELECT renders, or other expressions which can refer to the data
//   source do not refer to the "internal" data sources of the JOIN;
//   they always refer to the final result rows of the JOIN source as
//   a whole.
//
//   This implies that a single DataSourceInfo that provides metadata
//   for a complex JOIN clause must "know" which table name is
//   associated with each column in its result set.
//

// DataSourceInfo provides column metadata for exactly one data source.
type DataSourceInfo struct {
	// SourceColumns match the plan.Columns() 1-to-1. However the column
	// names might be different if the statement renames them using AS.
	SourceColumns ResultColumns

	// SourceAlias indicates to which table the source columns belong.
	// This often corresponds to the original table names for each column but
	// might be different if the statement renames them using AS.
	SourceAlias tree.TableName
}

func (src *DataSourceInfo) String() string {
	var buf bytes.Buffer
	for i := range src.SourceColumns {
		if i > 0 {
			buf.WriteByte('\t')
		}
		fmt.Fprintf(&buf, "%d", i)
	}
	buf.WriteString("\toutput column positions\n")
	for i, c := range src.SourceColumns {
		if i > 0 {
			buf.WriteByte('\t')
		}
		if c.Hidden {
			buf.WriteByte('*')
		}
		buf.WriteString(c.Name)
	}
	buf.WriteString("\toutput column names\n")
	if src.SourceAlias == descpb.AnonymousTable {
		buf.WriteString("\t<anonymous table>\n")
	} else {
		fmt.Fprintf(&buf, "\t'%s'\n", src.SourceAlias.String())
	}
	return buf.String()
}

// NewSourceInfoForSingleTable creates a simple DataSourceInfo
// which maps the same tableAlias to all columns.
func NewSourceInfoForSingleTable(tn tree.TableName, columns ResultColumns) *DataSourceInfo {
	if tn.ObjectName != "" && tn.SchemaName != "" {
		// When we're not looking at an unqualified table, we make sure that
		// the table name in the data source struct is fully qualified. This
		// ensures that queries like this are valid:
		//
		// select "".information_schema.schemata.schema_name from  "".information_schema.schemata
		tn.ExplicitCatalog = true
		tn.ExplicitSchema = true
	}
	return &DataSourceInfo{
		SourceColumns: columns,
		SourceAlias:   tn,
	}
}

type varFormatter struct {
	TableName  tree.TableName
	ColumnName tree.Name
}

// Format implements the NodeFormatter interface.
func (c *varFormatter) Format(ctx *tree.FmtCtx) {
	if ctx.HasFlags(tree.FmtShowTableAliases) && c.TableName.ObjectName != "" {
		// This logic is different from (*tree.TableName).Format() with
		// FmtAlwaysQualify, because FmtShowTableAliases only wants to
		// prefixes the table names for vars in expressions, not table
		// names in sub-queries.
		if c.TableName.SchemaName != "" {
			if c.TableName.CatalogName != "" {
				ctx.FormatNode(&c.TableName.CatalogName)
				ctx.WriteByte('.')
			}
			ctx.FormatNode(&c.TableName.SchemaName)
			ctx.WriteByte('.')
		}

		ctx.FormatNode(&c.TableName.ObjectName)
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&c.ColumnName)
}

// NodeFormatter returns a tree.NodeFormatter that, when formatted,
// represents the object at the input column index.
func (src *DataSourceInfo) NodeFormatter(colIdx int) tree.NodeFormatter {
	return &varFormatter{
		TableName:  src.SourceAlias,
		ColumnName: tree.Name(src.SourceColumns[colIdx].Name),
	}
}
