// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"bytes"
	"fmt"
	"strings"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// ShowCreateTable returns a SHOW CREATE TABLE statement for the specified table.
// Privileges: Any privilege on table.
func (p *planner) ShowCreateTable(ctx context.Context, n *tree.ShowCreateTable) (planNode, error) {
	// We make the check whether the name points to a table or not in
	// SQL, so as to avoid a double lookup (a first one to check if the
	// descriptor is of the right type, another to populate the
	// create_statements vtable).
	const showCreateTableQuery = `
     SELECT %[3]s AS "Table",
            IFNULL(create_statement,
                   crdb_internal.force_error('` + pgerror.CodeUndefinedTableError + `',
                                             %[1]s || '.' || %[2]s || ' is not a table')::string
            ) AS "CreateTable"
       FROM (SELECT create_statement FROM %[4]s.crdb_internal.create_statements
              WHERE database_name = %[1]s AND descriptor_name = %[2]s AND descriptor_type = 'table'
              UNION ALL VALUES (NULL) ORDER BY 1 DESC) LIMIT 1
  `
	return p.showTableDetails(ctx, "SHOW CREATE TABLE", n.Table, showCreateTableQuery)
}

// ShowCreateView returns a CREATE VIEW statement for the specified view.
// Privileges: Any privilege on view.
func (p *planner) ShowCreateView(ctx context.Context, n *tree.ShowCreateView) (planNode, error) {
	// We make the check whether the name points to a view or not in
	// SQL, so as to avoid a double lookup (a first one to check if the
	// descriptor is of the right type, another to populate the
	// create_statements vtable).
	const showCreateViewQuery = `
     SELECT %[3]s AS "View",
            IFNULL(create_statement,
                   crdb_internal.force_error('` + pgerror.CodeUndefinedTableError + `',
                                             %[1]s || '.' || %[2]s || ' is not a view')::string
            ) AS "CreateView"
       FROM (SELECT create_statement FROM %[4]s.crdb_internal.create_statements
              WHERE database_name = %[1]s AND descriptor_name = %[2]s AND descriptor_type = 'view'
              UNION ALL VALUES (NULL) ORDER BY 1 DESC) LIMIT 1
  `
	return p.showTableDetails(ctx, "SHOW CREATE VIEW", n.View, showCreateViewQuery)
}

// showCreateView returns a valid SQL representation of the CREATE
// VIEW statement used to create the given view.
func (p *planner) showCreateView(
	ctx context.Context, tn tree.Name, desc *sqlbase.TableDescriptor,
) (string, error) {
	var buf bytes.Buffer
	buf.WriteString("CREATE VIEW ")
	tn.Format(&buf, tree.FmtSimple)
	buf.WriteString(" (")
	for i, col := range desc.Columns {
		if i > 0 {
			buf.WriteString(", ")
		}
		tree.Name(col.Name).Format(&buf, tree.FmtSimple)
	}
	fmt.Fprintf(&buf, ") AS %s", desc.ViewQuery)
	return buf.String(), nil
}

// showCreateTable returns a valid SQL representation of the CREATE
// TABLE statement used to create the given table.
//
// The names of the tables references by foreign keys, and the
// interleaved parent if any, are prefixed by their own database name
// unless it is equal to the given dbPrefix. This allows us to elide
// the prefix when the given table references other tables in the
// current database.
func (p *planner) showCreateTable(
	ctx context.Context, tn tree.Name, dbPrefix string, desc *sqlbase.TableDescriptor,
) (string, error) {
	a := &sqlbase.DatumAlloc{}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE TABLE %s (", tn)
	var primary string
	for i, col := range desc.VisibleColumns() {
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString("\n\t")
		buf.WriteString(col.SQLString())
		if desc.IsPhysicalTable() && desc.PrimaryIndex.ColumnIDs[0] == col.ID {
			// Only set primary if the primary key is on a visible column (not rowid).
			primary = fmt.Sprintf(",\n\tCONSTRAINT %s PRIMARY KEY (%s)",
				quoteNames(desc.PrimaryIndex.Name),
				desc.PrimaryIndex.ColNamesString(),
			)
		}
	}
	buf.WriteString(primary)
	for _, idx := range append(desc.Indexes, desc.PrimaryIndex) {
		if fk := idx.ForeignKey; fk.IsSet() {
			fkTable, err := p.session.tables.getTableVersionByID(ctx, p.txn, fk.Table)
			if err != nil {
				return "", err
			}
			fkDb, err := sqlbase.GetDatabaseDescFromID(ctx, p.txn, fkTable.ParentID)
			if err != nil {
				return "", err
			}
			fkIdx, err := fkTable.FindIndexByID(fk.Index)
			if err != nil {
				return "", err
			}
			fkTableName := tree.TableName{
				DatabaseName:            tree.Name(fkDb.Name),
				TableName:               tree.Name(fkTable.Name),
				DBNameOriginallyOmitted: fkDb.Name == dbPrefix,
			}
			fmt.Fprintf(&buf, ",\n\tCONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
				tree.Name(fk.Name),
				quoteNames(idx.ColumnNames[0:idx.ForeignKey.SharedPrefixLen]...),
				&fkTableName,
				quoteNames(fkIdx.ColumnNames...),
			)
			if fk.OnDelete != sqlbase.ForeignKeyReference_NO_ACTION {
				fmt.Fprintf(&buf, " ON DELETE %s", fk.OnDelete.String())
			}
			if fk.OnUpdate != sqlbase.ForeignKeyReference_NO_ACTION {
				fmt.Fprintf(&buf, " ON UPDATE %s", fk.OnUpdate.String())
			}
		}
		if idx.ID != desc.PrimaryIndex.ID {
			// Showing the primary index is handled above.
			fmt.Fprintf(&buf, ",\n\t%s", idx.SQLString(""))
			// Showing the INTERLEAVE and PARTITION BY for the primary index are
			// handled last.
			if err := p.showCreateInterleave(ctx, &idx, &buf, dbPrefix); err != nil {
				return "", err
			}
			if err := showCreatePartitioning(
				a, desc, &idx, &idx.Partitioning, &buf, 1 /* indent */, 0, /* colOffset */
			); err != nil {
				return "", err
			}
		}
	}

	for _, fam := range desc.Families {
		activeColumnNames := make([]string, 0, len(fam.ColumnNames))
		for i, colID := range fam.ColumnIDs {
			if _, err := desc.FindActiveColumnByID(colID); err == nil {
				activeColumnNames = append(activeColumnNames, fam.ColumnNames[i])
			}
		}
		fmt.Fprintf(&buf, ",\n\tFAMILY %s (%s)",
			quoteNames(fam.Name),
			quoteNames(activeColumnNames...),
		)
	}

	for _, e := range desc.Checks {
		fmt.Fprintf(&buf, ",\n\t")
		if len(e.Name) > 0 {
			fmt.Fprintf(&buf, "CONSTRAINT %s ", quoteNames(e.Name))
		}
		fmt.Fprintf(&buf, "CHECK (%s)", e.Expr)
	}

	buf.WriteString("\n)")

	if err := p.showCreateInterleave(ctx, &desc.PrimaryIndex, &buf, dbPrefix); err != nil {
		return "", err
	}
	if err := showCreatePartitioning(
		a, desc, &desc.PrimaryIndex, &desc.PrimaryIndex.Partitioning, &buf, 0 /* indent */, 0, /* colOffset */
	); err != nil {
		return "", err
	}

	return buf.String(), nil
}

// quoteNames quotes and adds commas between names.
func quoteNames(names ...string) string {
	nameList := make(tree.NameList, len(names))
	for i, n := range names {
		nameList[i] = tree.Name(n)
	}
	return tree.AsString(nameList)
}

// showCreateInterleave returns an INTERLEAVE IN PARENT clause for the specified
// index, if applicable.
//
// The name of the parent table is prefixed by its database name unless
// it is equal to the given dbPrefix. This allows us to elide the prefix
// when the given index is interleaved in a table of the current database.
func (p *planner) showCreateInterleave(
	ctx context.Context, idx *sqlbase.IndexDescriptor, buf *bytes.Buffer, dbPrefix string,
) error {
	if len(idx.Interleave.Ancestors) == 0 {
		return nil
	}
	intl := idx.Interleave
	parentTable, err := sqlbase.GetTableDescFromID(ctx, p.txn, intl.Ancestors[len(intl.Ancestors)-1].TableID)
	if err != nil {
		return err
	}
	parentDbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, p.txn, parentTable.ParentID)
	if err != nil {
		return err
	}
	parentName := tree.TableName{
		DatabaseName:            tree.Name(parentDbDesc.Name),
		TableName:               tree.Name(parentTable.Name),
		DBNameOriginallyOmitted: parentDbDesc.Name == dbPrefix,
	}
	var sharedPrefixLen int
	for _, ancestor := range intl.Ancestors {
		sharedPrefixLen += int(ancestor.SharedPrefixLen)
	}
	interleavedColumnNames := quoteNames(idx.ColumnNames[:sharedPrefixLen]...)
	fmt.Fprintf(buf, " INTERLEAVE IN PARENT %s (%s)", &parentName, interleavedColumnNames)
	return nil
}

// showCreatePartitioning returns a PARTITION BY clause for the specified
// index, if applicable.
func showCreatePartitioning(
	a *sqlbase.DatumAlloc,
	tableDesc *sqlbase.TableDescriptor,
	idxDesc *sqlbase.IndexDescriptor,
	partDesc *sqlbase.PartitioningDescriptor,
	buf *bytes.Buffer,
	indent int,
	colOffset int,
) error {
	if partDesc.NumColumns == 0 {
		return nil
	}

	// We don't need real prefixes in the TranslateValueEncodingToSpan calls
	// because we only use the tree.Datums part of the output.
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	indentStr := strings.Repeat("\t", indent)
	buf.WriteString(` PARTITION BY `)
	if len(partDesc.List) > 0 {
		buf.WriteString(`LIST`)
	} else if len(partDesc.Range) > 0 {
		buf.WriteString(`RANGE`)
	} else {
		return errors.Errorf(`invalid partition descriptor: %v`, partDesc)
	}
	buf.WriteString(` (`)
	for i := 0; i < int(partDesc.NumColumns); i++ {
		if i != 0 {
			fmt.Printf(", ")
		}
		fmt.Fprintf(buf, idxDesc.ColumnNames[colOffset+i])
	}
	buf.WriteString(`) (`)
	for i, part := range partDesc.List {
		if i != 0 {
			buf.WriteString(`, `)
		}
		fmt.Fprintf(buf, "\n%s\tPARTITION ", indentStr)
		fmt.Fprintf(buf, part.Name)
		buf.WriteString(` VALUES IN (`)
		for j, values := range part.Values {
			if j != 0 {
				buf.WriteString(`, `)
			}
			datums, _, err := sqlbase.TranslateValueEncodingToSpan(
				a, tableDesc, idxDesc, partDesc, values, fakePrefixDatums,
			)
			if err != nil {
				return err
			}
			buf.WriteString(`(`)
			sqlbase.PrintPartitioningTuple(buf, datums, int(partDesc.NumColumns), "DEFAULT")
			buf.WriteString(`)`)
		}
		buf.WriteString(`)`)
		if err := showCreatePartitioning(
			a, tableDesc, idxDesc, &part.Subpartitioning, buf, indent+1,
			colOffset+int(partDesc.NumColumns),
		); err != nil {
			return err
		}
	}
	for i, part := range partDesc.Range {
		if i != 0 {
			buf.WriteString(`, `)
		}
		fmt.Fprintf(buf, "\n%s\tPARTITION ", indentStr)
		fmt.Fprintf(buf, part.Name)
		buf.WriteString(` VALUES < `)
		datums, _, err := sqlbase.TranslateValueEncodingToSpan(
			a, tableDesc, idxDesc, partDesc, part.UpperBound, fakePrefixDatums,
		)
		if err != nil {
			return err
		}
		buf.WriteString(`(`)
		sqlbase.PrintPartitioningTuple(buf, datums, int(partDesc.NumColumns), "MAXVALUE")
		buf.WriteString(`)`)
	}
	fmt.Fprintf(buf, "\n%s)", indentStr)
	return nil
}
