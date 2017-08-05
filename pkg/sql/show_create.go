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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// showCreateView returns a valid SQL representation of the CREATE
// VIEW statement used to create the given view.
func (p *planner) showCreateView(
	ctx context.Context, tn parser.Name, desc *sqlbase.TableDescriptor,
) (string, error) {
	var buf bytes.Buffer
	buf.WriteString("CREATE VIEW ")
	tn.Format(&buf, parser.FmtSimple)
	buf.WriteString(" (")
	for i, col := range desc.Columns {
		if i > 0 {
			buf.WriteString(", ")
		}
		parser.Name(col.Name).Format(&buf, parser.FmtSimple)
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
	ctx context.Context, tn parser.Name, dbPrefix string, desc *sqlbase.TableDescriptor,
) (string, error) {
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
	for _, idx := range desc.Indexes {
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
			fkTableName := parser.TableName{
				DatabaseName:            parser.Name(fkDb.Name),
				TableName:               parser.Name(fkTable.Name),
				DBNameOriginallyOmitted: fkDb.Name == dbPrefix,
			}
			fmt.Fprintf(&buf, ",\n\tCONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
				parser.Name(fk.Name),
				quoteNames(idx.ColumnNames...),
				&fkTableName,
				quoteNames(fkIdx.ColumnNames...),
			)
		}
		fmt.Fprintf(&buf, ",\n\t%s", idx.SQLString(""))
		if err := p.showCreateInterleave(ctx, &idx, &buf, dbPrefix); err != nil {
			return "", err
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

	return buf.String(), nil
}

// quoteNames quotes and adds commas between names.
func quoteNames(names ...string) string {
	nameList := make(parser.NameList, len(names))
	for i, n := range names {
		nameList[i] = parser.Name(n)
	}
	return parser.AsString(nameList)
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
	parentName := parser.TableName{
		DatabaseName:            parser.Name(parentDbDesc.Name),
		TableName:               parser.Name(parentTable.Name),
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
