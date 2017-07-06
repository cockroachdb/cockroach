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
//

package sql

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// showCreateView returns a valid SQL representation of the CREATE
// VIEW statement used to create the given view.
func (p *planner) showCreateView(
	ctx context.Context, tn parser.Name, desc *sqlbase.TableDescriptor,
) (string, error) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE VIEW %s ", tn)

	// Determine whether custom column names were specified when the view
	// was created, and include them if so.
	customColNames := false
	stmt, err := parser.ParseOne(desc.ViewQuery)
	if err != nil {
		return "", errors.Wrapf(err, "failed to parse underlying query from view %q", tn)
	}
	sel, ok := stmt.(*parser.Select)
	if !ok {
		return "", errors.Errorf("failed to parse underlying query from view %q as a select", tn)
	}

	// When constructing the Select plan, make sure we don't require any
	// privileges on the underlying tables.
	p.skipSelectPrivilegeChecks = true
	defer func() { p.skipSelectPrivilegeChecks = false }()

	sourcePlan, err := p.Select(ctx, sel, []parser.Type{})
	if err != nil {
		return "", err
	}
	for i, col := range planColumns(sourcePlan) {
		if col.Name != desc.Columns[i].Name {
			customColNames = true
			break
		}
	}
	if customColNames {
		buf.WriteByte('(')
		for i, col := range desc.Columns {
			if i > 0 {
				buf.WriteString(", ")
			}
			parser.Name(col.Name).Format(&buf, parser.FmtSimple)
		}
		buf.WriteString(") ")
	}

	fmt.Fprintf(&buf, "AS %s", desc.ViewQuery)
	return buf.String(), nil
}

// showCreateTable returns a valid SQL representation of the CREATE
// TABLE statement used to create the given table.
func (p *planner) showCreateTable(
	ctx context.Context, tn parser.Name, desc *sqlbase.TableDescriptor,
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
			fkIdx, err := fkTable.FindIndexByID(fk.Index)
			if err != nil {
				return "", err
			}
			fmt.Fprintf(&buf, ",\n\tCONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
				parser.Name(fk.Name),
				quoteNames(idx.ColumnNames...),
				parser.Name(fkTable.Name),
				quoteNames(fkIdx.ColumnNames...),
			)
		}
		interleave, err := p.showCreateInterleave(ctx, &idx)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&buf, ",\n\t%s%s",
			idx.SQLString(""),
			interleave,
		)
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

	interleave, err := p.showCreateInterleave(ctx, &desc.PrimaryIndex)
	if err != nil {
		return "", err
	}
	buf.WriteString(interleave)

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
func (p *planner) showCreateInterleave(
	ctx context.Context, idx *sqlbase.IndexDescriptor,
) (string, error) {
	if len(idx.Interleave.Ancestors) == 0 {
		return "", nil
	}
	intl := idx.Interleave
	parentTable, err := sqlbase.GetTableDescFromID(ctx, p.txn, intl.Ancestors[len(intl.Ancestors)-1].TableID)
	if err != nil {
		return "", err
	}
	parentDb, err := sqlbase.GetDatabaseDescFromID(ctx, p.txn, parentTable.ParentID)
	if err != nil {
		return "", err
	}
	parentName := parser.TableName{
		DatabaseName:            parser.Name(parentDb.Name),
		TableName:               parser.Name(parentTable.Name),
		DBNameOriginallyOmitted: parentDb.Name == p.session.Database,
	}
	var sharedPrefixLen int
	for _, ancestor := range intl.Ancestors {
		sharedPrefixLen += int(ancestor.SharedPrefixLen)
	}
	interleavedColumnNames := quoteNames(idx.ColumnNames[:sharedPrefixLen]...)
	s := fmt.Sprintf(" INTERLEAVE IN PARENT %s (%s)", &parentName, interleavedColumnNames)
	return s, nil
}
