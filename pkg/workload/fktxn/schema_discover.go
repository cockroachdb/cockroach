// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fktxn

import (
	gosql "database/sql"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// DiscoverSchema introspects a database via information_schema and returns a
// populated Schema with tables, columns, unique constraints, and FK edges.
func DiscoverSchema(db *gosql.DB, dbName string) (*Schema, error) {
	s := NewSchema()

	if err := discoverColumns(db, dbName, s); err != nil {
		return nil, errors.Wrap(err, "discovering columns")
	}
	if err := discoverUniqueConstraints(db, dbName, s); err != nil {
		return nil, errors.Wrap(err, "discovering unique constraints")
	}
	if err := discoverFKEdges(db, dbName, s); err != nil {
		return nil, errors.Wrap(err, "discovering FK edges")
	}

	s.Finalize()
	return s, nil
}

func discoverColumns(db *gosql.DB, dbName string, s *Schema) error {
	rows, err := db.Query(`
		SELECT table_name, column_name, is_nullable, crdb_sql_type, ordinal_position, is_generated
		FROM information_schema.columns
		WHERE table_catalog = $1
		  AND table_schema = 'public'
		ORDER BY table_name, ordinal_position`,
		dbName,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, colName, isNullable, sqlType, isGenerated string
		var ordinal int
		if err := rows.Scan(&tableName, &colName, &isNullable, &sqlType, &ordinal, &isGenerated); err != nil {
			return err
		}

		t, ok := s.Tables[tableName]
		if !ok {
			t = &Table{Name: tableName}
			s.AddTable(t)
		}
		col := Column{
			Name:     colName,
			Nullable: isNullable == "YES",
			Computed: isGenerated == "ALWAYS",
		}
		// Parse the column's SQL type. crdb_sql_type round-trips through the
		// parser. We only retain types that resolve to a built-in *types.T —
		// user-defined types (enums, etc.) leave Column.Type nil and the txn
		// generator must skip such columns.
		if typRef, err := parser.GetTypeFromValidSQLSyntax(sqlType); err == nil {
			if typ, ok := tree.GetStaticallyKnownType(typRef); ok {
				col.Type = typ
			}
		}
		t.Columns = append(t.Columns, col)
	}
	return rows.Err()
}

func discoverUniqueConstraints(db *gosql.DB, dbName string, s *Schema) error {
	rows, err := db.Query(`
		SELECT tc.table_name, tc.constraint_name, tc.constraint_type,
		       kcu.column_name, kcu.ordinal_position
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_name = kcu.table_name
		  AND tc.table_schema = kcu.table_schema
		WHERE tc.table_catalog = $1
		  AND tc.table_schema = 'public'
		  AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
		ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position`,
		dbName,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	type ucKey struct {
		table, constraint string
	}
	ucs := make(map[ucKey]*UniqueConstraint)
	var orderedKeys []ucKey

	for rows.Next() {
		var tableName, constraintName, constraintType, colName string
		var ordinal int
		if err := rows.Scan(&tableName, &constraintName, &constraintType, &colName, &ordinal); err != nil {
			return err
		}

		key := ucKey{tableName, constraintName}
		uc, ok := ucs[key]
		if !ok {
			uc = &UniqueConstraint{
				Name:      constraintName,
				IsPrimary: constraintType == "PRIMARY KEY",
			}
			ucs[key] = uc
			orderedKeys = append(orderedKeys, key)
		}
		uc.Columns = append(uc.Columns, colName)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, key := range orderedKeys {
		t, ok := s.Tables[key.table]
		if !ok {
			continue
		}
		t.UniqueConstraints = append(t.UniqueConstraints, *ucs[key])
	}
	return nil
}

func discoverFKEdges(db *gosql.DB, dbName string, s *Schema) error {
	rows, err := db.Query(`
		SELECT rc.constraint_name,
		       rc.table_name,
		       rc.referenced_table_name,
		       rc.unique_constraint_name,
		       kcu.column_name,
		       kcu.ordinal_position
		FROM information_schema.referential_constraints rc
		JOIN information_schema.key_column_usage kcu
		  ON rc.constraint_name = kcu.constraint_name
		  AND rc.constraint_schema = kcu.constraint_schema
		  AND kcu.table_name = rc.table_name
		WHERE rc.constraint_catalog = $1
		  AND rc.constraint_schema = 'public'
		ORDER BY rc.constraint_name, kcu.ordinal_position`,
		dbName,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	type fkBuilder struct {
		edge  FKEdge
		table string
	}
	fks := make(map[string]*fkBuilder)
	var orderedNames []string

	for rows.Next() {
		var constraintName, tableName, refTableName string
		var ucName, colName string
		var ordinal int
		if err := rows.Scan(
			&constraintName, &tableName, &refTableName,
			&ucName, &colName, &ordinal,
		); err != nil {
			return err
		}

		fb, ok := fks[constraintName]
		if !ok {
			fb = &fkBuilder{
				table: tableName,
				edge: FKEdge{
					Name:                 constraintName,
					ReferencingTable:     tableName,
					ReferencedTable:      refTableName,
					ReferencedConstraint: ucName,
				},
			}
			fks[constraintName] = fb
			orderedNames = append(orderedNames, constraintName)
		}
		fb.edge.ReferencingColumns = append(fb.edge.ReferencingColumns, colName)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, name := range orderedNames {
		fb := fks[name]

		// Resolve ReferencedColumns from the UC on the parent table.
		refTable, ok := s.Tables[fb.edge.ReferencedTable]
		if !ok {
			return errors.Newf(
				"FK %s references table %s which was not discovered",
				fb.edge.Name, fb.edge.ReferencedTable,
			)
		}
		var foundUC bool
		for _, uc := range refTable.UniqueConstraints {
			if uc.Name == fb.edge.ReferencedConstraint {
				fb.edge.ReferencedColumns = uc.Columns
				foundUC = true
				break
			}
		}
		if !foundUC {
			return errors.Newf(
				"FK %s references constraint %s on table %s which was not found",
				fb.edge.Name, fb.edge.ReferencedConstraint, fb.edge.ReferencedTable,
			)
		}

		// Derive Nullable: true if all referencing columns are nullable.
		fb.edge.Nullable = allColumnsNullable(s, fb.edge.ReferencingTable, fb.edge.ReferencingColumns)

		childTable, ok := s.Tables[fb.table]
		if !ok {
			return errors.Newf(
				"FK %s is on table %s which was not discovered",
				fb.edge.Name, fb.table,
			)
		}
		childTable.OutboundFKs = append(childTable.OutboundFKs, fb.edge)
	}
	return nil
}

func allColumnsNullable(s *Schema, tableName string, colNames []string) bool {
	t, ok := s.Tables[tableName]
	if !ok {
		return false
	}
	nullable := make(map[string]bool, len(t.Columns))
	for _, c := range t.Columns {
		nullable[c.Name] = c.Nullable
	}
	for _, name := range colNames {
		if !nullable[name] {
			return false
		}
	}
	return true
}
