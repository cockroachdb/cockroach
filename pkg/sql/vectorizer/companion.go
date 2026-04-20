// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package vectorizer implements the automatic embedding generation
// feature for CockroachDB tables. It provides helpers for creating
// companion embedding tables, views, and the background jobs that
// populate them.
package vectorizer

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// PKColumn describes a primary key column from the source table.
type PKColumn struct {
	// Name is the column name (unquoted).
	Name string
	// TypeSQL is the SQL type string, e.g. "INT8", "UUID", "STRING".
	TypeSQL string
}

// CompanionTableName returns a TableName for the companion embeddings
// table, derived from the source table name by appending "_embeddings".
// The companion table is created in the same database and schema as the
// source.
func CompanionTableName(source tree.TableName) tree.TableName {
	embName := string(source.ObjectName) + "_embeddings"
	return tree.MakeTableNameWithSchema(
		source.CatalogName,
		source.SchemaName,
		tree.Name(embName),
	)
}

// CompanionViewName returns a TableName for the companion view,
// derived from the source table name by appending "_embeddings_view".
func CompanionViewName(source tree.TableName) tree.TableName {
	viewName := string(source.ObjectName) + "_embeddings_view"
	return tree.MakeTableNameWithSchema(
		source.CatalogName,
		source.SchemaName,
		tree.Name(viewName),
	)
}

// CreateCompanionTableSQL returns the CREATE TABLE statement for the
// companion embeddings table. The table stores one row per chunk: short
// texts produce a single row (chunk_seq=0), while long texts produce
// multiple rows with sequential chunk_seq values.
//
// Parameters:
//   - source: fully qualified source table name
//   - pkCols: primary key column(s) of the source table
//   - dims: embedding vector dimensions (e.g. 384 for all-MiniLM-L6-v2)
func CreateCompanionTableSQL(source tree.TableName, pkCols []PKColumn, dims int) string {
	companion := CompanionTableName(source)

	// Build source_id column(s) matching the source PK.
	var sourceColDefs []string
	var sourceColNames []string
	var sourcePKRef []string
	for _, pk := range pkCols {
		colName := "source_" + pk.Name
		sourceColDefs = append(sourceColDefs,
			fmt.Sprintf("    %s %s NOT NULL", tree.NameString(colName), pk.TypeSQL))
		sourceColNames = append(sourceColNames, tree.NameString(colName))
		sourcePKRef = append(sourcePKRef, tree.NameString(pk.Name))
	}

	// Build the FK constraint referencing the source table PK.
	fkConstraint := fmt.Sprintf(
		"    CONSTRAINT fk_source FOREIGN KEY (%s) REFERENCES %s (%s) ON DELETE CASCADE",
		strings.Join(sourceColNames, ", "),
		source.String(),
		strings.Join(sourcePKRef, ", "),
	)

	// Build the unique constraint on (source columns, chunk_seq).
	uniqueCols := append(append([]string{}, sourceColNames...), "chunk_seq")
	uniqueConstraint := fmt.Sprintf(
		"    UNIQUE (%s)",
		strings.Join(uniqueCols, ", "),
	)

	return fmt.Sprintf(`CREATE TABLE %s (
    embedding_uuid UUID DEFAULT gen_random_uuid() PRIMARY KEY,
%s,
    chunk_seq INT8 NOT NULL DEFAULT 0,
    chunk STRING NOT NULL,
    embedding VECTOR(%d) NOT NULL,
%s,
%s
)`,
		companion.String(),
		strings.Join(sourceColDefs, ",\n"),
		dims,
		fkConstraint,
		uniqueConstraint,
	)
}

// CreateCompanionViewSQL returns the CREATE VIEW statement that joins
// the source table with its companion embeddings table for convenient
// querying.
func CreateCompanionViewSQL(source tree.TableName, pkCols []PKColumn) string {
	companion := CompanionTableName(source)
	view := CompanionViewName(source)

	// Build the JOIN condition on PK columns.
	var joinConds []string
	for _, pk := range pkCols {
		joinConds = append(joinConds,
			fmt.Sprintf("s.%s = e.%s",
				tree.NameString(pk.Name),
				tree.NameString("source_"+pk.Name)))
	}

	return fmt.Sprintf(`CREATE VIEW %s AS
SELECT s.*, e.chunk_seq, e.chunk, e.embedding
FROM %s AS s
JOIN %s AS e ON %s`,
		view.String(),
		source.String(),
		companion.String(),
		strings.Join(joinConds, " AND "),
	)
}
