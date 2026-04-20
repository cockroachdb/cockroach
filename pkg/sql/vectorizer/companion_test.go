// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vectorizer

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/stretchr/testify/require"
)

func makeTableName(db, schema, table string) tree.TableName {
	tn := tree.MakeTableNameWithSchema(
		tree.Name(db), tree.Name(schema), tree.Name(table),
	)
	tn.ExplicitCatalog = true
	tn.ExplicitSchema = true
	return tn
}

func TestCompanionTableName(t *testing.T) {
	source := makeTableName("mydb", "public", "documents")
	companion := CompanionTableName(source)
	require.Equal(t, "documents_embeddings", string(companion.ObjectName))
	require.Equal(t, "mydb", string(companion.CatalogName))
	require.Equal(t, "public", string(companion.SchemaName))
}

func TestCompanionViewName(t *testing.T) {
	source := makeTableName("mydb", "public", "documents")
	view := CompanionViewName(source)
	require.Equal(t, "documents_embeddings_view", string(view.ObjectName))
}

func TestCreateCompanionTableSQL_SinglePK(t *testing.T) {
	source := makeTableName("mydb", "public", "documents")
	pkCols := []PKColumn{{Name: "id", TypeSQL: "INT8"}}

	sql := CreateCompanionTableSQL(source, pkCols, 384)

	require.Contains(t, sql, "CREATE TABLE mydb.public.documents_embeddings")
	require.Contains(t, sql, "embedding_uuid UUID DEFAULT gen_random_uuid() PRIMARY KEY")
	require.Contains(t, sql, "source_id INT8 NOT NULL")
	require.Contains(t, sql, "chunk_seq INT8 NOT NULL DEFAULT 0")
	require.Contains(t, sql, "chunk STRING NOT NULL")
	require.Contains(t, sql, "VECTOR(384)")
	require.Contains(t, sql, "REFERENCES mydb.public.documents (id) ON DELETE CASCADE")
	require.Contains(t, sql, "UNIQUE (source_id, chunk_seq)")
}

func TestCreateCompanionTableSQL_CompositePK(t *testing.T) {
	source := makeTableName("mydb", "public", "events")
	pkCols := []PKColumn{
		{Name: "tenant_id", TypeSQL: "UUID"},
		{Name: "event_id", TypeSQL: "INT8"},
	}

	sql := CreateCompanionTableSQL(source, pkCols, 384)

	require.Contains(t, sql, "source_tenant_id UUID NOT NULL")
	require.Contains(t, sql, "source_event_id INT8 NOT NULL")
	require.Contains(t, sql, "FOREIGN KEY (source_tenant_id, source_event_id)")
	require.Contains(t, sql, "REFERENCES mydb.public.events (tenant_id, event_id)")
	require.Contains(t, sql, "UNIQUE (source_tenant_id, source_event_id, chunk_seq)")
}

func TestCreateCompanionViewSQL_SinglePK(t *testing.T) {
	source := makeTableName("mydb", "public", "documents")
	pkCols := []PKColumn{{Name: "id", TypeSQL: "INT8"}}

	sql := CreateCompanionViewSQL(source, pkCols)

	require.Contains(t, sql, "CREATE VIEW mydb.public.documents_embeddings_view")
	require.Contains(t, sql, "SELECT s.*, e.chunk_seq, e.chunk, e.embedding")
	require.Contains(t, sql, "FROM mydb.public.documents AS s")
	require.Contains(t, sql, "JOIN mydb.public.documents_embeddings AS e")
	require.Contains(t, sql, "s.id = e.source_id")
}

func TestCreateCompanionViewSQL_CompositePK(t *testing.T) {
	source := makeTableName("mydb", "public", "events")
	pkCols := []PKColumn{
		{Name: "tenant_id", TypeSQL: "UUID"},
		{Name: "event_id", TypeSQL: "INT8"},
	}

	sql := CreateCompanionViewSQL(source, pkCols)

	require.Contains(t, sql, "s.tenant_id = e.source_tenant_id")
	require.Contains(t, sql, "s.event_id = e.source_event_id")
}
