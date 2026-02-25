// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbexec

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlaceholders(t *testing.T) {
	tests := []struct {
		n    int
		want string
	}{{
		n:    0,
		want: "",
	}, {
		n:    1,
		want: "$1",
	}, {
		n:    3,
		want: "$1, $2, $3",
	}, {
		n:    5,
		want: "$1, $2, $3, $4, $5",
	}}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			got := placeholders(tt.n)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestValueTuples(t *testing.T) {
	tests := []struct {
		count     int
		tupleSize int
		want      string
	}{{
		count:     0,
		tupleSize: 2,
		want:      "",
	}, {
		count:     1,
		tupleSize: 2,
		want:      "($1, $2)",
	}, {
		count:     2,
		tupleSize: 2,
		want:      "($1, $2), ($3, $4)",
	}, {
		count:     2,
		tupleSize: 3,
		want:      "($1, $2, $3), ($4, $5, $6)",
	}, {
		count:     3,
		tupleSize: 1,
		want:      "($1), ($2), ($3)",
	}}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			got := valueTuples(tt.count, tt.tupleSize)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCockroachDialect_Name(t *testing.T) {
	d := CockroachDialect{}
	require.Equal(t, "crdb", d.Name())
}

func TestPostgresDialect_Name(t *testing.T) {
	d := PostgresDialect{}
	require.Equal(t, "postgres", d.Name())
}

func TestCockroachDialect_UpsertStmt(t *testing.T) {
	d := CockroachDialect{}

	t.Run("without enum", func(t *testing.T) {
		stmt := d.UpsertStmt("kv", 2, false)
		require.Equal(t, "UPSERT INTO kv (k, v) VALUES ($1, $2), ($3, $4)", stmt)
	})

	t.Run("with enum ignored", func(t *testing.T) {
		// CockroachDB's enum column is a computed stored column, so it is never
		// included in UPSERT statements.
		stmt := d.UpsertStmt("kv", 2, true)
		require.Equal(t, "UPSERT INTO kv (k, v) VALUES ($1, $2), ($3, $4)", stmt)
	})

	t.Run("single row", func(t *testing.T) {
		stmt := d.UpsertStmt("kv", 1, false)
		require.Equal(t, "UPSERT INTO kv (k, v) VALUES ($1, $2)", stmt)
	})
}

func TestPostgresDialect_UpsertStmt(t *testing.T) {
	d := PostgresDialect{}

	t.Run("without enum", func(t *testing.T) {
		stmt := d.UpsertStmt("kv", 2, false)
		expected := "INSERT INTO kv (k, v) VALUES ($1, $2), ($3, $4) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v"
		require.Equal(t, expected, stmt)
	})

	t.Run("with enum", func(t *testing.T) {
		stmt := d.UpsertStmt("kv", 2, true)
		expected := "INSERT INTO kv (k, v, e) VALUES ($1, $2, $3), ($4, $5, $6) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, e = EXCLUDED.e"
		require.Equal(t, expected, stmt)
	})

	t.Run("single row", func(t *testing.T) {
		stmt := d.UpsertStmt("kv", 1, false)
		expected := "INSERT INTO kv (k, v) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v"
		require.Equal(t, expected, stmt)
	})
}

func TestCockroachDialect_ReadStmt(t *testing.T) {
	d := CockroachDialect{}

	t.Run("without enum", func(t *testing.T) {
		stmt := d.ReadStmt("kv", 3, false)
		require.Equal(t, "SELECT k, v FROM kv WHERE k IN ($1, $2, $3)", stmt)
	})

	t.Run("with enum", func(t *testing.T) {
		stmt := d.ReadStmt("kv", 3, true)
		require.Equal(t, "SELECT k, v, e FROM kv WHERE k IN ($1, $2, $3)", stmt)
	})

	t.Run("single key", func(t *testing.T) {
		stmt := d.ReadStmt("kv", 1, false)
		require.Equal(t, "SELECT k, v FROM kv WHERE k IN ($1)", stmt)
	})
}

func TestPostgresDialect_ReadStmt(t *testing.T) {
	d := PostgresDialect{}

	t.Run("without enum", func(t *testing.T) {
		stmt := d.ReadStmt("kv", 3, false)
		require.Equal(t, "SELECT k, v FROM kv WHERE k IN ($1, $2, $3)", stmt)
	})

	t.Run("with enum", func(t *testing.T) {
		stmt := d.ReadStmt("kv", 3, true)
		require.Equal(t, "SELECT k, v, e FROM kv WHERE k IN ($1, $2, $3)", stmt)
	})
}

func TestCockroachDialect_FollowerReadStmt(t *testing.T) {
	d := CockroachDialect{}

	t.Run("contains AS OF SYSTEM TIME", func(t *testing.T) {
		stmt := d.FollowerReadStmt("kv", 2, false)
		require.Contains(t, stmt, "AS OF SYSTEM TIME follower_read_timestamp()")
		require.Contains(t, stmt, "WHERE k IN ($1, $2)")
	})

	t.Run("without enum", func(t *testing.T) {
		stmt := d.FollowerReadStmt("kv", 2, false)
		expected := "SELECT k, v FROM kv AS OF SYSTEM TIME follower_read_timestamp() WHERE k IN ($1, $2)"
		require.Equal(t, expected, stmt)
	})

	t.Run("with enum", func(t *testing.T) {
		stmt := d.FollowerReadStmt("kv", 2, true)
		expected := "SELECT k, v, e FROM kv AS OF SYSTEM TIME follower_read_timestamp() WHERE k IN ($1, $2)"
		require.Equal(t, expected, stmt)
	})
}

func TestPostgresDialect_FollowerReadStmt(t *testing.T) {
	d := PostgresDialect{}

	t.Run("equals ReadStmt", func(t *testing.T) {
		followerStmt := d.FollowerReadStmt("kv", 3, false)
		readStmt := d.ReadStmt("kv", 3, false)
		require.Equal(t, readStmt, followerStmt)
	})

	t.Run("does not contain AS OF SYSTEM TIME", func(t *testing.T) {
		stmt := d.FollowerReadStmt("kv", 2, false)
		require.NotContains(t, stmt, "AS OF SYSTEM TIME")
	})

	t.Run("with enum equals ReadStmt", func(t *testing.T) {
		followerStmt := d.FollowerReadStmt("kv", 2, true)
		readStmt := d.ReadStmt("kv", 2, true)
		require.Equal(t, readStmt, followerStmt)
	})
}

func TestCockroachDialect_DeleteStmt(t *testing.T) {
	d := CockroachDialect{}

	stmt := d.DeleteStmt("kv", 3)
	require.Equal(t, "DELETE FROM kv WHERE k IN ($1, $2, $3)", stmt)
}

func TestPostgresDialect_DeleteStmt(t *testing.T) {
	d := PostgresDialect{}

	stmt := d.DeleteStmt("kv", 3)
	require.Equal(t, "DELETE FROM kv WHERE k IN ($1, $2, $3)", stmt)
}

func TestCockroachDialect_SpanStmt(t *testing.T) {
	d := CockroachDialect{}

	t.Run("uses scalar subquery syntax", func(t *testing.T) {
		stmt := d.SpanStmt("kv", 100)
		// CockroachDB uses [SELECT ...] scalar subquery syntax.
		require.Contains(t, stmt, "[SELECT v FROM kv")
		require.Contains(t, stmt, "LIMIT 100")
	})

	t.Run("full statement", func(t *testing.T) {
		stmt := d.SpanStmt("kv", 50)
		expected := "SELECT count(v) FROM [SELECT v FROM kv WHERE k >= $1 ORDER BY k LIMIT 50]"
		require.Equal(t, expected, stmt)
	})

	t.Run("limit zero scans full table", func(t *testing.T) {
		stmt := d.SpanStmt("kv", 0)
		expected := "SELECT count(v) FROM [SELECT v FROM kv]"
		require.Equal(t, expected, stmt)
	})
}

func TestPostgresDialect_SpanStmt(t *testing.T) {
	d := PostgresDialect{}

	t.Run("uses derived table syntax", func(t *testing.T) {
		stmt := d.SpanStmt("kv", 100)
		// PostgreSQL uses (SELECT ...) AS t derived table syntax.
		require.Contains(t, stmt, "(SELECT v FROM kv")
		require.Contains(t, stmt, ") AS t")
		require.Contains(t, stmt, "LIMIT 100")
	})

	t.Run("full statement", func(t *testing.T) {
		stmt := d.SpanStmt("kv", 50)
		expected := "SELECT count(v) FROM (SELECT v FROM kv WHERE k >= $1 ORDER BY k LIMIT 50) AS t"
		require.Equal(t, expected, stmt)
	})

	t.Run("limit zero scans full table", func(t *testing.T) {
		stmt := d.SpanStmt("kv", 0)
		expected := "SELECT count(v) FROM (SELECT v FROM kv) AS t"
		require.Equal(t, expected, stmt)
	})
}

func TestCockroachDialect_SelectForUpdateStmt(t *testing.T) {
	d := CockroachDialect{}

	stmt := d.SelectForUpdateStmt("kv", 2)
	require.Equal(t, "SELECT k, v FROM kv WHERE k IN ($1, $2) FOR UPDATE", stmt)
}

func TestPostgresDialect_SelectForUpdateStmt(t *testing.T) {
	d := PostgresDialect{}

	stmt := d.SelectForUpdateStmt("kv", 2)
	require.Equal(t, "SELECT k, v FROM kv WHERE k IN ($1, $2) FOR UPDATE", stmt)
}

func TestCockroachDialect_SchemaFragment(t *testing.T) {
	d := CockroachDialect{}

	t.Run("basic table", func(t *testing.T) {
		frag := d.SchemaFragment(0, false, 0)
		require.Equal(t, "(\n\t\tk BIGINT NOT NULL PRIMARY KEY,\n\t\tv BYTES NOT NULL\n\t)", frag)
	})

	t.Run("with hash sharding", func(t *testing.T) {
		frag := d.SchemaFragment(0, false, 8)
		require.Contains(t, frag, "k BIGINT NOT NULL")
		require.Contains(t, frag, "USING HASH WITH (bucket_count = 8)")
	})

	t.Run("with secondary index", func(t *testing.T) {
		frag := d.SchemaFragment(0, true, 0)
		require.Contains(t, frag, "INDEX (v)")
	})

	t.Run("with string key", func(t *testing.T) {
		frag := d.SchemaFragment(20, false, 0)
		require.Contains(t, frag, "k STRING NOT NULL")
	})

	t.Run("hash sharding and secondary index", func(t *testing.T) {
		frag := d.SchemaFragment(0, true, 8)
		require.Contains(t, frag, "USING HASH WITH (bucket_count = 8)")
		require.Contains(t, frag, "INDEX (v)")
	})
}

func TestPostgresDialect_SchemaFragment(t *testing.T) {
	d := PostgresDialect{}

	t.Run("basic table", func(t *testing.T) {
		frag := d.SchemaFragment(0, false, 0)
		require.Equal(t, "(k BIGINT NOT NULL PRIMARY KEY, v BYTEA NOT NULL)", frag)
	})

	t.Run("with string key", func(t *testing.T) {
		frag := d.SchemaFragment(100, false, 0)
		require.Equal(t, "(k VARCHAR(100) NOT NULL PRIMARY KEY, v BYTEA NOT NULL)", frag)
	})

	t.Run("ignores hash sharding", func(t *testing.T) {
		withShards := d.SchemaFragment(0, false, 8)
		withoutShards := d.SchemaFragment(0, false, 0)
		require.Equal(t, withoutShards, withShards)
	})

	t.Run("ignores secondaryIndex", func(t *testing.T) {
		withIndex := d.SchemaFragment(0, true, 0)
		withoutIndex := d.SchemaFragment(0, false, 0)
		require.Equal(t, withoutIndex, withIndex)
	})

	t.Run("uses BYTEA not BYTES", func(t *testing.T) {
		frag := d.SchemaFragment(0, false, 0)
		require.Contains(t, frag, "BYTEA")
	})
}

func TestCockroachDialect_Capabilities(t *testing.T) {
	d := CockroachDialect{}
	caps := d.Capabilities()

	require.True(t, caps.FollowerReads, "CockroachDB supports follower reads")
	require.True(t, caps.Scatter, "CockroachDB supports scatter")
	require.True(t, caps.Splits, "CockroachDB supports splits")
	require.True(t, caps.HashShardedPK, "CockroachDB supports hash-sharded PKs")
	require.True(t, caps.TransactionQoS, "CockroachDB supports transaction QoS")
	require.True(t, caps.SelectForUpdate, "CockroachDB supports SELECT FOR UPDATE")
	require.True(t, caps.TransactionPriority, "CockroachDB supports transaction priority")
	require.True(t, caps.SerializationRetry, "CockroachDB supports serialization retry")
	require.True(t, caps.Select1, "CockroachDB supports SELECT 1 warmup")
	require.True(t, caps.EnumColumn, "CockroachDB supports enum columns")
}

func TestPostgresDialect_Capabilities(t *testing.T) {
	d := PostgresDialect{}
	caps := d.Capabilities()

	require.False(t, caps.FollowerReads, "PostgreSQL does not support follower reads")
	require.False(t, caps.Scatter, "PostgreSQL does not support scatter")
	require.False(t, caps.Splits, "PostgreSQL does not support splits")
	require.False(t, caps.HashShardedPK, "PostgreSQL does not support hash-sharded PKs")
	require.False(t, caps.TransactionQoS, "PostgreSQL does not support transaction QoS")
	require.True(t, caps.SelectForUpdate, "PostgreSQL supports SELECT FOR UPDATE")
	require.False(t, caps.TransactionPriority, "PostgreSQL does not support transaction priority")
	require.False(t, caps.SerializationRetry, "PostgreSQL does not support serialization retry")
	require.True(t, caps.Select1, "PostgreSQL supports SELECT 1")
	require.False(t, caps.EnumColumn, "PostgreSQL does not support enum columns")
}

func TestSpannerSchemaDialect_SchemaFragment(t *testing.T) {
	d := SpannerSchemaDialect{}

	t.Run("integer key", func(t *testing.T) {
		frag := d.SchemaFragment(0, false, 0)
		require.Equal(t, "(k INT64 NOT NULL, v BYTES(MAX) NOT NULL) PRIMARY KEY (k)", frag)
	})

	t.Run("string key", func(t *testing.T) {
		frag := d.SchemaFragment(100, false, 0)
		require.Equal(t, "(k STRING(100) NOT NULL, v BYTES(MAX) NOT NULL) PRIMARY KEY (k)", frag)
	})

	t.Run("ignores hash sharding", func(t *testing.T) {
		withShards := d.SchemaFragment(0, false, 8)
		withoutShards := d.SchemaFragment(0, false, 0)
		require.Equal(t, withoutShards, withShards)
	})

	t.Run("ignores secondaryIndex", func(t *testing.T) {
		withIndex := d.SchemaFragment(0, true, 0)
		withoutIndex := d.SchemaFragment(0, false, 0)
		require.Equal(t, withoutIndex, withIndex)
	})

	t.Run("uses BYTES(MAX)", func(t *testing.T) {
		frag := d.SchemaFragment(0, false, 0)
		require.Contains(t, frag, "BYTES(MAX)")
	})
}

func TestSecondaryIndexStmts(t *testing.T) {
	t.Run("CockroachDialect returns nil", func(t *testing.T) {
		d := CockroachDialect{}
		require.Nil(t, d.SecondaryIndexStmts("kv"))
	})

	t.Run("PostgresDialect returns CREATE INDEX IF NOT EXISTS", func(t *testing.T) {
		d := PostgresDialect{}
		stmts := d.SecondaryIndexStmts("kv")
		require.Equal(t, []string{"CREATE INDEX IF NOT EXISTS kv_v_idx ON kv (v)"}, stmts)
	})

	t.Run("SpannerSchemaDialect returns CREATE INDEX without IF NOT EXISTS", func(t *testing.T) {
		d := SpannerSchemaDialect{}
		stmts := d.SecondaryIndexStmts("kv")
		require.Equal(t, []string{"CREATE INDEX kv_v_idx ON kv (v)"}, stmts)
	})
}

func TestPostLoadStmts(t *testing.T) {
	t.Run("CockroachDialect", func(t *testing.T) {
		d := CockroachDialect{}

		t.Run("enum only", func(t *testing.T) {
			stmts := d.PostLoadStmts("kv", true, false)
			require.NotNil(t, stmts)
			require.Len(t, stmts, 1)
			require.Contains(t, stmts[0], "CREATE TYPE")
			require.Contains(t, stmts[0], "ALTER TABLE")
		})

		t.Run("scatter only", func(t *testing.T) {
			stmts := d.PostLoadStmts("kv", false, true)
			require.NotNil(t, stmts)
			require.Len(t, stmts, 1)
			require.Contains(t, stmts[0], "ALTER TABLE kv SCATTER")
		})

		t.Run("enum and scatter", func(t *testing.T) {
			stmts := d.PostLoadStmts("kv", true, true)
			require.NotNil(t, stmts)
			require.Len(t, stmts, 2)
		})

		t.Run("neither", func(t *testing.T) {
			stmts := d.PostLoadStmts("kv", false, false)
			require.Nil(t, stmts)
		})
	})

	t.Run("PostgresDialect always returns nil", func(t *testing.T) {
		d := PostgresDialect{}
		require.Nil(t, d.PostLoadStmts("kv", true, true))
		require.Nil(t, d.PostLoadStmts("kv", false, false))
	})

	t.Run("SpannerSchemaDialect always returns nil", func(t *testing.T) {
		d := SpannerSchemaDialect{}
		require.Nil(t, d.PostLoadStmts("kv", true, true))
		require.Nil(t, d.PostLoadStmts("kv", false, false))
	})
}

func TestSpannerSchemaDialect_Capabilities(t *testing.T) {
	d := SpannerSchemaDialect{}
	caps := d.Capabilities()

	require.True(t, caps.FollowerReads, "Spanner supports follower reads")
	require.True(t, caps.EnumColumn, "Spanner supports enum columns")
	require.False(t, caps.Select1, "Spanner does not support SELECT 1")
	require.False(t, caps.SelectForUpdate, "Spanner does not support SELECT FOR UPDATE")
	require.False(t, caps.Scatter, "Spanner does not support scatter")
	require.False(t, caps.Splits, "Spanner does not support splits")
	require.False(t, caps.HashShardedPK, "Spanner does not support hash-sharded PKs")
	require.False(t, caps.TransactionQoS, "Spanner does not support transaction QoS")
	require.False(t, caps.TransactionPriority, "Spanner does not support transaction priority")
	require.False(t, caps.SerializationRetry, "Spanner does not support serialization retry")
}

func TestDialectInterface(t *testing.T) {
	// Verify both dialects implement the Dialect interface.
	dialects := []Dialect{
		CockroachDialect{},
		PostgresDialect{},
	}

	for _, d := range dialects {
		t.Run(d.Name(), func(t *testing.T) {
			// All Dialect methods should be callable without panicking.
			require.NotEmpty(t, d.Name())
			require.NotEmpty(t, d.UpsertStmt("kv", 1, false))
			require.NotEmpty(t, d.ReadStmt("kv", 1, false))
			require.NotEmpty(t, d.FollowerReadStmt("kv", 1, false))
			require.NotEmpty(t, d.DeleteStmt("kv", 1))
			require.NotEmpty(t, d.SpanStmt("kv", 10))
			require.NotEmpty(t, d.SelectForUpdateStmt("kv", 1))

			// SchemaDialect methods should be callable.
			require.NotEmpty(t, d.SchemaFragment(0, false, 0))
			_ = d.SecondaryIndexStmts("kv")
			_ = d.Capabilities()
		})
	}

	// SpannerSchemaDialect only implements SchemaDialect, not full Dialect.
	t.Run("SpannerSchemaDialect", func(t *testing.T) {
		var d SchemaDialect = SpannerSchemaDialect{}
		require.NotEmpty(t, d.SchemaFragment(0, false, 0))
		require.NotNil(t, d.SecondaryIndexStmts("kv"))
		_ = d.Capabilities()
	})
}

func TestCustomTableName(t *testing.T) {
	dialects := []Dialect{
		CockroachDialect{},
		PostgresDialect{},
	}

	for _, d := range dialects {
		t.Run(d.Name(), func(t *testing.T) {
			customTable := "my_custom_table"

			stmt := d.UpsertStmt(customTable, 1, false)
			require.True(t, strings.Contains(stmt, customTable))

			stmt = d.ReadStmt(customTable, 1, false)
			require.True(t, strings.Contains(stmt, customTable))

			stmt = d.DeleteStmt(customTable, 1)
			require.True(t, strings.Contains(stmt, customTable))

			stmt = d.SpanStmt(customTable, 10)
			require.True(t, strings.Contains(stmt, customTable))

			// SecondaryIndexStmts should include the custom table name.
			stmts := d.SecondaryIndexStmts(customTable)
			if stmts != nil {
				for _, s := range stmts {
					require.True(t, strings.Contains(s, customTable))
				}
			}

			// PostLoadStmts should include the custom table name.
			postStmts := d.PostLoadStmts(customTable, true, true)
			for _, s := range postStmts {
				require.Contains(t, s, customTable)
			}
		})
	}

	// SpannerSchemaDialect SecondaryIndexStmts with custom table name.
	t.Run("SpannerSchemaDialect", func(t *testing.T) {
		d := SpannerSchemaDialect{}
		customTable := "my_custom_table"
		stmts := d.SecondaryIndexStmts(customTable)
		require.NotNil(t, stmts)
		for _, s := range stmts {
			require.True(t, strings.Contains(s, customTable))
		}
	})
}
