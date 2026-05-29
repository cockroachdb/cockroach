// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/pmezard/go-difflib/difflib"
)

var pgDumpSupportedTag = "REL_18_3"
var pgDumpPostgresDir = "/mnt/data1/postgres"
var pgDumpBinDir = pgDumpPostgresDir + "/src/bin/pg_dump"
var pgDumpOutputDir = "/mnt/data1/pgdump_output"
var pgDumpTestdataPrefix = "./pkg/cmd/roachtest/testdata/pg_dump/"

// pgDumpTestSchema defines a representative set of database objects for
// testing pg_dump round-trip compatibility. It covers common object types
// that pg_dump needs to handle correctly when dumping a CockroachDB
// database.
var pgDumpTestSchema = `
CREATE SCHEMA extra;

-- Enum types.
CREATE TYPE public.status_type AS ENUM ('active', 'inactive', 'pending');

-- Tables with various column types.
CREATE TABLE public.basic_types (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    active BOOL DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now(),
    metadata JSONB,
    tags TEXT[],
    unique_id UUID DEFAULT gen_random_uuid(),
    amount DECIMAL(10,2),
    small_val INT2,
    big_val INT8,
    description VARCHAR(255)
);

-- Table with constraints.
CREATE TABLE public.constrained (
    id INT PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    age INT CHECK (age >= 0 AND age <= 200),
    status status_type DEFAULT 'active'
);

-- Secondary indexes.
CREATE INDEX idx_basic_name ON public.basic_types (name);
CREATE INDEX idx_basic_partial ON public.basic_types (name) WHERE active = true;
CREATE INDEX idx_basic_covering ON public.basic_types (name) STORING (created_at);

-- Foreign keys.
CREATE TABLE public.orders (
    id INT PRIMARY KEY,
    customer_id INT REFERENCES public.basic_types(id),
    total DECIMAL(10,2) NOT NULL
);

-- Sequences.
CREATE SEQUENCE public.order_seq;

-- Views.
CREATE VIEW public.active_basics AS
    SELECT id, name FROM public.basic_types WHERE active = true;

-- Comments.
COMMENT ON TABLE public.basic_types IS 'Table for testing basic column types';
COMMENT ON COLUMN public.basic_types.name IS 'Name of the entry';

-- Multi-schema objects.
CREATE TABLE extra.extra_table (
    id INT PRIMARY KEY,
    ref_id INT REFERENCES public.basic_types(id),
    data TEXT
);

-- Default values with expressions.
CREATE TABLE public.default_exprs (
    id INT PRIMARY KEY DEFAULT unique_rowid(),
    created TIMESTAMPTZ DEFAULT current_timestamp(),
    code TEXT DEFAULT 'UNKNOWN'
);

-- Identity columns.
CREATE TABLE public.identity_test (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    label TEXT NOT NULL
);

-- Generated (computed) columns.
CREATE TABLE public.generated_cols (
    id INT PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED
);

-- Composite primary key.
CREATE TABLE public.composite_pk (
    tenant_id INT NOT NULL,
    item_id INT NOT NULL,
    value TEXT,
    PRIMARY KEY (tenant_id, item_id)
);

-- Self-referencing foreign key.
CREATE TABLE public.tree_node (
    id INT PRIMARY KEY,
    parent_id INT,
    label TEXT NOT NULL,
    CONSTRAINT self_ref_fk FOREIGN KEY (parent_id) REFERENCES public.tree_node(id)
);

-- Multi-level view (view referencing another view).
CREATE VIEW public.active_basics_summary AS
    SELECT count(*) AS cnt FROM public.active_basics;

-- Multiple foreign keys on same table.
CREATE TABLE public.audit_log (
    id INT PRIMARY KEY,
    actor_id INT REFERENCES public.basic_types(id),
    target_id INT REFERENCES public.basic_types(id),
    action TEXT NOT NULL
);

-- ===========================================================
-- Block A: User-defined functions, procedures, triggers.
-- ===========================================================

-- SQL-bodied UDF.
CREATE FUNCTION public.add_one(n INT) RETURNS INT
    LANGUAGE SQL AS 'SELECT n + 1';

-- PL/pgSQL trigger function.
CREATE FUNCTION public.touch_updated() RETURNS TRIGGER
    LANGUAGE plpgsql AS $$
    BEGIN
        NEW.updated_at = now();
        RETURN NEW;
    END;
    $$;

-- Stored procedure.
CREATE PROCEDURE public.noop() LANGUAGE SQL AS 'SELECT 1';

-- Table that fires the trigger function.
CREATE TABLE public.with_trigger (
    id INT PRIMARY KEY,
    val TEXT,
    updated_at TIMESTAMPTZ
);
CREATE TRIGGER touch_with_trigger
    BEFORE UPDATE ON public.with_trigger
    FOR EACH ROW EXECUTE FUNCTION public.touch_updated();

-- ===========================================================
-- Block B: Composite type, materialized view, RLS.
-- ===========================================================

-- Composite type.
CREATE TYPE public.addr AS (street TEXT, zip TEXT);
CREATE TABLE public.with_addr (
    id INT PRIMARY KEY,
    home addr
);

-- Materialized view (built on basic_types from above).
CREATE MATERIALIZED VIEW public.basic_count AS
    SELECT count(*) AS n FROM public.basic_types;

-- Row-level security.
CREATE TABLE public.rls_test (
    id INT PRIMARY KEY,
    owner TEXT NOT NULL
);
ALTER TABLE public.rls_test ENABLE ROW LEVEL SECURITY;
CREATE POLICY owner_policy ON public.rls_test
    AS PERMISSIVE FOR ALL TO PUBLIC USING (owner = current_user);

-- ===========================================================
-- Block C: Index variations.
-- ===========================================================

-- GIN index on JSONB column.
CREATE INDEX idx_basic_metadata_gin ON public.basic_types USING GIN (metadata);

-- GIN index on text array.
CREATE INDEX idx_basic_tags_gin ON public.basic_types USING GIN (tags);

-- Expression index.
CREATE INDEX idx_basic_lower_name ON public.basic_types (lower(name));

-- Hash-sharded index (CockroachDB-specific).
CREATE TABLE public.hash_sharded_table (
    id INT PRIMARY KEY USING HASH WITH (bucket_count = 8),
    val TEXT
);

-- Vector index (CockroachDB-specific).
CREATE TABLE public.vector_table (
    id INT PRIMARY KEY,
    embedding VECTOR(3)
);
CREATE VECTOR INDEX idx_vector_embedding ON public.vector_table (embedding);

-- ===========================================================
-- Block D: Constraint variations.
-- ===========================================================

-- Named UNIQUE constraint (vs. unique index).
CREATE TABLE public.named_unique (
    id INT PRIMARY KEY,
    code TEXT,
    CONSTRAINT named_unique_code UNIQUE (code)
);

-- NOT VALID check constraint.
CREATE TABLE public.not_valid_check (
    id INT PRIMARY KEY,
    val INT
);
ALTER TABLE public.not_valid_check
    ADD CONSTRAINT val_positive CHECK (val > 0) NOT VALID;

-- Parent table for FK action variations.
CREATE TABLE public.fk_parent (
    id INT PRIMARY KEY,
    label TEXT
);

-- Every FK action on a single table.
CREATE TABLE public.fk_actions (
    id INT PRIMARY KEY,
    cascade_id    INT REFERENCES public.fk_parent(id) ON DELETE CASCADE  ON UPDATE CASCADE,
    setnull_id    INT REFERENCES public.fk_parent(id) ON DELETE SET NULL,
    setdefault_id INT DEFAULT 0 REFERENCES public.fk_parent(id) ON DELETE SET DEFAULT,
    restrict_id   INT REFERENCES public.fk_parent(id) ON DELETE RESTRICT,
    noaction_id   INT REFERENCES public.fk_parent(id) ON DELETE NO ACTION
);

-- Multi-column FK referencing composite_pk.
CREATE TABLE public.composite_fk (
    id INT PRIMARY KEY,
    tenant_id INT,
    item_id INT,
    FOREIGN KEY (tenant_id, item_id) REFERENCES public.composite_pk(tenant_id, item_id)
);

-- Cyclic FKs (added via ALTER to break the dependency cycle).
CREATE TABLE public.node_a (id INT PRIMARY KEY, b_id INT);
CREATE TABLE public.node_b (id INT PRIMARY KEY, a_id INT);
ALTER TABLE public.node_a
    ADD CONSTRAINT a_to_b FOREIGN KEY (b_id) REFERENCES public.node_b(id);
ALTER TABLE public.node_b
    ADD CONSTRAINT b_to_a FOREIGN KEY (a_id) REFERENCES public.node_a(id);

-- ===========================================================
-- Block E: Sequence options.
-- ===========================================================

-- Sequence exercising every supported option (CYCLE is unimplemented in CRDB).
CREATE SEQUENCE public.full_seq
    START WITH 100
    INCREMENT BY 5
    MINVALUE 50
    MAXVALUE 1000
    CACHE 10;

-- Sequence owned by a column.
CREATE TABLE public.owned_seq_table (
    id INT PRIMARY KEY,
    counter INT
);
CREATE SEQUENCE public.owned_seq OWNED BY public.owned_seq_table.counter;

-- ===========================================================
-- Block F: Type variations.
-- ===========================================================

-- type_zoo omits CIDR (unimplemented as a column type in CRDB).
CREATE TABLE public.type_zoo (
    id INT PRIMARY KEY,
    text_collated TEXT COLLATE "en-US",
    inet_val INET,
    bit_fixed BIT(8),
    bit_var BIT VARYING(16),
    interval_val INTERVAL HOUR TO SECOND(3),
    ts_precision TIMESTAMPTZ(3),
    numeric_unparam NUMERIC,
    char_padded CHAR(10),
    status_arr status_type[]
);

-- ===========================================================
-- Block G: Identity BY DEFAULT.
-- ===========================================================

CREATE TABLE public.identity_default (
    id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    label TEXT NOT NULL
);

-- ===========================================================
-- Block H: Identifier quoting edge cases.
-- ===========================================================

-- Reserved-keyword identifiers.
CREATE TABLE public."user" (
    id INT PRIMARY KEY,
    "select" INT
);

-- Mixed-case identifiers.
CREATE TABLE public."MixedCase" (
    id INT PRIMARY KEY,
    "ColName" TEXT
);

-- Long identifier near the 63-byte PG limit.
CREATE TABLE public.a_very_long_identifier_name_to_test_pg_dump_quote_safety (
    id INT PRIMARY KEY
);

-- ===========================================================
-- Block I: Domain types.
-- ===========================================================

-- Plain domain (no constraints, no default).
CREATE DOMAIN public.dom_plain AS INT;

-- CHECK constraint, exercised via pg_constraint round-trip.
CREATE DOMAIN public.dom_positive AS INT CHECK (VALUE > 0);

-- Domain over TEXT with a CHECK constraint containing literal text.
CREATE DOMAIN public.dom_email AS TEXT CHECK (VALUE LIKE '%@%');

-- NOT NULL domain.
CREATE DOMAIN public.dom_nn_label AS TEXT NOT NULL;

-- DEFAULT expressions. Cover a literal, a no-arg function call, a
-- function call with arguments, and an arithmetic expression to make
-- sure pg_dump emits them raw (via typdefaultbin → pg_get_expr) rather
-- than wrapping the value in string literal quoting.
CREATE DOMAIN public.dom_default_lit AS INT DEFAULT 0;
CREATE DOMAIN public.dom_default_now AS TIMESTAMPTZ DEFAULT now();
CREATE DOMAIN public.dom_default_func AS TEXT DEFAULT upper('abc');
CREATE DOMAIN public.dom_default_arith AS INT DEFAULT 1 + 2;

-- Named CHECK constraints.
CREATE DOMAIN public.dom_bounded AS INT
    CONSTRAINT must_be_positive CHECK (VALUE > 0)
    CONSTRAINT must_be_small CHECK (VALUE < 100);

-- Table that uses domain columns to exercise dependency ordering in the dump.
CREATE TABLE public.with_domain (
    id INT PRIMARY KEY,
    score public.dom_positive,
    email public.dom_email,
    label public.dom_nn_label,
    code public.dom_default_lit,
    bounded public.dom_bounded
);
`

func initPGDump(ctx context.Context, t test.Test, c cluster.Cluster) {
	node := c.Node(1)
	t.Status("setting up cockroach")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())

	db, err := c.ConnE(ctx, t.L(), node[0])
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Status("installing psql")
	if err := c.Install(ctx, t.L(), c.All(), "postgresql"); err != nil {
		t.Fatal(err)
	}

	t.Status("getting postgres")
	if err := repeatGitCloneE(
		ctx, t, c,
		"https://github.com/postgres/postgres.git",
		pgDumpPostgresDir,
		pgDumpSupportedTag,
		node,
	); err != nil {
		t.Fatal(err)
	}

	latestTag, err := repeatGetLatestTag(
		ctx, t, "postgres", "postgres", postgresReleaseTagRegex,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.L().Printf("Latest postgres release is %s.", latestTag)
	t.L().Printf("Supported postgres release is %s.", pgDumpSupportedTag)

	t.Status("installing build dependencies")
	if err := repeatRunE(
		ctx, t, c, node, "update apt-get",
		`sudo apt-get -qq update`,
	); err != nil {
		t.Fatal(err)
	}
	if err := repeatRunE(
		ctx, t, c, node, "install build-essential",
		`sudo apt-get -qq install -y build-essential bison flex libssl-dev`,
	); err != nil {
		t.Fatal(err)
	}

	t.Status("building pg_dump")
	// Configure and build pg_dump. The Makefile handles building libpq
	// and fe_utils via submake targets automatically. We don't use
	// repeatRunE here because build failures are deterministic, not
	// transient.
	cmd := fmt.Sprintf(
		"cd %s && "+
			"./configure --without-icu --without-readline --without-zlib --with-openssl && "+
			"make -j$(nproc)",
		pgDumpPostgresDir)
	if err := c.RunE(
		ctx, option.WithNodes(node), cmd,
	); err != nil {
		t.Fatal(err)
	}

	t.Status("creating test schema")
	if _, err := db.ExecContext(ctx, "CREATE DATABASE pgdump_src"); err != nil {
		t.Fatal(err)
	}
	srcDB, err := c.ConnE(
		ctx, t.L(), node[0], option.DBName("pgdump_src"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()

	// Create filler tables in a throwaway database to push the global
	// descriptor ID counter into the range of PostgreSQL catalog OIDs
	// (1247–6104). This verifies that the tableoid remapping in
	// pg_dump_compatibility mode doesn't cause confusion when user
	// table descriptor IDs collide with standard PG catalog OIDs
	// (e.g., pg_class=1259, pg_type=1247). Each table uses ~2
	// descriptor IDs (table + primary index), so we create enough to
	// cover the full range. Starting descriptor ID is ~106 in a fresh
	// cluster. Using a separate database keeps the filler out of the
	// pg_dump output.
	t.Status("creating filler tables to force OID collisions")
	if _, err := db.ExecContext(ctx, "CREATE DATABASE filler_db"); err != nil {
		t.Fatal(err)
	}
	fillerDB, err := c.ConnE(
		ctx, t.L(), node[0], option.DBName("filler_db"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer fillerDB.Close()
	const fillerCount = 3100
	for i := 0; i < fillerCount; i += 100 {
		var sb strings.Builder
		for j := 0; j < 100 && i+j < fillerCount; j++ {
			fmt.Fprintf(&sb, "CREATE TABLE filler_%d (id INT PRIMARY KEY);\n", i+j)
		}
		if _, err := fillerDB.ExecContext(ctx, sb.String()); err != nil {
			t.Fatal(err)
		}
	}
	t.L().Printf("created %d filler tables to force descriptor ID collisions with PG catalog OIDs", fillerCount)

	if _, err := srcDB.ExecContext(ctx, pgDumpTestSchema); err != nil {
		t.Fatal(err)
	}

	if _, err := db.ExecContext(ctx, "CREATE DATABASE pgdump_target"); err != nil {
		t.Fatal(err)
	}
}

// pgDumpExpectedPatterns lists substrings that must appear in a correct
// pg_dump output. Each entry documents which schema object it validates.
// Patterns marked as knownFailure log warnings instead of failing the
// test, providing visibility into known gaps.
var pgDumpExpectedPatterns = []struct {
	pattern      string
	description  string
	knownFailure bool
}{
	// Tables.
	{"CREATE TABLE public.basic_types", "basic_types table", false},
	{"CREATE TABLE public.constrained", "constrained table", false},
	{"CREATE TABLE public.orders", "orders table", false},
	{"CREATE TABLE public.default_exprs", "default_exprs table", false},
	{"CREATE TABLE extra.extra_table", "extra schema table", false},
	{"CREATE TABLE public.identity_test", "identity_test table", false},
	{"CREATE TABLE public.generated_cols", "generated_cols table", false},
	{"CREATE TABLE public.composite_pk", "composite_pk table", false},
	{"CREATE TABLE public.tree_node", "tree_node table", false},
	{"CREATE TABLE public.audit_log", "audit_log table", false},
	// Views.
	{"CREATE VIEW public.active_basics ", "active_basics view", false},
	{"CREATE VIEW public.active_basics_summary", "multi-level view", false},
	// Sequences.
	{"CREATE SEQUENCE", "sequence", false},
	// Enum types.
	{"CREATE TYPE public.status_type AS ENUM", "status_type enum", false},
	// Schemas.
	{"CREATE SCHEMA extra", "extra schema", false},
	// Indexes.
	{"idx_basic_name", "basic_name index", false},
	{"idx_basic_partial", "partial index", false},
	{"idx_basic_covering", "covering index", false},
	// Foreign key constraints.
	{"FOREIGN KEY", "foreign key constraint", false},
	// Self-referencing FK.
	{"self_ref_fk", "self-referencing FK", false},
	// Comments.
	{"COMMENT ON TABLE", "table comment", false},
	{"COMMENT ON COLUMN", "column comment", false},
	// Block A: functions, procedures, triggers.
	{"CREATE FUNCTION public.add_one", "add_one SQL function", false},
	{"CREATE FUNCTION public.touch_updated", "touch_updated trigger function", false},
	{"CREATE PROCEDURE public.noop", "noop procedure", false},
	{"CREATE TABLE public.with_trigger", "with_trigger table", false},
	{"CREATE TRIGGER touch_with_trigger", "touch_with_trigger trigger", false},
	// Block B: composite type, materialized view, RLS.
	{"CREATE TYPE public.addr AS", "addr composite type", false},
	{"CREATE TABLE public.with_addr", "with_addr table", false},
	{"CREATE MATERIALIZED VIEW public.basic_count", "basic_count materialized view", false},
	{"CREATE TABLE public.rls_test", "rls_test table", false},
	{"ENABLE ROW LEVEL SECURITY", "RLS enable", false},
	{"CREATE POLICY owner_policy", "owner_policy RLS policy", false},
	// Block C: index variations.
	{"idx_basic_metadata_gin", "GIN index on JSONB", false},
	{"idx_basic_tags_gin", "GIN index on text array", false},
	{"idx_basic_lower_name", "expression index", false},
	{"CREATE TABLE public.hash_sharded_table", "hash-sharded table", false},
	{"CREATE TABLE public.vector_table", "vector table", false},
	{"idx_vector_embedding", "vector index", false},
	// Block D: constraint variations.
	{"named_unique_code", "named UNIQUE constraint", false},
	{"val_positive", "NOT VALID check constraint name", false},
	{"NOT VALID", "NOT VALID keyword", false},
	{"CREATE TABLE public.fk_parent", "fk_parent table", false},
	{"CREATE TABLE public.fk_actions", "fk_actions table", false},
	{"ON DELETE CASCADE", "ON DELETE CASCADE action", false},
	{"ON DELETE SET NULL", "ON DELETE SET NULL action", false},
	{"ON DELETE SET DEFAULT", "ON DELETE SET DEFAULT action", false},
	{"ON DELETE RESTRICT", "ON DELETE RESTRICT action", false},
	{"CREATE TABLE public.composite_fk", "composite_fk multi-column FK table", false},
	{"a_to_b", "cyclic FK a_to_b", false},
	{"b_to_a", "cyclic FK b_to_a", false},
	// Block E: sequence options.
	{"CREATE SEQUENCE public.full_seq", "full_seq sequence", false},
	{"START WITH 100", "sequence START WITH", false},
	{"INCREMENT BY 5", "sequence INCREMENT BY", false},
	{"OWNED BY", "sequence OWNED BY", false},
	// Block F: type variations.
	{"CREATE TABLE public.type_zoo", "type_zoo table", false},
	{"COLLATE", "COLLATE annotation", false},
	{" inet", "INET column", false},
	{"bit(8)", "BIT(8) column", false},
	{"bit varying", "BIT VARYING column", false},
	// Block G: identity BY DEFAULT.
	{"CREATE TABLE public.identity_default", "identity_default table", false},
	{"GENERATED BY DEFAULT AS IDENTITY", "BY DEFAULT identity", false},
	// Block H: identifier quoting edge cases.
	{`CREATE TABLE public."user"`, "reserved keyword table name", false},
	{`"select"`, "reserved keyword column name", false},
	{`CREATE TABLE public."MixedCase"`, "mixed-case table name", false},
	{`"ColName"`, "mixed-case column name", false},
	{"a_very_long_identifier_name_to_test_pg_dump_quote_safety", "long identifier", false},
	// Block I: domain types.
	{"CREATE DOMAIN public.dom_plain", "dom_plain domain", false},
	{"CREATE DOMAIN public.dom_positive", "dom_positive CHECK domain", false},
	{"CREATE DOMAIN public.dom_email", "dom_email CHECK domain", false},
	{"CREATE DOMAIN public.dom_nn_label", "dom_nn_label NOT NULL domain", false},
	{"CREATE DOMAIN public.dom_default_lit", "dom_default_lit literal DEFAULT", false},
	{"DEFAULT now()", "domain DEFAULT now() (no-arg function)", false},
	{"DEFAULT upper('abc')", "domain DEFAULT with function arg", false},
	{"DEFAULT 1 + 2", "domain DEFAULT arithmetic expr", false},
	{"CREATE DOMAIN public.dom_bounded", "dom_bounded domain", false},
	{"CONSTRAINT must_be_positive", "named domain CHECK constraint", false},
	{"CONSTRAINT must_be_small", "second named domain CHECK constraint", false},
	{"CREATE TABLE public.with_domain", "with_domain table", false},
}

// verifyDumpContent checks that the dump output contains all expected
// patterns. Missing patterns are split into unexpected (test failures)
// and known failures (warnings).
func verifyDumpContent(dump string) (missing, knownMissing []string) {
	for _, p := range pgDumpExpectedPatterns {
		if !strings.Contains(dump, p.pattern) {
			msg := fmt.Sprintf(
				"%s (expected: %q)", p.description, p.pattern,
			)
			if p.knownFailure {
				knownMissing = append(knownMissing, msg)
			} else {
				missing = append(missing, msg)
			}
		}
	}
	return missing, knownMissing
}

// parseRestoreErrors extracts ERROR lines from psql stderr output.
func parseRestoreErrors(stderr string) []string {
	var errs []string
	for _, line := range strings.Split(stderr, "\n") {
		if strings.Contains(line, "ERROR:") {
			errs = append(errs, strings.TrimSpace(line))
		}
	}
	return errs
}

// normalizePGDumpOutput strips non-deterministic content from pg_dump
// output so that two dumps of equivalent schemas can be compared
// reliably.
func normalizePGDumpOutput(dump string) string {
	var lines []string
	for _, line := range strings.Split(dump, "\n") {
		// Strip pg_dump version comments that vary between runs.
		if strings.HasPrefix(line, "-- Dumped from database version") {
			continue
		}
		if strings.HasPrefix(line, "-- Dumped by pg_dump version") {
			continue
		}
		// Strip the psql \restrict/\unrestrict meta-commands that
		// pg_dump 18+ emits with a random token per dump.
		if strings.HasPrefix(line, `\restrict `) ||
			strings.HasPrefix(line, `\unrestrict `) {
			continue
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func runPGDump(ctx context.Context, t test.Test, c cluster.Cluster) {
	initPGDump(ctx, t, c)

	node := c.Node(1)
	urls, err := c.InternalPGUrl(
		ctx, t.L(), node,
		roachprod.PGURLOptions{DisallowUnsafeInternals: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(urls[0])
	if err != nil {
		t.Fatal(err)
	}

	if err := repeatRunE(
		ctx, t, c, node, "create output dir",
		fmt.Sprintf("mkdir -p %s", pgDumpOutputDir),
	); err != nil {
		t.Fatal(err)
	}

	pgDumpBin := pgDumpBinDir + "/pg_dump"

	// Environment prefix for all pg_dump/psql commands: set
	// LD_LIBRARY_PATH for the locally-built libpq, PGPASSWORD for auth,
	// and PGSSLMODE for the secure cluster.
	envPrefix := fmt.Sprintf(
		`LD_LIBRARY_PATH=%s/src/interfaces/libpq `+
			`PGPASSWORD=%s PGSSLMODE=require`,
		pgDumpPostgresDir, install.DefaultPassword,
	)

	// Common pg_dump flags: schema-only, no owner/privilege noise.
	pgDumpFlags := "--schema-only --no-owner --no-privileges --no-sync"

	// Common connection flags.
	connFlags := fmt.Sprintf(
		"--host=%s --port=%s --username=%s",
		u.Hostname(), u.Port(), install.DefaultUser,
	)

	// Dump the source database with pg_dump_compatibility enabled.
	t.Status("dumping source database")
	srcDumpFile := pgDumpOutputDir + "/source.sql"
	cmd := fmt.Sprintf(
		`%s PGOPTIONS="-c pg_dump_compatibility=postgres" %s %s `+
			`%s --dbname=pgdump_src --file=%s`,
		envPrefix, pgDumpBin, pgDumpFlags,
		connFlags, srcDumpFile)
	result, err := c.RunWithDetailsSingleNode(
		ctx, t.L(), option.WithNodes(node), cmd,
	)
	if err != nil && (result.Err == nil || rperrors.IsTransient(err)) {
		t.Fatal(err)
	}
	if err != nil {
		t.L().Printf("pg_dump stderr: %s", result.Stderr)
		t.Fatal(err)
	}

	// Restore into the target database using psql.
	t.Status("restoring into target database")
	cmd = fmt.Sprintf(
		`%s psql %s --dbname=pgdump_target -f %s`,
		envPrefix, connFlags, srcDumpFile)
	result, err = c.RunWithDetailsSingleNode(
		ctx, t.L(), option.WithNodes(node), cmd,
	)
	if err != nil && (result.Err == nil || rperrors.IsTransient(err)) {
		t.Fatal(err)
	}
	// Parse restore output for errors. psql may exit 0 even when
	// individual statements fail, so check stderr unconditionally.
	restoreErrors := parseRestoreErrors(result.Stderr)
	if len(restoreErrors) > 0 {
		t.L().Printf(
			"restore encountered %d error(s):", len(restoreErrors),
		)
		for i, e := range restoreErrors {
			t.L().Printf("  [%d] %s", i+1, e)
		}
		errFile := filepath.Join(
			t.ArtifactsDir(), "restore_errors.txt",
		)
		_ = os.WriteFile(
			errFile,
			[]byte(strings.Join(restoreErrors, "\n")),
			0644,
		)
	}
	// err here is the original error from the psql command above. It is
	// non-nil if psql exited with a non-zero status, which can happen even
	// when most statements succeeded. The earlier check already fatalled
	// on transient/unexpected errors; this one just logs the non-fatal
	// psql exit so the diff comparison below can surface any resulting
	// schema differences.
	if err != nil {
		t.L().Printf("restore stderr: %s", result.Stderr)
		t.L().Printf(
			"WARNING: restore had errors (this may be expected)",
		)
	}

	// Dump the target database.
	t.Status("dumping target database")
	targetDumpFile := pgDumpOutputDir + "/target.sql"
	cmd = fmt.Sprintf(
		`%s PGOPTIONS="-c pg_dump_compatibility=postgres" %s %s `+
			`%s --dbname=pgdump_target --file=%s`,
		envPrefix, pgDumpBin, pgDumpFlags,
		connFlags, targetDumpFile)
	result, err = c.RunWithDetailsSingleNode(
		ctx, t.L(), option.WithNodes(node), cmd,
	)
	if err != nil && (result.Err == nil || rperrors.IsTransient(err)) {
		t.Fatal(err)
	}
	if err != nil {
		t.L().Printf("pg_dump (target) stderr: %s", result.Stderr)
		t.Fatal(err)
	}

	// Fetch and compare the dumps.
	t.Status("comparing dump outputs")
	outputDir := t.ArtifactsDir()

	for _, file := range []struct {
		src, dest string
	}{
		{src: srcDumpFile, dest: "source.sql"},
		{src: targetDumpFile, dest: "target.sql"},
	} {
		if err := c.Get(
			ctx, t.L(), file.src,
			filepath.Join(outputDir, file.dest), node,
		); err != nil {
			t.L().Printf("failed to retrieve %s: %s", file.src, err)
		}
	}

	srcDump, err := os.ReadFile(filepath.Join(outputDir, "source.sql"))
	if err != nil {
		t.Fatal(err)
	}
	targetDump, err := os.ReadFile(
		filepath.Join(outputDir, "target.sql"),
	)
	if err != nil {
		t.Fatal(err)
	}

	normalizedSrc := normalizePGDumpOutput(string(srcDump))
	normalizedTarget := normalizePGDumpOutput(string(targetDump))

	// Verify the source dump contains all expected objects. This
	// catches cases where pg_dump silently omits schema objects from
	// both dumps, which the round-trip comparison cannot detect.
	t.Status("verifying dump content")
	missing, knownMissing := verifyDumpContent(normalizedSrc)
	if len(knownMissing) > 0 {
		t.L().Printf(
			"known missing objects (%d):", len(knownMissing),
		)
		for _, m := range knownMissing {
			t.L().Printf("  KNOWN MISSING: %s", m)
		}
	}
	if len(missing) > 0 {
		for _, m := range missing {
			t.L().Printf("MISSING from dump: %s", m)
		}
		t.Errorf(
			"source dump is missing %d expected object(s); "+
				"see log for details",
			len(missing),
		)
	}

	// Produce a diff between source and target dumps.
	actualDiff, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        difflib.SplitLines(normalizedSrc),
		B:        difflib.SplitLines(normalizedTarget),
		FromFile: "source.sql",
		ToFile:   "target.sql",
		Context:  3,
	})
	if err != nil {
		t.Fatalf("failed to produce diff: %s", err)
	}

	// Write the actual diff to artifacts for inspection.
	diffFile := "expected.diff"
	actualDiffPath := filepath.Join(outputDir, diffFile)
	if err := os.WriteFile(
		actualDiffPath, []byte(actualDiff), 0644,
	); err != nil {
		t.L().Printf("failed to write actual diff: %s", err)
	}

	// Compare against expected baseline.
	expectedDiff, err := os.ReadFile(pgDumpTestdataPrefix + diffFile)
	if err != nil {
		t.L().Printf(
			"failed to read expected diff (assuming empty): %s", err,
		)
		expectedDiff = []byte{}
	}

	if actualDiff != string(expectedDiff) {
		metaDiff, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A:        difflib.SplitLines(string(expectedDiff)),
			B:        difflib.SplitLines(actualDiff),
			FromFile: "testdata/pg_dump/" + diffFile,
			ToFile:   diffFile,
			Context:  3,
		})
		if err != nil {
			t.Fatalf("failed to produce meta-diff: %s", err)
		}
		t.Errorf(
			"Dump round-trip diff does not match expected output.\n%s\n"+
				"If the diff is expected, copy %s from the test "+
				"artifacts to pkg/cmd/roachtest/testdata/pg_dump/%s",
			metaDiff, diffFile, diffFile,
		)
	}
}

func registerPGDump(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:              "pg_dump",
		Owner:             registry.OwnerSQLFoundations,
		Benchmark:         false,
		Cluster:           r.MakeClusterSpec(1),
		NonReleaseBlocker: true,
		CompatibleClouds:  registry.AllExceptAWS,
		Suites:            registry.Suites(registry.Nightly),
		Leases:            registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runPGDump(ctx, t, c)
		},
	})
}
