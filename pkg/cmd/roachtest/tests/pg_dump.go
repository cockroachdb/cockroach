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

var pgDumpSupportedTag = "REL_17_4"
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
	if _, err := srcDB.ExecContext(ctx, pgDumpTestSchema); err != nil {
		t.Fatal(err)
	}

	if _, err := db.ExecContext(ctx, "CREATE DATABASE pgdump_target"); err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.L().Printf("restore stderr: %s", result.Stderr)
		// Don't fatal on restore errors — some statements may fail.
		// The diff comparison will catch any resulting schema
		// differences.
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
