// Copyright 2023 The Cockroach Authors.
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
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/pmezard/go-difflib/difflib"
)

var postgresReleaseTagRegex = regexp.MustCompile(`^REL_(?P<major>\d+)_(?P<minor>\d+)$`)
var postgresSupportedTag = "REL_16_0"
var postgresDir = "/mnt/data1/postgres"
var pgregressDir = postgresDir + "/src/test/regress"
var testdataPrefix = "./pkg/cmd/roachtest/testdata/pg_regress/"

func initPGRegress(ctx context.Context, t test.Test, c cluster.Cluster) {
	node := c.Node(1)
	t.Status("setting up cockroach")
	// pg_regress does not support ssl connections.
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(install.SecureOption(false)), c.All())

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
		ctx,
		t,
		c,
		"https://github.com/postgres/postgres.git",
		postgresDir,
		postgresSupportedTag,
		node,
	); err != nil {
		t.Fatal(err)
	}
	latestTag, err := repeatGetLatestTag(ctx, t, "postgres", "postgres", postgresReleaseTagRegex)
	if err != nil {
		t.Fatal(err)
	}
	t.L().Printf("Latest postgres release is %s.", latestTag)
	t.L().Printf("Supported release is %s.", postgresSupportedTag)

	if err := repeatRunE(
		ctx, t, c, node, "update apt-get", `sudo apt-get -qq update`,
	); err != nil {
		t.Fatal(err)
	}

	if err := repeatRunE(
		ctx,
		t,
		c,
		node,
		"install dependencies",
		`sudo apt-get -qq install build-essential`,
	); err != nil {
		t.Fatal(err)
	}

	t.Status("modifying test scripts")
	for _, p := range patches {
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			p.filename,
			fmt.Sprintf(`cd %s && \
	echo '%s' | git apply --ignore-whitespace -`, postgresDir, p.patch),
		); err != nil {
			t.Fatal(err)
		}
	}

	for _, cmd := range []string{
		`CREATE DATABASE root;`,
		`SET CLUSTER SETTING sql.defaults.experimental_temporary_tables.enabled=true`,
		`SET create_table_with_schema_locked=false`,
		`ALTER ROLE ALL SET create_table_with_schema_locked=false`,
		`CREATE USER test_admin`,
		`GRANT admin TO test_admin`,
	} {
		if _, err := db.ExecContext(ctx, cmd); err != nil {
			t.Fatal(err)
		}
	}

	urls, err := c.InternalPGUrl(ctx, t.L(), node, roachprod.PGURLOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pgurl := urls[0]

	// We execute the test setup before running pg_regress because some of
	// the tables require copying input data. Since CRDB only supports
	// COPY FROM STDIN, we need to do this outside the test.
	t.L().Printf("Running test setup: psql %s -a -f %s/sql/test_setup.sql", pgurl, pgregressDir)
	err = c.RunE(ctx, option.WithNodes(node), fmt.Sprintf(`psql %s -a -f %s/sql/test_setup.sql`, pgurl, pgregressDir))
	if err != nil {
		t.Fatal(err)
	}

	// Copy data into some of the tables. This needs to be run after
	// test_setup.sql.
	dataTbls := [...]string{"onek", "tenk1"}
	dataFiles := [...]string{"onek", "tenk"}
	for i, tbl := range dataTbls {
		err = c.RunE(ctx, option.WithNodes(node), fmt.Sprintf(`cat %s/data/%s.data | psql %s -c "COPY %s FROM STDIN"`, pgregressDir, dataFiles[i], pgurl, tbl))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func runPGRegress(ctx context.Context, t test.Test, c cluster.Cluster) {
	// We could have run this test locally if we changed postgresDir to
	// something like /tmp/postgres, but local runs on a gceworker produce
	// slightly different results than the ones from the nightlies, so we
	// explicitly prohibit the local runs for consistency.
	if c.IsLocal() {
		t.Fatal("cannot be run in local mode")
	}

	initPGRegress(ctx, t, c)

	node := c.Node(1)
	urls, err := c.InternalPGUrl(ctx, t.L(), node, roachprod.PGURLOptions{})
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(urls[0])
	if err != nil {
		t.Fatal(err)
	}
	t.L().Printf("Parsed URL: %s\nHostname: %s\nPort: %s\n", urls[0], u.Hostname(), u.Port())

	t.Status("running the pg_regress test suite")

	// Tests are ordered in the approximate order that pg_regress does
	// when all tests are run with default settings (groups are run in
	// parallel when run against postgres).
	// The following tests have been removed instead of commented out,
	// because there are no plans to support the features under test:
	// - vacuum,
	// - txid,
	// - guc.
	tests := []string{
		"varchar",
		"char",
		"int2",
		"text",
		"boolean",
		"name",
		"int4",
		"oid",
		"int8",
		"bit",
		"uuid",
		"float4",
		"pg_lsn",
		"regproc",
		// TODO(#41578): Reenable when money types are supported.
		// "money",
		"enum",
		"float8",
		"numeric",
		"rangetypes",
		"md5",
		"numerology",
		"timetz",
		// TODO(#45813): Reenable when macaddr is supported.
		// "macaddr",
		// "macaddr8",
		"strings",
		"time",
		"date",
		"inet",
		"interval",
		"multirangetypes",
		"timestamp",
		"timestamptz",
		// TODO(#21286): Reenable when more geometric types are supported.
		// "point",
		// "circle",
		// "line",
		// "lseg",
		// "path",
		// "box",
		// "polygon",
		"unicode",
		"misc_sanity",
		"tstypes",
		"comments",
		"geometry",
		"xid",
		"horology",
		"type_sanity",
		"expressions",
		"mvcc",
		"regex",
		// TODO(#123651): re-enable when pg_catalog.pg_proc is fixed up.
		// "opr_sanity",
		"copyselect",
		"copydml",
		"copy",
		"insert_conflict",
		"insert",
		"create_function_c",
		"create_operator",
		"create_type",
		"create_procedure",
		"create_schema",
		"create_misc",
		"create_table",
		"index_including",
		"index_including_gist",
		"create_view",
		"create_index_spgist",
		"create_index",
		"create_cast",
		"create_aggregate",
		"hash_func",
		"select",
		"roleattributes",
		"drop_if_exists",
		"typed_table",
		"errors",
		"create_function_sql",
		"create_am",
		"infinite_recurse",
		"constraints",
		"updatable_views",
		"triggers",
		"random",
		"delete",
		"inherit",
		"select_distinct_on",
		"select_having",
		"select_implicit",
		"namespace",
		"case",
		"select_into",
		// TODO(#137549): prepared transactions cause hanging.
		//"prepared_xacts",
		"select_distinct",
		// TODO(#117689): Transactions test causes server crash.
		// "transactions",
		"portals",
		"union",
		"subselect",
		"arrays",
		"hash_index",
		"update",
		"aggregates",
		"join",
		"btree_index",
		"init_privs",
		"security_label",
		"drop_operator",
		"tablesample",
		"lock",
		"collate",
		"replica_identity",
		"object_address",
		"password",
		"identity",
		"groupingsets",
		"matview",
		"generated",
		"spgist",
		"rowsecurity",
		"gin",
		"gist",
		"brin",
		"join_hash",
		"privileges",
		"brin_bloom",
		"brin_multi",
		"async",
		"dbsize",
		"alter_operator",
		"tidrangescan",
		"collate.icu.utf8",
		"tsrf",
		"alter_generic",
		"tidscan",
		"tid",
		"create_role",
		"misc",
		"sysviews",
		"misc_functions",
		"incremental_sort",
		"merge",
		"create_table_like",
		"collate.linux.utf8",
		"collate.windows.win1252",
		"amutils",
		"psql_crosstab",
		"rules",
		// TODO(#117689): Psql test causes server crash.
		// "psql",
		"stats_ext",
		"subscription",
		"publication",
		"portals_p2",
		"dependency",
		"xmlmap",
		"advisory_lock",
		"select_views",
		"combocid",
		"tsdicts",
		"functional_deps",
		"equivclass",
		"bitmapops",
		"window",
		"indirect_toast",
		"tsearch",
		"cluster",
		"foreign_data",
		"foreign_key",
		"json_encoding",
		"jsonpath_encoding",
		"jsonpath",
		"sqljson",
		"json",
		"jsonb_jsonpath",
		"jsonb",
		"prepare",
		"limit",
		"returning",
		"plancache",
		"conversion",
		"xml",
		"temp",
		// TODO(#28296): Reenable copy2 when triggers and COPY [view] FROM are supported.
		// "copy2",
		"rangefuncs",
		"sequence",
		"truncate",
		"alter_table",
		"polymorphism",
		"rowtypes",
		"domain",
		"largeobject",
		// TODO(#113625): Reenable with when aggregation bug is fixed.
		// "with",
		"plpgsql",
		"hash_part",
		"partition_info",
		"reloptions",
		"explain",
		"memoize",
		"compression",
		"partition_aggregate",
		"indexing",
		"partition_join",
		"partition_prune",
		"tuplesort",
		"stats",
		"oidjoins",
		"event_trigger",
	}

	// Perform the setup only once.
	cmd := fmt.Sprintf("cd %s && "+
		"./configure --without-icu --without-readline --without-zlib && "+
		"cd %s && "+
		"make ",
		postgresDir, pgregressDir)
	result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(node), cmd)
	// Fatal for a roachprod or transient error. A roachprod error is when
	// result.Err==nil. Proceed for any other (command) errors
	if err != nil && (result.Err == nil || rperrors.IsTransient(err)) {
		t.Fatal(err)
	}

	outputDir := t.ArtifactsDir()
	var expected, actual []string

	// We'll run each test file separately to make reviewing the diff easier.
	for testIdx, testFile := range tests {
		// psql was installed in /usr/bin/psql, so use the prefix for bindir.
		// TODO(yuzefovich): figure out what's wrong with a few tests like
		// collate.linux.utf8 and unicode where we use "*_1.out" as expected.
		cmd = fmt.Sprintf("cd %s && "+
			"./pg_regress --bindir=/usr/bin --host=%s --port=%s --user=test_admin "+
			"--dbname=root --use-existing %s",
			pgregressDir, u.Hostname(), u.Port(), testFile)
		result, err = c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(node), cmd)
		// Fatal for a roachprod or transient error. A roachprod error is when
		// result.Err==nil. Proceed for any other (command) errors
		if err != nil && (result.Err == nil || rperrors.IsTransient(err)) {
			t.Fatal(err)
		}

		t.Status("collecting the test results for test '", testFile, "' (", testIdx, "/", len(tests), ")")
		diffFile := testFile + ".diffs"
		diffFilePath := filepath.Join(outputDir, diffFile)
		tmpFile := diffFile + ".tmp"
		tmpFilePath := filepath.Join(outputDir, tmpFile)

		// Copy the test result files.
		for _, file := range []struct {
			src, dest string
		}{
			{src: "regression.out", dest: testFile + ".out"},
			{src: "regression.diffs", dest: tmpFile},
		} {
			if err := c.Get(
				ctx, t.L(),
				filepath.Join(pgregressDir, file.src),
				filepath.Join(outputDir, file.dest),
				node,
			); err != nil {
				t.L().Printf("failed to retrieve %s: %s", file.src, err)
			}
		}

		// Replace specific versions in URIs with a generic "_version_".
		actualB, err := os.ReadFile(tmpFilePath)
		if err != nil {
			t.L().Printf("Failed to read %s: %s", tmpFilePath, err)
		}
		issueURI := regexp.MustCompile(`https://go\.crdb\.dev/issue-v/(\d+)/[^/|^\s]+`)
		actualB = issueURI.ReplaceAll(actualB, []byte("https://go.crdb.dev/issue-v/$1/_version_"))
		docsURI := regexp.MustCompile(`https://www\.cockroachlabs.com/docs/[^/|^\s]+`)
		actualB = docsURI.ReplaceAll(actualB, []byte("https://www.cockroachlabs.com/docs/_version_"))

		// Remove table ID from some errors (to reduce the diff churn).
		for _, re := range []*regexp.Regexp{
			regexp.MustCompile(`(.*ERROR:.*relation.*".*") \(\d+\)(: unimplemented: primary key dropped without subsequent addition of new primary key in same transaction*)`),
			regexp.MustCompile(`(.*ERROR:.*relation.*".*") \(\d+\)(: duplicate constraint name: *)`),
			regexp.MustCompile(`(.*ERROR:.*relation.*".*") \(\d+\)(: duplicate column name: *)`),
			regexp.MustCompile(`(.*ERROR:.*relation.*".*") \(\d+\)(: conflicting NULL/NOT NULL declarations for column *)`),
			regexp.MustCompile(`(.*ERROR:.*relation.*".*") \(\d+\)(: table must contain at least*)`),
		} {
			actualB = re.ReplaceAll(actualB, []byte("$1$2"))
		}

		err = os.WriteFile(diffFilePath, actualB, 0644)
		if err != nil {
			t.L().Printf("Failed to write %s: %s", diffFilePath, err)
		}
		actual = append(actual, string(actualB))
		if err = os.Remove(tmpFilePath); err != nil {
			t.L().Printf("Failed to remove %s: %s", tmpFilePath, err)
		}

		expectedB, err := os.ReadFile(testdataPrefix + diffFile)
		if err != nil {
			t.L().Printf("Failed to read %s: %s", testdataPrefix+diffFile, err)
		}
		expected = append(expected, string(expectedB))
	}

	// Compare the regression diffs.
	for i := range expected {
		exp, act := expected[i], actual[i]
		if exp != act {
			testFile := tests[i] + ".diffs"
			diff, diffErr := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
				A:        difflib.SplitLines(exp),
				B:        difflib.SplitLines(act),
				FromFile: "testdata/" + testFile,
				ToFile:   testFile,
				Context:  3,
			})
			if diffErr != nil {
				t.Fatalf("failed to produce diff: %s", diffErr)
			}
			t.Errorf("Regression diffs do not match expected output for %[1]s.\n%[2]s\n"+
				"If the diff is expected, copy %[1]s.diffs from the test artifacts to pkg/cmd/roachtest/testdata/%[1]s.diffs",
				tests[i], diff)
		}
	}
}

func registerPGRegress(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:      "pg_regress",
		Owner:     registry.OwnerSQLQueries,
		Benchmark: false,
		Cluster:   r.MakeClusterSpec(1 /* nodeCount */),
		// At the moment, we have a very large deviation from postgres, also
		// some diffs include line numbers, so we don't treat failures as
		// blockers for now.
		NonReleaseBlocker: true,
		CompatibleClouds:  registry.AllExceptAWS,
		Suites:            registry.Suites(registry.Weekly),
		Leases:            registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runPGRegress(ctx, t, c)
		},
	})
}

type regressPatch struct {
	filename string
	patch    string
}

// Diffs on files in the original pg_regress suite that help with setup
// and reduce non-determinism due to CRDB implementation.
// Note: Due to issues applying patches including single quotes, replace
// single quotes ' with '"'"'.
var patches = []regressPatch{
	{"pg_regress.c",
		`diff --git a/src/test/regress/pg_regress.c b/src/test/regress/pg_regress.c
index ec67588cf5..f944644e33 100644
--- a/src/test/regress/pg_regress.c
+++ b/src/test/regress/pg_regress.c
@@ -762,25 +762,6 @@ initialize_environment(void)
   * Set timezone and datestyle for datetime-related tests
   */
  setenv("PGTZ", "PST8PDT", 1);
-	setenv("PGDATESTYLE", "Postgres, MDY", 1);
-
-	/*
-	 * Likewise set intervalstyle to ensure consistent results.  This is a bit
-	 * more painful because we must use PGOPTIONS, and we want to preserve the
-	 * user'"'"'s ability to set other variables through that.
-	 */
-	{
-		const char *my_pgoptions = "-c intervalstyle=postgres_verbose";
-		const char *old_pgoptions = getenv("PGOPTIONS");
-		char	   *new_pgoptions;
-
-		if (!old_pgoptions)
-			old_pgoptions = "";
-		new_pgoptions = psprintf("%s %s",
-								 old_pgoptions, my_pgoptions);
-		setenv("PGOPTIONS", new_pgoptions, 1);
-		free(new_pgoptions);
-	}
 
  if (temp_instance)
  {
@@ -1520,15 +1501,15 @@ results_differ(const char *testname, const char *resultsfile, const char *defaul
        if (difffile)
        {
                fprintf(difffile,
-                               "diff %s %s %s\n",
-                               pretty_diff_opts, best_expect_file, resultsfile);
+                               "diff %s --label=%s --label=%s %s %s\n",
+                               pretty_diff_opts,best_expect_file, resultsfile, best_expect_file, resultsfile);
                fclose(difffile);
        }

        /* Run diff */
        snprintf(cmd, sizeof(cmd),
-                        "diff %s \"%s\" \"%s\" >> \"%s\"",
-                        pretty_diff_opts, best_expect_file, resultsfile, difffilename);
+                        "diff %s --label=%s --label=%s  \"%s\" \"%s\" >> \"%s\"",
+                        pretty_diff_opts,best_expect_file, resultsfile, best_expect_file, resultsfile, difffilename);
        run_diff(cmd, difffilename);

        unlink(diff);
@@ -1974,14 +1957,6 @@ create_database(const char *dbname)
  else
    psql_add_command(buf, "CREATE DATABASE \"%s\" TEMPLATE=template0%s", dbname,
              (nolocale) ? " LOCALE='"'"'C'"'"'" : "");
-	psql_add_command(buf,
-					 "ALTER DATABASE \"%s\" SET lc_messages TO '"'"'C'"'"';"
-					 "ALTER DATABASE \"%s\" SET lc_monetary TO '"'"'C'"'"';"
-					 "ALTER DATABASE \"%s\" SET lc_numeric TO '"'"'C'"'"';"
-					 "ALTER DATABASE \"%s\" SET lc_time TO '"'"'C'"'"';"
-					 "ALTER DATABASE \"%s\" SET bytea_output TO '"'"'hex'"'"';"
-					 "ALTER DATABASE \"%s\" SET timezone_abbreviations TO '"'"'Default'"'"';",
-					 dbname, dbname, dbname, dbname, dbname, dbname);
  psql_end_command(buf, "postgres");
 
  /*
`},
	{"test_setup.sql",
		`diff --git a/src/test/regress/sql/test_setup.sql b/src/test/regress/sql/test_setup.sql
index 1b2d434683..d371fe3f63 100644
--- a/src/test/regress/sql/test_setup.sql
+++ b/src/test/regress/sql/test_setup.sql
@@ -3,11 +3,11 @@
 --
 
 -- directory paths and dlsuffix are passed to us in environment variables
-\getenv abs_srcdir PG_ABS_SRCDIR
-\getenv libdir PG_LIBDIR
-\getenv dlsuffix PG_DLSUFFIX
+-- \getenv abs_srcdir PG_ABS_SRCDIR
+-- \getenv libdir PG_LIBDIR
+-- \getenv dlsuffix PG_DLSUFFIX
 
-\set regresslib :libdir '"'"'/regress'"'"' :dlsuffix
+-- \set regresslib :libdir '"'"'/regress'"'"' :dlsuffix
 
 --
 -- synchronous_commit=off delays when hint bits may be set. Some plans change
@@ -15,7 +15,10 @@
 -- influenced by the delayed hint bits. Force synchronous_commit=on to avoid
 -- that source of variability.
 --
-SET synchronous_commit = on;
+-- SET synchronous_commit = on;
+
+-- Many tests use temp tables.
+SET experimental_enable_temp_tables = 'on';
 
 --
 -- Postgres formerly made the public schema read/write by default,
@@ -24,8 +27,8 @@ SET synchronous_commit = on;
 GRANT ALL ON SCHEMA public TO public;
 
 -- Create a tablespace we can use in tests.
-SET allow_in_place_tablespaces = true;
-CREATE TABLESPACE regress_tblspace LOCATION '"'"''"'"';
+-- SET allow_in_place_tablespaces = true;
+-- CREATE TABLESPACE regress_tblspace LOCATION '"'"''"'"';
 
 --
 -- These tables have traditionally been referenced by many tests,
@@ -42,7 +45,7 @@ INSERT INTO CHAR_TBL (f1) VALUES
   ('"'"'ab'"'"'),
   ('"'"'abcd'"'"'),
   ('"'"'abcd    '"'"');
-VACUUM CHAR_TBL;
+-- VACUUM CHAR_TBL;
 
 CREATE TABLE FLOAT8_TBL(f1 float8);
 
@@ -52,7 +55,7 @@ INSERT INTO FLOAT8_TBL(f1) VALUES
   ('"'"'-1004.30'"'"'),
   ('"'"'-1.2345678901234e+200'"'"'),
   ('"'"'-1.2345678901234e-200'"'"');
-VACUUM FLOAT8_TBL;
+-- VACUUM FLOAT8_TBL;
 
 CREATE TABLE INT2_TBL(f1 int2);
 
@@ -62,7 +65,7 @@ INSERT INTO INT2_TBL(f1) VALUES
   ('"'"'    -1234'"'"'),
   ('"'"'32767'"'"'),  -- largest and smallest values
   ('"'"'-32767'"'"');
-VACUUM INT2_TBL;
+-- VACUUM INT2_TBL;
 
 CREATE TABLE INT4_TBL(f1 int4);
 
@@ -72,7 +75,7 @@ INSERT INTO INT4_TBL(f1) VALUES
   ('"'"'    -123456'"'"'),
   ('"'"'2147483647'"'"'),  -- largest and smallest values
   ('"'"'-2147483647'"'"');
-VACUUM INT4_TBL;
+-- VACUUM INT4_TBL;
 
 CREATE TABLE INT8_TBL(q1 int8, q2 int8);
 
@@ -82,7 +85,7 @@ INSERT INTO INT8_TBL VALUES
   ('"'"'4567890123456789'"'"','"'"'123'"'"'),
   (+4567890123456789,'"'"'4567890123456789'"'"'),
   ('"'"'+4567890123456789'"'"','"'"'-4567890123456789'"'"');
-VACUUM INT8_TBL;
+-- VACUUM INT8_TBL;
 
 CREATE TABLE POINT_TBL(f1 point);
 
@@ -104,7 +107,7 @@ CREATE TABLE TEXT_TBL (f1 text);
 INSERT INTO TEXT_TBL VALUES
   ('"'"'doh!'"'"'),
   ('"'"'hi de ho neighbor'"'"');
-VACUUM TEXT_TBL;
+-- VACUUM TEXT_TBL;
 
 CREATE TABLE VARCHAR_TBL(f1 varchar(4));
 
@@ -113,7 +116,7 @@ INSERT INTO VARCHAR_TBL (f1) VALUES
   ('"'"'ab'"'"'),
   ('"'"'abcd'"'"'),
   ('"'"'abcd    '"'"');
-VACUUM VARCHAR_TBL;
+-- VACUUM VARCHAR_TBL;
 
 CREATE TABLE onek (
  unique1		int4,
@@ -134,12 +137,12 @@ CREATE TABLE onek (
  string4		name
 );
 
-\set filename :abs_srcdir '"'"'/data/onek.data'"'"'
-COPY onek FROM :'"'"'filename'"'"';
-VACUUM ANALYZE onek;
+-- \set filename :abs_srcdir '"'"'/data/onek.data'"'"'
+-- COPY onek FROM :'"'"'filename'"'"';
+-- VACUUM ANALYZE onek;
 
 CREATE TABLE onek2 AS SELECT * FROM onek;
-VACUUM ANALYZE onek2;
+-- VACUUM ANALYZE onek2;
 
 CREATE TABLE tenk1 (
  unique1		int4,
@@ -160,12 +163,12 @@ CREATE TABLE tenk1 (
  string4		name
 );
 
-\set filename :abs_srcdir '"'"'/data/tenk.data'"'"'
-COPY tenk1 FROM :'"'"'filename'"'"';
-VACUUM ANALYZE tenk1;
+-- \set filename :abs_srcdir '"'"'/data/tenk.data'"'"'
+-- COPY tenk1 FROM :'"'"'filename'"'"';
+-- VACUUM ANALYZE tenk1;
 
 CREATE TABLE tenk2 AS SELECT * FROM tenk1;
-VACUUM ANALYZE tenk2;
+-- VACUUM ANALYZE tenk2;
 
 CREATE TABLE person (
  name 		text,
@@ -173,43 +176,43 @@ CREATE TABLE person (
  location 	point
 );
 
-\set filename :abs_srcdir '"'"'/data/person.data'"'"'
-COPY person FROM :'"'"'filename'"'"';
-VACUUM ANALYZE person;
+-- \set filename :abs_srcdir '"'"'/data/person.data'"'"'
+-- COPY person FROM :'"'"'filename'"'"';
+-- VACUUM ANALYZE person;
 
 CREATE TABLE emp (
  salary 		int4,
  manager 	name
 ) INHERITS (person);
 
-\set filename :abs_srcdir '"'"'/data/emp.data'"'"'
-COPY emp FROM :'"'"'filename'"'"';
-VACUUM ANALYZE emp;
+-- \set filename :abs_srcdir '"'"'/data/emp.data'"'"'
+-- COPY emp FROM :'"'"'filename'"'"';
+-- VACUUM ANALYZE emp;
 
 CREATE TABLE student (
  gpa 		float8
 ) INHERITS (person);
 
-\set filename :abs_srcdir '"'"'/data/student.data'"'"'
-COPY student FROM :'"'"'filename'"'"';
-VACUUM ANALYZE student;
+-- \set filename :abs_srcdir '"'"'/data/student.data'"'"'
+-- COPY student FROM :'"'"'filename'"'"';
+-- VACUUM ANALYZE student;
 
 CREATE TABLE stud_emp (
  percent 	int4
 ) INHERITS (emp, student);
 
-\set filename :abs_srcdir '"'"'/data/stud_emp.data'"'"'
-COPY stud_emp FROM :'"'"'filename'"'"';
-VACUUM ANALYZE stud_emp;
+-- \set filename :abs_srcdir '"'"'/data/stud_emp.data'"'"'
+-- COPY stud_emp FROM :'"'"'filename'"'"';
+-- VACUUM ANALYZE stud_emp;
 
 CREATE TABLE road (
  name		text,
  thepath 	path
 );
 
-\set filename :abs_srcdir '"'"'/data/streets.data'"'"'
-COPY road FROM :'"'"'filename'"'"';
-VACUUM ANALYZE road;
+-- \set filename :abs_srcdir '"'"'/data/streets.data'"'"'
+-- COPY road FROM :'"'"'filename'"'"';
+-- VACUUM ANALYZE road;
 
 CREATE TABLE ihighway () INHERITS (road);
 
@@ -217,7 +220,7 @@ INSERT INTO ihighway
    SELECT *
    FROM ONLY road
    WHERE name ~ '"'"'I- .*'"'"';
-VACUUM ANALYZE ihighway;
+-- VACUUM ANALYZE ihighway;
 
 CREATE TABLE shighway (
  surface		text
@@ -227,7 +230,7 @@ INSERT INTO shighway
    SELECT *, '"'"'asphalt'"'"'
    FROM ONLY road
    WHERE name ~ '"'"'State Hwy.*'"'"';
-VACUUM ANALYZE shighway;
+-- VACUUM ANALYZE shighway;
 
 --
 -- We must have some enum type in the database for opr_sanity and type_sanity.
@@ -270,7 +273,7 @@ CREATE FUNCTION get_columns_length(oid[])
 create function part_hashint4_noop(value int4, seed int8)
     returns int8 as $$
     select value + seed;
-    $$ language sql strict immutable parallel safe;
+    $$ language sql strict immutable;
 
 create operator class part_test_int4_ops for type int4 using hash as
     operator 1 =,
@@ -279,7 +282,7 @@ create operator class part_test_int4_ops for type int4 using hash as
 create function part_hashtext_length(value text, seed int8)
     returns int8 as $$
     select length(coalesce(value, '"'"''"'"'))::int8
-    $$ language sql strict immutable parallel safe;
+    $$ language sql strict immutable;
 
 create operator class part_test_text_ops for type text using hash as
     operator 1 =,
@@ -290,12 +293,12 @@ create operator class part_test_text_ops for type text using hash as
 -- mostly avoid so that the tests will pass in FIPS mode.
 --
 
-create function fipshash(bytea)
-    returns text
-    strict immutable parallel safe leakproof
-    return substr(encode(sha256($1), '"'"'hex'"'"'), 1, 32);
+-- create function fipshash(bytea)
+--     returns text
+--     strict immutable leakproof
+--     return substr(encode(sha256($1), '"'"'hex'"'"'), 1, 32);
 
-create function fipshash(text)
-    returns text
-    strict immutable parallel safe leakproof
-    return substr(encode(sha256($1::bytea), '"'"'hex'"'"'), 1, 32);
+-- create function fipshash(text)
+--     returns text
+--     strict immutable leakproof
+--     return substr(encode(sha256($1::bytea), '"'"'hex'"'"'), 1, 32);
`},
	// Add ordering for some statements.
	// TODO(#123705): remove the patch to comment out a query against
	// pg_catalog.pg_am vtable.
	// TODO(#123706): remove the patch to comment out a query against
	// pg_catalog.pg_attribute vtable.
	// TODO(#146255): remove the patch to include ORDER BY clause for the query
	// with "Text conversion routines must be provided." comment in pg_regress.
	{"type_sanity.sql", `diff --git a/src/test/regress/sql/type_sanity.sql b/src/test/regress/sql/type_sanity.sql
index 79ec410a6c..417d3dcdb2 100644
--- a/src/test/regress/sql/type_sanity.sql
+++ b/src/test/regress/sql/type_sanity.sql
@@ -23,7 +23,8 @@ WHERE t1.typnamespace = 0 OR
     (t1.typtype not in ('"'"'b'"'"', '"'"'c'"'"', '"'"'d'"'"', '"'"'e'"'"', '"'"'m'"'"', '"'"'p'"'"', '"'"'r'"'"')) OR
     NOT t1.typisdefined OR
     (t1.typalign not in ('"'"'c'"'"', '"'"'s'"'"', '"'"'i'"'"', '"'"'d'"'"')) OR
-    (t1.typstorage not in ('"'"'p'"'"', '"'"'x'"'"', '"'"'e'"'"', '"'"'m'"'"'));
+    (t1.typstorage not in ('"'"'p'"'"', '"'"'x'"'"', '"'"'e'"'"', '"'"'m'"'"'))
+ORDER BY t1.oid;

 -- Look for "pass by value" types that can'"'"'t be passed by value.

@@ -33,7 +34,8 @@ WHERE t1.typbyval AND
     (t1.typlen != 1 OR t1.typalign != '"'"'c'"'"') AND
     (t1.typlen != 2 OR t1.typalign != '"'"'s'"'"') AND
     (t1.typlen != 4 OR t1.typalign != '"'"'i'"'"') AND
-    (t1.typlen != 8 OR t1.typalign != '"'"'d'"'"');
+    (t1.typlen != 8 OR t1.typalign != '"'"'d'"'"')
+ORDER BY t1.oid;

 -- Look for "toastable" types that aren'"'"'t varlena.

@@ -91,7 +93,8 @@ WHERE t1.typtype = 'r' AND

 SELECT t1.oid, t1.typname
 FROM pg_type as t1
-WHERE (t1.typinput = 0 OR t1.typoutput = 0);
+WHERE (t1.typinput = 0 OR t1.typoutput = 0)
+ORDER BY t1.oid;

 -- Check for bogus typinput routines

@@ -288,7 +291,8 @@ WHERE t1.typelem = t2.oid AND NOT

 SELECT t1.oid, t1.typname, t2.oid, t2.typname
 FROM pg_type AS t1, pg_type AS t2
-WHERE t1.typarray = t2.oid AND NOT (t1.typdelim = t2.typdelim);
+WHERE t1.typarray = t2.oid AND NOT (t1.typdelim = t2.typdelim)
+ORDER BY t1.oid;

 -- Look for array types whose typalign isn'"'"'t sufficient

@@ -385,10 +388,10 @@ WHERE pc.relkind IN ('"'"'i'"'"', '"'"'I'"'"') and
     pa.amtype != '"'"'i'"'"';

 -- Tables, matviews etc should have AMs of type '"'"'t'"'"'
-SELECT pc.oid, pc.relname, pa.amname, pa.amtype
-FROM pg_class as pc JOIN pg_am AS pa ON (pc.relam = pa.oid)
-WHERE pc.relkind IN ('"'"'r'"'"', '"'"'t'"'"', '"'"'m'"'"') and
-    pa.amtype != '"'"'t'"'"';
+-- SELECT pc.oid, pc.relname, pa.amname, pa.amtype
+-- FROM pg_class as pc JOIN pg_am AS pa ON (pc.relam = pa.oid)
+-- WHERE pc.relkind IN ('"'"'r'"'"', '"'"'t'"'"', '"'"'m'"'"') and
+--     pa.amtype != '"'"'t'"'"';

 -- **************** pg_attribute ****************

@@ -402,9 +405,9 @@ WHERE a1.attrelid = 0 OR a1.atttypid = 0 OR a1.attnum = 0 OR

 -- Cross-check attnum against parent relation

-SELECT a1.attrelid, a1.attname, c1.oid, c1.relname
-FROM pg_attribute AS a1, pg_class AS c1
-WHERE a1.attrelid = c1.oid AND a1.attnum > c1.relnatts;
+-- SELECT a1.attrelid, a1.attname, c1.oid, c1.relname
+-- FROM pg_attribute AS a1, pg_class AS c1
+-- WHERE a1.attrelid = c1.oid AND a1.attnum > c1.relnatts;

 -- Detect missing pg_attribute entries: should have as many non-system
 -- attributes as parent relation expects
`},
	// Add ordering for some statements.
	{"opr_sanity.sql", `diff --git a/src/test/regress/sql/opr_sanity.sql b/src/test/regress/sql/opr_sanity.sql
index e2d2c70d70..3705030c1f 100644
--- a/src/test/regress/sql/opr_sanity.sql
+++ b/src/test/regress/sql/opr_sanity.sql
@@ -35,7 +35,8 @@ WHERE p1.prolang = 0 OR p1.prorettype = 0 OR
        CASE WHEN proretset THEN prorows <= 0 ELSE prorows != 0 END OR
        prokind NOT IN ('"'"'f'"'"', '"'"'a'"'"', '"'"'w'"'"', '"'"'p'"'"') OR
        provolatile NOT IN ('"'"'i'"'"', '"'"'s'"'"', '"'"'v'"'"') OR
-       proparallel NOT IN ('"'"'s'"'"', '"'"'r'"'"', '"'"'u'"'"');
+       proparallel NOT IN ('"'"'s'"'"', '"'"'r'"'"', '"'"'u'"'"')
+ORDER BY p1.oid;
 
 -- prosrc should never be null; it can be empty only if prosqlbody isn'"'"'t null
 SELECT p1.oid, p1.proname
@@ -80,7 +81,8 @@ FROM pg_proc AS p1, pg_proc AS p2
 WHERE p1.oid != p2.oid AND
     p1.proname = p2.proname AND
     p1.pronargs = p2.pronargs AND
-    p1.proargtypes = p2.proargtypes;
+    p1.proargtypes = p2.proargtypes
+ORDER BY p1.oid, p2.oid;
 
 -- Considering only built-in procs (prolang = 12), look for multiple uses
 -- of the same internal function (ie, matching prosrc fields).  It'"'"'s OK to
@@ -103,7 +105,8 @@ WHERE p1.oid < p2.oid AND
      p1.proisstrict != p2.proisstrict OR
      p1.proretset != p2.proretset OR
      p1.provolatile != p2.provolatile OR
-     p1.pronargs != p2.pronargs);
+     p1.pronargs != p2.pronargs)
+ORDER BY p1.oid, p2.oid;
 
 -- Look for uses of different type OIDs in the argument/result type fields
 -- for different aliases of the same built-in function.
@@ -460,7 +463,8 @@ WHERE castsource = casttarget AND castfunc = 0;
 
 SELECT c.*
 FROM pg_cast c, pg_proc p
-WHERE c.castfunc = p.oid AND p.pronargs < 2 AND castsource = casttarget;
+WHERE c.castfunc = p.oid AND p.pronargs < 2 AND castsource = casttarget
+ORDER BY c.oid;
 
 -- Look for cast functions that don'"'"'t have the right signature.  The
 -- argument and result types in pg_proc must be the same as, or binary
@@ -567,7 +571,8 @@ SELECT o1.oid, o1.oprname
 FROM pg_operator as o1
 WHERE (o1.oprleft = 0 and o1.oprkind != '"'"'l'"'"') OR
     (o1.oprleft != 0 and o1.oprkind = '"'"'l'"'"') OR
-    o1.oprright = 0;
+    o1.oprright = 0
+ORDER BY o1.oid;
 
 -- Look for conflicting operator definitions (same names and input datatypes).
`},
	// Serial is not deterministic.
	{"copyselect.sql", `diff --git a/src/test/regress/sql/copyselect.sql b/src/test/regress/sql/copyselect.sql
index e32a4f8e38..a5955c8752 100644
--- a/src/test/regress/sql/copyselect.sql
+++ b/src/test/regress/sql/copyselect.sql
@@ -1,7 +1,8 @@
 --
 -- Test cases for COPY (select) TO
 --
-create table test1 (id serial, t text);
+create sequence id_seq;
+create table test1 (id int default nextval('"'"'id_seq'"'"'), t text);
 insert into test1 (t) values ('"'"'a'"'"');
 insert into test1 (t) values ('"'"'b'"'"');
 insert into test1 (t) values ('"'"'c'"'"');
`},
	{"copydml.sql", `diff --git a/src/test/regress/sql/copydml.sql b/src/test/regress/sql/copydml.sql
index 4578342253..b4f0f4f645 100644
--- a/src/test/regress/sql/copydml.sql
+++ b/src/test/regress/sql/copydml.sql
@@ -1,7 +1,8 @@
 --
 -- Test cases for COPY (INSERT/UPDATE/DELETE) TO
 --
-create table copydml_test (id serial, t text);
+create sequence id_seq;
+create table copydml_test (id int default nextval('"'"'id_seq'"'"'), t text);
 insert into copydml_test (t) values ('"'"'a'"'"');
 insert into copydml_test (t) values ('"'"'b'"'"');
 insert into copydml_test (t) values ('"'"'c'"'"');
`},
	// TODO(#117598): Remove this patch once row IDs are omitted from star expansions.
	{"insert_conflict.sql", `diff --git a/src/test/regress/sql/insert_conflict.sql b/src/test/regress/sql/insert_conflict.sql
index 23d5778b82..9c0ce27440 100644
--- a/src/test/regress/sql/insert_conflict.sql
+++ b/src/test/regress/sql/insert_conflict.sql
@@ -238,8 +238,8 @@ insert into insertconflicttest as i values (23, '"'"'Jackfruit'"'"') on conflict (key) d
 insert into insertconflicttest as i values (23, '"'"'Jackfruit'"'"') on conflict (key) do update set fruit = excluded.fruit
   where i.* = excluded.* returning *;
 -- Assign:
-insert into insertconflicttest as i values (23, '"'"'Avocado'"'"') on conflict (key) do update set fruit = excluded.*::text
-  returning *;
+-- insert into insertconflicttest as i values (23, '"'"'Avocado'"'"') on conflict (key) do update set fruit = excluded.*::text
+--  returning *;
 -- deparse whole row var in WHERE and SET clauses:
 explain (costs off) insert into insertconflicttest as i values (23, '"'"'Avocado'"'"') on conflict (key) do update set fruit = excluded.fruit where excluded.* is null;
 explain (costs off) insert into insertconflicttest as i values (23, '"'"'Avocado'"'"') on conflict (key) do update set fruit = excluded.*::text;
`},
	// CRDB does not need to reindex.
	{"create_index.sql", `diff --git a/src/test/regress/sql/create_index.sql b/src/test/regress/sql/create_index.sql
index d49ce9f300..4f4bac2425 100644
--- a/src/test/regress/sql/create_index.sql
+++ b/src/test/regress/sql/create_index.sql
@@ -1191,7 +1191,7 @@ SELECT oid, relname, relfilenode, relkind, reltoastrelid
   FROM pg_class
   WHERE relname IN ('"'"'concur_temp_ind_1'"'"', '"'"'concur_temp_ind_2'"'"');
 SELECT pg_my_temp_schema()::regnamespace as temp_schema_name \gset
-REINDEX SCHEMA CONCURRENTLY :temp_schema_name;
+-- REINDEX SCHEMA CONCURRENTLY :temp_schema_name;
 SELECT  b.relname,
         b.relkind,
         CASE WHEN a.relfilenode = b.relfilenode THEN '"'"'relfilenode is unchanged'"'"'
`},
	// Serial is not deterministic.
	{"updatable_views.sql", `diff --git a/src/test/regress/sql/updatable_views.sql b/src/test/regress/sql/updatable_views.sql
index eaee0b7e1d..8f7150b657 100644
--- a/src/test/regress/sql/updatable_views.sql
+++ b/src/test/regress/sql/updatable_views.sql
@@ -754,7 +754,8 @@ DROP USER regress_view_user3;
 
 -- column defaults
 
-CREATE TABLE base_tbl (a int PRIMARY KEY, b text DEFAULT '"'"'Unspecified'"'"', c serial);
+CREATE SEQUENCE c_seq;
+CREATE TABLE base_tbl (a int PRIMARY KEY, b text DEFAULT '"'"'Unspecified'"'"', c int default nextval('"'"'c_seq'"'"'));
 INSERT INTO base_tbl VALUES (1, '"'"'Row 1'"'"');
 INSERT INTO base_tbl VALUES (2, '"'"'Row 2'"'"');
 INSERT INTO base_tbl VALUES (3);
 `},
	// Serial is not deterministic.
	{"triggers.sql", `diff --git a/src/test/regress/sql/triggers.sql b/src/test/regress/sql/triggers.sql
index d29e98d2ac..b3184dfb63 100644
--- a/src/test/regress/sql/triggers.sql
+++ b/src/test/regress/sql/triggers.sql
@@ -480,7 +480,8 @@ rollback;
 
 -- Test enable/disable triggers
 
-create table trigtest (i serial primary key);
+create sequence trigtestseq;
+create table trigtest (i int primary key default nextval('"'"'trigtestseq'"'"'));
 -- test that disabling RI triggers works
 create table trigtest2 (i int references trigtest(i) on delete cascade);
 
@@ -865,8 +866,9 @@ DROP VIEW main_view;
 --
 -- Test triggers on a join view
 --
+CREATE SEQUENCE country_id_seq;
 CREATE TABLE country_table (
-    country_id        serial primary key,
+    country_id        int primary key default nextval('"'"'country_id_seq'"'"'),
     country_name    text unique not null,
     continent        text not null
 );
 `},
	// Add order to some statements so that CRDB output is deterministic.
	{"aggregates.sql", `diff --git a/src/test/regress/sql/aggregates.sql b/src/test/regress/sql/aggregates.sql
index 75c78be640..00b543bf45 100644
--- a/src/test/regress/sql/aggregates.sql
+++ b/src/test/regress/sql/aggregates.sql
@@ -386,7 +386,7 @@ explain (costs off)
   select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
     from int4_tbl;
 select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
-  from int4_tbl;
+  from int4_tbl order by f1;
 
 -- check some cases that were handled incorrectly in 8.3.0
 explain (costs off)
@@ -809,10 +809,10 @@ select min(unique1) filter (where unique1 > 100) from tenk1;
 select sum(1/ten) filter (where ten > 0) from tenk1;
 
 select ten, sum(distinct four) filter (where four::text ~ '"'"'123'"'"') from onek a
-group by ten;
+group by ten order by ten;
 
 select ten, sum(distinct four) filter (where four > 10) from onek a
-group by ten
+group by ten order by ten
 having exists (select 1 from onek b where sum(distinct a.four) = b.four);
 
 select max(foo COLLATE "C") filter (where (bar collate "POSIX") > '"'"'0'"'"')
`},
	// The foreign key violation error message includes the row id, which is non-deterministic in CRDB.
	{"foreign_key.sql", `diff --git a/src/test/regress/sql/foreign_key.sql b/src/test/regress/sql/foreign_key.sql
index 22e177f89b..33a9afbc61 100644
--- a/src/test/regress/sql/foreign_key.sql
+++ b/src/test/regress/sql/foreign_key.sql
@@ -228,7 +228,7 @@ CREATE TABLE FKTABLE ( ftest1 int, ftest2 int );
 INSERT INTO PKTABLE VALUES (1, 2);
 INSERT INTO FKTABLE VALUES (1, NULL);
 
-ALTER TABLE FKTABLE ADD FOREIGN KEY(ftest1, ftest2) REFERENCES PKTABLE MATCH FULL;
+-- ALTER TABLE FKTABLE ADD FOREIGN KEY(ftest1, ftest2) REFERENCES PKTABLE MATCH FULL;
 
 DROP TABLE FKTABLE;
 DROP TABLE PKTABLE;
 `},
	// Serial is non-deterministic.
	{"returning.sql", `diff --git a/src/test/regress/sql/returning.sql b/src/test/regress/sql/returning.sql
index a460f82fb7..a9f7b99b84 100644
--- a/src/test/regress/sql/returning.sql
+++ b/src/test/regress/sql/returning.sql
@@ -4,7 +4,8 @@
 
 -- Simple cases
 
-CREATE TEMP TABLE foo (f1 serial, f2 text, f3 int default 42);
+CREATE SEQUENCE f1_seq;
+CREATE TEMP TABLE foo (f1 int default nextval('"'"'f1_seq'"'"'), f2 text, f3 int default 42);
 
 INSERT INTO foo (f2,f3)
   VALUES ('"'"'test'"'"', DEFAULT), ('"'"'More'"'"', 11), (upper('"'"'more'"'"'), 7+9)
`},
	// Serial is non-deterministic.
	// TODO(#114676): Remove the patch around getrngfunc9 when
	// the internal error is fixed.
	{"rangefuncs.sql", `diff --git a/src/test/regress/sql/rangefuncs.sql b/src/test/regress/sql/rangefuncs.sql
index 63351e1412..07d3216a9d 100644
--- a/src/test/regress/sql/rangefuncs.sql
+++ b/src/test/regress/sql/rangefuncs.sql
@@ -182,7 +182,7 @@ SELECT * FROM vw_getrngfunc;
 DROP VIEW vw_getrngfunc;

 -- plpgsql, proretset = f, prorettype = c
-CREATE FUNCTION getrngfunc9(int) RETURNS rngfunc AS '"'"'DECLARE rngfunctup rngfunc%ROWTYPE; BEGIN SELECT * into rngfunctup FROM rngfunc WHERE rngfuncid = $1; RETURN rngfunctup; END;'"'"' LANGUAGE plpgsql;
+-- CREATE FUNCTION getrngfunc9(int) RETURNS rngfunc AS '"'"'DECLARE rngfunctup rngfunc%ROWTYPE; BEGIN SELECT * into rngfunctup FROM rngfunc WHERE rngfuncid = $1; RETURN rngfunctup; END;'"'"' LANGUAGE plpgsql;
 SELECT * FROM getrngfunc9(1) AS t1;
 SELECT * FROM getrngfunc9(1) WITH ORDINALITY AS t1(a,b,c,o);
 CREATE VIEW vw_getrngfunc AS SELECT * FROM getrngfunc9(1);
@@ -457,7 +457,8 @@ DROP FUNCTION rngfunc();
 -- some tests on SQL functions with RETURNING
 --
 
-create temp table tt(f1 serial, data text);
+create sequence f1_seq;
+create temp table tt(f1 int default nextval('"'"'f1_seq'"'"'), data text);
 
 create function insert_tt(text) returns int as
 $$ insert into tt(data) values($1) returning f1 $$
`},
	// Serial is non-deterministic and non-monotonic in CRDB, so we modify the tests for serial slightly
	// to only print non-serial columns.
	// TODO(#114848): Allow SELECT * FROM pg_sequence_parameters('sequence_test4'::regclass);
	// when the internal error is fixed.
	{"sequence.sql", `diff --git a/src/test/regress/sql/sequence.sql b/src/test/regress/sql/sequence.sql
index 674f5f1f66..54baf75a7b 100644
--- a/src/test/regress/sql/sequence.sql
+++ b/src/test/regress/sql/sequence.sql
@@ -58,7 +58,7 @@ INSERT INTO serialTest1 VALUES ('"'"'bar'"'"');
 INSERT INTO serialTest1 VALUES ('"'"'force'"'"', 100);
 INSERT INTO serialTest1 VALUES ('"'"'wrong'"'"', NULL);

-SELECT * FROM serialTest1;
+SELECT f1 FROM serialTest1 ORDER BY f2;

 SELECT pg_get_serial_sequence('"'"'serialTest1'"'"', '"'"'f2'"'"');

@@ -100,7 +100,7 @@ INSERT INTO serialTest2 (f1, f5)
 INSERT INTO serialTest2 (f1, f6)
   VALUES ('"'"'bogus'"'"', 9223372036854775808);

-SELECT * FROM serialTest2 ORDER BY f2 ASC;
+SELECT f1 FROM serialTest2 ORDER BY f2 ASC;

 SELECT nextval('"'"'serialTest2_f2_seq'"'"');
 SELECT nextval('"'"'serialTest2_f3_seq'"'"');
@@ -143,7 +143,7 @@ DROP SEQUENCE foo_seq_new;
 -- renaming serial sequences
 ALTER TABLE serialtest1_f2_seq RENAME TO serialtest1_f2_foo;
 INSERT INTO serialTest1 VALUES ('"'"'more'"'"');
-SELECT * FROM serialTest1;
+SELECT f1 FROM serialTest1 ORDER BY f2;

 --
 -- Check dependencies of serial and ordinary sequences
@@ -242,7 +242,7 @@ WHERE sequencename ~ ANY(ARRAY['"'"'sequence_test'"'"', '"'"'serialtest'"'"'])
   ORDER BY sequencename ASC;


-SELECT * FROM pg_sequence_parameters('"'"'sequence_test4'"'"'::regclass);
+-- SELECT * FROM pg_sequence_parameters('"'"'sequence_test4'"'"'::regclass);


 \d sequence_test4
`},
	// Serial is non-deterministic.
	{"truncate.sql", `diff --git a/src/test/regress/sql/truncate.sql b/src/test/regress/sql/truncate.sql
index 54f26e3077..cf0195bfa6 100644
--- a/src/test/regress/sql/truncate.sql
+++ b/src/test/regress/sql/truncate.sql
@@ -181,8 +181,9 @@ DROP TABLE trunc_trigger_log;
 DROP FUNCTION trunctrigger();
 
 -- test TRUNCATE ... RESTART IDENTITY
+CREATE SEQUENCE truncate_a_id;
 CREATE SEQUENCE truncate_a_id1 START WITH 33;
-CREATE TABLE truncate_a (id serial,
+CREATE TABLE truncate_a (id int default nextval('"'"'truncate_a_id'"'"'),
                          id1 integer default nextval('"'"'truncate_a_id1'"'"'));
 ALTER SEQUENCE truncate_a_id1 OWNED BY truncate_a.id1;
`},
	// Some foreign key check error messages contain non-deterministic row ids. We comment out those
	// tests.
	{"alter_table.sql", `diff --git a/src/test/regress/sql/alter_table.sql b/src/test/regress/sql/alter_table.sql
index 58ea20ac3d..def1421e5f 100644
--- a/src/test/regress/sql/alter_table.sql
+++ b/src/test/regress/sql/alter_table.sql
@@ -362,7 +362,7 @@ ALTER TABLE attmp3 add constraint attmpconstr foreign key(c) references attmp2 m
 ALTER TABLE attmp3 add constraint attmpconstr foreign key(a) references attmp2(b) match full;
 
 -- Try (and fail) to add constraint due to invalid data
-ALTER TABLE attmp3 add constraint attmpconstr foreign key (a) references attmp2 match full;
+-- ALTER TABLE attmp3 add constraint attmpconstr foreign key (a) references attmp2 match full;
 
 -- Delete failing row
 DELETE FROM attmp3 where a=5;
@@ -375,7 +375,7 @@ INSERT INTO attmp3 values (5,50);
 
 -- Try NOT VALID and then VALIDATE CONSTRAINT, but fails. Delete failure then re-validate
 ALTER TABLE attmp3 add constraint attmpconstr foreign key (a) references attmp2 match full NOT VALID;
-ALTER TABLE attmp3 validate constraint attmpconstr;
+-- ALTER TABLE attmp3 validate constraint attmpconstr;
 
 -- Delete failing row
 DELETE FROM attmp3 where a=5;
@@ -385,9 +385,9 @@ ALTER TABLE attmp3 validate constraint attmpconstr;
 ALTER TABLE attmp3 validate constraint attmpconstr;
 
 -- Try a non-verified CHECK constraint
-ALTER TABLE attmp3 ADD CONSTRAINT b_greater_than_ten CHECK (b > 10); -- fail
+-- ALTER TABLE attmp3 ADD CONSTRAINT b_greater_than_ten CHECK (b > 10); -- fail
 ALTER TABLE attmp3 ADD CONSTRAINT b_greater_than_ten CHECK (b > 10) NOT VALID; -- succeeds
-ALTER TABLE attmp3 VALIDATE CONSTRAINT b_greater_than_ten; -- fails
+-- ALTER TABLE attmp3 VALIDATE CONSTRAINT b_greater_than_ten; -- fails
 DELETE FROM attmp3 WHERE NOT b > 10;
 ALTER TABLE attmp3 VALIDATE CONSTRAINT b_greater_than_ten; -- succeeds
 ALTER TABLE attmp3 VALIDATE CONSTRAINT b_greater_than_ten; -- succeeds
@@ -577,7 +577,7 @@ create table atacc1 ( test int );
 -- insert a soon to be failing row
 insert into atacc1 (test) values (2);
 -- add a check constraint (fails)
-alter table atacc1 add constraint atacc_test1 check (test>3);
+-- alter table atacc1 add constraint atacc_test1 check (test>3);
 insert into atacc1 (test) values (4);
 drop table atacc1;
 
@@ -807,7 +807,7 @@ drop table atacc1;
 create table atacc1 (a bigint, b int);
 insert into atacc1 values(1,2);
 alter table atacc1 add constraint atacc1_chk check(b = 1) not valid;
-alter table atacc1 validate constraint atacc1_chk, alter a type int;
+-- alter table atacc1 validate constraint atacc1_chk, alter a type int;
 drop table atacc1;
 
 -- something a little more complicated
@@ -875,11 +875,11 @@ create table atacc1 (test_a int, test_b int);
 insert into atacc1 values (null, 1);
 -- constraint not cover all values, should fail
 alter table atacc1 add constraint atacc1_constr_or check(test_a is not null or test_b < 10);
-alter table atacc1 alter test_a set not null;
+-- alter table atacc1 alter test_a set not null;
 alter table atacc1 drop constraint atacc1_constr_or;
 -- not valid constraint, should fail
 alter table atacc1 add constraint atacc1_constr_invalid check(test_a is not null) not valid;
-alter table atacc1 alter test_a set not null;
+-- alter table atacc1 alter test_a set not null;
 alter table atacc1 drop constraint atacc1_constr_invalid;
 -- with valid constraint
 update atacc1 set test_a = 1;
@@ -891,9 +891,9 @@ insert into atacc1 values (2, null);
 alter table atacc1 alter test_a drop not null;
 -- test multiple set not null at same time
 -- test_a checked by atacc1_constr_a_valid, test_b should fail by table scan
-alter table atacc1 alter test_a set not null, alter test_b set not null;
+-- alter table atacc1 alter test_a set not null, alter test_b set not null;
 -- commands order has no importance
-alter table atacc1 alter test_b set not null, alter test_a set not null;
+-- alter table atacc1 alter test_b set not null, alter test_a set not null;
 
 -- valid one by table scan, one by check constraints
 update atacc1 set test_b = 1;
@@ -915,10 +915,10 @@ insert into child (a, b) values (NULL, '"'"'foo'"'"');
 alter table parent alter a drop not null;
 insert into parent values (NULL);
 insert into child (a, b) values (NULL, '"'"'foo'"'"');
-alter table only parent alter a set not null;
+-- alter table only parent alter a set not null;
 alter table child alter a set not null;
 delete from parent;
-alter table only parent alter a set not null;
+-- alter table only parent alter a set not null;
 insert into parent values (NULL);
 alter table child alter a set not null;
 insert into child (a, b) values (NULL, '"'"'foo'"'"');
@@ -1883,7 +1883,8 @@ select non_strict(NULL);
 create schema alter1;
 create schema alter2;
 
-create table alter1.t1(f1 serial primary key, f2 int check (f2 > 0));
+create sequence f1_seq;
+create table alter1.t1(f1 int primary key default nextval('"'"'f1_seq'"'"'), f2 int check (f2 > 0));
 
 create view alter1.v1 as select * from alter1.t1;
`},
	// Serial is non-deterministic.
	{"tuplesort.sql", `diff --git a/src/test/regress/sql/tuplesort.sql b/src/test/regress/sql/tuplesort.sql
index 846484d561..a5c22929bf 100644
--- a/src/test/regress/sql/tuplesort.sql
+++ b/src/test/regress/sql/tuplesort.sql
@@ -6,8 +6,9 @@ SET max_parallel_workers = 0;
 -- key aborts. One easy way to achieve that is to use uuids that all
 -- have the same prefix, as abbreviated keys for uuids just use the
 -- first sizeof(Datum) bytes.
+CREATE TEMP SEQUENCE id_seq;
 CREATE TEMP TABLE abbrev_abort_uuids (
-    id serial not null,
+    id int not null default nextval('"'"'id_seq'"'"'),
     abort_increasing uuid,
     abort_decreasing uuid,
     noabort_increasing uuid,
`},
	{"delete.sql", `diff --git a/src/test/regress/sql/delete.sql b/src/test/regress/sql/delete.sql
index d8cb99e93c..48cff6ddff 100644
--- a/src/test/regress/sql/delete.sql
+++ b/src/test/regress/sql/delete.sql
@@ -1,5 +1,6 @@
+CREATE SEQUENCE id_seq;
 CREATE TABLE delete_test (
-    id SERIAL PRIMARY KEY,
+    id INT PRIMARY KEY DEFAULT nextval('"'"'id_seq'"'"'),
     a INT,
     b text
 );
`},
	// Avoid ordering by oids. Error message includes an oid.
	{"indexing.sql", `diff --git a/src/test/regress/sql/indexing.sql b/src/test/regress/sql/indexing.sql
index 6f60d1dc0f..161b7bded2 100644
--- a/src/test/regress/sql/indexing.sql
+++ b/src/test/regress/sql/indexing.sql
@@ -438,7 +438,7 @@ alter table idxpart attach partition idxpart1 for values from (0) to (1000);
 \d idxpart1
 select attrelid::regclass, attname, attnum from pg_attribute
   where attrelid::regclass::text like '"'"'idxpart%'"'"' and attnum > 0
-  order by attrelid::regclass, attnum;
+  order by attrelid::regclass::text, attnum;
 drop table idxpart;
 
 -- Column number mapping: dropped columns in the parent table
@@ -454,7 +454,7 @@ alter table idxpart attach partition idxpart1 for values from (0) to (1000);
 \d idxpart1
 select attrelid::regclass, attname, attnum from pg_attribute
   where attrelid::regclass::text like '"'"'idxpart%'"'"' and attnum > 0
-  order by attrelid::regclass, attnum;
+  order by attrelid::regclass::text, attnum;
 drop table idxpart;
 
 --
@@ -566,7 +566,7 @@ select indrelid::regclass, indexrelid::regclass, inhparent::regclass, indisvalid
 drop index idxpart0_pkey;                                                              -- fail
 drop index idxpart1_pkey;                                                              -- fail
 alter table idxpart0 drop constraint idxpart0_pkey;            -- fail
-alter table idxpart1 drop constraint idxpart1_pkey;            -- fail
+-- alter table idxpart1 drop constraint idxpart1_pkey;         -- fail
 alter table idxpart drop constraint idxpart_pkey;              -- ok
 select indrelid::regclass, indexrelid::regclass, inhparent::regclass, indisvalid,
   conname, conislocal, coninhcount, connoinherit, convalidated

`},
	{"misc_sanity.sql", `diff --git a/src/test/regress/sql/misc_sanity.sql b/src/test/regress/sql/misc_sanity.sql
index 2c0f87a651..26bd75bbf0 100644
--- a/src/test/regress/sql/misc_sanity.sql
+++ b/src/test/regress/sql/misc_sanity.sql
@@ -26,11 +26,12 @@ WHERE refclassid = 0 OR refobjid = 0 OR
 
 -- Look for illegal values in pg_shdepend fields.
 
-SELECT *
-FROM pg_shdepend as d1
-WHERE refclassid = 0 OR refobjid = 0 OR
-      classid = 0 OR objid = 0 OR
-      deptype NOT IN ('"'"'a'"'"', '"'"'o'"'"', '"'"'r'"'"', '"'"'t'"'"');
+-- CRDB still supports deptype '"'"'p'"'"'.
+-- SELECT *
+-- FROM pg_shdepend as d1
+-- WHERE refclassid = 0 OR refobjid = 0 OR
+--       classid = 0 OR objid = 0 OR
+--       deptype NOT IN ('"'"'a'"'"', '"'"'o'"'"', '"'"'r'"'"', '"'"'t'"'"');
 
 
 -- **************** pg_class ****************
`},
	// TODO(#114676): Remove the patch around wslot_slotlink_view when
	// the internal error is fixed.
	{"plpgsql.sql", `diff --git a/src/test/regress/sql/plpgsql.sql b/src/test/regress/sql/plpgsql.sql
index 924d524094..eb7bc0cf87 100644
--- a/src/test/regress/sql/plpgsql.sql
+++ b/src/test/regress/sql/plpgsql.sql
@@ -1087,51 +1087,51 @@ end;
 -- ************************************************************
 -- * Describe the front of a wall connector slot
 -- ************************************************************
-create function wslot_slotlink_view(bpchar)
-returns text as '"'"'
-declare
-    rec		record;
-    sltype	char(2);
-    retval	text;
-begin
-    select into rec * from WSlot where slotname = $1;
-    if not found then
-        return '"'"''"'"''"'"''"'"';
-    end if;
-    if rec.slotlink = '"'"''"'"''"'"''"'"' then
-        return '"'"''"'"'-'"'"''"'"';
-    end if;
-    sltype := substr(rec.slotlink, 1, 2);
-    if sltype = '"'"''"'"'PH'"'"''"'"' then
-        select into rec * from PHone where slotname = rec.slotlink;
-	retval := '"'"''"'"'Phone '"'"''"'"' || trim(rec.slotname);
-	if rec.comment != '"'"''"'"''"'"''"'"' then
-	    retval := retval || '"'"''"'"' ('"'"''"'"';
-	    retval := retval || rec.comment;
-	    retval := retval || '"'"''"'"')'"'"''"'"';
-	end if;
-	return retval;
-    end if;
-    if sltype = '"'"''"'"'IF'"'"''"'"' then
-	declare
-	    syrow	System%RowType;
-	    ifrow	IFace%ROWTYPE;
-        begin
-	    select into ifrow * from IFace where slotname = rec.slotlink;
-	    select into syrow * from System where name = ifrow.sysname;
-	    retval := syrow.name || '"'"''"'"' IF '"'"''"'"';
-	    retval := retval || ifrow.ifname;
-	    if syrow.comment != '"'"''"'"''"'"''"'"' then
-	        retval := retval || '"'"''"'"' ('"'"''"'"';
-		retval := retval || syrow.comment;
-		retval := retval || '"'"''"'"')'"'"''"'"';
-	    end if;
-	    return retval;
-	end;
-    end if;
-    return rec.slotlink;
-end;
-'"'"' language plpgsql;
+-- create function wslot_slotlink_view(bpchar)
+-- returns text as '"'"'
+-- declare
+--     rec		record;
+--     sltype	char(2);
+--     retval	text;
+-- begin
+--     select into rec * from WSlot where slotname = $1;
+--     if not found then
+--         return '"'"''"'"''"'"''"'"';
+--     end if;
+--     if rec.slotlink = '"'"''"'"''"'"''"'"' then
+--         return '"'"''"'"'-'"'"''"'"';
+--     end if;
+--     sltype := substr(rec.slotlink, 1, 2);
+--     if sltype = '"'"''"'"'PH'"'"''"'"' then
+--         select into rec * from PHone where slotname = rec.slotlink;
+-- 	retval := '"'"''"'"'Phone '"'"''"'"' || trim(rec.slotname);
+-- 	if rec.comment != '"'"''"'"''"'"''"'"' then
+-- 	    retval := retval || '"'"''"'"' ('"'"''"'"';
+-- 	    retval := retval || rec.comment;
+-- 	    retval := retval || '"'"''"'"')'"'"''"'"';
+-- 	end if;
+-- 	return retval;
+--     end if;
+--     if sltype = '"'"''"'"'IF'"'"''"'"' then
+-- 	declare
+-- 	    syrow	System%RowType;
+-- 	    ifrow	IFace%ROWTYPE;
+--         begin
+-- 	    select into ifrow * from IFace where slotname = rec.slotlink;
+-- 	    select into syrow * from System where name = ifrow.sysname;
+-- 	    retval := syrow.name || '"'"''"'"' IF '"'"''"'"';
+-- 	    retval := retval || ifrow.ifname;
+-- 	    if syrow.comment != '"'"''"'"''"'"''"'"' then
+-- 	        retval := retval || '"'"''"'"' ('"'"''"'"';
+-- 		retval := retval || syrow.comment;
+-- 		retval := retval || '"'"''"'"')'"'"''"'"';
+-- 	    end if;
+-- 	    return retval;
+-- 	end;
+--     end if;
+--     return rec.slotlink;
+-- end;
+-- '"'"' language plpgsql;
 
 
 
@@ -2191,25 +2191,25 @@ drop function missing_return_expr();
 create table eifoo (i integer, y integer);
 create type eitype as (i integer, y integer);
 
-create or replace function execute_into_test(varchar) returns record as $$
-declare
-    _r record;
-    _rt eifoo%rowtype;
-    _v eitype;
-    i int;
-    j int;
-    k int;
-begin
-    execute '"'"'insert into '"'"'||$1||'"'"' values(10,15)'"'"';
-    execute '"'"'select (row).* from (select row(10,1)::eifoo) s'"'"' into _r;
-    raise notice '"'"'% %'"'"', _r.i, _r.y;
-    execute '"'"'select * from '"'"'||$1||'"'"' limit 1'"'"' into _rt;
-    raise notice '"'"'% %'"'"', _rt.i, _rt.y;
-    execute '"'"'select *, 20 from '"'"'||$1||'"'"' limit 1'"'"' into i, j, k;
-    raise notice '"'"'% % %'"'"', i, j, k;
-    execute '"'"'select 1,2'"'"' into _v;
-    return _v;
-end; $$ language plpgsql;
+-- create or replace function execute_into_test(varchar) returns record as $$
+-- declare
+--     _r record;
+--     _rt eifoo%rowtype;
+--     _v eitype;
+--     i int;
+--     j int;
+--     k int;
+-- begin
+--     execute '"'"'insert into '"'"'||$1||'"'"' values(10,15)'"'"';
+--     execute '"'"'select (row).* from (select row(10,1)::eifoo) s'"'"' into _r;
+--     raise notice '"'"'% %'"'"', _r.i, _r.y;
+--     execute '"'"'select * from '"'"'||$1||'"'"' limit 1'"'"' into _rt;
+--     raise notice '"'"'% %'"'"', _rt.i, _rt.y;
+--     execute '"'"'select *, 20 from '"'"'||$1||'"'"' limit 1'"'"' into i, j, k;
+--     raise notice '"'"'% % %'"'"', i, j, k;
+--     execute '"'"'select 1,2'"'"' into _v;
+--     return _v;
+-- end; $$ language plpgsql;
 
 select execute_into_test('"'"'eifoo'"'"');

`},
	// TODO(#114847): Remove this patch when the internal error is fixed.
	{"rowtypes.sql", `diff --git a/src/test/regress/sql/rowtypes.sql b/src/test/regress/sql/rowtypes.sql
index 565e6249d5..f36ce2844b 100644
--- a/src/test/regress/sql/rowtypes.sql
+++ b/src/test/regress/sql/rowtypes.sql
@@ -245,8 +245,8 @@ create type testtype3 as (a int, b text);
 select row(1, 2)::testtype1 < row(1, '"'"'abc'"'"')::testtype3;
 select row(1, 2)::testtype1 <> row(1, '"'"'abc'"'"')::testtype3;
 create type testtype5 as (a int);
-select row(1, 2)::testtype1 < row(1)::testtype5;
-select row(1, 2)::testtype1 <> row(1)::testtype5;
+-- select row(1, 2)::testtype1 < row(1)::testtype5;
+-- select row(1, 2)::testtype1 <> row(1)::testtype5;
 
 -- non-comparable types
 create type testtype6 as (a int, b point);

`},
	// TODO(#118698): Remove patch when internal error is fixed.
	{"union.sql", `diff --git a/src/test/regress/sql/union.sql b/src/test/regress/sql/union.sql
index ca8c9b4d12..7b8aaf2df2 100644
--- a/src/test/regress/sql/union.sql
+++ b/src/test/regress/sql/union.sql
@@ -294,7 +294,7 @@ SELECT q1 FROM int8_tbl EXCEPT (((SELECT q2 FROM int8_tbl ORDER BY q2 LIMIT 1)))
 -- Check behavior with empty select list (allowed since 9.4)
 --
 
-select union select;
+-- select union select;
 select intersect select;
 select except select;
 
@@ -307,11 +307,11 @@ select from generate_series(1,5) union select from generate_series(1,3);
 explain (costs off)
 select from generate_series(1,5) intersect select from generate_series(1,3);
 
-select from generate_series(1,5) union select from generate_series(1,3);
+-- select from generate_series(1,5) union select from generate_series(1,3);
 select from generate_series(1,5) union all select from generate_series(1,3);
-select from generate_series(1,5) intersect select from generate_series(1,3);
+-- select from generate_series(1,5) intersect select from generate_series(1,3);
 select from generate_series(1,5) intersect all select from generate_series(1,3);
-select from generate_series(1,5) except select from generate_series(1,3);
+-- select from generate_series(1,5) except select from generate_series(1,3);
 select from generate_series(1,5) except all select from generate_series(1,3);
 
 -- check sorted implementation
@@ -323,11 +323,11 @@ select from generate_series(1,5) union select from generate_series(1,3);
 explain (costs off)
 select from generate_series(1,5) intersect select from generate_series(1,3);
 
-select from generate_series(1,5) union select from generate_series(1,3);
+-- select from generate_series(1,5) union select from generate_series(1,3);
 select from generate_series(1,5) union all select from generate_series(1,3);
-select from generate_series(1,5) intersect select from generate_series(1,3);
+-- select from generate_series(1,5) intersect select from generate_series(1,3);
 select from generate_series(1,5) intersect all select from generate_series(1,3);
-select from generate_series(1,5) except select from generate_series(1,3);
+-- select from generate_series(1,5) except select from generate_series(1,3);
 select from generate_series(1,5) except all select from generate_series(1,3);
 
 reset enable_hashagg;
`},
	// Use SET ROLE instead of SET SESSION AUTHORIZATION. Adjust SQL to allow
	// creation of a function (f_leak), which is used throughout the test.
	{"rowsecurity.sql", `diff --git a/src/test/regress/sql/rowsecurity.sql b/src/test/regress/sql/rowsecurity.sql
index dec7340538c..6e458908369 100644
--- a/src/test/regress/sql/rowsecurity.sql
+++ b/src/test/regress/sql/rowsecurity.sql
@@ -36,14 +36,14 @@ GRANT ALL ON SCHEMA regress_rls_schema to public;
 SET search_path = regress_rls_schema;

 -- setup of malicious function
-CREATE OR REPLACE FUNCTION f_leak(text) RETURNS bool
-    COST 0.0000001 LANGUAGE plpgsql
+CREATE OR REPLACE FUNCTION f_leak(col text) RETURNS bool
+    LANGUAGE plpgsql
     AS '"'"'BEGIN RAISE NOTICE '"'"''"'"'f_leak => %'"'"''"'"', $1; RETURN true; END'"'"';
 GRANT EXECUTE ON FUNCTION f_leak(text) TO public;

 -- BASIC Row-Level Security Scenario

-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE TABLE uaccount (
     pguser      name primary key,
     seclv       int
@@ -112,7 +112,7 @@ CREATE POLICY p1r ON document AS RESTRICTIVE TO regress_rls_dave
 SELECT * FROM pg_policies WHERE schemaname = '"'"'regress_rls_schema'"'"' AND tablename = '"'"'document'"'"' ORDER BY policyname;
 
 -- viewpoint from regress_rls_bob
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SET row_security TO ON;
 SELECT * FROM document WHERE f_leak(dtitle) ORDER BY did;
 SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle) ORDER BY did;
@@ -122,7 +122,7 @@ SELECT * FROM document TABLESAMPLE BERNOULLI(50) REPEATABLE(0)
   WHERE f_leak(dtitle) ORDER BY did;
 
 -- viewpoint from regress_rls_carol
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SELECT * FROM document WHERE f_leak(dtitle) ORDER BY did;
 SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle) ORDER BY did;
 
@@ -134,7 +134,7 @@ EXPLAIN (COSTS OFF) SELECT * FROM document WHERE f_leak(dtitle);
 EXPLAIN (COSTS OFF) SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle);
 
 -- viewpoint from regress_rls_dave
-SET SESSION AUTHORIZATION regress_rls_dave;
+SET ROLE regress_rls_dave;
 SELECT * FROM document WHERE f_leak(dtitle) ORDER BY did;
 SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle) ORDER BY did;
 
@@ -151,16 +151,16 @@ INSERT INTO document VALUES (100, 55, 1, '"'"'regress_rls_dave'"'"', '"'"'testing sorting of
 ALTER POLICY p1 ON document USING (true);    --fail
 DROP POLICY p1 ON document;                  --fail
 
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 ALTER POLICY p1 ON document USING (dauthor = current_user);
 
 -- viewpoint from regress_rls_bob again
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM document WHERE f_leak(dtitle) ORDER BY did;
 SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle) ORDER by did;
 
 -- viewpoint from rls_regres_carol again
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SELECT * FROM document WHERE f_leak(dtitle) ORDER BY did;
 SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle) ORDER by did;
 
@@ -168,7 +168,7 @@ EXPLAIN (COSTS OFF) SELECT * FROM document WHERE f_leak(dtitle);
 EXPLAIN (COSTS OFF) SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle);
 
 -- interaction of FK/PK constraints
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE POLICY p2 ON category
     USING (CASE WHEN current_user = '"'"'regress_rls_bob'"'"' THEN cid IN (11, 33)
            WHEN current_user = '"'"'regress_rls_carol'"'"' THEN cid IN (22, 44)
@@ -177,17 +177,17 @@ CREATE POLICY p2 ON category
 ALTER TABLE category ENABLE ROW LEVEL SECURITY;
 
 -- cannot delete PK referenced by invisible FK
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM document d FULL OUTER JOIN category c on d.cid = c.cid ORDER BY d.did, c.cid;
 DELETE FROM category WHERE cid = 33;    -- fails with FK violation
 
 -- can insert FK referencing invisible PK
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SELECT * FROM document d FULL OUTER JOIN category c on d.cid = c.cid ORDER BY d.did, c.cid;
 INSERT INTO document VALUES (11, 33, 1, current_user, '"'"'hoge'"'"');
 
 -- UNIQUE or PRIMARY KEY constraint violation DOES reveal presence of row
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 INSERT INTO document VALUES (8, 44, 1, '"'"'regress_rls_bob'"'"', '"'"'my third manga'"'"'); -- Must fail with unique violation, revealing presence of did we can'"'"'t see
 SELECT * FROM document WHERE did = 8; -- and confirm we can'"'"'t see it
 
@@ -196,31 +196,31 @@ INSERT INTO document VALUES (8, 44, 1, '"'"'regress_rls_carol'"'"', '"'"'my third manga'"'"'); -
 UPDATE document SET did = 8, dauthor = '"'"'regress_rls_carol'"'"' WHERE did = 5; -- Should fail with RLS check violation, not duplicate key violation
 
 -- database superuser does bypass RLS policy when enabled
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 SET row_security TO ON;
 SELECT * FROM document;
 SELECT * FROM category;
 
 -- database superuser does bypass RLS policy when disabled
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 SET row_security TO OFF;
 SELECT * FROM document;
 SELECT * FROM category;
 
 -- database non-superuser with bypass privilege can bypass RLS policy when disabled
-SET SESSION AUTHORIZATION regress_rls_exempt_user;
+SET ROLE regress_rls_exempt_user;
 SET row_security TO OFF;
 SELECT * FROM document;
 SELECT * FROM category;
 
 -- RLS policy does not apply to table owner when RLS enabled.
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 SET row_security TO ON;
 SELECT * FROM document;
 SELECT * FROM category;
 
 -- RLS policy does not apply to table owner when RLS disabled.
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 SET row_security TO OFF;
 SELECT * FROM document;
 SELECT * FROM category;
@@ -228,7 +228,7 @@ SELECT * FROM category;
 --
 -- Table inheritance and RLS policy
 --
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 
 SET row_security TO ON;
 
@@ -269,7 +269,7 @@ CREATE POLICY p2 ON t2 FOR ALL TO PUBLIC USING (a % 2 = 1); -- be odd number
 ALTER TABLE t1 ENABLE ROW LEVEL SECURITY;
 ALTER TABLE t2 ENABLE ROW LEVEL SECURITY;
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 
 SELECT * FROM t1;
 EXPLAIN (COSTS OFF) SELECT * FROM t1;
@@ -297,13 +297,13 @@ SELECT a, b, tableoid::regclass FROM t2 UNION ALL SELECT a, b, tableoid::regclas
 EXPLAIN (COSTS OFF) SELECT a, b, tableoid::regclass FROM t2 UNION ALL SELECT a, b, tableoid::regclass FROM t3;
 
 -- superuser is allowed to bypass RLS checks
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 SET row_security TO OFF;
 SELECT * FROM t1 WHERE f_leak(b);
 EXPLAIN (COSTS OFF) SELECT * FROM t1 WHERE f_leak(b);
 
 -- non-superuser with bypass privilege can bypass RLS policy when disabled
-SET SESSION AUTHORIZATION regress_rls_exempt_user;
+SET ROLE regress_rls_exempt_user;
 SET row_security TO OFF;
 SELECT * FROM t1 WHERE f_leak(b);
 EXPLAIN (COSTS OFF) SELECT * FROM t1 WHERE f_leak(b);
@@ -312,7 +312,7 @@ EXPLAIN (COSTS OFF) SELECT * FROM t1 WHERE f_leak(b);
 -- Partitioned Tables
 --
 
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 
 CREATE TABLE part_document (
     did         int,
@@ -359,18 +359,18 @@ CREATE POLICY pp1r ON part_document AS RESTRICTIVE TO regress_rls_dave
 SELECT * FROM pg_policies WHERE schemaname = '"'"'regress_rls_schema'"'"' AND tablename like '"'"'%part_document%'"'"' ORDER BY policyname;
 
 -- viewpoint from regress_rls_bob
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SET row_security TO ON;
 SELECT * FROM part_document WHERE f_leak(dtitle) ORDER BY did;
 EXPLAIN (COSTS OFF) SELECT * FROM part_document WHERE f_leak(dtitle);
 
 -- viewpoint from regress_rls_carol
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SELECT * FROM part_document WHERE f_leak(dtitle) ORDER BY did;
 EXPLAIN (COSTS OFF) SELECT * FROM part_document WHERE f_leak(dtitle);
 
 -- viewpoint from regress_rls_dave
-SET SESSION AUTHORIZATION regress_rls_dave;
+SET ROLE regress_rls_dave;
 SELECT * FROM part_document WHERE f_leak(dtitle) ORDER BY did;
 EXPLAIN (COSTS OFF) SELECT * FROM part_document WHERE f_leak(dtitle);
 
@@ -390,12 +390,12 @@ SELECT * FROM part_document WHERE f_leak(dtitle) ORDER BY did;
 SELECT * FROM part_document_satire WHERE f_leak(dtitle) ORDER BY did;
 
 -- Turn on RLS and create policy on child to show RLS is checked before constraints
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 ALTER TABLE part_document_satire ENABLE ROW LEVEL SECURITY;
 CREATE POLICY pp3 ON part_document_satire AS RESTRICTIVE
     USING (cid < 55);
 -- This should fail with RLS violation now.
-SET SESSION AUTHORIZATION regress_rls_dave;
+SET ROLE regress_rls_dave;
 INSERT INTO part_document_satire VALUES (101, 55, 1, '"'"'regress_rls_dave'"'"', '"'"'testing RLS with partitions'"'"'); -- fail
 -- And now we cannot see directly into the partition either, due to RLS
 SELECT * FROM part_document_satire WHERE f_leak(dtitle) ORDER BY did;
@@ -405,7 +405,7 @@ SELECT * FROM part_document WHERE f_leak(dtitle) ORDER BY did;
 EXPLAIN (COSTS OFF) SELECT * FROM part_document WHERE f_leak(dtitle);
 
 -- viewpoint from regress_rls_carol
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SELECT * FROM part_document WHERE f_leak(dtitle) ORDER BY did;
 EXPLAIN (COSTS OFF) SELECT * FROM part_document WHERE f_leak(dtitle);
 
@@ -413,54 +413,54 @@ EXPLAIN (COSTS OFF) SELECT * FROM part_document WHERE f_leak(dtitle);
 ALTER POLICY pp1 ON part_document USING (true);    --fail
 DROP POLICY pp1 ON part_document;                  --fail
 
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 ALTER POLICY pp1 ON part_document USING (dauthor = current_user);
 
 -- viewpoint from regress_rls_bob again
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM part_document WHERE f_leak(dtitle) ORDER BY did;
 
 -- viewpoint from rls_regres_carol again
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SELECT * FROM part_document WHERE f_leak(dtitle) ORDER BY did;
 
 EXPLAIN (COSTS OFF) SELECT * FROM part_document WHERE f_leak(dtitle);
 
 -- database superuser does bypass RLS policy when enabled
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 SET row_security TO ON;
 SELECT * FROM part_document ORDER BY did;
 SELECT * FROM part_document_satire ORDER by did;
 
 -- database non-superuser with bypass privilege can bypass RLS policy when disabled
-SET SESSION AUTHORIZATION regress_rls_exempt_user;
+SET ROLE regress_rls_exempt_user;
 SET row_security TO OFF;
 SELECT * FROM part_document ORDER BY did;
 SELECT * FROM part_document_satire ORDER by did;
 
 -- RLS policy does not apply to table owner when RLS enabled.
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 SET row_security TO ON;
 SELECT * FROM part_document ORDER by did;
 SELECT * FROM part_document_satire ORDER by did;
 
 -- When RLS disabled, other users get ERROR.
-SET SESSION AUTHORIZATION regress_rls_dave;
+SET ROLE regress_rls_dave;
 SET row_security TO OFF;
 SELECT * FROM part_document ORDER by did;
 SELECT * FROM part_document_satire ORDER by did;
 
 -- Check behavior with a policy that uses a SubPlan not an InitPlan.
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 SET row_security TO ON;
 CREATE POLICY pp3 ON part_document AS RESTRICTIVE
     USING ((SELECT dlevel <= seclv FROM uaccount WHERE pguser = current_user));
 
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 INSERT INTO part_document VALUES (100, 11, 5, '"'"'regress_rls_carol'"'"', '"'"'testing pp3'"'"'); -- fail
 
 ----- Dependencies -----
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 SET row_security TO ON;
 
 CREATE TABLE dependee (x integer, y integer);
@@ -481,58 +481,58 @@ EXPLAIN (COSTS OFF) SELECT * FROM dependent; -- After drop, should be unqualifie
 --
 -- Simple recursion
 --
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE TABLE rec1 (x integer, y integer);
 CREATE POLICY r1 ON rec1 USING (x = (SELECT r.x FROM rec1 r WHERE y = r.y));
 ALTER TABLE rec1 ENABLE ROW LEVEL SECURITY;
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM rec1; -- fail, direct recursion
 
 --
 -- Mutual recursion
 --
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE TABLE rec2 (a integer, b integer);
 ALTER POLICY r1 ON rec1 USING (x = (SELECT a FROM rec2 WHERE b = y));
 CREATE POLICY r2 ON rec2 USING (a = (SELECT x FROM rec1 WHERE y = b));
 ALTER TABLE rec2 ENABLE ROW LEVEL SECURITY;
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM rec1;    -- fail, mutual recursion
 
 --
 -- Mutual recursion via views
 --
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 CREATE VIEW rec1v AS SELECT * FROM rec1;
 CREATE VIEW rec2v AS SELECT * FROM rec2;
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 ALTER POLICY r1 ON rec1 USING (x = (SELECT a FROM rec2v WHERE b = y));
 ALTER POLICY r2 ON rec2 USING (a = (SELECT x FROM rec1v WHERE y = b));
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM rec1;    -- fail, mutual recursion via views
 
 --
 -- Mutual recursion via .s.b views
 --
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 
 DROP VIEW rec1v, rec2v CASCADE;
 
 CREATE VIEW rec1v WITH (security_barrier) AS SELECT * FROM rec1;
 CREATE VIEW rec2v WITH (security_barrier) AS SELECT * FROM rec2;
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE POLICY r1 ON rec1 USING (x = (SELECT a FROM rec2v WHERE b = y));
 CREATE POLICY r2 ON rec2 USING (a = (SELECT x FROM rec1v WHERE y = b));
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM rec1;    -- fail, mutual recursion via s.b. views
 
 --
 -- recursive RLS and VIEWs in policy
 --
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE TABLE s1 (a int, b text);
 INSERT INTO s1 (SELECT x, public.fipshash(x::text) FROM generate_series(-10,10) x);
 
@@ -548,32 +548,32 @@ CREATE POLICY p3 ON s1 FOR INSERT WITH CHECK (a = (SELECT a FROM s1));
 ALTER TABLE s1 ENABLE ROW LEVEL SECURITY;
 ALTER TABLE s2 ENABLE ROW LEVEL SECURITY;
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 CREATE VIEW v2 AS SELECT * FROM s2 WHERE y like '"'"'%af%'"'"';
 SELECT * FROM s1 WHERE f_leak(b); -- fail (infinite recursion)
 
 INSERT INTO s1 VALUES (1, '"'"'foo'"'"'); -- fail (infinite recursion)
 
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 DROP POLICY p3 on s1;
 ALTER POLICY p2 ON s2 USING (x % 2 = 0);
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM s1 WHERE f_leak(b);	-- OK
 EXPLAIN (COSTS OFF) SELECT * FROM only s1 WHERE f_leak(b);
 
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 ALTER POLICY p1 ON s1 USING (a in (select x from v2)); -- using VIEW in RLS policy
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM s1 WHERE f_leak(b);	-- OK
 EXPLAIN (COSTS OFF) SELECT * FROM s1 WHERE f_leak(b);
 
 SELECT (SELECT x FROM s1 LIMIT 1) xx, * FROM s2 WHERE y like '"'"'%28%'"'"';
 EXPLAIN (COSTS OFF) SELECT (SELECT x FROM s1 LIMIT 1) xx, * FROM s2 WHERE y like '"'"'%28%'"'"';
 
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 ALTER POLICY p2 ON s2 USING (x in (select a from s1 where b like '"'"'%d2%'"'"'));
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM s1 WHERE f_leak(b);	-- fail (infinite recursion via view)
 
 -- prepared statement with regress_rls_alice privilege
@@ -582,7 +582,7 @@ EXECUTE p1(2);
 EXPLAIN (COSTS OFF) EXECUTE p1(2);
 
 -- superuser is allowed to bypass RLS checks
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 SET row_security TO OFF;
 SELECT * FROM t1 WHERE f_leak(b);
 EXPLAIN (COSTS OFF) SELECT * FROM t1 WHERE f_leak(b);
@@ -596,7 +596,7 @@ EXECUTE p2(2);
 EXPLAIN (COSTS OFF) EXECUTE p2(2);
 
 -- also, case when privilege switch from superuser
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SET row_security TO ON;
 EXECUTE p2(2);
 EXPLAIN (COSTS OFF) EXECUTE p2(2);
@@ -604,7 +604,7 @@ EXPLAIN (COSTS OFF) EXECUTE p2(2);
 --
 -- UPDATE / DELETE and Row-level security
 --
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 EXPLAIN (COSTS OFF) UPDATE t1 SET b = b || b WHERE f_leak(b);
 UPDATE t1 SET b = b || b WHERE f_leak(b);
 
@@ -652,11 +652,11 @@ UPDATE t1 t1_1 SET b = t1_2.b FROM t1 t1_2
 WHERE t1_1.a = 4 AND t1_2.a = t1_1.a AND t1_2.b = t1_1.b
 AND f_leak(t1_1.b) AND f_leak(t1_2.b) RETURNING *, t1_1, t1_2;
 
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 SET row_security TO OFF;
 SELECT * FROM t1 ORDER BY a,b;
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SET row_security TO ON;
 EXPLAIN (COSTS OFF) DELETE FROM only t1 WHERE f_leak(b);
 EXPLAIN (COSTS OFF) DELETE FROM t1 WHERE f_leak(b);
@@ -667,7 +667,7 @@ DELETE FROM t1 WHERE f_leak(b) RETURNING tableoid::regclass, *, t1;
 --
 -- S.b. view on top of Row-level security
 --
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE TABLE b1 (a int, b text);
 INSERT INTO b1 (SELECT x, public.fipshash(x::text) FROM generate_series(-10,10) x);
 
@@ -675,11 +675,11 @@ CREATE POLICY p1 ON b1 USING (a % 2 = 0);
 ALTER TABLE b1 ENABLE ROW LEVEL SECURITY;
 GRANT ALL ON b1 TO regress_rls_bob;
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 CREATE VIEW bv1 WITH (security_barrier) AS SELECT * FROM b1 WHERE a > 0 WITH CHECK OPTION;
 GRANT ALL ON bv1 TO regress_rls_carol;
 
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 
 EXPLAIN (COSTS OFF) SELECT * FROM bv1 WHERE f_leak(b);
 SELECT * FROM bv1 WHERE f_leak(b);
@@ -694,13 +694,13 @@ UPDATE bv1 SET b = '"'"'yyy'"'"' WHERE a = 4 AND f_leak(b);
 EXPLAIN (COSTS OFF) DELETE FROM bv1 WHERE a = 6 AND f_leak(b);
 DELETE FROM bv1 WHERE a = 6 AND f_leak(b);
 
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 SELECT * FROM b1;
 --
 -- INSERT ... ON CONFLICT DO UPDATE and Row-level security
 --
 
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 DROP POLICY p1 ON document;
 DROP POLICY p1r ON document;
 
@@ -710,7 +710,7 @@ CREATE POLICY p3 ON document FOR UPDATE
   USING (cid = (SELECT cid from category WHERE cname = '"'"'novel'"'"'))
   WITH CHECK (dauthor = current_user);
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 
 -- Exists...
 SELECT * FROM document WHERE did = 2;
@@ -756,7 +756,7 @@ INSERT INTO document VALUES (79, (SELECT cid from category WHERE cname = '"'"'techno
     ON CONFLICT (did) DO UPDATE SET dtitle = EXCLUDED.dtitle RETURNING *;
 
 -- Test default USING qual enforced as WCO
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 DROP POLICY p1 ON document;
 DROP POLICY p2 ON document;
 DROP POLICY p3 ON document;
@@ -764,7 +764,7 @@ DROP POLICY p3 ON document;
 CREATE POLICY p3_with_default ON document FOR UPDATE
   USING (cid = (SELECT cid from category WHERE cname = '"'"'novel'"'"'));
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 -- Just because WCO-style enforcement of USING quals occurs with
 -- existing/target tuple does not mean that the implementation can be allowed
 -- to fail to also enforce this qual against the final tuple appended to
@@ -786,7 +786,7 @@ INSERT INTO document VALUES (79, (SELECT cid from category WHERE cname = '"'"'techno
 INSERT INTO document VALUES (2, (SELECT cid from category WHERE cname = '"'"'technology'"'"'), 1, '"'"'regress_rls_bob'"'"', '"'"'my first novel'"'"')
     ON CONFLICT (did) DO UPDATE SET cid = EXCLUDED.cid, dtitle = EXCLUDED.dtitle RETURNING *;
 
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 DROP POLICY p3_with_default ON document;
 
 --
@@ -797,7 +797,7 @@ CREATE POLICY p3_with_all ON document FOR ALL
   USING (cid = (SELECT cid from category WHERE cname = '"'"'novel'"'"'))
   WITH CHECK (dauthor = current_user);
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 
 -- Fails, since ALL WCO is enforced in insert path:
 INSERT INTO document VALUES (80, (SELECT cid from category WHERE cname = '"'"'novel'"'"'), 1, '"'"'regress_rls_carol'"'"', '"'"'my first novel'"'"')
@@ -813,7 +813,7 @@ INSERT INTO document VALUES (1, (SELECT cid from category WHERE cname = '"'"'novel'"'"')
 --
 -- MERGE
 --
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 DROP POLICY p3_with_all ON document;
 
 ALTER TABLE document ADD COLUMN dnotes text DEFAULT '"'"''"'"';
@@ -831,7 +831,7 @@ CREATE POLICY p4 ON document FOR DELETE
 
 SELECT * FROM document;
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 
 -- Fails, since update violates WITH CHECK qual on dlevel
 MERGE INTO document d
@@ -905,8 +905,8 @@ SELECT * FROM document WHERE did = 4;
 
 -- Switch to regress_rls_carol role and try the DELETE again. It should succeed
 -- this time
-RESET SESSION AUTHORIZATION;
-SET SESSION AUTHORIZATION regress_rls_carol;
+RESET ROLE;
+SET ROLE regress_rls_carol;
 
 MERGE INTO document d
 USING (SELECT 4 as sdid) s
@@ -917,8 +917,8 @@ WHEN MATCHED THEN
 	DELETE;
 
 -- Switch back to regress_rls_bob role
-RESET SESSION AUTHORIZATION;
-SET SESSION AUTHORIZATION regress_rls_bob;
+RESET ROLE;
+SET ROLE regress_rls_bob;
 
 -- Try INSERT action. This fails because we are trying to insert
 -- dauthor = regress_rls_dave and INSERT'"'"'s WITH CHECK does not allow
@@ -951,12 +951,12 @@ WHEN NOT MATCHED THEN
 
 -- drop and create a new SELECT policy which prevents us from reading
 -- any document except with category '"'"'novel'"'"'
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 DROP POLICY p1 ON document;
 CREATE POLICY p1 ON document FOR SELECT
   USING (cid = (SELECT cid from category WHERE cname = '"'"'novel'"'"'));
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 
 -- MERGE can no longer see the matching row and hence attempts the
 -- NOT MATCHED action, which results in unique key violation
@@ -993,7 +993,7 @@ WHEN MATCHED THEN
 WHEN NOT MATCHED THEN
 	INSERT VALUES (13, 44, 1, '"'"'regress_rls_bob'"'"', '"'"'new manga'"'"');
 
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 -- drop the restrictive SELECT policy so that we can look at the
 -- final state of the table
 DROP POLICY p1 ON document;
@@ -1003,7 +1003,7 @@ SELECT * FROM document;
 --
 -- ROLE/GROUP
 --
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE TABLE z1 (a int, b text);
 CREATE TABLE z2 (a int, b text);
 
@@ -1021,7 +1021,7 @@ CREATE POLICY p2 ON z1 TO regress_rls_group2 USING (a % 2 = 1);
 
 ALTER TABLE z1 ENABLE ROW LEVEL SECURITY;
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM z1 WHERE f_leak(b);
 EXPLAIN (COSTS OFF) SELECT * FROM z1 WHERE f_leak(b);
 
@@ -1042,7 +1042,7 @@ EXPLAIN (COSTS OFF) EXECUTE plancache_test;
 EXPLAIN (COSTS OFF) EXECUTE plancache_test2;
 EXPLAIN (COSTS OFF) EXECUTE plancache_test3;
 
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SELECT * FROM z1 WHERE f_leak(b);
 EXPLAIN (COSTS OFF) SELECT * FROM z1 WHERE f_leak(b);
 
@@ -1062,92 +1062,92 @@ EXPLAIN (COSTS OFF) EXECUTE plancache_test3;
 -- Views should follow policy for view owner.
 --
 -- View and Table owner are the same.
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE VIEW rls_view AS SELECT * FROM z1 WHERE f_leak(b);
 GRANT SELECT ON rls_view TO regress_rls_bob;
 
 -- Query as role that is not owner of view or table.  Should return all records.
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM rls_view;
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view;
 
 -- Query as view/table owner.  Should return all records.
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 SELECT * FROM rls_view;
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view;
 DROP VIEW rls_view;
 
 -- View and Table owners are different.
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 CREATE VIEW rls_view AS SELECT * FROM z1 WHERE f_leak(b);
 GRANT SELECT ON rls_view TO regress_rls_alice;
 
 -- Query as role that is not owner of view but is owner of table.
 -- Should return records based on view owner policies.
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 SELECT * FROM rls_view;
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view;
 
 -- Query as role that is not owner of table but is owner of view.
 -- Should return records based on view owner policies.
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM rls_view;
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view;
 
 -- Query as role that is not the owner of the table or view without permissions.
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SELECT * FROM rls_view; --fail - permission denied.
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view; --fail - permission denied.
 
 -- Query as role that is not the owner of the table or view with permissions.
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 GRANT SELECT ON rls_view TO regress_rls_carol;
 
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SELECT * FROM rls_view;
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view;
 
 -- Policy requiring access to another table.
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE TABLE z1_blacklist (a int);
 INSERT INTO z1_blacklist VALUES (3), (4);
 CREATE POLICY p3 ON z1 AS RESTRICTIVE USING (a NOT IN (SELECT a FROM z1_blacklist));
 
 -- Query as role that is not owner of table but is owner of view without permissions.
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM rls_view; --fail - permission denied.
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view; --fail - permission denied.
 
 -- Query as role that is not the owner of the table or view without permissions.
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SELECT * FROM rls_view; --fail - permission denied.
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view; --fail - permission denied.
 
 -- Query as role that is not owner of table but is owner of view with permissions.
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 GRANT SELECT ON z1_blacklist TO regress_rls_bob;
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM rls_view;
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view;
 
 -- Query as role that is not the owner of the table or view with permissions.
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SELECT * FROM rls_view;
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view;
 
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 REVOKE SELECT ON z1_blacklist FROM regress_rls_bob;
 DROP POLICY p3 ON z1;
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 DROP VIEW rls_view;
 
 --
 -- Security invoker views should follow policy for current user.
 --
 -- View and table owner are the same.
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE VIEW rls_view WITH (security_invoker) AS
     SELECT * FROM z1 WHERE f_leak(b);
 GRANT SELECT ON rls_view TO regress_rls_bob;
@@ -1159,81 +1159,81 @@ EXPLAIN (COSTS OFF) SELECT * FROM rls_view;
 
 -- Queries as other users.
 -- Should return records based on current user'"'"'s policies.
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM rls_view;
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view;
 
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SELECT * FROM rls_view;
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view;
 
 -- View and table owners are different.
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 DROP VIEW rls_view;
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 CREATE VIEW rls_view WITH (security_invoker) AS
     SELECT * FROM z1 WHERE f_leak(b);
 GRANT SELECT ON rls_view TO regress_rls_alice;
 GRANT SELECT ON rls_view TO regress_rls_carol;
 
 -- Query as table owner.  Should return all records.
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 SELECT * FROM rls_view;
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view;
 
 -- Queries as other users.
 -- Should return records based on current user'"'"'s policies.
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM rls_view;
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view;
 
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SELECT * FROM rls_view;
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view;
 
 -- Policy requiring access to another table.
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE POLICY p3 ON z1 AS RESTRICTIVE USING (a NOT IN (SELECT a FROM z1_blacklist));
 
 -- Query as role that is not owner of table but is owner of view without permissions.
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM rls_view; --fail - permission denied.
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view; --fail - permission denied.
 
 -- Query as role that is not the owner of the table or view without permissions.
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SELECT * FROM rls_view; --fail - permission denied.
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view; --fail - permission denied.
 
 -- Query as role that is not owner of table but is owner of view with permissions.
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 GRANT SELECT ON z1_blacklist TO regress_rls_bob;
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM rls_view;
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view;
 
 -- Query as role that is not the owner of the table or view without permissions.
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SELECT * FROM rls_view; --fail - permission denied.
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view; --fail - permission denied.
 
 -- Query as role that is not the owner of the table or view with permissions.
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 GRANT SELECT ON z1_blacklist TO regress_rls_carol;
 
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SELECT * FROM rls_view;
 EXPLAIN (COSTS OFF) SELECT * FROM rls_view;
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 DROP VIEW rls_view;
 
 --
 -- Command specific
 --
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 
 CREATE TABLE x1 (a int, b text, c text);
 GRANT ALL ON x1 TO PUBLIC;
@@ -1256,11 +1256,11 @@ CREATE POLICY p4 ON x1 FOR DELETE USING (a < 8);
 
 ALTER TABLE x1 ENABLE ROW LEVEL SECURITY;
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM x1 WHERE f_leak(b) ORDER BY a ASC;
 UPDATE x1 SET b = b || '"'"'_updt'"'"' WHERE f_leak(b) RETURNING *;
 
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SELECT * FROM x1 WHERE f_leak(b) ORDER BY a ASC;
 UPDATE x1 SET b = b || '"'"'_updt'"'"' WHERE f_leak(b) RETURNING *;
 DELETE FROM x1 WHERE f_leak(b) RETURNING *;
@@ -1268,7 +1268,7 @@ DELETE FROM x1 WHERE f_leak(b) RETURNING *;
 --
 -- Duplicate Policy Names
 --
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE TABLE y1 (a int, b text);
 CREATE TABLE y2 (a int, b text);
 
@@ -1286,14 +1286,14 @@ ALTER TABLE y2 ENABLE ROW LEVEL SECURITY;
 -- Expression structure with SBV
 --
 -- Create view as table owner.  RLS should NOT be applied.
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE VIEW rls_sbv WITH (security_barrier) AS
     SELECT * FROM y1 WHERE f_leak(b);
 EXPLAIN (COSTS OFF) SELECT * FROM rls_sbv WHERE (a = 1);
 DROP VIEW rls_sbv;
 
 -- Create view as role that does not own table.  RLS should be applied.
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 CREATE VIEW rls_sbv WITH (security_barrier) AS
     SELECT * FROM y1 WHERE f_leak(b);
 EXPLAIN (COSTS OFF) SELECT * FROM rls_sbv WHERE (a = 1);
@@ -1302,12 +1302,12 @@ DROP VIEW rls_sbv;
 --
 -- Expression structure
 --
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 INSERT INTO y2 (SELECT x, public.fipshash(x::text) FROM generate_series(0,20) x);
 CREATE POLICY p2 ON y2 USING (a % 3 = 0);
 CREATE POLICY p3 ON y2 USING (a % 4 = 0);
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM y2 WHERE f_leak(b);
 EXPLAIN (COSTS OFF) SELECT * FROM y2 WHERE f_leak(b);
 
@@ -1334,7 +1334,7 @@ DROP TABLE test_qual_pushdown;
 --
 -- Plancache invalidate on user change.
 --
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 
 DROP TABLE t1 CASCADE;
 
@@ -1366,7 +1366,7 @@ EXPLAIN (COSTS OFF) EXECUTE role_inval;
 --
 -- CTE and RLS
 --
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 DROP TABLE t1 CASCADE;
 CREATE TABLE t1 (a integer, b text);
 CREATE POLICY p1 ON t1 USING (a % 2 = 0);
@@ -1377,7 +1377,7 @@ GRANT ALL ON t1 TO regress_rls_bob;
 
 INSERT INTO t1 (SELECT x, public.fipshash(x::text) FROM generate_series(0,20) x);
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 
 WITH cte1 AS MATERIALIZED (SELECT * FROM t1 WHERE f_leak(b)) SELECT * FROM cte1;
 EXPLAIN (COSTS OFF)
@@ -1392,7 +1392,7 @@ WITH cte1 AS (INSERT INTO t1 VALUES (20, '"'"'Success'"'"') RETURNING *) SELECT * FROM c
 --
 -- Rename Policy
 --
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 ALTER POLICY p1 ON t1 RENAME TO p1; --fail
 
 SELECT polname, relname
@@ -1410,7 +1410,7 @@ SELECT polname, relname
 --
 -- Check INSERT SELECT
 --
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 CREATE TABLE t2 (a integer, b text);
 INSERT INTO t2 (SELECT * FROM t1);
 EXPLAIN (COSTS OFF) INSERT INTO t2 (SELECT * FROM t1);
@@ -1424,7 +1424,7 @@ SELECT * FROM t4;
 --
 -- RLS with JOIN
 --
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE TABLE blog (id integer, author text, post text);
 CREATE TABLE comment (blog_id integer, message text);
 
@@ -1449,48 +1449,48 @@ INSERT INTO comment VALUES
     (4, '"'"'insane!'"'"'),
     (2, '"'"'who did it?'"'"');
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 -- Check RLS JOIN with Non-RLS.
 SELECT id, author, message FROM blog JOIN comment ON id = blog_id;
 -- Check Non-RLS JOIN with RLS.
 SELECT id, author, message FROM comment JOIN blog ON id = blog_id;
 
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE POLICY comment_1 ON comment USING (blog_id < 4);
 
 ALTER TABLE comment ENABLE ROW LEVEL SECURITY;
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 -- Check RLS JOIN RLS
 SELECT id, author, message FROM blog JOIN comment ON id = blog_id;
 SELECT id, author, message FROM comment JOIN blog ON id = blog_id;
 
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 DROP TABLE blog, comment;
 
 --
 -- Default Deny Policy
 --
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 DROP POLICY p2 ON t1;
 ALTER TABLE t1 OWNER TO regress_rls_alice;
 
 -- Check that default deny does not apply to superuser.
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 SELECT * FROM t1;
 EXPLAIN (COSTS OFF) SELECT * FROM t1;
 
 -- Check that default deny does not apply to table owner.
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 SELECT * FROM t1;
 EXPLAIN (COSTS OFF) SELECT * FROM t1;
 
 -- Check that default deny applies to non-owner/non-superuser when RLS on.
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SET row_security TO ON;
 SELECT * FROM t1;
 EXPLAIN (COSTS OFF) SELECT * FROM t1;
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM t1;
 EXPLAIN (COSTS OFF) SELECT * FROM t1;
 
@@ -1498,7 +1498,7 @@ EXPLAIN (COSTS OFF) SELECT * FROM t1;
 -- COPY TO/FROM
 --
 
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 DROP TABLE copy_t CASCADE;
 CREATE TABLE copy_t (a integer, b text);
 CREATE POLICY p1 ON copy_t USING (a % 2 = 0);
@@ -1510,35 +1510,35 @@ GRANT ALL ON copy_t TO regress_rls_bob, regress_rls_exempt_user;
 INSERT INTO copy_t (SELECT x, public.fipshash(x::text) FROM generate_series(0,10) x);
 
 -- Check COPY TO as Superuser/owner.
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 SET row_security TO OFF;
 COPY (SELECT * FROM copy_t ORDER BY a ASC) TO STDOUT WITH DELIMITER '"'"','"'"';
 SET row_security TO ON;
 COPY (SELECT * FROM copy_t ORDER BY a ASC) TO STDOUT WITH DELIMITER '"'"','"'"';
 
 -- Check COPY TO as user with permissions.
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SET row_security TO OFF;
 COPY (SELECT * FROM copy_t ORDER BY a ASC) TO STDOUT WITH DELIMITER '"'"','"'"'; --fail - would be affected by RLS
 SET row_security TO ON;
 COPY (SELECT * FROM copy_t ORDER BY a ASC) TO STDOUT WITH DELIMITER '"'"','"'"'; --ok
 
 -- Check COPY TO as user with permissions and BYPASSRLS
-SET SESSION AUTHORIZATION regress_rls_exempt_user;
+SET ROLE regress_rls_exempt_user;
 SET row_security TO OFF;
 COPY (SELECT * FROM copy_t ORDER BY a ASC) TO STDOUT WITH DELIMITER '"'"','"'"'; --ok
 SET row_security TO ON;
 COPY (SELECT * FROM copy_t ORDER BY a ASC) TO STDOUT WITH DELIMITER '"'"','"'"'; --ok
 
 -- Check COPY TO as user without permissions. SET row_security TO OFF;
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SET row_security TO OFF;
 COPY (SELECT * FROM copy_t ORDER BY a ASC) TO STDOUT WITH DELIMITER '"'"','"'"'; --fail - would be affected by RLS
 SET row_security TO ON;
 COPY (SELECT * FROM copy_t ORDER BY a ASC) TO STDOUT WITH DELIMITER '"'"','"'"'; --fail - permission denied
 
 -- Check COPY relation TO; keep it just one row to avoid reordering issues
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 SET row_security TO ON;
 CREATE TABLE copy_rel_to (a integer, b text);
 CREATE POLICY p1 ON copy_rel_to USING (a % 2 = 0);
@@ -1550,69 +1550,69 @@ GRANT ALL ON copy_rel_to TO regress_rls_bob, regress_rls_exempt_user;
 INSERT INTO copy_rel_to VALUES (1, public.fipshash('"'"'1'"'"'));
 
 -- Check COPY TO as Superuser/owner.
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 SET row_security TO OFF;
 COPY copy_rel_to TO STDOUT WITH DELIMITER '"'"','"'"';
 SET row_security TO ON;
 COPY copy_rel_to TO STDOUT WITH DELIMITER '"'"','"'"';
 
 -- Check COPY TO as user with permissions.
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SET row_security TO OFF;
 COPY copy_rel_to TO STDOUT WITH DELIMITER '"'"','"'"'; --fail - would be affected by RLS
 SET row_security TO ON;
 COPY copy_rel_to TO STDOUT WITH DELIMITER '"'"','"'"'; --ok
 
 -- Check COPY TO as user with permissions and BYPASSRLS
-SET SESSION AUTHORIZATION regress_rls_exempt_user;
+SET ROLE regress_rls_exempt_user;
 SET row_security TO OFF;
 COPY copy_rel_to TO STDOUT WITH DELIMITER '"'"','"'"'; --ok
 SET row_security TO ON;
 COPY copy_rel_to TO STDOUT WITH DELIMITER '"'"','"'"'; --ok
 
 -- Check COPY TO as user without permissions. SET row_security TO OFF;
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SET row_security TO OFF;
 COPY copy_rel_to TO STDOUT WITH DELIMITER '"'"','"'"'; --fail - permission denied
 SET row_security TO ON;
 COPY copy_rel_to TO STDOUT WITH DELIMITER '"'"','"'"'; --fail - permission denied
 
 -- Check behavior with a child table.
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 SET row_security TO ON;
 CREATE TABLE copy_rel_to_child () INHERITS (copy_rel_to);
 INSERT INTO copy_rel_to_child VALUES (1, '"'"'one'"'"'), (2, '"'"'two'"'"');
 
 -- Check COPY TO as Superuser/owner.
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 SET row_security TO OFF;
 COPY copy_rel_to TO STDOUT WITH DELIMITER '"'"','"'"';
 SET row_security TO ON;
 COPY copy_rel_to TO STDOUT WITH DELIMITER '"'"','"'"';
 
 -- Check COPY TO as user with permissions.
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SET row_security TO OFF;
 COPY copy_rel_to TO STDOUT WITH DELIMITER '"'"','"'"'; --fail - would be affected by RLS
 SET row_security TO ON;
 COPY copy_rel_to TO STDOUT WITH DELIMITER '"'"','"'"'; --ok
 
 -- Check COPY TO as user with permissions and BYPASSRLS
-SET SESSION AUTHORIZATION regress_rls_exempt_user;
+SET ROLE regress_rls_exempt_user;
 SET row_security TO OFF;
 COPY copy_rel_to TO STDOUT WITH DELIMITER '"'"','"'"'; --ok
 SET row_security TO ON;
 COPY copy_rel_to TO STDOUT WITH DELIMITER '"'"','"'"'; --ok
 
 -- Check COPY TO as user without permissions. SET row_security TO OFF;
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SET row_security TO OFF;
 COPY copy_rel_to TO STDOUT WITH DELIMITER '"'"','"'"'; --fail - permission denied
 SET row_security TO ON;
 COPY copy_rel_to TO STDOUT WITH DELIMITER '"'"','"'"'; --fail - permission denied
 
 -- Check COPY FROM as Superuser/owner.
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 SET row_security TO OFF;
 COPY copy_t FROM STDIN; --ok
 1	abc
@@ -1629,14 +1629,14 @@ COPY copy_t FROM STDIN; --ok
 \.
 
 -- Check COPY FROM as user with permissions.
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SET row_security TO OFF;
 COPY copy_t FROM STDIN; --fail - would be affected by RLS.
 SET row_security TO ON;
 COPY copy_t FROM STDIN; --fail - COPY FROM not supported by RLS.
 
 -- Check COPY FROM as user with permissions and BYPASSRLS
-SET SESSION AUTHORIZATION regress_rls_exempt_user;
+SET ROLE regress_rls_exempt_user;
 SET row_security TO ON;
 COPY copy_t FROM STDIN; --ok
 1	abc
@@ -1646,18 +1646,18 @@ COPY copy_t FROM STDIN; --ok
 \.
 
 -- Check COPY FROM as user without permissions.
-SET SESSION AUTHORIZATION regress_rls_carol;
+SET ROLE regress_rls_carol;
 SET row_security TO OFF;
 COPY copy_t FROM STDIN; --fail - permission denied.
 SET row_security TO ON;
 COPY copy_t FROM STDIN; --fail - permission denied.
 
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 DROP TABLE copy_t;
 DROP TABLE copy_rel_to CASCADE;
 
 -- Check WHERE CURRENT OF
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 
 CREATE TABLE current_check (currentid int, payload text, rlsuser text);
 GRANT ALL ON current_check TO PUBLIC;
@@ -1674,7 +1674,7 @@ CREATE POLICY p3 ON current_check FOR UPDATE USING (currentid = 4) WITH CHECK (r
 
 ALTER TABLE current_check ENABLE ROW LEVEL SECURITY;
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 
 -- Can SELECT even rows
 SELECT * FROM current_check;
@@ -1709,7 +1709,7 @@ COMMIT;
 -- check pg_stats view filtering
 --
 SET row_security TO ON;
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 ANALYZE current_check;
 -- Stats visible
 SELECT row_security_active('"'"'current_check'"'"');
@@ -1717,7 +1717,7 @@ SELECT attname, most_common_vals FROM pg_stats
   WHERE tablename = '"'"'current_check'"'"'
   ORDER BY 1;
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 -- Stats not visible
 SELECT row_security_active('"'"'current_check'"'"');
 SELECT attname, most_common_vals FROM pg_stats
@@ -1733,14 +1733,14 @@ CREATE POLICY coll_p ON coll_t USING (c < ('"'"'foo'"'"'::text COLLATE "C"));
 ALTER TABLE coll_t ENABLE ROW LEVEL SECURITY;
 GRANT SELECT ON coll_t TO regress_rls_alice;
 SELECT (string_to_array(polqual, '"'"':'"'"'))[7] AS inputcollid FROM pg_policy WHERE polrelid = '"'"'coll_t'"'"'::regclass;
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 SELECT * FROM coll_t;
 ROLLBACK;
 
 --
 -- Shared Object Dependencies
 --
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 BEGIN;
 CREATE ROLE regress_rls_eve;
 CREATE ROLE regress_rls_frank;
@@ -1792,7 +1792,7 @@ ROLLBACK;
 --
 -- Non-target relations are only subject to SELECT policies
 --
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE TABLE r1 (a int);
 CREATE TABLE r2 (a int);
 INSERT INTO r1 VALUES (10), (20);
@@ -1809,7 +1809,7 @@ CREATE POLICY p3 ON r2 FOR UPDATE USING (false);
 CREATE POLICY p4 ON r2 FOR DELETE USING (false);
 ALTER TABLE r2 ENABLE ROW LEVEL SECURITY;
 
-SET SESSION AUTHORIZATION regress_rls_bob;
+SET ROLE regress_rls_bob;
 SELECT * FROM r1;
 SELECT * FROM r2;
 
@@ -1825,14 +1825,14 @@ DELETE FROM r1 USING r2 WHERE r1.a = r2.a + 2 RETURNING *; -- OK
 SELECT * FROM r1;
 SELECT * FROM r2;
 
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 DROP TABLE r1;
 DROP TABLE r2;
 
 --
 -- FORCE ROW LEVEL SECURITY applies RLS to owners too
 --
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 SET row_security = on;
 CREATE TABLE r1 (a int);
 INSERT INTO r1 VALUES (10), (20);
@@ -1866,7 +1866,7 @@ DROP TABLE r1;
 --
 -- FORCE ROW LEVEL SECURITY does not break RI
 --
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 SET row_security = on;
 CREATE TABLE r1 (a int PRIMARY KEY);
 CREATE TABLE r2 (a int REFERENCES r1);
@@ -1961,7 +1961,7 @@ DROP TABLE r1;
 -- Test INSERT+RETURNING applies SELECT policies as
 -- WithCheckOptions (meaning an error is thrown)
 --
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 SET row_security = on;
 CREATE TABLE r1 (a int);
 
@@ -1991,7 +1991,7 @@ DROP TABLE r1;
 -- Test UPDATE+RETURNING applies SELECT policies as
 -- WithCheckOptions (meaning an error is thrown)
 --
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 SET row_security = on;
 CREATE TABLE r1 (a int PRIMARY KEY);
 
@@ -2033,7 +2033,7 @@ INSERT INTO r1 VALUES (10)
 DROP TABLE r1;
 
 -- Check dependency handling
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 CREATE TABLE dep1 (c1 int);
 CREATE TABLE dep2 (c1 int);
 
@@ -2063,7 +2063,7 @@ SELECT count(*) = 0 FROM pg_depend
 					 AND refobjid = (SELECT oid FROM pg_class WHERE relname = '"'"'dep2'"'"');
 
 -- DROP OWNED BY testing
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 
 CREATE ROLE regress_rls_dob_role1;
 CREATE ROLE regress_rls_dob_role2;
@@ -2112,11 +2112,11 @@ CREATE VIEW rls_view AS SELECT * FROM rls_tbl;
 ALTER VIEW rls_view OWNER TO regress_rls_bob;
 GRANT SELECT ON rls_view TO regress_rls_alice;
 
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 SELECT * FROM ref_tbl; -- Permission denied
 SELECT * FROM rls_tbl; -- Permission denied
 SELECT * FROM rls_view; -- OK
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 
 DROP VIEW rls_view;
 DROP TABLE rls_tbl;
@@ -2130,7 +2130,7 @@ ANALYZE rls_tbl;
 ALTER TABLE rls_tbl ENABLE ROW LEVEL SECURITY;
 GRANT SELECT ON rls_tbl TO regress_rls_alice;
 
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE FUNCTION op_leak(int, int) RETURNS bool
     AS '"'"'BEGIN RAISE NOTICE '"'"''"'"'op_leak => %, %'"'"''"'"', $1, $2; RETURN $1 < $2; END'"'"'
     LANGUAGE plpgsql;
@@ -2139,11 +2139,11 @@ CREATE OPERATOR <<< (procedure = op_leak, leftarg = int, rightarg = int,
 SELECT * FROM rls_tbl WHERE a <<< 1000;
 DROP OPERATOR <<< (int, int);
 DROP FUNCTION op_leak(int, int);
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 DROP TABLE rls_tbl;
 
 -- Bug #16006: whole-row Vars in a policy don'"'"'t play nice with sub-selects
-SET SESSION AUTHORIZATION regress_rls_alice;
+SET ROLE regress_rls_alice;
 CREATE TABLE rls_tbl (a int, b int, c int);
 CREATE POLICY p1 ON rls_tbl USING (rls_tbl >= ROW(1,1,1));
 
@@ -2159,7 +2159,7 @@ INSERT INTO rls_tbl
 SELECT * FROM rls_tbl;
 
 DROP TABLE rls_tbl;
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 
 -- CVE-2023-2455: inlining an SRF may introduce an RLS dependency
 create table rls_t (c text);
@@ -2184,7 +2184,7 @@ DROP TABLE rls_t;
 --
 -- Clean up objects
 --
-RESET SESSION AUTHORIZATION;
+RESET ROLE;
 
 DROP SCHEMA regress_rls_schema CASCADE;
`},
}
