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
var testdata = "./pkg/cmd/roachtest/testdata/regression.diffs"

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
	// because there are no plans to support the features under test: vacuum,
	// txid, guc.
	tests := "varchar " +
		"char " +
		"int2 " +
		"text " +
		"boolean " +
		"name " +
		"int4 " +
		"oid " +
		"int8 " +
		"bit " +
		"uuid " +
		"float4 " +
		"pg_lsn " +
		"regproc " +
		// TODO(#41578): Reenable when money types are supported.
		// "money " +
		"enum " +
		"float8 " +
		"numeric " +
		"rangetypes " +
		"md5 " +
		"numerology " +
		"timetz " +
		// TODO(#45813): Reenable when macaddr is supported.
		// "macaddr " +
		// "macaddr8 " +
		"strings " +
		"time " +
		"date " +
		"inet " +
		"interval " +
		"multirangetypes " +
		"timestamp " +
		"timestamptz " +
		// TODO(#21286): Reenable when more geometric types are supported.
		// "point " +
		// "circle " +
		// "line " +
		// "lseg " +
		// "path " +
		// "box " +
		// "polygon " +
		"unicode " +
		"misc_sanity " +
		"tstypes " +
		"comments " +
		"geometry " +
		"xid " +
		"horology " +
		"type_sanity " +
		"expressions " +
		"mvcc " +
		"regex " +
		// TODO(#123651): re-enable when pg_catalog.pg_proc is fixed up.
		// "opr_sanity " +
		"copyselect " +
		"copydml " +
		"copy " +
		"insert_conflict " +
		"insert " +
		"create_function_c " +
		"create_operator " +
		"create_type " +
		"create_procedure " +
		"create_schema " +
		"create_misc " +
		"create_table " +
		"index_including " +
		"index_including_gist " +
		"create_view " +
		"create_index_spgist " +
		"create_index " +
		"create_cast " +
		"create_aggregate " +
		"hash_func " +
		"select " +
		"roleattributes " +
		"drop_if_exists " +
		"typed_table " +
		"errors " +
		"create_function_sql " +
		"create_am " +
		"infinite_recurse " +
		"constraints " +
		"updatable_views " +
		"triggers " +
		"random " +
		"delete " +
		"inherit " +
		"select_distinct_on " +
		"select_having " +
		"select_implicit " +
		"namespace " +
		"case " +
		"select_into " +
		"prepared_xacts " +
		"select_distinct " +
		// TODO(#117689): Transactions test causes server crash.
		// "transactions " +
		"portals " +
		"union " +
		"subselect " +
		"arrays " +
		"hash_index " +
		"update " +
		"aggregates " +
		"join " +
		"btree_index " +
		"init_privs " +
		"security_label " +
		"drop_operator " +
		"tablesample " +
		"lock " +
		"collate " +
		"replica_identity " +
		"object_address " +
		"password " +
		"identity " +
		"groupingsets " +
		"matview " +
		"generated " +
		"spgist " +
		"rowsecurity " +
		"gin " +
		"gist " +
		"brin " +
		"join_hash " +
		"privileges " +
		"brin_bloom " +
		"brin_multi " +
		"async " +
		"dbsize " +
		"alter_operator " +
		"tidrangescan " +
		"collate.icu.utf8 " +
		"tsrf " +
		"alter_generic " +
		"tidscan " +
		"tid " +
		"create_role " +
		"misc " +
		"sysviews " +
		"misc_functions " +
		"incremental_sort " +
		"merge " +
		"create_table_like " +
		"collate.linux.utf8 " +
		"collate.windows.win1252 " +
		"amutils " +
		"psql_crosstab " +
		"rules " +
		// TODO(#117689): Psql test causes server crash.
		// "psql " +
		"stats_ext " +
		"subscription " +
		"publication " +
		"portals_p2 " +
		"dependency " +
		"xmlmap " +
		"advisory_lock " +
		"select_views " +
		"combocid " +
		"tsdicts " +
		"functional_deps " +
		"equivclass " +
		"bitmapops " +
		"window " +
		"indirect_toast " +
		"tsearch " +
		"cluster " +
		"foreign_data " +
		"foreign_key " +
		"json_encoding " +
		"jsonpath_encoding " +
		"jsonpath " +
		"sqljson " +
		"json " +
		"jsonb_jsonpath " +
		"jsonb " +
		"prepare " +
		"limit " +
		"returning " +
		"plancache " +
		"conversion " +
		"xml " +
		"temp " +
		// TODO(#28296): Reenable copy2 when triggers and COPY [view] FROM are supported.
		// "copy2 " +
		"rangefuncs " +
		"sequence " +
		"truncate " +
		"alter_table " +
		"polymorphism " +
		"rowtypes " +
		"domain " +
		"largeobject " +
		// TODO(#113625): Reenable with when aggregation bug is fixed.
		// "with " +
		"plpgsql " +
		"hash_part " +
		"partition_info " +
		"reloptions " +
		"explain " +
		"memoize " +
		"compression " +
		"partition_aggregate " +
		"indexing " +
		"partition_join " +
		"partition_prune " +
		"tuplesort " +
		"stats " +
		// TODO(#107345): Reenable oidjoins when DO statements are supported.
		// "oidjoins " +
		"event_trigger "

	// psql was installed in /usr/bin/psql, so use the prefix for bindir.
	cmd := fmt.Sprintf("cd %s && "+
		"./configure --without-icu --without-readline --without-zlib && "+
		"cd %s && "+
		"make && "+
		"./pg_regress --bindir=/usr/bin --host=%s --port=%s --user=test_admin "+
		"--dbname=root --use-existing %s",
		postgresDir, pgregressDir, u.Hostname(), u.Port(), tests)
	result, err := c.RunWithDetailsSingleNode(
		ctx, t.L(), option.WithNodes(node),
		cmd,
	)

	rawResults := "stdout:\n" + result.Stdout + "\n\nstderr:\n" + result.Stderr
	t.L().Printf("Test results for pg_regress: %s", rawResults)

	// Fatal for a roachprod or transient error. A roachprod error is when result.Err==nil.
	// Proceed for any other (command) errors
	if err != nil && (result.Err == nil || rperrors.IsTransient(err)) {
		t.Fatal(err)
	}

	outputDir := t.ArtifactsDir()

	t.Status("collecting the test results")
	t.L().Printf("If the test failure is expected, consider updating the testdata with regression.diffs in artifacts")

	// Copy the test result files.
	collectFiles := []string{
		"regression.out", "regression.diffs",
	}
	for _, file := range collectFiles {
		if err := c.Get(
			ctx, t.L(),
			filepath.Join(pgregressDir, file),
			filepath.Join(outputDir, file),
			node,
		); err != nil {
			t.L().Printf("failed to retrieve %s: %s", file, err)
		}
	}

	// Compare the regression diffs.
	expectedB, err := os.ReadFile(testdata)
	if err != nil {
		t.L().Printf("Failed to read %s: %s", testdata, err)
	}
	expected := string(expectedB)
	actualB, err := os.ReadFile(filepath.Join(outputDir, "regression.diffs"))
	if err != nil {
		t.L().Printf("Failed to read %s: %s", testdata, err)
	}

	// Replace specific versions in URIs with a generic "_version_".
	issueURI := regexp.MustCompile(`https:\/\/go\.crdb\.dev\/issue-v\/(\d+)\/[^\/|^\s]+`)
	actualB = issueURI.ReplaceAll(actualB, []byte("https://go.crdb.dev/issue-v/$1/_version_"))
	docsURI := regexp.MustCompile(`https:\/\/www\.cockroachlabs.com\/docs\/[^\/|^\s]+`)
	actualB = docsURI.ReplaceAll(actualB, []byte("https://www.cockroachlabs.com/docs/_version_"))
	actual := string(actualB)

	if expected != actual {
		diff, diffErr := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A:        difflib.SplitLines(expected),
			B:        difflib.SplitLines(actual),
			FromFile: "testdata/regression.diffs",
			ToFile:   "regression.diffs",
			Context:  3,
		})
		if diffErr != nil {
			t.Fatalf("failed to produce diff: %s", diffErr)
		}
		t.Fatalf("Regression diffs do not match expected output:\n%s\n"+
			"If the diff is expected, copy regression.diffs from the test artifacts to pkg/cmd/roachtest/testdata/regression.diffs",
			diff)
	} else {
		t.L().Printf("Regression diffs passed")
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

@@ -288,7 +290,8 @@ WHERE t1.typelem = t2.oid AND NOT

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
	// TODO(#114846): Enable array_to_set function calls when internal
	// error is fixed.
	// TODO(#118702): Remove the patch around getrngfunc9 when
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
@@ -537,7 +538,7 @@ select array_to_set(array['"'"'one'"'"', '"'"'two'"'"']);
 select * from array_to_set(array['"'"'one'"'"', '"'"'two'"'"']) as t(f1 int,f2 text);
 select * from array_to_set(array['"'"'one'"'"', '"'"'two'"'"']); -- fail
 -- after-the-fact coercion of the columns is now possible, too
-select * from array_to_set(array['"'"'one'"'"', '"'"'two'"'"']) as t(f1 numeric(4,2),f2 text);
+-- select * from array_to_set(array['"'"'one'"'"', '"'"'two'"'"']) as t(f1 numeric(4,2),f2 text);
 -- and if it doesn'"'"'t work, you get a compile-time not run-time error
 select * from array_to_set(array['"'"'one'"'"', '"'"'two'"'"']) as t(f1 point,f2 text);
 
@@ -553,7 +554,7 @@ $$ language sql immutable;
 
 select array_to_set(array['"'"'one'"'"', '"'"'two'"'"']);
 select * from array_to_set(array['"'"'one'"'"', '"'"'two'"'"']) as t(f1 int,f2 text);
-select * from array_to_set(array['"'"'one'"'"', '"'"'two'"'"']) as t(f1 numeric(4,2),f2 text);
+-- select * from array_to_set(array['"'"'one'"'"', '"'"'two'"'"']) as t(f1 numeric(4,2),f2 text);
 select * from array_to_set(array['"'"'one'"'"', '"'"'two'"'"']) as t(f1 point,f2 text);
 explain (verbose, costs off)
   select * from array_to_set(array['"'"'one'"'"', '"'"'two'"'"']) as t(f1 numeric(4,2),f2 text);
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
	// TODO(#114858): Remove this patch when the internal error is fixed.
	{"stats.sql", `diff --git a/src/test/regress/sql/stats.sql b/src/test/regress/sql/stats.sql
index 1e21e55c6d..a2b6cce79e 100644
--- a/src/test/regress/sql/stats.sql
+++ b/src/test/regress/sql/stats.sql
@@ -131,8 +131,8 @@ COMMIT;
 ---
 CREATE FUNCTION stats_test_func1() RETURNS VOID LANGUAGE plpgsql AS $$BEGIN END;$$;
 SELECT '"'"'stats_test_func1()'"'"'::regprocedure::oid AS stats_test_func1_oid \gset
-CREATE FUNCTION stats_test_func2() RETURNS VOID LANGUAGE plpgsql AS $$BEGIN END;$$;
-SELECT '"'"'stats_test_func2()'"'"'::regprocedure::oid AS stats_test_func2_oid \gset
+-- CREATE FUNCTION stats_test_func2() RETURNS VOID LANGUAGE plpgsql AS $$BEGIN END;$$;
+-- SELECT '"'"'stats_test_func2()'"'"'::regprocedure::oid AS stats_test_func2_oid \gset
 
 -- test that stats are accumulated
 BEGIN;
`},
	// TODO(#114849): Remove the patch around void_return_expr when the
	// internal error is fixed.
	// TODO(#118702): Remove the patch around wslot_slotlink_view when
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
 
 
 
@@ -2171,7 +2171,7 @@ begin
     perform 2+2;
 end;$$ language plpgsql;
 
-select void_return_expr();
+-- select void_return_expr();
 
 -- but ordinary functions are not
 create function missing_return_expr() returns int as $$
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
}
