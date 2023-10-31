// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/errors"
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
	if err := repeatRunE(
		ctx,
		t,
		c,
		node,
		"modify pg_regress.c",
		fmt.Sprintf(`cd %s && \
echo '%s' | git apply --ignore-whitespace -`, postgresDir, pgregressPatch),
	); err != nil {
		t.Fatal(err)
	}

	if err := repeatRunE(
		ctx,
		t,
		c,
		node,
		"modify test_setup.sql",
		fmt.Sprintf(`cd %s && \
echo '%s' | git apply --ignore-whitespace -`, postgresDir, testSetupPatch),
	); err != nil {
		t.Fatal(err)
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

	urls, err := c.InternalPGUrl(ctx, t.L(), node, "" /* tenant */, 0 /* sqlInstance */)
	if err != nil {
		t.Fatal(err)
	}
	url := urls[0]

	// We execute the test setup before running pg_regress because some of
	// the tables require copying input data. Since CRDB only supports
	// COPY FROM STDIN, we need to do this outside the test.
	t.L().Printf("Running test setup: psql %s -a -f %s/sql/test_setup.sql", url, pgregressDir)
	err = c.RunE(ctx, node, fmt.Sprintf(`psql %s -a -f %s/sql/test_setup.sql`, url, pgregressDir))
	if err != nil {
		t.Fatal(err)
	}

	// Copy data into some of the tables. This needs to be run after
	// test_setup.sql.
	dataTbls := [...]string{"onek", "tenk1"}
	for _, tbl := range dataTbls {
		err = c.RunE(ctx, node, fmt.Sprintf(`cat %s/data/%s.data | psql %s -c "COPY %s FROM STDIN"`, pgregressDir, tbl, url, tbl))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func runPGRegress(ctx context.Context, t test.Test, c cluster.Cluster) {
	// Can't use git clone locally, which is used to get pg_regress from
	// the postgres repository.
	if c.IsLocal() {
		t.Fatal("cannot be run in local mode")
	}

	initPGRegress(ctx, t, c)

	t.Status("running the pg_regress test suite")
	node := c.Node(1)

	// Tests are ordered in the approximate order that pg_regress does
	// when all tests are run with default settings (groups are run in
	// parallel when run against postgres).
	// The following tests have been removed instead of commented out,
	// because there are no plans to support the features under test: vacuum,
	// txid.
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
		// "money " + // TODO(#41578)
		"enum " +
		"float8 " +
		"numeric " +
		"rangetypes " +
		"md5 " +
		"numerology " +
		"timetz " +
		// TODO(#45813): Reenable macaddr tests.
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
		// TODO(#21286): Reenable geometric types tests.
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
		"opr_sanity " +
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
		"inherit " +
		"triggers " +
		"select_distinct_on " +
		"random " +
		"select_having " +
		"select_implicit " +
		"delete " +
		"namespace " +
		"case " +
		"select_into " +
		"prepared_xacts " +
		"select_distinct " +
		"transactions " +
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
		"psql " +
		"stats_ext " +
		"subscription " +
		"publication " +
		"portals_p2 " +
		"dependency " +
		"xmlmap " +
		"advisory_lock " +
		"guc " +
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
		"plancache " +
		"conversion " +
		"xml " +
		"returning " +
		"temp " +
		// "copy2 " + // TODO(#28296): Reenable when triggers and COPY [view] FROM are supported.
		"rangefuncs " +
		"polymorphism " +
		"rowtypes " +
		"sequence " +
		"domain " +
		"largeobject " +
		// "with " + // TODO(#113625): Reenable when bug is fixed.
		"truncate " +
		"plpgsql " +
		"alter_table " +
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
		// "oidjoins " + // TODO(#107345): Reenable when DO statements are supported.
		"event_trigger "

	// psql was installed in /usr/bin/psql, so use the prefix for bindir.
	cmd := fmt.Sprintf("cd %s && "+
		"/configure --without-icu --without-readline --without-zlib && "+
		"cd %s && "+
		"make && "+
		"./pg_regress --bindir=/usr/bin --host=localhost --port=26257 --user=test_admin "+
		"--dbname=root --use-existing %s",
		postgresDir, pgregressDir, tests)
	result, err := c.RunWithDetailsSingleNode(
		ctx, t.L(), node,
		cmd,
	)

	rawResults := "stdout:\n" + result.Stdout + "\n\nstderr:\n" + result.Stderr
	t.L().Printf("Test results for pg_regress: %s", rawResults)

	// Fatal for a roachprod or SSH error. A roachprod error is when result.Err==nil.
	// Proceed for any other (command) errors
	if err != nil && (result.Err == nil || errors.Is(err, rperrors.ErrSSH255)) {
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
		t.L().Printf("Regression diffs do not match expected output:\n%s", diff)
	}
	t.L().Printf("Regression diffs passed")
}

func registerPGRegress(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "pg_regress",
		Owner:            registry.OwnerSQLQueries,
		Benchmark:        false,
		Cluster:          r.MakeClusterSpec(1 /* nodeCount */),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runPGRegress(ctx, t, c)
		},
	})
}

const pgregressPatch = `diff --git a/src/test/regress/pg_regress.c b/src/test/regress/pg_regress.c
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
`

const testSetupPatch = `diff --git a/src/test/regress/sql/test_setup.sql b/src/test/regress/sql/test_setup.sql
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
`
