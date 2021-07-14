package multiregionccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestAutoHoming(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var p int
	var s string
	var crdbRegion string

	tc, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(t, 3, base.TestingKnobs{})
	defer cleanup()

	sql0 := sqlutils.MakeSQLRunner(tc.Conns[0])
	sql1 := sqlutils.MakeSQLRunner(tc.Conns[1])
	sql2 := sqlutils.MakeSQLRunner(tc.Conns[2])

	/*	sql0.Exec(t, `
		SET CLUSTER SETTING sql.defaults.rehome_on_update.enabled = true;
		SET enable_rehome_on_update = true;`)*/

	sql0.Exec(t, `
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east2", "us-east3";
USE test;
CREATE TABLE rbr (p int, s string) LOCALITY REGIONAL BY ROW;
INSERT INTO rbr (p, s) VALUES (1, 'hi');
`)

	row := sql0.QueryRow(t, `
SELECT p, s, crdb_region FROM rbr WHERE p = 1`)
	row.Scan(&p, &s, &crdbRegion)

	if crdbRegion != "us-east1" {
		t.Fatalf("expected crdbRegion to be us-east1 but got %s", crdbRegion)
	}

	// Update from us-east2 should rehome row to us-east2.
	sql1.Exec(t, `
USE test;
UPDATE rbr SET (s) = ('whaddup') WHERE p = 1;`)

	row = sql0.QueryRow(t, `
SELECT p, s, crdb_region FROM rbr WHERE p = 1`)
	row.Scan(&p, &s, &crdbRegion)

	if crdbRegion != "us-east2" {
		t.Fatalf("expected crdbRegion to be us-east2 but got %s", crdbRegion)
	}

	// Update from us-east3 should rehome row to us-east3.
	sql2.Exec(t, `
USE test;
UPDATE rbr SET (s) = ('anothaone') WHERE p = 1;`)

	row = sql0.QueryRow(t, `
SELECT p, s, crdb_region FROM rbr WHERE p = 1`)
	row.Scan(&p, &s, &crdbRegion)

	if crdbRegion != "us-east3" {
		t.Fatalf("expected crdbRegion to be us-east3 but got %s", crdbRegion)
	}
}

func TestAutoHomingDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var p int
	var s string
	var crdbRegion string

	tc, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(t, 3, base.TestingKnobs{})
	defer cleanup()

	sql0 := sqlutils.MakeSQLRunner(tc.Conns[0])
	sql1 := sqlutils.MakeSQLRunner(tc.Conns[1])
	sql2 := sqlutils.MakeSQLRunner(tc.Conns[2])

	sql0.Exec(t, `
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east2", "us-east3";
USE test;
CREATE TABLE rbr (p int, s string) LOCALITY REGIONAL BY ROW;
INSERT INTO rbr (p, s) VALUES (1, 'hi');
`)

	row := sql0.QueryRow(t, `
SELECT p, s, crdb_region FROM rbr WHERE p = 1`)
	row.Scan(&p, &s, &crdbRegion)

	if crdbRegion != "us-east1" {
		t.Fatalf("expected crdbRegion to be us-east1 but got %s", crdbRegion)
	}

	// Update from us-east2 should not rehome row to us-east2 because cluster setting is disabled.
	sql1.Exec(t, `
USE test;
UPDATE rbr SET (s) = ('whaddup') WHERE p = 1;`)

	row = sql0.QueryRow(t, `
SELECT p, s, crdb_region FROM rbr WHERE p = 1`)
	row.Scan(&p, &s, &crdbRegion)

	if crdbRegion != "us-east1" {
		t.Fatalf("expected crdbRegion to be us-east1 but got %s", crdbRegion)
	}

	// Update from us-east3 should not rehome row to us-east3.
	sql2.Exec(t, `
USE test;
UPDATE rbr SET (s) = ('anothaone') WHERE p = 1;`)

	row = sql0.QueryRow(t, `
SELECT p, s, crdb_region FROM rbr WHERE p = 1`)
	row.Scan(&p, &s, &crdbRegion)

	if crdbRegion != "us-east1" {
		t.Fatalf("expected crdbRegion to be us-east1 but got %s", crdbRegion)
	}
}

func TestAutoHomingOutsideRegion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var p int
	var s string
	var crdbRegion string

	tc, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(t, 3, base.TestingKnobs{})
	defer cleanup()

	sql0 := sqlutils.MakeSQLRunner(tc.Conns[0])
	sql1 := sqlutils.MakeSQLRunner(tc.Conns[1])
	sql2 := sqlutils.MakeSQLRunner(tc.Conns[2])

	sql0.Exec(t, `
SET CLUSTER SETTING sql.defaults.rehome_on_update.enabled = true;
SET enable_rehome_on_update = true;`)

	sql0.Exec(t, `
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east2";
USE test;
CREATE TABLE rbr (p int, s string) LOCALITY REGIONAL BY ROW;
INSERT INTO rbr (p, s) VALUES (1, 'hi');
`)

	row := sql0.QueryRow(t, `
SELECT p, s, crdb_region FROM rbr WHERE p = 1`)
	row.Scan(&p, &s, &crdbRegion)

	if crdbRegion != "us-east1" {
		t.Fatalf("expected crdbRegion to be us-east1 but got %s", crdbRegion)
	}

	// Update from us-east2 should rehome row to us-east2.
	sql1.Exec(t, `
USE test;
UPDATE rbr SET (s) = ('whaddup') WHERE p = 1;`)

	row = sql0.QueryRow(t, `
SELECT p, s, crdb_region FROM rbr WHERE p = 1`)
	row.Scan(&p, &s, &crdbRegion)

	if crdbRegion != "us-east2" {
		t.Fatalf("expected crdbRegion to be us-east2 but got %s", crdbRegion)
	}

	// Update from us-east3 should rehome row to us-east1 since us-east3 isn't in the database and us-east1 is primary.
	sql2.Exec(t, `
USE test;
UPDATE rbr SET (s) = ('anothaone') WHERE p = 1;`)

	row = sql0.QueryRow(t, `
SELECT p, s, crdb_region FROM rbr WHERE p = 1`)
	row.Scan(&p, &s, &crdbRegion)

	if crdbRegion != "us-east1" {
		t.Fatalf("expected crdbRegion to be us-east1 but got %s", crdbRegion)
	}
}
