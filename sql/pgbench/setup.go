// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: David Taylor (david@cockroachlabs.com)

package pgbench

import (
	"bytes"
	"database/sql"
	"fmt"
	"net/url"
	"os/exec"
)

const schema = `
DROP TABLE IF EXISTS pgbench_accounts;
DROP TABLE IF EXISTS pgbench_branches;
DROP TABLE IF EXISTS pgbench_tellers;
DROP TABLE IF EXISTS pgbench_history;

CREATE TABLE pgbench_accounts (
    aid integer NOT NULL PRIMARY KEY,
    bid integer,
    abalance integer,
    filler character(84)
);

CREATE TABLE pgbench_branches (
    bid integer NOT NULL PRIMARY KEY,
    bbalance integer,
    filler character(88)
);

CREATE TABLE pgbench_tellers (
    tid integer NOT NULL PRIMARY KEY,
    bid integer,
    tbalance integer,
    filler character(84)
);

CREATE TABLE pgbench_history (
    tid integer,
    bid integer,
    aid integer,
    delta integer,
    mtime timestamp,
    filler character(22)
);
`

// CreateAndConnect connects and creates the requested DB (dropping
// if exists) then returns a new connection to the created DB.
func CreateAndConnect(pgURL url.URL, name string) (*sql.DB, error) {
	{
		pgURL.Path = ""
		db, err := sql.Open("postgres", pgURL.String())
		if err != nil {
			return nil, err
		}
		defer db.Close()

		if _, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", name)); err != nil {
			return nil, err
		}

		if _, err := db.Exec(fmt.Sprintf(`CREATE DATABASE %s`, name)); err != nil {
			return nil, err
		}
	}

	pgURL.Path = name

	db, err := sql.Open("postgres", pgURL.String())
	if err != nil {
		return nil, err
	}
	return db, nil
}

// SetupExec creates and fills a DB and prepares a `pgbench` command
// to be run against it.
func SetupExec(pgURL url.URL, name string, accounts, transactions int) (*exec.Cmd, error) {
	db, err := CreateAndConnect(pgURL, name)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	if err := SetupBenchDB(db, accounts, true /*quiet*/); err != nil {
		return nil, err
	}

	return ExecPgbench(pgURL, name, transactions)
}

// SetupBenchDB sets up a db with the schema and initial data used by `pgbench`.
// The `-i` flag to `pgbench` is usually used to do this when testing postgres
// but the statements it generates use postgres-specific flags that cockroach does
// not support. The queries this script runs are based on a dump of a db created
// by `pgbench -i`, but sticking to the compatible subset that both cockroach and
// postgres support.
func SetupBenchDB(db *sql.DB, accounts int, quiet bool) error {
	if _, err := db.Exec(schema); err != nil {
		return err
	}
	return populateDB(db, accounts, quiet)
}

const tellers = 10

func populateDB(db *sql.DB, accounts int, quiet bool) error {
	branches := `INSERT INTO pgbench_branches (bid, bbalance, filler) VALUES (1, 7354, NULL)`
	if r, err := db.Exec(branches); err != nil {
		return err
	} else if x, err := r.RowsAffected(); err != nil {
		return err
	} else if !quiet {
		fmt.Printf("Inserted %d branch records\n", x)
	}

	// Various magic numbers came from `pg_dump` of a `pgbench` created DB.
	tellers := `INSERT INTO pgbench_tellers VALUES (1, 1, 0, NULL),
	(2, 1, 955, NULL),
	(3, 1, -3338, NULL),
	(4, 1, -24, NULL),
	(5, 1, 2287, NULL),
	(6, 1, 4129, NULL),
	(7, 1, 0, NULL),
	(8, 1, 0, NULL),
	(9, 1, 0, NULL),
	(10, 1, 3345, NULL)
	`
	if r, err := db.Exec(tellers); err != nil {
		return err
	} else if x, err := r.RowsAffected(); err != nil {
		return err
	} else if !quiet {
		fmt.Printf("Inserted %d teller records\n", x)
	}

	// Split account inserts into batches to avoid giant query.
	done := 0
	for {
		batch := 5000
		remaining := accounts - done
		if remaining < 1 {
			break
		}
		if batch > remaining {
			batch = remaining
		}
		var placeholders bytes.Buffer
		for i := 0; i < batch; i++ {
			if i > 0 {
				placeholders.WriteString(", ")
			}
			fmt.Fprintf(&placeholders, "(%d, 1, 0, '                                                                                    ')", done+i)
		}
		stmt := fmt.Sprintf(`INSERT INTO pgbench_accounts VALUES %s`, placeholders.String())
		if r, err := db.Exec(stmt); err != nil {
			return err
		} else if x, err := r.RowsAffected(); err != nil {
			return err
		} else if !quiet {
			fmt.Printf("Inserted %d account records\n", x)
		}
		done += batch
	}

	history := `
INSERT INTO pgbench_history VALUES
(5, 1, 36833, 407, CURRENT_TIMESTAMP, NULL),
(3, 1, 43082, -3338, CURRENT_TIMESTAMP, NULL),
(2, 1, 49129, 2872, CURRENT_TIMESTAMP, NULL),
(6, 1, 81223, 1064, CURRENT_TIMESTAMP, NULL),
(6, 1, 28316, 3065, CURRENT_TIMESTAMP, NULL),
(4, 1, 10146, -24, CURRENT_TIMESTAMP, NULL),
(10, 1, 12019, 2265, CURRENT_TIMESTAMP, NULL),
(2, 1, 46717, -1917, CURRENT_TIMESTAMP, NULL),
(5, 1, 68648, 1880, CURRENT_TIMESTAMP, NULL),
(10, 1, 46989, 1080, CURRENT_TIMESTAMP, NULL);`

	if r, err := db.Exec(history); err != nil {
		return err
	} else if x, err := r.RowsAffected(); err != nil {
		return err
	} else if !quiet {
		fmt.Printf("Inserted %d history records\n", x)
	}

	return nil
}
