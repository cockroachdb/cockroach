// Copyright 2018 The Cockroach Authors.
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

package sql

import (
	"context"
	gosql "database/sql"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func TestQueryCache(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	conns := make([]*gosql.Conn, 4)
	runners := make([]*sqlutils.SQLRunner, len(conns))
	for i := range conns {
		var err error
		conns[i], err = db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		runners[i] = sqlutils.MakeSQLRunner(conns[i])
	}
	r0, r1 := runners[0], runners[1]

	r0.Exec(t, "SET CLUSTER SETTING sql.query_cache.enabled = true")

	init := func(t *testing.T) {
		r0.Exec(t, "DROP DATABASE IF EXISTS db1")
		r0.Exec(t, "DROP DATABASE IF EXISTS db2")
		r0.Exec(t, "CREATE DATABASE db1")
		r0.Exec(t, "CREATE TABLE db1.t (a INT, b INT)")
		r0.Exec(t, "INSERT INTO db1.t VALUES (1, 1)")
		for _, r := range runners {
			r.Exec(t, "SET DATABASE = db1")
		}
	}

	t.Run("simple", func(t *testing.T) {
		init(t)
		// Alternate between the connections.
		for i := 0; i < 5; i++ {
			for _, r := range runners {
				r.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1", "1"}})
			}
		}
	})

	t.Run("parallel", func(t *testing.T) {
		init(t)
		var group errgroup.Group
		for connIdx := range conns {
			c := conns[connIdx]
			group.Go(func() error {
				for j := 0; j < 10; j++ {
					rows, err := c.QueryContext(context.Background(), "SELECT * FROM t")
					if err != nil {
						return err
					}
					res, err := sqlutils.RowsToStrMatrix(rows)
					if err != nil {
						return err
					}
					if !reflect.DeepEqual(res, [][]string{{"1", "1"}}) {
						return errors.Errorf("incorrect results %v", res)
					}
				}
				return nil
			})
		}
		if err := group.Wait(); err != nil {
			t.Fatal(err)
		}
	})

	// Test connections running the same statement but under different databases.
	t.Run("multidb", func(t *testing.T) {
		init(t)
		r0.Exec(t, "CREATE DATABASE db2")
		r0.Exec(t, "CREATE TABLE db2.t (a INT)")
		r0.Exec(t, "INSERT INTO db2.t VALUES (2)")
		for i := range runners {
			if i%2 == 1 {
				runners[i].Exec(t, "SET DATABASE = db2")
			}
		}
		// Alternate between the connections.
		for i := 0; i < 5; i++ {
			for i, r := range runners {
				var res [][]string
				if i%2 == 0 {
					res = [][]string{{"1", "1"}}
				} else {
					res = [][]string{{"2"}}
				}
				r.CheckQueryResults(t, "SELECT * FROM t", res)
			}
		}
	})

	// Test that a schema change triggers cache invalidation.
	t.Run("schemachange", func(t *testing.T) {
		init(t)
		r0.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1", "1"}})
		r1.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1", "1"}})
		r0.Exec(t, "ALTER TABLE t ADD COLUMN c INT AS (a+b) STORED")
		r1.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1", "1", "2"}})
	})

	// Test a schema change where the other connections are running the query in
	// parallel.
	t.Run("schemachange-parallel", func(t *testing.T) {
		init(t)
		var group errgroup.Group
		for connIdx := 1; connIdx < len(conns); connIdx++ {
			c := conns[connIdx]
			group.Go(func() error {
				sawChanged := false
				doQuery := func() error {
					rows, err := c.QueryContext(context.Background(), "SELECT * FROM t")
					if err != nil {
						return err
					}
					res, err := sqlutils.RowsToStrMatrix(rows)
					if err != nil {
						return err
					}
					if reflect.DeepEqual(res, [][]string{{"1", "1"}}) {
						if sawChanged {
							return errors.Errorf("Saw updated results, then older results")
						}
					} else if reflect.DeepEqual(res, [][]string{{"1", "1", "2"}}) {
						sawChanged = true
					} else {
						return errors.Errorf("incorrect results %v", res)
					}
					return nil
				}

				// Run the query until we see an updated result.
				for !sawChanged {
					if err := doQuery(); err != nil {
						return err
					}
				}

				// Now run the query a bunch more times to make sure we keep reading the
				// updated version.
				for i := 0; i < 10; i++ {
					if err := doQuery(); err != nil {
						return err
					}
				}
				return nil
			})
		}
		r0.Exec(t, "ALTER TABLE t ADD COLUMN c INT AS (a+b) STORED")
	})
}
