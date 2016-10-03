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
// Author: Matt Jibson (mjibson@gmail.com)

package sql_test

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestSplitAt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := createTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

	r := sqlutils.MakeSQLRunner(t, db)

	r.Exec("CREATE DATABASE d")
	r.Exec(`CREATE TABLE d.t (
		i INT,
		s STRING,
		PRIMARY KEY (i, s),
		INDEX s_idx (s)
	)`)
	r.Exec(`CREATE TABLE d.i (k INT PRIMARY KEY)`)

	tests := []struct {
		in    string
		error string
		args  []interface{}
	}{
		{
			in: "ALTER TABLE d.t SPLIT AT (2, 'b')",
		},
		{
			in:    "ALTER TABLE d.t SPLIT AT (2, 'b')",
			error: "range is already split",
		},
		{
			in:    "ALTER TABLE d.t SPLIT AT ('c', 3)",
			error: "argument of SPLIT AT must be type int, not type string",
		},
		{
			in:    "ALTER TABLE d.t SPLIT AT (4)",
			error: "expected 2 expressions, got 1",
		},
		{
			in: "ALTER TABLE d.t SPLIT AT (5, 'e')",
		},
		{
			in:    "ALTER TABLE d.t SPLIT AT (i, s)",
			error: `name "i" is not defined`,
		},
		{
			in: "ALTER INDEX d.t@s_idx SPLIT AT ('f')",
		},
		{
			in:    "ALTER INDEX d.t@not_present SPLIT AT ('g')",
			error: `index "not_present" does not exist`,
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT (avg(1))",
			error: "unknown signature for avg: avg(int) (desired <int>)",
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT (avg(k))",
			error: `avg: name "k" is not defined`,
		},
		{
			in:   "ALTER TABLE d.i SPLIT AT ($1)",
			args: []interface{}{8},
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT ($1)",
			error: "no value provided for placeholder: $1",
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT ($1)",
			args:  []interface{}{"blah"},
			error: "error in argument for $1: strconv.ParseInt",
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT ($1::string)",
			args:  []interface{}{"1"},
			error: "argument of SPLIT AT must be type int, not type string",
		},
		{
			in: "ALTER TABLE d.i SPLIT AT ((SELECT 1))",
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT ((SELECT 1, 2))",
			error: "subquery must return only one column, found 2",
		},
	}

	for _, tt := range tests {
		var key roachpb.Key
		var pretty string
		err := db.QueryRow(tt.in, tt.args...).Scan(&key, &pretty)
		if err != nil && tt.error == "" {
			t.Fatalf("%s: unexpected error: %s", tt.in, err)
		} else if tt.error != "" && err == nil {
			t.Fatalf("%s: expected error: %s", tt.in, tt.error)
		} else if err != nil && tt.error != "" {
			if !strings.Contains(err.Error(), tt.error) {
				t.Fatalf("%s: unexpected error: %s", tt.in, err)
			}
		} else {
			// Successful split, verify it happened.
			rng, err := serverutils.LookupRange(s.DistSender(), key)
			if err != nil {
				t.Fatal(err)
			}
			expect := roachpb.Key(keys.MakeRowSentinelKey(rng.StartKey))
			if !expect.Equal(key) {
				t.Fatalf("%s: expected range start %s, got %s", tt.in, pretty, expect)
			}
		}
	}
}
