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
	"time"

	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestSplitAt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := createTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

	if _, err := db.Exec(`
		CREATE DATABASE d;
		CREATE TABLE d.t (i INT, s STRING, PRIMARY KEY (i, s))
		`); err != nil {
		t.Fatal(err)
	}

	// Without this, this test regularly fails with: "conflict updating range
	// descriptors".
	time.Sleep(time.Millisecond * 100)

	tests := []struct {
		in    string
		error string
	}{
		{
			in: "SPLIT d.t AT 2, 'b'",
		},
		{
			in:    "SPLIT d.t AT 2, 'b'",
			error: "range is already split",
		},
		{
			in:    "SPLIT d.t AT 'c', 3",
			error: "expected type int for column 1, got string",
		},
		{
			in:    "SPLIT d.t AT 4",
			error: "expected 2 expressions, got 1",
		},
		{
			in: "SPLIT d.t AT 5, 'd'",
		},
		{
			in:    "SPLIT d.t AT i, s",
			error: `name "i" is not defined`,
		},
	}

	for _, tt := range tests {
		_, err := db.Exec(tt.in)
		if err != nil && tt.error == "" {
			t.Fatalf("%s: unexpected error: %s", tt.in, err)
		} else if tt.error != "" && err == nil {
			t.Fatalf("%s: expected error: %s", tt.in, tt.error)
		} else if err != nil && tt.error != "" {
			if !strings.Contains(err.Error(), tt.error) {
				t.Fatalf("%s: unexpected error: %s", tt.in, err)
			}
		}
	}
}
