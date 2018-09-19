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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func eqWhitespace(a, b string) bool {
	return strings.Replace(a, "\t", "", -1) == strings.Replace(b, "\t", "", -1)
}

func TestStandAloneShowCreateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	want := `CREATE TABLE jobs (
			id INT NOT NULL DEFAULT unique_rowid(),
			status STRING NOT NULL,
			created TIMESTAMP NOT NULL DEFAULT now():::TIMESTAMP,
			payload BYTES NOT NULL,
			CONSTRAINT "primary" PRIMARY KEY (id ASC),
			CONSTRAINT fk FOREIGN KEY (status) REFERENCES "[52 as ref]" ("???"),
			INDEX jobs_status_created_idx (status ASC, created ASC) INTERLEAVE IN PARENT "[51 as parent]" (status),
			FAMILY fam_0_id_status_created_payload (id, status, created, payload)
		)`

	desc := sqlbase.JobsTable
	desc.Indexes = []sqlbase.IndexDescriptor{sqlbase.JobsTable.Indexes[0]}
	desc.Indexes[0].ForeignKey = sqlbase.ForeignKeyReference{Table: 52, Name: "fk", Index: 1, SharedPrefixLen: 1}
	desc.Indexes[0].Interleave.Ancestors = []sqlbase.InterleaveDescriptor_Ancestor{{TableID: 51, IndexID: 10, SharedPrefixLen: 1}}

	name := tree.Name(desc.Name)
	got, err := ShowCreateTable(context.TODO(), &name, "", &desc, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if !eqWhitespace(got, want) {
		t.Fatalf("%s, want %s", got, want)
	}
}
