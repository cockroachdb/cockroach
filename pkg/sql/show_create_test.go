// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
			id INT8 NOT NULL DEFAULT unique_rowid(),
			status STRING NOT NULL,
			created TIMESTAMP NOT NULL DEFAULT now():::TIMESTAMP,
			payload BYTES NOT NULL,
			CONSTRAINT "primary" PRIMARY KEY (id ASC),
			CONSTRAINT fk FOREIGN KEY ("???") REFERENCES "[52 as ref]"("???"),
			INDEX jobs_status_created_idx (status ASC, created ASC) INTERLEAVE IN PARENT "[51 as parent]" (status),
			FAMILY fam_0_id_status_created_payload (id, status, created, payload)
		)`

	desc := sqlbase.JobsTable
	desc.Indexes = []sqlbase.IndexDescriptor{sqlbase.JobsTable.Indexes[0]}
	desc.Indexes[0].Interleave.Ancestors = []sqlbase.InterleaveDescriptor_Ancestor{{TableID: 51, IndexID: 10, SharedPrefixLen: 1}}
	desc.OutboundFKs = make([]*sqlbase.ForeignKeyConstraint, 1)
	desc.OutboundFKs[0] = &sqlbase.ForeignKeyConstraint{
		ReferencedTableID: 52,
		Name:              "fk",
	}

	name := tree.Name(desc.Name)
	got, err := ShowCreateTable(context.TODO(), &name, "", &desc, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if !eqWhitespace(got, want) {
		t.Fatalf("%s, want %s", got, want)
	}
}
