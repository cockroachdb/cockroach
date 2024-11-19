// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDefaultOid(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		id  descpb.ID
		oid *tree.DOid
	}{
		{
			1,
			tree.NewDOid(1),
		},
		{
			2,
			tree.NewDOid(2),
		},
	}

	for _, tc := range testCases {
		oid := tableOid(tc.id)
		if tc.oid.Oid != oid.Oid {
			t.Fatalf("expected oid %d(%32b), got %d(%32b)", tc.oid.Oid, tc.oid.Oid, oid.Oid, oid.Oid)
		}
	}
}
