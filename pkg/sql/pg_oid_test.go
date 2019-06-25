// Copyright 2019 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDefaultOid(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		id  sqlbase.ID
		oid *tree.DOid
	}{
		{
			1,
			tree.NewDOid(tree.DInt(1)),
		},
		{
			2,
			tree.NewDOid(tree.DInt(2)),
		},
	}

	for _, tc := range testCases {
		oid := defaultOid(tc.id)
		if tc.oid.DInt != oid.DInt {
			t.Fatalf("expected oid %d(%32b), got %d(%32b)", tc.oid.DInt, tc.oid.DInt, oid.DInt, oid.DInt)
		}
	}
}
