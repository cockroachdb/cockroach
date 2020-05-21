// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestIsSupportedSchemaName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name  string
		valid bool
	}{
		{"db_name", false},
		{"public", true},
		{"pg_temp", true},
		{"pg_temp_1234_1", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.valid, isSupportedSchemaName(tree.Name(tc.name)))
		})
	}
}
