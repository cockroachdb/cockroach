// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestParseQueryXML(t *testing.T) {
	expectedDataMap := map[string][]string{
		"small": {
			"//pkg/sql/sem/eval/cast_test:cast_test_test",
			"//pkg/sql/sem/eval/eval_test:eval_test_test",
		},
		"medium": {
			"//pkg/sql/sem/builtins:builtins_test",
			"//pkg/sql/sem/builtins/pgformat:pgformat_test",
			"//pkg/sql/sem/tree:tree_test",
		},
		"large": {
			"//pkg/sql/sem/cast:cast_test",
		},
		"enormous": {
			"//pkg/sql/sem/eval:eval_test",
			"//pkg/sql/sem/normalize:normalize_test",
		},
	}
	xmlData, err := os.ReadFile(filepath.Join(testutils.TestDataPath(t, "TestParseQueryXML"), "tc1.xml"))
	require.NoError(t, err)
	sizeToTargets, err := parseQueryXML(xmlData)
	require.NoError(t, err)
	for size := range expectedDataMap {
		require.ElementsMatch(t, expectedDataMap[size], sizeToTargets[size])
	}
}
