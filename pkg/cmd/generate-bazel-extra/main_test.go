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
	"bytes"
	"encoding/gob"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestParseQueryXML(t *testing.T) {
	testDataDir := testutils.TestDataPath(t, "TestParseQueryXML")
	files, err := os.ReadDir(testDataDir)
	require.NoError(t, err)
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".expected") {
			continue
		}
		xmlData, err := os.ReadFile(filepath.Join(testDataDir, file.Name()))
		require.NoError(t, err)
		sizeToTargets, err := parseQueryXML(xmlData)
		require.NoError(t, err)
		expectedData, err := os.ReadFile(filepath.Join(testDataDir, strings.Split(file.Name(), ".xml")[0]+".expected"))
		require.NoError(t, err)
		var expectedDataMap map[string][]string
		require.NoError(t, gob.NewDecoder(bytes.NewReader(expectedData)).Decode(&expectedDataMap))
		for size := range expectedDataMap {
			require.ElementsMatch(t, expectedDataMap[size], sizeToTargets[size])
		}
	}
}
