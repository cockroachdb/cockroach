// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sessiondatapb

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/stretchr/testify/require"
)

func TestSessionDataJsonCompat(t *testing.T) {
	expectedSessionData := SessionData{
		VectorizeMode: VectorizeOn,
	}
	json, err := protoreflect.MessageToJSON(&expectedSessionData, protoreflect.FmtFlags{})
	require.NoError(t, err)
	actualSessionData := SessionData{}
	_, err = protoreflect.JSONBMarshalToMessage(json, &actualSessionData)
	require.NoError(t, err)
	require.Equal(t, expectedSessionData, actualSessionData)
}

func TestSerialNormalizationRoundTrip(t *testing.T) {
	for s := range maxSerialNormalizationMode {
		expectedVal := SerialNormalizationMode(s)
		str := expectedVal.String()
		actualVal, ok := SerialNormalizationModeFromString(str)
		require.True(t, ok)
		require.Equal(t, expectedVal, actualVal)
	}
}

func TestDefaultPgDumpCompatibilityForAppName(t *testing.T) {
	tests := []struct {
		name        string
		appName     string
		expectedVal string
		expectedOk  bool
	}{
		{name: "pg_dump", appName: "pg_dump", expectedVal: PgDumpCompatibilityCockroachDB, expectedOk: true},
		{name: "pg_restore", appName: "pg_restore", expectedVal: PgDumpCompatibilityCockroachDB, expectedOk: true},
		{name: "pg_dumpall", appName: "pg_dumpall", expectedVal: PgDumpCompatibilityCockroachDB, expectedOk: true},
		{name: "unrelated app name", appName: "psql", expectedOk: false},
		{name: "empty app name", appName: "", expectedOk: false},
		// The match is case-sensitive because that is exactly what the tools send.
		{name: "wrong case is not matched", appName: "PG_DUMP", expectedOk: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			val, ok := DefaultPgDumpCompatibilityForAppName(tc.appName)
			require.Equal(t, tc.expectedOk, ok)
			require.Equal(t, tc.expectedVal, val)
		})
	}
}
