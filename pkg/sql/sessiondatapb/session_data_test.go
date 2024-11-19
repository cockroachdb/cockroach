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
