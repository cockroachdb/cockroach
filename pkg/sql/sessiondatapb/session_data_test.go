// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
