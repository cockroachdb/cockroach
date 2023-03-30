// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilities

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIDs ensures that iterating IDs always works for the ID lookup functions.
func TestIDs(t *testing.T) {
	for _, id := range IDs {
		_, err := GetValueByID(DefaultCapabilities(), id)
		require.NoError(t, err, id)
		_, ok := FromID(id)
		require.True(t, ok, id)
	}
}
