// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clusterversionpb

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestClusterVersionEncodeDecode(t *testing.T) {
	leaktest.AfterTest(t)()

	encoded, err := EncodingFromVersionStr("22.2")
	require.NoError(t, err)
	cv, err := Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, "22.2", cv.String())
	require.Equal(t, encoded, cv.Encode())
}
