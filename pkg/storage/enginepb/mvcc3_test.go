// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package enginepb

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/zerofields"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

func TestMVCCValueHeader_IsEmpty(t *testing.T) {
	allFieldsSet := MVCCValueHeader{
		LocalTimestamp: hlc.ClockTimestamp{WallTime: 1, Logical: 1, Synthetic: true},
	}
	require.NoError(t, zerofields.NoZeroField(allFieldsSet), "make sure you update the IsEmpty method")
	require.True(t, MVCCValueHeader{}.IsEmpty())
	require.False(t, allFieldsSet.IsEmpty())
}
