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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/zerofields"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func populatedMVCCValueHeader() MVCCValueHeader {
	allFieldsSet := MVCCValueHeader{
		LocalTimestamp: hlc.ClockTimestamp{WallTime: 1, Logical: 1, Synthetic: true},
	}
	allFieldsSet.KVNemesisSeq.Set(123)
	return allFieldsSet
}

func TestMVCCValueHeader_IsEmpty(t *testing.T) {
	allFieldsSet := populatedMVCCValueHeader()
	require.NoError(t, zerofields.NoZeroField(allFieldsSet), "make sure you update the IsEmpty method")
	require.True(t, MVCCValueHeader{}.IsEmpty())
	require.False(t, allFieldsSet.IsEmpty())
}

func TestMVCCValueHeader_MarshalUnmarshal(t *testing.T) {
	vh := populatedMVCCValueHeader()
	b, err := protoutil.Marshal(&vh)
	require.NoError(t, err)
	var vh2 MVCCValueHeader
	require.NoError(t, protoutil.Unmarshal(b, &vh2))
	reflect.DeepEqual(vh, vh2)
}
