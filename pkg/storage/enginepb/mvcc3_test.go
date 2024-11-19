// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package enginepb

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/testutils/zerofields"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestTxnMetaSizeOf(t *testing.T) {
	var tm TxnMeta
	require.Equal(t, 96, int(unsafe.Sizeof(tm)))
}

func populatedMVCCValueHeader() MVCCValueHeader {
	allFieldsSet := MVCCValueHeader{
		LocalTimestamp:   hlc.ClockTimestamp{WallTime: 1, Logical: 1},
		OmitInRangefeeds: true,
		ImportEpoch:      1,
		OriginID:         1,
		OriginTimestamp:  hlc.Timestamp{WallTime: 1, Logical: 1},
	}
	allFieldsSet.KVNemesisSeq.Set(123)
	return allFieldsSet
}

func defaultMVCCValueHeader() MVCCValueHeader {
	return MVCCValueHeader{
		LocalTimestamp:   hlc.ClockTimestamp{},
		OmitInRangefeeds: false,
		ImportEpoch:      0,
		OriginID:         0,
		OriginTimestamp:  hlc.Timestamp{},
	}
}

func TestMVCCValueHeader_IsEmpty(t *testing.T) {
	allFieldsSet := populatedMVCCValueHeader()
	require.NoError(t, zerofields.NoZeroField(allFieldsSet), "make sure you update TestMVCCValueHeader_IsEmpty for the new field")

	require.True(t, MVCCValueHeader{}.IsEmpty())

	// Assert that default values in the value header are equivalent to empty, so
	// we can omit them entirely from the encoding.
	require.True(t, defaultMVCCValueHeader().IsEmpty())

	require.False(t, allFieldsSet.IsEmpty())
	require.False(t, MVCCValueHeader{LocalTimestamp: allFieldsSet.LocalTimestamp}.IsEmpty())
	require.False(t, MVCCValueHeader{OmitInRangefeeds: allFieldsSet.OmitInRangefeeds}.IsEmpty())
	require.False(t, MVCCValueHeader{ImportEpoch: allFieldsSet.ImportEpoch}.IsEmpty())
	require.False(t, MVCCValueHeader{OriginID: allFieldsSet.OriginID}.IsEmpty())
	require.False(t, MVCCValueHeader{OriginTimestamp: allFieldsSet.OriginTimestamp}.IsEmpty())
}

func TestMVCCValueHeader_MarshalUnmarshal(t *testing.T) {
	// TODO: test this with random combinations of header fields set.
	vh := populatedMVCCValueHeader()
	b, err := protoutil.Marshal(&vh)
	require.NoError(t, err)
	var vh2 MVCCValueHeader
	require.NoError(t, protoutil.Unmarshal(b, &vh2))
	reflect.DeepEqual(vh, vh2)
}
