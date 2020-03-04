// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ptstorage

import (
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestValidateRecordForProtect(t *testing.T) {
	spans := []roachpb.Span{
		{
			Key:    roachpb.Key("a"),
			EndKey: roachpb.Key("b"),
		},
	}
	for i, tc := range []struct {
		r   *ptpb.Record
		err error
	}{
		{
			r: &ptpb.Record{
				ID:        uuid.MakeV4(),
				Timestamp: hlc.Timestamp{WallTime: 1, Logical: 1},
				MetaType:  "job",
				Meta:      []byte("junk"),
				Spans:     spans,
			},
			err: nil,
		},
		{
			r: &ptpb.Record{
				Timestamp: hlc.Timestamp{WallTime: 1, Logical: 1},
				MetaType:  "job",
				Meta:      []byte("junk"),
				Spans:     spans,
			},
			err: errZeroID,
		},
		{
			r: &ptpb.Record{
				ID:       uuid.MakeV4(),
				MetaType: "job",
				Meta:     []byte("junk"),
				Spans:    spans,
			},
			err: errZeroTimestamp,
		},
		{
			r: &ptpb.Record{
				ID:        uuid.MakeV4(),
				Timestamp: hlc.Timestamp{WallTime: 1, Logical: 1},
				Meta:      []byte("junk"),
				Spans:     spans,
			},
			err: errInvalidMeta,
		},
		{
			r: &ptpb.Record{
				ID:        uuid.MakeV4(),
				Timestamp: hlc.Timestamp{WallTime: 1, Logical: 1},
				MetaType:  "job",
				Meta:      []byte("junk"),
			},
			err: errEmptySpans,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require.Equal(t, validateRecordForProtect(tc.r), tc.err)
		})
	}
}
