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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestValidateRecordForProtect(t *testing.T) {
	target := ptpb.MakeClusterTarget()
	for i, tc := range []struct {
		r   *ptpb.Record
		err error
	}{
		{
			r: &ptpb.Record{
				ID:        uuid.MakeV4().GetBytes(),
				Timestamp: hlc.Timestamp{WallTime: 1, Logical: 1},
				MetaType:  "job",
				Meta:      []byte("junk"),
				Target:    target,
			},
			err: nil,
		},
		{
			r: &ptpb.Record{
				Timestamp: hlc.Timestamp{WallTime: 1, Logical: 1},
				MetaType:  "job",
				Meta:      []byte("junk"),
				Target:    target,
			},
			err: errZeroID,
		},
		{
			r: &ptpb.Record{
				ID:       uuid.MakeV4().GetBytes(),
				MetaType: "job",
				Meta:     []byte("junk"),
				Target:   target,
			},
			err: errZeroTimestamp,
		},
		{
			r: &ptpb.Record{
				ID:        uuid.MakeV4().GetBytes(),
				Timestamp: hlc.Timestamp{WallTime: 1, Logical: 1},
				Meta:      []byte("junk"),
				Target:    target,
			},
			err: errInvalidMeta,
		},
		{
			r: &ptpb.Record{
				ID:        uuid.MakeV4().GetBytes(),
				Timestamp: hlc.Timestamp{WallTime: 1, Logical: 1},
				MetaType:  "job",
				Meta:      []byte("junk"),
			},
			err: errNilTarget,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require.Equal(t, validateRecordForProtect(tc.r), tc.err)
		})
	}
}
