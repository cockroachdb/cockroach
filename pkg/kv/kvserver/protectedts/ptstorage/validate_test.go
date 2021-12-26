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
	"context"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestValidateRecordForProtect(t *testing.T) {
	target := ptpb.MakeRecordClusterTarget()
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
			require.Equal(t, validateRecordForProtect(context.Background(), tc.r, cluster.MakeTestingClusterSettings()), tc.err)
		})

		// TODO(adityamaru): Remove test when we delete `deprecatedSpans` field from
		// record.
		t.Run("errEmptySpans", func(t *testing.T) {
			r := &ptpb.Record{
				ID:        uuid.MakeV4().GetBytes(),
				Timestamp: hlc.Timestamp{WallTime: 1, Logical: 1},
				MetaType:  "job",
				Meta:      []byte("junk"),
				Target:    target,
			}
			versionBeforeAlterSystemProtectedTimestampAddColumn := roachpb.Version{Major: 21, Minor: 2, Internal: 34}
			require.Equal(t, validateRecordForProtect(context.Background(), r,
				cluster.MakeTestingClusterSettingsWithVersions(versionBeforeAlterSystemProtectedTimestampAddColumn,
					versionBeforeAlterSystemProtectedTimestampAddColumn, true)), errEmptySpans)
		})
	}
}
