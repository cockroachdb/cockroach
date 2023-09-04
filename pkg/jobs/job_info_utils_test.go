// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestReadWriteChunkedFileToJobInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()
	ctx := context.Background()
	params := base.TestServerArgs{}
	params.Knobs.JobsTestingKnobs = NewTestingKnobsWithShortIntervals()
	s := serverutils.StartServerOnly(t, params)
	defer s.Stopper().Stop(ctx)

	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "empty file",
			data: []byte{},
		},
		{
			name: "file less than 1MiB",
			data: make([]byte, 1<<20-1), // 1 MiB - 1 byte
		},
		{
			name: "file equal to 1MiB",
			data: make([]byte, 1<<20), // 1 MiB
		},
		{
			name: "file greater than 1MiB",
			data: make([]byte, 1<<20+1), // 1 MiB + 1 byte
		},
	}

	db := s.InternalDB().(isql.DB)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.data) > 0 {
				randutil.ReadTestdataBytes(rng, tt.data)
			}
			require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				err := WriteChunkedFileToJobInfo(ctx, tt.name, tt.data, txn, jobspb.JobID(123))
				if err != nil {
					return err
				}
				got, err := ReadChunkedFileToJobInfo(ctx, tt.name, txn, jobspb.JobID(123))
				if err != nil {
					return err
				}
				require.Equal(t, tt.data, got)
				return nil
			}))
		})
	}
}
