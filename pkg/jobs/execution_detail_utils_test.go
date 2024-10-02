// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestReadWriteListExecutionDetailFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()
	ctx := context.Background()
	params := base.TestServerArgs{}
	params.Knobs.JobsTestingKnobs = NewTestingKnobsWithShortIntervals()
	s := serverutils.StartServerOnly(t, params)
	defer s.Stopper().Stop(ctx)
	ts := s.ApplicationLayer()

	// Write a few files of varying lengths.
	filenames := []string{"file1", "file2", "file3"}
	filenameToData := make(map[string][]byte)
	db := ts.InternalDB().(isql.DB)
	msg := &jobspb.BackupDetails{
		URI: "gs://foo/bar",
	}
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		for _, filename := range filenames {
			// Generate random data of size between 15 bytes and 5 MiB.
			data := make([]byte, 15+rand.Intn(5*1024*1024-15))
			randutil.ReadTestdataBytes(rng, data)
			err := WriteExecutionDetailFile(ctx, filename, data, txn, jobspb.JobID(123))
			if err != nil {
				return err
			}
			filenameToData[filename] = data[:]
		}

		// Write a binpb format file.
		return WriteProtobinExecutionDetailFile(ctx, "testproto", msg, txn, jobspb.JobID(123))
	}))

	// List the files.
	listedFiles, err := ListExecutionDetailFiles(ctx, ts.InternalDB().(isql.DB), jobspb.JobID(123))
	require.NoError(t, err)
	require.ElementsMatch(t, append(filenames,
		"testproto~cockroach.sql.jobs.jobspb.BackupDetails.binpb.txt",
		"testproto~cockroach.sql.jobs.jobspb.BackupDetails.binpb"), listedFiles)

	// Read the files and verify that the content matches.
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		for _, filename := range listedFiles {
			if strings.HasSuffix(filename, "binpb.txt") {
				// Skip the text version of the binpb file.
				continue
			}
			readData, err := ReadExecutionDetailFile(ctx, filename, txn, jobspb.JobID(123))
			require.NoError(t, err)
			if strings.HasSuffix(filename, "binpb") {
				// For the binpb file, unmarshal the data and compare it to the original message.
				unmarshaledMsg := &jobspb.BackupDetails{}
				err = protoutil.Unmarshal(readData, unmarshaledMsg)
				require.NoError(t, err)
				require.Equal(t, msg, unmarshaledMsg)
			} else {
				require.Equal(t, filenameToData[filename], readData)
			}
		}
		return nil
	}))
}
