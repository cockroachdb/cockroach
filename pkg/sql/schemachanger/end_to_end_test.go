// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachanger_test

import (
	"flag"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/corpus"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/sctest"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// Used for saving corpus information in TestGenerateCorpus
var corpusPath string

func init() {
	flag.StringVar(&corpusPath, "declarative-corpus", "", "Path to the corpus file")
}

func TestSchemaChangerSideEffects(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	sctest.EndToEndSideEffects(t, testutils.TestDataPath(t), sctest.SingleNodeCluster)
}

func TestRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sctest.Rollback(t, testutils.TestDataPath(t), sctest.SingleNodeCluster)
}

func TestPause(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sctest.Pause(t, testutils.TestDataPath(t), sctest.SingleNodeCluster)
}

// TestGenerateCorpus generates a corpus based on the end to end test files.
func TestGenerateCorpus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	if corpusPath == "" {
		skip.IgnoreLintf(t, "requires decalarative-corpus path parameter")
	}
	sctest.GenerateSchemaChangeCorpus(t, testutils.TestDataPath(t), corpusPath, sctest.SingleNodeCluster)
}

// TestValidateCorpuses validates that any generated corpus file on disk, a
// path needs to be specified.
func TestValidateCorpuses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	if corpusPath == "" {
		skip.IgnoreLintf(t, "requires decalarative-corpus path parameter")
	}
	t.Run("corpus-validation", func(t *testing.T) {
		cr, err := corpus.NewCorpusReader(corpusPath)
		require.NoError(t, err)
		require.NoError(t, cr.ReadCorpus())

		for idx := 0; idx < cr.GetNumEntries(); idx++ {
			entryName, state := cr.GetCorpus(idx)
			t.Run(entryName, func(t *testing.T) {
				jobID := jobspb.JobID(0)
				params := scplan.Params{
					InRollback:     state.InRollback,
					ExecutionPhase: scop.LatestPhase,
					SchemaChangerJobIDSupplier: func() jobspb.JobID {
						jobID++
						return jobID
					},
				}
				_, err := scplan.MakePlan(*state, params)
				require.NoError(t, err)
			})
		}
	})
}
