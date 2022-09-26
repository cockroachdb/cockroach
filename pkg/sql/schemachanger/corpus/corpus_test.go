// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package corpus_test

import (
	"flag"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/corpus"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/stretchr/testify/require"
)

// Used for reading corpus information in TestValidateCorpuses
var corpusPath string

func init() {
	flag.StringVar(&corpusPath, "declarative-corpus", "", "path to the corpus file")
}

// TestValidateCorpuses validates that any generated corpus file on disk, a
// path needs to be specified.
func TestValidateCorpuses(t *testing.T) {
	if corpusPath == "" {
		skip.IgnoreLintf(t, "requires declarative-corpus path parameter")
	}
	reader, err := corpus.NewCorpusReader(corpusPath)
	require.NoError(t, err)
	require.NoError(t, reader.ReadCorpus())
	for corpusIdx := 0; corpusIdx < reader.GetNumEntries(); corpusIdx++ {
		jobID := jobspb.InvalidJobID
		name, state := reader.GetCorpus(corpusIdx)
		t.Run(name, func(t *testing.T) {
			_, err := scplan.MakePlan(*state, scplan.Params{
				ExecutionPhase: scop.LatestPhase,
				InRollback:     state.InRollback,
				SchemaChangerJobIDSupplier: func() jobspb.JobID {
					jobID++
					return jobID
				}})
			require.NoError(t, err)
		})
	}
}
