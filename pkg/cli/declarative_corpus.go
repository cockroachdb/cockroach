// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/corpus"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/spf13/cobra"
)

var declarativeValidateCorpus = &cobra.Command{
	Use:   "declarative-corpus-validate <filename>",
	Short: "validates a corpus file for the declarative schema changer",
	Long: `
Validates very single declarative schema changer state can be planned against in
a given corpus file.
`,
	Args: cobra.ExactArgs(1),
	RunE: clierrorplus.MaybeDecorateError(
		func(cmd *cobra.Command, args []string) (resErr error) {
			var firstError error
			cr, err := corpus.NewCorpusReaderWithPath(args[0])
			if err != nil {
				panic(err)
			}
			err = cr.ReadCorpus()
			if err != nil {
				panic(err)
			}
			for idx := 0; idx < cr.GetNumEntries(); idx++ {
				name, state := cr.GetCorpus(idx)
				scpb.MigrateCurrentState(clusterversion.TestingClusterVersion, state)
				jobID := jobspb.JobID(0)
				params := scplan.Params{
					ActiveVersion:  clusterversion.TestingClusterVersion,
					InRollback:     state.InRollback,
					ExecutionPhase: scop.LatestPhase,
					SchemaChangerJobIDSupplier: func() jobspb.JobID {
						jobID++
						return jobID
					},
				}
				_, err := scplan.MakePlan(cmd.Context(), *state, params)
				if err != nil {
					fmt.Printf("failed to validate %s with error %v\n", name, err)
					if firstError == nil {
						firstError = err
					}
				} else {
					fmt.Printf("validated %s\n", name)
				}
			}
			return firstError
		}),
}
