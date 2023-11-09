// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var declarativePrintRules = &cobra.Command{
	Use:   "declarative-print-rules <version> <dep|op>",
	Short: "validates a corpus file for the declarative schema changer",
	Long: `
Validates very single declarative schema changer state can be planned against in
a given corpus file.
`,
	Args: cobra.ExactArgs(2),
	RunE: clierrorplus.MaybeDecorateError(
		func(cmd *cobra.Command, args []string) (resErr error) {
			ctx := context.Background()
			version, err := roachpb.ParseVersion(args[0])
			if err != nil {
				return err
			}
			rules := scplan.GetRulesRegistryForRelease(ctx,
				clusterversion.ClusterVersion{
					Version: version,
				})
			if rules == nil {
				fmt.Printf("unsupported version number, the supported versions are: \n")
				for _, v := range scplan.GetReleasesForRulesRegistries() {
					fmt.Printf(" %s\n", v)
				}
				return nil
			}
			switch args[1] {
			case "dep":
				depRules, err := rules.MarshalDepRules()
				if err != nil {
					return err
				}
				fmt.Printf("deprules\n----\n%s", depRules)
			default:
				return errors.AssertionFailedf("unknown rule type: %s", args[1])
			}

			return nil
		}),
}
