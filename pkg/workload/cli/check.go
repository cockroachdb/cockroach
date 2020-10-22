// Copyright 2018 The Cockroach Authors.
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
	gosql "database/sql"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func init() {
	AddSubCmd(func(userFacing bool) *cobra.Command {
		var checkCmd = SetCmdDefaults(&cobra.Command{
			Use:   `check`,
			Short: `check a running cluster's data for consistency`,
		})
		for _, meta := range workload.Registered() {
			gen := meta.New()
			if hooks, ok := gen.(workload.Hookser); !ok || hooks.Hooks().CheckConsistency == nil {
				continue
			}

			var genFlags *pflag.FlagSet
			if f, ok := gen.(workload.Flagser); ok {
				genFlags = f.Flags().FlagSet
				// Hide irrelevant flags so they don't clutter up the help text, but
				// don't remove them entirely so if someone switches from
				// `./workload run` to `./workload check` they don't have to remove
				// them from the invocation.
				for flagName, meta := range f.Flags().Meta {
					if meta.RuntimeOnly && !meta.CheckConsistencyOnly {
						_ = genFlags.MarkHidden(flagName)
					}
				}
			}

			genCheckCmd := SetCmdDefaults(&cobra.Command{
				Use:  meta.Name + ` [CRDB URI]`,
				Args: cobra.RangeArgs(0, 1),
			})
			genCheckCmd.Flags().AddFlagSet(genFlags)
			genCheckCmd.Run = CmdHelper(gen, check)
			checkCmd.AddCommand(genCheckCmd)
		}
		return checkCmd
	})
}

func check(gen workload.Generator, urls []string, dbName string) error {
	ctx := context.Background()

	var fn func(context.Context, *gosql.DB) error
	if hooks, ok := gen.(workload.Hookser); ok {
		fn = hooks.Hooks().CheckConsistency
	}
	if fn == nil {
		return errors.Errorf(`no consistency checks are defined for %s`, gen.Meta().Name)
	}

	sqlDB, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return err
	}
	defer sqlDB.Close()
	if err := sqlDB.Ping(); err != nil {
		return err
	}
	return fn(ctx, sqlDB)
}
