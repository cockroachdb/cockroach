// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	gosql "database/sql"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/pkg/workload"
)

var checkCmd = &cobra.Command{
	Use:   `check`,
	Short: `Check a running cluster's data for consistency`,
}

func init() {
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

		genCheckCmd := &cobra.Command{
			Use:  meta.Name + ` [CRDB URI]`,
			Args: cobra.RangeArgs(0, 1),
		}
		genCheckCmd.Flags().AddFlagSet(genFlags)
		genCheckCmd.Run = cmdHelper(gen, check)
		checkCmd.AddCommand(genCheckCmd)
	}
	rootCmd.AddCommand(checkCmd)
}

func check(gen workload.Generator, urls []string, dbName string) error {
	ctx := context.Background()

	var fn func(context.Context, *gosql.DB) error
	if hooks, ok := gen.(workload.Hookser); ok {
		fn = hooks.Hooks().CheckConsistency
	}
	if fn == nil {
		return errors.Errorf(`no consistency checks are defined for %s` + gen.Meta().Name)
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
