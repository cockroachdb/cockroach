// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV1"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var checkFlags = pflag.NewFlagSet("check", pflag.ContinueOnError)
var datadogSite = checkFlags.String("datadog-site", "us5.datadoghq.com",
	"Datadog site to communicate with (e.g., us5.datadoghq.com).")
var datadogAPIKey = checkFlags.String("datadog-api-key", "",
	"Datadog API key to emit telemetry data to Datadog.")
var datadogTags = checkFlags.String("datadog-tags", "",
	"A comma-separated list of tags to attach to telemetry data (e.g., key1:val1,key2:val2).")

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
			genCheckCmd.Flags().AddFlagSet(checkFlags)
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
	err = fn(ctx, sqlDB)
	if err != nil {
		// For automated operations running the consistency checker like the DRT team,
		// there is a need to send an event to Datadog so that a Slack alert can be
		// configured. Here, we are attempting to emit an error event to Datadog.
		datadogContext := workload.NewDatadogContext(ctx, *datadogSite, *datadogAPIKey)
		title := fmt.Sprintf("Consistency check failed for %s", gen.Meta().Name)
		text := fmt.Sprintf("%v", err)
		workload.EmitDatadogEvent(datadogContext, title, text, datadogV1.EVENTALERTTYPE_ERROR, *datadogTags)
	}
	return err
}
