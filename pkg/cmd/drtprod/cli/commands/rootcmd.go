// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package commands

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV1"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/spf13/cobra"
)

var (
	cmdStartTime  time.Time
	datadogSite   = os.Getenv("DD_SITE")
	datadogAPIKey = os.Getenv("DD_API_KEY")
)

func publishEventToDatadog(cmd *cobra.Command, args []string, cmdPreRun bool) {
	if len(args) == 0 {
		// This command is not targeting a cluster.
		return
	}

	if len(datadogSite) == 0 {
		datadogSite = "us5.datadoghq.com"
	}
	// If datadog credentials are not configured return from here.
	if len(datadogAPIKey) == 0 {
		return
	}

	clusterName := args[0]
	title := fmt.Sprintf("%s %s", cmd.CommandPath(), strings.Join(args[1:], " "))
	text := fmt.Sprintf("cmd: %s", strings.Join(args, " "))
	endTime := timeutil.Now()

	if cmdPreRun {
		title = "[STARTED] " + title
	} else {
		title = "[FINISHED] " + title
		text = fmt.Sprintf("%s\n\nstartTime: %s\n\nendTime: %s", text, cmdStartTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
		dataDogDashboard := fmt.Sprintf("https://us5.datadoghq.com/dashboard/pbe-ic2-3qt/drt?tpl_var_cluster=%s&from_ts=%d&to_ts=%d",
			clusterName, cmdStartTime.UnixMilli(), endTime.UnixMilli())
		text += fmt.Sprintf("\n\ndatadogUrl: %s", dataDogDashboard)
	}
	title = roachprod.TruncateString(title, 90)
	tags := fmt.Sprintf("env:development,cluster:%s,team:drt,service:drt-cockroachdb", clusterName)

	ctx := context.Background()
	ctx = workload.NewDatadogContext(ctx, datadogSite, datadogAPIKey)
	workload.EmitDatadogEvent(ctx, title, text, datadogV1.EVENTALERTTYPE_INFO, tags)
}

// GetRootCommand returns the root command
func GetRootCommand(_ context.Context) *cobra.Command {
	return &cobra.Command{
		Use:   "drtprod [command] (flags)",
		Short: "drtprod runs roachprod commands against DRT clusters",
		Long: `drtprod is a tool for manipulating drt clusters using roachprod,
allowing easy creating, destruction, controls and configurations of clusters.

Commands include:
  execute: executes the commands as per the YAML configuration. Refer to pkg/cmd/drtprod/configs/drt_test.yaml as an example
  *: any other command is passed to roachprod, potentially with flags added
`,
		Version: "details:\n" + build.GetInfo().Long(),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			cmdStartTime = timeutil.Now()
			publishEventToDatadog(cmd, args, true)
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			publishEventToDatadog(cmd, args, false)
		},
	}
}
