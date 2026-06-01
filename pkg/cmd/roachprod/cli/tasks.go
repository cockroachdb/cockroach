// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/client"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// buildTaskCmd creates the parent "task" command.
func (cr *commandRegistry) buildTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "task",
		Short: "manage background tasks",
		Long:  `Commands for listing and inspecting background tasks.`,
	}
	cmd.AddCommand(
		cr.buildTaskListCmd(),
		cr.buildTaskLogsCmd(),
	)
	return cmd
}

// buildTaskListCmd lists tasks with optional filters.
func (cr *commandRegistry) buildTaskListCmd() *cobra.Command {
	var (
		stateFlag  string
		typeFlag   string
		outputFlag string
	)

	cmd := &cobra.Command{
		Use:   "list",
		Short: "list tasks",
		Args:  cobra.NoArgs,
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			tasks, err := c.ListTasks(context.Background(), l,
				client.ListTasksOptions{
					Type:  typeFlag,
					State: stateFlag,
				},
			)
			if err != nil {
				return errors.Wrap(err, "list tasks")
			}

			if outputFlag == "json" {
				return printJSON(tasks)
			}

			if len(tasks) == 0 {
				fmt.Println("No tasks found.")
				return nil
			}

			tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
			fmt.Fprintf(tw, "ID\tTYPE\tSTATE\tREFERENCE\tCREATED\n")
			for _, t := range tasks {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
					t.ID, t.Type, t.State, t.Reference, t.CreationDatetime,
				)
			}
			return tw.Flush()
		}),
	}

	cmd.Flags().StringVar(&stateFlag, "state", "", "filter by state (pending, running, done, failed)")
	cmd.Flags().StringVar(&typeFlag, "type", "", "filter by task type")
	cmd.Flags().StringVarP(&outputFlag, "output", "o", "text", "output format (text, json)")
	_ = cmd.RegisterFlagCompletionFunc("state", func(
		cmd *cobra.Command, args []string, toComplete string,
	) ([]string, cobra.ShellCompDirective) {
		return []string{"pending", "running", "done", "failed"},
			cobra.ShellCompDirectiveNoFileComp
	})
	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildTaskLogsCmd streams logs for a task.
func (cr *commandRegistry) buildTaskLogsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logs <task-id>",
		Short: "stream task logs",
		Args:  cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}
			_ = l

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			return streamSSELogs(ctx, c, args[0], 0)
		}),
	}

	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}
