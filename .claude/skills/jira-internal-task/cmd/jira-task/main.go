// jira-task manages Jira internal tasks and sprints.
//
// NOTE FOR AI AGENTS: Never create test issues to verify this tool works.
// Only create issues when the user explicitly requests a real task.
// You MAY run the "sprints" subcommand for sanity checks since it has no side effects.
package main

import (
	"os"

	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "jira-task",
		Short: "Manage Jira internal tasks and sprints",
	}

	rootCmd.AddCommand(newCreateCmd())
	rootCmd.AddCommand(newSprintsCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
