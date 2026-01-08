package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"
)

var (
	createToken       string
	createEmail       string
	createProject     string
	createEngTeam     string
	createBoardID     int
	createTitle       string
	createDescription string
	createParent      string
	createSprint      string
	createAssignee    string
)

func newCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a Jira internal task",
		RunE:  runCreate,
	}

	// Auth/config flags
	cmd.Flags().StringVar(&createToken, "token", os.Getenv("JIRA_TOKEN"), "Jira API token (default: $JIRA_TOKEN)")
	cmd.Flags().StringVar(&createEmail, "email", os.Getenv("JIRA_EMAIL"), "Jira account email (default: $JIRA_EMAIL)")
	cmd.Flags().StringVar(&createProject, "project", "CRDB", "Jira project key")
	cmd.Flags().StringVar(&createEngTeam, "eng-team", os.Getenv("JIRA_ENG_TEAM"), "Engineering team (default: $JIRA_ENG_TEAM)")
	cmd.Flags().IntVar(&createBoardID, "board-id", 0, "Sprint board ID (default: $JIRA_<ENG_TEAM>_BOARD_ID, -1 to skip)")

	// Task content flags
	cmd.Flags().StringVar(&createTitle, "title", "", "Task title (required)")
	cmd.Flags().StringVar(&createDescription, "description", "", "Task description (required)")
	cmd.Flags().StringVar(&createParent, "parent", "", "Parent issue key, e.g., CRDB-12345 (optional)")
	cmd.Flags().StringVar(&createSprint, "sprint", "", "Sprint: numeric ID, \"latest\", or -1 to skip (required)")
	cmd.Flags().StringVar(&createAssignee, "assignee", "", "Assignee email, or \"unassigned\" to skip (required)")

	_ = cmd.MarkFlagRequired("title")
	_ = cmd.MarkFlagRequired("description")
	_ = cmd.MarkFlagRequired("sprint")
	_ = cmd.MarkFlagRequired("assignee")

	return cmd
}

func runCreate(cmd *cobra.Command, args []string) error {
	resolvedBoardID, err := resolveBoardID(createEngTeam, createBoardID)
	if err != nil {
		return err
	}

	// Build config and client
	config := JiraConfig{
		Token:   createToken,
		Email:   createEmail,
		Project: createProject,
		EngTeam: createEngTeam,
		BoardID: resolvedBoardID,
	}
	client := &RealJiraClient{Config: config}

	// Resolve sprint
	var sprintID, sprintName string
	if createSprint != "-1" && createSprint != "" {
		info, err := client.resolveSprintID(createSprint)
		if err != nil {
			return err
		}
		sprintID = strconv.Itoa(info.ID)
		sprintName = info.Name
	}

	// Resolve assignee email to account ID and display name.
	// Special value "unassigned" skips setting the assignee.
	var assigneeID, assigneeName string
	if createAssignee != "" && createAssignee != "unassigned" {
		user, err := client.resolveUser(createAssignee)
		if err != nil {
			return err
		}
		assigneeID = user.AccountID
		assigneeName = user.DisplayName
	}

	// Build task parameters
	params := TaskParams{
		Title:        createTitle,
		Description:  createDescription,
		Parent:       createParent,
		Sprint:       sprintID,
		SprintName:   sprintName,
		Assignee:     assigneeID,
		AssigneeName: assigneeName,
	}
	issueKey, err := run(config, params, client)
	if err != nil {
		return err
	}

	// Print summary
	fmt.Printf("\nCreated: %s\n", issueKey)
	fmt.Printf("  URL:      https://cockroachlabs.atlassian.net/browse/%s\n", issueKey)
	if params.AssigneeName != "" {
		fmt.Printf("  Assignee: %s\n", params.AssigneeName)
	} else {
		fmt.Printf("  Assignee: Unassigned\n")
	}
	if params.SprintName != "" {
		fmt.Printf("  Sprint:   %s\n", params.SprintName)
	}
	if params.Parent != "" {
		fmt.Printf("  Parent:   %s\n", params.Parent)
	}

	// Verify the issue was created correctly
	if warnings := client.VerifyIssue(issueKey, params); len(warnings) > 0 {
		fmt.Println("\n⚠️  Warnings:")
		for _, w := range warnings {
			fmt.Printf("  - %s\n", w)
		}
	}

	return nil
}
