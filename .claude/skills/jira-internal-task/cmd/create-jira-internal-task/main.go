// create-jira-internal-task creates Internal Task issues in Jira.
//
// NOTE FOR AI AGENTS: Never create test issues to verify this tool works.
// Only create issues when the user explicitly requests a real task.
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	// Auth/config flags
	token := flag.String("token", os.Getenv("JIRA_TOKEN"), "Jira API token (default: $JIRA_TOKEN)")
	email := flag.String("email", os.Getenv("JIRA_EMAIL"), "Jira account email (default: $JIRA_EMAIL)")
	project := flag.String("project", "CRDB", "Jira project key (default: CRDB)")
	engTeam := flag.String("eng-team", os.Getenv("JIRA_ENG_TEAM"), "Engineering team (default: $JIRA_ENG_TEAM)")
	boardID := flag.Int("board-id", 0, "Sprint board ID (default: $JIRA_<ENG_TEAM>_BOARD_ID, -1 to skip). "+
		"List boards with: curl -u \"$JIRA_EMAIL:$JIRA_TOKEN\" "+
		"\"https://cockroachlabs.atlassian.net/rest/agile/1.0/board/<id>/sprint?state=active\"")

	// Task content flags
	title := flag.String("title", "", "Task title (required)")
	description := flag.String("description", "", "Task description (required)")
	parent := flag.String("parent", "", "Parent issue key, e.g., CRDB-12345 (optional)")
	sprint := flag.String("sprint", "latest", "Sprint: numeric ID, name substring (e.g. \"26.1\"), \"latest\", or \"\" to skip")
	assignee := flag.String("assignee", os.Getenv("JIRA_EMAIL"), "Assignee email (default: $JIRA_EMAIL)")

	flag.Parse()

	// Validate eng-team
	if *engTeam == "" {
		fmt.Fprintf(os.Stderr, "Error: eng-team is required (set --eng-team or $JIRA_ENG_TEAM)\n")
		os.Exit(1)
	}

	// Resolve board ID: -1 means skip, explicit flag > $JIRA_<ENG_TEAM>_BOARD_ID
	resolvedBoardID := *boardID
	if resolvedBoardID == 0 {
		envVar := "JIRA_" + strings.ToUpper(*engTeam) + "_BOARD_ID"
		if v := os.Getenv(envVar); v != "" {
			var err error
			resolvedBoardID, err = strconv.Atoi(v)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: invalid %s value %q: %v\n", envVar, v, err)
				os.Exit(1)
			}
		} else {
			fmt.Fprintf(os.Stderr, "Error: board-id is required (set --board-id or $%s, or -1 to skip)\n", envVar)
			os.Exit(1)
		}
	}
	skipSprint := resolvedBoardID < 0

	// Build config and client
	config := JiraConfig{
		Token:   *token,
		Email:   *email,
		Project: *project,
		EngTeam: *engTeam,
		BoardID: resolvedBoardID,
	}
	client := &RealJiraClient{Config: config}

	// Resolve sprint (skip if board-id < 0)
	var sprintID, sprintName string
	if !skipSprint && *sprint != "" {
		info, err := client.resolveSprintID(*sprint)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		sprintID = strconv.Itoa(info.ID)
		sprintName = info.Name
	}

	// Resolve assignee email to account ID and display name
	var assigneeID, assigneeName string
	if *assignee != "" {
		user, err := client.resolveUser(*assignee)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		assigneeID = user.AccountID
		assigneeName = user.DisplayName
	}

	// Build task parameters
	params := TaskParams{
		Title:        *title,
		Description:  *description,
		Parent:       *parent,
		Sprint:       sprintID,
		SprintName:   sprintName,
		Assignee:     assigneeID,
		AssigneeName: assigneeName,
	}
	issueKey, err := run(config, params, client)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Print summary
	fmt.Printf("\nCreated: %s\n", issueKey)
	fmt.Printf("  URL:      https://cockroachlabs.atlassian.net/browse/%s\n", issueKey)
	fmt.Printf("  Assignee: %s\n", params.AssigneeName)
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
}
