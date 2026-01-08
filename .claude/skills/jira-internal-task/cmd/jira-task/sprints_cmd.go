package main

import (
	"encoding/json"
	"os"

	"github.com/spf13/cobra"
)

// SprintListItem is the JSON output format for sprint listing.
type SprintListItem struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	State     string `json:"state"`
	StartDate string `json:"startDate"`
	EndDate   string `json:"endDate"`
}

var (
	sprintsToken   string
	sprintsEmail   string
	sprintsEngTeam string
	sprintsBoardID int
)

func newSprintsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sprints",
		Short: "List active and future sprints (JSON output)",
		RunE:  runSprints,
	}

	cmd.Flags().StringVar(&sprintsToken, "token", os.Getenv("JIRA_TOKEN"), "Jira API token (default: $JIRA_TOKEN)")
	cmd.Flags().StringVar(&sprintsEmail, "email", os.Getenv("JIRA_EMAIL"), "Jira account email (default: $JIRA_EMAIL)")
	cmd.Flags().StringVar(&sprintsEngTeam, "eng-team", os.Getenv("JIRA_ENG_TEAM"), "Engineering team (default: $JIRA_ENG_TEAM)")
	cmd.Flags().IntVar(&sprintsBoardID, "board-id", 0, "Sprint board ID (default: $JIRA_<ENG_TEAM>_BOARD_ID)")

	return cmd
}

func runSprints(cmd *cobra.Command, args []string) error {
	resolvedBoardID, err := resolveBoardID(sprintsEngTeam, sprintsBoardID)
	if err != nil {
		return err
	}

	config := JiraConfig{
		Token:   sprintsToken,
		Email:   sprintsEmail,
		Project: "CRDB",
		EngTeam: sprintsEngTeam,
		BoardID: resolvedBoardID,
	}
	client := &RealJiraClient{Config: config}

	sprints, err := client.fetchNonClosedSprints()
	if err != nil {
		return err
	}

	items := formatSprintList(sprints, resolvedBoardID)
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(items)
}

func formatSprintList(sprints []Sprint, boardID int) []SprintListItem {
	var items []SprintListItem
	for _, s := range sprints {
		if s.OriginBoardID != boardID {
			continue
		}
		items = append(items, SprintListItem{
			ID:        s.ID,
			Name:      s.Name,
			State:     s.State,
			StartDate: s.StartDate,
			EndDate:   s.EndDate,
		})
	}
	return items
}
