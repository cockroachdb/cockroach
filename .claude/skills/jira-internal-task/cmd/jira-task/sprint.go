package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Sprint represents a Jira sprint from the Agile API.
type Sprint struct {
	ID            int    `json:"id"`
	Name          string `json:"name"`
	State         string `json:"state"`
	StartDate     string `json:"startDate"`
	EndDate       string `json:"endDate"`
	OriginBoardID int    `json:"originBoardId"`
}

// SprintResponse is the response from the sprint list API.
type SprintResponse struct {
	Values []Sprint `json:"values"`
}

// SprintInfo contains the resolved sprint ID and name.
type SprintInfo struct {
	ID   int
	Name string
}

// resolveSprintID resolves a sprint flag value to a sprint ID and name.
// If the value is already numeric, it's returned as-is.
// If it's "latest", the active sprint with the largest end time is chosen.
// Otherwise, it's treated as a search string that must match exactly one sprint.
func (c *RealJiraClient) resolveSprintID(sprintFlag string) (SprintInfo, error) {
	if sprintFlag == "" {
		return SprintInfo{}, nil
	}

	// If it's already numeric, use it directly
	if id, err := strconv.Atoi(sprintFlag); err == nil {
		return SprintInfo{ID: id, Name: fmt.Sprintf("(ID %d)", id)}, nil
	}

	// Fetch non-closed sprints (active + future)
	sprints, err := c.fetchNonClosedSprints()
	if err != nil {
		return SprintInfo{}, err
	}

	// Filter to only sprints from this board
	var boardSprints []Sprint
	for _, s := range sprints {
		if s.OriginBoardID == c.Config.BoardID {
			boardSprints = append(boardSprints, s)
		}
	}

	if sprintFlag == "latest" {
		return findLatestActiveSprint(boardSprints)
	}

	// Search for a sprint matching the string
	return findSprintByName(boardSprints, sprintFlag)
}

// fetchNonClosedSprints fetches active and future sprints (excludes closed).
func (c *RealJiraClient) fetchNonClosedSprints() ([]Sprint, error) {
	url := fmt.Sprintf("%s/board/%d/sprint?state=active,future", jiraAgileURL, c.Config.BoardID)
	body, err := c.doRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch sprints: %w", err)
	}

	var result SprintResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse sprints: %w", err)
	}

	return result.Values, nil
}

// findLatestActiveSprint finds the active sprint with the largest end time.
// It filters the input sprints for state=="active" and returns the one with the latest end date.
func findLatestActiveSprint(sprints []Sprint) (SprintInfo, error) {
	var latest Sprint
	var latestEnd time.Time

	for _, s := range sprints {
		if s.State != "active" {
			continue
		}
		endTime, err := time.Parse(time.RFC3339, s.EndDate)
		if err != nil {
			continue // skip sprints with unparseable dates
		}
		if endTime.After(latestEnd) {
			latestEnd = endTime
			latest = s
		}
	}

	if latest.ID == 0 {
		return SprintInfo{}, fmt.Errorf("no active sprints found")
	}

	return SprintInfo{ID: latest.ID, Name: latest.Name}, nil
}

// findSprintByName finds a sprint whose name contains the search string.
// Returns an error if zero or multiple sprints match.
func findSprintByName(sprints []Sprint, search string) (SprintInfo, error) {
	var matches []Sprint
	for _, s := range sprints {
		if strings.Contains(s.Name, search) {
			matches = append(matches, s)
		}
	}

	if len(matches) == 0 {
		var names []string
		for _, s := range sprints {
			names = append(names, s.Name)
		}
		return SprintInfo{}, fmt.Errorf("no sprint matching %q found; available: %v", search, names)
	}

	if len(matches) > 1 {
		var names []string
		for _, s := range matches {
			names = append(names, s.Name)
		}
		return SprintInfo{}, fmt.Errorf("multiple sprints match %q: %v", search, names)
	}

	return SprintInfo{ID: matches[0].ID, Name: matches[0].Name}, nil
}
