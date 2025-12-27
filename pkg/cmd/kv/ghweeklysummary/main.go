// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// ghweeklysummary searches GitHub for PRs from specified authors and generates
// weekly summaries with 3 bullet points per week, including PR links.
//
// Usage:
//
//	ghweeklysummary --author wenyihu6 --weeks 4
//
// Or specify a date range:
//
//	ghweeklysummary --author wenyihu6 --after 2025-01-01 --before 2025-02-01
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
)

var (
	author = flag.String("author", "", "GitHub username (required)")
	repo   = flag.String("repo", "cockroachdb/cockroach", "Repository")
	weeks  = flag.Int("weeks", 4, "Number of weeks to look back (ignored if --after is set)")
	after  = flag.String("after", "", "Start date (YYYY-MM-DD)")
	before = flag.String("before", "", "End date (YYYY-MM-DD), defaults to today")
	limit  = flag.Int("limit", 200, "Maximum number of PRs to fetch")
)

type PRInfo struct {
	Number   int
	Title    string
	URL      string
	MergedAt time.Time
	State    string
}

type WeekSummary struct {
	WeekStart time.Time
	WeekEnd   time.Time
	PRs       []PRInfo
}

func main() {
	flag.Parse()

	if *author == "" {
		fmt.Fprintln(os.Stderr, "error: --author is required")
		flag.Usage()
		os.Exit(1)
	}

	// Parse date range.
	var startDate, endDate time.Time
	var err error

	if *before != "" {
		endDate, err = time.Parse("2006-01-02", *before)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: invalid --before date: %v\n", err)
			os.Exit(1)
		}
	} else {
		endDate = time.Now()
	}

	if *after != "" {
		startDate, err = time.Parse("2006-01-02", *after)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: invalid --after date: %v\n", err)
			os.Exit(1)
		}
	} else {
		startDate = endDate.AddDate(0, 0, -7**weeks)
	}

	fmt.Fprintf(os.Stderr, "Fetching PRs for %s from %s to %s\n",
		*author, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"))

	// Search for PRs.
	prs, err := fetchPRs(*repo, *author, startDate, endDate, *limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error fetching PRs: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "Found %d PRs\n", len(prs))

	// Group by week.
	weekSummaries := groupByWeek(prs, startDate, endDate)

	// Output summary as 3 bullet points with inline PR links.
	fmt.Println()
	fmt.Printf("## Summary (%s - %s)\n\n", startDate.Format("Jan 2"), endDate.Format("Jan 2, 2006"))

	// Group PRs by theme and generate narrative bullets.
	bullets := generateNarrativeBullets(prs)
	for _, bullet := range bullets {
		fmt.Printf("- %s\n\n", bullet)
	}

	// Weekly breakdown.
	fmt.Println("---")
	fmt.Println()
	fmt.Println("## Weekly Breakdown")
	fmt.Println()

	for _, week := range weekSummaries {
		fmt.Printf("**Week of %s:**\n", week.WeekStart.Format("Jan 2"))
		if len(week.PRs) == 0 {
			fmt.Println("_No PRs_")
			fmt.Println()
			continue
		}
		for _, pr := range week.PRs {
			fmt.Printf("- %s ([#%d](%s))\n", pr.Title, pr.Number, pr.URL)
		}
		fmt.Println()
	}
}

// generateNarrativeBullets creates 3 narrative bullet points summarizing the work.
func generateNarrativeBullets(prs []PRInfo) []string {
	// Group PRs by theme/area.
	groups := make(map[string][]PRInfo)
	for _, pr := range prs {
		prefix := extractPrefix(pr.Title)
		if prefix == "" {
			prefix = "other"
		}
		// Normalize similar prefixes.
		prefix = normalizePrefix(prefix)
		groups[prefix] = append(groups[prefix], pr)
	}

	// Sort groups by size.
	type group struct {
		name string
		prs  []PRInfo
	}
	var sorted []group
	for name, prList := range groups {
		sorted = append(sorted, group{name, prList})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return len(sorted[i].prs) > len(sorted[j].prs)
	})

	// Generate up to 3 narrative bullets.
	var bullets []string
	for i := 0; i < len(sorted) && len(bullets) < 3; i++ {
		g := sorted[i]
		bullet := generateNarrativeBullet(g.name, g.prs)
		if bullet != "" {
			bullets = append(bullets, bullet)
		}
	}

	return bullets
}

// normalizePrefix groups similar package prefixes together.
func normalizePrefix(prefix string) string {
	// Group MMA-related work together.
	if strings.Contains(prefix, "mma") || strings.Contains(prefix, "allocator") {
		return "mma"
	}
	return prefix
}

// generateNarrativeBullet creates a single narrative bullet for a group of PRs.
func generateNarrativeBullet(area string, prs []PRInfo) string {
	if len(prs) == 0 {
		return ""
	}

	// Build narrative with inline PR links.
	var parts []string

	// Group PRs by action type.
	var added, fixed, improved, refactored, other []PRInfo
	for _, pr := range prs {
		titleLower := strings.ToLower(pr.Title)
		switch {
		case strings.Contains(titleLower, "add") || strings.Contains(titleLower, "implement") || strings.Contains(titleLower, "introduce") || strings.Contains(titleLower, "support"):
			added = append(added, pr)
		case strings.Contains(titleLower, "fix") || strings.Contains(titleLower, "bug") || strings.Contains(titleLower, "resolve"):
			fixed = append(fixed, pr)
		case strings.Contains(titleLower, "improve") || strings.Contains(titleLower, "optimize") || strings.Contains(titleLower, "enhance"):
			improved = append(improved, pr)
		case strings.Contains(titleLower, "refactor") || strings.Contains(titleLower, "clean") || strings.Contains(titleLower, "simplif") || strings.Contains(titleLower, "move"):
			refactored = append(refactored, pr)
		default:
			other = append(other, pr)
		}
	}

	// Build narrative parts. First part gets capitalized action.
	if len(added) > 0 {
		action := "added"
		if len(parts) == 0 {
			action = "Added"
		}
		parts = append(parts, formatPRGroup(action, added))
	}
	if len(fixed) > 0 {
		action := "fixed"
		if len(parts) == 0 {
			action = "Fixed"
		}
		parts = append(parts, formatPRGroup(action, fixed))
	}
	if len(improved) > 0 {
		action := "improved"
		if len(parts) == 0 {
			action = "Improved"
		}
		parts = append(parts, formatPRGroup(action, improved))
	}
	if len(refactored) > 0 {
		action := "refactored"
		if len(parts) == 0 {
			action = "Refactored"
		}
		parts = append(parts, formatPRGroup(action, refactored))
	}
	if len(other) > 0 && len(parts) == 0 {
		parts = append(parts, formatPRGroup("Worked on", other))
	}

	if len(parts) == 0 {
		return ""
	}

	// Join with commas and "and".
	narrative := strings.Join(parts, ", ")
	if idx := strings.LastIndex(narrative, ", "); idx != -1 && strings.Count(narrative, ", ") > 0 {
		narrative = narrative[:idx] + " and" + narrative[idx+1:]
	}

	return narrative
}

// formatPRGroup formats a group of PRs with an action verb.
func formatPRGroup(action string, prs []PRInfo) string {
	if len(prs) == 1 {
		return fmt.Sprintf("%s %s ([#%d](%s))", action, stripPrefix(prs[0].Title), prs[0].Number, prs[0].URL)
	}

	// Multiple PRs - summarize with all links.
	var links []string
	var descriptions []string
	for _, pr := range prs {
		links = append(links, fmt.Sprintf("[#%d](%s)", pr.Number, pr.URL))
		descriptions = append(descriptions, stripPrefix(pr.Title))
	}

	// Use first description as representative, list all PR numbers.
	linkStr := strings.Join(links, ", ")
	return fmt.Sprintf("%s %s (%s)", action, descriptions[0], linkStr)
}

// extractPrefix extracts the package prefix from a PR title.
func extractPrefix(title string) string {
	idx := strings.Index(title, ":")
	if idx == -1 || idx > 40 {
		return ""
	}
	prefix := strings.TrimSpace(title[:idx])
	if strings.Contains(prefix, " ") {
		return ""
	}
	return prefix
}

// stripPrefix removes the package prefix from a PR title.
func stripPrefix(title string) string {
	idx := strings.Index(title, ":")
	if idx == -1 || idx > 40 {
		return title
	}
	return strings.TrimSpace(title[idx+1:])
}

func fetchPRs(repo, author string, after, before time.Time, limit int) ([]PRInfo, error) {
	// Build search query.
	query := fmt.Sprintf("repo:%s author:%s is:pr", repo, author)

	var allPRs []PRInfo
	page := 1
	perPage := 100
	if limit < perPage {
		perPage = limit
	}

	for len(allPRs) < limit {
		fmt.Fprintf(os.Stderr, "Fetching page %d...\n", page)

		cmd := exec.Command("gh", "api", "-X", "GET", "search/issues",
			"-f", fmt.Sprintf("q=%s", query),
			"-f", fmt.Sprintf("per_page=%d", perPage),
			"-f", fmt.Sprintf("page=%d", page),
			"-f", "sort=updated",
			"-f", "order=desc",
		)
		out, err := cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("gh api failed: %w", err)
		}

		var response struct {
			TotalCount int `json:"total_count"`
			Items      []struct {
				Number      int    `json:"number"`
				Title       string `json:"title"`
				State       string `json:"state"`
				HTMLURL     string `json:"html_url"`
				CreatedAt   string `json:"created_at"`
				UpdatedAt   string `json:"updated_at"`
				ClosedAt    string `json:"closed_at"`
				PullRequest *struct {
					MergedAt string `json:"merged_at"`
				} `json:"pull_request"`
			} `json:"items"`
		}
		if err := json.Unmarshal(out, &response); err != nil {
			return nil, fmt.Errorf("parsing response: %w", err)
		}

		for _, item := range response.Items {
			// Determine the relevant date (merged date or created date).
			var prDate time.Time
			if item.PullRequest != nil && item.PullRequest.MergedAt != "" {
				prDate, _ = time.Parse(time.RFC3339, item.PullRequest.MergedAt)
			} else if item.ClosedAt != "" {
				prDate, _ = time.Parse(time.RFC3339, item.ClosedAt)
			} else {
				prDate, _ = time.Parse(time.RFC3339, item.CreatedAt)
			}

			// Filter by date range.
			if prDate.Before(after) || !prDate.Before(before.AddDate(0, 0, 1)) {
				continue
			}

			allPRs = append(allPRs, PRInfo{
				Number:   item.Number,
				Title:    item.Title,
				URL:      item.HTMLURL,
				MergedAt: prDate,
				State:    item.State,
			})

			if len(allPRs) >= limit {
				break
			}
		}

		if len(response.Items) < perPage || len(allPRs) >= limit {
			break
		}
		page++
	}

	// Sort by date descending.
	sort.Slice(allPRs, func(i, j int) bool {
		return allPRs[i].MergedAt.After(allPRs[j].MergedAt)
	})

	return allPRs, nil
}

func groupByWeek(prs []PRInfo, start, end time.Time) []WeekSummary {
	// Find the Monday of each week.
	getWeekStart := func(t time.Time) time.Time {
		weekday := int(t.Weekday())
		if weekday == 0 {
			weekday = 7
		}
		return time.Date(t.Year(), t.Month(), t.Day()-weekday+1, 0, 0, 0, 0, t.Location())
	}

	// Create week buckets.
	weeks := make(map[time.Time][]PRInfo)
	for _, pr := range prs {
		weekStart := getWeekStart(pr.MergedAt)
		weeks[weekStart] = append(weeks[weekStart], pr)
	}

	// Build sorted list of weeks.
	var weekStarts []time.Time
	for ws := range weeks {
		weekStarts = append(weekStarts, ws)
	}
	sort.Slice(weekStarts, func(i, j int) bool {
		return weekStarts[i].After(weekStarts[j])
	})

	// Build summaries.
	var summaries []WeekSummary
	for _, ws := range weekStarts {
		summaries = append(summaries, WeekSummary{
			WeekStart: ws,
			WeekEnd:   ws.AddDate(0, 0, 6),
			PRs:       weeks[ws],
		})
	}

	return summaries
}
