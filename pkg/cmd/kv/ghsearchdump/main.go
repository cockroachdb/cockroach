// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// ghsearchdump searches GitHub for PRs and issues with activity from specified
// authors and dumps their content in a format suitable for LLM consumption.
// It filters out bot noise automatically.
//
// Usage:
//
//	ghsearchdump --search "repo:cockroachdb/cockroach author:tbg author:wenyihu6" \
//	  --contribution-after 2025-11-01 --contribution-before 2025-12-01
//
// For PRs, "contribution" means a push (commit). For issues, it means creating
// the issue or adding a comment.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

var (
	searchQuery        = flag.String("search", "", "GitHub search query (required, must include author: terms)")
	includeIssues      = flag.Bool("include-issues", true, "Also search issues, not just PRs")
	limit              = flag.Int("limit", 1000, "Maximum number of results (GitHub caps at 1000)")
	repo               = flag.String("repo", "", "Repository (extracted from search query if present)")
	contributionAfter  = flag.String("contribution-after", "", "Only include items with contribution on or after this date (YYYY-MM-DD)")
	contributionBefore = flag.String("contribution-before", "", "Only include items with contribution before this date (YYYY-MM-DD)")
)

// Known bots whose comments we filter out.
var botUsers = map[string]bool{
	"blathers-crl":        true,
	"cockroach-teamcity":  true,
	"craig":               true,
	"codecov":             true,
	"codecov-commenter":   true,
	"github-actions":      true,
	"dependabot":          true,
	"renovate":            true,
	"copilot-pull-review": true,
}

type Author struct {
	Login string `json:"login"`
}

type Comment struct {
	ID        string `json:"id"`
	Author    Author `json:"author"`
	Body      string `json:"body"`
	CreatedAt string `json:"createdAt"`
}

type Review struct {
	ID          string `json:"id"`
	Author      Author `json:"author"`
	Body        string `json:"body"`
	State       string `json:"state"`
	SubmittedAt string `json:"submittedAt"`
}

type Commit struct {
	CommittedDate string `json:"committedDate"`
}

type PRDetail struct {
	Number   int       `json:"number"`
	Title    string    `json:"title"`
	Body     string    `json:"body"`
	State    string    `json:"state"`
	MergedAt string    `json:"mergedAt,omitempty"`
	Author   Author    `json:"author"`
	Comments []Comment `json:"comments"`
	Reviews  []Review  `json:"reviews"`
	Commits  []Commit  `json:"commits"`
}

type IssueDetail struct {
	Number    int       `json:"number"`
	Title     string    `json:"title"`
	Body      string    `json:"body"`
	State     string    `json:"state"`
	CreatedAt string    `json:"createdAt"`
	Author    Author    `json:"author"`
	Comments  []Comment `json:"comments"`
}

type CleanComment struct {
	Author    string `json:"author"`
	Body      string `json:"body"`
	CreatedAt string `json:"createdAt"`
}

type CleanReview struct {
	Author      string `json:"author"`
	Body        string `json:"body"`
	State       string `json:"state"`
	SubmittedAt string `json:"submittedAt"`
}

type CleanPR struct {
	Number           int            `json:"number"`
	Title            string         `json:"title"`
	URL              string         `json:"url"`
	Author           string         `json:"author"`
	State            string         `json:"state"`
	MergedAt         string         `json:"mergedAt,omitempty"`
	LastContribution string         `json:"lastContribution,omitempty"`
	Body             string         `json:"body"`
	Comments         []CleanComment `json:"comments,omitempty"`
	Reviews          []CleanReview  `json:"reviews,omitempty"`
}

type CleanIssue struct {
	Number           int            `json:"number"`
	Title            string         `json:"title"`
	URL              string         `json:"url"`
	Author           string         `json:"author"`
	State            string         `json:"state"`
	CreatedAt        string         `json:"createdAt"`
	LastContribution string         `json:"lastContribution,omitempty"`
	Body             string         `json:"body"`
	Comments         []CleanComment `json:"comments,omitempty"`
}

type Output struct {
	Query  string       `json:"query"`
	PRs    []CleanPR    `json:"prs,omitempty"`
	Issues []CleanIssue `json:"issues,omitempty"`
}

func main() {
	flag.Parse()

	if *searchQuery == "" {
		fmt.Fprintln(os.Stderr, "error: --search is required")
		flag.Usage()
		os.Exit(1)
	}

	// Parse date filters.
	var contribAfter, contribBefore time.Time
	var err error
	if *contributionAfter != "" {
		contribAfter, err = time.Parse("2006-01-02", *contributionAfter)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: invalid --contribution-after date: %v\n", err)
			os.Exit(1)
		}
	}
	if *contributionBefore != "" {
		contribBefore, err = time.Parse("2006-01-02", *contributionBefore)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: invalid --contribution-before date: %v\n", err)
			os.Exit(1)
		}
	}

	// Extract repo from search query if not explicitly provided.
	repoName := *repo
	if repoName == "" {
		repoName = extractRepo(*searchQuery)
	}
	if repoName == "" {
		fmt.Fprintln(os.Stderr, "error: could not extract repo from search query; use --repo")
		os.Exit(1)
	}

	// Extract authors from search query - these are the users whose contributions count.
	authors := extractAuthors(*searchQuery)
	if len(authors) == 0 {
		fmt.Fprintln(os.Stderr, "warning: no author: terms found in search query; all comments will be considered")
	} else {
		fmt.Fprintf(os.Stderr, "tracking contributions from: %v\n", authors)
	}
	authorSet := make(map[string]bool)
	for _, a := range authors {
		authorSet[strings.ToLower(a)] = true
	}

	output := Output{
		Query: *searchQuery,
	}

	// Search for PRs.
	prNumbers, err := searchItems(*searchQuery, "pr", *limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error searching PRs: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "found %d PRs from search\n", len(prNumbers))

	// Fetch details for each PR.
	skippedPRs := 0
	for i, num := range prNumbers {
		fmt.Fprintf(os.Stderr, "fetching PR %d/%d: #%d", i+1, len(prNumbers), num)
		detail, err := fetchPRDetail(repoName, num)
		if err != nil {
			fmt.Fprintf(os.Stderr, " - warning: failed to fetch: %v\n", err)
			continue
		}

		// Get last push date from commits.
		lastPush := getLastPushDate(detail.Commits)

		// Apply date filters.
		if !contribAfter.IsZero() && lastPush.Before(contribAfter) {
			fmt.Fprintf(os.Stderr, " - skipped (last push %s before %s)\n",
				lastPush.Format("2006-01-02"), contribAfter.Format("2006-01-02"))
			skippedPRs++
			continue
		}
		if !contribBefore.IsZero() && !lastPush.Before(contribBefore) {
			fmt.Fprintf(os.Stderr, " - skipped (last push %s on/after %s)\n",
				lastPush.Format("2006-01-02"), contribBefore.Format("2006-01-02"))
			skippedPRs++
			continue
		}

		fmt.Fprintf(os.Stderr, " - last push %s\n", lastPush.Format("2006-01-02"))
		output.PRs = append(output.PRs, cleanPRDetail(repoName, detail, lastPush))
	}

	if skippedPRs > 0 {
		fmt.Fprintf(os.Stderr, "included %d PRs (%d skipped by date filter)\n", len(output.PRs), skippedPRs)
	}

	// Search for issues if requested.
	if *includeIssues {
		issueNumbers, err := searchItems(*searchQuery, "issue", *limit)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error searching issues: %v\n", err)
			os.Exit(1)
		}

		fmt.Fprintf(os.Stderr, "found %d issues from search\n", len(issueNumbers))

		skippedIssues := 0
		for i, num := range issueNumbers {
			fmt.Fprintf(os.Stderr, "fetching issue %d/%d: #%d", i+1, len(issueNumbers), num)
			detail, err := fetchIssueDetail(repoName, num)
			if err != nil {
				fmt.Fprintf(os.Stderr, " - warning: failed to fetch: %v\n", err)
				continue
			}

			// Find last contribution from one of the tracked authors.
			lastContrib := getLastIssueContribution(detail, authorSet, contribAfter, contribBefore)
			if lastContrib.IsZero() {
				fmt.Fprintf(os.Stderr, " - skipped (no matching contribution in date range)\n")
				skippedIssues++
				continue
			}

			fmt.Fprintf(os.Stderr, " - last contribution %s\n", lastContrib.Format("2006-01-02"))
			output.Issues = append(output.Issues, cleanIssueDetail(repoName, detail, lastContrib))
		}

		if skippedIssues > 0 {
			fmt.Fprintf(os.Stderr, "included %d issues (%d skipped by date/author filter)\n", len(output.Issues), skippedIssues)
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(output); err != nil {
		fmt.Fprintf(os.Stderr, "error encoding output: %v\n", err)
		os.Exit(1)
	}
}

func extractRepo(query string) string {
	for _, part := range strings.Fields(query) {
		if strings.HasPrefix(part, "repo:") {
			return strings.TrimPrefix(part, "repo:")
		}
	}
	return ""
}

func extractAuthors(query string) []string {
	var authors []string
	for _, part := range strings.Fields(query) {
		if strings.HasPrefix(part, "author:") {
			authors = append(authors, strings.TrimPrefix(part, "author:"))
		}
	}
	return authors
}

func searchItems(query string, itemType string, limit int) ([]int, error) {
	// Use gh api directly because `gh search prs` with multiple --author flags
	// AND's them together instead of OR'ing them like the web UI does.
	fullQuery := query + " is:" + itemType

	var allNumbers []int
	page := 1
	perPage := 100
	if limit < perPage {
		perPage = limit
	}

	for len(allNumbers) < limit {
		cmd := exec.Command("gh", "api", "-X", "GET", "search/issues",
			"-f", fmt.Sprintf("q=%s", fullQuery),
			"-f", fmt.Sprintf("per_page=%d", perPage),
			"-f", fmt.Sprintf("page=%d", page),
		)
		cmd.Stderr = os.Stderr
		out, err := cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("gh api search/issues failed: %w", err)
		}

		var response struct {
			TotalCount int `json:"total_count"`
			Items      []struct {
				Number int `json:"number"`
			} `json:"items"`
		}
		if err := json.Unmarshal(out, &response); err != nil {
			return nil, fmt.Errorf("parsing search results: %w", err)
		}

		for _, item := range response.Items {
			allNumbers = append(allNumbers, item.Number)
			if len(allNumbers) >= limit {
				break
			}
		}

		// Check if there are more pages.
		if len(response.Items) < perPage || len(allNumbers) >= response.TotalCount {
			break
		}
		page++
	}

	return allNumbers, nil
}

func fetchPRDetail(repo string, number int) (*PRDetail, error) {
	cmd := exec.Command("gh", "pr", "view", fmt.Sprintf("%d", number),
		"--repo", repo,
		"--json", "number,title,body,state,mergedAt,author,comments,reviews,commits",
	)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("gh pr view failed: %w", err)
	}

	var detail PRDetail
	if err := json.Unmarshal(out, &detail); err != nil {
		return nil, fmt.Errorf("parsing PR detail: %w", err)
	}
	return &detail, nil
}

func fetchIssueDetail(repo string, number int) (*IssueDetail, error) {
	cmd := exec.Command("gh", "issue", "view", fmt.Sprintf("%d", number),
		"--repo", repo,
		"--json", "number,title,body,state,createdAt,author,comments",
	)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("gh issue view failed: %w", err)
	}

	var detail IssueDetail
	if err := json.Unmarshal(out, &detail); err != nil {
		return nil, fmt.Errorf("parsing issue detail: %w", err)
	}
	return &detail, nil
}

func getLastPushDate(commits []Commit) time.Time {
	var latest time.Time
	for _, c := range commits {
		t, err := time.Parse(time.RFC3339, c.CommittedDate)
		if err != nil {
			continue
		}
		if t.After(latest) {
			latest = t
		}
	}
	return latest
}

// getLastIssueContribution finds the most recent contribution (issue creation or comment)
// from one of the tracked authors within the date range. Returns zero time if none found.
func getLastIssueContribution(issue *IssueDetail, authorSet map[string]bool, after, before time.Time) time.Time {
	var latest time.Time

	isInRange := func(t time.Time) bool {
		if !after.IsZero() && t.Before(after) {
			return false
		}
		if !before.IsZero() && !t.Before(before) {
			return false
		}
		return true
	}

	// Check issue creation.
	if len(authorSet) == 0 || authorSet[strings.ToLower(issue.Author.Login)] {
		if t, err := time.Parse(time.RFC3339, issue.CreatedAt); err == nil {
			if isInRange(t) && t.After(latest) {
				latest = t
			}
		}
	}

	// Check comments.
	for _, c := range issue.Comments {
		if len(authorSet) == 0 || authorSet[strings.ToLower(c.Author.Login)] {
			if t, err := time.Parse(time.RFC3339, c.CreatedAt); err == nil {
				if isInRange(t) && t.After(latest) {
					latest = t
				}
			}
		}
	}

	return latest
}

func cleanPRDetail(repo string, pr *PRDetail, lastPush time.Time) CleanPR {
	clean := CleanPR{
		Number:   pr.Number,
		Title:    pr.Title,
		URL:      fmt.Sprintf("https://github.com/%s/pull/%d", repo, pr.Number),
		Author:   pr.Author.Login,
		State:    pr.State,
		MergedAt: pr.MergedAt,
		Body:     pr.Body,
	}
	if !lastPush.IsZero() {
		clean.LastContribution = lastPush.Format(time.RFC3339)
	}

	// Filter comments, removing bots and empty bodies.
	for _, c := range pr.Comments {
		if botUsers[c.Author.Login] {
			continue
		}
		body := strings.TrimSpace(c.Body)
		if body == "" {
			continue
		}
		// Skip "bors r+" type comments.
		if strings.HasPrefix(strings.ToLower(body), "bors r") {
			continue
		}
		// Skip Reviewable status-only comments.
		if strings.Contains(body, "<!-- Reviewable:start -->") && !strings.Contains(body, "___") {
			continue
		}
		clean.Comments = append(clean.Comments, CleanComment{
			Author:    c.Author.Login,
			Body:      body,
			CreatedAt: c.CreatedAt,
		})
	}

	// Filter reviews, removing empty ones.
	for _, r := range pr.Reviews {
		if botUsers[r.Author.Login] {
			continue
		}
		body := strings.TrimSpace(r.Body)
		if body == "" {
			continue
		}
		clean.Reviews = append(clean.Reviews, CleanReview{
			Author:      r.Author.Login,
			Body:        body,
			State:       r.State,
			SubmittedAt: r.SubmittedAt,
		})
	}

	return clean
}

func cleanIssueDetail(repo string, issue *IssueDetail, lastContrib time.Time) CleanIssue {
	clean := CleanIssue{
		Number:    issue.Number,
		Title:     issue.Title,
		URL:       fmt.Sprintf("https://github.com/%s/issues/%d", repo, issue.Number),
		Author:    issue.Author.Login,
		State:     issue.State,
		CreatedAt: issue.CreatedAt,
		Body:      issue.Body,
	}
	if !lastContrib.IsZero() {
		clean.LastContribution = lastContrib.Format(time.RFC3339)
	}

	// Filter comments, removing bots and empty bodies.
	for _, c := range issue.Comments {
		if botUsers[c.Author.Login] {
			continue
		}
		body := strings.TrimSpace(c.Body)
		if body == "" {
			continue
		}
		clean.Comments = append(clean.Comments, CleanComment{
			Author:    c.Author.Login,
			Body:      body,
			CreatedAt: c.CreatedAt,
		})
	}

	return clean
}
