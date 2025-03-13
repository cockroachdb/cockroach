// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type SentryIssue struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	FirstSeen string `json:"firstSeen"`
	LastSeen  string `json:"lastSeen"`
}

const baseURL = "https://sentry.io/api/0/organizations/cockroach-labs/issues/"

var queryParams = map[string]string{
	"project":     "164528",
	"query":       "panic cmd:demo",
	"statsPeriod": "1d",
}

func findSentryIssues(token string) ([]SentryIssue, error) {
	log.Println("Waiting 1 minute before querying Sentry...")
	time.Sleep(1 * time.Minute)

	q := url.Values{}
	for k, v := range queryParams {
		q.Add(k, v)
	}

	reqURL := fmt.Sprintf("%s?%s", baseURL, q.Encode())
	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	client := &http.Client{Timeout: 30 * time.Second}

	log.Printf("Querying Sentry API: %s", reqURL)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	var sentryResp []SentryIssue
	if err := json.NewDecoder(resp.Body).Decode(&sentryResp); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	return sentryResp, nil
}

func deleteSentryIssue(token, issueID string) error {
	url := fmt.Sprintf("https://sentry.io/api/0/issues/%s/", issueID)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("creating delete request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	client := &http.Client{Timeout: 30 * time.Second}

	log.Printf("Deleting Sentry issue: %s", issueID)
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("executing delete request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

type GitHubIssue struct {
	Number    int    `json:"number"`
	Title     string `json:"title"`
	Body      string `json:"body"`
	CreatedAt string `json:"created_at"`
	HTMLURL   string `json:"html_url"`
}

type GitHubSearchResponse struct {
	TotalCount int           `json:"total_count"`
	Items      []GitHubIssue `json:"items"`
}

// findGitHubIssues searches for GitHub issues in the cockroachdb/cockroach repo
// with the specified parameters
func findGitHubIssues(token string, version string) ([]GitHubIssue, error) {
	today := timeutil.Now().Format("2006-01-02")
	query := url.QueryEscape(
		fmt.Sprintf("repo:cockroachdb/cockroach is:issue author:cockroach-sentry label:O-sentry sort:updated-desc \"panic\" in:title \"| Cockroach Release | %s |\" in:body created:%s",
			version, today),
	)

	reqURL := fmt.Sprintf("https://api.github.com/search/issues?q=%s&per_page=100", query)

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating GitHub request: %w", err)
	}

	// Add authorization if token is provided
	if token != "" {
		req.Header.Set("Authorization", fmt.Sprintf("token %s", token))
	}

	req.Header.Set("Accept", "application/vnd.github.v3+json")
	client := &http.Client{Timeout: 30 * time.Second}

	log.Printf("Querying GitHub API: %s", reqURL)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing GitHub request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected GitHub status code: %d, body: %s", resp.StatusCode, string(body))
	}

	var searchResp GitHubSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&searchResp); err != nil {
		return nil, fmt.Errorf("parsing GitHub response: %w", err)
	}

	log.Printf("GitHub search found %d matching issues", searchResp.TotalCount)
	return searchResp.Items, nil
}

func main() {
	// requires issues:admin scope
	sentryToken := os.Getenv("SENTRY_AUTH_TOKEN")
	if sentryToken == "" {
		log.Fatal("SENTRY_AUTH_TOKEN environment variable is required")
	}

	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "cockroach-sentry-*")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = os.RemoveAll(tmpDir) // Clean up temp directory at the end
	}()
	log.Printf("Created temp directory: %s", tmpDir)

	// Change to temp directory
	originalDir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		log.Fatal(err)
	}
	// Ensure we change back to original directory when done
	defer func() {
		_ = os.Chdir(originalDir)
	}()

	// Set Google credentials and log into gcloud
	if credJSON := os.Getenv("GOOGLE_CREDENTIALS"); credJSON != "" {
		log.Println("Setting Google credentials")
		credFile := filepath.Join(tmpDir, "google-credentials.json")
		if err := os.WriteFile(credFile, []byte(credJSON), 0o600); err != nil {
			log.Fatal(err)
		}
		// Log in to GCP using the credentials file
		log.Println("Logging in to Google Cloud Platform...")
		gcloudCmd := exec.Command("gcloud", "auth", "activate-service-account", "--key-file", credFile)
		if out, err := gcloudCmd.CombinedOutput(); err != nil {
			log.Fatalf("Failed to log in to GCP: %v\n%s", err, string(out))
		}
		log.Println("Successfully logged in to GCP")
	}

	// Read version from file (using absolute path)
	versionBytes, err := os.ReadFile(filepath.Join(originalDir, "pkg/build/version.txt"))
	if err != nil {
		log.Fatal(err)
	}
	version := strings.TrimSpace(strings.Split(string(versionBytes), "\n")[0])
	if strings.HasPrefix(version, "#") {
		version = strings.TrimSpace(strings.Split(string(versionBytes), "\n")[1])
	}
	if envVersion := os.Getenv("VERSION"); envVersion != "" {
		log.Printf("Overriding version with VERSION environment variable: %s", envVersion)
		version = envVersion
	}
	log.Printf("Using version: %s", version)

	platform := "linux-amd64"
	if runtime.GOOS == "darwin" {
		platform = "darwin-11.0-arm64"
	}
	artifactName := fmt.Sprintf("cockroach-%s.%s.tgz", version, platform)
	log.Printf("Downloading artifact: %s", artifactName)
	gsutilCmd := exec.Command("gsutil", "cp",
		fmt.Sprintf("gs://cockroach-release-artifacts-staged-prod/%s", artifactName),
		"./")
	if err := gsutilCmd.Run(); err != nil {
		log.Fatal(err)
	}
	log.Printf("Downloaded artifact: %s", artifactName)

	tarCmd := exec.Command("tar", "xf", artifactName)
	if err := tarCmd.Run(); err != nil {
		log.Fatal(err)
	}

	cockroachPath := fmt.Sprintf("./cockroach-%s.%s/cockroach", version, platform)
	panicCmd := exec.Command(cockroachPath, "demo", "--insecure", "-e", "select crdb_internal.force_panic('testing');")
	out, _ := panicCmd.CombinedOutput()
	log.Printf("panic command output: %s", string(out))

	// After running the panic command, find and delete Sentry issues
	issues, err := findSentryIssues(sentryToken)
	if err != nil {
		log.Printf("Error finding Sentry issues: %v", err)
	}

	if len(issues) == 0 {
		log.Fatal("No Sentry issues found")
	}

	log.Printf("Found %d issues matching the query", len(issues))
	for _, issue := range issues {
		if err := deleteSentryIssue(sentryToken, issue.ID); err != nil {
			log.Printf("Error deleting Sentry issue %s: %v", issue.ID, err)
		} else {
			log.Printf("Successfully deleted Sentry issue %s (title: %s, first seen: %s, last seen: %s)",
				issue.ID, issue.Title, issue.FirstSeen, issue.LastSeen)
		}
	}
	// After handling Sentry issues, search GitHub issues
	githubToken := os.Getenv("GITHUB_TOKEN")
	if githubToken == "" {
		log.Println("Warning: GITHUB_TOKEN not set, GitHub API requests may be rate limited")
	}

	githubIssues, err := findGitHubIssues(githubToken, version)
	if err != nil {
		log.Printf("Error finding GitHub issues: %v", err)
	} else {
		log.Printf("Found %d GitHub issues", len(githubIssues))
		for _, issue := range githubIssues {
			log.Printf("GitHub Issue #%d: %s\n  URL: %s\n  Created: %s",
				issue.Number, issue.Title, issue.HTMLURL, issue.CreatedAt)
		}
	}
}
