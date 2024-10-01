// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

func main() {
	log.SetFlags(0)
	var tokenPath, fromRef, toRef, repository, gitCheckoutDir string
	var dryRun bool

	flag.StringVar(&tokenPath, "token", "", "Path to a file containing the GitHub token with repo:public_repo scope")
	flag.StringVar(&fromRef, "from", "", "From git ref")
	flag.StringVar(&toRef, "to", "", "To git ref")
	flag.StringVar(&repository, "repo", "", "Github \"owner/repo\" to work on. Default: cockroachdb/cockroach")
	flag.StringVar(&gitCheckoutDir, "dir", "", "Git checkout directory")
	flag.BoolVar(&dryRun, "dry-run", false, "Dry run")
	flag.Parse()

	// Validating required flags.
	allRequiredFlagsSet := true
	if tokenPath == "" {
		allRequiredFlagsSet = false
		log.Println("missing required --token-path argument/flag")
	}
	if fromRef == "" {
		allRequiredFlagsSet = false
		log.Println("missing required --from argument/flag")
	}
	if toRef == "" {
		allRequiredFlagsSet = false
		log.Println("missing required --to argument/flag")
	}
	if gitCheckoutDir == "" {
		allRequiredFlagsSet = false
		log.Println("missing required --dir argument/flag")
	}

	if !allRequiredFlagsSet {
		log.Fatal("Try running with `-h` for help\nPlease provide all required parameters")
	}

	if repository == "" {
		repository = "cockroachdb/cockroach"
		log.Printf("No repository specified. Using %s\n", repository)
	}

	if gitCheckoutDir != "" {
		if err := os.Chdir(gitCheckoutDir); err != nil {
			log.Fatalf("Cannot chdir to %s: %+v\n", gitCheckoutDir, err)
		}
	}
	token, err := readToken(tokenPath)
	if err != nil {
		log.Fatalf("Cannot read token from %s: %+v\n", tokenPath, err)
	}
	commonBaseRef, err := getCommonBaseRef(fromRef, toRef)
	if err != nil {
		log.Fatalf("Cannot get common base ref: %+v\n", err)
	}

	refList, err := getRefs(commonBaseRef, toRef)
	if err != nil {
		log.Fatalf("Cannot get refs: %+v\n", err)
	}

	for _, ref := range refList {
		tag, err := getFirstTagContainingRef(ref)
		if err != nil {
			log.Fatalf("Error getting first tag containing ref %s: %+v\n", ref, err)
		}
		if tag == "" {
			log.Printf("Ref %s has not yet appeared in a released version.", ref)
			continue
		}
		prs, err := getPrNumbers(ref)
		if err != nil {
			log.Fatalf("Cannot find PR for ref %s: %+v\n", ref, err)
		}
		if len(prs) == 0 {
			log.Printf("No PRs for ref %s. There should be at least one.", ref)
			continue
		}
		for _, pr := range prs {
			log.Printf("Labeling PR#%s (ref %s) using git tag %s", pr, ref, tag)
			if dryRun {
				log.Println("DRY RUN: skipping labeling")
				continue
			}
			if err := labelPR(http.DefaultClient, repository, token, pr, tag); err != nil {
				log.Fatalf("Failed on label creation for Pull Request %s: '%s'\n", pr, err)
			}
		}
	}
}

func readToken(path string) (string, error) {
	token, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(token)), nil
}

func getCommonBaseRef(fromRef, toRef string) (string, error) {
	cmd := exec.Command("git", "merge-base", fromRef, toRef)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func filterPullRequests(text string) []string {
	var shas []string
	matchMerge := regexp.MustCompile(`Merge (#|pull request)`)
	for _, line := range strings.Split(text, "\n") {
		if !matchMerge.MatchString(line) {
			continue
		}
		sha := strings.Fields(line)[0]
		shas = append(shas, sha)
	}
	return shas
}

func getRefs(fromRef, toRef string) ([]string, error) {
	cmd := exec.Command("git", "log", "--merges", "--reverse", "--oneline",
		"--format=format:%h %s", "--ancestry-path", fmt.Sprintf("%s..%s", fromRef, toRef))
	out, err := cmd.Output()
	if err != nil {
		return []string{}, err
	}
	return filterPullRequests(string(out)), nil
}

func matchVersion(text string) string {
	for _, line := range strings.Fields(text) {
		// Only looking for version tags.
		regVersion := regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)`)
		if !regVersion.MatchString(line) {
			continue
		}
		// Should avoid *-alpha.00000000 tag, so we continue with the next line value.
		alpha00Regex := regexp.MustCompile(`v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-alpha.00+$`)
		if alpha00Regex.MatchString(line) {
			continue
		}
		// Checking first for An alpha/beta/rc tag.
		// if present, return line value
		alphaBetaRcRegex := regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-[-.0-9A-Za-z]+$`)
		if alphaBetaRcRegex.MatchString(line) {
			return line
		}
		// Second check is vX.Y.Z patch release >= .1 is first (ex: v20.1.1).
		patchRegex := regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.[1-9][0-9]*$`)
		if patchRegex.MatchString(line) {
			return line
		}
		// Third check is major release A vX.Y.0 release.
		majorReleaseRegex := regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.0$`)
		if majorReleaseRegex.MatchString(line) {
			return line
		}
	}
	return ""
}

func getFirstTagContainingRef(ref string) (string, error) {
	cmd := exec.Command("git", "tag", "--contains", ref)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	version := matchVersion(string(out))
	return version, nil
}

func extractPrNumbers(text string) []string {
	var numbers []string
	lines := strings.SplitN(text, "\n", 2)
	for _, prNumber := range strings.Fields(lines[0]) {
		if strings.HasPrefix(prNumber, "#") {
			numbers = append(numbers, strings.TrimPrefix(prNumber, "#"))
		}
	}
	return numbers
}

func getPrNumbers(ref string) ([]string, error) {
	cmd := exec.Command("git", "show", "--oneline", "--format=format:%h %s", ref)
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	prs := extractPrNumbers(string(out))
	return prs, nil
}

func apiCall(client *http.Client, url string, token string, payload interface{}) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", url, bytes.NewReader(payloadJSON))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", fmt.Sprintf("token %s", token))
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Status code 422 is returned when a label already exist
	if !(resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusUnprocessableEntity || resp.
		StatusCode == http.StatusOK) {
		return fmt.Errorf("status code %d from %s", resp.StatusCode, url)
	}
	return nil
}

func labelPR(client *http.Client, repository string, token string, pr string, tag string) error {
	label := fmt.Sprintf("earliest-release-%s", tag)
	payload := struct {
		Name  string `json:"name"`
		Color string `json:"color"`
	}{
		Name:  label,
		Color: "000000",
	}
	if err := apiCall(client, fmt.Sprintf("https://api.github.com/repos/%s/labels", repository), token,
		payload); err != nil {
		return err
	}
	url := fmt.Sprintf("https://api.github.com/repos/%s/issues/%s/labels", repository, pr)
	labels := []string{label}
	if err := apiCall(client, url, token, labels); err != nil {
		return err
	}
	log.Printf("Label %s added to PR %s\n", label, pr)
	return nil
}
