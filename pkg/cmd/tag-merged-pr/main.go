// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

//// Options : Input flags
//type Options struct {
//	TokenPath      string `long:"token" description:"Path to GitHub token with repo:public_repo scope" required:"true"`
//	FromRef        string `long:"from" description:"From git ref" required:"true"`
//	ToRef          string `long:"to" description:"To git ref" required:"true"`
//	Repository     string `long:"repo" description:"Github repository to work on in 'owner/repo' format" required:"true"`
//	GitCheckoutDir string `long:"dir" description:"Git checkout directory"`
//	DryRun         bool   `long:"dry-run" description:"Dry run"`
//}

// Flags vars creation.
//var (
//	// TokenPath Path to GitHub token with repo:public_repo scope
//	TokenPath      string
//	//
//	FromRef        string
//	ToRef          string
//	Repository     string
//	GitCheckoutDir string
//	DryRun         bool
//)

func main() {
	log.SetFlags(0)
	//// Validating args
	//var opts Options
	//if _, err := flags.Parse(&opts); err != nil {
	//	log.Fatalf("Parse error: %+v\n", err)
	//}
	// Flags vars creation
	var TokenPath, FromRef, ToRef, Repository, GitCheckoutDir string
	var DryRun bool

	flag.StringVar(&TokenPath, "token", "", "Path to GitHub token with repo:public_repo scope")
	flag.StringVar(&FromRef, "from", "", "From git ref")
	flag.StringVar(&ToRef, "to", "", "To git ref")
	flag.StringVar(&Repository, "repo", "", "Github repository to work on in 'owner/repo")
	flag.StringVar(&GitCheckoutDir, "dir", "", "Git checkout directory")
	flag.BoolVar(&DryRun, "dry-run", false, "Dry run")
	// Creating required flags.
	required := []string{"token", "from", "to", "repo", "dir"}
	flag.Parse()

	seen := make(map[string]string)
	// Validating args.
	flag.Visit(func(f *flag.Flag) { seen[f.Name] = "yes" })
	for _, req := range required {
		if seen[req] != "yes" {
			seen[req] = fmt.Sprintf("missing required -%s argument/flag\n", req)
			continue
		} else {
			delete(seen, req)
		}
	}

	// printing all missing values at the same time.
	if len(seen) != 0 {
		for _, value := range seen {
			fmt.Print(value)
		}
		os.Exit(2) // the same exit code flag.Parse uses
	}

	if GitCheckoutDir != "" {
		if err := os.Chdir(GitCheckoutDir); err != nil {
			log.Fatalf("Cannot chdir to %s: %+v\n", GitCheckoutDir, err)
		}
	}
	token, err := readToken(TokenPath)
	if err != nil {
		log.Fatalf("Cannot read token from %s: %+v\n", TokenPath, err)
	}
	refList, err := getRefs(FromRef, ToRef)
	if err != nil {
		log.Fatalf("Cannot get refs: %+v\n", err)
	}

	for _, ref := range refList {
		tag, err := getTag(ref)
		if err != nil {
			log.Fatalf("Cannot get tag for ref %s: %+v\n", ref, err)
		}
		pr, err := getPrInfo(ref)
		if err != nil {
			log.Fatalf("Cannot find PR for ref %s: %+v\n", ref, err)
		}
		log.Printf("Labeling PR#%s (ref %s) using tag %s", pr, ref, tag)
		if DryRun {
			log.Println("DRY RUN: skipping labeling")
			continue
		}
		if err := labelPR(http.DefaultClient, Repository, token, pr, tag); err != nil {
			log.Fatalf("Failed on tag creation for Pull Request %s, error : '%s'\n", pr, err)
		}
	}
}

func readToken(path string) (string, error) {
	token, err := ioutil.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(token)), nil
}

func filterPullRequests(text string) []string {
	var shas []string
	for _, line := range strings.Split(text, "\n") {
		if !strings.Contains(line, "Merge pull request") {
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

func getTag(ref string) (string, error) {
	cmd := exec.Command("git", "tag", "--contains", ref)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	version := matchVersion(string(out))
	if version == "" {
		return "", fmt.Errorf("cannot find valid version")
	}
	return version, nil
}

func getPrNumber(text string) string {
	for _, prNumber := range strings.Fields(text) {
		if strings.HasPrefix(prNumber, "#") {
			return strings.TrimPrefix(prNumber, "#")
		}
	}
	return ""
}

func getPrInfo(ref string) (string, error) {
	cmd := exec.Command("git", "show", "--oneline", "--format=format:%h %s", ref)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	pr := getPrNumber(string(out))
	if pr == "" {
		return "", fmt.Errorf("cannot find PR number")
	}
	return pr, nil
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
