// Copyright 2022 The Cockroach Authors.
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
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"

	"github.com/google/go-github/github"
	"golang.org/x/oauth2"
)

var (
	fixIssueRefRE        = regexp.MustCompile(`(?im)(?i:close[sd]?|fix(?:e[sd])?|resolve[sd]?):?\s+(?:(?:(#\d+)|([\w.-]+[/][\w.-]+#\d+)|([A-Z]+-\d+))[,.;]?(?:[ \t\n\r]+|$))+`)
	informIssueRefRE     = regexp.MustCompile(`(?im)(?:see also|informs):?\s+(?:(?:(#\d+)|([\w.-]+[/][\w.-]+#\d+)|([A-Z]+-\d+))[,.;]?(?:[ \t\n\r]+|$))+`)
	epicRefRE            = regexp.MustCompile(`(?im)epic:?\s+(?:([A-Z]+-[0-9]+)[,.;]?(?:[ \t\n\r]+|$))+`)
	epicNoneRE           = regexp.MustCompile(`(?im)epic:?\s+(?:(none)[,.;]?(?:[ \t\n\r]+|$))+`)
	githubJiraIssueRefRE = regexp.MustCompile(`(#\d+)|([\w.-]+[/][\w.-]+#\d+)|([A-Za-z]+-\d+)`)
	jiraIssueRefRE       = regexp.MustCompile(`[A-Za-z]+-[0-9]+`)
)

type commitInfo struct {
	epicRefs        map[string]int
	epicNone        bool
	issueCloseRefs  map[string]int
	issueInformRefs map[string]int
	sha             string
}

type prBodyInfo struct {
	epicRefs        map[string]int
	epicNone        bool
	issueCloseRefs  map[string]int
	issueInformRefs map[string]int
	prNumber        int
}

func isMultiPrBackport(prBody string) bool {
	multiPrBackportRE := regexp.MustCompile(`(?i)^\s*Backport:\s*`)
	multiPrBackportRE2 := regexp.MustCompile(`(?i)^\s*Backport for\s*`)

	if multiPrBackportRE.MatchString(prBody) || multiPrBackportRE2.MatchString(prBody) {
		return true
	}

	return false
}

func extractStringsFromMessage(
	message string, firstMatch, secondMatch *regexp.Regexp,
) map[string]int {
	ids := map[string]int{}
	if allMatches := firstMatch.FindAllString(message, -1); len(allMatches) > 0 {
		for _, x := range allMatches {
			matches := secondMatch.FindAllString(x, -1)
			for _, match := range matches {
				ids[match]++
			}
		}
	}
	return ids
}

func extractFixIssueIDs(message string) map[string]int {
	return extractStringsFromMessage(message, fixIssueRefRE, githubJiraIssueRefRE)
}

func extractInformIssueIDs(message string) map[string]int {
	return extractStringsFromMessage(message, informIssueRefRE, githubJiraIssueRefRE)
}

func extractEpicIDs(message string) map[string]int {
	return extractStringsFromMessage(message, epicRefRE, jiraIssueRefRE)
}

func extractEpicNone(message string) bool {
	if allMatches := epicNoneRE.FindAllString(message, -1); len(allMatches) > 0 {
		return true
	}
	return false
}

func scanCommitsForEpicAndIssueRefs(
	ctx context.Context, ghClient *github.Client, params *parameters,
) ([]commitInfo, error) {
	commits := []*github.RepositoryCommit{}
	for page := 1; page <= 3; page++ {
		commitsInQuery, _, err := ghClient.PullRequests.ListCommits(
			ctx, params.Org, params.Repo, params.PrNumber, &github.ListOptions{PerPage: 100, Page: page},
		)
		if err != nil {
			return nil, err
		}
		commits = append(commits, commitsInQuery...)
		if len(commitsInQuery) < 100 {
			break
		}
	}

	var infos []commitInfo
	for _, commit := range commits {
		message := commit.GetCommit().GetMessage()
		var info = commitInfo{
			epicRefs:        extractEpicIDs(message),
			epicNone:        extractEpicNone(message),
			issueCloseRefs:  extractFixIssueIDs(message),
			issueInformRefs: extractInformIssueIDs(message),
			sha:             commit.GetSHA(),
		}
		infos = append(infos, info)
	}

	return infos, nil
}

func commitShaURL(sha string, params *parameters) string {
	return fmt.Sprintf("https://github.com/%s/%s/commit/%s", params.Org, params.Repo, sha)
}

// checkForMissingRefs determines if the PR and its commits has the needed refs.
// When the PR body is missing a ref and one or more commits are missing a ref, tell the
// caller which commits are missing refs.
//
// Returns:
// - True if the check passed. False if it failed.
func checkForMissingRefs(prInfo prBodyInfo, commitInfos []commitInfo, params *parameters) bool {
	// When the PR body has a ref, no refs are needed in individual commits
	if len(prInfo.epicRefs)+len(prInfo.issueInformRefs)+len(prInfo.issueCloseRefs) > 0 || prInfo.epicNone {
		return true
	}

	commitsWithoutRefs := []string{}
	for _, info := range commitInfos {
		if len(info.epicRefs)+len(info.issueInformRefs)+len(info.issueCloseRefs) == 0 && !info.epicNone {
			commitsWithoutRefs = append(commitsWithoutRefs, commitShaURL(info.sha, params))
		}
	}
	if len(commitsWithoutRefs) > 0 {
		if len(commitsWithoutRefs) == len(commitInfos) {
			fmt.Print("Error: The PR body or each commit in the PR should have an epic or issue ref but they don't\n\n")
		} else {
			fmt.Printf("Error: These commits should have an epic or issue ref but don't: %v\n\n", commitsWithoutRefs)
		}
		return false
	}
	return true
}

// checkPrEpicsUsedInCommits checks that all epic references in the PR body are used in at least one
// commit message and that the individual commits note the epic with which they are associated.
//
// Returns:
// - True if the check passed. False if it failed.
func checkPrEpicsUsedInCommits(
	prInfo prBodyInfo, commitInfos []commitInfo, params *parameters,
) bool {
	if len(prInfo.epicRefs) < 2 {
		return true
	}

	passedCheck := true
	printHeader := func() {
		if !passedCheck {
			return
		}
		fmt.Print(
			"When multiple epics are referenced in the PR body, each commit of the PR should " +
				"reference the epic(s) that commit is part of and all epics listed in the commit messages " +
				"should be listed in the PR body. These unexpected cases were found:\n",
		)
		passedCheck = false
	}

	// Check PR body epics are all referenced from commit messages
	for _, prEpic := range reflect.ValueOf(prInfo.epicRefs).MapKeys() {
		found := false
		for _, ci := range commitInfos {
			if ci.epicRefs[prEpic.String()] > 0 {
				found = true
			}
		}
		if !found {
			printHeader()
			fmt.Printf(
				"- Error: This epic was listed in the PR body but was not referenced in any commits: %s\n",
				prEpic.String(),
			)
		}
	}

	// Check that all commit messages reference one of the PR epics
	for _, ci := range commitInfos {
		if ci.epicNone {
			continue
		}
		if len(ci.epicRefs) == 0 {
			printHeader()
			fmt.Printf("- Error: This commit did not reference an epic but should: %#v\n", commitShaURL(ci.sha, params))
		} else {
			for _, epicRef := range reflect.ValueOf(ci.epicRefs).MapKeys() {
				if _, ok := prInfo.epicRefs[epicRef.String()]; !ok {
					printHeader()
					fmt.Printf(
						"- Error: This commit references an epic that isn't referenced in the PR body. "+
							"epic_ref: %v, commit: %s\n",
						epicRef,
						commitShaURL(ci.sha, params),
					)
				}
			}
		}
	}

	if !passedCheck {
		fmt.Println()
	}

	return passedCheck
}

// checkMultipleEpicsInCommits checks for commits that contain multiple epic references and adds a
// warning that it is not a common case and to check it is intended.
//
// Returns:
// - True if the check passed. False if it failed.
func checkMultipleEpicsInCommits(commitInfos []commitInfo, params *parameters) bool {
	passedCheck := true
	for _, ci := range commitInfos {
		if len(ci.epicRefs) > 1 {
			fmt.Printf(
				"Warning: This commit references multiple epics. Normally a commit only references one epic. "+
					"Noting this to verify this commit relates to multiple epics. "+
					"epic_refs=[%v], commit: %s\n\n",
				ci.epicRefs,
				commitShaURL(ci.sha, params),
			)
			passedCheck = false
		}
	}

	return passedCheck
}

func lintEpicAndIssueRefs(ctx context.Context, ghClient *github.Client, params *parameters) error {
	commitInfos, err := scanCommitsForEpicAndIssueRefs(ctx, ghClient, params)
	if err != nil {
		return err
	}

	pr, _, err := ghClient.PullRequests.Get(
		ctx, params.Org, params.Repo, params.PrNumber,
	)
	if err != nil {
		// nolint:errwrap
		return fmt.Errorf("error getting pull requests from GitHub: %v", err)
	}
	prBody := pr.GetBody()

	// Skip checking multi-PR backports for now. Verifying them is complex and the source PRs
	// have already been checked.
	if isMultiPrBackport(prBody) {
		// TODO(jamesl): handle backport PRs containing multiple source PRs differently
		// How might they be handled?
		// - check that all source PR bodies have epic or issue refs?
		// - figure out which commits in this PR correspond to the commits in the source PRs and ...
		// - check all source PRs have all the expected references a PR should have. Do something
		//   with that info, like maybe link it back to the commits in this PR and report on it
		//   somehow??? (This seems really heavy / not useful.)
		// - don't check backports with multiple source PRs for the complex cases like
		//   "checkPrEpicsUsedInCommits"?
		// - these checks are really for making it possible for the automated docs issue generation to
		//   get the right epic(s) applied to them. Maybe make this a super simple check and then add
		//   info to the generated docs issue stating it needs to be reviewed more. This case shouldn't
		//   happen too often. The majority of backports are for a single source PR.
		fmt.Println("This is a multi-PR backport. Skipping checking it.")
		return nil
	}

	var prInfo = prBodyInfo{
		epicRefs:        extractEpicIDs(prBody),
		epicNone:        extractEpicNone(prBody),
		issueCloseRefs:  extractFixIssueIDs(prBody),
		issueInformRefs: extractInformIssueIDs(prBody),
		prNumber:        params.PrNumber,
	}

	passedAllTests := checkForMissingRefs(prInfo, commitInfos, params) &&
		checkPrEpicsUsedInCommits(prInfo, commitInfos, params) &&
		checkMultipleEpicsInCommits(commitInfos, params)

	if !passedAllTests {
		fmt.Println("For more information about issue and epic references, see: https://wiki.crdb.io/wiki/spaces/CRDB/pages/2009039063/")
		return errors.New("one or more checks failed; see logs above")
	}

	// TODO:
	// - add some "try" messages for how they can fix the issue in question.
	return nil
}

func lintPR(params *parameters) error {
	ctx := context.Background()

	client := github.NewClient(oauth2.NewClient(ctx, oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: params.Token},
	)))

	return lintEpicAndIssueRefs(ctx, client, params)
}
