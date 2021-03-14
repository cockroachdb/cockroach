// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package blathers

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/google/go-github/v32/github"
)

// blacklistedLogins contains a list of organization members to
// ignore in certain situations.
var blacklistedLogins = map[string]struct{}{
	"cockroach-teamcity":  struct{}{},
	"cockroach-oncall":    struct{}{},
	"cockroach-roachdash": struct{}{},
	"crl-monitor-roach":   struct{}{},
	"exalate-issue-sync":  struct{}{},
}

// listBuilder keeps track of action items needed to be done.
// This will be output as a GitHub list.
type listBuilder []string

func (lb listBuilder) String() string {
	return strings.Join([]string(lb), "\n")
}

func (lb listBuilder) add(item string) listBuilder {
	lb = append(lb, fmt.Sprintf("* %s", item))
	return lb
}

func (lb listBuilder) addf(item string, fmts ...interface{}) listBuilder {
	lb = append(lb, fmt.Sprintf("* %s", fmt.Sprintf(item, fmts...)))
	return lb
}

// HandleGithubWebhook handles a Github based webhook.
func (srv *blathersServer) HandleGithubWebhook(w http.ResponseWriter, r *http.Request) {
	ctx := WithRequestID(context.Background(), r.Header.Get("X-Github-Delivery"))
	t := time.Now()
	defer func() {
		writeLogf(ctx, "time: %s", time.Now().Sub(t))
	}()

	var payload []byte
	var err error
	// Validate the secret if one is provided.
	if srv.githubAppSecret == "" {
		payload, err = ioutil.ReadAll(r.Body)
	} else {
		payload, err = github.ValidatePayload(r, []byte(srv.githubAppSecret))
	}
	if err != nil {
		w.WriteHeader(400)
		fmt.Fprint(w, err.Error())
		writeLogf(ctx, "validate error: %s", err.Error())
		return
	}

	event, err := github.ParseWebHook(github.WebHookType(r), payload)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprint(w, err.Error())
		writeLogf(ctx, "parse error: %s", err.Error())
		return
	}
	switch event := event.(type) {
	case *github.IssuesEvent:
		if event.Installation == nil {
			w.WriteHeader(400)
			writeLogf(ctx, "no installation")
			return
		}
		err = srv.handleIssuesWebhook(ctx, event)
	case *github.PullRequestEvent:
		if event.Installation == nil {
			w.WriteHeader(400)
			writeLogf(ctx, "no installation")
			return
		}
		err = srv.handlePullRequestWebhook(ctx, event)
	case *github.StatusEvent:
		if event.Installation == nil {
			w.WriteHeader(400)
			writeLogf(ctx, "no installation")
			return
		}
		err = srv.handleStatusWebhook(ctx, event)
	case *github.PingEvent:
		fmt.Fprintf(w, "ok")
	}
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprint(w, err.Error())
		writeLogf(ctx, "[%s] error: %s", r.Header.Get("X-GitHub-Delivery"), err.Error())
		return
	}
	w.WriteHeader(200)
}

// handleStatusWebhook handles the status component of a webhook.
func (srv *blathersServer) handleStatusWebhook(
	ctx context.Context, event *github.StatusEvent,
) error {
	ctx = WithDebuggingPrefix(ctx, fmt.Sprintf("[Status][%s]", event.GetSHA()))
	writeLogf(ctx, "handling status update (%s, %s)", event.GetContext(), event.GetState())
	handler, ok := statusHandlers[handlerKey{context: event.GetContext(), state: event.GetState()}]
	if !ok {
		return nil
	}
	return handler(ctx, srv, event)
}

func silenceRequested(ls []*github.Label) bool {
	for _, l := range ls {
		if l.GetName() == "X-blathers-silence" {
			return true
		}
	}
	return false
}

type handlerKey struct {
	context string
	state   string
}

var statusHandlers = map[handlerKey]func(ctx context.Context, srv *blathersServer, event *github.StatusEvent) error{
	// This is disabled due to mixed feelings.
	// Alternative: https://teamcity.cockroachdb.com/profile.html?item=userNotifications.
	{"__GitHub CI (Cockroach)", "failure"}: func(ctx context.Context, srv *blathersServer, event *github.StatusEvent) error {
		ghClient := srv.getGithubClientFromInstallation(
			ctx,
			installationID(event.Installation.GetID()),
		)

		// So we have a commit SHA, get the PRs.
		opts := &github.PullRequestListOptions{}
		more := true
		numbers := []int{}
		for more && len(numbers) == 0 {
			prs, resp, err := ghClient.PullRequests.ListPullRequestsWithCommit(
				ctx,
				event.GetRepo().GetOwner().GetLogin(),
				event.GetRepo().GetName(),
				event.GetSHA(),
				opts,
			)
			if err != nil {
				return wrapf(ctx, err, "error fetching pull requests using ListPRsWithCommit")
			}

			for _, pr := range prs {
				if pr.GetState() != "open" {
					continue
				}
				if silenceRequested(pr.Labels) {
					continue
				}
				if pr.GetHead().GetSHA() != event.GetSHA() {
					writeLogf(ctx, "aborting - PR no longer has head - new head is %s", pr.GetHead().GetSHA())
					return nil
				}
				number := pr.GetNumber()
				numbers = append(numbers, number)
				// Only take the first one for now.
				break
			}

			more = resp.NextPage != 0
			if more {
				opts.Page = resp.NextPage
			}
		}

		// The previous API is experimental. Let's find the real one using a hackier way.
		// But this is also experimental....
		if len(numbers) == 0 {
			writeLogf(ctx, "sha %s: unable to find using ListPRsWithCommit, using fallback", event.GetSHA())
			opts = &github.PullRequestListOptions{
				State:     "open",
				Sort:      "updated",
				Direction: "desc",
				ListOptions: github.ListOptions{
					PerPage: 100,
				},
			}
			// Only process one page.
			prs, _, err := ghClient.PullRequests.List(
				ctx,
				event.GetRepo().GetOwner().GetLogin(),
				event.GetRepo().GetName(),
				opts,
			)
			if err != nil {
				return wrapf(ctx, err, "error fetching pull requests using List")
			}

			for _, pr := range prs {
				if silenceRequested(pr.Labels) {
					continue
				}
				if pr.GetHead().GetSHA() == event.GetSHA() {
					numbers = append(numbers, pr.GetNumber())
					// Only take the first one for now.
					break
				}
			}
		}

		writeLogf(ctx, "sha %s: found PRs %#v", event.GetSHA(), numbers)
		for _, number := range numbers {
			// Build the message to send.
			builder := githubPullRequestIssueCommentBuilder{
				reviewers: make(map[string]struct{}),
				githubIssueCommentBuilder: githubIssueCommentBuilder{
					owner:  event.GetRepo().GetOwner().GetLogin(),
					repo:   event.GetRepo().GetName(),
					number: number,
				},
			}

			builder.addParagraphf(
				":x: The [%s build](%s) has failed on [%s](%s).",
				event.GetContext(),
				event.GetTargetURL(),
				event.GetSHA()[:8],
				event.GetCommit().GetHTMLURL(),
			)

			if err := builder.finish(ctx, ghClient); err != nil {
				return wrapf(ctx, err, "#%d: failed to finish building issue comment", number)
			}
			writeLogf(ctx, "#%d: status updated", number)
		}
		writeLogf(ctx, "complete")
		return nil
	},
}

// handleIssuesWebhook handles the issue component of a webhook.
func (srv *blathersServer) handleIssuesWebhook(
	ctx context.Context, event *github.IssuesEvent,
) error {
	ctx = WithDebuggingPrefix(ctx, fmt.Sprintf("[Webhook][Issue #%d]", event.Issue.GetNumber()))
	writeLogf(ctx, "handling issues action: %s", event.GetAction())

	if event.Issue.PullRequestLinks != nil {
		writeLogf(ctx, "ignoring pull requests")
		return nil
	}

	switch event.GetAction() {
	case "opened":
		return srv.handleIssueOpened(ctx, event)
	case "labeled":
		return srv.handleIssueLabelled(ctx, event)
	default:
		writeLogf(ctx, "not an event we care about")
		return nil
	}
}

// handleIssueOpened handles the webhook event for opened issues.
func (srv *blathersServer) handleIssueOpened(ctx context.Context, event *github.IssuesEvent) error {
	ghClient := srv.getGithubClientFromInstallation(
		ctx,
		installationID(event.Installation.GetID()),
	)

	// These bots all have their own logic to label and tag issues.
	if _, isBlacklistedLogin := blacklistedLogins[event.GetSender().GetLogin()]; isBlacklistedLogin {
		writeLogf(ctx, "skipping issue %s as member %s is blacklisted", event.GetIssue(), event.GetSender().GetLogin())
		return nil
	}

	isMember, err := isOrgMember(
		ctx,
		ghClient,
		event.GetRepo().GetOwner().GetLogin(),
		event.GetSender().GetLogin(),
	)
	if err != nil {
		return err
	}

	issue := event.GetIssue()
	builder := githubIssueCommentBuilderFromEvent(event)
	authorName := event.Sender.GetLogin()
	body := issue.GetBody()
	if isMember {
		writeLogf(ctx, "Running org member routine")
		// Check to see if there is a missing C- label.
		foundCLabel := false
		foundALabel := false
		for _, l := range issue.Labels {
			if strings.HasPrefix(l.GetName(), "C-") {
				foundCLabel = true
			}
			if strings.HasPrefix(l.GetName(), "A-") {
				foundALabel = true
			}
		}
		if !foundCLabel {
			body = strings.ToLower(body)
			guessedCLabel := false
			// Try to guess if it's a bug.
			if strings.Contains(body, "bug") {
				builder.addLabel("C-bug")
				guessedCLabel = true
			} else if strings.Contains(body, "feature") || strings.Contains(body, "enhancement") || strings.Contains(body, "support") {
				builder.addLabel("C-enhancement")
				guessedCLabel = true
			}
			if guessedCLabel {
				builder.addParagraphf("Hi @%s, I've guessed the C-ategory of your issue "+
					"and suitably labeled it. Please re-label if inaccurate.",
					authorName)
			} else {
				builder.addParagraphf("Hi @%s, please add a C-ategory label to your issue. Check out "+
					"the [label system docs](https://go.crdb.dev/p/issue-labels).",
					authorName)
			}
			if !foundALabel {
				builder.addParagraph("While you're here, please consider adding an A- label to help " +
					"keep our repository tidy. ")
			}
			if err := builder.finish(ctx, ghClient); err != nil {
				return wrapf(ctx, err, "failed to finish building issue comment")
			}
		}
		return nil
	}

	builder.addLabel("O-community")

	builder.addParagraph("Hello, I am Blathers. I am here to help you get the issue triaged.")
	if strings.Contains(body, "Describe the problem") {
		builder.addParagraph("Hoot - a bug! Though bugs are the bane of my existence, rest assured the wretched thing will get the best of care here.")
		builder.addLabel("C-bug")
	} else if strings.Contains(body, "Is your feature request related to a problem? Please describe.") {
		builder.addLabel("C-enhancement")
	} else if strings.Contains(body, "What is your situation?") {
		builder.addLabel("C-investigation")
	} else {
		builder.addParagraph("It looks like you have not filled out the issue in the format of any of our templates. To best assist you, we advise you to use one of these [templates](https://github.com/cockroachdb/cockroach/tree/master/.github/ISSUE_TEMPLATE).")
	}

	participantToReasons, err := findRelevantUsersFromAttachedIssues(
		ctx,
		ghClient,
		event.GetRepo().GetOwner().GetLogin(),
		event.GetRepo().GetName(),
		event.Issue.GetNumber(),
		issue.GetBody(),
		event.GetSender().GetLogin(),
	)
	if err != nil {
		return wrapf(ctx, err, "failed to find relevant users")
	}

	teamsToKeywords := findTeamsFromKeywords(issue.GetBody())
	if len(teamsToKeywords) > 0 {
		var teams []string
		for team := range teamsToKeywords {
			teams = append(teams, team)
		}
		teamsToProjects, err := findProjectsForTeams(ctx, ghClient, teams)
		if err != nil {
			return wrapf(ctx, err, "error finding relevant projects")
		}
		for team, keywords := range teamsToKeywords {
			if projectID, ok := teamsToProjects[team]; ok {
				builder.addProject(projectID)
			}
			for _, owner := range teamToContacts[team] {
				participantToReasons[owner] = append(
					participantToReasons[owner],
					fmt.Sprintf("found keywords: %s", strings.Join(keywords, ",")),
				)
			}
		}
	}

	if len(participantToReasons) == 0 {
		writeLogf(ctx, "resorting to support oncall")
		oncalls, err := srv.FindSupportOncall(ctx, ghClient, event.GetRepo().GetOwner().GetLogin())
		if err != nil {
			writeLogf(ctx, "failed to find support oncall: %s", err.Error())
		} else {
			for _, oncall := range oncalls {
				participantToReasons[oncall] = append(
					participantToReasons[oncall],
					fmt.Sprintf("member of the technical support engineering team"),
				)
			}
			if len(oncalls) > 0 {
				builder.addLabel("X-blathers-oncall")
			}
		}
	} else {
		builder.addLabel("X-blathers-triaged")
	}

	if len(participantToReasons) == 0 {
		builder.addLabel("X-blathers-untriaged")
		builder.addParagraph(`I was unable to automatically find someone to ping.`)
	} else {
		var assignedReasons listBuilder
		for author, reasons := range participantToReasons {
			assignedReasons = assignedReasons.addf("@%s (%s)", author, strings.Join(reasons, ", "))
			// Adding an assignee is very aggressive. Blathers is a benevolent owl.
			// builder.addAssignee(author)
		}
		builder.addParagraphf("I have CC'd a few people who may be able to assist you:\n%s", assignedReasons.String())
	}
	builder.addParagraphf(`If we have not gotten back to your issue within a few business days, you can try the following:
* Join our [community slack channel](https://go.crdb.dev/p/slack) and ask on #cockroachdb.
* Try find someone from [here](https://github.com/orgs/cockroachdb/people) if you know they worked closely on the area and CC them.`)

	builder.setMustComment(true)
	return builder.finish(ctx, ghClient)
}

// handleIssueLabelled handles changes to issue labels.
func (srv *blathersServer) handleIssueLabelled(
	ctx context.Context, event *github.IssuesEvent,
) error {
	ghClient := srv.getGithubClientFromInstallation(ctx, installationID(event.Installation.GetID()))
	builder := githubIssueCommentBuilderFromEvent(event)

	isReleaseBlockerAdd := false
	foundBranchLabel := false
	for _, l := range event.GetIssue().Labels {
		if strings.HasPrefix(l.GetName(), "branch-") {
			foundBranchLabel = true
		}
	}
	if label := event.GetLabel(); label != nil {
		if label.GetName() == "release-blocker" {
			isReleaseBlockerAdd = true
		}
	}

	if foundBranchLabel || !isReleaseBlockerAdd {
		return nil
	}

	builder.addParagraphf(
		"Hi @%s, please add branch-* labels to identify which branch(es) this release-blocker affects.",
		event.Sender.GetLogin(),
	).setMustComment(true)

	if err := builder.finish(ctx, ghClient); err != nil {
		return wrapf(ctx, err, "failed to finish building issue comment")
	}
	return nil
}

func overrideStatus(currentState, nextState string) bool {
	switch currentState {
	case "APPROVED":
		return nextState != "COMMENTED"
	case "CHANGES_REQUESTED":
		return nextState == "APPROVED" || nextState == "DISMISSED"
	}
	return true
}

// handlePullRequestWebhook handles the pull request component
// of a webhook.
func (srv *blathersServer) handlePullRequestWebhook(
	ctx context.Context, event *github.PullRequestEvent,
) error {
	ctx = WithDebuggingPrefix(ctx, fmt.Sprintf("[Webhook][PR #%d]", event.GetNumber()))
	writeLogf(ctx, "handling pull request action: %s", event.GetAction())

	// We only care about requests being opened, or new PR updates.
	switch event.GetAction() {
	case "opened", "synchronize":
	default:
		writeLogf(ctx, "not an event we care about")
		return nil
	}

	ghClient := srv.getGithubClientFromInstallation(
		ctx,
		installationID(event.Installation.GetID()),
	)

	isMember, err := isOrgMember(
		ctx,
		ghClient,
		event.GetRepo().GetOwner().GetLogin(),
		event.GetSender().GetLogin(),
	)
	if err != nil {
		return err
	}

	// Request reviews from people who have previously commented.
	if event.GetAction() == "synchronize" {
		reviews, err := getReviews(
			ctx,
			ghClient,
			event.GetRepo().GetOwner().GetLogin(),
			event.GetRepo().GetName(),
			event.GetNumber(),
		)
		if err != nil {
			return err
		}
		reviewersByState := make(map[string]string)
		for _, review := range reviews {
			state := review.GetState()
			// If the body contains lgtm, it's considered approved in CRDB land.
			lgtmRegex := regexp.MustCompile("(^i)lgtm(^s obtained)")
			if review.GetState() == "COMMENTED" && lgtmRegex.MatchString(review.GetBody()) {
				state = "APPROVED"
			}
			if currentState, ok := reviewersByState[review.GetUser().GetLogin()]; ok {
				if !overrideStatus(currentState, state) {
					continue
				}
			}
			reviewersByState[review.GetUser().GetLogin()] = state
		}

		reviewers := make([]string, 0, len(reviewersByState))
		for reviewer, state := range reviewersByState {
			if reviewer == event.GetSender().GetLogin() {
				continue
			}
			if reviewer == event.GetPullRequest().GetUser().GetLogin() {
				continue
			}
			if state == "CHANGES_REQUESTED" || state == "COMMENTED" {
				// Let's only do this for @otan & @irfansharif for now.
				if reviewer == "otan" || reviewer == "irfansharif" {
					reviewers = append(reviewers, reviewer)
				}
			}
		}

		if len(reviewers) > 0 {
			writeLogf(ctx, "adding reviewers after found reviewers %#v for #%d", reviewers, event.GetNumber())
			_, _, err = ghClient.PullRequests.RequestReviewers(
				ctx,
				event.GetRepo().GetOwner().GetLogin(),
				event.GetRepo().GetName(),
				event.GetNumber(),
				github.ReviewersRequest{
					Reviewers: reviewers,
				},
			)
			if err != nil {
				return err
			}
		}
	}

	if isMember {
		writeLogf(ctx, "skipping rest as member is part of organization")
		return nil
	}

	if _, isBlacklistedLogin := blacklistedLogins[event.GetSender().GetLogin()]; isBlacklistedLogin {
		writeLogf(ctx, "skipping as member %s is blacklisted", event.GetSender().GetLogin())
		return nil
	}

	builder := githubPullRequestIssueCommentBuilder{
		reviewers: make(map[string]struct{}),
		githubIssueCommentBuilder: githubIssueCommentBuilder{
			labels: map[string]struct{}{},
			owner:  event.GetRepo().GetOwner().GetLogin(),
			repo:   event.GetRepo().GetName(),
			number: event.GetNumber(),
		},
	}

	builder.addLabel("O-community")

	// Send guidelines.
	if event.GetSender().GetLogin() == "otan" {
		builder.addParagraph("Welcome back, creator. Thank you for testing me.")
	} else {
		switch event.GetAction() {
		case "opened":
			builder.setMustComment(true)
			if event.GetPullRequest().GetAuthorAssociation() == "FIRST_TIME_CONTRIBUTOR" {
				builder.addParagraph("Thank you for contributing your first PR! Please ensure you have read the instructions for [creating your first PR](https://wiki.crdb.io/wiki/spaces/CRDB/pages/181633464/Your+first+CockroachDB+PR]).")
			} else {
				builder.addParagraph("Thank you for contributing to CockroachDB. Please ensure you have followed the guidelines for [creating a PR](https://wiki.crdb.io/wiki/spaces/CRDB/pages/181633464/Your+first+CockroachDB+PR]).")
			}
		case "synchronize":
			builder.addParagraph("Thank you for updating your pull request.")
		}
	}

	// Build a list of action items that we can easily scan for.
	var ais listBuilder

	commits, err := listCommitsInPR(
		ctx,
		ghClient,
		event.GetRepo().GetOwner().GetLogin(),
		event.GetRepo().GetName(),
		event.GetNumber(),
	)
	if err != nil {
		return err
	}
	mustComment := false
	if len(commits) > 1 {
		ais = ais.add(
			`We notice you have more than one commit in your PR. We try break logical changes into separate commits, ` +
				`but commits such as "fix typo" or "address review commits" should be ` +
				`[squashed into one commit](https://github.com/wprig/wprig/wiki/How-to-squash-commits) and pushed with ` + "`--force`",
		)
	}
	for _, commit := range commits {
		if !strings.Contains(commit.GetCommit().GetMessage(), "Release note") {
			mustComment = true
			ais = ais.add("Please ensure your git commit message contains a [release note](https://wiki.crdb.io/wiki/spaces/CRDB/pages/186548364/Release+notes).")
			break
		}
	}

	builder.setMustComment(mustComment)
	if len(ais) == 0 {
		builder.addParagraph("My owl senses detect your PR is good for review. Please keep an eye out for any test failures in CI.")
	} else {
		ais = ais.add("When CI has completed, please ensure no errors have appeared.")
		builder.addParagraphf("Before a member of our team reviews your PR, I have some potential action items for you:\n%s", ais.String())
	}

	if len(event.GetPullRequest().RequestedReviewers) == 0 && len(event.GetPullRequest().RequestedTeams) == 0 {
		// If there are no requested reviewers, check whether there are any reviews.
		// If there have been, that means someone is already on it.
		if hasReviews, err := hasReviews(
			ctx,
			ghClient,
			event.GetRepo().GetOwner().GetLogin(),
			event.GetRepo().GetName(),
			event.GetNumber(),
		); err != nil {
			return err
		} else if !hasReviews {
			participantToReasons, err := findRelevantUsersFromAttachedIssues(
				ctx,
				ghClient,
				event.GetRepo().GetOwner().GetLogin(),
				event.GetRepo().GetName(),
				event.GetNumber(),
				event.GetPullRequest().GetBody(),
				event.GetSender().GetLogin(),
			)
			if err != nil {
				return wrapf(ctx, err, "failed to find relevant users")
			}
			if len(participantToReasons) == 0 {
				builder.addLabel("X-blathers-untriaged")
				builder.addParagraph(`I was unable to automatically find a reviewer. You can try CCing one of the following members:
* A person you worked with closely on this PR.
* The person who created the ticket, or a [CRDB organization member](https://github.com/orgs/cockroachdb/people) involved with the ticket (author, commenter, etc.).
* Join our [community slack channel](https://go.crdb.dev/p/slack) and ask on #contributors.
* Try find someone else from [here](https://github.com/orgs/cockroachdb/people).`)
			} else {
				builder.addLabel("X-blathers-triaged")
				builder.setMustComment(true)
				var reviewerReasons listBuilder
				for author, reasons := range participantToReasons {
					reviewerReasons = reviewerReasons.addf("@%s (%s)", author, strings.Join(reasons, ", "))
					builder.addReviewer(author)
				}
				builder.addParagraphf("I have added a few people who may be able to assist in reviewing:\n%s", reviewerReasons.String())
			}
		}
	}

	// We've compiled everything we want to happen. Send the message.
	if err := builder.finish(ctx, ghClient); err != nil {
		return wrapf(ctx, err, "failed to finish building issue comment")
	}
	writeLogf(ctx, "completed all checks")
	return nil
}
