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
	"net/http"
	"strings"

	"github.com/google/go-github/v32/github"
)

// githubIssueCommentBuilder handles building a GitHub issue comment.
type githubIssueCommentBuilder struct {
	paragraphs       []string
	labels           map[string]struct{}
	assignees        map[string]struct{}
	projectColumnIDs map[int64]struct{}

	mustComment bool
	owner       string
	repo        string
	id          int64
	number      int
}

func githubIssueCommentBuilderFromEvent(event *github.IssuesEvent) githubIssueCommentBuilder {
	return githubIssueCommentBuilder{
		labels: map[string]struct{}{},
		owner:  event.GetRepo().GetOwner().GetLogin(),
		repo:   event.GetRepo().GetName(),
		number: event.GetIssue().GetNumber(),
		id:     event.GetIssue().GetID(),
	}
}

func (icb *githubIssueCommentBuilder) addParagraph(paragraph string) *githubIssueCommentBuilder {
	icb.paragraphs = append(icb.paragraphs, paragraph)
	return icb
}

func (icb *githubIssueCommentBuilder) addLabel(label string) *githubIssueCommentBuilder {
	if icb.labels == nil {
		icb.labels = map[string]struct{}{}
	}
	icb.labels[label] = struct{}{}
	return icb
}

func (icb *githubIssueCommentBuilder) addAssignee(a string) *githubIssueCommentBuilder {
	if icb.assignees == nil {
		icb.assignees = map[string]struct{}{}
	}
	icb.assignees[a] = struct{}{}
	return icb
}

func (icb *githubIssueCommentBuilder) addProject(a int64) *githubIssueCommentBuilder {
	if icb.projectColumnIDs == nil {
		icb.projectColumnIDs = map[int64]struct{}{}
	}
	icb.projectColumnIDs[a] = struct{}{}
	return icb
}

func (icb *githubIssueCommentBuilder) hasComment(
	ctx context.Context, ghClient *github.Client, contains string,
) (bool, error) {
	sort := "created"
	direction := "desc"
	opts := &github.IssueListCommentsOptions{
		Sort:      &sort,
		Direction: &direction,
	}
	more := true
	for more {
		comments, resp, err := ghClient.Issues.ListComments(
			ctx,
			icb.owner,
			icb.repo,
			icb.number,
			opts,
		)
		if err != nil {
			return false, wrapf(ctx, err, "error getting listing issue comments for status update")
		}

		for _, comment := range comments {
			if comment.GetBody() == contains {
				return true, nil
			}
		}
		more = resp.NextPage != 0
		if more {
			opts.Page = resp.NextPage
		}
	}

	return false, nil
}

func (icb *githubIssueCommentBuilder) addParagraphf(
	paragraph string, args ...interface{},
) *githubIssueCommentBuilder {
	icb.paragraphs = append(icb.paragraphs, fmt.Sprintf(paragraph, args...))
	return icb
}

func (icb *githubIssueCommentBuilder) setMustComment(must bool) *githubIssueCommentBuilder {
	icb.mustComment = must
	return icb
}

func (icb *githubIssueCommentBuilder) finish(ctx context.Context, ghClient *github.Client) error {
	if len(icb.paragraphs) == 0 {
		return nil
	}
	icb.paragraphs = append(
		icb.paragraphs,
		"<sub>:owl: Hoot! I am a [Blathers](https://github.com/apps/blathers-crl), a bot for [CockroachDB](https://github.com/cockroachdb). My owner is [otan](https://github.com/otan).</sub>",
	)
	body := strings.Join(icb.paragraphs, "\n\n")
	if !icb.mustComment {
		// Check we haven't posted this exact comment before.
		hasComment, err := icb.hasComment(ctx, ghClient, body)
		if err != nil {
			return wrapf(ctx, err, "error finding a comment")
		}
		if hasComment {
			writeLogf(ctx, "comment already made; aborting")
			return nil
		}
	}
	_, _, err := ghClient.Issues.CreateComment(
		ctx,
		icb.owner,
		icb.repo,
		icb.number,
		&github.IssueComment{Body: &body},
	)
	if err != nil {
		return wrapf(ctx, err, "error creating a comment")
	}

	if len(icb.labels) > 0 {
		labels := make([]string, 0, len(icb.labels))
		for label := range icb.labels {
			labels = append(labels, label)
		}
		_, _, err := ghClient.Issues.AddLabelsToIssue(
			ctx,
			icb.owner,
			icb.repo,
			icb.number,
			labels,
		)
		if err != nil {
			return wrapf(ctx, err, "error adding labels")
		}
	}

	if len(icb.assignees) > 0 {
		assignees := make([]string, 0, len(icb.assignees))
		for assignee := range icb.assignees {
			assignees = append(assignees, assignee)
		}
		_, _, err := ghClient.Issues.AddAssignees(
			ctx,
			icb.owner,
			icb.repo,
			icb.number,
			assignees,
		)
		if err != nil {
			return wrapf(ctx, err, "error adding assignees")
		}
	}

	for colID := range icb.projectColumnIDs {
		_, _, err := ghClient.Projects.CreateProjectCard(ctx, colID, &github.ProjectCardOptions{
			ContentID:   icb.id,
			ContentType: "Issue",
		})
		if err != nil {
			return wrapf(ctx, err, "error creating project card")
		}
	}
	return nil
}

// findParticipants finds all participants belonging on the owner
// on a given issue.
// It returns a map of username -> participant text.
// Prioritized as "author" > "assigned" > "commented in issue".
func findParticipants(
	ctx context.Context, ghClient *github.Client, owner string, repo string, issueNum int,
) (map[string]string, error) {
	participants := make(map[string]string)
	addParticipant := func(author, reason string) {
		if _, ok := participants[author]; !ok {
			participants[author] = reason
		}
	}

	// Find author and assigned members of the issue.
	issue, _, err := ghClient.Issues.Get(ctx, owner, repo, issueNum)
	if err != nil {
		// Issue does not exist. We should not error here.
		if err, ok := err.(*github.ErrorResponse); ok && err.Response.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		return nil, wrapf(ctx, err, "error getting participant issue")
	}
	issueRef := fmt.Sprintf("%s/%s#%d", owner, repo, issueNum)
	addParticipant(issue.GetUser().GetLogin(), fmt.Sprintf("author of %s", issueRef))
	for _, assigned := range issue.Assignees {
		addParticipant(assigned.GetLogin(), fmt.Sprintf("assigned to %s", issueRef))
	}

	// Now find anyone who's commented
	opts := &github.IssueListCommentsOptions{}
	more := true
	for more {
		comments, resp, err := ghClient.Issues.ListComments(
			ctx,
			owner,
			repo,
			issueNum,
			opts,
		)
		if err != nil {
			return nil, wrapf(ctx, err, "error getting listing issue comments for findParticipants")
		}

		for _, comment := range comments {
			addParticipant(comment.GetUser().GetLogin(), fmt.Sprintf("commented on %s", issueRef))
		}
		more = resp.NextPage != 0
		if more {
			opts.Page = resp.NextPage
		}
	}

	return participants, nil
}

// findRelevantUsersFromAttachedIssues attempts to find relevant users based on a body of text.
func findRelevantUsersFromAttachedIssues(
	ctx context.Context,
	ghClient *github.Client,
	owner string,
	repo string,
	issueNum int,
	body string,
	blacklistUserLogin string,
) (map[string][]string, error) {

	mentionedIssues := findMentionedIssues(
		owner,
		repo,
		body,
	)
	participantToReasons := make(map[string][]string)
	for _, iss := range mentionedIssues {
		participantToReason, err := findParticipants(
			ctx,
			ghClient,
			owner,
			repo,
			iss.number,
		)
		if err != nil {
			return nil, err
		}
		for participant, reason := range participantToReason {
			participantToReasons[participant] = append(participantToReasons[participant], reason)
		}
	}

	// Filter out anyone not in the organization.
	orgMembers, err := getOrganizationLogins(ctx, ghClient, owner)
	if err != nil {
		return nil, err
	}
	for name := range participantToReasons {
		_, isOrgMember := orgMembers[name]
		_, isBlacklistedLogin := blacklistedLogins[name]
		if !isOrgMember || isBlacklistedLogin || name == blacklistUserLogin {
			delete(participantToReasons, name)
		}
	}
	return participantToReasons, nil
}
