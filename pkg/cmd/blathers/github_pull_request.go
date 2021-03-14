package blathers

import (
	"context"

	"github.com/google/go-github/v32/github"
)

// githubPullRequestIssueCommentBuilder wraps githubIssueCommentBuilder, adding PR-based issue
// only capabilities.
type githubPullRequestIssueCommentBuilder struct {
	reviewers map[string]struct{}
	githubIssueCommentBuilder
}

func (prb *githubPullRequestIssueCommentBuilder) addReviewer(reviewer string) {
	if prb.reviewers == nil {
		prb.reviewers = map[string]struct{}{}
	}
	prb.reviewers[reviewer] = struct{}{}
}

func (prb *githubPullRequestIssueCommentBuilder) finish(
	ctx context.Context, ghClient *github.Client,
) error {
	err := prb.githubIssueCommentBuilder.finish(ctx, ghClient)
	if err != nil {
		return err
	}

	if len(prb.reviewers) > 0 {
		reviewers := make([]string, 0, len(prb.reviewers))
		for reviewer := range prb.reviewers {
			reviewers = append(reviewers, reviewer)
		}

		_, _, err = ghClient.PullRequests.RequestReviewers(
			ctx,
			prb.owner,
			prb.repo,
			prb.number,
			github.ReviewersRequest{
				Reviewers: reviewers,
			},
		)
		if err != nil {
			return wrapf(ctx, err, "error adding reviewers")
		}
	}
	return nil
}

// listCommitsInPR lists all commits in a PR.
func listCommitsInPR(
	ctx context.Context, ghClient *github.Client, owner string, repo string, number int,
) ([]*github.RepositoryCommit, error) {
	more := true
	opts := &github.ListOptions{}
	var allCommits []*github.RepositoryCommit
	for more {
		commits, resp, err := ghClient.PullRequests.ListCommits(
			ctx,
			owner,
			repo,
			number,
			opts,
		)
		if err != nil {
			return nil, wrapf(ctx, err, "error found listing commits in PR")
		}
		allCommits = append(allCommits, commits...)
		more = resp.NextPage != 0
		if more {
			opts.Page = resp.NextPage
		}
	}
	return allCommits, nil
}

// hasReviews returns whether there are reviewers on a given commit.
func hasReviews(
	ctx context.Context, ghClient *github.Client, owner string, repo string, number int,
) (bool, error) {
	reviews, _, err := ghClient.PullRequests.ListReviews(
		ctx,
		owner,
		repo,
		number,
		&github.ListOptions{},
	)
	if err != nil {
		return false, wrapf(ctx, err, "erroring listing reviews")
	}
	return len(reviews) > 0, nil
}

// getReviews returns whether there are reviewers on a given commit.
func getReviews(
	ctx context.Context, ghClient *github.Client, owner string, repo string, number int,
) ([]*github.PullRequestReview, error) {
	more := true
	opts := &github.ListOptions{}
	var allReviews []*github.PullRequestReview
	for more {
		reviews, resp, err := ghClient.PullRequests.ListReviews(
			ctx,
			owner,
			repo,
			number,
			opts,
		)
		if err != nil {
			return nil, wrapf(ctx, err, "erroring listing reviews")
		}
		allReviews = append(allReviews, reviews...)
		more = resp.NextPage != 0
		if more {
			opts.Page = resp.NextPage
		}
	}
	return allReviews, nil
}
