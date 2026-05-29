// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"net/http"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/go-github/v61/github"
	"golang.org/x/oauth2"
)

// githubHTTPTimeout caps each go-github call. The oauth2-wrapped client
// from oauth2.NewClient has no timeout by default, so without this a
// wedged GitHub API would hang the cron run.
const githubHTTPTimeout = 30 * time.Second

// errBranchNotFound is returned by GetBranchSHA when the GitHub API responds
// with 404. Callers can use errors.Is to distinguish "the branch isn't
// there" from "the request itself failed" (network error, auth, 5xx).
var errBranchNotFound = errors.New("branch not found")

// githubClient is a thin wrapper around go-github exposing the operations
// the release-tooling workflows need: read a ref, create a ref, list
// branches, dispatch a workflow_dispatch event, and create a label.
type githubClient struct {
	client *github.Client
	owner  string
	repo   string
}

func newGitHubClient(token, owner, repo string) *githubClient {
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
	tc := oauth2.NewClient(context.Background(), ts)
	tc.Timeout = githubHTTPTimeout
	return &githubClient{
		client: github.NewClient(tc),
		owner:  owner,
		repo:   repo,
	}
}

// GetBranchSHA returns the commit SHA at the tip of the named branch. It
// returns errBranchNotFound (wrapped) when the GitHub API responds with 404
// so callers can distinguish "the branch is gone" from a transport-level
// failure with errors.Is.
func (c *githubClient) GetBranchSHA(ctx context.Context, branch string) (string, error) {
	ref, resp, err := c.client.Git.GetRef(ctx, c.owner, c.repo, "refs/heads/"+branch)
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return "", errors.Wrapf(errBranchNotFound, "get ref refs/heads/%s", branch)
		}
		return "", errors.Wrapf(err, "get ref refs/heads/%s", branch)
	}
	if ref.Object == nil || ref.Object.SHA == nil {
		return "", errors.Newf("ref refs/heads/%s missing object SHA", branch)
	}
	return *ref.Object.SHA, nil
}

// CreateBranch creates refs/heads/{branch} pointing at sha.
func (c *githubClient) CreateBranch(ctx context.Context, branch, sha string) error {
	ref := &github.Reference{
		Ref:    github.String("refs/heads/" + branch),
		Object: &github.GitObject{SHA: github.String(sha)},
	}
	if _, _, err := c.client.Git.CreateRef(ctx, c.owner, c.repo, ref); err != nil {
		return errors.Wrapf(err, "create ref refs/heads/%s at %s", branch, sha)
	}
	return nil
}

// BranchExists reports whether the named branch exists. It uses the Git
// Refs API (one round-trip) rather than paginating ListBranches, which on
// cockroachdb/cockroach would be O(hundreds-of-branches).
func (c *githubClient) BranchExists(ctx context.Context, branch string) (bool, error) {
	if _, err := c.GetBranchSHA(ctx, branch); err != nil {
		if errors.Is(err, errBranchNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// DispatchWorkflow triggers a workflow_dispatch event on the named workflow
// file. The ref selects which branch/tag the workflow runs against; inputs
// are forwarded to the workflow's `inputs:` block. The GitHub API only
// accepts string-valued inputs for workflow_dispatch (booleans are coerced
// from "true"/"false"), so the input type is constrained to map[string]string
// at the wrapper boundary — callers stringify non-strings themselves.
func (c *githubClient) DispatchWorkflow(
	ctx context.Context, ref, workflowFileName string, inputs map[string]string,
) error {
	// go-github's request type is map[string]interface{} (the workflow_dispatch
	// REST API technically accepts other JSON scalars too, but GitHub coerces
	// everything to string on receipt). Promote our typed map into theirs at
	// the boundary so the wrapper's contract stays string-only.
	apiInputs := make(map[string]interface{}, len(inputs))
	for k, v := range inputs {
		apiInputs[k] = v
	}
	event := github.CreateWorkflowDispatchEventRequest{
		Ref:    ref,
		Inputs: apiInputs,
	}
	if _, err := c.client.Actions.CreateWorkflowDispatchEventByFileName(
		ctx, c.owner, c.repo, workflowFileName, event,
	); err != nil {
		return errors.Wrapf(err, "dispatch workflow %s on %s", workflowFileName, ref)
	}
	return nil
}

// CreateLabel creates a repository label. If the label already exists (HTTP
// 422 with "already_exists"), the call is treated as a no-op.
func (c *githubClient) CreateLabel(ctx context.Context, name, description, color string) error {
	label := &github.Label{
		Name:        github.String(name),
		Description: github.String(description),
		Color:       github.String(color),
	}
	_, _, err := c.client.Issues.CreateLabel(ctx, c.owner, c.repo, label)
	if err == nil {
		return nil
	}
	var errResp *github.ErrorResponse
	if errors.As(err, &errResp) && errResp.Response != nil &&
		errResp.Response.StatusCode == http.StatusUnprocessableEntity {
		// Treat "already_exists" as success — re-running the workflow on the
		// same release shouldn't fail just because the label survived.
		return nil
	}
	return errors.Wrapf(err, "create label %s", name)
}
