// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/executil"
	"github.com/google/go-github/github"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"golang.org/x/oauth2"
)

const usage = `usage: backport [-c <commit>] [-r <release>] <pull-request>...
   or: backport [--continue|--abort]`

const helpString = `backport attempts to automatically backport GitHub pull requests to a
release branch.

By default, backport will cherry-pick all commits in the specified PRs.
If you explicitly list commits on the command line, backport will
cherry-pick only the mentioned commits.

If manual conflict resolution is required, backport will quit so you
can use standard Git commands to resolve the conflict. After you have
resolved the conflict, resume backporting with 'backport --continue'.
To give up instead, run 'backport --abort'.

To determine what Git remote to push to, backport looks at the value of
the cockroach.remote Git config option. You can set this option by
running 'git config cockroach.remote REMOTE-NAME'.

Options:

      --continue           resume an in-progress backport
      --abort              cancel an in-progress backport
  -c, --commit <commit>    only cherry-pick the mentioned commits
  -r, --release <release>  select release to backport to
      --help               display this help

Example invocations:

    $ backport 23437
    $ backport 23389 23437 -r 1.1 -c 00c6a87 -c a26506b
    $ backport --continue
    $ backport --abort`

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %s\n", err)

		cause := errors.Cause(err)
		if _, ok := cause.(*github.RateLimitError); ok {
			fmt.Fprintln(os.Stderr, `hint: unauthenticated GitHub requests are subject to a very strict rate
limit. Please configure backport with a personal access token:

			$ git config cockroach.githubToken TOKEN

For help creating a personal access token, see https://goo.gl/Ep2E6x.`)
		} else if err, ok := cause.(hintedErr); ok {
			fmt.Fprintf(os.Stderr, "hint: %s\n", err.hint)
		}

		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	var cont, abort, help bool
	var commits []string
	var release string

	pflag.Usage = func() { fmt.Fprintln(os.Stderr, usage) }
	pflag.BoolVarP(&help, "help", "h", false, "")
	pflag.BoolVar(&cont, "continue", false, "")
	pflag.BoolVar(&abort, "abort", false, "")
	pflag.StringArrayVarP(&commits, "commit", "c", nil, "")
	pflag.StringVarP(&release, "release", "r", "", "")
	pflag.Parse()

	if help {
		return runHelp(ctx)
	}

	if (cont || abort) && len(os.Args) != 2 {
		return errors.New(usage)
	}

	if cont {
		return runContinue(ctx)
	} else if abort {
		return runAbort(ctx)
	}
	return runBackport(ctx, pflag.Args(), commits, release)
}

func runHelp(ctx context.Context) error {
	fmt.Fprintln(os.Stderr, usage)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, helpString)
	return nil
}

func runBackport(ctx context.Context, prArgs, commitArgs []string, release string) error {
	if len(prArgs) == 0 {
		return runHelp(ctx)
	}

	var prNos []int
	for _, prArg := range prArgs {
		prNo, err := strconv.Atoi(prArg)
		if err != nil {
			return errors.Errorf("%q is not a valid pull request number", prArg)
		}
		prNos = append(prNos, prNo)
	}

	c, err := loadConfig(ctx)
	if err != nil {
		return err
	}

	if ok, err := isBackporting(c); err != nil {
		return err
	} else if ok {
		return errors.New("backport already in progress")
	}

	pullRequests, err := loadPullRequests(ctx, c, prNos)
	if err != nil {
		return err
	}

	if len(commitArgs) > 0 {
		if err := pullRequests.selectCommits(commitArgs); err != nil {
			return err
		}
	}

	if release == "" {
		release, err = getLatestRelease(ctx, c)
		if err != nil {
			return err
		}
	}

	releaseBranch := "release-" + release
	err = executil.Run("git", "fetch", "https://github.com/cockroachdb/cockroach.git",
		"refs/heads/"+releaseBranch)
	if err != nil {
		return errors.Wrapf(err, "fetching %q branch", releaseBranch)
	}

	backportBranch := fmt.Sprintf("backport%s-%s", release, strings.Join(prArgs, "-"))
	err = executil.Run("git", "checkout", "-B", backportBranch, "FETCH_HEAD")
	if err != nil {
		return errors.Wrapf(err, "creating backport branch %q", backportBranch)
	}

	query := url.Values{}
	query.Add("expand", "1")
	query.Add("title", pullRequests.title(release))
	query.Add("body", pullRequests.message())
	backportURL := fmt.Sprintf("https://github.com/cockroachdb/cockroach/compare/%s...%s:%s?%s",
		releaseBranch, c.username, backportBranch, query.Encode())

	err = ioutil.WriteFile(c.urlFile(), []byte(backportURL), 0644)
	if err != nil {
		return errors.Wrap(err, "writing url file")
	}

	err = executil.Run(append([]string{"git", "cherry-pick"}, pullRequests.selectedCommits()...)...)
	if err != nil {
		return hintedErr{
			error: err,
			hint: `Automatic cherry-picking failed. This usually indicates that manual
conflict resolution is required. Run 'backport --continue' to resume
backporting. To give up instead, run 'backport --abort'.`,
		}
	}

	return finalize(c, backportBranch, backportURL)
}

func runContinue(ctx context.Context) error {
	c, err := loadConfig(ctx)
	if err != nil {
		return err
	}

	if ok, err := isBackporting(c); err != nil {
		return err
	} else if !ok {
		return errors.New("no backport in progress")
	}

	if ok, err := isCherryPicking(c); err != nil {
		return err
	} else if ok {
		err = executil.Run("git", "cherry-pick", "--continue")
		if err != nil {
			return err
		}
	}

	in, err := ioutil.ReadFile(c.urlFile())
	if err != nil {
		return errors.Wrap(err, "reading url file")
	}
	backportURL := string(in)

	matches := regexp.MustCompile(`:(backport.*)\?`).FindStringSubmatch(backportURL)
	if len(matches) == 0 {
		return errors.Errorf("malformatted url file: %s", backportURL)
	}
	backportBranch := matches[1]

	return finalize(c, backportBranch, backportURL)
}

func runAbort(ctx context.Context) error {
	c, err := loadConfig(ctx)
	if err != nil {
		return err
	}

	if ok, err := isBackporting(c); err != nil {
		return err
	} else if !ok {
		return errors.New("no backport in progress")
	}

	err = os.Remove(c.urlFile())
	if err != nil {
		return errors.Wrap(err, "removing url file")
	}

	if ok, err := isCherryPicking(c); err != nil {
		return err
	} else if ok {
		err = executil.Run("git", "cherry-pick", "--abort")
		if err != nil {
			return err
		}
	}

	return checkoutPrevious()
}

func finalize(c config, backportBranch, backportURL string) error {
	err := executil.Run("git", "push", "--force", c.remote, fmt.Sprintf("%[1]s:%[1]s", backportBranch))
	if err != nil {
		return errors.Wrap(err, "pushing branch")
	}

	err = os.Remove(c.urlFile())
	if err != nil {
		return errors.Wrap(err, "removing url file")
	}

	err = executil.Run("python", "-m", "webbrowser", backportURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: unable to launch web browser: %s\n", err)
		fmt.Fprintf(os.Stderr, "Submit PR manually at:\n    %s\n", backportURL)
	}

	return checkoutPrevious()
}

func isCherryPicking(c config) (bool, error) {
	_, err := os.Stat(filepath.Join(c.gitDir, "CHERRY_PICK_HEAD"))
	if err == nil {
		return true, nil
	} else if !os.IsNotExist(err) {
		return false, errors.Wrap(err, "checking for in-progress cherry-pick")
	}
	return false, nil
}

func isBackporting(c config) (bool, error) {
	_, err := os.Stat(c.urlFile())
	if err == nil {
		return true, nil
	} else if !os.IsNotExist(err) {
		return false, errors.Wrap(err, "checking for in-progress backport")
	}
	return false, nil
}

func checkoutPrevious() error {
	branch, err := executil.Capture("git", "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return errors.Wrap(err, "looking up current branch name")
	}
	if !regexp.MustCompile(`^backport\d+`).MatchString(branch) {
		return nil
	}
	err = executil.Run("git", "checkout", "-")
	return errors.Wrap(err, "returning to previous branch")
}

type config struct {
	ghClient *github.Client
	remote   string
	username string
	gitDir   string
}

func loadConfig(ctx context.Context) (config, error) {
	var c config

	// Determine remote.
	c.remote, _ = executil.Capture("git", "config", "--get", "cockroach.remote")
	if c.remote == "" {
		return c, hintedErr{
			error: errors.New("missing cockroach.remote configuration"),
			hint: `set cockroach.remote to the name of the Git remote to push
backports to. For example:

    $ git config cockroach.remote origin
`,
		}
	}

	// Determine username.
	remoteURL, err := executil.Capture("git", "remote", "get-url", "--push", c.remote)
	if err != nil {
		return c, errors.Wrapf(err, "determining URL for remote %q", c.remote)
	}
	m := regexp.MustCompile(`github.com(:|/)([[:alnum:]\-]+)`).FindStringSubmatch(remoteURL)
	if len(m) != 3 {
		return c, errors.Errorf("unable to guess GitHub username from remote %q (%s)",
			c.remote, remoteURL)
	} else if m[2] == "cockroachdb" {
		return c, errors.Errorf("refusing to use unforked remote %q (%s)",
			c.remote, remoteURL)
	}
	c.username = m[2]

	// Build GitHub client.
	var ghAuthClient *http.Client
	ghToken, _ := executil.Capture("git", "config", "--get", "cockroach.githubToken")
	if ghToken != "" {
		ghAuthClient = oauth2.NewClient(ctx, oauth2.StaticTokenSource(
			&oauth2.Token{AccessToken: ghToken}))
	}
	c.ghClient = github.NewClient(ghAuthClient)

	// Determine Git directory.
	c.gitDir, err = executil.Capture("git", "rev-parse", "--git-dir")
	return c, errors.Wrap(err, "looking up git directory")
}

func (c config) urlFile() string {
	return filepath.Join(c.gitDir, "BACKPORT_URL")
}

func getLatestRelease(ctx context.Context, c config) (string, error) {
	opt := &github.ListOptions{PerPage: 100}
	var allBranches []*github.Branch
	for {
		branches, res, err := c.ghClient.Repositories.ListBranches(ctx, "cockroachdb", "cockroach", opt)
		if err != nil {
			return "", errors.Wrap(err, "discovering release branches")
		}
		allBranches = append(allBranches, branches...)
		if res.NextPage == 0 {
			break
		}
		opt.Page = res.NextPage
	}

	var lastRelease string
	for _, branch := range allBranches {
		if !strings.HasPrefix(branch.GetName(), "release-") {
			continue
		}
		lastRelease = strings.TrimPrefix(branch.GetName(), "release-")
	}
	if lastRelease == "" {
		return "", errors.New("unable to determine latest release; try specifying --release")
	}
	return lastRelease, nil
}

type pullRequest struct {
	number          int
	title           string
	body            string
	commits         []string
	selectedCommits []string
}

type pullRequests []pullRequest

func loadPullRequests(ctx context.Context, c config, prNos []int) (pullRequests, error) {
	var prs pullRequests
	for _, prNo := range prNos {
		ghPR, _, err := c.ghClient.PullRequests.Get(ctx, "cockroachdb", "cockroach", prNo)
		if err != nil {
			return nil, errors.Wrapf(err, "fetching PR #%d", prNo)
		}
		commits, _, err := c.ghClient.PullRequests.ListCommits(ctx, "cockroachdb", "cockroach", prNo, nil)
		if err != nil {
			return nil, errors.Wrapf(err, "fetching commits from PR #%d", prNo)
		}
		pr := pullRequest{
			number: prNo,
			title:  ghPR.GetTitle(),
			body:   ghPR.GetBody(),
		}
		for _, c := range commits {
			pr.commits = append(pr.commits, c.GetSHA())
			pr.selectedCommits = append(pr.selectedCommits, c.GetSHA())
		}
		prs = append(prs, pr)
	}
	return prs, nil
}

func (prs pullRequests) selectCommits(refs []string) error {
	for i := range prs {
		prs[i].selectedCommits = nil
	}

	for _, ref := range refs {
		var found bool
		for i := range prs {
			for _, commit := range prs[i].commits {
				if strings.HasPrefix(commit, ref) {
					if found {
						return errors.Errorf("commit ref %q is ambiguous", ref)
					}
					prs[i].selectedCommits = append(prs[i].selectedCommits, commit)
					found = true
				}
			}
		}
		if !found {
			return errors.Errorf("commit %q was not found in any of the specified PRs", ref)
		}
	}
	return nil
}

func (prs pullRequests) selectedCommits() []string {
	var commits []string
	for _, pr := range prs {
		commits = append(commits, pr.selectedCommits...)
	}
	return commits
}

func (prs pullRequests) selectedPRs() pullRequests {
	var selectedPRs []pullRequest
	for _, pr := range prs {
		if len(pr.selectedCommits) > 0 {
			selectedPRs = append(selectedPRs, pr)
		}
	}
	return selectedPRs
}

func (prs pullRequests) title(release string) string {
	prs = prs.selectedPRs()
	if len(prs) == 1 {
		return fmt.Sprintf("backport-%s: %s", release, prs[0].title)
	}
	return fmt.Sprintf("backport-%s: TODO", release)
}

func (prs pullRequests) message() string {
	prs = prs.selectedPRs()
	var s strings.Builder
	if len(prs) == 1 {
		fmt.Fprintf(&s, "Backport %d/%d commits from #%d.\n",
			len(prs[0].selectedCommits), len(prs[0].commits), prs[0].number)
	} else {
		fmt.Fprintln(&s, "Backport:")
		for _, pr := range prs {
			fmt.Fprintf(&s, "  * %d/%d commits from %q (#%d)\n",
				len(pr.selectedCommits), len(pr.commits), pr.title, pr.number)
		}
		fmt.Fprintln(&s)
		fmt.Fprintln(&s, "Please see individual PRs for details.")
	}
	fmt.Fprintln(&s)
	fmt.Fprintln(&s, "/cc @cockroachdb/release")
	if len(prs) == 1 {
		fmt.Fprintln(&s)
		fmt.Fprintln(&s, "---")
		fmt.Fprintln(&s)
		fmt.Fprintln(&s, prs[0].body)
	}
	return s.String()
}

type hintedErr struct {
	hint string
	error
}
