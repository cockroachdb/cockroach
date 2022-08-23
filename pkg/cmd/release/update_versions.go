// Copyright 2019 The Cockroach Authors.
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
	"fmt"
	"html/template"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/spf13/cobra"
)

const commitTemplate = `release: update CockroachDB version to %s

Release note: None
`

var updateVersionsFlags = struct {
	versionStr     string
	templatesDir   string
	smtpUser       string
	smtpHost       string
	smtpPort       int
	emailAddresses []string
}{}

var updateVersionsCmd = &cobra.Command{
	Use:   "update-versions",
	Short: "Update CRDB version in various repos",
	Long:  "Updates CRDB version in various repos",
	RunE:  updateVersions,
}

func init() {
	updateVersionsCmd.Flags().StringVar(&updateVersionsFlags.versionStr, versionFlag, "", "cockroachdb version")
	updateVersionsCmd.Flags().StringVar(&updateVersionsFlags.templatesDir, templatesDir, "", "templates directory")
	updateVersionsCmd.Flags().StringVar(&updateVersionsFlags.smtpUser, smtpUser, os.Getenv(envSMTPUser), "SMTP user name")
	updateVersionsCmd.Flags().StringVar(&updateVersionsFlags.smtpHost, smtpHost, "", "SMTP host")
	updateVersionsCmd.Flags().IntVar(&updateVersionsFlags.smtpPort, smtpPort, 0, "SMTP port")
	updateVersionsCmd.Flags().StringArrayVar(&updateVersionsFlags.emailAddresses, emailAddresses, []string{}, "email addresses")
	requiredFlags := []string{
		versionFlag,
		smtpUser,
		smtpHost,
		smtpPort,
		emailAddresses,
	}
	for _, flag := range requiredFlags {
		if err := updateVersionsCmd.MarkFlagRequired(flag); err != nil {
			panic(err)
		}
	}
	// Export some environment variables that affect git metadata.
	// Using environment variables instead of `git config` makes the changes temporary and doesn't touch any
	// configuration files.
	_ = os.Setenv("GIT_AUTHOR_NAME", "Justin Beaver")
	_ = os.Setenv("GIT_COMMITTER_NAME", "Justin Beaver")
	_ = os.Setenv("GIT_AUTHOR_EMAIL", "teamcity@cockroachlabs.com")
	_ = os.Setenv("GIT_COMMITTER_EMAIL", "teamcity@cockroachlabs.com")
}

type prRepo struct {
	owner string
	repo  string
	// what branch should be used as the PR base
	branch         string
	commitMessage  string
	githubUsername string
	prBranch       string
	// commands to run in order to modify the repo
	commands []*exec.Cmd
}

func (r prRepo) name() string {
	return r.owner + "/" + r.repo
}

func (r prRepo) pushURL() string {
	if token := os.Getenv("GH_TOKEN"); token != "" {
		return fmt.Sprintf("https://%s:%s@github.com/%s/%s", r.githubUsername, token, r.githubUsername, r.repo)
	}
	return fmt.Sprintf("git@github.com:%s/%s.git", r.githubUsername, r.repo)
}

func updateVersions(_ *cobra.Command, _ []string) error {
	smtpPassword := os.Getenv("SMTP_PASSWORD")
	if smtpPassword == "" {
		return fmt.Errorf("SMTP_PASSWORD environment variable should be set")
	}
	// make sure we have the leading "v" in the version
	updateVersionsFlags.versionStr = "v" + strings.TrimPrefix(updateVersionsFlags.versionStr, "v")
	version, err := semver.NewVersion(updateVersionsFlags.versionStr)
	if err != nil {
		return fmt.Errorf("cannot parse version %s: %w", updateVersionsFlags.versionStr, err)
	}
	dir, err := os.MkdirTemp("", "")
	if err != nil {
		return fmt.Errorf("cannot create a temporary directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	var prs []string
	repos, err := productionRepos(version)
	if err != nil {
		return fmt.Errorf("cannot list repos: %w", err)
	}
	// The first for loop combines all local commands that we can run without pushing the changes.
	// This way we can fail early and avoid unnecessary work closing the PRs we were able to create.
	for _, repo := range repos {
		dest := path.Join(dir, repo.name())
		fmt.Printf("Cloning repo %s to %s", repo.name(), dest)
		if err := cloneRepo(repo, dest); err != nil {
			return fmt.Errorf("cannot clone %s: %w", repo.name(), err)
		}
		fmt.Printf("Branching repo %s to %s", repo.name(), dest)
		if err := createBranch(repo, dest); err != nil {
			return fmt.Errorf("cannot create branch %s: %w", repo.name(), err)
		}
		fmt.Printf("Munging repo %s in %s", repo.name(), dest)
		if err := applyCommands(repo, dest); err != nil {
			return fmt.Errorf("cannot mutate repo %s: %w", repo.name(), err)
		}
		fmt.Printf("commiting changes to repo %s in %s", repo.name(), dest)
		if err := commitChanges(repo, dest); err != nil {
			return fmt.Errorf("cannot commit changes in repo %s: %w", repo.name(), err)
		}
	}
	// Now that our local changes are staged, we can try and publish them.
	for _, repo := range repos {
		dest := path.Join(dir, repo.name())
		fmt.Printf("pushing changes to repo %s in %s", repo.name(), dest)
		if err := pushChanges(repo, dest); err != nil {
			return fmt.Errorf("cannot push changes for %s: %w", repo.name(), err)
		}
		fmt.Printf("creating pull request for %s in %s", repo.name(), dest)
		pr, err := createPullRequest(repo, dest)
		if err != nil {
			return fmt.Errorf("cannot push changes for %s: %w", repo.name(), err)
		}
		fmt.Printf("Created PR: %s\n", pr)
		prs = append(prs, pr)
	}

	if err := sendPrReport(version, prs, smtpPassword); err != nil {
		return fmt.Errorf("cannot send email: %w", err)
	}
	return nil
}

func cloneRepo(repo prRepo, dest string) error {
	cmd := exec.Command("gh", "repo", "clone", repo.name(), dest)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed cloning %s with message '%s': %w", repo.name(), string(out), err)
	}
	fmt.Printf("cloned %s to %s: %s\n", repo.name(), dest, string(out))
	return nil
}

func createBranch(repo prRepo, dest string) error {
	cmd := exec.Command("git", "checkout", "-b", repo.prBranch, "origin/"+repo.branch)
	cmd.Dir = dest
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed running `%s` with output '%s': %w", cmd.String(), string(out), err)
	}
	fmt.Printf("created `%s` branch based on `%s` in `%s`: %s\n", repo.prBranch, repo.branch, dest, string(out))
	return nil
}

func applyCommands(repo prRepo, dest string) error {
	for _, cmd := range repo.commands {
		cmd.Dir = dest
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed running '%s' with message '%s': %w", cmd.String(), string(out), err)
		}
		fmt.Printf("ran '%s': %s\n", cmd.String(), string(out))
	}
	cmd := exec.Command("git", "diff")
	cmd.Dir = dest
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed diffing by '%s' with message '%s': %w", cmd.String(), string(out), err)
	}
	fmt.Printf("ran '%s':\n%s\n", cmd.String(), string(out))

	return nil
}

func commitChanges(repo prRepo, dest string) error {
	cmd := exec.Command("git", "commit", "-a", "-m", repo.commitMessage)
	cmd.Dir = dest
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed commiting to %s with message '%s': %w", repo.name(), string(out), err)
	}
	fmt.Printf("changes committed to %s in %s: %s\n", repo.name(), dest, string(out))
	return nil
}

func pushChanges(repo prRepo, dest string) error {
	cmd := exec.Command("git", "push", repo.pushURL(), fmt.Sprintf("%s:%s", repo.prBranch, repo.prBranch))
	cmd.Dir = dest
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed pushing %s with message '%s': %w", repo.prBranch, string(out), err)
	}
	fmt.Printf("changes pushed to %s in %s: %s\n", repo.name(), dest, string(out))
	return nil
}

func createPullRequest(repo prRepo, dest string) (string, error) {
	args := []string{"pr", "create", "--base", repo.branch, "--fill", "--head",
		fmt.Sprintf("%s:%s", repo.githubUsername, repo.prBranch)}
	cmd := exec.Command("gh", args...)
	cmd.Dir = dest
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed creating pull request via `%s` with message '%s': %w", cmd.String(), string(out), err)
	}
	fmt.Printf("PR created in %s in %s: %s\n", repo.name(), dest, string(out))
	return strings.TrimSpace(string(out)), nil
}

func sendPrReport(version *semver.Version, prs []string, smtpPassword string) error {
	fmt.Println("========================================================")
	fmt.Println("The following PRs are created:")
	for i, pr := range prs {
		fmt.Printf("%d. %s\n", i+1, pr)
	}
	fmt.Println("========================================================")
	args := messageDataUpdateVersions{
		Version: version.Original(),
	}
	for _, pr := range prs {
		args.PRs = append(args.PRs, template.URL(pr))
	}
	opts := sendOpts{
		templatesDir: updateVersionsFlags.templatesDir,
		from:         fmt.Sprintf(fromEmailFormat, updateVersionsFlags.smtpUser),
		host:         updateVersionsFlags.smtpHost,
		port:         updateVersionsFlags.smtpPort,
		user:         updateVersionsFlags.smtpUser,
		password:     smtpPassword,
		to:           updateVersionsFlags.emailAddresses,
	}
	fmt.Println("Sending email")
	if err := sendMailUpdateVersions(args, opts); err != nil {
		return fmt.Errorf("cannot send email: %w", err)
	}
	return nil
}
func randomString(n int) string {
	rand.Seed(timeutil.Now().UnixNano())
	var alphanumerics = []rune("abcdefghijklmnopqrstuvwxyz0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = alphanumerics[rand.Intn(len(alphanumerics))]
	}
	return string(s)
}

func productionRepos(version *semver.Version) ([]prRepo, error) {
	defaultCommitMessage := fmt.Sprintf(commitTemplate, version)
	// Add a random 4-letter string to the end of the branch name to make it unique.
	// This simplifies recovery in case something goes wrong with pushes or PR creation.
	defaultPrBranch := fmt.Sprintf("update-versions-%s-%s", version, randomString(4))
	latest, err := isLatestStableBranch(version)
	if err != nil {
		return []prRepo{}, fmt.Errorf("cannot get branch info: %w", err)
	}
	nextBranch, err := nextReleaseBranch(version)
	if err != nil {
		return []prRepo{}, fmt.Errorf("cannot get next branch info: %w", err)
	}
	fmt.Printf("Using %s as next branch for %s\n", nextBranch, version.Original())
	self, err := os.Executable()
	if err != nil {
		return []prRepo{}, fmt.Errorf("cannot get executable name")
	}
	crdbRepo := prRepo{
		owner: "cockroachdb",
		repo:  "cockroach",
		// the PR will be created against the "next" branch: for latest stable versions it is "master",
		// for other versions it is the next release version's branch.
		branch:         nextBranch,
		prBranch:       defaultPrBranch,
		githubUsername: "cockroach-teamcity",
		commitMessage:  defaultCommitMessage,
		commands: []*exec.Cmd{
			exec.Command(self, "update-version-map", "--version", version.Original()),
		},
	}
	if latest {
		// Combine 2 PRs (commands) in one. Applicable for latest stable releases only.
		crdbRepo.commands = append(crdbRepo.commands,
			exec.Command(self, "set-orchestration-version", "--template-dir", "./cloud/kubernetes/templates",
				"--output-dir", "./cloud/kubernetes/", "--version", version.Original()))
	}
	repos := []prRepo{crdbRepo}

	// Repos to change for the latest stable releases only
	if latest {
		repos = append(repos, prRepo{
			owner:          "cockroachdb",
			repo:           "homebrew-tap",
			branch:         "master",
			githubUsername: "cockroach-teamcity",
			prBranch:       defaultPrBranch,
			commitMessage:  defaultCommitMessage,
			commands: []*exec.Cmd{
				exec.Command("make", fmt.Sprintf("VERSION=%s", version), "PRODUCT=cockroach"),
			},
		})
		repos = append(repos, prRepo{
			owner:          "cockroachdb",
			repo:           "helm-charts",
			branch:         "master",
			githubUsername: "cockroach-teamcity",
			prBranch:       defaultPrBranch,
			commitMessage:  defaultCommitMessage,
			commands: []*exec.Cmd{
				exec.Command("bazel", "build", "//build"),
				exec.Command("sh", "-c", fmt.Sprintf("$(bazel info bazel-bin)/build/build_/build %s", version.Original())),
			},
		})
	}
	return repos, nil
}

func isLatestStableBranch(version *semver.Version) (bool, error) {
	// Here we ignore pre-releases (alphas and betas), because we still want to run these operations.
	// This way we exclude greater pre-release versions from this decision.
	latestRelease, err := findPreviousRelease("", true)
	if err != nil {
		return false, fmt.Errorf("cannot find latest version: %w", err)
	}
	fmt.Printf("The latest released version is %s\n", latestRelease)
	latestVersion, err := semver.NewVersion(latestRelease)
	if err != nil {
		return false, fmt.Errorf("cannot parse latest version: %w", err)
	}
	return version.GreaterThan(latestVersion), nil
}

func nextReleaseBranch(version *semver.Version) (string, error) {
	next := nextReleaseSeries(version)
	potentialReleaseBranch := "release-" + next
	cmd := exec.Command("git", "rev-parse", "--quiet", "--verify", "origin/"+potentialReleaseBranch)
	if err := cmd.Run(); err != nil {
		//nolint:returnerrcheck
		return "master", nil
	}
	return potentialReleaseBranch, nil
}
