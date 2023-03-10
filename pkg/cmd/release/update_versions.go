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
	"encoding/json"
	"fmt"
	"html/template"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/spf13/cobra"
)

// first %s is the released version. Second is an optional message
// about the next released version for releases that include the
// version.txt file.
const commitTemplate = `release: released CockroachDB version %s%s

Release note: None
`

const releasedVersionFlag = "released-version"
const nextVersionFlag = "next-version"

var updateVersionsFlags = struct {
	dryRun             bool
	releasedVersionStr string
	nextVersionStr     string
	templatesDir       string
	smtpUser           string
	smtpHost           string
	smtpPort           int
	emailAddresses     []string
}{}

var updateVersionsCmd = &cobra.Command{
	Use:   "update-versions",
	Short: "Update CRDB version in various repos",
	Long:  "Updates CRDB version in various repos",
	RunE:  updateVersions,
}

func init() {
	updateVersionsCmd.Flags().BoolVar(&updateVersionsFlags.dryRun, dryRun, false, "print diff and emails without any side effects")
	updateVersionsCmd.Flags().StringVar(&updateVersionsFlags.releasedVersionStr, releasedVersionFlag, "", "released cockroachdb version")
	updateVersionsCmd.Flags().StringVar(&updateVersionsFlags.nextVersionStr, nextVersionFlag, "", "next cockroachdb version")
	updateVersionsCmd.Flags().StringVar(&updateVersionsFlags.templatesDir, templatesDir, "", "templates directory")
	updateVersionsCmd.Flags().StringVar(&updateVersionsFlags.smtpUser, smtpUser, os.Getenv(envSMTPUser), "SMTP user name")
	updateVersionsCmd.Flags().StringVar(&updateVersionsFlags.smtpHost, smtpHost, "", "SMTP host")
	updateVersionsCmd.Flags().IntVar(&updateVersionsFlags.smtpPort, smtpPort, 0, "SMTP port")
	updateVersionsCmd.Flags().StringArrayVar(&updateVersionsFlags.emailAddresses, emailAddresses, []string{}, "email addresses")
	requiredFlags := []string{
		releasedVersionFlag,
		nextVersionFlag,
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
	releasedVersion, err := parseVersion(updateVersionsFlags.releasedVersionStr)
	if err != nil {
		return err
	}
	nextVersion, err := parseVersion(updateVersionsFlags.nextVersionStr)
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "")
	if err != nil {
		return fmt.Errorf("cannot create a temporary directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	var prs []string
	repos, err := productionRepos(releasedVersion, nextVersion)
	if err != nil {
		return fmt.Errorf("cannot list repos: %w", err)
	}
	// The first for loop combines all local commands that we can run without pushing the changes.
	// This way we can fail early and avoid unnecessary work closing the PRs we were able to create.
	for _, repo := range repos {
		dest := path.Join(dir, repo.name())
		fmt.Printf("Cloning repo %s to %s\n", repo.name(), dest)
		if err := cloneRepo(repo, dest); err != nil {
			return fmt.Errorf("cannot clone %s: %w", repo.name(), err)
		}
		fmt.Printf("Branching repo %s to %s\n", repo.name(), dest)
		if err := createBranch(repo, dest); err != nil {
			return fmt.Errorf("cannot create branch %s: %w", repo.name(), err)
		}
		fmt.Printf("Munging repo %s in %s\n", repo.name(), dest)
		if err := applyCommands(repo, dest); err != nil {
			return fmt.Errorf("cannot mutate repo %s: %w", repo.name(), err)
		}
		fmt.Printf("commiting changes to repo %s in %s\n", repo.name(), dest)
		if err := commitChanges(repo, dest); err != nil {
			return fmt.Errorf("cannot commit changes in repo %s: %w", repo.name(), err)
		}
	}

	// Now that our local changes are staged, we can try and publish them.
	for _, repo := range repos {
		dest := path.Join(dir, repo.name())
		// We avoid creating duplicated PRs to allow this command to be
		// run multiple times.
		prDesc, err := prExists(repo, dest, releasedVersion, nextVersion)
		if err != nil {
			return err
		}
		if prDesc != "" {
			fmt.Printf("pull request for %s already exists: %s", repo.name(), prDesc)
			continue
		}
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

	if err := sendPrReport(releasedVersion, prs, smtpPassword); err != nil {
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
			msg := fmt.Sprintf("failed running '%s' with message '%s': %s", cmd.String(), string(out), err)
			// In dry-run mode, log the error message and continue.  It's
			// hard to make every command succeed in dry-run mode (as, for
			// instance, binaries or tags might not exist in remote
			// repositories). Logging the error message should be sufficient
			// since the engineer that triggered the dry-run will likely
			// monitor the output of the build pretty closely.
			if updateVersionsFlags.dryRun {
				fmt.Printf("dry-run: %s\n", msg)
				continue
			}
			return fmt.Errorf("%s", msg)
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
	parts := []string{"git", "commit", "-a", "-m", repo.commitMessage}
	if updateVersionsFlags.dryRun {
		fmt.Printf("dry-run: commitChanges: %s\n", strings.Join(parts, " "))
		return nil
	}
	cmd := exec.Command(parts[0], parts[1:]...)
	cmd.Dir = dest
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed commiting to %s with message '%s': %w", repo.name(), string(out), err)
	}
	fmt.Printf("changes committed to %s in %s: %s\n", repo.name(), dest, string(out))
	return nil
}

func pushChanges(repo prRepo, dest string) error {
	parts := []string{
		"git", "push", repo.pushURL(), fmt.Sprintf("%s:%s", repo.prBranch, repo.prBranch),
	}
	if updateVersionsFlags.dryRun {
		fmt.Printf("dry-run: pushChanges: %q", strings.Join(parts, " "))
		return nil
	}
	cmd := exec.Command(parts[0], parts[1:]...)
	cmd.Dir = dest
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed pushing %s with message '%s': %w", repo.prBranch, string(out), err)
	}
	fmt.Printf("changes pushed to %s in %s: %s\n", repo.name(), dest, string(out))
	return nil
}

func createPullRequest(repo prRepo, dest string) (string, error) {
	parts := []string{"gh", "pr", "create", "--base", repo.branch, "--fill", "--head",
		fmt.Sprintf("%s:%s", repo.githubUsername, repo.prBranch)}
	if updateVersionsFlags.dryRun {
		fmt.Printf("dry-run: createPullRequest: %q", strings.Join(parts, " "))
		return "DRY-RUN", nil
	}
	cmd := exec.Command(parts[0], parts[1:]...)
	cmd.Dir = dest
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed creating pull request via `%s` with message '%s': %w", cmd.String(), string(out), err)
	}
	fmt.Printf("PR created in %s in %s: %s\n", repo.name(), dest, string(out))
	return strings.TrimSpace(string(out)), nil
}

// prExists checks whether a PR (represented as an instance of
// `prRepo`) already exists. Returns a description of the PR when it
// exists and any errors found in the process.
func prExists(repo prRepo, dest string, released, next *semver.Version) (string, error) {
	title := strings.Split(updateCommitMessage(released, next), "\n")[0]
	fmt.Printf("checking if PR %q already exists\n", title)
	query := fmt.Sprintf("in:title %q", title)
	args := []string{
		"pr", "list", "--search", query, "--author", repo.githubUsername, "--json", "number,title",
	}
	cmd := exec.Command("gh", args...)
	cmd.Dir = dest
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed searching for existing PRs: %w\nOutput: %s", err, string(out))
	}

	var prs []struct {
		Number int    `json:"number"`
		Title  string `json:"title"`
	}
	if err := json.Unmarshal(out, &prs); err != nil {
		return "", fmt.Errorf("failed to parse PR search result: %w\nOutput: %s", err, string(out))
	}

	if len(prs) == 0 {
		return "", nil
	}

	return fmt.Sprintf("#%d: %s", prs[0].Number, prs[0].Title), nil
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
	if err := sendMailUpdateVersions(args, opts, updateVersionsFlags.dryRun); err != nil {
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

func productionRepos(released, next *semver.Version) ([]prRepo, error) {
	// Add a random 4-letter string to the end of the branch name to make it unique.
	// This simplifies recovery in case something goes wrong with pushes or PR creation.
	defaultPrBranch := fmt.Sprintf("update-versions-%s-%s", released, randomString(4))
	latest, err := isLatestStableBranch(released)
	if err != nil {
		return []prRepo{}, fmt.Errorf("cannot get branch info: %w", err)
	}
	nextBranch, err := nextReleaseBranch(released)
	if err != nil {
		return []prRepo{}, fmt.Errorf("cannot get next branch info: %w", err)
	}
	fmt.Printf("Using %s as next branch for %s\n", nextBranch, released.Original())
	self, err := os.Executable()
	if err != nil {
		return []prRepo{}, fmt.Errorf("cannot get executable name")
	}
	newCrdbRepo := func(branch string) prRepo {
		return prRepo{
			owner:          "cockroachdb",
			repo:           "cockroach",
			branch:         branch,
			prBranch:       defaultPrBranch,
			githubUsername: "cockroach-teamcity",
			commitMessage:  updateCommitMessage(released, next),
		}
	}

	// The roachtest predecessor file changed locations after a
	// refactoring that took place in the 23.1 cycle, so we account for
	// that difference here.
	// TODO(renato): remove this logic once we stop releasing anything
	// older than v23 releases.
	releaseBranchRe := regexp.MustCompile(`^release-(\d\d).\d.*`)
	updateRoachtestPred := func(branch string) *exec.Cmd {
		predecessorFile := "pkg/util/version/predecessor_version.json"
		if matches := releaseBranchRe.FindStringSubmatch(branch); matches != nil {
			major, _ := strconv.Atoi(matches[1])
			if major < 23 {
				predecessorFile = "pkg/cmd/roachtest/tests/predecessor_version.json"
			}
		}
		return exec.Command(self, "update-roachtest-predecessors",
			"--version", released.Original(), "--version-map-file", predecessorFile,
		)
	}

	// the PR will be created against the "next" branch: for latest stable versions it is "master",
	// for other versions it is the next release version's branch. The following logic combines all
	// changes in a single PR.
	updateVersionPr := newCrdbRepo(nextBranch)
	updateVersionPr.commands = []*exec.Cmd{updateRoachtestPred(nextBranch)}
	// Releases 23.{minor} and above include the version.txt file that
	// needs to be bumped when a release is published.
	// TODO(renato): remove this logic once we stop releasing anything
	// older than v23.
	if hasVersionTxt(released) {
		updateVersionPr.commands = append(
			updateVersionPr.commands,
			exec.Command(self, "set-cockroach-version", "--version", next.Original()),
		)
	}

	if latest {
		updateVersionPr.commands = append(updateVersionPr.commands,
			exec.Command(self, "set-orchestration-version", "--template-dir", "./cloud/kubernetes/templates",
				"--output-dir", "./cloud/kubernetes/", "--version", released.Original()))
	}

	repos := []prRepo{updateVersionPr}

	// If we are updating the predecessor version on a branch other than
	// master, then we need to update the predecessors on master as well.
	if nextBranch != "master" {
		updateVersionMaster := newCrdbRepo("master")
		updateVersionMaster.commands = []*exec.Cmd{updateRoachtestPred("master")}
		repos = append(repos, updateVersionMaster)
	}

	// Repos to change for the latest stable releases only
	if latest {
		repos = append(repos, prRepo{
			owner:          "cockroachdb",
			repo:           "homebrew-tap",
			branch:         "master",
			githubUsername: "cockroach-teamcity",
			prBranch:       defaultPrBranch,
			commitMessage:  updateCommitMessage(released, next),
			commands: []*exec.Cmd{
				exec.Command("make", fmt.Sprintf("VERSION=%s", released), "PRODUCT=cockroach"),
			},
		})
		repos = append(repos, prRepo{
			owner:          "cockroachdb",
			repo:           "helm-charts",
			branch:         "master",
			githubUsername: "cockroach-teamcity",
			prBranch:       defaultPrBranch,
			commitMessage:  updateCommitMessage(released, next),
			commands: []*exec.Cmd{
				exec.Command("bazel", "build", "//build"),
				exec.Command("sh", "-c", fmt.Sprintf("$(bazel info bazel-bin)/build/build_/build %s", released.Original())),
			},
		})
	}
	return repos, nil
}

func isLatestStableBranch(version *semver.Version) (bool, error) {
	// Here we ignore pre-releases (alphas and betas), because we still want to run these operations.
	// This way we exclude greater pre-release versions from this decision.
	latestRelease, err := findPreviousRelease("", true /* ignorePrerelease */)
	if err != nil {
		return false, fmt.Errorf("cannot find latest version: %w", err)
	}
	fmt.Printf("The latest released version is %s\n", latestRelease)
	latestVersion, err := semver.NewVersion(latestRelease)
	if err != nil {
		return false, fmt.Errorf("cannot parse latest version: %w", err)
	}
	// Check if the version we processing here is greater than or equal
	// to the latest known released version.
	return version.Compare(latestVersion) >= 0, nil
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

func parseVersion(versionStr string) (*semver.Version, error) {
	// make sure we have the leading "v" in the version
	leadingV := "v" + strings.TrimPrefix(versionStr, "v")
	version, err := semver.NewVersion(leadingV)
	if err != nil {
		return nil, fmt.Errorf("cannot parse version %s: %w", versionStr, err)
	}

	return version, nil
}

// hasVersionTxt returns whether a given version uses the version.txt
// file to determine binary version.
func hasVersionTxt(version *semver.Version) bool {
	return version.Major() >= 23
}

func updateCommitMessage(released, next *semver.Version) string {
	var nextVersionMsg string
	if hasVersionTxt(released) {
		nextVersionMsg = ". Next version: %s" + next.String()
	}
	return fmt.Sprintf(commitTemplate, released, nextVersionMsg)
}
