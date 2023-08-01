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
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path"
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
	fn       func(gitDir string) error
}

func (r prRepo) String() string {
	return r.owner + "/" + r.repo + "@" + r.branch
}

func (r prRepo) name() string {
	return r.owner + "/" + r.repo
}

func (r prRepo) checkoutDir() string {
	return fmt.Sprintf("%s_%s_%s", r.owner, r.repo, r.branch)
}

func (r prRepo) pushURL() string {
	if token := os.Getenv("GH_TOKEN"); token != "" {
		return fmt.Sprintf("https://%s:%s@github.com/%s/%s", r.githubUsername, token, r.githubUsername, r.repo)
	}
	return fmt.Sprintf("git@github.com:%s/%s.git", r.githubUsername, r.repo)
}

func (r prRepo) clone() error {
	cmd := exec.Command("gh", "repo", "clone", r.name(), r.checkoutDir())
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed cloning %s with message '%s': %w", r.name(), string(out), err)
	}
	log.Printf("cloned %s to %s: %s\n", r.name(), r.checkoutDir(), string(out))
	return nil
}

func (r prRepo) checkout() error {
	cmd := exec.Command("git", "checkout", "-b", r.prBranch, "origin/"+r.branch)
	cmd.Dir = r.checkoutDir()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed running `%s` with output '%s': %w", cmd.String(), string(out), err)
	}
	log.Printf("created `%s` branch based on `%s` in `%s`: %s\n", r.prBranch, r.branch, r.checkoutDir(), string(out))
	return nil
}

func (r prRepo) apply() error {
	if r.fn != nil {
		return r.fn(r.checkoutDir())
	}
	log.Printf("no functions to apply, skipping...")
	return nil
}

func (r prRepo) commit(dryRun bool) error {
	parts := []string{"git", "commit", "-a", "-m", r.commitMessage}
	cmd := exec.Command(parts[0], parts[1:]...)
	cmd.Dir = r.checkoutDir()
	out, err := cmd.CombinedOutput()
	if err != nil {
		if dryRun {
			log.Printf("commit failed in dry-run mode, ignoring the change")
			return nil
		}
		return fmt.Errorf("failed commiting to %s with message '%s': %w", r.name(), string(out), err)
	}
	log.Printf("changes committed to %s: %s\n", r.name(), string(out))
	return nil
}

// prExists checks whether a PR (represented as an instance of
// `prRepo`) already exists. Returns a description of the PR when it
// exists and any errors found in the process.
func (r prRepo) prExists() (string, error) {
	title := strings.Split(r.commitMessage, "\n")[0]
	log.Printf("checking if PR %q already exists", title)
	query := fmt.Sprintf("in:title %q", title)
	args := []string{
		"pr", "list", "--search", query, "--author", r.githubUsername, "--json", "number,title",
	}
	cmd := exec.Command("gh", args...)
	cmd.Dir = r.checkoutDir()
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

func (r prRepo) push(dryRun bool) error {
	parts := []string{
		"git", "push", r.pushURL(), fmt.Sprintf("%s:%s", r.prBranch, r.prBranch),
	}
	cmd := exec.Command(parts[0], parts[1:]...)
	cmd.Dir = r.checkoutDir()
	out, err := cmd.CombinedOutput()
	if err != nil {
		if dryRun {
			log.Printf("push failed in dry-run mode, ignoring the change")
			return nil
		}
		return fmt.Errorf("failed pushing %s with message '%s': %w", r.prBranch, string(out), err)
	}
	log.Printf("changes pushed to %s: %s", r.name(), string(out))
	return nil
}

func (r prRepo) createPullRequest(dryRun bool) (string, error) {
	parts := []string{"gh", "pr", "create", "--base", r.branch, "--fill", "--head",
		fmt.Sprintf("%s:%s", r.githubUsername, r.prBranch)}
	cmd := exec.Command(parts[0], parts[1:]...)
	log.Printf("creating PR by running `%s`", strings.Join(parts, " "))
	cmd.Dir = r.checkoutDir()
	out, err := cmd.CombinedOutput()
	if err != nil {
		if dryRun {
			log.Printf("pr create failed in dry-run mode, ignoring the change")
			return "DRY-RUN", nil
		}
		return "", fmt.Errorf("failed creating pull request via `%s` with message '%s': %w", cmd.String(), string(out), err)
	}
	log.Printf("PR created in %s: %s\n", r.name(), string(out))
	return strings.TrimSpace(string(out)), nil
}

func updateVersions(_ *cobra.Command, _ []string) error {
	dbUrl := os.Getenv("REGSERVER_DATABASE_URL")
	if dbUrl == "" {
		return fmt.Errorf("REGSERVER_DATABASE_URL environment variable should be set")
	}
	smtpPassword := os.Getenv("SMTP_PASSWORD")
	if smtpPassword == "" {
		return fmt.Errorf("SMTP_PASSWORD environment variable should be set")
	}
	releasedVersion, err := parseVersion(updateVersionsFlags.releasedVersionStr)
	if err != nil {
		return err
	}
	if updateVersionsFlags.nextVersionStr == "" {
		updateVersionsFlags.nextVersionStr, err = bumpVersion(updateVersionsFlags.releasedVersionStr)
		if err != nil {
			return fmt.Errorf("bumping version: %w", err)
		}
		log.Printf("Next version is not passed, using calculated value of %s", updateVersionsFlags.nextVersionStr)
	}
	nextVersion, err := parseVersion(updateVersionsFlags.nextVersionStr)
	if err != nil {
		return fmt.Errorf("parsing next version: %w", err)
	}

	globalWorkDir, err := os.MkdirTemp("", "")
	if err != nil {
		return fmt.Errorf("cannot create a temporary directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(globalWorkDir) }()

	latest, err := isLatestStableBranch(releasedVersion)
	if err != nil {
		return fmt.Errorf("finding latest: %w", err)
	}
	if latest {
		log.Printf("Assuming %s is the latest stable release", releasedVersion.Original())
	}
	// 1. Update the regserver. This is applicable to all releases, including pre-releases.
	if err := updateRegserver(dbUrl, releasedVersion, updateVersionsFlags.dryRun); err != nil {
		return fmt.Errorf("updating regserver: %w", err)
	}
	reposToWorkOn, err := generateRepoList(releasedVersion, nextVersion, latest, updateVersionsFlags.dryRun)
	if err != nil {
		return fmt.Errorf("generating repo list: %w", err)
	}
	var prs []string
	//repos, err := productionRepos(releasedVersion, nextVersion)
	//if err != nil {
	//	return fmt.Errorf("cannot list repos: %w", err)
	//}
	// The first for loop combines all local commands that we can run without pushing the changes.
	// This way we can fail early and avoid unnecessary work closing the PRs we were able to create.
	log.Printf("repos to work on: %s\n", reposToWorkOn)
	for _, repo := range reposToWorkOn {
		log.Printf("Cloning repo %s", repo.name())
		if err := repo.clone(); err != nil {
			return fmt.Errorf("cannot clone %s: %w", repo.name(), err)
		}
		log.Printf("Branching repo %s", repo.name())
		if err := repo.checkout(); err != nil {
			return fmt.Errorf("cannot create branch %s: %w", repo.name(), err)
		}
		log.Printf("Munging repo %s", repo.name())
		if err := repo.apply(); err != nil {
			return fmt.Errorf("cannot mutate repo %s: %w", repo.name(), err)
		}
		log.Printf("commiting changes to repo %s", repo.name())
		if err := repo.commit(updateVersionsFlags.dryRun); err != nil {
			return fmt.Errorf("cannot commit changes in repo %s: %w", repo.name(), err)
		}
	}

	//if updateVersionsFlags.dryRun {
	//	log.Println("Dry run mode in effect, stopping here...")
	//	return nil
	//}

	// Now that our local changes are staged, we can try and publish them.
	for _, repo := range reposToWorkOn {
		dest := path.Join(globalWorkDir, repo.checkoutDir())
		// We avoid creating duplicated PRs to allow this command to be
		// run multiple times.
		prDesc, err := repo.prExists()
		if err != nil {
			return err
		}
		if prDesc != "" {
			log.Printf("pull request for %s already exists: %s", repo.name(), prDesc)
			continue
		}
		log.Printf("pushing changes to repo %s in %s", repo.name(), dest)
		if err := repo.push(updateVersionsFlags.dryRun); err != nil {
			return fmt.Errorf("cannot push changes for %s: %w", repo.name(), err)
		}
		log.Printf("creating pull request for %s in %s", repo.name(), dest)
		pr, err := repo.createPullRequest(updateVersionsFlags.dryRun)
		if err != nil {
			return fmt.Errorf("cannot create pull request for %s: %w", repo.name(), err)
		}
		log.Printf("Created PR: %s\n", pr)
		prs = append(prs, pr)
	}

	if err := sendPrReport(releasedVersion, prs, smtpPassword, updateVersionsFlags.dryRun); err != nil {
		return fmt.Errorf("cannot send email: %w", err)
	}
	return nil
}

func sendPrReport(version *semver.Version, prs []string, smtpPassword string, dryRun bool) error {
	log.Println("========================================================")
	log.Println("The following PRs are created:")
	for i, pr := range prs {
		log.Printf("%d. %s\n", i+1, pr)
	}
	log.Println("========================================================")
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
	log.Println("Sending email")
	if err := sendMailUpdateVersions(args, opts, dryRun); err != nil {
		return fmt.Errorf("cannot send email: %w", err)
	}
	return nil
}

func randomString(n int) string {
	r := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	var alphanumerics = []rune("abcdefghijklmnopqrstuvwxyz0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = alphanumerics[r.Intn(len(alphanumerics))]
	}
	return string(s)
}

func generateRepoList(
	releasedVersion *semver.Version, nextVersion *semver.Version, isLatest bool, dryRun bool,
) ([]prRepo, error) {
	// for repo in cockroach, homebrew-tap, helm-charts; do
	//   git clone --bare https://github.com/cockroachdb/$repo $repo
	//   git --git-dir $dir push --mirror git@github.com:crltest/$repo.git
	// done
	owner := "cockroachdb"
	prefix := ""
	// TODO: use the following repos for dry run
	if dryRun {
		owner = "crltest"
		prefix = "crltest-"
	}
	// Repos we want to create PRs against
	var reposToWorkOn []prRepo

	// 2. Bump the version. Branches we need to bump the version on:
	// alpha, beta, rc: master and maybe release-major.minor.0
	// stable releases: release-major.minor
	var maybeVersionBumpBranches []string
	stabilizationBranch := fmt.Sprintf("release-%d.%d.0", releasedVersion.Major(), releasedVersion.Minor())
	stabilizationBranchExists, err := remoteBranchExists(stabilizationBranch)
	if err != nil {
		return []prRepo{}, fmt.Errorf("checking stabilization branch: %w", err)
	}
	if stabilizationBranchExists {
		maybeVersionBumpBranches = append(maybeVersionBumpBranches, stabilizationBranch)
	}
	if releasedVersion.Prerelease() == "" {
		maybeVersionBumpBranches = append(maybeVersionBumpBranches, fmt.Sprintf("release-%d.%d", releasedVersion.Major(), releasedVersion.Minor()))
	} else {
		maybeVersionBumpBranches = append(maybeVersionBumpBranches, "master")
	}

	for _, branch := range maybeVersionBumpBranches {
		ok, err := fileExistsInGit(branch, versionFile)
		if err != nil {
			return []prRepo{}, fmt.Errorf("checking version file: %w", err)
		}
		if !ok {
			log.Printf("skipping version bump on the %s branch, because %s does not exist on that branch", branch, versionFile)
			continue
		}
		prBranch := fmt.Sprintf("update-versions-%s-%s-%s", branch, releasedVersion.Original(), randomString(4))
		repo := prRepo{
			owner:          owner,
			repo:           prefix + "cockroach",
			branch:         branch,
			prBranch:       prBranch,
			githubUsername: "cockroach-teamcity",
			commitMessage:  generateCommitMessage(releasedVersion, nextVersion),
			fn: func(gitDir string) error {
				return updateVersionTxt(gitDir, nextVersion.Original(), versionFile)
			},
		}
		reposToWorkOn = append(reposToWorkOn, repo)
	}

	// 3. Brew. Update for all stable releases
	if releasedVersion.Prerelease() == "" {
		reposToWorkOn = append(reposToWorkOn, prRepo{
			owner:          owner,
			repo:           prefix + "homebrew-tap",
			branch:         "master",
			githubUsername: "cockroach-teamcity",
			prBranch:       fmt.Sprintf("update-versions-%s-%s", releasedVersion.Original(), randomString(4)),
			commitMessage:  generateCommitMessage(releasedVersion, nextVersion),
			fn: func(gitDir string) error {
				return updateBrew(gitDir, releasedVersion, isLatest, dryRun)
			},
		})
	}
	// 4. Helm. Only for latest stable releases
	if isLatest && releasedVersion.Prerelease() == "" {
		reposToWorkOn = append(reposToWorkOn, prRepo{
			owner:          owner,
			repo:           prefix + "helm-charts",
			branch:         "master",
			githubUsername: "cockroach-teamcity",
			prBranch:       fmt.Sprintf("update-versions-%s-%s", releasedVersion.Original(), randomString(4)),
			commitMessage:  generateCommitMessage(releasedVersion, nextVersion),
			fn: func(gitDir string) error {
				return updateHelm(gitDir, releasedVersion)
			},
		})
	}
	// 5. Orchestration. Only for latest stable releases
	if isLatest && releasedVersion.Prerelease() == "" {
		reposToWorkOn = append(reposToWorkOn, prRepo{
			owner:          owner,
			repo:           prefix + "cockroach",
			branch:         "master",
			githubUsername: "cockroach-teamcity",
			prBranch:       fmt.Sprintf("update-orchestration-versions-%s-%s", releasedVersion.Original(), randomString(4)),
			commitMessage:  generateCommitMessage(releasedVersion, nextVersion),
			fn: func(gitDir string) error {
				return updateOrchestration(gitDir, releasedVersion.Original())
			},
		})
	}
	// 6. Predecessor version, aka cockroach_releases.yaml
	// TODO: potentially move to a nightly job that submits a PR to update the files
	// TODO: this uses the compiled version instead of releasedVersion, may need to rework
	//reposToWorkOn = append(reposToWorkOn, prRepo{
	//	owner:         owner,
	//	repo:          prefix + "cockroach",
	//	branch:        "master",
	// githubUsername: "cockroach-teamcity",
	//	prBranch:      fmt.Sprintf("update-releases-versions-%s-%s", releasedVersion.Original(), randomString(4)),
	//	commitMessage: generateCommitMessage(releasedVersion, nextVersion),
	//	fn: func(gitDir string) error {
	//		return nil
	//	},
	//})
	return reposToWorkOn, nil
}

func productionRepos(released, next *semver.Version) ([]prRepo, error) {
	// Add a random 4-letter string to the end of the branch name to make it unique.
	// This simplifies recovery in case something goes wrong with pushes or PR creation.
	defaultPrBranch := fmt.Sprintf("update-versions-%s-%s", released, randomString(4))
	latest, err := isLatestStableBranch(released)
	if err != nil {
		return []prRepo{}, fmt.Errorf("cannot get branch info: %w", err)
	}
	//currentBranch, err := currentReleaseBranch(released)
	//if err != nil {
	//	return []prRepo{}, fmt.Errorf("cannot get current branch info: %w", err)
	//}
	nextBranch, err := nextReleaseBranch(released)
	if err != nil {
		return []prRepo{}, fmt.Errorf("cannot get next branch info: %w", err)
	}
	log.Printf("Using %s as next branch for %s\n", nextBranch, released.Original())
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
			commitMessage:  generateCommitMessage(released, next),
		}
	}

	// the PR will be created against the "next" branch: for latest stable versions it is "master",
	// for other versions it is the next release version's branch. The following logic combines all
	// changes in a single PR.
	updateVersionPr := newCrdbRepo(nextBranch)
	updateVersionPr.commands = []*exec.Cmd{}
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

	// Repos to change for the latest stable releases only
	// if latest {
	//repos = append(repos, prRepo{
	//	owner:          "cockroachdb",
	//	repo:           "homebrew-tap",
	//	branch:         "master",
	//	githubUsername: "cockroach-teamcity",
	//	prBranch:       defaultPrBranch,
	//	commitMessage:  generateCommitMessage(released, next),
	//	commands: []*exec.Cmd{
	//		exec.Command("make", fmt.Sprintf("VERSION=%s", released), "PRODUCT=cockroach"),
	//	},
	//})
	//repos = append(repos, prRepo{
	//	owner:          "cockroachdb",
	//	repo:           "helm-charts",
	//	branch:         "master",
	//	githubUsername: "cockroach-teamcity",
	//	prBranch:       defaultPrBranch,
	//	commitMessage:  generateCommitMessage(released, next),
	//	commands: []*exec.Cmd{
	//		exec.Command("bazel", "build", "//build"),
	//		exec.Command("sh", "-c", fmt.Sprintf("$(bazel info bazel-bin)/build/build_/build %s", released.Original())),
	//	},
	//})
	// }
	return repos, nil
}

func isLatestStableBranch(version *semver.Version) (bool, error) {
	// Here we ignore pre-releases (alphas and betas), because we still want to run these operations.
	// This way we exclude greater pre-release versions from this decision.
	latestRelease, err := findPreviousRelease("", true /* ignorePrerelease */)
	if err != nil {
		return false, fmt.Errorf("cannot find latest version: %w", err)
	}
	log.Printf("The latest released version is %s", latestRelease)
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

func generateCommitMessage(released, next *semver.Version) string {
	var nextVersionMsg string
	if hasVersionTxt(released) {
		nextVersionMsg = ". Next version: " + next.String()
	}
	return fmt.Sprintf(commitTemplate, released, nextVersionMsg)
}

// nextReleaseSeries parses the version and returns the next release series assuming we have 2 releases yearly
func nextReleaseSeries(version *semver.Version) string {
	nextMinor := version.IncMinor()
	// TODO(rail): revisit when we have more than 2 releases a year
	if nextMinor.Minor() > 2 {
		nextMinor = nextMinor.IncMajor()
		// IncMajor() resets all version parts to 0, thus we need to bump the minor part to match our version schema.
		nextMinor = nextMinor.IncMinor()
	}
	return fmt.Sprintf("%d.%d", nextMinor.Major(), nextMinor.Minor())
}
