// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"encoding/json"
	"errors"
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
const commitTemplate = `%s: released CockroachDB version %s%s

Release note: None
Epic: None
Release justification: non-production (release infra) change.
`

const (
	releasedVersionFlag = "released-version"
	nextVersionFlag     = "next-version"
	versionBumpOnly     = "version-bump-only"
)

var updateVersionsFlags = struct {
	dryRun             bool
	releasedVersionStr string
	nextVersionStr     string
	templatesDir       string
	smtpUser           string
	smtpHost           string
	smtpPort           int
	emailAddresses     []string
	versionBumpOnly    bool
}{}

var updateVersionsCmd = &cobra.Command{
	Use:   "update-versions",
	Short: "Update CRDB version in various repos",
	Long:  "Updates CRDB version in various repos",
	RunE:  updateVersions,
}

func init() {
	updateVersionsCmd.Flags().BoolVar(&updateVersionsFlags.dryRun, dryRun, false, "print diff and emails without any side effects")
	updateVersionsCmd.Flags().BoolVar(&updateVersionsFlags.versionBumpOnly, versionBumpOnly, false, "only bump the version, do not create any other PRs")
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
	// pushToOrigin tells if the PR branch should be pushed to the origin repo.
	// This is required for the repos, where auto-merge is enabled in order to
	// grant proper permissions to the corresponding GitHub Actions.
	pushToOrigin bool
	// what branch should be used as the PR base
	branch         string
	commitMessage  string
	githubUsername string
	prBranch       string
	fn             func(gitDir string) error
	// workOnRepoError is set to workOnRepo() result
	workOnRepoError error
}

type metadata struct {
	PRs []string `json:"prs"`
}

func (r prRepo) String() string {
	return r.owner + "/" + r.repo + "@" + r.branch
}

func (r prRepo) name() string {
	return r.owner + "/" + r.repo
}

func (r prRepo) checkoutDir() string {
	return fmt.Sprintf("%s_%s_%s", r.owner, r.repo, r.prBranch)
}

func (r prRepo) pushURL() string {
	pushOwner := r.githubUsername
	if r.pushToOrigin {
		pushOwner = r.owner
	}

	if token := os.Getenv("GH_TOKEN"); token != "" {
		return fmt.Sprintf("https://%s:%s@github.com/%s/%s", r.githubUsername, token, pushOwner, r.repo)
	}
	return fmt.Sprintf("git@github.com:%s/%s.git", pushOwner, r.repo)
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
	cmd := exec.Command("git", "checkout", "-b", r.prBranch, remoteOrigin+"/"+r.branch)
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

func (r prRepo) commit() error {
	parts := []string{"git", "commit", "-a", "-m", r.commitMessage}
	cmd := exec.Command(parts[0], parts[1:]...)
	cmd.Dir = r.checkoutDir()
	out, err := cmd.CombinedOutput()
	if err != nil {
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

func (r prRepo) push() error {
	parts := []string{
		"git", "push", r.pushURL(), fmt.Sprintf("%s:%s", r.prBranch, r.prBranch),
	}
	cmd := exec.Command(parts[0], parts[1:]...)
	cmd.Dir = r.checkoutDir()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed pushing %s with message '%s': %w", r.prBranch, string(out), err)
	}
	log.Printf("changes pushed to %s: %s", r.name(), string(out))
	return nil
}

func (r prRepo) createPullRequest() (string, error) {
	head := fmt.Sprintf("%s:%s", r.githubUsername, r.prBranch)
	if r.pushToOrigin {
		head = r.prBranch
	}
	parts := []string{
		"gh", "pr", "create", "--base", r.branch, "--head", head,
	}
	title, body, _ := strings.Cut(r.commitMessage, "\n")
	if title == "" {
		parts = append(parts, "--fill")
	} else {
		parts = append(parts, "--title", title, "--body", body)
	}
	cmd := exec.Command(parts[0], parts[1:]...)
	log.Printf("creating PR by running `%s`", strings.Join(parts, " "))
	cmd.Dir = r.checkoutDir()
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed creating pull request via `%s` with message '%s': %w", cmd.String(), string(out), err)
	}
	log.Printf("PR created in %s: %s\n", r.name(), string(out))
	return strings.TrimSpace(string(out)), nil
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
	reposToWorkOn, err := generateRepoList(releasedVersion, nextVersion, latest, updateVersionsFlags.dryRun)
	if err != nil {
		return fmt.Errorf("generating repo list: %w", err)
	}
	// The first for loop combines all local commands that we can run without pushing the changes.
	// This way we can fail early and avoid unnecessary work closing the PRs we were able to create.
	log.Printf("repos to work on: %s\n", reposToWorkOn)
	var prs []string
	var workOnRepoErrors []error
	for _, repo := range reposToWorkOn {
		err := workOnRepo(repo)
		repo.workOnRepoError = err
		if repo.workOnRepoError != nil {
			err = fmt.Errorf("workOnRepo: error occurred while working on repo %s: %w", repo.name(), err)
			workOnRepoErrors = append(workOnRepoErrors, err)
			log.Printf("%s", err)
		}
	}

	// Now that our local changes are staged, we can try and publish them.
	for _, repo := range reposToWorkOn {
		if repo.workOnRepoError != nil {
			log.Printf("PR creation skipped due to previous errors while working on %s: %s", repo.name(), repo.workOnRepoError)
			continue
		}
		dest := path.Join(globalWorkDir, repo.checkoutDir())
		// We avoid creating duplicated PRs to allow this command to be
		// run multiple times.
		prDesc, err := repo.prExists()
		if err != nil {
			err = fmt.Errorf("error while checking if pull request exists for repo %s: %w", repo.name(), err)
			workOnRepoErrors = append(workOnRepoErrors, err)
			log.Printf("%s", err)
			continue
		}
		if prDesc != "" {
			log.Printf("pull request for %s already exists: %s", repo.name(), prDesc)
			continue
		}
		log.Printf("pushing changes to repo %s in %s", repo.name(), dest)
		if err := repo.push(); err != nil {
			err = fmt.Errorf("error while pushing changes to repo %s: %w", repo.name(), err)
			workOnRepoErrors = append(workOnRepoErrors, err)
			log.Printf("%s", err)
			continue
		}
		log.Printf("creating pull request for %s in %s", repo.name(), dest)
		pr, err := repo.createPullRequest()
		if err != nil {
			err = fmt.Errorf("error creating pull request for %s: %w", repo.name(), err)
			workOnRepoErrors = append(workOnRepoErrors, err)
			log.Printf("%s", err)
			continue
		}
		log.Printf("Created PR: %s\n", pr)
		prs = append(prs, pr)
	}

	if err := sendPrReport(releasedVersion, prs, smtpPassword); err != nil {
		err = fmt.Errorf("error sending email: %w", err)
		workOnRepoErrors = append(workOnRepoErrors, err)
		log.Printf("%s", err)
	}
	if artifactsDir != "" {
		if err := saveMetadata(artifactsDir, metadata{PRs: prs}); err != nil {
			err = fmt.Errorf("error saving metadata: %w", err)
			workOnRepoErrors = append(workOnRepoErrors, err)
			log.Printf("%s", err)
		}
	}
	if len(workOnRepoErrors) > 0 {
		return errors.Join(workOnRepoErrors...)
	}
	return nil
}

func saveMetadata(dir string, meta metadata) error {
	dest := path.Join(dir, "prs.json")
	log.Printf("saving metadata to %s", dest)
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling PR metadata: %w", err)
	}
	if err := os.WriteFile(dest, data, 0o644); err != nil {
		return fmt.Errorf("error writing PR metadata file: %w", err)
	}
	return nil
}

func sendPrReport(version *semver.Version, prs []string, smtpPassword string) error {
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
	if err := sendMailUpdateVersions(args, opts); err != nil {
		return fmt.Errorf("cannot send email: %w", err)
	}
	return nil
}

func randomString(n int) string {
	r := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	alphanumerics := []rune("abcdefghijklmnopqrstuvwxyz0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = alphanumerics[r.Intn(len(alphanumerics))]
	}
	return string(s)
}

func generateRepoList(
	releasedVersion *semver.Version, nextVersion *semver.Version, isLatest bool, dryRun bool,
) ([]prRepo, error) {
	owner := "cockroachdb"
	prefix := ""
	if dryRun {
		// For test/dry-run purposes, we need to create "base repos" and "forked repos". The PRs will be submitted against the
		// base repos and the branches will be created in the forked repos.
		// for repo in cockroach, homebrew-tap, helm-charts; do
		//   git clone --bare https://github.com/cockroachdb/$repo $repo
		//   git --git-dir $dir push --mirror git@github.com:crltest/$repo.git
		//   # fork the repo to crltest-$repo
		// done
		owner = "crltest"
		prefix = "crltest-"
	}
	// Repos we want to create PRs against
	var reposToWorkOn []prRepo

	// 1. Bump the version. Branches we need to bump the version on:
	// stable releases: release-major.minor and all RC branches of the same release series.
	// alpha, beta, rc: 1) master or 2) release-major.minor and all RC branches for the same release series
	maybeVersionBumpBranches, err := listRemoteBranches(fmt.Sprintf("release-%d.%d.*", releasedVersion.Major(), releasedVersion.Minor()))
	if err != nil {
		return []prRepo{}, fmt.Errorf("listing staging branches: %w", err)
	}
	if releasedVersion.Prerelease() == "" {
		maybeVersionBumpBranches = append(maybeVersionBumpBranches, fmt.Sprintf("release-%d.%d", releasedVersion.Major(), releasedVersion.Minor()))
	} else {
		// For alpha/betas/rc releases, if we have not created the dot-zero branch
		// (which is covered by the `release-major.minor.*` pattern), then use either the `release-major.minor` or the master branch for version bump.
		// First, try to find the `release-major.minor` branch.
		maybeReleaseBranches, err := listRemoteBranches(fmt.Sprintf("release-%d.%d", releasedVersion.Major(), releasedVersion.Minor()))
		if err != nil {
			return []prRepo{}, fmt.Errorf("listing release branches: %w", err)
		}
		if len(maybeReleaseBranches) > 1 {
			return []prRepo{}, fmt.Errorf("more than one release branch found: %q", maybeReleaseBranches)
		}
		if len(maybeReleaseBranches) == 1 {
			maybeVersionBumpBranches = append(maybeVersionBumpBranches, maybeReleaseBranches[0])
		}
		// if no staging/release branches found, fall back to the master branch.
		if len(maybeVersionBumpBranches) == 0 {
			maybeVersionBumpBranches = []string{"master"}
		}
	}
	log.Printf("will bump version in the following branches: %s", strings.Join(maybeVersionBumpBranches, ", "))

	for _, branch := range maybeVersionBumpBranches {
		// skip extraordinary branches
		if strings.HasPrefix(branch, "staging-") {
			log.Printf("not bumping version on staging branch %s", branch)
			continue
		}
		if branch == fmt.Sprintf("release-%s-rc", releasedVersion.String()) {
			log.Printf("not bumping version on the same branch %s", branch)
			continue
		}
		curVersion, err := fileContent(remoteOrigin+"/"+branch, versionFile)
		if err != nil {
			return []prRepo{}, fmt.Errorf("reading git file content: %w", err)
		}
		if strings.TrimSpace(curVersion) == nextVersion.Original() {
			log.Printf("skipping version bump on the %s branch, because the versions are the same", branch)
			continue
		}
		prBranch := fmt.Sprintf("update-versions-%s-%s-%s", branch, releasedVersion.Original(), randomString(4))
		commitMessagePrefix := "release"
		if branch != "master" {
			commitMessagePrefix = branch
		}
		repo := prRepo{
			owner:          owner,
			repo:           prefix + "cockroach",
			branch:         branch,
			prBranch:       prBranch,
			githubUsername: "cockroach-teamcity",
			commitMessage:  generateCommitMessage(commitMessagePrefix, releasedVersion, nextVersion),
			fn: func(gitDir string) error {
				return updateVersionFile(path.Join(gitDir, versionFile), nextVersion.Original())
			},
		}
		reposToWorkOn = append(reposToWorkOn, repo)
	}
	if updateVersionsFlags.versionBumpOnly {
		log.Println("Version bump only, skipping other PRs")
		return reposToWorkOn, nil
	}

	// 2. Brew. Update for all stable releases
	if releasedVersion.Prerelease() == "" {
		reposToWorkOn = append(reposToWorkOn, prRepo{
			owner:          owner,
			repo:           prefix + "homebrew-tap",
			pushToOrigin:   true,
			branch:         "master",
			githubUsername: "cockroach-teamcity",
			prBranch:       fmt.Sprintf("update-versions-%s-%s", releasedVersion.Original(), randomString(4)),
			commitMessage:  fmt.Sprintf("release: advance to %s", releasedVersion.Original()),
			fn: func(gitDir string) error {
				if dryRun {
					log.Printf("brew fetches and verifies the binaries, so it's likely it'll fail in dry-run mode. Skipping..")
					return nil
				}
				return updateBrew(gitDir, releasedVersion, isLatest)
			},
		})
	}
	// 3. Helm. Only for latest stable releases
	if isLatest && releasedVersion.Prerelease() == "" {
		reposToWorkOn = append(reposToWorkOn, prRepo{
			owner:          owner,
			repo:           prefix + "helm-charts",
			pushToOrigin:   true,
			branch:         "master",
			githubUsername: "cockroach-teamcity",
			prBranch:       fmt.Sprintf("update-versions-%s-%s", releasedVersion.Original(), randomString(4)),
			commitMessage:  fmt.Sprintf("release: advance to %s", releasedVersion.Original()),
			fn: func(gitDir string) error {
				return updateHelm(gitDir, releasedVersion.Original())
			},
		})
	}
	// 4. Orchestration. Only for latest stable releases
	if isLatest && releasedVersion.Prerelease() == "" {
		reposToWorkOn = append(reposToWorkOn, prRepo{
			owner:          owner,
			repo:           prefix + "cockroach",
			branch:         "master",
			githubUsername: "cockroach-teamcity",
			prBranch:       fmt.Sprintf("update-orchestration-versions-%s-%s", releasedVersion.Original(), randomString(4)),
			commitMessage:  generateCommitMessage("orchestration", releasedVersion, nextVersion),
			fn: func(gitDir string) error {
				return updateOrchestration(gitDir, releasedVersion.Original())
			},
		})
	}
	// 5. Merge baking branch back to the release branch.
	maybeBakingbranches := []string{
		fmt.Sprintf("release-%s-rc", releasedVersion.String()), // e.g. release-23.1.17-rc
		fmt.Sprintf("staging-v%s", releasedVersion.String()),   // e.g. staging-v23.1.17
	}
	var bakingBranches []string
	for _, branch := range maybeBakingbranches {
		maybeMergeBranches, err := listRemoteBranches(branch)
		if err != nil {
			return []prRepo{}, fmt.Errorf("listing merge branch %s: %w", branch, err)
		}
		bakingBranches = append(bakingBranches, maybeMergeBranches...)
	}
	if len(bakingBranches) > 1 {
		return []prRepo{}, fmt.Errorf("too many baking branches: %s", strings.Join(maybeBakingbranches, ", "))
	}
	// 6. Merge baking branch to the main release branch (e.g. release-25.1).
	// For pre-releases we may have no baking branches, thus we use `for` loop
	// to simplify the code.
	for _, mergeBranch := range bakingBranches {
		baseBranch := fmt.Sprintf("release-%d.%d", releasedVersion.Major(), releasedVersion.Minor())
		// Sometimes there are no changes on the baking/staging branches and a merge is not needed.
		alreadyOnBaseBranch, err := isAncestor(mergeBranch, baseBranch)
		if err != nil {
			return []prRepo{}, fmt.Errorf("checking if %s is ancestor of %s: %w", baseBranch, mergeBranch, err)
		}
		if alreadyOnBaseBranch {
			log.Printf("skipping merge of %s to %s, because %s is already an ancestor of %s", mergeBranch, baseBranch, mergeBranch, baseBranch)
			continue
		}
		// TODO: add a check to make sure the merge generates no unexpected
		// changes (we can ignore version.txt changes). The "ours" strategy
		// doesn't account for changes in the merge branch.
		commitMessage := generateCommitMessage(fmt.Sprintf("merge %s to %s", mergeBranch, baseBranch), releasedVersion, nextVersion)
		commitMessage += "\nDocs: noop merge\n"
		repo := prRepo{
			owner:          owner,
			repo:           prefix + "cockroach",
			branch:         baseBranch,
			prBranch:       fmt.Sprintf("merge-%s-to-%s-%s", mergeBranch, baseBranch, randomString(4)),
			githubUsername: "cockroach-teamcity",
			commitMessage:  commitMessage,
			fn: func(gitDir string) error {
				cmd := exec.Command("git", "merge", "-s", "ours", "--no-commit", remoteOrigin+"/"+mergeBranch)
				cmd.Dir = gitDir
				out, err := cmd.CombinedOutput()
				if err != nil {
					return fmt.Errorf("failed running '%s' with message '%s': %w", cmd.String(), string(out), err)
				}
				log.Printf("ran '%s': %s\n", cmd.String(), string(out))
				return nil
			},
		}
		reposToWorkOn = append(reposToWorkOn, repo)
	}
	// 7. Merge staging branch to the next release RC branch if it is present.
	for _, mergeBranch := range bakingBranches {
		// When we have extraordinary releases, we may have next release RC
		// branches created. Make sure we merge this branch to the RC branch
		// only if there are changes.
		if !strings.HasPrefix(mergeBranch, "staging-") {
			log.Printf("skipping merge of %s, because it's not a staging branch", mergeBranch)
			continue
		}
		nextRCBranch := fmt.Sprintf("release-%s-rc", nextVersion.String())
		maybeNextReleaseRCBranches, err := listRemoteBranches(nextRCBranch)
		if err != nil {
			return []prRepo{}, fmt.Errorf("listing rc branch %s: %w", nextRCBranch, err)
		}
		if len(maybeNextReleaseRCBranches) < 1 {
			log.Printf("no next release RC branches found, skipping merge to %s", nextRCBranch)
			continue
		}
		alreadyOnRCBranch, err := isAncestor(mergeBranch, nextRCBranch)
		if err != nil {
			return []prRepo{}, fmt.Errorf("checking if %s is ancestor of %s: %w", mergeBranch, nextRCBranch, err)
		}
		if alreadyOnRCBranch {
			log.Printf("skipping merge of %s to %s, because %s is already an ancestor of %s", mergeBranch, nextRCBranch, mergeBranch, nextRCBranch)
			continue
		}
		// try to merge and see if anything is changed, ignore version.txt changes.
		createsMergeCommit, err := mergeCreatesContentChanges(mergeBranch, nextRCBranch, []string{versionFile})
		if err != nil {
			return []prRepo{}, fmt.Errorf("checking if merge creates content changes: %w", err)
		}
		if !createsMergeCommit {
			log.Printf("skipping merge of %s to %s, because the merge does not create content changes", mergeBranch, nextRCBranch)
			continue
		}
		repo := prRepo{
			owner:          owner,
			repo:           prefix + "cockroach",
			branch:         nextRCBranch,
			prBranch:       fmt.Sprintf("merge-%s-to-%s-%s", mergeBranch, nextRCBranch, randomString(4)),
			githubUsername: "cockroach-teamcity",
			commitMessage:  generateCommitMessage(fmt.Sprintf("merge %s to %s", mergeBranch, nextRCBranch), releasedVersion, nextVersion),
			fn: func(gitDir string) error {
				cmd := exec.Command("git", "merge", "-X", "ours", "--strategy=recursive", "--no-commit", "--no-ff", remoteOrigin+"/"+mergeBranch)
				cmd.Dir = gitDir
				out, err := cmd.CombinedOutput()
				if err != nil {
					return fmt.Errorf("failed running '%s' with message '%s': %w", cmd.String(), string(out), err)
				}
				log.Printf("ran '%s': %s\n", cmd.String(), string(out))
				coCmd := exec.Command("git", "checkout", remoteOrigin+"/"+nextRCBranch, "--", versionFile)
				coCmd.Dir = gitDir
				out, err = coCmd.CombinedOutput()
				if err != nil {
					return fmt.Errorf("failed running '%s' with message '%s': %w", coCmd.String(), string(out), err)
				}
				log.Printf("ran '%s': %s\n", coCmd.String(), string(out))
				return nil
			},
		}
		reposToWorkOn = append(reposToWorkOn, repo)
	}
	return reposToWorkOn, nil
}

func workOnRepo(repo prRepo) error {
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
	if err := repo.commit(); err != nil {
		return fmt.Errorf("cannot commit changes in repo %s: %w", repo.name(), err)
	}

	return nil
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

func generateCommitMessage(prefix string, released, next *semver.Version) string {
	var nextVersionMsg string
	if hasVersionTxt(released) {
		nextVersionMsg = ". Next version: " + next.String()
	}
	return fmt.Sprintf(commitTemplate, prefix, released, nextVersionMsg)
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
