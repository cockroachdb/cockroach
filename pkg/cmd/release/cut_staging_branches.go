// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/version"
	"github.com/spf13/cobra"
)

// JQL used to find release tickets that are ready to have their staging
// branch cut: type=Release, parented (REL-217 holds historical/template
// tickets without parents that we don't want to act on), open, and missing
// a Staging Branch value.
const cutStagingJQL = `issueType="CRDB Release" and parent is not empty and ` +
	`status not in (done, cancelled, "Post-Publish Tasks") and ` +
	`"Staging Branch[Short text]" is empty`

// cutStagingJQLDryRun drops the `parent is not empty` filter so dry runs can
// rehearse against parent/template tickets (e.g. REL-217 children themselves
// being parents). We never act on those in production runs.
const cutStagingJQLDryRun = `issueType="CRDB Release" and ` +
	`status not in (done, cancelled, "Post-Publish Tasks") and ` +
	`"Staging Branch[Short text]" is empty`

// Custom field IDs in the cockroachlabs.atlassian.net "CRDB Release" project.
const (
	cfReleaseStatus       = "customfield_10584"
	cfCutBranchDate       = "customfield_10585"
	cfPickSHADate         = "customfield_10586"
	cfCloudReleaseNotes   = "customfield_10588"
	cfPublishBinary       = "customfield_10589"
	cfStagingBranch       = "customfield_10592"
	cfReleaseTypeOverride = "customfield_10507"
	cfBuildSHA            = "customfield_10590"
)

// Jira workflow transition IDs. These are project-wide and stable.
const (
	transitionBaking       = "151" // moves a release ticket into Baking status
	transitionSubtaskDone  = "51"  // marks a CRDB Sub-task as Done
	cutStagingSubtaskMatch = "Cut Staging Branch"
	// bakingStatusName is the workflow status name the ticket lands in after
	// transitionBaking. processCandidate compares against this to skip the
	// transition on partial-failure recovery (Jira rejects re-transitioning
	// to the current state).
	bakingStatusName = "Baking"
)

// Release types — used only to choose between the two Slack message
// templates. The values here are also accepted from Jira's customfield_10507
// override (case-insensitive); preReleaseType matters in the comparison.
const (
	preReleaseType   = "Pre-Release"
	majorReleaseType = "Major Release"
	scheduledType    = "Scheduled"
)

const (
	// defaultCockroachRepo is the fallback GitHub repo that holds release
	// branches when neither --repo nor GITHUB_REPOSITORY is set (e.g. local
	// invocations outside of GitHub Actions).
	defaultCockroachRepo = "cockroachdb/cockroach"
	releaseChannel       = "#db-release-status"
	opsChannel           = "#release-ops"
	// nonProdChannel keeps Slack noise out of the production channel when
	// the workflow is exercised against a fork (e.g. user/cockroach).
	// The repo identity, not --dry-run, decides which channel is used.
	nonProdChannel = "#db-release-test"

	envJiraToken     = "JIRA_API_TOKEN"
	envJiraEmail     = "JIRA_EMAIL"
	envSlackBotToken = "SLACK_BOT_TOKEN"
	// envCutGitHubToken is the GitHub personal-access token used by the
	// branch-cut and pick-sha commands. The standard GITHUB_TOKEN name is
	// used so GitHub Actions forwards the fetched PAT without any extra
	// env-var renaming in the workflow step.
	envCutGitHubToken = "GITHUB_TOKEN"
	// envGitHubRepository is the standard env var GitHub Actions populates
	// with the owner/name of the running repository.
	envGitHubRepository = "GITHUB_REPOSITORY"
)

var cutStagingFlags = struct {
	dryRun       bool
	testIssueKey string
	today        string
	repo         string
	channel      string
	opsChannel   string
}{}

var cutStagingBranchesCmd = &cobra.Command{
	Use:   "cut-staging-branches",
	Short: "Cut release staging branches whose Jira cut date has arrived",
	Long: `Queries Jira for CRDB Release tickets whose Cut Branch Date is today or
earlier and whose Staging Branch field is unset, then for each such ticket
creates the staging branch on GitHub, updates the Jira ticket, posts a Slack
notification, and creates the matching backport label.

Pre-release tickets (e.g. v25.3.1-alpha.1) get a different Slack template.
Patch-zero releases are skipped — those .0 builds ship from the existing
release-X.Y branch.`,
	RunE: runCutStagingBranches,
}

// defaultRepo picks the GitHub Actions-provided $GITHUB_REPOSITORY when
// available so the workflow doesn't need to pass --repo explicitly. It
// falls back to defaultCockroachRepo for local invocations.
func defaultRepo() string {
	if r := os.Getenv(envGitHubRepository); r != "" {
		return r
	}
	return defaultCockroachRepo
}

func init() {
	cutStagingBranchesCmd.Flags().BoolVar(&cutStagingFlags.dryRun, "dry-run", false,
		"print actions and skip Jira/GitHub mutations; Slack messages are prefixed with [DRY RUN]")
	cutStagingBranchesCmd.Flags().StringVar(&cutStagingFlags.testIssueKey, "test-issue-key", "",
		"treat the named issue as the only candidate, bypassing the JQL search "+
			"(use to rehearse in dry-run, or to scope a production run to a single ticket)")
	cutStagingBranchesCmd.Flags().StringVar(&cutStagingFlags.today, "today", "",
		"override 'today' for eligibility checks (YYYY-MM-DD)")
	cutStagingBranchesCmd.Flags().StringVar(&cutStagingFlags.repo, "repo", defaultRepo(),
		"target GitHub repository in owner/name form (defaults to $GITHUB_REPOSITORY when set)")
	cutStagingBranchesCmd.Flags().StringVar(&cutStagingFlags.channel, "slack-channel", "",
		"Slack channel for success notifications (default: "+releaseChannel+
			" when "+envIsProductionRepo+"=true, "+nonProdChannel+" otherwise)")
	cutStagingBranchesCmd.Flags().StringVar(&cutStagingFlags.opsChannel, "ops-channel", opsChannel,
		"Slack channel for failure notifications")
}

func runCutStagingBranches(_ *cobra.Command, _ []string) error {
	jiraToken := os.Getenv(envJiraToken)
	if jiraToken == "" {
		return errors.Newf("%s is not set", envJiraToken)
	}
	jiraEmail := os.Getenv(envJiraEmail)
	if jiraEmail == "" {
		return errors.Newf("%s is not set", envJiraEmail)
	}
	ghToken := os.Getenv(envCutGitHubToken)
	if ghToken == "" {
		return errors.Newf("%s is not set", envCutGitHubToken)
	}
	slackToken := os.Getenv(envSlackBotToken)
	if slackToken == "" {
		return errors.Newf("%s is not set", envSlackBotToken)
	}

	owner, repoName, err := splitRepo(cutStagingFlags.repo)
	if err != nil {
		return errors.Wrap(err, "parsing --repo")
	}

	today, err := resolveToday(cutStagingFlags.today)
	if err != nil {
		return errors.Wrap(err, "resolving today's date")
	}

	jira := newJiraClient(jiraEmail, jiraToken)
	gh := newGitHubClient(ghToken, owner, repoName)
	sl := newSlackClient(slackToken)

	// When the workflow is exercised against a fork, route Slack to a
	// non-prod channel so rehearsal traffic doesn't reach #db-release-status.
	// An explicit --slack-channel always wins.
	channel := cutStagingFlags.channel
	if channel == "" {
		channel = nonProdChannel
		if isProductionRepo() {
			channel = releaseChannel
		}
	}
	r := &cutRunner{
		jira:         jira,
		gh:           gh,
		slack:        sl,
		dryRun:       cutStagingFlags.dryRun,
		testIssueKey: cutStagingFlags.testIssueKey,
		today:        today,
		channel:      channel,
		opsChannel:   cutStagingFlags.opsChannel,
		repo:         cutStagingFlags.repo,
	}
	return r.run(context.Background())
}

// cutRunner holds the wired-up dependencies and per-invocation knobs for one
// run of the branch-cut workflow.
type cutRunner struct {
	jira  *jiraClient
	gh    *githubClient
	slack *slackClient

	dryRun       bool
	testIssueKey string
	today        time.Time
	channel      string
	opsChannel   string
	repo         string
}

func (r *cutRunner) run(ctx context.Context) error {
	candidates, err := r.candidates()
	if err != nil {
		r.notifyFailure(err)
		return err
	}
	log.Printf("Jira returned %d candidate ticket(s)", len(candidates))

	var firstErr error
	for _, c := range candidates {
		if err := r.processCandidate(ctx, c); err != nil {
			log.Printf("ticket %s failed: %v", c.Key, err)
			r.notifyFailure(errors.Wrapf(err, "ticket %s", c.Key))
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (r *cutRunner) processCandidate(ctx context.Context, c jiraIssue) error {
	v, err := parseReleaseVersion(c.Fields.Summary)
	if err != nil {
		// Tracking/template tickets that match the JQL but aren't real
		// release tickets (e.g. summary "Release: 26.1|25.4|25.2 next
		// patch") aren't actionable — log and move on rather than
		// failing the whole run. The relaxed dry-run JQL exposes more
		// of these than production ever will.
		log.Printf("ticket %s: skipping unparseable summary: %v", c.Key, err)
		return nil
	}

	// Patch .0 releases ship from the existing release-X.Y branch — no
	// staging branch to cut.
	if v.Patch() == 0 {
		log.Printf("ticket %s: skipping patch-zero release %s", c.Key, v)
		return nil
	}

	// Cut Branch Date is the one schedule field the workflow can't
	// proceed without — the rest only feed the Slack/Jira message
	// templates and may legitimately be filled in later.
	cutDate, err := c.stringField(cfCutBranchDate)
	if err != nil {
		return errors.Wrap(err, "reading cut-branch-date field")
	}
	if cutDate == "" {
		log.Printf("ticket %s: skipping — Cut Branch Date is unset", c.Key)
		return nil
	}

	// When --test-issue-key is set the operator picked this ticket
	// explicitly (rehearsal or one-off recovery), so we bypass the
	// cut-date readiness check; otherwise a future-dated ticket would
	// be silently skipped.
	if r.testIssueKey == "" {
		ready, err := isCutDateReady(cutDate, r.today)
		if err != nil {
			return errors.Wrap(err, "evaluating cut-branch-date readiness")
		}
		if !ready {
			log.Printf("ticket %s: cut date %s not yet reached (today=%s)",
				c.Key, cutDate, dateOnly(r.today))
			return nil
		}
	}

	full, err := r.jira.GetIssue(c.Key)
	if err != nil {
		return errors.Wrapf(err, "fetching full Jira issue %s", c.Key)
	}
	issueKey := c.Key

	branchNames := deriveBranchNames(v)
	baseSHA, branchAlreadyCut, err := r.validate(ctx, full, branchNames)
	if err != nil {
		return errors.Wrap(err, "validating ticket and repo state")
	}
	now := timeNow()
	details, err := buildReleaseDetails(v, full, branchNames, baseSHA, now, r.repo)
	if err != nil {
		return errors.Wrap(err, "building release details")
	}

	if !r.dryRun {
		if branchAlreadyCut {
			log.Printf("ticket %s: staging branch %s already exists (Jira matches), "+
				"resuming after prior partial failure",
				issueKey, branchNames.staging)
		} else if err := r.gh.CreateBranch(ctx, branchNames.staging, baseSHA); err != nil {
			return errors.Wrapf(err, "creating staging branch %s at %s",
				branchNames.staging, baseSHA)
		}
		if err := r.jira.UpdateFields(issueKey, map[string]interface{}{
			cfStagingBranch: details.StagingBranch,
			cfReleaseStatus: details.ReleaseStatus,
		}); err != nil {
			return errors.Wrap(err, "updating Jira staging-branch and release-status fields")
		}
		// Skip the transition when the ticket is already Baking — Jira
		// rejects transitioning into the current state, which would otherwise
		// wedge a recovery run that previously got past this point but
		// failed at a later step (label create, Slack post, Jira comment).
		// Treat an empty status name (older snapshot, parse failure) as
		// "unknown, proceed" so a fresh ticket is never blocked.
		if full.statusName() != bakingStatusName {
			if err := r.jira.Transition(issueKey, transitionBaking); err != nil {
				return errors.Wrapf(err, "transitioning ticket %s to Baking", issueKey)
			}
		} else {
			log.Printf("ticket %s: already in %s status, skipping transition",
				issueKey, bakingStatusName)
		}
		if subtaskKey := findCutSubtask(full); subtaskKey != "" {
			if err := r.jira.Transition(subtaskKey, transitionSubtaskDone); err != nil {
				return errors.Wrapf(err, "transitioning subtask %s to Done", subtaskKey)
			}
		}
		if err := r.gh.CreateLabel(ctx,
			details.BackportLabel, details.BackportLabelDescription, "f29513"); err != nil {
			return errors.Wrapf(err, "creating backport label %s", details.BackportLabel)
		}
	} else {
		log.Printf("[DRY RUN] would create branch %s at %s in %s",
			branchNames.staging, baseSHA, r.repo)
		log.Printf("[DRY RUN] would update Jira %s: stagingBranch=%s releaseStatus=%s",
			issueKey, details.StagingBranch, details.ReleaseStatus)
		log.Printf("[DRY RUN] would transition %s to Baking", issueKey)
		log.Printf("[DRY RUN] would create label %s (%s)",
			details.BackportLabel, details.BackportLabelDescription)
	}

	msg := buildSlackMessage(details, r.dryRun)
	link, err := r.slack.PostMessage(r.channel, msg)
	if err != nil {
		return errors.Wrap(err, "posting Slack message")
	}
	if !r.dryRun {
		if err := r.jira.AddComment(issueKey, buildJiraComment(details, link)); err != nil {
			return errors.Wrap(err, "adding Jira comment")
		}
	} else {
		log.Printf("[DRY RUN] would add Jira comment on %s with Slack link %q", issueKey, link)
	}
	if r.dryRun {
		log.Printf("[DRY RUN] ticket %s: would cut staging branch %s at %s", issueKey, branchNames.staging, baseSHA)
	} else {
		log.Printf("ticket %s: cut staging branch %s at %s", issueKey, branchNames.staging, baseSHA)
	}
	return nil
}

// candidates returns the list of Jira tickets to evaluate. When
// --test-issue-key is set it bypasses the JQL search and returns just that
// ticket — so the same flag works for dry-run rehearsals and for scoping a
// production run to a single ticket (e.g. one-off recovery).
func (r *cutRunner) candidates() ([]jiraIssue, error) {
	if r.testIssueKey != "" {
		issue, err := r.jira.GetIssue(r.testIssueKey)
		if err != nil {
			return nil, errors.Wrapf(err, "fetching test issue %s", r.testIssueKey)
		}
		return []jiraIssue{*issue}, nil
	}
	jql := cutStagingJQL
	if r.dryRun {
		jql = cutStagingJQLDryRun
	}
	candidates, err := r.jira.SearchJQL(jql, []string{"*all"})
	if err != nil {
		return nil, errors.Wrap(err, "searching Jira for candidate tickets")
	}
	return candidates, nil
}

// validate verifies that the ticket and the GitHub repo are in a state where
// we can cut the staging branch, and returns the SHA the caller should record
// as "cut at". It distinguishes a missing base branch (errBranchNotFound)
// from any other GitHub failure, which is propagated as-is so transient
// infrastructure errors don't get reported as "branch not found".
//
// When the staging branch already exists on GitHub *and* Jira's
// cfStagingBranch field already names it, validate treats this as a recovery
// from a partially-failed prior run: it returns branchAlreadyCut=true and
// the existing staging-branch tip SHA, and the caller should skip
// CreateBranch and re-run the remaining (idempotent) steps. Without this
// gate, a failure between CreateBranch and the Jira/Slack side effects
// would wedge the ticket until an operator manually deleted the branch.
func (r *cutRunner) validate(
	ctx context.Context, full *jiraIssue, b branchNames,
) (sha string, branchAlreadyCut bool, _ error) {
	jiraStaging, err := full.stringField(cfStagingBranch)
	if err != nil {
		return "", false, errors.Wrap(err, "reading staging-branch field from Jira")
	}
	exists, err := r.gh.BranchExists(ctx, b.staging)
	if err != nil {
		return "", false, errors.Wrapf(err, "checking for existing staging branch %s on GitHub", b.staging)
	}
	if exists {
		// The branch is on GitHub. Only resume if Jira agrees this is the
		// branch for this ticket — otherwise we don't know who created it,
		// and creating the rest of the side effects on top would mislabel
		// foreign state as ours.
		if jiraStaging != b.staging {
			return "", false, errors.Newf(
				"staging branch %s already exists on GitHub but cfStagingBranch in Jira is %q",
				b.staging, jiraStaging)
		}
		sha, err := r.gh.GetBranchSHA(ctx, b.staging)
		if err != nil {
			return "", false, errors.Wrapf(err,
				"fetching tip SHA for existing staging branch %s", b.staging)
		}
		return sha, true, nil
	}
	// The branch isn't on GitHub. If Jira already names a staging branch
	// the workspace state is inconsistent — refuse to cut silently and let
	// an operator reconcile.
	if jiraStaging != "" && !r.dryRun {
		return "", false, errors.Newf(
			"cfStagingBranch in Jira is %q but no such branch exists on GitHub",
			jiraStaging)
	}
	baseSHA, err := r.gh.GetBranchSHA(ctx, b.base)
	if err != nil {
		if errors.Is(err, errBranchNotFound) {
			return "", false, errors.Newf("base branch %s does not exist on GitHub", b.base)
		}
		return "", false, errors.Wrapf(err, "fetching tip SHA for base branch %s", b.base)
	}
	return baseSHA, false, nil
}

// notifyFailure posts a one-line summary to Slack so operators see "something
// broke" with a pointer to the GHA run for the full error. We deliberately
// don't forward err.Error() — upstream API errors can include large response
// bodies, and a short message keeps #release-ops readable. The full error
// is still logged via log.Printf in the caller and visible in the run log.
func (r *cutRunner) notifyFailure(err error) {
	headline := firstLine(err.Error())
	msg := fmt.Sprintf(":x: branch-cut failed (%s): %s", r.repo, headline)
	if link := actionsRunURL(); link != "" {
		msg += " — see " + link
	}
	if r.dryRun {
		msg = "[DRY RUN] " + msg
	}
	if _, postErr := r.slack.PostMessage(r.opsChannel, msg); postErr != nil {
		log.Printf("failed to post failure to %s: %v", r.opsChannel, postErr)
	}
}

// firstLine returns the first line of s, trimmed and capped at 200 chars,
// suitable for a one-line Slack summary.
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	s = strings.TrimSpace(s)
	const max = 200
	if len(s) > max {
		s = s[:max] + "…"
	}
	return s
}

// actionsRunURL returns a link to the current GitHub Actions run when the
// standard GHA env vars are set, else "".
func actionsRunURL() string {
	server := os.Getenv("GITHUB_SERVER_URL")
	repo := os.Getenv(envGitHubRepository)
	runID := os.Getenv("GITHUB_RUN_ID")
	if server == "" || repo == "" || runID == "" {
		return ""
	}
	return fmt.Sprintf("%s/%s/actions/runs/%s", server, repo, runID)
}

// branchNames holds the base and staging branch names derived from a
// release version.
type branchNames struct {
	base    string // release-25.4
	staging string // release-25.4.3-rc
}

func deriveBranchNames(v version.Version) branchNames {
	base := fmt.Sprintf("release-%d.%d", v.Major().Year, v.Major().Ordinal)
	staging := fmt.Sprintf("release-%d.%d.%d-rc", v.Major().Year, v.Major().Ordinal, v.Patch())
	return branchNames{base: base, staging: staging}
}

// parseReleaseVersion extracts the version from a Jira summary of the form
// "Release: vX.Y.Z[ ...]".
func parseReleaseVersion(summary string) (version.Version, error) {
	const prefix = "Release: v"
	if !strings.HasPrefix(summary, prefix) {
		return version.Version{}, errors.Newf("summary does not start with %q: %s", prefix, summary)
	}
	// Take everything after "Release: " and split on whitespace to keep
	// only the first word (the version itself).
	rest := strings.TrimPrefix(summary, "Release: ")
	versionStr := strings.Fields(rest)[0]
	v, err := version.Parse(versionStr)
	if err != nil {
		return version.Version{}, errors.Wrapf(err, "parsing version %q from summary", versionStr)
	}
	return v, nil
}

// isCutDateReady reports whether the cut date string (YYYY-MM-DD) is on or
// before today. An empty cut date is treated as "not ready" so we never cut
// a branch for a ticket missing the date.
func isCutDateReady(cutDate string, today time.Time) (bool, error) {
	if cutDate == "" {
		return false, nil
	}
	d, err := time.Parse("2006-01-02", cutDate)
	if err != nil {
		return false, errors.Wrapf(err, "parsing cut date %q", cutDate)
	}
	return !dateOnly(today).Before(d), nil
}

// resolveToday returns the override date when set, else today's local date.
func resolveToday(override string) (time.Time, error) {
	if override == "" {
		return timeNow(), nil
	}
	t, err := time.Parse("2006-01-02", override)
	if err != nil {
		return time.Time{}, errors.Wrapf(err, "parsing --today=%q", override)
	}
	return t, nil
}

func dateOnly(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// timeNow is a seam for tests. It returns the current local date.
var timeNow = func() time.Time { return timeutil.Now() }

// findCutSubtask returns the key of the "Cut Staging Branch" subtask, or ""
// if none was found.
func findCutSubtask(issue *jiraIssue) string {
	return findOpenSubtask(issue, cutStagingSubtaskMatch)
}

// findSubtask returns the key of the first subtask whose summary contains
// needle (case-insensitive) and a bool indicating whether that subtask is
// in the "Done" state. If no matching subtask exists, returns ("", false).
func findSubtask(issue *jiraIssue, needle string) (key string, done bool) {
	lower := strings.ToLower(needle)
	for _, st := range issue.Fields.Subtasks {
		if !strings.Contains(strings.ToLower(st.Fields.Summary), lower) {
			continue
		}
		return st.Key, strings.EqualFold(st.Fields.Status.Name, "Done")
	}
	return "", false
}

// findOpenSubtask returns the key of the first subtask whose summary contains
// needle (case-insensitive) and is not already in the "Done" state, or "" if
// none matches. We only transition open subtasks — re-running the workflow on
// a ticket whose subtask was already closed must be a no-op.
func findOpenSubtask(issue *jiraIssue, needle string) string {
	key, done := findSubtask(issue, needle)
	if done {
		return ""
	}
	return key
}

// releaseDetails holds the formatted strings used in Jira updates and Slack
// messages for one release.
type releaseDetails struct {
	Release                  string // v25.4.3
	FinalRelease             string // v25.4.3 (without -alpha/-beta suffix)
	Series                   string // 25.4
	StagingBranch            string // release-25.4.3-rc
	BaseBranch               string // release-25.4
	CutAtSHA                 string
	CutAtCommitURL           string // https://github.com/<repo>/commit/<sha>
	BranchCutDateTime        string
	PickSHADate              string
	CloudReleaseNotesDate    string
	PublishBinaryDate        string
	ReleaseStatus            string
	BackportLabel            string
	BackportLabelDescription string
	ReleaseType              string
}

func buildReleaseDetails(
	v version.Version, issue *jiraIssue, b branchNames, baseSHA string, now time.Time, repo string,
) (releaseDetails, error) {
	pickSHA, err := issue.stringField(cfPickSHADate)
	if err != nil {
		return releaseDetails{}, errors.Wrap(err, "reading pick-SHA-date field")
	}
	cloudNotes, err := issue.stringField(cfCloudReleaseNotes)
	if err != nil {
		return releaseDetails{}, errors.Wrap(err, "reading cloud-release-notes-date field")
	}
	publishBin, err := issue.stringField(cfPublishBinary)
	if err != nil {
		return releaseDetails{}, errors.Wrap(err, "reading publish-binary-date field")
	}
	override, err := issue.optionField(cfReleaseTypeOverride)
	if err != nil {
		return releaseDetails{}, errors.Wrap(err, "reading release-type-override field")
	}

	pickSHAPretty := formatJiraDate(pickSHA, "Monday, 01/02")
	final := finalReleaseString(v)
	series := fmt.Sprintf("%d.%d", v.Major().Year, v.Major().Ordinal)
	releaseStatus := ""
	if pickSHA != "" {
		releaseStatus = formatJiraDate(pickSHA, "01/02") + ": pick SHA"
	}
	return releaseDetails{
		Release:                  v.String(),
		FinalRelease:             final,
		Series:                   series,
		StagingBranch:            b.staging,
		BaseBranch:               b.base,
		CutAtSHA:                 baseSHA,
		CutAtCommitURL:           fmt.Sprintf("https://github.com/%s/commit/%s", repo, baseSHA),
		BranchCutDateTime:        now.Format("2006-01-02 15:04 MST"),
		PickSHADate:              orTBD(pickSHAPretty),
		CloudReleaseNotesDate:    orTBD(formatJiraDate(cloudNotes, "Monday, 01/02")),
		PublishBinaryDate:        orTBD(formatJiraDate(publishBin, "Monday, 01/02")),
		ReleaseStatus:            releaseStatus,
		BackportLabel:            "backport-" + strings.TrimPrefix(final, "v") + "-rc",
		BackportLabelDescription: orTBD(pickSHAPretty) + ": " + b.staging + " will be frozen",
		ReleaseType:              releaseTypeFor(v, override),
	}, nil
}

// orTBD substitutes "TBD" for an empty string so display strings like
// Slack messages and label descriptions never render as a bare backtick
// pair or leading colon when a Jira date field hasn't been filled in yet.
func orTBD(s string) string {
	if s == "" {
		return "TBD"
	}
	return s
}

// finalReleaseString returns the version with any -prerelease suffix
// removed: "v25.3.1-alpha.1" → "v25.3.1".
func finalReleaseString(v version.Version) string {
	return fmt.Sprintf("v%d.%d.%d", v.Major().Year, v.Major().Ordinal, v.Patch())
}

// releaseTypeFor mirrors the Superblocks logic: explicit Jira override wins,
// then pre-release (any suffix), then patch==0 means a major release,
// otherwise a scheduled patch.
func releaseTypeFor(v version.Version, jiraOverride string) string {
	if jiraOverride != "" {
		return jiraOverride
	}
	if v.IsPrerelease() {
		return preReleaseType
	}
	if v.Patch() == 0 {
		return majorReleaseType
	}
	return scheduledType
}

func isPreRelease(releaseType string) bool {
	// Match "pre-release" / "Pre-Release" / "PreRelease" — Jira's
	// customfield_10507 values aren't fixed in code and we want the
	// comparison to be tolerant.
	return strings.Contains(strings.ToLower(releaseType), "pre")
}

// formatJiraDate formats a YYYY-MM-DD Jira date string using a Go layout.
// Empty input yields empty output.
func formatJiraDate(jiraDate, layout string) string {
	if jiraDate == "" {
		return ""
	}
	t, err := time.Parse("2006-01-02", jiraDate)
	if err != nil {
		return jiraDate
	}
	return t.Format(layout)
}

// buildSlackMessage selects the pre-release or scheduled template and fills
// it from the prepared release details. Both templates match the strings the
// Superblocks flow produced so downstream readers (release-team channel
// archives, Jira comments) see the same output.
func buildSlackMessage(d releaseDetails, dryRun bool) string {
	var body string
	if isPreRelease(d.ReleaseType) {
		body = fmt.Sprintf(`Staging branch `+"`%s`"+` has now been cut and frozen for the RC and final .0:
• The <https://cockroachlabs.atlassian.net/wiki/spaces/ENG/pages/1043235321/Database+Backport+Patch+Policy#Backporting-for-major-releases|Database Backport Policy> is now in effect for %s

*If your backport needs to go into `+"`%s`"+` it must*:
• Be approved by via ER request
• Be backported to *BOTH* `+"`%s`"+` and `+"`release-%s`"+`

Staging branch `+"`%s`"+`:
• Cut: `+"`%s`"+`
• Cut at SHA: <%s|%s>`,
			d.StagingBranch, d.Series,
			d.FinalRelease,
			d.StagingBranch, d.Series,
			d.StagingBranch, d.BranchCutDateTime, d.CutAtCommitURL, d.CutAtSHA)
	} else {
		body = fmt.Sprintf("`%s` staging branch `%s` has been cut:\n"+
			"• *Pick SHA (Branch Freeze) Date:* `%s`\n"+
			"• *Cloud Release Notes Date:* `%s`\n"+
			"• *Publish Binaries Date:* `%s`\n\n"+
			"Staging branch `%s`:\n"+
			"• Cut: `%s`\n"+
			"• Cut at SHA: <%s|%s>",
			d.Release, d.StagingBranch,
			d.PickSHADate, d.CloudReleaseNotesDate, d.PublishBinaryDate,
			d.StagingBranch, d.BranchCutDateTime, d.CutAtCommitURL, d.CutAtSHA)
	}
	if dryRun {
		return "[DRY RUN] " + body
	}
	return body
}

// backportPolicyURL is the wiki page linked from the pre-release Slack and
// Jira messages. Kept here so both renderers stay in lockstep.
const backportPolicyURL = "https://cockroachlabs.atlassian.net/wiki/spaces/ENG/pages/1043235321/Database+Backport+Patch+Policy#Backporting-for-major-releases"

// buildJiraComment returns an ADF doc that mirrors buildSlackMessage's
// content but uses native Jira formatting (strong/code marks, bullet
// lists, real links) so the comment doesn't show literal `*` and
// backticks. The Slack permalink, when non-empty, is rendered as the
// first paragraph for traceability.
func buildJiraComment(d releaseDetails, slackLink string) map[string]interface{} {
	var blocks []interface{}
	if slackLink != "" {
		blocks = append(blocks, adfPara(adfMarked(slackLink, adfLink(slackLink))))
	}
	if isPreRelease(d.ReleaseType) {
		blocks = append(blocks,
			adfPara(
				adfText("Staging branch "),
				adfMarked(d.StagingBranch, adfCode()),
				adfText(" has now been cut and frozen for the RC and final .0:"),
			),
			adfBullet([]interface{}{
				adfText("The "),
				adfMarked("Database Backport Policy", adfLink(backportPolicyURL)),
				adfText(" is now in effect for " + d.Series),
			}),
			adfPara(
				adfMarked("If your backport needs to go into ", adfStrong()),
				adfMarked(d.StagingBranch, adfStrong(), adfCode()),
				adfMarked(" it must:", adfStrong()),
			),
			adfBullet(
				[]interface{}{adfText("Be approved by via ER request")},
				[]interface{}{
					adfText("Be backported to "),
					adfMarked("BOTH", adfStrong()),
					adfText(" "),
					adfMarked(d.StagingBranch, adfCode()),
					adfText(" and "),
					adfMarked("release-"+d.Series, adfCode()),
				},
			),
		)
	} else {
		blocks = append(blocks,
			adfPara(
				adfMarked(d.Release, adfCode()),
				adfText(" staging branch "),
				adfMarked(d.StagingBranch, adfCode()),
				adfText(" has been cut:"),
			),
			adfBullet(
				[]interface{}{
					adfMarked("Pick SHA (Branch Freeze) Date: ", adfStrong()),
					adfMarked(d.PickSHADate, adfCode()),
				},
				[]interface{}{
					adfMarked("Cloud Release Notes Date: ", adfStrong()),
					adfMarked(d.CloudReleaseNotesDate, adfCode()),
				},
				[]interface{}{
					adfMarked("Publish Binaries Date: ", adfStrong()),
					adfMarked(d.PublishBinaryDate, adfCode()),
				},
			),
		)
	}
	blocks = append(blocks,
		adfPara(
			adfText("Staging branch "),
			adfMarked(d.StagingBranch, adfCode()),
			adfText(":"),
		),
		adfBullet(
			[]interface{}{
				adfText("Cut: "),
				adfMarked(d.BranchCutDateTime, adfCode()),
			},
			[]interface{}{
				adfText("Cut at SHA: "),
				adfMarked(d.CutAtSHA, adfLink(d.CutAtCommitURL), adfCode()),
			},
		),
	)
	return adfDoc(blocks...)
}

// splitRepo turns "owner/name" into its parts.
func splitRepo(repo string) (string, string, error) {
	parts := strings.Split(repo, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", errors.Newf("invalid repo %q, expected owner/name", repo)
	}
	return parts[0], parts[1], nil
}
