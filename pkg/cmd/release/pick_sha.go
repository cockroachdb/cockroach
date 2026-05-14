// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// pickSHAJQL builds the candidate set for the pick-SHA workflow:
// type=Release, parented (REL-217 holds historical/template tickets without
// parents that we don't want to act on), still open, and with a Staging
// Branch already set (which the cut-staging-branches workflow does first).
// The Pick SHA Date readiness check happens client-side in processCandidate
// after the search returns — JQL date arithmetic on custom fields is fiddly
// and the daily candidate set is small enough that the extra round-trip
// doesn't matter.
const pickSHAJQL = `issueType="CRDB Release" and parent is not empty and ` +
	`status not in (done, cancelled, "Post-Publish Tasks") and ` +
	`"Staging Branch[Short text]" is not empty`

// pickSHAJQLDryRun drops the `parent is not empty` filter so dry runs can
// rehearse against parent/template tickets, mirroring cutStagingJQLDryRun.
const pickSHAJQLDryRun = `issueType="CRDB Release" and ` +
	`status not in (done, cancelled, "Post-Publish Tasks") and ` +
	`"Staging Branch[Short text]" is not empty`

// pickSHASubtaskMatch is the case-insensitive substring used to find the
// "Pick SHA" subtask on a release ticket.
const pickSHASubtaskMatch = "Pick SHA"

// defaultBuildWorkflow is the workflow file dispatched by `pick-sha`. It
// must accept `dry_run` and `sha` workflow_dispatch inputs.
const defaultBuildWorkflow = "release-build-and-sign.yml"

var pickSHAFlags = struct {
	dryRun        bool
	testIssueKey  string
	today         string
	repo          string
	channel       string
	opsChannel    string
	buildWorkflow string
}{}

var pickSHACmd = &cobra.Command{
	Use:   "pick-sha",
	Short: "Pick the staging branch tip and trigger build-and-sign for due tickets",
	Long: `Queries Jira for open CRDB Release tickets that have a Staging Branch
set, then for each one whose Pick SHA Date is today or earlier fetches the
tip SHA of the staging branch, dispatches the build-and-sign workflow
against that branch and SHA, posts a Slack notification, comments on the
Jira ticket, and transitions the "Pick SHA" subtask to Done.

Dry-run still dispatches build-and-sign but with dry_run=true so the entire
chain is exercised end-to-end against dev infra; no Jira mutations happen on
our side and Slack messages are prefixed with [DRY RUN]. Slack channel
selection is based on the IS_PRODUCTION_REPO env var, not --dry-run, so a
dry-run from the production repo still posts to the production channel.`,
	RunE: runPickSHA,
}

func init() {
	pickSHACmd.Flags().BoolVar(&pickSHAFlags.dryRun, "dry-run", false,
		"pass dry_run=true to the dispatched build-and-sign workflow and skip "+
			"our Jira mutations; Slack messages are prefixed with [DRY RUN]")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.testIssueKey, "test-issue-key", "",
		"treat the named issue as the only candidate, bypassing the JQL search "+
			"and the Pick SHA Date readiness check (use to rehearse in dry-run, "+
			"or to scope a production run to a single ticket)")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.today, "today", "",
		"override 'today' for eligibility checks (YYYY-MM-DD)")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.repo, "repo", defaultRepo(),
		"target GitHub repository in owner/name form (defaults to $GITHUB_REPOSITORY when set)")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.channel, "slack-channel", "",
		"Slack channel for success notifications (default: "+releaseChannel+
			" when "+envIsProductionRepo+"=true, "+nonProdChannel+" otherwise)")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.opsChannel, "ops-channel", opsChannel,
		"Slack channel for failure notifications")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.buildWorkflow, "build-workflow", defaultBuildWorkflow,
		"name of the workflow file (in .github/workflows) to dispatch")
}

func runPickSHA(_ *cobra.Command, _ []string) error {
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
	releaseNotesAPIKey := os.Getenv(envReleaseNotesAPIKey)
	if releaseNotesAPIKey == "" {
		return errors.Newf("%s is not set", envReleaseNotesAPIKey)
	}

	owner, repoName, err := splitRepo(pickSHAFlags.repo)
	if err != nil {
		return errors.Wrap(err, "parsing --repo")
	}

	today, err := resolveToday(pickSHAFlags.today)
	if err != nil {
		return errors.Wrap(err, "resolving today's date")
	}

	jira := newJiraClient(jiraEmail, jiraToken)
	gh := newGitHubClient(ghToken, owner, repoName)
	sl := newSlackClient(slackToken)

	// Repo identity (not --dry-run) decides which channel to post to: a fork
	// rehearsal must not echo into #db-release-status. Explicit
	// --slack-channel always wins.
	channel := pickSHAFlags.channel
	if channel == "" {
		channel = nonProdChannel
		if isProductionRepo() {
			channel = releaseChannel
		}
	}
	r := &pickSHARunner{
		jira:               jira,
		gh:                 gh,
		slack:              sl,
		dryRun:             pickSHAFlags.dryRun,
		testIssueKey:       pickSHAFlags.testIssueKey,
		today:              today,
		channel:            channel,
		opsChannel:         pickSHAFlags.opsChannel,
		repo:               pickSHAFlags.repo,
		buildWorkflow:      pickSHAFlags.buildWorkflow,
		releaseNotesAPIKey: releaseNotesAPIKey,
	}
	return r.run(context.Background())
}

// pickSHARunner holds the wired-up dependencies and per-invocation knobs for
// one run of the pick-sha workflow.
type pickSHARunner struct {
	jira  *jiraClient
	gh    *githubClient
	slack *slackClient

	dryRun        bool
	testIssueKey  string
	today         time.Time
	channel       string
	opsChannel    string
	repo          string
	buildWorkflow string
	// releaseNotesAPIKey is the X-API-Key for the docs release-notes
	// automation endpoint. Required at startup; per-candidate API failures
	// are non-fatal (warning to #release-ops, continue).
	releaseNotesAPIKey string
}

func (r *pickSHARunner) run(ctx context.Context) error {
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

func (r *pickSHARunner) processCandidate(ctx context.Context, c jiraIssue) error {
	if _, err := parseReleaseVersion(c.Fields.Summary); err != nil {
		// Tracking/template tickets that match the JQL but aren't real
		// release tickets (e.g. summary "Release: 26.1|25.4|25.2 next
		// patch") aren't actionable — log and move on rather than
		// failing the whole run. The relaxed dry-run JQL exposes more
		// of these than production ever will.
		log.Printf("ticket %s: skipping unparseable summary: %v", c.Key, err)
		return nil
	}

	pickDate, err := c.stringField(cfPickSHADate)
	if err != nil {
		return errors.Wrap(err, "reading pick-SHA-date field")
	}
	if pickDate == "" {
		log.Printf("ticket %s: skipping — Pick SHA Date is unset", c.Key)
		return nil
	}

	// When --test-issue-key is set the operator picked this ticket
	// explicitly (rehearsal or one-off recovery), so we bypass the
	// pick-date readiness check; otherwise a future-dated ticket would
	// be silently skipped.
	if r.testIssueKey == "" {
		ready, err := isCutDateReady(pickDate, r.today)
		if err != nil {
			return errors.Wrap(err, "evaluating pick-SHA-date readiness")
		}
		if !ready {
			log.Printf("ticket %s: pick SHA date %s not yet reached (today=%s)",
				c.Key, pickDate, dateOnly(r.today))
			return nil
		}
	}

	full, err := r.jira.GetIssue(c.Key)
	if err != nil {
		return errors.Wrapf(err, "fetching full Jira issue %s", c.Key)
	}

	// Idempotency gate. The JQL doesn't filter on subtask state, so a
	// failure between DispatchWorkflow and the Slack/Jira side effects
	// would otherwise let the next cron re-fire build-and-sign — an
	// expensive, days-long workflow. Treat a Done Pick SHA subtask as
	// "already handled". A missing subtask (e.g. a template ticket
	// exposed by the dry-run JQL) falls through, since that path is
	// only reachable in dry-run.
	//
	// Residual risk: if DispatchWorkflow succeeds but the subtask
	// transition below fails, the next run will re-dispatch. The
	// failure-notify Slack post should catch this within the cron
	// window so an operator can intervene.
	subtaskKey, subtaskDone := findSubtask(full, pickSHASubtaskMatch)
	if subtaskDone {
		log.Printf("ticket %s: Pick SHA subtask already Done, skipping re-dispatch", c.Key)
		return nil
	}

	staging, err := full.stringField(cfStagingBranch)
	if err != nil {
		return errors.Wrap(err, "reading staging-branch field")
	}
	if staging == "" {
		return errors.Newf("ticket %s has no Staging Branch set", c.Key)
	}
	sha, err := r.gh.GetBranchSHA(ctx, staging)
	if err != nil {
		return errors.Wrapf(err, "fetching tip SHA for staging branch %s", staging)
	}

	details, err := buildPickSHADetails(full, staging, sha, r.repo, r.buildWorkflow)
	if err != nil {
		return errors.Wrap(err, "building pick-SHA details")
	}

	ref := "refs/heads/" + staging
	// The workflow_dispatch REST endpoint requires every input value to be
	// a JSON string — booleans typed in workflow YAML are coerced from
	// "true"/"false" on receipt — so we stringify dry_run rather than
	// passing a Go bool.
	inputs := map[string]string{
		"sha":     sha,
		"dry_run": strconv.FormatBool(r.dryRun),
	}
	if err := r.gh.DispatchWorkflow(ctx, ref, r.buildWorkflow, inputs); err != nil {
		return errors.Wrapf(err, "dispatching %s on %s", r.buildWorkflow, ref)
	}

	if !r.dryRun {
		if subtaskKey != "" {
			if err := r.jira.Transition(subtaskKey, transitionSubtaskDone); err != nil {
				return errors.Wrapf(err, "transitioning subtask %s to Done", subtaskKey)
			}
		}
		if err := r.jira.UpdateFields(c.Key, map[string]interface{}{
			cfBuildSHA: sha,
		}); err != nil {
			return errors.Wrapf(err, "updating SHA field on %s", c.Key)
		}
	} else {
		log.Printf("[DRY RUN] would transition Pick SHA subtask of %s to Done", c.Key)
		log.Printf("[DRY RUN] would set SHA field on %s to %s", c.Key, sha)
	}

	msg := buildPickSHASlackMessage(details, r.dryRun)
	link, err := r.slack.PostMessage(r.channel, msg)
	if err != nil {
		return errors.Wrap(err, "posting Slack message")
	}
	if !r.dryRun {
		if err := r.jira.AddComment(c.Key, buildPickSHAJiraComment(details, link)); err != nil {
			return errors.Wrap(err, "adding Jira comment")
		}
	} else {
		log.Printf("[DRY RUN] would add Jira comment on %s with Slack link %q", c.Key, link)
	}

	// Notify the docs release-notes API last, after every other side effect
	// has succeeded. Failures here are non-fatal: we already dispatched
	// build-and-sign and updated Jira/Slack, so failing the candidate would
	// re-fire those (expensive) side effects on the next cron. A Slack
	// warning to #release-ops gives operators a chance to re-trigger
	// generation manually.
	r.notifyReleaseNotes(c.Key, full, sha)

	log.Printf("ticket %s: dispatched %s on %s at %s", c.Key, r.buildWorkflow, ref, sha)
	return nil
}

// notifyReleaseNotes builds the release-notes API payload from the Jira
// issue and POSTs it. Skipped (with a log line) under --dry-run, since the
// API has no non-prod endpoint and a dry-run call would create real docs
// drafts. Errors are swallowed after a warning is posted to #release-ops:
// see the call site for why.
//
// Idempotency: the call is also skipped when the docs subtask is already
// marked Done, because that means the docs team has already opened (and
// likely shipped) the release-notes draft for this release. Re-POSTing
// would create a duplicate draft. The pick-SHA-subtask gate in
// processCandidate normally prevents re-entry on the same ticket, but a
// transition failure between DispatchWorkflow and the subtask transition
// could re-fire the rest of the chain — this gate covers that window.
func (r *pickSHARunner) notifyReleaseNotes(key string, full *jiraIssue, sha string) {
	if r.dryRun {
		log.Printf("[DRY RUN] would call release-notes API for %s", key)
		return
	}
	if _, docsDone := findSubtask(full, docsSubtaskMatch); docsDone {
		log.Printf("ticket %s: docs subtask already Done, skipping release-notes API call", key)
		return
	}
	payload, err := buildReleaseNotesPayload(full, sha)
	if err != nil {
		// We never built the payload, so the warning has nothing to dump.
		// Pass the zero value; buildReleaseNotesWarning falls back to the
		// ticket key in the headline.
		log.Printf("ticket %s: building release-notes payload failed: %v", key, err)
		r.warnReleaseNotes(key, releaseNotesPayload{}, err)
		return
	}
	if err := postReleaseNotes(releaseNotesAPIURL, r.releaseNotesAPIKey, payload); err != nil {
		log.Printf("ticket %s: release-notes API call failed: %v", key, err)
		r.warnReleaseNotes(key, payload, err)
	}
}

func (r *pickSHARunner) warnReleaseNotes(key string, payload releaseNotesPayload, err error) {
	msg := buildReleaseNotesWarning(key, payload, err)
	if _, postErr := r.slack.PostMessage(r.opsChannel, msg); postErr != nil {
		log.Printf("failed to post release-notes warning to %s: %v", r.opsChannel, postErr)
	}
}

// jiraBrowseBaseURL is the public Jira base URL for issue links rendered
// into Slack notifications. The cockroachlabs.atlassian.net host is fixed.
const jiraBrowseBaseURL = "https://cockroachlabs.atlassian.net/browse"

// docsInfraSlackLink is a pre-rendered Slack mrkdwn link to the
// #docs-infrastructure-team channel — the team that owns the
// release-notes-automation API. The channel ID is hardcoded so an
// operator who reads the warning has a direct hop to the right channel.
const docsInfraSlackLink = "<https://cockroachlabs.slack.com/archives/C03TH7EEMAA|#docs-infrastructure-team>"

// buildReleaseNotesWarning renders the Slack message posted to
// #release-ops when the release-notes API call (or its payload
// construction) fails. The shape mirrors the Superblocks message this
// replaces so docs-infrastructure-team eyes can recognize it. Payload may
// be the zero value when the failure happened before the payload was
// built; in that case the *Payload* block is omitted and the headline
// falls back to the Jira ticket key.
func buildReleaseNotesWarning(ticketKey string, payload releaseNotesPayload, err error) string {
	headline := payload.CurrentRelease
	if headline == "" {
		headline = ticketKey
	}
	var b strings.Builder
	fmt.Fprintf(&b, ":x: *release-notes-api failed for %s*\n\n", headline)
	fmt.Fprintf(&b, "*Release*: <%s/%s|%s>\n\n", jiraBrowseBaseURL, ticketKey, ticketKey)
	fmt.Fprintf(&b, "*Error*: %s\n\n", err)
	if payload != (releaseNotesPayload{}) {
		// json.MarshalIndent with a tab indent matches the Superblocks
		// formatting verbatim; an unindented dump would be hard to read in
		// Slack. Marshalling a fixed struct cannot fail in practice, but
		// guard for it anyway — losing the payload section is preferable
		// to dropping the whole warning.
		if pretty, jerr := json.MarshalIndent(payload, "", "\t"); jerr == nil {
			fmt.Fprintf(&b, "*Payload*: ```%s```\n\n", pretty)
		}
	}
	fmt.Fprintf(&b, "Please forward this to the %s\n", docsInfraSlackLink)
	return b.String()
}

// buildReleaseNotesPayload extracts the fields the docs API needs from the
// Jira issue. release_date prefers the cloud-release-notes date and falls
// back to the publish-binary date (matching the prior Superblocks logic).
// cloud is true for Scheduled and Major Release types — pre-release builds
// don't generate cloud notes.
func buildReleaseNotesPayload(issue *jiraIssue, sha string) (releaseNotesPayload, error) {
	v, err := parseReleaseVersion(issue.Fields.Summary)
	if err != nil {
		return releaseNotesPayload{}, errors.Wrap(err, "parsing release version")
	}
	cloudNotes, err := issue.stringField(cfCloudReleaseNotes)
	if err != nil {
		return releaseNotesPayload{}, errors.Wrap(err, "reading cloud-release-notes-date field")
	}
	publishBin, err := issue.stringField(cfPublishBinary)
	if err != nil {
		return releaseNotesPayload{}, errors.Wrap(err, "reading publish-binary-date field")
	}
	override, err := issue.optionField(cfReleaseTypeOverride)
	if err != nil {
		return releaseNotesPayload{}, errors.Wrap(err, "reading release-type-override field")
	}

	releaseDate := cloudNotes
	if releaseDate == "" {
		releaseDate = publishBin
	}
	releaseType := releaseTypeFor(v, override)
	docsTicket, _ := findSubtask(issue, docsSubtaskMatch)

	return releaseNotesPayload{
		CurrentRelease: v.String(),
		ReleaseDate:    releaseDate,
		ReleaseSHA:     sha,
		Cloud:          releaseType == scheduledType || releaseType == majorReleaseType,
		DocsTicket:     docsTicket,
	}, nil
}

// candidates returns the list of Jira tickets to evaluate. When
// --test-issue-key is set it bypasses the JQL search and returns just that
// ticket — so the same flag works for dry-run rehearsals and for scoping a
// production run to a single ticket (e.g. one-off recovery).
func (r *pickSHARunner) candidates() ([]jiraIssue, error) {
	if r.testIssueKey != "" {
		issue, err := r.jira.GetIssue(r.testIssueKey)
		if err != nil {
			return nil, errors.Wrapf(err, "fetching test issue %s", r.testIssueKey)
		}
		return []jiraIssue{*issue}, nil
	}
	jql := pickSHAJQL
	if r.dryRun {
		jql = pickSHAJQLDryRun
	}
	candidates, err := r.jira.SearchJQL(jql, []string{"*all"})
	if err != nil {
		return nil, errors.Wrap(err, "searching Jira for candidate tickets")
	}
	return candidates, nil
}

// notifyFailure posts a one-line :x: alert to #release-ops with a link to
// the GHA run for the full error trace.
func (r *pickSHARunner) notifyFailure(err error) {
	headline := firstLine(err.Error())
	msg := fmt.Sprintf(":x: pick-sha failed (%s): %s", r.repo, headline)
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

// pickSHADetails holds the formatted strings used in the Slack and Jira
// announcements for one pick-sha dispatch.
type pickSHADetails struct {
	StagingBranch         string // release-25.4.3-rc
	PickedSHA             string
	PickedCommitURL       string // https://github.com/<repo>/commit/<sha>
	BuildRunListURL       string // https://github.com/<repo>/actions/workflows/<file>
	CloudReleaseNotesDate string
	PublishBinaryDate     string
}

// buildPickSHADetails reads the date custom fields from the Jira issue and
// assembles the display strings needed by buildPickSHASlackMessage and
// buildPickSHAJiraComment. BuildRunListURL is best-effort: the
// workflow_dispatch REST endpoint returns no run ID, so the announcement
// links to the workflow's run list rather than the specific run.
func buildPickSHADetails(
	issue *jiraIssue, stagingBranch, sha, repo, buildWorkflow string,
) (pickSHADetails, error) {
	cloudNotes, err := issue.stringField(cfCloudReleaseNotes)
	if err != nil {
		return pickSHADetails{}, errors.Wrap(err, "reading cloud-release-notes-date field")
	}
	publishBin, err := issue.stringField(cfPublishBinary)
	if err != nil {
		return pickSHADetails{}, errors.Wrap(err, "reading publish-binary-date field")
	}
	return pickSHADetails{
		StagingBranch:         stagingBranch,
		PickedSHA:             sha,
		PickedCommitURL:       fmt.Sprintf("https://github.com/%s/commit/%s", repo, sha),
		BuildRunListURL:       fmt.Sprintf("https://github.com/%s/actions/workflows/%s", repo, buildWorkflow),
		CloudReleaseNotesDate: orTBD(formatJiraDate(cloudNotes, "Monday, 01/02")),
		PublishBinaryDate:     orTBD(formatJiraDate(publishBin, "Monday, 01/02")),
	}, nil
}

// buildPickSHASlackMessage renders the announcement that goes to
// #db-release-status (or the non-prod channel for fork rehearsals).
func buildPickSHASlackMessage(d pickSHADetails, dryRun bool) string {
	body := fmt.Sprintf("SHA picked for `%s` — build-and-sign triggered:\n"+
		"• Picked SHA: <%s|%s>\n"+
		"• Build run: <%s|view in Actions>\n"+
		"• *Cloud Release Notes Date:* `%s`\n"+
		"• *Publish Binaries Date:* `%s`",
		d.StagingBranch,
		d.PickedCommitURL, d.PickedSHA,
		d.BuildRunListURL,
		d.CloudReleaseNotesDate,
		d.PublishBinaryDate)
	if dryRun {
		return "[DRY RUN] " + body
	}
	return body
}

// buildPickSHAJiraComment returns an ADF doc that mirrors the Slack message
// using native Jira marks (strong, code, link) so the comment doesn't show
// literal `*` and backticks. The Slack permalink, when non-empty, is
// rendered as the first paragraph for traceability.
func buildPickSHAJiraComment(d pickSHADetails, slackLink string) map[string]interface{} {
	var blocks []interface{}
	if slackLink != "" {
		blocks = append(blocks, adfPara(adfMarked(slackLink, adfLink(slackLink))))
	}
	blocks = append(blocks,
		adfPara(
			adfText("SHA picked for "),
			adfMarked(d.StagingBranch, adfCode()),
			adfText(" — build-and-sign triggered:"),
		),
		adfBullet(
			[]interface{}{
				adfMarked("Picked SHA: ", adfStrong()),
				adfMarked(d.PickedSHA, adfLink(d.PickedCommitURL), adfCode()),
			},
			[]interface{}{
				adfMarked("Build run: ", adfStrong()),
				adfMarked("view in Actions", adfLink(d.BuildRunListURL)),
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
	return adfDoc(blocks...)
}
