// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// notifyPublishJQL selects the CRDB Release ticket the publish SHA
// belongs to. cfBuildSHA (customfield_10590) is set by pick-sha when it
// dispatches build-and-sign, so by the time a publish completes there is
// exactly one ticket tagged with the SHA we're publishing.
//
// Built with fmt.Sprintf at call time; the placeholder is the SHA, which
// the operator supplies via --sha.
const notifyPublishJQLFormat = `issueType="CRDB Release" and cf[10590]="%s"`

// blessedBullets is the canonical body shared by both the Jira comment
// and the Slack message. Both renderers consume this slice so the two
// surfaces cannot drift; editing the bullet text here updates both. If
// the text needs to vary per release in the future, plumb it through a
// flag rather than splitting the constant.
var blessedBullets = []string{
	"Working on Homebrew versions.",
	"Ready for docs for download/SH.",
	"Ready for SRE to enable on CockroachCloud.",
}

// publishedBullets is the body of the second Slack post — the
// IBM-OEM-facing "binaries have been published" announcement. Kept
// separate from blessedBullets because the audience is different
// (IBM-OEM team, not the wider release-team) and the verb differs
// ("published" vs "blessed").
var publishedBullets = []string{
	"Ready for Security scans and SBOM",
	"Ready for Initial Images and VCoO artifacts",
}

// ibmOEMChannel is the prod Slack channel for the
// "binaries have been published" announcement consumed by the IBM-OEM
// release team. Non-prod runs route to nonProdChannel instead so
// rehearsal traffic doesn't reach the IBM team.
const ibmOEMChannel = "#proj-ibm-oem-releases"

// defaultVersionFilePath is the in-repo path the publish workflow
// checks out at the publish SHA; the file holds the version being
// shipped. Override via --version-file for local rehearsals.
const defaultVersionFilePath = "pkg/build/version.txt"

var notifyPublishFlags = struct {
	sha         string
	dryRun      bool
	channel     string
	ibmChannel  string
	jiraIssue   string
	versionFile string
}{}

var notifyPublishCmd = &cobra.Command{
	Use:   "notify-publish",
	Short: "Announce a finished release publish on Jira and #db-release-status",
	Long: `Adds a "binaries have been blessed" comment to the CRDB Release Jira
ticket whose Build SHA matches --sha, then posts a self-contained Slack
message to #db-release-status containing the same body and a permalink
back to the new Jira comment.

The Jira ticket is located via JQL on customfield_10590 (Build SHA),
which pick-sha sets when it dispatches build-and-sign. Pass --jira-issue
to bypass the JQL lookup when zero or multiple tickets match.

Dry-run logs the JQL, comment body, and Slack message body without
writing to Jira or Slack — useful for local rehearsal with read-only
Jira creds.`,
	RunE: runNotifyPublish,
}

func init() {
	notifyPublishCmd.Flags().StringVar(&notifyPublishFlags.sha, "sha", "",
		"git SHA being published; used to look up the matching Jira ticket and "+
			"required so the same JQL is reproducible offline (no env-var fallback)")
	notifyPublishCmd.Flags().BoolVar(&notifyPublishFlags.dryRun, "dry-run", false,
		"log the JQL, Jira comment body, and Slack message body without writing")
	notifyPublishCmd.Flags().StringVar(&notifyPublishFlags.channel, "slack-channel", "",
		"Slack channel for the 'blessed' announcement (default: "+releaseChannel+
			" when "+envIsProductionRepo+"=true, "+nonProdChannel+" otherwise)")
	notifyPublishCmd.Flags().StringVar(&notifyPublishFlags.ibmChannel, "ibm-slack-channel", "",
		"Slack channel for the IBM-OEM 'published' announcement (default: "+ibmOEMChannel+
			" when "+envIsProductionRepo+"=true, "+nonProdChannel+" otherwise)")
	notifyPublishCmd.Flags().StringVar(&notifyPublishFlags.jiraIssue, "jira-issue", "",
		"skip JQL lookup and post the comment on this ticket key directly "+
			"(recovery escape hatch for the zero/multiple-match cases)")
	notifyPublishCmd.Flags().StringVar(&notifyPublishFlags.versionFile, "version-file", defaultVersionFilePath,
		"path to the version.txt file, read relative to the working dir")
}

func runNotifyPublish(_ *cobra.Command, _ []string) error {
	if notifyPublishFlags.sha == "" {
		return errors.New("--sha is required")
	}
	jiraToken := os.Getenv(envJiraToken)
	if jiraToken == "" {
		return errors.Newf("%s is not set", envJiraToken)
	}
	jiraEmail := os.Getenv(envJiraEmail)
	if jiraEmail == "" {
		return errors.Newf("%s is not set", envJiraEmail)
	}
	slackToken := os.Getenv(envSlackBotToken)
	if slackToken == "" {
		return errors.Newf("%s is not set", envSlackBotToken)
	}

	version, err := readVersionFile(notifyPublishFlags.versionFile)
	if err != nil {
		return errors.Wrapf(err, "reading version from %s", notifyPublishFlags.versionFile)
	}

	channel := notifyPublishFlags.channel
	if channel == "" {
		channel = nonProdChannel
		if isProductionRepo() {
			channel = releaseChannel
		}
	}
	ibmChannel := notifyPublishFlags.ibmChannel
	if ibmChannel == "" {
		ibmChannel = nonProdChannel
		if isProductionRepo() {
			ibmChannel = ibmOEMChannel
		}
	}

	r := &notifyPublishRunner{
		jira:       newJiraClient(jiraEmail, jiraToken),
		slack:      newSlackClient(slackToken),
		sha:        notifyPublishFlags.sha,
		version:    version,
		channel:    channel,
		ibmChannel: ibmChannel,
		jiraIssue:  notifyPublishFlags.jiraIssue,
		dryRun:     notifyPublishFlags.dryRun,
	}
	return r.run()
}

// notifyPublishRunner holds the wired-up dependencies and per-invocation
// knobs for one publish-notification run. Kept narrow so unit tests can
// substitute a fake jira/slack pair.
type notifyPublishRunner struct {
	jira  jiraSearchCommenter
	slack slackPoster

	sha        string
	version    string // e.g. "v24.3.33"
	channel    string // "blessed" announcement target
	ibmChannel string // IBM-OEM "published" announcement target
	jiraIssue  string // when set, skips the JQL lookup
	dryRun     bool
}

// jiraSearchCommenter is the narrow Jira surface the runner needs.
// Implemented by *jiraClient in production and by fakes in tests.
type jiraSearchCommenter interface {
	SearchJQL(jql string, fields []string) ([]jiraIssue, error)
	AddComment(key string, doc map[string]interface{}) (string, error)
}

// slackPoster is the narrow Slack surface the runner needs. Implemented
// by *slackClient in production.
type slackPoster interface {
	PostMessage(channel, text string) (string, error)
}

func (r *notifyPublishRunner) run() error {
	key, err := r.resolveTicketKey()
	if err != nil {
		return err
	}

	commentDoc := buildBlessedJiraComment(r.version)
	var commentID string
	if r.dryRun {
		log.Printf("[DRY RUN] would post Jira comment on %s; body: %s",
			key, mustJSON(commentDoc))
		commentID = "DRYRUN"
	} else {
		commentID, err = r.jira.AddComment(key, commentDoc)
		if err != nil {
			return errors.Wrapf(err, "adding blessed comment to %s", key)
		}
		if commentID == "" {
			return errors.AssertionFailedf("Jira returned empty comment ID for %s", key)
		}
	}

	slackBody := buildBlessedSlackMessage(r.version, key, commentID)
	ibmBody := buildPublishedSlackMessage(r.version)
	if r.dryRun {
		log.Printf("[DRY RUN] would post to %s; body:\n%s", r.channel, slackBody)
		log.Printf("[DRY RUN] would post to %s; body:\n%s", r.ibmChannel, ibmBody)
		return nil
	}
	if _, err := r.slack.PostMessage(r.channel, slackBody); err != nil {
		// The Jira comment is already live — operator can repost to
		// Slack manually using the permalink in the run log below.
		log.Printf("posted Jira comment %s on %s; Slack post failed, body was:\n%s",
			commentID, key, slackBody)
		return errors.Wrap(err, "posting blessed message to Slack")
	}
	log.Printf("ticket %s: posted blessed comment %s and Slack announcement to %s",
		key, commentID, r.channel)
	// The IBM-OEM post is Slack-only and intentionally has no Jira side.
	// A failure here leaves the blessed message already posted: log the
	// prepared body so the operator can copy-paste into the IBM channel
	// manually before re-running anything.
	if _, err := r.slack.PostMessage(r.ibmChannel, ibmBody); err != nil {
		log.Printf("blessed announcement posted to %s; IBM-OEM post to %s failed, body was:\n%s",
			r.channel, r.ibmChannel, ibmBody)
		return errors.Wrapf(err, "posting IBM-OEM published message to %s", r.ibmChannel)
	}
	log.Printf("posted IBM-OEM announcement to %s", r.ibmChannel)
	return nil
}

// resolveTicketKey returns the Jira ticket key to post on. When the
// operator passed --jira-issue we trust it without an extra round-trip;
// otherwise we run the JQL and require exactly one match. Multiple
// matches and zero matches both surface as errors that name the SHA and
// remind the operator about --jira-issue so recovery is one flag away.
func (r *notifyPublishRunner) resolveTicketKey() (string, error) {
	if r.jiraIssue != "" {
		return r.jiraIssue, nil
	}
	jql := fmt.Sprintf(notifyPublishJQLFormat, r.sha)
	if r.dryRun {
		log.Printf("[DRY RUN] JQL: %s", jql)
	}
	issues, err := r.jira.SearchJQL(jql, []string{"summary"})
	if err != nil {
		return "", errors.Wrap(err, "looking up release ticket by cfBuildSHA")
	}
	switch len(issues) {
	case 0:
		return "", errors.Newf(
			"no CRDB Release ticket found with cfBuildSHA=%s; pass --jira-issue to override",
			r.sha)
	case 1:
		return issues[0].Key, nil
	default:
		keys := make([]string, len(issues))
		for i, is := range issues {
			keys[i] = is.Key
		}
		return "", errors.Newf(
			"multiple CRDB Release tickets match cfBuildSHA=%s: %v; pass --jira-issue to disambiguate",
			r.sha, keys)
	}
}

// readVersionFile returns the version string from pkg/build/version.txt:
// the first non-empty, non-comment line, with a leading "v" prepended if
// it isn't already there so callers can render the version uniformly as
// "v<X.Y.Z>".
func readVersionFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, "v") {
			line = "v" + line
		}
		return line, nil
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", errors.Newf("no version line found in %s", path)
}

// buildBlessedJiraComment renders the "binaries have been blessed"
// announcement as an ADF document. The version is bolded and rendered
// as inline code (matching the manual Jira comments this replaces);
// each bullet is one item in blessedBullets.
func buildBlessedJiraComment(version string) map[string]interface{} {
	bullets := make([][]interface{}, len(blessedBullets))
	for i, b := range blessedBullets {
		bullets[i] = []interface{}{adfText(b)}
	}
	return adfDoc(
		adfPara(
			adfMarked(version, adfCode()),
			adfMarked(" binaries have been blessed:", adfStrong()),
		),
		adfBullet(bullets...),
	)
}

// buildBlessedSlackMessage renders the announcement as Slack mrkdwn.
// Layout matches the manual posts this replaces: a Jira-comment
// permalink on the first line (rendered with the ticket key as the
// link text), then the body and bullets. The message is self-contained
// — PostMessage disables Slack's link unfurling, so the body must not
// depend on the Jira link expanding.
func buildBlessedSlackMessage(version, jiraKey, commentID string) string {
	link := jiraCommentPermalink(jiraKey, commentID)
	var b strings.Builder
	fmt.Fprintf(&b, "<%s|%s>\n", link, jiraKey)
	fmt.Fprintf(&b, "`%s` binaries have been blessed:", version)
	for _, bullet := range blessedBullets {
		fmt.Fprintf(&b, "\n• %s", bullet)
	}
	return b.String()
}

// buildPublishedSlackMessage renders the IBM-OEM-facing "binaries have
// been published" announcement as Slack mrkdwn. Intentionally has no
// Jira link — the IBM-OEM audience doesn't track our Jira tickets, and
// the screenshot of the manual post this replaces is body-only.
func buildPublishedSlackMessage(version string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "`%s` binaries have been published:", version)
	for _, bullet := range publishedBullets {
		fmt.Fprintf(&b, "\n• %s", bullet)
	}
	return b.String()
}

// mustJSON formats v as compact JSON for dry-run logging. Only used on
// values we constructed in-process (ADF docs), so the marshal cannot
// realistically fail; on the off chance it does we surface the error
// inline rather than aborting the dry-run.
func mustJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("<json.Marshal failed: %v>", err)
	}
	return string(b)
}
