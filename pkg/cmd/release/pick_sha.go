// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"html/template"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

const (
	qualifyBucket       = "qualify-bucket"
	qualifyObjectPrefix = "qualify-object-prefix"
	releaseBucket       = "release-bucket"
	releaseObjectPrefix = "release-object-prefix"
)

var pickSHAFlags = struct {
	qualifyBucket       string
	qualifyObjectPrefix string
	releaseBucket       string
	releaseObjectPrefix string
	releaseSeries       string
	templatesDir        string
	smtpUser            string
	smtpHost            string
	smtpPort            int
	emailAddresses      []string
	dryRun              bool
}{}

var pickSHACmd = &cobra.Command{
	Use:   "pick-sha",
	Short: "Pick release git SHA for a particular version and communicate the choice via Jira and email",
	// TODO: improve Long description
	Long: "Pick release git SHA for a particular version and communicate the choice via Jira and email",
	RunE: pickSHA,
}

func init() {
	// TODO: improve flag usage comments
	pickSHACmd.Flags().StringVar(&pickSHAFlags.qualifyBucket, qualifyBucket, "", "release qualification metadata GCS bucket")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.qualifyObjectPrefix, qualifyObjectPrefix, "",
		"release qualification object prefix")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.releaseBucket, releaseBucket, "", "release candidates metadata GCS bucket")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.releaseObjectPrefix, releaseObjectPrefix, "", "release candidate object prefix")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.releaseSeries, releaseSeries, "", "major release series")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.templatesDir, templatesDir, "", "templates directory")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.smtpUser, smtpUser, os.Getenv(envSMTPUser), "SMTP user name")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.smtpHost, smtpHost, "", "SMTP host")
	pickSHACmd.Flags().IntVar(&pickSHAFlags.smtpPort, smtpPort, 0, "SMTP port")
	pickSHACmd.Flags().StringArrayVar(&pickSHAFlags.emailAddresses, emailAddresses, []string{}, "email addresses")
	pickSHACmd.Flags().BoolVar(&pickSHAFlags.dryRun, dryRun, false, "use dry run Jira project for issues")

	requiredFlags := []string{
		qualifyBucket,
		qualifyObjectPrefix,
		releaseBucket,
		releaseObjectPrefix,
		releaseSeries,
		smtpUser,
		smtpHost,
		smtpPort,
		emailAddresses,
	}
	for _, flag := range requiredFlags {
		if err := pickSHACmd.MarkFlagRequired(flag); err != nil {
			panic(err)
		}
	}
}

func pickSHA(_ *cobra.Command, _ []string) error {
	smtpPassword := os.Getenv("SMTP_PASSWORD")
	if smtpPassword == "" {
		return fmt.Errorf("SMTP_PASSWORD environment variable should be set")
	}
	jiraUsername := os.Getenv("JIRA_USERNAME")
	if jiraUsername == "" {
		return fmt.Errorf("JIRA_USERNAME environment variable should be set")
	}
	jiraToken := os.Getenv("JIRA_TOKEN")
	if jiraToken == "" {
		return fmt.Errorf("JIRA_TOKEN environment variable should be set")
	}

	nextRelease, err := findNextRelease(pickSHAFlags.releaseSeries)
	if err != nil {
		return fmt.Errorf("cannot find next release: %w", err)
	}
	// TODO: improve stdout message
	fmt.Println("Previous version:", nextRelease.prevReleaseVersion)
	fmt.Println("Next version:", nextRelease.nextReleaseVersion)
	fmt.Println("Release SHA:", nextRelease.buildInfo.SHA)

	// Verify that the guessed version matches the contents of the version.txt file.
	curVersion, err := fileContent(nextRelease.buildInfo.SHA, versionFile)
	if err != nil {
		return fmt.Errorf("reading version file: %w", err)
	}
	curVersion = strings.TrimSpace(curVersion)
	if curVersion != nextRelease.nextReleaseVersion {
		return fmt.Errorf("guessed version %s does not match version.txt %s", nextRelease.nextReleaseVersion, curVersion)
	}

	// TODO: before copying check if it's already there and bail if exists, can be forced by -f
	releaseInfoPath := fmt.Sprintf("%s/%s.json", pickSHAFlags.releaseObjectPrefix, nextRelease.nextReleaseVersion)
	fmt.Println("Publishing release candidate metadata")
	if err := publishReleaseCandidateInfo(context.Background(), nextRelease, pickSHAFlags.releaseBucket, releaseInfoPath); err != nil {
		return fmt.Errorf("cannot publish release metadata: %w", err)
	}

	jiraClient, err := newJiraClient(jiraBaseURL, jiraUsername, jiraToken)
	if err != nil {
		return fmt.Errorf("cannot create Jira client: %w", err)
	}

	fmt.Println("Creating tracking issue")
	trackingIssue, err := createTrackingIssue(jiraClient, nextRelease, pickSHAFlags.dryRun)
	if err != nil {
		return fmt.Errorf("cannot create tracking issue: %w", err)
	}
	diffURL := template.URL(
		fmt.Sprintf("https://github.com/cockroachdb/cockroach/compare/%s...%s",
			nextRelease.prevReleaseVersion,
			nextRelease.buildInfo.SHA))
	args := messageDataPickSHA{
		Version:          nextRelease.nextReleaseVersion,
		SHA:              nextRelease.buildInfo.SHA,
		TrackingIssue:    trackingIssue.Key,
		TrackingIssueURL: template.URL(trackingIssue.url()),
		DiffURL:          diffURL,
	}
	opts := sendOpts{
		templatesDir: pickSHAFlags.templatesDir,
		from:         fmt.Sprintf(fromEmailFormat, pickSHAFlags.smtpUser),
		host:         pickSHAFlags.smtpHost,
		port:         pickSHAFlags.smtpPort,
		user:         pickSHAFlags.smtpUser,
		password:     smtpPassword,
		to:           pickSHAFlags.emailAddresses,
	}
	fmt.Println("Sending email")
	if err := sendMailPickSHA(args, opts); err != nil {
		return fmt.Errorf("cannot send email: %w", err)
	}
	return nil
}
