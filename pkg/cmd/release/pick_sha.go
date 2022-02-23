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
	"context"
	"fmt"
	"html/template"
	"os"

	"github.com/spf13/cobra"
)

var pickSHAFlags = struct {
	qualifyBucket       string
	qualifyObjectPrefix string
	releaseBucket       string
	releaseObjectPrefix string
	releaseSeries       string
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
	pickSHACmd.Flags().StringVar(&pickSHAFlags.qualifyBucket, "qualify-bucket", "", "release qualification metadata GCS bucket")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.qualifyObjectPrefix, "qualify-object-prefix", "",
		"release qualification object prefix")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.releaseBucket, "release-bucket", "", "release candidates metadata GCS bucket")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.releaseObjectPrefix, "release-object-prefix", "", "release candidate object prefix")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.releaseSeries, "release-series", "", "major release series")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.smtpUser, "smtp-user", os.Getenv("SMTP_USER"), "SMTP user name")
	pickSHACmd.Flags().StringVar(&pickSHAFlags.smtpHost, "smtp-host", "", "SMTP host")
	pickSHACmd.Flags().IntVar(&pickSHAFlags.smtpPort, "smtp-port", 0, "SMTP port")
	pickSHACmd.Flags().StringArrayVar(&pickSHAFlags.emailAddresses, "to", []string{}, "email addresses")
	pickSHACmd.Flags().BoolVar(&pickSHAFlags.dryRun, "dry-run", false, "use dry run Jira project for issues")
	requiredFlags := []string{
		"qualify-bucket",
		"qualify-object-prefix",
		"release-bucket",
		"release-object-prefix",
		"release-series",
		"smtp-user",
		"smtp-host",
		"smtp-port",
		"to",
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

	// TODO: before copying check if it's already there and bail if exists, can be forced by -f
	releaseInfoPath := fmt.Sprintf("%s/%s.json", pickSHAFlags.releaseObjectPrefix, nextRelease.nextReleaseVersion)
	fmt.Println("Publishing release candidate metadata")
	if err := publishReleaseCandidateInfo(context.Background(), nextRelease, pickSHAFlags.releaseBucket, releaseInfoPath); err != nil {
		return fmt.Errorf("cannot publish release metadata: %w", err)
	}

	fmt.Println("Creating SRE issue")
	jiraClient, err := newJiraClient(jiraBaseURL, jiraUsername, jiraToken)
	if err != nil {
		return fmt.Errorf("cannot create Jira client: %w", err)
	}
	sreIssue, err := createSREIssue(jiraClient, nextRelease, pickSHAFlags.dryRun)
	if err != nil {
		return err
	}

	fmt.Println("Creating tracking issue")
	trackingIssue, err := createTrackingIssue(jiraClient, nextRelease, sreIssue, pickSHAFlags.dryRun)
	if err != nil {
		return fmt.Errorf("cannot create tracking issue: %w", err)
	}
	diffURL := template.URL(
		fmt.Sprintf("https://github.com/cockroachdb/cockroach/compare/%s...%s",
			nextRelease.prevReleaseVersion,
			nextRelease.buildInfo.SHA))
	args := emailArgs{
		Version:          nextRelease.nextReleaseVersion,
		SHA:              nextRelease.buildInfo.SHA,
		TrackingIssue:    trackingIssue.Key,
		TrackingIssueURL: template.URL(trackingIssue.url()),
		DiffURL:          diffURL,
	}
	opts := smtpOpts{
		from:     fmt.Sprintf("Justin Beaver <%s>", pickSHAFlags.smtpUser),
		host:     pickSHAFlags.smtpHost,
		port:     pickSHAFlags.smtpPort,
		user:     pickSHAFlags.smtpUser,
		password: smtpPassword,
		to:       pickSHAFlags.emailAddresses,
	}
	fmt.Println("Sending email")
	if err := sendmail(args, opts); err != nil {
		return fmt.Errorf("cannot send email: %w", err)
	}
	return nil
}
