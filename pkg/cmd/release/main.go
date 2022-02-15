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
	"errors"
	"fmt"
	"html/template"
	"os"

	"github.com/spf13/cobra"
)

var (
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
)

type osEnv struct {
	key   string
	value string
	error error
}

var smtpPassword, jiraUsername, jiraToken, githubToken osEnv

func init() {
	smtpPassword = osEnv{
		key:   "SMTP_PASSWORD",
		error: fmt.Errorf("SMTP_PASSWORD environment variable should be set"),
	}
	jiraUsername = osEnv{
		key:   "JIRA_USERNAME",
		error: fmt.Errorf("JIRA_USERNAME environment variable should be set"),
	}
	jiraToken = osEnv{
		key:   "JIRA_TOKEN",
		error: fmt.Errorf("JIRA_TOKEN environment variable should be set"),
	}
	githubToken = osEnv{
		key:   "GITHUB_TOKEN",
		error: fmt.Errorf("GITHUB_TOKEN environment variable should be set"),
	}
}

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	cmdPickSHA := &cobra.Command{
		Use:   "pick-sha",
		Short: "Pick release git SHA for a particular version and communicate the choice via Jira and email",
		// TODO: improve Long description
		Long: "Pick release git SHA for a particular version and communicate the choice via Jira and email",
		RunE: pickSHAWrapper,
	}
	// TODO: improve flag usage comments
	cmdPickSHA.Flags().StringVar(&qualifyBucket, "qualify-bucket", "", "release qualification metadata GCS bucket")
	cmdPickSHA.Flags().StringVar(&qualifyObjectPrefix, "qualify-object-prefix", "",
		"release qualification object prefix")
	cmdPickSHA.Flags().StringVar(&releaseBucket, "release-bucket", "", "release candidates metadata GCS bucket")
	cmdPickSHA.Flags().StringVar(&releaseObjectPrefix, "release-object-prefix", "", "release candidate object prefix")
	cmdPickSHA.Flags().StringVar(&releaseSeries, "release-series", "", "major release series")
	cmdPickSHA.Flags().StringVar(&smtpUser, "smtp-user", os.Getenv("SMTP_USER"), "SMTP user name")
	cmdPickSHA.Flags().StringVar(&smtpHost, "smtp-host", "", "SMTP host")
	cmdPickSHA.Flags().IntVar(&smtpPort, "smtp-port", 0, "SMTP port")
	cmdPickSHA.Flags().StringArrayVar(&emailAddresses, "to", []string{}, "email addresses")
	cmdPickSHA.Flags().BoolVar(&dryRun, "dry-run", false, "use dry run Jira project for issues")
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
		if err := cmdPickSHA.MarkFlagRequired(flag); err != nil {
			return err
		}
	}
	// TODO: improve rootCmd description
	rootCmd := &cobra.Command{Use: "release"}
	rootCmd.AddCommand(cmdPickSHA)

	cmdGetBlockers := &cobra.Command{
		Use:   "get-blockers",
		Short: "Get release blocker details for a particular release branch and communicate via email",
		// TODO: improve Long description
		Long: "Get release blocker details for a particular release branch and communicate via email",
		RunE: getReleaseBlockersWrapper,
	}
	// TODO: add optional & required flag details
	rootCmd.AddCommand(cmdGetBlockers)

	if err := rootCmd.Execute(); err != nil {
		return err
	}
	return nil
}

func setRequiredEnvs(requiredEnvs ...osEnv) error {
	var err error
	for _, env := range requiredEnvs {
		env.value = os.Getenv(env.key)
		if env.value == "" {
			if err == nil {
				err = env.error
			} else {
				err = fmt.Errorf("%w\n %s", err, env.error.Error())
			}
			return env.error
		}
	}
	return nil
}

func getReleaseBlockersWrapper(_ *cobra.Command, _ []string) error {
	err := setRequiredEnvs(githubToken, smtpPassword)
	if err != nil {
		return err
	}
	return getReleaseBlockers(githubToken.value, smtpPassword.value)
}

func getReleaseBlockers(githubToken, smtpPassword string) error {
	return errors.New("not yet implemented")
}

func pickSHAWrapper(_ *cobra.Command, _ []string) error {
	err := setRequiredEnvs(jiraUsername, jiraToken, smtpPassword)
	if err != nil {
		return err
	}
	return pickSHA(jiraUsername.value, jiraToken.value, smtpPassword.value)
}

func pickSHA(jiraUsername, jiraToken, smtpPassword string) error {
	nextRelease, err := findNextRelease(releaseSeries)
	if err != nil {
		return fmt.Errorf("cannot find next release: %w", err)
	}
	// TODO: improve stdout message
	fmt.Println("Previous version:", nextRelease.prevReleaseVersion)
	fmt.Println("Next version:", nextRelease.nextReleaseVersion)
	fmt.Println("Release SHA:", nextRelease.buildInfo.SHA)

	// TODO: before copying check if it's already there and bail if exists, can be forced by -f
	releaseInfoPath := fmt.Sprintf("%s/%s.json", releaseObjectPrefix, nextRelease.nextReleaseVersion)
	fmt.Println("Publishing release candidate metadata")
	if err := publishReleaseCandidateInfo(context.Background(), nextRelease, releaseBucket, releaseInfoPath); err != nil {
		return fmt.Errorf("cannot publish release metadata: %w", err)
	}

	fmt.Println("Creating SRE issue")
	jiraClient, err := newJiraClient(jiraBaseURL, jiraUsername, jiraToken)
	if err != nil {
		return fmt.Errorf("cannot create Jira client: %w", err)
	}
	sreIssue, err := createSREIssue(jiraClient, nextRelease, dryRun)
	if err != nil {
		return err
	}

	fmt.Println("Creating tracking issue")
	trackingIssue, err := createTrackingIssue(jiraClient, nextRelease, sreIssue, dryRun)
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
		from:     fmt.Sprintf("Justin Beaver <%s>", smtpUser),
		host:     smtpHost,
		port:     smtpPort,
		user:     smtpUser,
		password: smtpPassword,
		to:       emailAddresses,
	}
	fmt.Println("Sending email")
	if err := sendmail(args, opts); err != nil {
		return fmt.Errorf("cannot send email: %w", err)
	}
	return nil
}
