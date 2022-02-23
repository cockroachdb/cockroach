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
	"os"

	"github.com/spf13/cobra"
)

const (
	envSMTPUser     = "SMTP_USER"
	envSMTPPassword = "SMTP_PASSWORD"
	envGithubToken  = "GITHUB_TOKEN"
	releaseSeries   = "release-series"
	smtpUser        = "smtp-user"
	smtpHost        = "smtp-host"
	smtpPort        = "smtp-port"
	emailAddresses  = "to"
	dryRun          = "dry-run"
)

var blockersFlags = struct {
	releaseSeries  string
	smtpUser       string
	smtpHost       string
	smtpPort       int
	emailAddresses []string
	dryRun         bool
}{}

var postReleaseSeriesBlockersCmd = &cobra.Command{
	Use:   "post-blockers",
	Short: "Post blockers against release series",
	Long:  "Fetch post blockers against release series",
	RunE:  fetchReleaseSeriesBlockers,
}

func init() {
	// TODO: improve flag usage comments
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.releaseSeries, releaseSeries, "", "major release series")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.smtpUser, smtpUser, os.Getenv(envSMTPUser), "SMTP user name")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.smtpHost, smtpHost, "", "SMTP host")
	postReleaseSeriesBlockersCmd.Flags().IntVar(&blockersFlags.smtpPort, smtpPort, 0, "SMTP port")
	postReleaseSeriesBlockersCmd.Flags().StringArrayVar(&blockersFlags.emailAddresses, emailAddresses, []string{}, "email addresses")
	postReleaseSeriesBlockersCmd.Flags().BoolVar(&blockersFlags.dryRun, dryRun, false, "use dry run Jira project for issues")

	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(releaseSeries)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(smtpUser)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(smtpHost)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(smtpPort)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(emailAddresses)
}

func fetchReleaseSeriesBlockers(_ *cobra.Command, _ []string) error {
	smtpPassword := os.Getenv(envSMTPPassword)
	if smtpPassword == "" {
		return fmt.Errorf("%s environment variable should be set", envSMTPPassword)
	}
	githubToken := os.Getenv(envGithubToken)
	if githubToken == "" {
		return fmt.Errorf("%s environment variable should be set", envGithubToken)
	}

	// TODO(celia): fetch blockers and send out email
	fmt.Println("TODO(celia): fetch blockers and send out email")

	return nil
}
