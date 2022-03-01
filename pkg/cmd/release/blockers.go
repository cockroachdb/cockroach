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
	"os"
	"sort"
	"time"

	"github.com/spf13/cobra"
)

const (
	// dateFormatCommandLine is the expected date format for prepDate and publishDate input values.
	dateFormatCommandLine = "2006-01-02"
	// dateFormatCommandLine is the expected date format for prepDate and publishDate input values.
	dateFormatEmail = "Monday, January 2"

	// prepDate is the date when we expect to select the release candidate.
	prepDate = "prep-date"
	// publishDate is the date when we expect to publish the release candidate.
	publishDate = "publish-date"

	// nextVersion can be left out for stable/patch releases, but needs to be passed in for pre-releases (alpha/beta/rc).
	nextVersion = "next-version"

	// NoProjectName is the project value for issues without a project.
	NoProjectName = "No Project"
)

var blockersFlags = struct {
	releaseSeries  string
	templatesDir   string
	prepDate       string
	publishDate    string
	nextVersion    string
	smtpUser       string
	smtpHost       string
	smtpPort       int
	emailAddresses []string
}{}

var postReleaseSeriesBlockersCmd = &cobra.Command{
	Use:   "post-blockers",
	Short: "Post blockers against release series",
	Long:  "Fetch post blockers against release series",
	RunE:  fetchReleaseSeriesBlockers,
}

func init() {
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.releaseSeries, releaseSeries, "", "major release series")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.templatesDir, templatesDir, "", "templates directory")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.prepDate, prepDate, "", "date to select candidate")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.publishDate, publishDate, "", "date to publish candidate")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.nextVersion, nextVersion, "", "next release version")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.smtpUser, smtpUser, os.Getenv(envSMTPUser), "SMTP user name")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.smtpHost, smtpHost, "", "SMTP host")
	postReleaseSeriesBlockersCmd.Flags().IntVar(&blockersFlags.smtpPort, smtpPort, 0, "SMTP port")
	postReleaseSeriesBlockersCmd.Flags().StringArrayVar(&blockersFlags.emailAddresses, emailAddresses, []string{}, "email addresses")

	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(releaseSeries)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(templatesDir)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(prepDate)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(publishDate)
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
	if blockersFlags.smtpUser == "" {
		return fmt.Errorf("either %s environment variable or %s flag should be set", envSMTPUser, smtpUser)
	}
	releasePrepDate, err := time.Parse(dateFormatCommandLine, blockersFlags.prepDate)
	if err != nil {
		return fmt.Errorf("%s is not parseable into %s date layout", blockersFlags.prepDate, dateFormatCommandLine)
	}
	releasePublishDate, err := time.Parse(dateFormatCommandLine, blockersFlags.publishDate)
	if err != nil {
		return fmt.Errorf("%s is not parseable into %s date layout", blockersFlags.publishDate, dateFormatCommandLine)
	}
	if blockersFlags.nextVersion == "" {
		var err error
		blockersFlags.nextVersion, err = findNextVersion(blockersFlags.releaseSeries)
		if err != nil {
			return fmt.Errorf("cannot find next release version: %w", err)
		}
	}

	blockersURL := "go.crdb.dev/blockers/" + blockersFlags.releaseSeries
	releaseBranch := "release-" + blockersFlags.releaseSeries
	releaseBranchLabel := fmt.Sprintf("branch-release-%s", blockersFlags.releaseSeries)

	client := newGithubClient(context.Background(), githubToken)
	branchExists, err := client.branchExists(releaseBranch)
	if err != nil {
		return fmt.Errorf("cannot fetch branches: %w", err)
	}
	if !branchExists {
		blockersURL = "go.crdb.dev/blockers"
		releaseBranch = "master"
		releaseBranchLabel = "branch-master"
	}

	blockers, err := fetchOpenBlockers(client, releaseBranchLabel)
	if err != nil {
		return fmt.Errorf("cannot fetch blockers: %w", err)
	}

	args := messageDataPostBlockers{
		Version:       blockersFlags.nextVersion,
		PrepDate:      releasePrepDate.Format(dateFormatEmail),
		ReleaseDate:   releasePublishDate.Format(dateFormatEmail),
		TotalBlockers: blockers.TotalBlockers,
		BlockersURL:   blockersURL,
		ReleaseBranch: releaseBranch,
		BlockerList:   blockers.BlockerList,
	}
	opts := sendOpts{
		templatesDir: blockersFlags.templatesDir,
		from:         fmt.Sprintf("Justin Beaver <%s>", blockersFlags.smtpUser),
		host:         blockersFlags.smtpHost,
		port:         blockersFlags.smtpPort,
		user:         blockersFlags.smtpUser,
		password:     smtpPassword,
		to:           blockersFlags.emailAddresses,
	}

	fmt.Println("Sending email")
	if err := sendMailPostBlockers(args, opts); err != nil {
		return fmt.Errorf("cannot send email: %w", err)
	}
	return nil
}

type openBlockers struct {
	TotalBlockers int
	BlockerList   []ProjectBlocker
}

func fetchOpenBlockers(
	client githubClient,
	releaseBranchLabel string,
) (*openBlockers, error) {
	issues, err := client.openIssues([]string{
		"release-blocker",
		releaseBranchLabel,
	})
	if err != nil {
		return nil, err
	}
	if len(issues) == 0 {
		return &openBlockers{}, nil
	}
	numBlockersByProject := make(map[string]int)
	for _, issue := range issues {
		if len(issue.ProjectName) == 0 {
			issue.ProjectName = NoProjectName
		}
		total, _ := numBlockersByProject[issue.ProjectName]
		numBlockersByProject[issue.ProjectName] = total + 1
	}
	var blockers projectBlockers
	for projectName, numBlockers := range numBlockersByProject {
		blockers = append(blockers, ProjectBlocker{
			ProjectName: projectName,
			NumBlockers: numBlockers,
		})
	}
	// sorting blockers Projects alphabetically, except for "No Project", which always goes last.
	sort.Sort(blockers)
	return &openBlockers{
		TotalBlockers: len(issues),
		BlockerList:   blockers,
	}, nil
}

// projectBlockers implements the sort.Sort interface, so that we can
// sort blockers alphabetically by Project Name,
// except for "No Project" (blockers without a project), which should always
// be listed last.
type projectBlockers []ProjectBlocker

func (p projectBlockers) Len() int {
	return len(p)
}

func (p projectBlockers) Less(i, j int) bool {
	if p[i].ProjectName == NoProjectName {
		return false
	}
	if p[j].ProjectName == NoProjectName {
		return true
	}
	return p[i].ProjectName < p[j].ProjectName
}

func (p projectBlockers) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
