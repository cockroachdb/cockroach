// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type daysBeforePrep string

const (
	// dateFormatCommandLine is the expected date format for prepDate and publishDate input values.
	dateFormatCommandLine = "Jan 2, 2006"
	// dateFormatEmail is the expected date format for prepDate and publishDate input values.
	dateFormatEmail = "Monday, January 2"

	// prepDate is the date when we expect to select the release candidate.
	prepDate = "prep-date"
	// publishDate is the date when we expect to publish the release candidate.
	publishDate = "publish-date"
	// nextPublishDate is the new publish date for the release candidate.
	nextPublishDate = "next-publish-date"

	// nextVersion can be left out for stable/patch releases, but needs to be passed in for pre-releases (alpha/beta/rc).
	nextVersion = "next-version"

	// daysBeforePrepDate should be one of "many-days", "day-before", "day-of".
	daysBeforePrepDate = "days-before-prep-date"

	daysBeforePrepManyDays  = daysBeforePrep("many-days")
	daysBeforePrepDayBefore = daysBeforePrep("day-before")
	daysBeforePrepDayOf     = daysBeforePrep("day-of")

	// NoProjectName is the project value for issues without a project.
	NoProjectName = "No Project"

	eventRemovedProject = "removed_from_project"
	eventAddedProject   = "added_to_project"
)

var daysBeforePrepOptions []daysBeforePrep

var blockersFlags = struct {
	releaseSeries      string
	templatesDir       string
	prepDate           string
	publishDate        string
	nextPublishDate    string
	nextVersion        string
	daysBeforePrepDate string
	smtpUser           string
	smtpHost           string
	smtpPort           int
	emailAddresses     []string
}{}

var postReleaseSeriesBlockersCmd = &cobra.Command{
	Use:   "post-blockers",
	Short: "Post blockers against release series",
	Long:  "Fetch post blockers against release series",
	RunE:  fetchReleaseSeriesBlockers,
}

var cancelReleaseSeriesDateCmd = &cobra.Command{
	Use:   "cancel-release-date",
	Short: "Cancel next release series date",
	Long:  "Cancel this release date for the series and push release to next scheduled date",
	RunE:  cancelReleaseSeriesPublishDate,
}

func init() {
	daysBeforePrepOptions = []daysBeforePrep{daysBeforePrepManyDays, daysBeforePrepDayBefore, daysBeforePrepDayOf}

	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.releaseSeries, releaseSeries, "", "major release series")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.templatesDir, templatesDir, "", "templates directory")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.prepDate, prepDate, "", "date to select candidate")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.publishDate, publishDate, "", "date to publish candidate")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.nextVersion, nextVersion, "", "next release version")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.daysBeforePrepDate, daysBeforePrepDate, "", "days before prep date")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.smtpUser, smtpUser, os.Getenv(envSMTPUser), "SMTP user name")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.smtpHost, smtpHost, "", "SMTP host")
	postReleaseSeriesBlockersCmd.Flags().IntVar(&blockersFlags.smtpPort, smtpPort, 0, "SMTP port")
	postReleaseSeriesBlockersCmd.Flags().StringArrayVar(&blockersFlags.emailAddresses, emailAddresses, []string{}, "email addresses")

	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(releaseSeries)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(templatesDir)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(prepDate)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(publishDate)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(daysBeforePrepDate)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(smtpHost)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(smtpPort)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(emailAddresses)

	cancelReleaseSeriesDateCmd.Flags().StringVar(&blockersFlags.releaseSeries, releaseSeries, "", "major release series")
	cancelReleaseSeriesDateCmd.Flags().StringVar(&blockersFlags.templatesDir, templatesDir, "", "templates directory")
	cancelReleaseSeriesDateCmd.Flags().StringVar(&blockersFlags.publishDate, publishDate, "", "date that will be cancelled")
	cancelReleaseSeriesDateCmd.Flags().StringVar(&blockersFlags.nextPublishDate, nextPublishDate, "", "new publish date for release series")
	cancelReleaseSeriesDateCmd.Flags().StringVar(&blockersFlags.smtpUser, smtpUser, os.Getenv(envSMTPUser), "SMTP user name")
	cancelReleaseSeriesDateCmd.Flags().StringVar(&blockersFlags.smtpHost, smtpHost, "", "SMTP host")
	cancelReleaseSeriesDateCmd.Flags().IntVar(&blockersFlags.smtpPort, smtpPort, 0, "SMTP port")
	cancelReleaseSeriesDateCmd.Flags().StringArrayVar(&blockersFlags.emailAddresses, emailAddresses, []string{}, "email addresses")

	_ = cancelReleaseSeriesDateCmd.MarkFlagRequired(releaseSeries)
	_ = cancelReleaseSeriesDateCmd.MarkFlagRequired(templatesDir)
	_ = cancelReleaseSeriesDateCmd.MarkFlagRequired(prepDate)
	_ = cancelReleaseSeriesDateCmd.MarkFlagRequired(publishDate)
	_ = cancelReleaseSeriesDateCmd.MarkFlagRequired(smtpHost)
	_ = cancelReleaseSeriesDateCmd.MarkFlagRequired(smtpPort)
	_ = cancelReleaseSeriesDateCmd.MarkFlagRequired(emailAddresses)
}

func cancelReleaseSeriesPublishDate(_ *cobra.Command, _ []string) error {
	smtpPassword := os.Getenv(envSMTPPassword)
	if smtpPassword == "" {
		return fmt.Errorf("%s environment variable should be set", envSMTPPassword)
	}
	if blockersFlags.smtpUser == "" {
		return fmt.Errorf("either %s environment variable or %s flag should be set", envSMTPUser, smtpUser)
	}
	releasePublishDate, err := time.Parse(dateFormatCommandLine, blockersFlags.publishDate)
	if err != nil {
		return fmt.Errorf("%s is not parseable into %s date layout", blockersFlags.publishDate, dateFormatCommandLine)
	}
	nextReleasePublishDate, err := time.Parse(dateFormatCommandLine, blockersFlags.nextPublishDate)
	if err != nil {
		return fmt.Errorf("%s is not parseable into %s date layout", blockersFlags.nextPublishDate, dateFormatCommandLine)
	}
	nextVersion, err := findNextVersion(blockersFlags.releaseSeries)
	if err != nil {
		return fmt.Errorf("cannot find next release version: %w", err)
	}

	args := messageDataCancelReleaseDate{
		Version:         nextVersion,
		ReleaseSeries:   blockersFlags.releaseSeries,
		ReleaseDate:     releasePublishDate.Format(dateFormatEmail),
		NextReleaseDate: nextReleasePublishDate.Format(dateFormatEmail),
	}
	opts := sendOpts{
		templatesDir: blockersFlags.templatesDir,
		from:         fmt.Sprintf(fromEmailFormat, blockersFlags.smtpUser),
		host:         blockersFlags.smtpHost,
		port:         blockersFlags.smtpPort,
		user:         blockersFlags.smtpUser,
		password:     smtpPassword,
		to:           blockersFlags.emailAddresses,
	}

	fmt.Println("Sending email")
	if err := sendMailCancelReleaseDate(args, opts); err != nil {
		return fmt.Errorf("cannot send email: %w", err)
	}
	return nil
}

func newDaysBeforePrep(daysBefore string) (option daysBeforePrep, ok bool) {
	for _, v := range daysBeforePrepOptions {
		d := daysBeforePrep(daysBefore)
		if d == v {
			return d, true
		}
	}
	return "", false
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
	daysBefore, ok := newDaysBeforePrep(blockersFlags.daysBeforePrepDate)
	if !ok {
		return fmt.Errorf("%s must be one of: %s", daysBeforePrepDate, daysBeforePrepOptions)
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

	// simple check to ensure that .releaseSeries matches .nextVersion
	if !strings.HasPrefix(blockersFlags.nextVersion, fmt.Sprintf("v%s.", blockersFlags.releaseSeries)) {
		return fmt.Errorf("version %s does not match release series %s", blockersFlags.nextVersion, blockersFlags.releaseSeries)
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
		Version:           blockersFlags.nextVersion,
		DaysBeforePrep:    daysBefore,
		DayBeforePrepDate: releasePrepDate.AddDate(0, 0, -1).Format(dateFormatEmail),
		PrepDate:          releasePrepDate.Format(dateFormatEmail),
		ReleaseDate:       releasePublishDate.Format(dateFormatEmail),
		TotalBlockers:     blockers.TotalBlockers,
		BlockersURL:       blockersURL,
		ReleaseBranch:     releaseBranch,
		BlockerList:       blockers.BlockerList,
	}
	opts := sendOpts{
		templatesDir: blockersFlags.templatesDir,
		from:         fmt.Sprintf(fromEmailFormat, blockersFlags.smtpUser),
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

func fetchOpenBlockers(client githubClient, releaseBranchLabel string) (*openBlockers, error) {
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
		total := numBlockersByProject[issue.ProjectName]
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

// mostRecentProjectName returns the most recently added project name, by iterating
// through issue.Project.Name values found in the issues' timeline.
// Returns nil if no project name found.
func mostRecentProjectName(client githubClient, issueNum int) (string, error) {
	removedProjects := make(map[string]bool)
	events, err := client.issueEvents(issueNum)
	if err != nil {
		return "", err
	}

	// Ensure that events are sorted in chronological order.
	sort.Sort(githubEvents(events))

	// Iterate backwards through the events, to find the most recently added project.
	for i := len(events) - 1; i >= 0; i-- {
		projectName := events[i].ProjectName
		if len(projectName) == 0 {
			continue
		}

		// A "removed" project event means that this ProjectCard was added
		// at some earlier point in time, but was subsequently removed.
		if events[i].Event == eventRemovedProject {
			// Add ProjectName to a removedProjects "stack", so
			// that we don't return this Project when we eventually
			// get to the earlier "added" project event.
			removedProjects[projectName] = true
		} else if events[i].Event == eventAddedProject {

			if _, exists := removedProjects[projectName]; exists {
				// This "added" project was subsequently removed from the issue,
				// so we shouldn't return this Project. Now that we got to the
				// "added" event, we can pop it from the "stack".
				delete(removedProjects, projectName)
			} else {
				// This is the most recent project that was added; return this.
				return projectName, err
			}
		}

	}

	return "", nil
}

// githubEvents implements the sort.Sort interface, so that we can
// sort events chronologically by CreatedAt.
type githubEvents []githubEvent

func (e githubEvents) Len() int {
	return len(e)
}

func (e githubEvents) Less(i, j int) bool {
	return e[i].CreatedAt.Before(e[j].CreatedAt)
}

func (e githubEvents) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
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
